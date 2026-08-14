package workload

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/stablelabs/loadtester/accounts"
	"github.com/stablelabs/loadtester/deployment"
)

// These tests cover the rotation invariants that per-account in-flight depth
// introduced. The engine used to keep at most one tx per account in flight, so
// "account is in exactly one place" was enforced by the pending map itself.
// With depth > 1 an account is simultaneously in the pending map (several
// times) AND eligible to send, and the only thing keeping it from being
// enqueued once per in-flight tx is the queued CAS in offer().

// testDriver builds the minimum Driver the engine touches: a signer-only pool,
// a builder, and a sink.
func testDriver(t *testing.T) (*Driver, *accounts.Pool) {
	t.Helper()
	chainID := big.NewInt(988)
	pool := &accounts.Pool{ChainID: chainID, Signer: types.LatestSignerForChainID(chainID)}
	d := &Driver{
		pool:    pool,
		builder: NewBuilder(pool, nil, &deployment.Deployment{}, map[Kind]int32{}),
		sink:    &Sink{},
	}
	d.feeCap.Store(big.NewInt(1_000_000_000))
	d.tip.Store(big.NewInt(1))
	return d, pool
}

func testAccounts(t *testing.T, n int) []*accounts.Account {
	t.Helper()
	accs := make([]*accounts.Account, n)
	for i := range accs {
		k, err := crypto.GenerateKey()
		if err != nil {
			t.Fatalf("key: %v", err)
		}
		a := accounts.NewAccount(k)
		a.SetBase(0, 0) // seeded, so ensureSeeded never probes
		accs[i] = a
	}
	return accs
}

// okEngine builds an engine whose sends always succeed and whose probe reports
// a fixed committed nonce.
func okEngine(t *testing.T, accs []*accounts.Account, depth int, sent *atomic.Int64) *laneEngine {
	t.Helper()
	send := func(context.Context, *types.Transaction) error { sent.Add(1); return nil }
	probe := func(context.Context, common.Address) (uint64, error) { return 0, nil }
	head := func(context.Context) (uint64, error) { return 0, nil }
	hashes := func(context.Context, uint64) ([]common.Hash, error) { return nil, nil }
	return newLaneEngine("test", 0, send, probe, head, hashes, accs, depth, 0)
}

// TestDepthCapsInflightPerAccount is the core property: one account may hold
// exactly `depth` txs in flight and not one more, and it stays sendable the
// whole way up to that cap WITHOUT any confirmation arriving. At depth 1 this
// reduces to the old behavior (one send, then blocked).
func TestDepthCapsInflightPerAccount(t *testing.T) {
	for _, depth := range []int{1, 2, 4, 16} {
		accs := testAccounts(t, 1)
		var sent atomic.Int64
		e := okEngine(t, accs, depth, &sent)
		d, _ := testDriver(t)

		// One pop fills the whole depth in a burst; the extra calls must add
		// nothing, since no confirmation has freed a slot.
		for i := 0; i < 3; i++ {
			e.sendOne(context.Background(), d, KindValue)
		}
		if got := sent.Load(); got != int64(depth) {
			t.Fatalf("depth %d: sent %d txs, want exactly %d", depth, got, depth)
		}
		if got := e.npend.Load(); got != int64(depth) {
			t.Fatalf("depth %d: npend %d, want %d", depth, got, depth)
		}
		if got := e.inflight(accs[0].Addr); got != int64(depth) {
			t.Fatalf("depth %d: inflight %d, want %d", depth, got, depth)
		}
		// Each send must have used a distinct, consecutive nonce.
		if got := accs[0].Peek(0); got != uint64(depth) {
			t.Fatalf("depth %d: next nonce %d, want %d", depth, got, depth)
		}
	}
}

// TestBurstFillsDepthInOneRotation is the throughput property the whole change
// exists for. One pop must put the account's entire nonce run into the mempool
// back to back, with no confirmation in between: consecutive nonces are only
// admissible inside one CheckTx epoch, because the app resets its CheckTx nonce
// on Commit and selective recheck only restores senders the block touched.
func TestBurstFillsDepthInOneRotation(t *testing.T) {
	accs := testAccounts(t, 1)
	var sent atomic.Int64
	e := okEngine(t, accs, 4, &sent)
	d, _ := testDriver(t)

	n := e.sendOne(context.Background(), d, KindValue)
	if n != 4 {
		t.Fatalf("one rotation sent %d txs, want the full depth of 4", n)
	}
	if got := sent.Load(); got != 4 {
		t.Fatalf("node saw %d sends, want 4", got)
	}
	// Nonces must be a contiguous run, and the account is now at its cap, so it
	// must NOT be back in the ready queue.
	if got := accs[0].Peek(0); got != 4 {
		t.Fatalf("next nonce %d after burst, want 4 (contiguous run)", got)
	}
	if got := len(e.ready); got != 0 {
		t.Fatalf("ready=%d after filling the depth, want 0", got)
	}
}

// TestBurstStopsAtFirstRejection: a refused nonce must end the burst. Sending
// nonce+1 after nonce N was rejected would open a gap the chain never closes.
func TestBurstStopsAtFirstRejection(t *testing.T) {
	accs := testAccounts(t, 1)
	var calls atomic.Int64
	send := func(context.Context, *types.Transaction) error {
		if calls.Add(1) >= 3 {
			return errors.New("mempool is full")
		}
		return nil
	}
	probe := func(context.Context, common.Address) (uint64, error) { return 0, nil }
	head := func(context.Context) (uint64, error) { return 0, nil }
	hashes := func(context.Context, uint64) ([]common.Hash, error) { return nil, nil }
	e := newLaneEngine("test", 0, send, probe, head, hashes, accs, 8, 0)
	d, _ := testDriver(t)

	n := e.sendOne(context.Background(), d, KindValue)
	if n != 2 {
		t.Fatalf("burst sent %d txs, want 2 (stop at the third, which was refused)", n)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("node saw %d sends, want 3 (two accepted plus the one refusal)", got)
	}
	// The refused nonce must not have been consumed.
	if got := accs[0].Peek(0); got != 2 {
		t.Fatalf("next nonce %d, want 2 - a refused nonce must stay unused", got)
	}
}

// TestResolveFreesOneSlot: confirming one tx drops in-flight by exactly one and
// makes the account sendable again, without ever double-enqueueing it.
func TestResolveFreesOneSlot(t *testing.T) {
	accs := testAccounts(t, 1)
	var sent atomic.Int64
	e := okEngine(t, accs, 2, &sent)
	d, _ := testDriver(t)

	e.sendOne(context.Background(), d, KindValue) // one burst fills depth 2
	if e.inflight(accs[0].Addr) != 2 || len(e.ready) != 0 {
		t.Fatalf("setup: inflight=%d ready=%d, want 2/0", e.inflight(accs[0].Addr), len(e.ready))
	}

	// Resolve the oldest pending entry, as the confirm feed would.
	var first *pendingTx
	e.pending.Range(func(k, v any) bool {
		p := v.(*pendingTx)
		if first == nil || p.nonce < first.nonce {
			first = p
		}
		return true
	})
	if !e.resolve(d, first) {
		t.Fatal("resolve returned false")
	}
	if got := e.inflight(accs[0].Addr); got != 1 {
		t.Fatalf("inflight %d after one resolve, want 1", got)
	}
	if got := len(e.ready); got != 1 {
		t.Fatalf("ready %d after resolve, want exactly 1 (no duplicate enqueue)", got)
	}
	// Resolving the second must not enqueue a second copy.
	e.pending.Range(func(k, v any) bool { e.resolve(d, v.(*pendingTx)); return true })
	if got := len(e.ready); got != 1 {
		t.Fatalf("ready %d after both resolves, want 1 - the account was enqueued twice", got)
	}
}

// TestNoDuplicateReadyEntriesUnderConcurrency hammers the queued CAS with
// senders and resolvers racing on a small pool. The invariant is that the ready
// queue never holds more entries than there are accounts: a duplicate would let
// two workers own one account and reintroduce duplicate-nonce sends.
func TestNoDuplicateReadyEntriesUnderConcurrency(t *testing.T) {
	const nAcc, depth = 16, 4
	accs := testAccounts(t, nAcc)
	var sent atomic.Int64
	e := okEngine(t, accs, depth, &sent)
	d, _ := testDriver(t)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var maxReady atomic.Int64

	for i := 0; i < 8; i++ { // senders
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				e.sendOne(ctx, d, KindValue)
				if n := int64(len(e.ready)); n > maxReady.Load() {
					maxReady.Store(n)
				}
			}
		}()
	}
	for i := 0; i < 4; i++ { // resolvers, standing in for the confirm feed
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				e.pending.Range(func(_, v any) bool {
					e.resolve(d, v.(*pendingTx))
					return false // one per pass, to keep them interleaved
				})
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	wg.Wait()

	if got := maxReady.Load(); got > nAcc {
		t.Fatalf("ready queue peaked at %d entries for %d accounts - accounts are being enqueued twice", got, nAcc)
	}
	if got := e.overflow.Load(); got != 0 {
		t.Fatalf("ready queue overflowed %d times", got)
	}
	// Every account must still be accounted for: in flight, in ready, or both.
	for _, a := range accs {
		if e.inflight(a.Addr) == 0 && !e.slot(a.Addr).queued.Load() {
			t.Fatalf("account %s fell out of rotation entirely", a.Addr)
		}
	}
}

// TestConfirmFeedResolvesOnlyMatchingHashes is the unit-level guard for the
// failure that ValidateBlockFeed exists to prevent. A block feed pointed at a
// node whose eth_getBlockByNumber reports hashes the engine never sent (what a
// node with the EVM tx indexer disabled does) resolves NOTHING - silently. The
// engine keeps running, the janitor probe picks up the slack, and throughput
// collapses with no error logged.
func TestConfirmFeedResolvesOnlyMatchingHashes(t *testing.T) {
	accs := testAccounts(t, 4)
	var sent atomic.Int64
	e := okEngine(t, accs, 1, &sent)
	d, _ := testDriver(t)
	for range accs {
		e.sendOne(context.Background(), d, KindValue)
	}
	if e.npend.Load() != 4 {
		t.Fatalf("setup: npend %d, want 4", e.npend.Load())
	}

	// Feed a block of hashes that are NOT ours: nothing may resolve.
	foreign := []common.Hash{{1}, {2}, {3}, {4}}
	e.hashes = func(context.Context, uint64) ([]common.Hash, error) { return foreign, nil }
	e.head = func(context.Context) (uint64, error) { return 1, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	e.confirmLoop(ctx, d, 10*time.Millisecond)
	cancel()
	if got := e.npend.Load(); got != 4 {
		t.Fatalf("npend %d after a foreign-hash feed, want 4 untouched", got)
	}

	// Now feed the real hashes: everything resolves.
	var ours []common.Hash
	e.pending.Range(func(_, v any) bool {
		h, _, _, _, _ := v.(*pendingTx).snapshot()
		ours = append(ours, h)
		return true
	})
	e.hashes = func(context.Context, uint64) ([]common.Hash, error) { return ours, nil }
	e.head = func(context.Context) (uint64, error) { return 2, nil }
	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	e.confirmLoop(ctx2, d, 10*time.Millisecond)
	cancel2()
	if got := e.npend.Load(); got != 0 {
		t.Fatalf("npend %d after the matching feed, want 0", got)
	}
}

// TestMempoolDepthControllerRefillsOnDrain is the property that makes steady
// block sizes possible: when the proposer empties the mempool, the driver must
// be free to refill it AT ONCE, without waiting for its own confirmations. The
// in-flight ceiling cannot do this - it still counts the txs that were just
// mined - so the controller keys on the measured depth instead.
func TestMempoolDepthControllerRefillsOnDrain(t *testing.T) {
	const target = 500
	accs := testAccounts(t, 4000)
	var sent atomic.Int64
	e := okEngine(t, accs, 1, &sent)
	e.depthTarget = target
	e.depthBudget.Store(target)
	d, _ := testDriver(t)

	drive := func() int64 {
		before := sent.Load()
		for i := 0; i < target*2; i++ {
			e.sendOne(context.Background(), d, KindValue)
		}
		return sent.Load() - before
	}

	// Fill to the setpoint, then stop dead.
	if n := drive(); n != target {
		t.Fatalf("first fill sent %d, want exactly the setpoint %d", n, target)
	}
	if n := drive(); n != 0 {
		t.Fatalf("sent %d more while the mempool was believed full, want 0", n)
	}

	// A block takes everything. Every account that just sent is still
	// unconfirmed, so npend is unchanged and an in-flight budget would still be
	// saturated - but the mempool is empty and the driver must refill it.
	if e.npend.Load() != target {
		t.Fatalf("npend %d, want %d still unconfirmed", e.npend.Load(), target)
	}
	e.observeMempoolDepth(0)
	if n := drive(); n != target {
		t.Fatalf("after the mempool drained to 0 the driver sent %d, want a full refill of %d "+
			"(this is the empty-block bug: refill must not wait on confirmations)", n, target)
	}

	// A partially drained mempool tops up by exactly the shortfall.
	e.observeMempoolDepth(target - 120)
	if n := drive(); n != 120 {
		t.Fatalf("top-up sent %d, want exactly the 120-tx shortfall", n)
	}

	// An over-full mempool sends nothing, and the negative budget must not
	// bank into a later burst - the next observation resets it outright.
	e.observeMempoolDepth(target + 900)
	if n := drive(); n != 0 {
		t.Fatalf("sent %d while over the setpoint, want 0", n)
	}
	e.observeMempoolDepth(target - 50)
	if n := drive(); n != 50 {
		t.Fatalf("sent %d after recovering, want 50 - a stale negative budget was carried over", n)
	}
}

// TestInflightLimitBacksOffAndRecovers covers the offered-load controller. A
// full mempool must pull the ceiling down to what the endpoint actually
// accepted - and must do it WITHOUT further sends, since the whole point is to
// stop spending the endpoint's CheckTx budget on txs it has no room for.
func TestInflightLimitBacksOffAndRecovers(t *testing.T) {
	accs := testAccounts(t, 100)
	var sent atomic.Int64
	e := okEngine(t, accs, 4, &sent)
	d, _ := testDriver(t)

	if got := e.inflightLimit.Load(); got != 400 {
		t.Fatalf("initial limit %d, want accountsN*depth = 400", got)
	}

	// Fill some in-flight, then report backpressure at that level. The floor is
	// sized for real pools, so drop it out of the way to exercise the decrease.
	e.inflightFloor = 1
	for i := 0; i < 20; i++ {
		e.sendOne(context.Background(), d, KindValue)
	}
	inflight := e.npend.Load()
	if inflight == 0 {
		t.Fatal("no txs in flight after 20 rotations")
	}
	e.noteMempoolFull(d)
	limit := e.inflightLimit.Load()
	if limit >= inflight {
		t.Fatalf("limit %d did not drop below the refused in-flight level %d", limit, inflight)
	}

	// The floor is a hard stop: repeated backpressure must never drive the
	// offered load to zero, or the run would wedge instead of throttling.
	e.inflightFloor = 64
	for i := 0; i < 500; i++ {
		e.noteMempoolFull(d)
	}
	if got := e.inflightLimit.Load(); got < e.inflightFloor {
		t.Fatalf("limit %d fell below the floor %d", got, e.inflightFloor)
	}

	// Over the ceiling, sendOne must do nothing at all - no send, no pop.
	e.inflightLimit.Store(e.npend.Load())
	before, readyBefore := sent.Load(), len(e.ready)
	if n := e.sendOne(context.Background(), d, KindValue); n != 0 {
		t.Fatalf("sendOne sent %d txs while at the in-flight ceiling, want 0", n)
	}
	if sent.Load() != before {
		t.Fatalf("engine hit the node %d times while over the ceiling", sent.Load()-before)
	}
	if len(e.ready) != readyBefore {
		t.Fatalf("ready queue churned (%d -> %d) while over the ceiling", readyBefore, len(e.ready))
	}

	// Accepted sends creep the ceiling back up.
	e.inflightLimit.Store(e.inflightFloor)
	low := e.inflightLimit.Load()
	for i := 0; i < 50; i++ {
		e.noteAccepted()
	}
	if got := e.inflightLimit.Load(); got <= low {
		t.Fatalf("limit did not recover on success: %d -> %d", low, got)
	}
	if got := e.inflightLimit.Load(); got > e.inflightMax {
		t.Fatalf("limit %d exceeded max %d", got, e.inflightMax)
	}
}

// TestRetireLatches: an out-of-funds account must stay out even though its
// OTHER in-flight txs resolve later and each one calls offer().
func TestRetireLatches(t *testing.T) {
	accs := testAccounts(t, 1)
	var sent atomic.Int64
	e := okEngine(t, accs, 4, &sent)
	d, _ := testDriver(t)

	e.sendOne(context.Background(), d, KindValue)

	// Retire via the janitor path: finish(requeue=false) on one entry.
	var p *pendingTx
	e.pending.Range(func(_, v any) bool { p = v.(*pendingTx); return false })
	if !e.retire(d, p) {
		t.Fatal("retire returned false")
	}
	// Drain whatever was queued before the retirement.
	for len(e.ready) > 0 {
		<-e.ready
	}
	// The remaining in-flight tx resolving must NOT bring the account back.
	e.pending.Range(func(_, v any) bool { e.resolve(d, v.(*pendingTx)); return true })
	if got := len(e.ready); got != 0 {
		t.Fatalf("retired account came back into rotation (ready=%d)", got)
	}
}

// TestNonceRewindOnlyWithNothingInFlight covers the recovery path that depth
// made necessary. If our assign pointer runs above the endpoint's committed
// nonce while nothing of ours is in flight, the in-flight run was lost and
// every future send from that account would hit the same gap forever - so the
// pointer must rewind. With txs still in flight it must NOT rewind, or it would
// replay nonces that are about to mine.
func TestNonceRewindOnlyWithNothingInFlight(t *testing.T) {
	const committed = 5

	newStaleEngine := func(accs []*accounts.Account) *laneEngine {
		send := func(context.Context, *types.Transaction) error {
			return errors.New("nonce is higher than account nonce")
		}
		probe := func(context.Context, common.Address) (uint64, error) { return committed, nil }
		head := func(context.Context) (uint64, error) { return 0, nil }
		hashes := func(context.Context, uint64) ([]common.Hash, error) { return nil, nil }
		e := newLaneEngine("test", 0, send, probe, head, hashes, accs, 4, 0)
		e.recheckCooldown = time.Millisecond
		return e
	}

	t.Run("rewinds when nothing is in flight", func(t *testing.T) {
		accs := testAccounts(t, 1)
		accs[0].SetBase(0, committed+9) // pointer stranded above committed
		e := newStaleEngine(accs)
		d, _ := testDriver(t)

		e.sendOne(context.Background(), d, KindValue)
		if got := accs[0].Peek(0); got != committed {
			t.Fatalf("next nonce %d after stale send with nothing in flight, want rewind to %d", got, committed)
		}
	})

	t.Run("does not rewind while txs are in flight", func(t *testing.T) {
		accs := testAccounts(t, 1)
		accs[0].SetBase(0, committed)
		var sent atomic.Int64
		ok := okEngine(t, accs, 1, &sent) // depth 1: exactly one tx in flight
		d, _ := testDriver(t)
		ok.sendOne(context.Background(), d, KindValue)
		if ok.inflight(accs[0].Addr) != 1 {
			t.Fatalf("setup: inflight %d, want 1", ok.inflight(accs[0].Addr))
		}

		// Same account, now on an engine whose sends report a nonce gap.
		stale := newStaleEngine(accs)
		stale.slots[accs[0].Addr].inflight.Store(1) // mirror the in-flight tx
		before := accs[0].Peek(0)
		stale.sendOne(context.Background(), d, KindValue)
		if got := accs[0].Peek(0); got != before {
			t.Fatalf("next nonce moved from %d to %d while a tx was in flight; must not rewind", before, got)
		}
	})
}
