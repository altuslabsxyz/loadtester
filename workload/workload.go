package workload

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/stablelabs/loadtester/accounts"
	"github.com/stablelabs/loadtester/deployment"
	stabletypes "github.com/stablelabs/stable/x/stable/types"
)

// Kind identifies a workload type.
type Kind string

const (
	KindValue         Kind = "value"         // native value transfer -> LaneNormal
	KindERC20Transfer Kind = "erc20Transfer" // token transfer -> erc20 lane
	KindSwap          Kind = "swap"          // callee swap -> uniswap-swap lane
	KindVIP           Kind = "vip"           // 2D-nonce VIP tx -> vip lane
	KindBump          Kind = "bump"          // same-slot contention -> LaneNormal
	KindSelfDestruct  Kind = "selfdestruct"  // create+selfdestruct -> LaneNormal
	KindUnordered     Kind = "unordered"     // 2D-nonce unordered/timeout tx (STAB-185 path)
)

// unorderedTTL is how far in the future each unordered tx's timeout is set. Kept
// well under the chain's max TTL (~10m). Short enough that, under load, some txs
// expire in the mempool - exercising selective-recheck eviction.
const unorderedTTL = 90 * time.Second

// unorderedSeq makes each unordered timeout unique (the chain dedupes unordered
// txs by (sender, timeout)).
var unorderedSeq atomic.Int64

// nextUnorderedTimeout returns a unique future Unix-nanosecond timeout.
func nextUnorderedTimeout() int64 {
	return time.Now().UnixNano() + unorderedTTL.Nanoseconds() + unorderedSeq.Add(1)
}

// burnRecipient receives value/token transfers; keeps balances flowing without
// needing a second managed account per send.
var burnRecipient = common.HexToAddress("0x000000000000000000000000000000000000dEaD")

// gasFor returns a per-kind gas limit. Generous to avoid out-of-gas on the
// heavier contract calls; the chain meters actual gasUsed which the collector
// reads from receipts.
func gasFor(k Kind) uint64 {
	switch k {
	case KindValue, KindVIP:
		return 21000
	case KindUnordered:
		return 60000 // intrinsic + ante's extra unordered-tx gas cost
	case KindERC20Transfer:
		return 80000
	case KindBump:
		return 80000
	case KindSwap:
		return 400000
	case KindSelfDestruct:
		return 600000
	default:
		return 200000
	}
}

// SentTx records a submitted tx and the lane it is expected to land in. The
// expected lane is the harness's source of truth (esp. for VIP/nonce-key, which
// standard JSON-RPC does not expose), cross-referenced by the lane collector.
type SentTx struct {
	Hash         common.Hash
	From         common.Address
	Kind         Kind
	ExpectedLane int32
	Gas          uint64
	SendTime     time.Time
}

// KindCount aggregates sent txs for one workload kind.
type KindCount struct {
	Kind         string `json:"kind"`
	Count        int    `json:"count"`
	ExpectedLane int32  `json:"expectedLane"`
}

// Sink is a concurrency-safe AGGREGATE recorder of sent txs. It keeps only
// per-kind counts (not every tx) so continuous runs do not grow unbounded.
type Sink struct {
	mu      sync.Mutex
	total   int
	byKind  map[Kind]int
	expLane map[Kind]int32
}

func (s *Sink) Add(t SentTx) {
	s.mu.Lock()
	if s.byKind == nil {
		s.byKind = make(map[Kind]int)
		s.expLane = make(map[Kind]int32)
	}
	s.total++
	s.byKind[t.Kind]++
	s.expLane[t.Kind] = t.ExpectedLane
	s.mu.Unlock()
}

// Total returns the cumulative count of sent txs.
func (s *Sink) Total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

// Stats returns per-kind counts sorted by kind.
func (s *Sink) Stats() []KindCount {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]KindCount, 0, len(s.byKind))
	for k, c := range s.byKind {
		out = append(out, KindCount{Kind: string(k), Count: c, ExpectedLane: s.expLane[k]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind < out[j].Kind })
	return out
}

// Builder builds and signs txs for each workload kind.
type Builder struct {
	pool         *accounts.Pool
	abis         *ABIs
	dep          *deployment.Deployment
	expectedLane map[Kind]int32

	pool0     common.Address // first uniswap pool (for swaps), zero if none
	token0    common.Address // first test token (for erc20 transfer), zero if none
	hasPool   bool
	hasToken  bool
	hasCallee bool
	hasDestr  bool
}

// NewBuilder wires a builder from the pool, ABIs, deployment and expected-lane map.
func NewBuilder(pool *accounts.Pool, abis *ABIs, dep *deployment.Deployment, expectedLane map[Kind]int32) *Builder {
	b := &Builder{pool: pool, abis: abis, dep: dep, expectedLane: expectedLane}
	if len(dep.Tokens) > 0 {
		b.token0 = common.HexToAddress(dep.Tokens[0].Address)
		b.hasToken = true
	}
	if p, ok := dep.FirstPool(); ok {
		b.pool0 = common.HexToAddress(p.Address)
		b.hasPool = true
	}
	b.hasCallee = dep.Callee != ""
	b.hasDestr = dep.Destructible != ""
	return b
}

// Supports reports whether the deployment has what a kind needs.
func (b *Builder) Supports(k Kind) bool {
	switch k {
	case KindValue, KindVIP, KindUnordered:
		return true
	case KindERC20Transfer:
		return b.hasToken
	case KindSwap:
		return b.hasCallee && b.hasPool
	case KindBump, KindSelfDestruct:
		return b.hasDestr
	default:
		return false
	}
}

// AllKinds lists every built-in workload kind.
var AllKinds = []Kind{
	KindValue, KindERC20Transfer, KindSwap, KindVIP, KindBump, KindSelfDestruct, KindUnordered,
}

// SetExpectedLane overrides the workload->lane map (used after deriving it from
// the on-chain params).
func (b *Builder) SetExpectedLane(m map[Kind]int32) { b.expectedLane = m }

// DeriveExpectedLanes computes each supported kind's expected lane by building a
// representative tx and classifying it with the provided function (the chain's
// lane matcher over the on-chain params). VIP is skipped: its lane id must be
// known to build the tx, so it is taken from the plan, not derived.
func (b *Builder) DeriveExpectedLanes(classify func(*types.Transaction) int32, acc *accounts.Account, feeCap, tip *big.Int) {
	for _, k := range AllKinds {
		if k == KindVIP || !b.Supports(k) {
			continue
		}
		tx, err := b.build(k, acc, 0, feeCap, tip)
		if err != nil {
			continue
		}
		b.expectedLane[k] = classify(tx)
	}
}

// NonceKey returns the 2D-nonce key a kind's txs use: VIP txs carry the VIP
// bit + lane id; everything else uses the standard key 0.
func (b *Builder) NonceKey(k Kind) uint64 {
	switch k {
	case KindVIP:
		return stabletypes.VipFlag | uint64(vipLaneID(b.expectedLane))
	case KindUnordered:
		return math.MaxUint64
	default:
		return 0
	}
}

// build constructs+signs the tx for a kind from the given account with an
// explicit nonce (caller owns nonce assignment) and caller-provided fees.
func (b *Builder) build(k Kind, a *accounts.Account, nonce uint64, feeCap, tip *big.Int) (*types.Transaction, error) {
	gas := gasFor(k)
	switch k {
	case KindValue:
		return b.pool.SignStandard(a, nonce, &burnRecipient, big.NewInt(1), nil, gas, feeCap, tip)
	case KindVIP:
		return b.pool.SignVIP(a, nonce, vipLaneID(b.expectedLane), &burnRecipient, big.NewInt(1), nil, gas, feeCap, tip)
	case KindERC20Transfer:
		data, err := b.abis.PackTransfer(burnRecipient, big.NewInt(1))
		if err != nil {
			return nil, err
		}
		return b.pool.SignStandard(a, nonce, &b.token0, nil, data, gas, feeCap, tip)
	case KindSwap:
		// small exact-in swap of token0; recipient is the sender itself.
		// zeroForOne=true requires sqrtPriceLimit > MIN_SQRT_RATIO.
		data, err := b.abis.PackSwapExact0For1(b.pool0, big.NewInt(1000), a.Addr, minSqrtRatioPlusOne())
		if err != nil {
			return nil, err
		}
		callee := b.dep.CalleeAddr()
		return b.pool.SignStandard(a, nonce, &callee, nil, data, gas, feeCap, tip)
	case KindBump:
		data, err := b.abis.PackBump()
		if err != nil {
			return nil, err
		}
		d := b.dep.DestructibleAddr()
		return b.pool.SignStandard(a, nonce, &d, nil, data, gas, feeCap, tip)
	case KindSelfDestruct:
		data, err := b.abis.PackSpawnAndDestroy(big.NewInt(3))
		if err != nil {
			return nil, err
		}
		d := b.dep.DestructibleAddr()
		return b.pool.SignStandard(a, nonce, &d, nil, data, gas, feeCap, tip)
	case KindUnordered:
		// Unordered tx: NonceKey=MaxUint64, Nonce=0, unique future timeout.
		return b.pool.SignUnordered(a, &burnRecipient, big.NewInt(1), nil, gas, feeCap, tip, nextUnorderedTimeout())
	default:
		return nil, fmt.Errorf("unknown workload kind %q", k)
	}
}

// vipLaneID looks up the VIP lane id from the expected-lane map.
func vipLaneID(expected map[Kind]int32) int32 {
	if id, ok := expected[KindVIP]; ok {
		return id
	}
	return 1
}

// Spec is a workload to run at a sustained in-flight level.
type Spec struct {
	Kind     Kind
	Inflight int
}

// ahead is the per-account in-flight window (assigned-minus-confirmed nonces).
//
// It is 1 on the stable chain: the ante admits only nonce == the account's
// committed nonce and there is NO future-nonce queue (a gapped nonce is rejected
// with ErrNonceGap and dropped, not held). So sending nonce N+1 before N commits
// just wastes the send. Real concurrency therefore comes from the NUMBER of
// accounts, not depth per account - each account sustains exactly one in-flight
// tx and advances as the confirmed-nonce poller observes inclusion.
const ahead = 1

// Driver drives load with one goroutine per account, each sustaining `ahead`
// (=1) in-flight txs: send the next nonce, then wait for the confirmed-nonce
// poller to observe inclusion before sending again. Lane oversubscription comes
// from running MANY accounts concurrently, not from depth per account (the chain
// rejects future nonces, so per-account depth >1 is impossible). VIP (2D-nonce,
// key!=0) runs closed-loop (send+wait) on a few reserved accounts; its sequence
// is seeded/resynced from the on-chain noncekey precompile.
type Driver struct {
	pool    *accounts.Pool
	builder *Builder
	sink    *Sink

	// vipClient is the dedicated endpoint VIP (2D-nonce) txs are sent to. VIP txs
	// are only accepted by the role:vip node, so when this is nil the VIP
	// workload is skipped entirely.
	vipClient *ethclient.Client

	feeCap  atomic.Pointer[big.Int]
	tip     atomic.Pointer[big.Int]
	recvTTL time.Duration
	acctRR  atomic.Uint64 // round-robin account selector for unordered senders
}

// NewDriver creates a driver. It seeds the cached fees immediately. vipClient
// may be nil (no role:vip node configured) - VIP txs are then not sent.
func NewDriver(ctx context.Context, pool *accounts.Pool, builder *Builder, sink *Sink, vipClient *ethclient.Client) (*Driver, error) {
	d := &Driver{pool: pool, builder: builder, sink: sink, vipClient: vipClient, recvTTL: 15 * time.Second}
	if err := d.refreshFees(ctx); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Driver) refreshFees(ctx context.Context) error {
	feeCap, tip, err := d.pool.Fees(ctx)
	if err != nil {
		return err
	}
	d.feeCap.Store(feeCap)
	d.tip.Store(tip)
	return nil
}

// Run executes the enabled specs, recording every sent tx. If duration > 0 it
// stops after that long; if duration <= 0 it runs until ctx is cancelled
// (continuous mode).
func (d *Driver) Run(ctx context.Context, duration time.Duration, specs []Spec) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if duration > 0 {
		runCtx, cancel = context.WithDeadline(ctx, time.Now().Add(duration))
		defer cancel()
	}

	// Split specs into VIP, unordered, and standard (key-0) kinds.
	var vipEnabled bool
	unorderedInflight := 0
	weighted := make([]Kind, 0, 32)
	for _, s := range specs {
		if s.Inflight <= 0 || !d.builder.Supports(s.Kind) {
			continue
		}
		if s.Kind == KindVIP {
			// VIP txs require the dedicated role:vip endpoint. Without it, skip.
			if d.vipClient == nil {
				log.Printf("[load] VIP workload requested but no VIP RPC (role:vip node) configured - skipping VIP txs")
				continue
			}
			vipEnabled = true
			continue
		}
		if s.Kind == KindUnordered {
			unorderedInflight = s.Inflight
			continue
		}
		// Repeat each kind proportional to its inflight so higher-target lanes
		// are oversubscribed harder in the per-account mix.
		reps := s.Inflight / 10
		if reps < 1 {
			reps = 1
		}
		for i := 0; i < reps; i++ {
			weighted = append(weighted, s.Kind)
		}
	}

	accs := d.pool.Accs
	// Reserve a few accounts for VIP closed-loop sending.
	nVip := 0
	if vipEnabled {
		nVip = len(accs) / 5
		if nVip < 1 {
			nVip = 1
		}
	}
	stdAccs := accs[nVip:]
	vipAccs := accs[:nVip]

	var wg sync.WaitGroup

	// Fee refresher.
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				_ = d.refreshFees(runCtx)
			}
		}
	}()

	// Confirmed-nonce poller for the standard key so the open-loop window can
	// advance (without this, Inflight never shrinks and senders stall).
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				for _, a := range stdAccs {
					if n, err := d.pool.Client.NonceAt(runCtx, a.Addr, nil); err == nil {
						a.SetConfirmed(0, n)
					}
				}
			}
		}
	}()

	// Standard open-loop senders: one goroutine per account.
	if len(weighted) > 0 {
		for _, a := range stdAccs {
			wg.Add(1)
			go func(a *accounts.Account) {
				defer wg.Done()
				d.stdWorker(runCtx, a, weighted)
			}(a)
		}
	}

	// VIP closed-loop senders: one goroutine per reserved account.
	for _, a := range vipAccs {
		wg.Add(1)
		go func(a *accounts.Account) {
			defer wg.Done()
			d.vipWorker(runCtx, a)
		}(a)
	}

	// Unordered (2D-nonce, Nonce=0) senders: fire-and-forget across all accounts.
	// Each tx is independent (unique timeout, no nonce stream), so no per-account
	// window applies. Worker count is capped to keep send concurrency bounded.
	if unorderedInflight > 0 {
		nWorkers := unorderedInflight
		if nWorkers > 32 {
			nWorkers = 32
		}
		for i := 0; i < nWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d.unorderedWorker(runCtx)
			}()
		}
	}

	wg.Wait()
}

// unorderedWorker fire-and-forgets unordered txs (NonceKey=MaxUint64, Nonce=0)
// from rotating accounts. No nonce commit / window: each tx is independent and
// deduped by its unique timeout, so there is no gap/wedge risk.
func (d *Driver) unorderedWorker(ctx context.Context) {
	accs := d.pool.Accs
	for {
		if ctx.Err() != nil {
			return
		}
		a := accs[int(d.acctRR.Add(1))%len(accs)]
		tx, err := d.builder.build(KindUnordered, a, 0, d.feeCap.Load(), d.tip.Load())
		if err != nil {
			if sleep(ctx, 200*time.Millisecond) {
				return
			}
			continue
		}
		if err := d.pool.Client.SendTransaction(ctx, tx); err != nil {
			if sleep(ctx, 150*time.Millisecond) {
				return
			}
			continue
		}
		d.sink.Add(SentTx{
			Hash: tx.Hash(), From: a.Addr, Kind: KindUnordered,
			ExpectedLane: d.builder.expectedLane[KindUnordered], Gas: tx.Gas(), SendTime: time.Now(),
		})
	}
}

// stdWorker submits standard (key-0) txs open-loop from one account, cycling the
// weighted kind mix, bounded by the `ahead` window.
func (d *Driver) stdWorker(ctx context.Context, a *accounts.Account, weighted []Kind) {
	i := 0
	cappedSince := time.Time{} // when the account first hit the ahead cap
	for {
		if ctx.Err() != nil {
			return
		}
		if a.Inflight(0) >= ahead {
			// Wedge recovery: if we stay capped for a while, the confirmed nonce
			// has stopped advancing (a mid-stream tx was dropped, leaving a gap
			// nothing can fill). Re-seed the local nonce to the chain's actual
			// next nonce, abandoning the gapped backlog so the account resumes.
			if cappedSince.IsZero() {
				cappedSince = time.Now()
			} else if time.Since(cappedSince) > 10*time.Second {
				if n, e := d.pool.Client.NonceAt(ctx, a.Addr, nil); e == nil {
					a.SetBase(0, n)
				}
				cappedSince = time.Time{}
			}
			if sleep(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		cappedSince = time.Time{}
		k := weighted[i%len(weighted)]
		i++
		nonce := a.Peek(0)
		tx, err := d.builder.build(k, a, nonce, d.feeCap.Load(), d.tip.Load())
		if err != nil {
			if sleep(ctx, 200*time.Millisecond) {
				return
			}
			continue
		}
		if err := d.pool.Client.SendTransaction(ctx, tx); err != nil {
			// Do NOT commit: the nonce is reused next iteration (no burn).
			// Re-sync from chain in case our local nonce drifted ahead.
			if n, e := d.pool.Client.NonceAt(ctx, a.Addr, nil); e == nil && n > a.Peek(0) {
				a.SetBase(0, n)
			}
			if sleep(ctx, 150*time.Millisecond) {
				return
			}
			continue
		}
		a.Commit(0)
		d.sink.Add(SentTx{
			Hash: tx.Hash(), From: a.Addr, Kind: k,
			ExpectedLane: d.builder.expectedLane[k], Gas: tx.Gas(), SendTime: time.Now(),
		})
	}
}

// vipWorker submits VIP (2D-nonce) txs closed-loop from one dedicated account.
// Commit happens only AFTER the receipt is observed, so a dropped/timed-out VIP
// tx does not advance the nonce and leave a permanent gap - the same nonce is
// retried. Sequential per account keeps the VIP stream valid without a 2D-nonce
// confirmed query.
func (d *Driver) vipWorker(ctx context.Context, a *accounts.Account) {
	vipKey := d.builder.NonceKey(KindVIP)
	// Seed the VIP (2D-nonce) sequence from the on-chain noncekey precompile.
	// eth_getTransactionCount only reports nonce key 0, so without this the local
	// counter would wrongly start at 0 for a key the account may have already
	// used (prior run / preconfigured), and every VIP tx would be rejected.
	if seq, err := accounts.Nonce2D(ctx, d.vipClient, a.Addr, vipKey); err == nil {
		a.SetBase(vipKey, seq)
	} else {
		log.Printf("[vip] seed nonce via precompile failed for %s (key=%d): %v", a.Addr, vipKey, err)
	}
	for {
		if ctx.Err() != nil {
			return
		}
		nonce := a.Peek(vipKey)
		tx, err := d.builder.build(KindVIP, a, nonce, d.feeCap.Load(), d.tip.Load())
		if err != nil {
			if sleep(ctx, 300*time.Millisecond) {
				return
			}
			continue
		}
		// VIP txs go ONLY to the dedicated role:vip endpoint.
		if err := d.vipClient.SendTransaction(ctx, tx); err != nil {
			// Resync the VIP nonce from the precompile in case the local counter
			// drifted from the on-chain 2D-nonce sequence.
			if seq, e := accounts.Nonce2D(ctx, d.vipClient, a.Addr, vipKey); e == nil {
				a.SetBase(vipKey, seq)
			}
			if sleep(ctx, 200*time.Millisecond) {
				return
			}
			continue
		}
		d.sink.Add(SentTx{
			Hash: tx.Hash(), From: a.Addr, Kind: KindVIP,
			ExpectedLane: d.builder.expectedLane[KindVIP], Gas: tx.Gas(), SendTime: time.Now(),
		})
		if d.waitReceipt(ctx, d.vipClient, tx.Hash()) {
			a.Commit(vipKey) // mined -> advance; else retry the same nonce
		}
	}
}

// waitReceipt polls client until the tx is mined (returns true) or recvTTL
// elapses / ctx ends (returns false).
func (d *Driver) waitReceipt(ctx context.Context, client *ethclient.Client, hash common.Hash) bool {
	deadline := time.Now().Add(d.recvTTL)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return false
		}
		r, err := client.TransactionReceipt(ctx, hash)
		if err == nil && r != nil {
			return true
		}
		if sleep(ctx, 250*time.Millisecond) {
			return false
		}
	}
	return false
}

// sleep waits d or until ctx is done; returns true if ctx ended.
func sleep(ctx context.Context, dur time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(dur):
		return false
	}
}
