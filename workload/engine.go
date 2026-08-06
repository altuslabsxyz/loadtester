package workload

// engine.go is the conflict-free ordered-tx send engine.
//
// Why it exists: on the stable chain there is NO chain-side signal for "does
// this sender have a tx in the mempool" - eth_getTransactionCount("pending")
// equals "latest" because the EVM txpool is vestigial, and the chain admits at
// most ONE ordered tx per (sender, nonce-key) (a future nonce is rejected, not
// queued). Any driver that re-sends a nonce while the previous tx still sits in
// the mempool produces exactly the two errors seen on the devnet RPC logs:
// "tx already in mempool" (identical bytes) and "tx doesn't fit the replacement
// rule, oldPriority ..." (same nonce, fresh fees). At high TPS that duplicate
// spam saturates the node's CheckTx pipeline and collapses real admission.
//
// The engine therefore makes duplicate-nonce sends STRUCTURALLY impossible via
// an ownership invariant: an account is, per nonce-key, in exactly ONE place -
//
//	ready queue    -> eligible: no tx of ours can be in the mempool
//	pending map    -> one tx in flight; the account is NOT sendable
//	held by worker -> a worker is between pop and park/requeue
//	retired        -> dropped from rotation (e.g. out of funds)
//
// Confirmation is fed by ONE cheap RPC per block (eth_getBlockByNumber with
// hashes only) matched against the in-flight hash index - not per-tx receipt
// polling. Each engine runs its OWN feed against the SAME endpoint it sends
// to. That sameness is load-bearing: distinct RPC nodes commit the same block
// seconds apart under load (the 2026-07-23 devnet bench measured the
// enterprise RPC node 10-90s behind the normal one), and admission is checked
// against the receiving node's COMMITTED state. Confirming a tx through a
// faster node's blocks and then sending nonce+1 to a slower node yields a
// guaranteed "tx nonce is higher than account nonce" rejection per tx - the
// 361k-error spam on that bench. A feed tied to the send endpoint makes
// "confirmed" mean "the node I send to has committed past this tx", so the
// follow-up send is admissible by construction and the engine self-paces to
// each endpoint's real commit rate. A janitor probes overdue entries against
// the committed nonce and, only after pendingTTL, re-sends the SAME nonce as a
// deliberate replacement with fees bumped enough to satisfy the chain's
// replacement rule.
import (
	"context"
	"errors"
	"log"
	"math/big"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/stablelabs/loadtester/accounts"
)

// Engine timing/limit defaults. Overridable per laneEngine for tests.
const (
	// defaultConfirmPoll is the block-hash feed cadence. It is a direct
	// throughput term, not just a latency nicety: an account is unusable until
	// the feed sees its tx committed, so with per-account in-flight depth 1 the
	// achievable rate is accountsN/releaseLatency (Little's law), and the tick
	// contributes tick/2 on average. Measured on stable_988-1 with 7001-tx
	// blocks, the hashes-only fetch itself costs ~207ms (473 KB), so a 500ms
	// tick made the feed the largest controllable share of release latency and
	// deepened the refill sawtooth after each full block - and it is the
	// sawtooth TROUGH that sets how many non-stale candidates the proposer
	// finds. 200ms keeps the tick well under the fetch cost while the extra
	// head reads stay negligible (a hashes fetch happens only on a new height).
	defaultConfirmPoll   = 200 * time.Millisecond // block-hash feed poll cadence
	confirmCatchupMax    = 256                    // max blocks consumed per feed tick
	defaultJanitorEvery  = 3 * time.Second        // pending-map sweep cadence
	defaultProbeAfter    = 8 * time.Second        // probe pending entries older than this
	defaultPendingTTL    = 90 * time.Second       // deliberate replacement after this
	defaultMaxBumps      = 4                      // fee-bumped replacements before hard reset
	probeBatch           = 512                    // max probes per janitor sweep (oldest first)
	mempoolFullBackoff   = 2 * time.Second        // per-engine send pause on chain backpressure
	defaultStaleCooldown = 500 * time.Millisecond // requeue delay when the endpoint is behind our view
	replaceBumpNumerator = 13                     // fee bump = old * 13/10 + 1 wei
	replaceBumpDivisor   = 10
)

// sendVerdict classifies a SendTransaction outcome into a recovery action.
type sendVerdict int

const (
	// verdictAccepted: the node admitted the tx into its mempool.
	verdictAccepted sendVerdict = iota
	// verdictAlreadyKnown: a byte-identical tx is already in the pool/cache -
	// an earlier (possibly ambiguous) attempt landed. Treat as accepted.
	verdictAlreadyKnown
	// verdictNonceConflict: a DIFFERENT tx already holds this (sender, nonce)
	// and ours did not out-bid it (replacement rule). Ours is NOT in the pool;
	// the resident one is - park and let the probe resolve it.
	verdictNonceConflict
	// verdictNonceStale: the nonce is behind or ahead of committed state -
	// resync from the committed nonce and retry with a fresh slot.
	verdictNonceStale
	// verdictMempoolFull: chain backpressure. The tx was rejected; do not
	// advance the nonce, pause the send loop briefly.
	verdictMempoolFull
	// verdictBroke: the sender cannot pay for the tx - retire it.
	verdictBroke
	// verdictRejected: a definitive JSON-RPC rejection for any other reason.
	// The tx is not in the pool; the nonce slot stays free.
	verdictRejected
	// verdictAmbiguous: a transport-level failure - the node MAY have admitted
	// the tx. Never blind-resend: park with the hash and let the block feed or
	// the probe decide.
	verdictAmbiguous
)

// classifySendError maps a broadcast error onto a sendVerdict. Substring
// matching is deliberately broad (geth- and cosmos-flavored strings both
// appear on this chain, wrapped arbitrarily). The final split is: a JSON-RPC
// error object is a DEFINITIVE server verdict (the tx was evaluated and
// rejected), anything else (timeouts, resets, HTTP/proxy failures) is
// ambiguous - the request may have been processed with the response lost.
func classifySendError(err error) sendVerdict {
	if err == nil {
		return verdictAccepted
	}
	msg := strings.ToLower(err.Error())
	has := func(subs ...string) bool {
		for _, s := range subs {
			if strings.Contains(msg, s) {
				return true
			}
		}
		return false
	}
	switch {
	case has("already known", "already in mempool", "already exists in cache", "known transaction"):
		return verdictAlreadyKnown
	case has("replacement rule", "replacement transaction", "oldpriority"):
		return verdictNonceConflict
	case has(
		// stable chain ante (EVM/ante/evm/09_increment_sequence.go): plain
		// errors surfaced verbatim through eth_sendRawTransaction's RawLog.
		"nonce is lower than account nonce", "nonce is higher than account nonce",
		// geth/cosmos variants kept for portability across endpoints.
		"nonce too low", "nonce too high", "nonce gap", "invalid nonce",
		"invalid sequence", "sequence mismatch"):
		return verdictNonceStale
	case has("mempool is full", "mempool full", "txpool is full", "too many transactions"):
		return verdictMempoolFull
	case has("insufficient funds", "insufficient balance"):
		return verdictBroke
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// Local cancellation (shutdown / pacer deadline): treat as a plain
		// rejection so the account is requeued rather than parked.
		return verdictRejected
	}
	var rpcErr interface{ ErrorCode() int } // geth rpc JSON-RPC error object
	if errors.As(err, &rpcErr) {
		return verdictRejected
	}
	return verdictAmbiguous
}

// bumpFees returns replacement fees for a stuck tx: at least old*1.3+1wei on
// both tip and feeCap (comfortably over common 10-12.5% replacement rules and
// strictly-greater priority rules), and never below the current suggestion so
// a risen base fee cannot strand the replacement. Guarantees feeCap >= tip.
func bumpFees(oldFeeCap, oldTip, curFeeCap, curTip *big.Int) (*big.Int, *big.Int) {
	bump := func(x *big.Int) *big.Int {
		if x == nil {
			return big.NewInt(1)
		}
		v := new(big.Int).Mul(x, big.NewInt(replaceBumpNumerator))
		v.Div(v, big.NewInt(replaceBumpDivisor))
		return v.Add(v, big.NewInt(1))
	}
	tip := bump(oldTip)
	if curTip != nil && curTip.Cmp(tip) > 0 {
		tip = new(big.Int).Set(curTip)
	}
	feeCap := bump(oldFeeCap)
	if curFeeCap != nil && curFeeCap.Cmp(feeCap) > 0 {
		feeCap = new(big.Int).Set(curFeeCap)
	}
	if feeCap.Cmp(tip) < 0 {
		feeCap = new(big.Int).Set(tip)
	}
	return feeCap, tip
}

// sendFn broadcasts a signed tx; probeFn returns the COMMITTED next nonce for
// (account, the engine's nonce-key): NonceAt for key 0, the 2D-nonce precompile
// for VIP keys. Both are satisfied by thin closures over the real clients and
// by fakes in tests.
type (
	sendFn  func(context.Context, *types.Transaction) error
	probeFn func(context.Context, common.Address) (uint64, error)
)

// pendingTx is one in-flight ordered tx. Mutable fields are guarded by mu and
// only mutated by the janitor (under the account's nonce-key slot); resolved is
// the CAS gate that makes feed/janitor resolution exactly-once.
type pendingTx struct {
	engine *laneEngine
	acc    *accounts.Account
	kind   Kind
	nonce  uint64

	mu         sync.Mutex
	hash       common.Hash // zero when the pool-resident tx is not ours/unknown
	feeCap     *big.Int
	tip        *big.Int
	sentAt     time.Time
	bumps      int
	resetNoted bool // OutcomeHardReset counted once when bumps reach maxBumps

	resolved atomic.Bool
}

func (p *pendingTx) snapshot() (hash common.Hash, sentAt time.Time, bumps int, feeCap, tip *big.Int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hash, p.sentAt, p.bumps, p.feeCap, p.tip
}

// laneEngine drives ordered txs for one nonce-key over one endpoint. Its
// confirmation feed (head/hashes) MUST read the same endpoint send targets:
// admission is checked against the receiving node's committed state, so a
// confirmation signal from any other node is meaningless here (see the
// package comment - that mismatch was the devnet error-spam bug).
type laneEngine struct {
	name   string
	key    uint64
	send   sendFn
	probe  probeFn
	head   headFn
	hashes blockHashesFn

	ready   chan *accounts.Account
	pending sync.Map // common.Address -> *pendingTx
	npend   atomic.Int64
	hashIdx sync.Map // common.Hash -> *pendingTx (this engine's block-feed index)

	backoffUntilNS atomic.Int64 // mempool-full send pause for THIS endpoint (unixnano)

	// tunables (defaults above; narrowed in tests)
	probeAfter    time.Duration
	pendingTTL    time.Duration
	maxBumps      int
	staleCooldown time.Duration

	starved  atomic.Int64 // sends skipped because ready was empty
	overflow atomic.Int64 // requeue overflow (invariant violation signal)
	busyRace atomic.Int64 // benign janitor/worker slot overlaps (see sendOne)
	broke    atomic.Int64 // accounts retired out-of-funds (see noteBroke)
}

// brokeLogEvery makes out-of-funds retirement logging logarithmic. One line per
// account turned a mispriced run into 22k identical lines that buried every
// other signal (observed 2026-08-05), while suppressing it entirely would hide a
// pool quietly draining. The running total is in the report as OutcomeRetired;
// these lines exist only to make the problem visible while it happens.
const brokeLogEvery = 500

// noteBroke records an out-of-funds retirement and logs the FIRST one (with the
// full diagnosis, since that is the actionable moment) and every Nth after.
func (e *laneEngine) noteBroke(addr common.Address) {
	n := e.broke.Add(1)
	switch {
	case n == 1:
		log.Printf("[load] %s account %s retired: OUT OF FUNDS. The chain refuses a tx unless "+
			"balance >= gas*feeCap, and feeCap RISES all run when tipRampWeiPerSec > 0 - so a pool sized for "+
			"early-run prices goes bankrupt late. Every retirement shrinks the pool and with it mempool depth. "+
			"Lower tipRampWeiPerSec/durationSec, or raise fundPerAccount and re-run --fund-only. "+
			"Further retirements log every %d.", e.name, addr, brokeLogEvery)
	case n%brokeLogEvery == 0:
		log.Printf("[load] %s: %d accounts retired out-of-funds so far (pool shrinking - see the first "+
			"out-of-funds line)", e.name, n)
	}
}

func newLaneEngine(name string, key uint64, send sendFn, probe probeFn, head headFn, hashes blockHashesFn, accs []*accounts.Account) *laneEngine {
	e := &laneEngine{
		name: name, key: key, send: send, probe: probe, head: head, hashes: hashes,
		ready:         make(chan *accounts.Account, len(accs)+8),
		probeAfter:    defaultProbeAfter,
		pendingTTL:    defaultPendingTTL,
		maxBumps:      defaultMaxBumps,
		staleCooldown: defaultStaleCooldown,
	}
	for _, a := range accs {
		e.ready <- a
	}
	return e
}

// requeue returns an account to the ready queue. The one-place invariant makes
// overflow impossible; if it ever fires it is a bug, not an operational state.
func (e *laneEngine) requeue(a *accounts.Account) {
	select {
	case e.ready <- a:
	default:
		e.overflow.Add(1)
		log.Printf("[load] BUG: %s ready queue overflow for %s (account dropped from rotation)", e.name, a.Addr)
	}
}

// requeueAfter returns the account to rotation after a cooldown. Used when the
// endpoint's committed state is BEHIND our confirmed view: an immediate requeue
// would re-send the same future nonce into the same rejection, worker-loop
// fast, until the node catches up (the hot spin behind the devnet error spam).
// The account is exclusively ours here, so at most one timer exists per
// account; a timer firing after shutdown just parks the account in a channel
// nobody reads.
func (e *laneEngine) requeueAfter(a *accounts.Account, d time.Duration) {
	if d <= 0 {
		e.requeue(a)
		return
	}
	time.AfterFunc(d, func() { e.requeue(a) })
}

// ensureSeeded lazily initializes the account's nonce for this engine's key
// from committed chain state (needed for reused seeded pools and VIP keys).
func (e *laneEngine) ensureSeeded(ctx context.Context, a *accounts.Account) bool {
	if a.HasBase(e.key) {
		return true
	}
	n, err := e.probe(ctx, a.Addr)
	if err != nil {
		return false
	}
	a.SetBase(e.key, n)
	return true
}

// park records an in-flight tx and indexes its hash for the block feed.
func (e *laneEngine) park(d *Driver, a *accounts.Account, kind Kind, nonce uint64, hash common.Hash, feeCap, tip *big.Int) {
	p := &pendingTx{engine: e, acc: a, kind: kind, nonce: nonce, hash: hash, feeCap: feeCap, tip: tip, sentAt: time.Now()}
	e.pending.Store(a.Addr, p)
	e.npend.Add(1)
	if hash != (common.Hash{}) {
		e.hashIdx.Store(hash, p)
	}
}

// finish completes p exactly once (CAS-gated, safe to race between the feed
// and the janitor): unindexes it, removes it from pending and, unless the
// account is being retired, returns the account to the ready rotation.
func (e *laneEngine) finish(d *Driver, p *pendingTx, requeue bool) bool {
	if !p.resolved.CompareAndSwap(false, true) {
		return false
	}
	hash, _, _, _, _ := p.snapshot()
	if hash != (common.Hash{}) {
		e.hashIdx.Delete(hash)
	}
	e.pending.Delete(p.acc.Addr)
	e.npend.Add(-1)
	if requeue {
		e.requeue(p.acc)
	}
	return true
}

// resolve finishes p and requeues the account.
func (e *laneEngine) resolve(d *Driver, p *pendingTx) bool { return e.finish(d, p, true) }

// retire finishes p WITHOUT requeueing: the account leaves the rotation.
func (e *laneEngine) retire(d *Driver, p *pendingTx) bool { return e.finish(d, p, false) }

// sendOne pops a ready account and sends one tx of the given kind. Returns
// true only when a tx was newly accepted by the node. Accounts with a tx in
// flight are simply not in the ready queue, so a duplicate-nonce send cannot
// be constructed here.
func (e *laneEngine) sendOne(ctx context.Context, d *Driver, kind Kind) bool {
	var a *accounts.Account
	select {
	case a = <-e.ready:
	default:
		e.starved.Add(1)
		return false
	}
	// Possession of the account (popped from ready) is the real exclusivity.
	// TryAcquire can still fail benignly: the janitor may briefly hold an
	// account whose pending entry the feed resolved (and requeued) while it
	// sat in the sweep snapshot. Put it back; the next pop finds it free.
	if !a.TryAcquire(e.key) {
		e.busyRace.Add(1)
		e.requeue(a)
		return false
	}
	defer a.Release(e.key)

	if !e.ensureSeeded(ctx, a) {
		e.requeue(a)
		return false
	}
	nonce := a.Peek(e.key)
	feeCap, tip := d.currentFees()
	tx, err := d.builder.build(kind, a, nonce, feeCap, tip)
	if err != nil {
		e.requeue(a)
		return false
	}

	verdict := classifySendError(e.send(ctx, tx))
	switch verdict {
	case verdictAccepted, verdictAlreadyKnown, verdictAmbiguous:
		// In all three cases a tx with this nonce may be (or is) in the pool:
		// advance the local nonce and park until the feed/probe resolves it.
		a.SetBase(e.key, nonce+1)
		e.park(d, a, kind, nonce, tx.Hash(), feeCap, tip)
		switch verdict {
		case verdictAccepted:
			d.sink.Add(SentTx{Hash: tx.Hash(), From: a.Addr, Kind: kind,
				ExpectedLane: d.builder.expectedLane[kind], Gas: tx.Gas(), SendTime: time.Now()})
			return true
		case verdictAlreadyKnown:
			d.sink.Note(OutcomeDuplicate)
		default:
			d.sink.Note(OutcomeAmbiguous)
		}
	case verdictNonceConflict:
		// A different tx of ours (a lost earlier attempt / prior run) holds the
		// slot. It is real and will mine or expire: park with unknown hash and
		// let the committed-nonce probe resolve it. Never fight it head-on.
		a.SetBase(e.key, nonce+1)
		e.park(d, a, kind, nonce, common.Hash{}, feeCap, tip)
		d.sink.Note(OutcomeConflictParked)
	case verdictNonceStale:
		// Resync ONLY forward (an external tx consumed slots: adopt the higher
		// committed nonce). A probe at or below our next nonce means the
		// endpoint's committed state is BEHIND our confirmed view - never move
		// the assign pointer backwards onto nonces that already mined, and
		// never hot-requeue into the same rejection: cool the account down and
		// let the endpoint catch up.
		if n, perr := e.probe(ctx, a.Addr); perr == nil && n > a.Peek(e.key) {
			a.SetBase(e.key, n)
			e.requeue(a)
			d.sink.Note(OutcomeNonceResync)
		} else {
			e.requeueAfter(a, e.staleCooldown)
			d.sink.Note(OutcomeEndpointBehind)
		}
	case verdictMempoolFull:
		e.noteMempoolFull(d)
		e.requeue(a)
	case verdictBroke:
		d.sink.Note(OutcomeRetired)
		e.noteBroke(a.Addr)
	default: // verdictRejected
		e.requeue(a)
		d.sink.Note(OutcomeRejected)
	}
	return false
}

// janitorSweep probes overdue pending entries (oldest first, bounded batch)
// against the committed nonce: confirmed entries are resolved (feed-blind
// fallback), entries past pendingTTL are re-sent as deliberate fee-bumped
// replacements, and entries that out-live maxBumps are hard reset.
func (e *laneEngine) janitorSweep(ctx context.Context, d *Driver) {
	now := time.Now()
	type due struct {
		p      *pendingTx
		sentAt time.Time
	}
	var overdue []due
	e.pending.Range(func(_, v any) bool {
		p := v.(*pendingTx)
		if p.resolved.Load() {
			return true
		}
		if _, sentAt, _, _, _ := p.snapshot(); now.Sub(sentAt) >= e.probeAfter {
			overdue = append(overdue, due{p: p, sentAt: sentAt})
		}
		return true
	})
	sort.Slice(overdue, func(i, j int) bool { return overdue[i].sentAt.Before(overdue[j].sentAt) })
	if len(overdue) > probeBatch {
		overdue = overdue[:probeBatch]
	}
	for _, o := range overdue {
		if ctx.Err() != nil {
			return
		}
		p := o.p
		// The account is not in ready (it has a pending entry), so no worker
		// holds it; the acquire only fences against a concurrent sweep.
		if !p.acc.TryAcquire(e.key) {
			continue
		}
		e.janitorOne(ctx, d, p)
		p.acc.Release(e.key)
	}
}

func (e *laneEngine) janitorOne(ctx context.Context, d *Driver, p *pendingTx) {
	if p.resolved.Load() {
		return
	}
	committedNext, err := e.probe(ctx, p.acc.Addr)
	if err != nil {
		return
	}
	if committedNext > p.nonce {
		// The slot was consumed (mined tx the feed missed, or an external tx).
		// Never move the assign pointer backwards.
		if committedNext > p.acc.Peek(e.key) {
			p.acc.SetBase(e.key, committedNext)
		}
		if e.resolve(d, p) {
			d.sink.Note(OutcomeProbeConfirmed)
		}
		return
	}

	_, sentAt, bumps, oldFeeCap, oldTip := p.snapshot()
	// Exponential backoff between same-nonce retries: TTL, 2xTTL, 4xTTL, then
	// 8xTTL forever. A slot that refuses to resolve gets quieter, never louder,
	// and no account is ever frozen out of recovery.
	backoff := e.pendingTTL << uint(min(bumps, 3))
	if time.Since(sentAt) < backoff {
		return
	}
	if bumps == e.maxBumps {
		// Escalation budget reached without the slot resolving - either the
		// resident tx is wedged in the node's mempool or the chain stopped
		// including us. Retries continue (backed off), but count it once:
		// non-zero hard-reset in the report = stuck slots, a chain-health
		// signal on a run that should be healthy.
		p.mu.Lock()
		noted := p.resetNoted
		p.resetNoted = true
		p.mu.Unlock()
		if !noted {
			d.sink.Note(OutcomeHardReset)
		}
	}

	// Deliberate replacement: the ONLY same-nonce resend in the engine, and it
	// is ALWAYS priced >=1.3x over the previous attempt - never same-fee, never
	// unbumped - so on an RBF chain it always out-bids our own resident tx. On
	// the stable chain the app mempool disables RBF outright (app/mempool.go:
	// TxReplacement always false), so while the original tx is STILL resident
	// this re-send is cleanly rejected and re-parked; its real job there is
	// EVICTION recovery, where the slot is empty and the re-send is a fresh
	// insert.
	curFeeCap, curTip := d.currentFees()
	feeCap, tip := bumpFees(oldFeeCap, oldTip, curFeeCap, curTip)
	tx, err := d.builder.build(p.kind, p.acc, p.nonce, feeCap, tip)
	if err != nil {
		return
	}
	if p.resolved.Load() {
		// The feed resolved this slot while we were probing/building - the
		// account is already back in rotation. Do not fire a stale-nonce send.
		return
	}
	verdict := classifySendError(e.send(ctx, tx))
	switch verdict {
	case verdictAccepted, verdictAlreadyKnown, verdictAmbiguous:
		p.mu.Lock()
		oldHash := p.hash
		p.hash = tx.Hash()
		p.feeCap, p.tip = feeCap, tip
		p.sentAt = time.Now()
		p.bumps = bumps + 1
		p.mu.Unlock()
		if oldHash != (common.Hash{}) {
			e.hashIdx.Delete(oldHash)
		}
		if !p.resolved.Load() {
			e.hashIdx.Store(tx.Hash(), p)
		}
		d.sink.Note(OutcomeBumped)
	case verdictNonceStale:
		// Consumed while we prepared the bump: resync and resolve.
		if n, perr := e.probe(ctx, p.acc.Addr); perr == nil && n > p.acc.Peek(e.key) {
			p.acc.SetBase(e.key, n)
		}
		if e.resolve(d, p) {
			d.sink.Note(OutcomeProbeConfirmed)
		}
	case verdictBroke:
		if e.retire(d, p) {
			d.sink.Note(OutcomeRetired)
			e.noteBroke(p.acc.Addr)
		}
	case verdictMempoolFull:
		e.noteMempoolFull(d)
		p.mu.Lock()
		p.sentAt = time.Now()
		p.bumps = bumps + 1
		p.mu.Unlock()
	default: // verdictNonceConflict, verdictRejected
		// Still out-bid (or rejected): try again harder next TTL window.
		p.mu.Lock()
		p.sentAt = time.Now()
		p.feeCap, p.tip = feeCap, tip
		p.bumps = bumps + 1
		p.mu.Unlock()
		d.sink.Note(OutcomeConflictParked)
	}
}

// headFn / blockHashesFn feed the confirm loop; thin closures over the real
// client (eth_blockNumber + eth_getBlockByNumber with fullTx=false) or fakes.
type (
	headFn        func(context.Context) (uint64, error)
	blockHashesFn func(context.Context, uint64) ([]common.Hash, error)
)

// confirmLoop is this engine's per-block confirmation feed: ONE hashes-only
// block fetch per new height on the engine's OWN endpoint, matched against its
// in-flight hash index. It starts at the CURRENT head - historic blocks are
// irrelevant. Reading the send endpoint's chain view (never a faster node's)
// is what guarantees a confirmed account's next nonce is admissible there.
func (e *laneEngine) confirmLoop(ctx context.Context, d *Driver, poll time.Duration) {
	// Prime `last` from the FIRST successful head read, whenever that happens:
	// defaulting to 0 after a failed startup read would walk the feed up from
	// block 1 on a long-lived chain - silently disabling hash confirmation and
	// dumping all resolution onto the slower janitor probes.
	last, primed := uint64(0), false
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		h, err := e.head(ctx)
		if err != nil {
			continue
		}
		if !primed {
			primed = true
			if h > 0 {
				last = h - 1 // process the block at startup first
			}
		}
		for n := 0; last < h && n < confirmCatchupMax; n++ {
			txs, herr := e.hashes(ctx, last+1)
			if herr != nil {
				break // transient: retry the same height next tick
			}
			last++
			for _, hash := range txs {
				if v, ok := e.hashIdx.LoadAndDelete(hash); ok {
					p := v.(*pendingTx)
					if p.engine.resolve(d, p) {
						d.sink.Note(OutcomeConfirmed)
					}
				}
			}
		}
	}
}

// noteMempoolFull records backpressure from THIS engine's endpoint and pauses
// its senders briefly. Scoped per engine: the normal node's mempool filling up
// says nothing about the enterprise node's admission capacity (and vice
// versa), so one lane's backpressure must not freeze the other.
func (e *laneEngine) noteMempoolFull(d *Driver) {
	e.backoffUntilNS.Store(time.Now().Add(mempoolFullBackoff).UnixNano())
	d.sink.Note(OutcomeMempoolFull)
}

// backoffWait sleeps (bounded) while this engine's mempool-full backoff is
// active. Returns true when it consumed the caller's send slot.
func (e *laneEngine) backoffWait(ctx context.Context) bool {
	until := e.backoffUntilNS.Load()
	now := time.Now().UnixNano()
	if until <= now {
		return false
	}
	dur := time.Duration(until - now)
	if dur > 100*time.Millisecond {
		dur = 100 * time.Millisecond
	}
	sleep(ctx, dur)
	return true
}
