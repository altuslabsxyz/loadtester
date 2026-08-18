package workload

// engine.go is the conflict-free ordered-tx send engine.
//
// Why it exists: on the stable chain there is NO chain-side signal for "does
// this sender have a tx in the mempool" - eth_getTransactionCount("pending")
// equals "latest" because the EVM txpool is vestigial. Any driver that re-sends
// the SAME nonce while the previous tx still sits in the mempool produces
// exactly the two errors seen on the devnet RPC logs: "tx already in mempool"
// (identical bytes) and "tx doesn't fit the replacement rule, oldPriority ..."
// (same nonce, fresh fees). At high TPS that duplicate spam saturates the
// node's CheckTx pipeline and collapses real admission.
//
// The engine therefore makes duplicate-nonce sends STRUCTURALLY impossible via
// an ownership invariant: an account is, per nonce-key, in exactly ONE place -
//
//	ready queue    -> eligible: it may send its NEXT unused nonce
//	pending map    -> keyed by (addr, nonce); one entry per tx in flight
//	held by worker -> a worker is between pop and park/requeue
//	retired        -> dropped from rotation (e.g. out of funds)
//
// # Per-account in-flight depth
//
// This file used to assert that the depth was necessarily 1 - that "a future
// nonce is rejected, not queued" - and returned an account to the ready queue
// only once the send endpoint had COMMITTED a block containing its tx. That is
// wrong, and it was the single largest throughput limiter in the driver.
//
// What the chain actually does (stable-evm ante/evm/09_increment_sequence.go):
// CheckTx rejects txNonce > accountNonce, but it reads accountNonce from the
// CheckTx state and WRITES BACK nonce+1 there. That state survives between
// CheckTx calls, so nonce N+1 arriving behind an admitted nonce N sees
// accountNonce == N+1 and is admitted too. Commit resets the CheckTx state to
// committed state, but the app's selective recheck immediately replays the
// mempool residents in nonce order, which walks the per-sender nonce back up.
// Measured on stable_988-1 (scripts/depthprobe.go, 2000 accounts x 16 nonces
// fired back to back): 98.5% of accounts had all 16 admitted AND mined; the
// 1.5% that failed hit the narrow post-Commit / pre-recheck window and are
// retried by the verdictNonceStale path below.
//
// Why depth 1 was so expensive: with depth 1 the ready queue is gated on
// confirmation, so by Little's law the achievable rate is exactly
//
//	rate = accountsN / releaseLatency
//
// and releaseLatency includes the send endpoint's own commit lag. On this
// devnet the send endpoint is the tx-provider full node, which commits 3-4s
// AFTER the validators (measured p50 3s, p90 4s over the 2026-08-14 02:40 run).
// So ~3 of every ~5.4s of release latency was the driver waiting on the
// laggiest node in the network, and 25,000 accounts could only sustain
// 25,000/5.4 = 4,630 tx/s no matter what targetTPS or workers were set to.
// That is precisely the rate that run achieved.
//
// With depth D the in-flight cap becomes accountsN*D and an account re-enters
// the ready queue the moment its send returns, so the send rate is set by the
// pacer and the chain's admission, not by confirmation latency. Confirmation
// still matters - it bounds the backlog and drives the janitor - but it is no
// longer in the critical path of every single tx.
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
	// confirmFetchers is how many block-hash fetches the feed runs at once.
	//
	// One at a time is not enough here. eth_getBlockByNumber is not a cheap read
	// on this chain: stable-evm's backend fetches the CometBFT block AND the full
	// BlockResults - every tx result with its events - before converting, so the
	// call scales with block size. At ~10,000 txs per block it can take longer
	// than the block interval, and a strictly sequential feed then falls further
	// behind with every block.
	//
	// That is not a reporting delay, it is a throughput bug: at depth 1 an
	// account cannot send again until its tx is confirmed, so a lagging feed
	// starves the driver of senders. Measured with the sequential feed: 69,939
	// txs unconfirmed against a mempool of only ~5,000, ready accounts down to
	// 4,969 of 75,000, and the mempool draining to zero - which costs an empty
	// block every time it happens.
	confirmFetchers = 8
	defaultJanitorEvery  = 3 * time.Second        // pending-map sweep cadence
	defaultProbeAfter    = 8 * time.Second        // probe pending entries older than this
	defaultPendingTTL    = 90 * time.Second       // deliberate replacement after this
	defaultMaxBumps      = 4                      // fee-bumped replacements before hard reset
	probeBatch           = 512                    // max probes per janitor sweep (oldest first)
	// mempoolFullBackoff is the per-engine send pause when the endpoint refuses
	// a tx because its mempool is full.
	//
	// This used to be 2s, which is a reasonable pause for an incident and a
	// disastrous one for a saturation test - and a saturation test is what this
	// tool is. Keeping the mempool FULL is the goal, not a fault: the proposer
	// can only put in a block what the provider's mempool holds. On
	// stable_988-1 the node's mempool.size is 30,000 while a pool of 25,000
	// accounts at depth 4 can offer 100,000, so "full" is the normal steady
	// state and the refusal arrives thousands of times a minute. At 2s each,
	// overlapping pauses stalled the senders for most of a 120s run and cut
	// throughput to 2.4k tx/s - well under half of what the same chain did with
	// no backoff pressure at all.
	//
	// A block frees thousands of slots at once, roughly once per second, so the
	// useful retry horizon is one block at most. 25ms retries ~40x per block:
	// enough to refill the instant space appears, short enough that the pause
	// never outlives the condition that caused it.
	mempoolFullBackoff   = 25 * time.Millisecond
	defaultStaleCooldown = 500 * time.Millisecond // requeue delay when the endpoint is behind our view
	replaceBumpNumerator = 13                     // fee bump = old * 13/10 + 1 wei
	replaceBumpDivisor   = 10

	// defaultRecheckCooldown is the requeue delay for the OTHER cause of a
	// "nonce is higher than account nonce" rejection, the one that only exists
	// at depth > 1: our nonce ran ahead of the node's CheckTx view because a
	// Commit just reset it and the selective recheck has not yet replayed this
	// sender's mempool residents. That window is milliseconds, not seconds, so
	// reusing the 500ms endpoint-behind cooldown here would idle an account for
	// ~10 blocks over a hiccup that clears almost immediately. Measured at
	// ~1.5% of sends at depth 16.
	defaultRecheckCooldown = 40 * time.Millisecond

	// readyQueueSlack is headroom in the ready channel over the account count.
	// The queued flag admits at most one entry per account, except in the
	// window between a worker popping an account and re-offering it, where a
	// concurrent resolve may enqueue it again. That window is bounded by the
	// worker count, so the slack covers the maximum configurable workers.
	readyQueueSlack = 4096
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
	pending sync.Map // slotKey{addr,nonce} -> *pendingTx
	npend   atomic.Int64
	hashIdx sync.Map // common.Hash -> *pendingTx (this engine's block-feed index)

	// slots is the per-account rotation state, built once at construction and
	// never mutated as a map afterwards, so it needs no lock of its own.
	slots map[common.Address]*acctSlot

	backoffUntilNS atomic.Int64 // mempool-full send pause for THIS endpoint (unixnano)

	// depth is the max concurrent in-flight ordered txs per account for this
	// nonce-key. 1 restores the old confirmation-gated behavior.
	depth int

	// inflightLimit is the engine-wide ceiling on npend, adapted at runtime by
	// AIMD (see noteMempoolFull / noteAccepted). inflightMax is its upper bound,
	// inflightFloor its lower one.
	inflightLimit atomic.Int64
	inflightMax   int64
	inflightFloor int64

	// depthTarget is the setpoint for the CLOSED-LOOP controller: how many txs
	// this engine tries to keep sitting in the send endpoint's mempool. Zero
	// disables the controller and falls back to the in-flight ceiling above.
	//
	// Controlling on observed mempool depth instead of on unconfirmed-tx
	// bookkeeping is what lets the driver refill a block-drained mempool
	// IMMEDIATELY. npend counts every tx the driver has not yet seen confirmed,
	// and on this chain confirmation lags the validators by seconds - so a tx
	// that a validator already put in a block still counts against an in-flight
	// budget for seconds afterwards, blocking accounts that are perfectly free
	// to send. Measured: with 50,000 accounts and maxInflight 20,000, ~30,000
	// accounts sat idle while the mempool was empty and the chain produced empty
	// blocks. The mempool's actual depth has none of that lag.
	depthTarget int64
	// depthBudget is how many more txs may be sent before the mempool reaches
	// depthTarget. Each observation RESETS it to (target - observed), so poll
	// latency bounds the overshoot and there is no integrator wind-up; each
	// accepted send decrements it.
	depthBudget atomic.Int64
	// depthObserved is the last measured mempool depth, for reporting.
	depthObserved atomic.Int64

	// tunables (defaults above; narrowed in tests)
	probeAfter      time.Duration
	pendingTTL      time.Duration
	maxBumps        int
	staleCooldown   time.Duration
	recheckCooldown time.Duration

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

// slotKey identifies one in-flight ordered tx: an account's nonce for this
// engine's nonce-key. At depth > 1 an account holds several of these at once,
// so the pending map cannot be keyed by address alone.
type slotKey struct {
	addr  common.Address
	nonce uint64
}

// acctSlot is one account's rotation state within an engine.
//
// queued is the anti-duplication gate: it is true exactly while the account
// sits in the ready channel, so concurrent offers (a worker finishing a send
// and the confirm feed resolving an older tx of the same account) enqueue it
// once, not twice. Without it, depth > 1 would multiply an account's presence
// in the queue by its in-flight count.
type acctSlot struct {
	inflight atomic.Int64
	queued   atomic.Bool
	retired  atomic.Bool
}

func newLaneEngine(name string, key uint64, send sendFn, probe probeFn, head headFn, hashes blockHashesFn, accs []*accounts.Account, depth, maxInflight int) *laneEngine {
	if depth < 1 {
		depth = 1
	}
	e := &laneEngine{
		name: name, key: key, send: send, probe: probe, head: head, hashes: hashes,
		ready:           make(chan *accounts.Account, len(accs)+readyQueueSlack),
		slots:           make(map[common.Address]*acctSlot, len(accs)),
		depth:           depth,
		probeAfter:      defaultProbeAfter,
		pendingTTL:      defaultPendingTTL,
		maxBumps:        defaultMaxBumps,
		staleCooldown:   defaultStaleCooldown,
		recheckCooldown: defaultRecheckCooldown,
	}
	for _, a := range accs {
		s := &acctSlot{}
		s.queued.Store(true) // every account starts in the ready channel
		e.slots[a.Addr] = s
		e.ready <- a
	}
	e.inflightMax = int64(len(accs)) * int64(depth)
	if maxInflight > 0 && int64(maxInflight) < e.inflightMax {
		e.inflightMax = int64(maxInflight)
	}
	e.inflightFloor = e.inflightMax / 4
	if e.inflightFloor < 256 {
		e.inflightFloor = 256
	}
	if e.inflightFloor > e.inflightMax {
		e.inflightFloor = e.inflightMax
	}
	e.inflightLimit.Store(e.inflightMax)
	return e
}

// noteAccepted books one admitted tx against both controllers: it spends a
// depth-budget credit and is the additive-increase half of the AIMD ceiling.
func (e *laneEngine) noteAccepted() {
	if e.depthTarget > 0 {
		e.depthBudget.Add(-1)
	}
	if v := e.inflightLimit.Load(); v < e.inflightMax {
		e.inflightLimit.Store(min(v+1, e.inflightMax))
	}
}

// admit reserves room for a new burst under the current in-flight ceiling.
//
// Without this the engine learns the endpoint's mempool capacity the expensive
// way - by being refused. A pool of 25,000 accounts at depth 2 can offer 50,000
// txs while this chain's mempool.size is 30,000, so the excess arrives as a
// steady stream of "mempool is full" rejections: 173,122 of them in one 120s
// run, ~1,400/s of signed txs built, sent, CheckTx'd and thrown away.
//
// That waste is not free, and on this topology it is actively harmful. The
// endpoint is the tx-provider full node, and it has to stay within
// max_height_lag (3) of the validators or it refuses to serve them any txs at
// all. Spending its CheckTx budget on transactions there is no room for is what
// pushes it over that line - the measured result was runs of ~8 completely
// empty blocks while the mempool sat at its 30,000 cap.
//
// So the offered load is congestion-controlled instead: back off on refusal,
// creep back up on success, and never build a tx there is no room for.
func (e *laneEngine) admit() bool {
	// The closed-loop controller, when enabled, is the primary gate.
	if e.depthTarget > 0 && e.depthBudget.Load() <= 0 {
		e.starved.Add(1)
		return false
	}
	if e.npend.Load() >= e.inflightLimit.Load() {
		e.starved.Add(1)
		return false
	}
	return true
}

// observeMempoolDepth records a fresh measurement of the send endpoint's
// mempool and re-derives the send budget from it.
func (e *laneEngine) observeMempoolDepth(n int) { e.observeSupply(n, 0, false) }

// observeSupply re-derives the send budget from a fresh-supply measurement,
// cross-checked against the endpoint's ACTUAL mempool depth when available.
//
// It takes the SMALLER of the two. Depth is a physical upper bound on fresh -
// the mempool holds the fresh transactions plus committed ones the node has
// not pruned yet - so the minimum can never hide a real backlog from the
// controller. What it does do is let a mempool that has genuinely drained
// overrule a fresh counter that has drifted high.
//
// The asymmetry is deliberate. Over-supplying costs a deeper mempool, which
// the next poll corrects; under-supplying costs an EMPTY BLOCK, which is gone
// for good. When the two signals disagree, believe the one that keeps
// transactions in front of the proposer.
func (e *laneEngine) observeSupply(fresh, depth int, haveDepth bool) {
	n := fresh
	if haveDepth && depth < n {
		n = depth
	}
	e.depthObserved.Store(int64(n))
	if e.depthTarget > 0 {
		e.depthBudget.Store(e.depthTarget - int64(n))
	}
}

// slot returns the account's rotation state. Every account the engine was
// built with has one; anything else is a programming error, and a zero slot
// would silently disable the depth cap, so make it loud instead.
func (e *laneEngine) slot(addr common.Address) *acctSlot {
	s, ok := e.slots[addr]
	if !ok {
		log.Printf("[load] BUG: %s has no slot for %s", e.name, addr)
		return &acctSlot{}
	}
	return s
}

// inflight reports how many of the account's txs this engine has in flight.
func (e *laneEngine) inflight(addr common.Address) int64 { return e.slot(addr).inflight.Load() }

// offer returns an account to the ready queue if it is eligible: not retired,
// below the per-account depth cap, and not already queued. It is the ONLY way
// an account re-enters rotation, and it is idempotent - calling it from both a
// finishing send and a resolving confirmation is the normal case, and exactly
// one of them wins the queued CAS.
func (e *laneEngine) offer(a *accounts.Account) {
	s := e.slot(a.Addr)
	if s.retired.Load() || s.inflight.Load() >= int64(e.depth) {
		return
	}
	if !s.queued.CompareAndSwap(false, true) {
		return // already sitting in the ready channel
	}
	select {
	case e.ready <- a:
	default:
		s.queued.Store(false)
		e.overflow.Add(1)
		log.Printf("[load] BUG: %s ready queue overflow for %s (account dropped from rotation)", e.name, a.Addr)
	}
}

// offerAfter returns the account to rotation after a cooldown. Used when the
// endpoint's committed state is BEHIND our confirmed view: an immediate requeue
// would re-send the same future nonce into the same rejection, worker-loop
// fast, until the node catches up (the hot spin behind the devnet error spam).
// A timer firing after shutdown just parks the account in a channel nobody
// reads.
func (e *laneEngine) offerAfter(a *accounts.Account, d time.Duration) {
	if d <= 0 {
		e.offer(a)
		return
	}
	time.AfterFunc(d, func() { e.offer(a) })
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

// park records an in-flight tx and indexes its hash for the block feed. The
// caller has already advanced the account's nonce, so the (addr, nonce) key is
// unique among live entries.
func (e *laneEngine) park(d *Driver, a *accounts.Account, kind Kind, nonce uint64, hash common.Hash, feeCap, tip *big.Int) {
	p := &pendingTx{engine: e, acc: a, kind: kind, nonce: nonce, hash: hash, feeCap: feeCap, tip: tip, sentAt: time.Now()}
	e.pending.Store(slotKey{a.Addr, nonce}, p)
	e.npend.Add(1)
	e.slot(a.Addr).inflight.Add(1)
	if hash != (common.Hash{}) {
		e.hashIdx.Store(hash, p)
	}
}

// finish completes p exactly once (CAS-gated, safe to race between the feed
// and the janitor): unindexes it, removes it from pending, drops the account's
// in-flight count and re-offers the account. At depth > 1 the account may well
// have been sendable already - offer is idempotent, so that is a no-op.
//
// requeue=false RETIRES the account: it is latched off permanently rather than
// merely skipped this once, because its other in-flight txs will resolve later
// and each of those would otherwise offer it straight back into rotation.
func (e *laneEngine) finish(d *Driver, p *pendingTx, requeue bool) bool {
	if !p.resolved.CompareAndSwap(false, true) {
		return false
	}
	hash, _, _, _, _ := p.snapshot()
	if hash != (common.Hash{}) {
		e.hashIdx.Delete(hash)
	}
	e.pending.Delete(slotKey{p.acc.Addr, p.nonce})
	e.npend.Add(-1)
	s := e.slot(p.acc.Addr)
	s.inflight.Add(-1)
	if !requeue {
		s.retired.Store(true)
	}
	e.offer(p.acc)
	return true
}

// resolve finishes p and requeues the account.
func (e *laneEngine) resolve(d *Driver, p *pendingTx) bool { return e.finish(d, p, true) }

// retire finishes p WITHOUT requeueing: the account leaves the rotation.
func (e *laneEngine) retire(d *Driver, p *pendingTx) bool { return e.finish(d, p, false) }

// sendOne pops a ready account and fills its in-flight depth in ONE burst of
// consecutive nonces, returning how many txs the node newly accepted. Accounts
// at the depth cap are simply not in the ready queue, so a duplicate-nonce send
// cannot be constructed here.
//
// # Why the whole run is sent at once
//
// It would be simpler to send one tx and requeue, letting the account come back
// around for nonce+1. That does not work on this chain, and measuring it is what
// produced this design. The app resets its CheckTx state to committed state on
// every Commit, and the mempool's selective recheck only replays the residents
// of accounts the block TOUCHED (stable-bft mempool/clist_mempool.go
// recheckTxsSelective -> GetAffectedTxSet(affectedAccounts)). A sender whose txs
// were all left in the mempool is not touched, so its CheckTx nonce stays at the
// committed value and its next nonce is refused with "nonce is higher than
// account nonce".
//
// Under load that is the common case, not the rare one: with ~25k txs in flight
// draining at ~4.5k/s, a tx takes seconds to mine while an account comes back
// around every accountsN/targetTPS seconds. A 120s run at depth 4 sending one tx
// per rotation was measured at 427k such rejections - roughly one per accepted
// tx - and it was SLOWER than depth 1.
//
// Bursting inverts that. Consecutive nonces issued back to back land inside one
// CheckTx epoch, where each admitted tx advances the CheckTx nonce for the next
// (scripts/depthprobe.go: 2000 accounts x 16 nonces, 98.5% admitted and mined).
// And once an account sits AT its depth cap, the only event that frees a slot is
// a block including one of its txs - which is exactly the event that makes the
// account "affected", triggers its recheck, and leaves its next nonce
// admissible. Filling the depth is therefore self-consistent: the engine only
// ever sends when the chain has just told it that it can.
func (e *laneEngine) sendOne(ctx context.Context, d *Driver, kind Kind) int {
	// Check the in-flight ceiling BEFORE taking an account: over the limit there
	// is nothing useful to do with one, and popping it would only churn the
	// queue. No RPC and no signing happens on this path.
	if !e.admit() {
		return 0
	}
	var a *accounts.Account
	select {
	case a = <-e.ready:
	default:
		e.starved.Add(1)
		return 0
	}
	s := e.slot(a.Addr)
	// Clear queued BEFORE re-reading inflight. A resolve racing us either
	// already failed its CAS (in which case its decrement is visible to the
	// check below) or lands after this store and re-enqueues the account
	// itself. Either way the account cannot be lost from rotation.
	s.queued.Store(false)
	if s.retired.Load() {
		return 0
	}
	if s.inflight.Load() >= int64(e.depth) {
		// At the depth cap: drop it here and let the next resolve re-offer it.
		// offer() re-checks, so this is a no-op if the cap just cleared.
		e.offer(a)
		return 0
	}
	// Possession of the account (popped from ready) is the real exclusivity.
	// TryAcquire can still fail benignly: the janitor may briefly hold an
	// account whose pending entry the feed resolved (and requeued) while it
	// sat in the sweep snapshot. Put it back; the next pop finds it free.
	if !a.TryAcquire(e.key) {
		e.busyRace.Add(1)
		e.offer(a)
		return 0
	}
	// Every exit below returns the account to rotation, and offer() applies the
	// depth cap, so a send that filled the last slot simply does not re-enqueue.
	// The two defers run LIFO: Release first, then offer - re-offering while we
	// still hold the signer slot would let the next worker pop the account and
	// lose its TryAcquire, counting a spurious busyRace on every single send.
	// Branches that schedule their own delayed offer, or retire the account,
	// clear reoffer.
	reoffer := true
	defer func() {
		if reoffer {
			e.offer(a)
		}
	}()
	defer a.Release(e.key)

	if !e.ensureSeeded(ctx, a) {
		return 0
	}

	sent := 0
	// Fill the depth in consecutive nonces. Every iteration past the first is an
	// EXTRA tx beyond the one send credit the caller spent, so the count is
	// returned and the caller repays the difference before its next burst - the
	// rate cap is preserved without ever blocking mid-run, which would spread
	// the nonces across Commits and reintroduce the rejections.
	for sent == 0 || (s.inflight.Load() < int64(e.depth) && e.npend.Load() < e.inflightLimit.Load()) {
		if ctx.Err() != nil {
			break
		}
		n, stop := e.sendNext(ctx, d, a, kind, &reoffer)
		sent += n
		if stop {
			break
		}
	}
	return sent
}

// sendNext builds and sends the account's next nonce, applies the verdict, and
// reports whether a tx was accepted and whether the burst must stop. The caller
// holds the account's signer slot for the whole burst.
//
// stop=true on anything that is not a clean accept: a rejected nonce must not be
// followed by nonce+1 (that would open a gap the chain never closes), and a
// backpressure or funding verdict means this account is done for now.
func (e *laneEngine) sendNext(ctx context.Context, d *Driver, a *accounts.Account, kind Kind, reoffer *bool) (sent int, stop bool) {
	nonce := a.Peek(e.key)
	feeCap, tip := d.currentFees()
	tx, err := d.builder.build(kind, a, nonce, feeCap, tip)
	if err != nil {
		return 0, true
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
			// The ONLY path that continues a burst: the node took this nonce, so
			// its CheckTx state now expects nonce+1 and the next send is
			// admissible by construction.
			e.noteAccepted()
			return 1, false
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
		// Deciding between the two causes below costs one RPC, and at depth > 1
		// this verdict is common, so probing every time is not affordable: it
		// aims thousands of extra requests per second at the very node whose
		// CheckTx pipeline is under test (a 120s depth-4 run issued 427k of
		// them). While we hold txs in flight the answer is already known - our
		// run is real and will mine, the pointer must not move - so skip the
		// probe entirely and just retry the same nonce.
		if e.inflight(a.Addr) > 0 {
			*reoffer = false
			e.offerAfter(a, e.staleRetryDelay())
			d.sink.Note(OutcomeEndpointBehind)
			return 0, true
		}
		n, perr := e.probe(ctx, a.Addr)
		switch {
		case perr != nil:
			*reoffer = false
			e.offerAfter(a, e.staleCooldown)
			d.sink.Note(OutcomeEndpointBehind)
		case n > a.Peek(e.key):
			// An external tx consumed slots: adopt the higher committed nonce.
			a.SetBase(e.key, n)
			d.sink.Note(OutcomeNonceResync)
		case n < a.Peek(e.key) && e.inflight(a.Addr) == 0:
			// Our assign pointer is above the endpoint's committed nonce and we
			// hold NOTHING in flight, so the run of txs that advanced it is gone
			// (evicted, or never admitted) and every future send from this
			// account would be rejected on the same gap forever. This is the one
			// place a backward resync is safe: with no pending entry there is no
			// already-mined nonce to replay. At depth 1 an in-flight tx always
			// existed here, which is why the old code could never rewind.
			a.SetBase(e.key, n)
			d.sink.Note(OutcomeNonceResync)
		default:
			// Our nonce ran ahead of the endpoint's CheckTx view while our own
			// earlier txs are still in flight - the post-Commit recheck window
			// at depth > 1, or a genuinely lagging endpoint at depth 1. Never
			// move the assign pointer backwards onto nonces that already mined,
			// and never hot-requeue into the same rejection.
			*reoffer = false
			e.offerAfter(a, e.staleRetryDelay())
			d.sink.Note(OutcomeEndpointBehind)
		}
	case verdictMempoolFull:
		e.noteMempoolFull(d)
	case verdictBroke:
		// Retire BEFORE the deferred offer runs, so it declines to requeue.
		e.slot(a.Addr).retired.Store(true)
		d.sink.Note(OutcomeRetired)
		e.noteBroke(a.Addr)
	default: // verdictRejected
		d.sink.Note(OutcomeRejected)
	}
	return 0, true
}

// staleRetryDelay is how long to hold an account whose next nonce the endpoint
// called "higher than account nonce" while our earlier txs are still in flight.
// At depth 1 that means the endpoint is genuinely behind our confirmed view and
// needs real time to catch up. At depth > 1 the overwhelmingly common cause is
// the post-Commit / pre-recheck window, which clears in milliseconds.
func (e *laneEngine) staleRetryDelay() time.Duration {
	if e.depth > 1 {
		return e.recheckCooldown
	}
	return e.staleCooldown
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
		for done := 0; last < h && done < confirmCatchupMax; {
			lo := last + 1
			hi := lo + confirmFetchers - 1
			if hi > h {
				hi = h
			}
			if n := int(hi-lo) + 1; done+n > confirmCatchupMax {
				hi = lo + uint64(confirmCatchupMax-done) - 1
			}
			type fetched struct {
				txs []common.Hash
				err error
			}
			out := make([]fetched, hi-lo+1)
			var wg sync.WaitGroup
			for i := range out {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					txs, err := e.hashes(ctx, lo+uint64(i))
					out[i] = fetched{txs, err}
				}(i)
			}
			wg.Wait()
			// Apply in height order and advance `last` only across a CONTIGUOUS
			// run of successes, so a height that failed is retried next tick
			// rather than silently skipped - a skipped height would strand every
			// account whose tx was in it until the janitor's probe found it.
			progressed := false
			for i := range out {
				if out[i].err != nil {
					break
				}
				for _, hash := range out[i].txs {
					if v, ok := e.hashIdx.LoadAndDelete(hash); ok {
						p := v.(*pendingTx)
						if p.engine.resolve(d, p) {
							d.sink.Note(OutcomeConfirmed)
						}
					}
				}
				last = lo + uint64(i)
				done++
				progressed = true
			}
			if !progressed || ctx.Err() != nil {
				break
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
	// Multiplicative decrease, measured from what is ACTUALLY in flight rather
	// than from the current limit: the in-flight count at the moment of refusal
	// is a direct reading of the endpoint's capacity, while the limit may still
	// be far above it and would take many steps to walk down.
	if n := e.npend.Load(); n > 0 {
		want := n - n/16 // ~6% below the level that was just refused
		if want < e.inflightFloor {
			want = e.inflightFloor
		}
		if want < e.inflightLimit.Load() {
			e.inflightLimit.Store(want)
		}
	}
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
