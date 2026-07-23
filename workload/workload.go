package workload

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"math/big"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
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
	KindEnterprise    Kind = "vip"           // legacy YAML key for Enterprise traffic
	KindVIP                = KindEnterprise  // legacy Go API compatibility
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

// legacyHotRecipient preserves the old single-recipient workload when
// recipientPoolSize=1. Capacity tests use per-sender recipients by default.
var legacyHotRecipient = common.HexToAddress("0x000000000000000000000000000000000000dEaD")

var (
	perSenderRecipientDomain = []byte("stable-loadtester/per-sender-recipient/v1")
	poolAssignmentDomain     = []byte("stable-loadtester/recipient-pool-assignment/v1")
	poolRecipientDomain      = []byte("stable-loadtester/recipient-pool-address/v1")
)

// derivedRecipient returns a domain-separated deterministic EOA-like address.
// Setting the high bit keeps generated addresses away from the low precompile
// range. This is load-generator logic only; it never participates in consensus.
func derivedRecipient(domain, input []byte) common.Address {
	digest := crypto.Keccak256(domain, input)
	digest[12] |= 0x80
	return common.BytesToAddress(digest[12:])
}

// recipientFor maps a sender to the configured contention topology:
//   - poolSize == 0: a disjoint deterministic recipient for this sender
//   - poolSize == 1: the legacy shared hot recipient
//   - poolSize > 1:  a deterministic shared pool of poolSize recipients
func recipientFor(sender common.Address, poolSize int) common.Address {
	if poolSize == 1 {
		return legacyHotRecipient
	}
	if poolSize <= 0 {
		return derivedRecipient(perSenderRecipientDomain, sender.Bytes())
	}

	assignment := crypto.Keccak256(poolAssignmentDomain, sender.Bytes())
	bucket := binary.BigEndian.Uint64(assignment[:8]) % uint64(poolSize)
	var bucketBytes [8]byte
	binary.BigEndian.PutUint64(bucketBytes[:], bucket)
	return derivedRecipient(poolRecipientDomain, bucketBytes[:])
}

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

// Outcome labels one send-engine event class. The counts expose, per run, how
// the engine interacted with the chain's admission rules - after the
// duplicate-nonce fix a healthy run shows ~zero already-known / conflict-parked
// events, and any mempool-full count is explicit chain backpressure.
type Outcome string

const (
	OutcomeConfirmed      Outcome = "confirmed-in-block"      // resolved by the block-hash feed
	OutcomeProbeConfirmed Outcome = "confirmed-by-probe"      // resolved by the committed-nonce probe
	OutcomeDuplicate      Outcome = "already-known"           // node already had the identical tx
	OutcomeConflictParked Outcome = "nonce-conflict-parked"   // slot held by an unknown tx of ours; parked
	OutcomeNonceResync    Outcome = "nonce-resynced"          // stale nonce; resynced from committed state
	OutcomeMempoolFull    Outcome = "mempool-full-backoff"    // chain backpressure; senders paused
	OutcomeAmbiguous      Outcome = "ambiguous-parked"        // transport error; parked pending proof
	OutcomeRejected       Outcome = "rejected"                // definitive rejection; slot reused
	OutcomeBumped         Outcome = "fee-bumped-replacement"  // deliberate same-nonce replacement sent
	OutcomeHardReset      Outcome = "hard-reset"              // slot unresolved past the escalation budget (stuck-slot signal)
	OutcomeRetired        Outcome = "account-retired"         // insufficient funds; account dropped
)

// Sink is a concurrency-safe AGGREGATE recorder of sent txs and engine
// outcomes. It keeps only counters (not every tx) so continuous runs do not
// grow unbounded.
type Sink struct {
	mu       sync.Mutex
	total    int
	byKind   map[Kind]int
	expLane  map[Kind]int32
	outcomes map[Outcome]int
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

// Note counts one engine outcome event.
func (s *Sink) Note(o Outcome) {
	s.mu.Lock()
	if s.outcomes == nil {
		s.outcomes = make(map[Outcome]int)
	}
	s.outcomes[o]++
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

// OutcomeCount is one outcome counter for reports.
type OutcomeCount struct {
	Outcome string `json:"outcome"`
	Count   int    `json:"count"`
}

// Outcomes returns the engine outcome counters sorted by outcome name.
func (s *Sink) Outcomes() []OutcomeCount {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]OutcomeCount, 0, len(s.outcomes))
	for o, c := range s.outcomes {
		out = append(out, OutcomeCount{Outcome: string(o), Count: c})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Outcome < out[j].Outcome })
	return out
}

// outcomeSummary renders a compact k=v line for progress logs.
func (s *Sink) outcomeSummary() string {
	counts := s.Outcomes()
	if len(counts) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(counts))
	for _, c := range counts {
		parts = append(parts, fmt.Sprintf("%s=%d", c.Outcome, c.Count))
	}
	return strings.Join(parts, " ")
}

// Builder builds and signs txs for each workload kind.
type Builder struct {
	pool         *accounts.Pool
	abis         *ABIs
	dep          *deployment.Deployment
	expectedLane map[Kind]int32
	// recipientPoolSize controls transfer contention. Zero is the capacity-test
	// default: one deterministic recipient per sender.
	recipientPoolSize int

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

// SetRecipientPoolSize configures transfer-recipient contention. Configuration
// validation rejects negative values before the builder is constructed.
func (b *Builder) SetRecipientPoolSize(n int) { b.recipientPoolSize = n }

// SetEnterpriseLane sets the on-chain lane ID encoded into Enterprise nonce
// keys. It must match the effective chain parameters, not merely the YAML.
func (b *Builder) SetEnterpriseLane(id int32) { b.expectedLane[KindEnterprise] = id }

// SetVIPLane retains the legacy Go API name.
func (b *Builder) SetVIPLane(id int32) { b.SetEnterpriseLane(id) }

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

// NonceKey returns the 2D-nonce key a kind's txs use: the legacy VIP workload
// carries the Enterprise bit plus lane ID; everything else uses key 0.
func (b *Builder) NonceKey(k Kind) uint64 {
	switch k {
	case KindVIP:
		return stabletypes.EnterpriseFlag | uint64(vipLaneID(b.expectedLane))
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
	recipient := recipientFor(a.Addr, b.recipientPoolSize)
	switch k {
	case KindValue:
		return b.pool.SignStandard(a, nonce, &recipient, big.NewInt(1), nil, gas, feeCap, tip)
	case KindVIP:
		return b.pool.SignVIP(a, nonce, vipLaneID(b.expectedLane), &recipient, big.NewInt(1), nil, gas, feeCap, tip)
	case KindERC20Transfer:
		data, err := b.abis.PackTransfer(recipient, big.NewInt(1))
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
		return b.pool.SignUnordered(a, &recipient, big.NewInt(1), nil, gas, feeCap, tip, nextUnorderedTimeout())
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

// Driver schedules a large account pool through a bounded set of workers over
// the conflict-free lane engines (see engine.go). Standard kinds and VIP each
// get an engine with its own ready queue and pending map; unordered txs bypass
// nonce ordering by protocol design and stay fire-and-forget.
type Driver struct {
	pool    *accounts.Pool
	builder *Builder
	sink    *Sink

	// vipClient is the dedicated endpoint VIP (2D-nonce) txs are sent to. VIP txs
	// are only accepted by the role:vip node, so when this is nil the VIP
	// workload is skipped entirely.
	vipClient *ethclient.Client

	feeCap atomic.Pointer[big.Int]
	tip    atomic.Pointer[big.Int]

	std     *laneEngine
	vip     *laneEngine
	hashIdx sync.Map // common.Hash -> *pendingTx (all engines; block-feed index)

	unordRR        atomic.Uint64
	backoffUntilNS atomic.Int64 // mempool-full global send pause (unixnano)
}

// NewDriver creates a driver. It seeds the cached fees immediately. vipClient
// may be nil (no role:vip node configured) - VIP txs are then not sent.
func NewDriver(ctx context.Context, pool *accounts.Pool, builder *Builder, sink *Sink, vipClient *ethclient.Client) (*Driver, error) {
	d := &Driver{pool: pool, builder: builder, sink: sink, vipClient: vipClient}
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

// RunConfigured executes a deterministic weighted workload through at most
// workers sender goroutines. targetTPS is an aggregate scheduling cap; zero
// means the sender pool itself is the cap. Send slots that find every worker
// busy are dropped (no burst backlog); sends that find every account in flight
// are counted as starvation - that is the chain's inclusion rate acting as
// natural backpressure, not an error.
func (d *Driver) RunConfigured(ctx context.Context, duration time.Duration, specs []Spec, workers, targetTPS int) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if duration > 0 {
		runCtx, cancel = context.WithDeadline(ctx, time.Now().Add(duration))
		defer cancel()
	}

	if workers <= 0 || len(d.pool.Accs) == 0 {
		return
	}
	picker := newWeightedPicker(specs, func(kind Kind) bool {
		if kind == KindVIP && d.vipClient == nil {
			return false
		}
		return d.builder.Supports(kind)
	})
	if len(picker.entries) == 0 {
		return
	}
	if targetTPS < 0 || targetTPS > int(time.Second) {
		log.Printf("[load] invalid targetTPS=%d; no load sent", targetTPS)
		return
	}

	vipEnabled, stdEnabled := false, false
	for _, en := range picker.entries {
		switch en.kind {
		case KindVIP:
			vipEnabled = true
		case KindUnordered:
			// unordered runs off the shared pool; no engine needed
		default:
			stdEnabled = true
		}
	}

	// Partition accounts: VIP reserves a fifth of the pool (whole pool when no
	// standard kind runs). Per-sender ordered depth is 1 per nonce-key; keeping
	// the partitions disjoint keeps each lane's admission independent.
	accs := d.pool.Accs
	nVip := 0
	if vipEnabled {
		nVip = len(accs) / 5
		if nVip < 1 {
			nVip = 1
		}
		if !stdEnabled {
			nVip = len(accs)
		}
	}
	stdAccs, vipAccs := accs[nVip:], accs[:nVip]
	if stdEnabled && len(stdAccs) == 0 {
		log.Printf("[load] WARNING: no accounts left for standard kinds after VIP reservation (accountsN too small)")
	}
	d.std = newLaneEngine("std", 0,
		d.pool.Client.SendTransaction,
		func(ctx context.Context, addr common.Address) (uint64, error) {
			return d.pool.Client.NonceAt(ctx, addr, nil)
		},
		stdAccs)
	if vipEnabled {
		vipKey := d.builder.NonceKey(KindVIP)
		vipClient := d.vipClient
		d.vip = newLaneEngine("vip", vipKey,
			vipClient.SendTransaction,
			func(ctx context.Context, addr common.Address) (uint64, error) {
				return accounts.Nonce2D(ctx, vipClient, addr, vipKey)
			},
			vipAccs)
	}

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

	// Confirmation feed: ONE hashes-only block fetch per new height on the
	// primary endpoint (VIP txs land in the same blocks). The janitors'
	// committed-nonce probes cover anything the block view might omit.
	rc := d.pool.Client.Client()
	head := func(ctx context.Context) (uint64, error) { return d.pool.Client.BlockNumber(ctx) }
	hashes := func(ctx context.Context, height uint64) ([]common.Hash, error) {
		var blk struct {
			Transactions []common.Hash `json:"transactions"`
		}
		if err := rc.CallContext(ctx, &blk, "eth_getBlockByNumber", hexutil.EncodeUint64(height), false); err != nil {
			return nil, err
		}
		return blk.Transactions, nil
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.confirmLoop(runCtx, defaultConfirmPoll, head, hashes)
	}()

	// Janitors: probe overdue in-flight txs; escalate to fee-bumped replacement.
	engines := []*laneEngine{d.std}
	if d.vip != nil {
		engines = append(engines, d.vip)
	}
	for _, e := range engines {
		wg.Add(1)
		go func(e *laneEngine) {
			defer wg.Done()
			t := time.NewTicker(defaultJanitorEvery)
			defer t.Stop()
			for {
				select {
				case <-runCtx.Done():
					return
				case <-t.C:
					e.janitorSweep(runCtx, d)
				}
			}
		}(e)
	}

	// Progress logger: the at-a-glance health line for a running load test.
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				line := fmt.Sprintf("[load] progress: accepted=%d std{pending=%d ready=%d starved=%d races=%d}",
					d.sink.Total(), d.std.npend.Load(), len(d.std.ready), d.std.starved.Load(), d.std.busyRace.Load())
				if d.vip != nil {
					line += fmt.Sprintf(" vip{pending=%d ready=%d starved=%d races=%d}",
						d.vip.npend.Load(), len(d.vip.ready), d.vip.starved.Load(), d.vip.busyRace.Load())
				}
				log.Printf("%s outcomes{%s}", line, d.sink.outcomeSummary())
			}
		}
	}()

	// Pacer -> unbuffered token channel: a tick that finds every worker busy is
	// DROPPED, so integer truncation and busy workers can never produce a burst.
	var tokens chan struct{}
	if targetTPS > 0 {
		pacer, err := newIntegerPacer(targetTPS)
		if err != nil {
			log.Printf("[load] %v; no load sent", err)
			return
		}
		tokens = make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pacer.wait(runCtx) {
				select {
				case tokens <- struct{}{}:
				default:
				}
			}
		}()
	}

	var pickMu sync.Mutex
	nextKind := func() (Kind, bool) {
		pickMu.Lock()
		defer pickMu.Unlock()
		return picker.next()
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for runCtx.Err() == nil {
				if tokens != nil {
					select {
					case <-runCtx.Done():
						return
					case <-tokens:
					}
				}
				if d.backoffWait(runCtx) {
					continue // chain said "mempool full"; skip this slot
				}
				kind, ok := nextKind()
				if !ok {
					return
				}
				sent := false
				switch kind {
				case KindUnordered:
					sent = d.unorderedAttempt(runCtx)
				case KindVIP:
					if d.vip != nil {
						sent = d.vip.sendOne(runCtx, d, KindVIP)
					}
				default:
					sent = d.std.sendOne(runCtx, d, kind)
				}
				if tokens == nil && !sent {
					// Uncapped mode: everything in flight - don't hot-spin.
					if sleep(runCtx, 2*time.Millisecond) {
						return
					}
				}
			}
		}()
	}

	wg.Wait()
}

// backoffWait sleeps (bounded) while a mempool-full backoff is active.
// Returns true when it consumed the caller's send slot.
func (d *Driver) backoffWait(ctx context.Context) bool {
	until := d.backoffUntilNS.Load()
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

// unorderedAttempt fire-and-forgets one unordered tx (NonceKey=MaxUint64,
// Nonce=0, unique timeout) from a rotating account. No nonce state is touched:
// unordered txs are deduped by (sender, timeout) and cannot conflict.
func (d *Driver) unorderedAttempt(ctx context.Context) bool {
	accs := d.pool.Accs
	a := accs[int(d.unordRR.Add(1))%len(accs)]
	tx, err := d.builder.build(KindUnordered, a, 0, d.feeCap.Load(), d.tip.Load())
	if err != nil {
		return false
	}
	switch classifySendError(d.pool.Client.SendTransaction(ctx, tx)) {
	case verdictAccepted:
		d.sink.Add(SentTx{
			Hash: tx.Hash(), From: a.Addr, Kind: KindUnordered,
			ExpectedLane: d.builder.expectedLane[KindUnordered], Gas: tx.Gas(), SendTime: time.Now(),
		})
		return true
	case verdictMempoolFull:
		d.noteMempoolFull()
	}
	return false
}

// weightedPicker is deterministic smooth weighted round-robin. Advancing it is
// independent of queue admission, so a saturated kind cannot pin the cursor.
type weightedPicker struct {
	entries []weightedEntry
	total   int64
}

type weightedEntry struct {
	kind    Kind
	weight  int64
	current int64
}

func newWeightedPicker(specs []Spec, supports func(Kind) bool) *weightedPicker {
	entries := make([]weightedEntry, 0, len(specs))
	for _, spec := range specs {
		if spec.Inflight > 0 && supports(spec.Kind) {
			entries = append(entries, weightedEntry{kind: spec.Kind, weight: int64(spec.Inflight)})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].kind < entries[j].kind })
	p := &weightedPicker{entries: entries}
	for i := range entries {
		p.total += entries[i].weight
	}
	return p
}

func (p *weightedPicker) next() (Kind, bool) {
	if len(p.entries) == 0 {
		return "", false
	}
	best := 0
	for i := range p.entries {
		p.entries[i].current += p.entries[i].weight
		if p.entries[i].current > p.entries[best].current {
			best = i
		}
	}
	p.entries[best].current -= p.total
	return p.entries[best].kind, true
}

// integerPacer emits no initial burst. Its kth deadline is
// start+ceil(k*1s/target), so integer truncation can never exceed the cap.
type integerPacer struct {
	target uint64
	baseNS uint64
	remNS  uint64
	carry  uint64
}

func newIntegerPacer(target int) (*integerPacer, error) {
	if target <= 0 || target > int(time.Second) {
		return nil, fmt.Errorf("target TPS must be between 1 and %d", time.Second)
	}
	t := uint64(target)
	ns := uint64(time.Second)
	return &integerPacer{target: t, baseNS: ns / t, remNS: ns % t, carry: t - 1}, nil
}

func (p *integerPacer) nextDelay() time.Duration {
	delay := p.baseNS
	p.carry += p.remNS
	if p.carry >= p.target {
		delay++
		p.carry -= p.target
	}
	return time.Duration(delay)
}

func (p *integerPacer) wait(ctx context.Context) bool {
	t := time.NewTimer(p.nextDelay())
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
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
