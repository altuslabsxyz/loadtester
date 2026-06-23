package workload

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/stablelabs/loadtester/accounts"
	"github.com/stablelabs/loadtester/deployment"
)

// Kind identifies a workload type. All kinds are standard EVM txs (nonce key 0).
type Kind string

const (
	KindValue         Kind = "value"         // native value transfer
	KindERC20Transfer Kind = "erc20Transfer" // ERC20 transfer
	KindSwap          Kind = "swap"          // uniswap-v3 swap via callee
	KindBump          Kind = "bump"          // Destructible.bump() (same-slot writes)
	KindSelfDestruct  Kind = "selfdestruct"  // create+selfdestruct in one tx
)

// AllKinds lists every workload kind.
var AllKinds = []Kind{KindValue, KindERC20Transfer, KindSwap, KindBump, KindSelfDestruct}

// burnRecipient receives value/token transfers.
var burnRecipient = common.HexToAddress("0x000000000000000000000000000000000000dEaD")

func gasFor(k Kind) uint64 {
	switch k {
	case KindValue:
		return 21000
	case KindERC20Transfer, KindBump:
		return 80000
	case KindSwap:
		return 400000
	case KindSelfDestruct:
		return 600000
	default:
		return 200000
	}
}

// SentTx records a submitted tx.
type SentTx struct {
	Hash     common.Hash
	From     common.Address
	Kind     Kind
	Gas      uint64
	SendTime time.Time
}

// KindCount aggregates sent txs for one workload kind.
type KindCount struct {
	Kind  string `json:"kind"`
	Count int    `json:"count"`
}

// Sink is a concurrency-safe AGGREGATE recorder (per-kind counts only, so long
// runs don't grow unbounded).
type Sink struct {
	mu     sync.Mutex
	total  int
	byKind map[Kind]int
}

func (s *Sink) Add(t SentTx) {
	s.mu.Lock()
	if s.byKind == nil {
		s.byKind = make(map[Kind]int)
	}
	s.total++
	s.byKind[t.Kind]++
	s.mu.Unlock()
}

func (s *Sink) Total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

func (s *Sink) Stats() []KindCount {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]KindCount, 0, len(s.byKind))
	for k, c := range s.byKind {
		out = append(out, KindCount{Kind: string(k), Count: c})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind < out[j].Kind })
	return out
}

// Builder builds and signs txs for each workload kind.
type Builder struct {
	pool *accounts.Pool
	abis *ABIs
	dep  *deployment.Deployment

	pool0     common.Address
	token0    common.Address
	hasPool   bool
	hasToken  bool
	hasCallee bool
	hasDestr  bool
}

func NewBuilder(pool *accounts.Pool, abis *ABIs, dep *deployment.Deployment) *Builder {
	b := &Builder{pool: pool, abis: abis, dep: dep}
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
	case KindValue:
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

// build constructs+signs the tx for a kind from the given account with an
// explicit nonce and caller-provided fees.
func (b *Builder) build(k Kind, a *accounts.Account, nonce uint64, feeCap, tip *big.Int) (*types.Transaction, error) {
	gas := gasFor(k)
	switch k {
	case KindValue:
		return b.pool.SignStandard(a, nonce, &burnRecipient, big.NewInt(1), nil, gas, feeCap, tip)
	case KindERC20Transfer:
		data, err := b.abis.PackTransfer(burnRecipient, big.NewInt(1))
		if err != nil {
			return nil, err
		}
		return b.pool.SignStandard(a, nonce, &b.token0, nil, data, gas, feeCap, tip)
	case KindSwap:
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
	default:
		return nil, fmt.Errorf("unknown workload kind %q", k)
	}
}

// Spec is a workload to run at a sustained in-flight level.
type Spec struct {
	Kind     Kind
	Inflight int
}

// ahead is the per-account open-loop in-flight window.
const ahead = 64

// Driver drives load OPEN-LOOP: each account runs a goroutine that keeps
// submitting txs without waiting for receipts, bounded by the `ahead` window
// (refreshed from on-chain confirmed nonce). Throughput scales with
// accounts*ahead, not block latency.
type Driver struct {
	pool    *accounts.Pool
	builder *Builder
	sink    *Sink

	feeCap atomic.Pointer[big.Int]
	tip    atomic.Pointer[big.Int]
}

func NewDriver(ctx context.Context, pool *accounts.Pool, builder *Builder, sink *Sink) (*Driver, error) {
	d := &Driver{pool: pool, builder: builder, sink: sink}
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

// Run executes the enabled specs. duration > 0 stops after that long; duration
// <= 0 runs until ctx is cancelled (continuous mode).
func (d *Driver) Run(ctx context.Context, duration time.Duration, specs []Spec) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if duration > 0 {
		runCtx, cancel = context.WithDeadline(ctx, time.Now().Add(duration))
		defer cancel()
	}

	// Weighted kind mix: repeat each kind proportional to its inflight.
	weighted := make([]Kind, 0, 32)
	for _, s := range specs {
		if s.Inflight <= 0 || !d.builder.Supports(s.Kind) {
			continue
		}
		reps := s.Inflight / 10
		if reps < 1 {
			reps = 1
		}
		for range reps {
			weighted = append(weighted, s.Kind)
		}
	}
	if len(weighted) == 0 {
		return
	}

	accs := d.pool.Accs
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

	// Confirmed-nonce poller so the open-loop window can advance.
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
				for _, a := range accs {
					if n, err := d.pool.Client.NonceAt(runCtx, a.Addr, nil); err == nil {
						a.SetConfirmed(0, n)
					}
				}
			}
		}
	}()

	// One open-loop sender goroutine per account.
	for _, a := range accs {
		wg.Add(1)
		go func(a *accounts.Account) {
			defer wg.Done()
			d.worker(runCtx, a, weighted)
		}(a)
	}

	wg.Wait()
}

// worker submits txs open-loop from one account, cycling the weighted kind mix,
// bounded by the `ahead` window. A failed send does not burn a nonce.
func (d *Driver) worker(ctx context.Context, a *accounts.Account, weighted []Kind) {
	i := 0
	cappedSince := time.Time{}
	for {
		if ctx.Err() != nil {
			return
		}
		if a.Inflight(0) >= ahead {
			// Wedge recovery: stay capped too long -> a mid-stream tx was dropped
			// (gap). Re-seed local nonce to chain's next nonce so we resume.
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
			if n, e := d.pool.Client.NonceAt(ctx, a.Addr, nil); e == nil && n > a.Peek(0) {
				a.SetBase(0, n)
			}
			if sleep(ctx, 150*time.Millisecond) {
				return
			}
			continue
		}
		a.Commit(0)
		d.sink.Add(SentTx{Hash: tx.Hash(), From: a.Addr, Kind: k, Gas: tx.Gas(), SendTime: time.Now()})
	}
}

// sleep waits dur or until ctx is done; returns true if ctx ended.
func sleep(ctx context.Context, dur time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(dur):
		return false
	}
}
