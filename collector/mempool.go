package collector

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
)

// MempoolSample is one observation of mempool depth.
type MempoolSample struct {
	T          time.Time
	CListNTxs  int // CometBFT CList size (num_unconfirmed_txs)
	EVMPending int // app PriorityNonceMempool pending (txpool_status)
	EVMQueued  int // app PriorityNonceMempool queued
}

// MempoolResult is the mempool collector output.
type MempoolResult struct {
	Samples   []MempoolSample
	PeakCList int
	PeakEVM   int
	// FinalCList/FinalEVM are the last observed depths (post drain window).
	FinalCList int
	FinalEVM   int
	// EVMQueryOK is true if txpool_status ever returned successfully. When
	// false, the EVM-side depth was never actually measured (don't treat
	// FinalEVM==0 as "drained").
	EVMQueryOK bool
}

// Drained reports whether the observed mempool(s) emptied by the last sample.
// The EVM side only counts if it was actually measurable.
func (r MempoolResult) Drained() bool {
	if r.FinalCList != 0 {
		return false
	}
	if r.EVMQueryOK && r.FinalEVM != 0 {
		return false
	}
	return true
}

// StillDraining reports whether CList was strictly trending DOWN at the end -
// i.e. a non-zero final depth is a slow tail being worked off, not a stuck
// backlog. Distinguishes "didn't finish in the drain cap" from "wedged".
func (r MempoolResult) StillDraining() bool {
	n := len(r.Samples)
	if n < 6 {
		return false
	}
	// Compare the last sample to ~5 samples earlier; require a real decrease.
	return r.Samples[n-1].CListNTxs < r.Samples[n-6].CListNTxs
}

// MempoolCollector polls CometBFT CList depth and EVM txpool depth over time.
type MempoolCollector struct {
	cometRPC string
	evmRPC   *rpc.Client

	mu  sync.Mutex
	res MempoolResult
}

func NewMempoolCollector(ctx context.Context, cometRPC, jsonRPC string) (*MempoolCollector, error) {
	c, err := rpc.DialContext(ctx, jsonRPC)
	if err != nil {
		return nil, err
	}
	return &MempoolCollector{cometRPC: cometRPC, evmRPC: c}, nil
}

func (mc *MempoolCollector) Run(ctx context.Context, pollInterval time.Duration) {
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			mc.sample(ctx)
		}
	}
}

func (mc *MempoolCollector) sample(ctx context.Context) {
	clist, _ := NumUnconfirmedTxs(ctx, mc.cometRPC)
	pending, queued, ok := mc.txpoolStatus(ctx)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	s := MempoolSample{T: time.Now(), CListNTxs: clist, EVMPending: pending, EVMQueued: queued}
	mc.res.Samples = append(mc.res.Samples, s)
	if clist > mc.res.PeakCList {
		mc.res.PeakCList = clist
	}
	if pending+queued > mc.res.PeakEVM {
		mc.res.PeakEVM = pending + queued
	}
	mc.res.FinalCList = clist
	if ok {
		mc.res.EVMQueryOK = true
		mc.res.FinalEVM = pending + queued
	}
	// Bound memory for continuous runs: keep only the most recent samples.
	if n := len(mc.res.Samples); n > maxMempoolSamples {
		mc.res.Samples = append(mc.res.Samples[:0], mc.res.Samples[n-maxMempoolSamples:]...)
	}
}

// maxMempoolSamples caps the retained sample ring (continuous-mode safety).
const maxMempoolSamples = 4096

// txpoolStatus calls eth JSON-RPC txpool_status; returns (pending, queued, ok).
// ok=false means the query failed/unsupported - the caller must NOT treat that
// as zero depth (would falsely report the EVM mempool as drained).
func (mc *MempoolCollector) txpoolStatus(ctx context.Context) (int, int, bool) {
	var out struct {
		Pending string `json:"pending"`
		Queued  string `json:"queued"`
	}
	if err := mc.evmRPC.CallContext(ctx, &out, "txpool_status"); err != nil {
		return 0, 0, false
	}
	return parseHexInt(out.Pending), parseHexInt(out.Queued), true
}

func parseHexInt(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	s = strings.TrimPrefix(s, "0x")
	n, err := strconv.ParseInt(s, 16, 64)
	if err != nil {
		// maybe decimal
		if d, e := strconv.ParseInt(s, 10, 64); e == nil {
			return int(d)
		}
		return 0
	}
	return int(n)
}

func (mc *MempoolCollector) Result() MempoolResult {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	cp := mc.res
	cp.Samples = append([]MempoolSample(nil), mc.res.Samples...)
	return cp
}
