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
	// CListQueryOK is true if CometBFT num_unconfirmed_txs ever returned
	// successfully. On a JSON-RPC-only testnet there is no CometRPC, so the
	// CList side is never measured - FinalCList==0 then means "unknown", NOT
	// "drained". Without this guard Drained() would report a false PASS.
	CListQueryOK bool
}

// Drained reports whether the observed mempool(s) emptied by the last sample.
// Each side only counts if it was actually measurable; if NEITHER side was
// measured we cannot claim the mempool drained (avoids a false-positive PASS on
// a JSON-RPC-only target where no mempool signal was available).
func (r MempoolResult) Drained() bool {
	if !r.CListQueryOK && !r.EVMQueryOK {
		return false
	}
	if r.CListQueryOK && r.FinalCList != 0 {
		return false
	}
	if r.EVMQueryOK && r.FinalEVM != 0 {
		return false
	}
	return true
}

// StillDraining reports whether the mempool was strictly trending DOWN at the
// end - i.e. a non-zero final depth is a slow tail being worked off, not a
// stuck backlog. Distinguishes "didn't finish in the drain cap" from "wedged".
// Uses the CList trend when it was measured, else the EVM trend (JSON-RPC-only).
func (r MempoolResult) StillDraining() bool {
	n := len(r.Samples)
	if n < 6 {
		return false
	}
	// Compare the last sample to ~5 samples earlier; require a real decrease.
	if r.CListQueryOK {
		return r.Samples[n-1].CListNTxs < r.Samples[n-6].CListNTxs
	}
	if r.EVMQueryOK {
		// Use EVMPending only (exclude queued): queued txs are non-executable
		// (e.g. blocked behind a nonce gap), so a shrinking queued count can look
		// like progress while executable txs are actually wedged. Pending is the
		// tighter "being worked off" signal, closest to the CometBFT CList.
		return r.Samples[n-1].EVMPending < r.Samples[n-6].EVMPending
	}
	return false
}

// MempoolCollector polls CometBFT CList depth and EVM txpool depth over time.
type MempoolCollector struct {
	cometRPC string
	evmRPC   *rpc.Client

	mu  sync.Mutex
	res MempoolResult
}

// NewMempoolCollector dials the EVM JSON-RPC (always required) and records the
// optional CometBFT RPC. cometRPC may be "" on a JSON-RPC-only testnet: the
// collector then observes mempool depth via EVM txpool_status alone.
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
	clist, clistOK := 0, false
	if mc.cometRPC != "" {
		if n, err := NumUnconfirmedTxs(ctx, mc.cometRPC); err == nil {
			clist, clistOK = n, true
		}
	}
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
	if clistOK {
		mc.res.CListQueryOK = true
		mc.res.FinalCList = clist
	}
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

// CurrentEVMDepth returns the live EVM mempool depth (pending+queued) over
// JSON-RPC. ok=false means txpool_status was unavailable. Used by the drain
// loop on JSON-RPC-only targets where CometBFT CList is not reachable.
func (mc *MempoolCollector) CurrentEVMDepth(ctx context.Context) (int, bool) {
	p, q, ok := mc.txpoolStatus(ctx)
	return p + q, ok
}

func (mc *MempoolCollector) Result() MempoolResult {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	cp := mc.res
	cp.Samples = append([]MempoolSample(nil), mc.res.Samples...)
	return cp
}
