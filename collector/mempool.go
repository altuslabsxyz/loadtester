package collector

import (
	"context"
	"sync"
	"time"
)

// MempoolSample is one observation of mempool depth (CometBFT CList).
type MempoolSample struct {
	T         time.Time
	CListNTxs int // CometBFT CList size (num_unconfirmed_txs)
}

// MempoolResult is the mempool collector output.
//
// NOTE: on the stable chain the EVM txpool RPCs (txpool_status / txpool_content)
// are vestigial and always report 0 - the app uses a Cosmos PriorityNonceMempool
// and never wires cosmos-evm's geth txpool, so the RPC backend short-circuits to
// zero. The CometBFT CList depth (num_unconfirmed_txs) is therefore the ONLY
// trustworthy mempool-depth signal, and this collector tracks it alone. Without
// a reachable CometRPC there is no reliable mempool signal at all, so Goal 2 is
// reported NOT EVALUATED rather than a false PASS.
type MempoolResult struct {
	Samples    []MempoolSample
	PeakCList  int
	FinalCList int // last observed CList depth (post drain window)
	// CListQueryOK is true once CometBFT num_unconfirmed_txs returned
	// successfully. When false the depth was never measured - FinalCList==0 then
	// means "unknown", NOT "drained".
	CListQueryOK bool
}

// Drained reports whether the CList emptied by the last sample. Returns false
// when CList was never measured (no CometRPC) so it cannot emit a false PASS.
func (r MempoolResult) Drained() bool {
	return r.CListQueryOK && r.FinalCList == 0
}

// StillDraining reports whether the CList was strictly trending DOWN at the end
// - a non-zero final depth being worked off, not a wedged backlog.
func (r MempoolResult) StillDraining() bool {
	n := len(r.Samples)
	if n < 6 || !r.CListQueryOK {
		return false
	}
	return r.Samples[n-1].CListNTxs < r.Samples[n-6].CListNTxs
}

// MempoolCollector polls CometBFT CList depth over time.
type MempoolCollector struct {
	cometRPC string

	mu  sync.Mutex
	res MempoolResult
}

// NewMempoolCollector records the CometBFT RPC to poll. cometRPC may be "" (no
// CometRPC reachable); the collector then records nothing and Goal 2 is NOT
// EVALUATED. There is no EVM-txpool fallback because txpool_status is unreliable
// (always 0) on the stable chain.
func NewMempoolCollector(cometRPC string) *MempoolCollector {
	return &MempoolCollector{cometRPC: cometRPC}
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
	if mc.cometRPC == "" {
		return
	}
	n, err := NumUnconfirmedTxs(ctx, mc.cometRPC)
	if err != nil {
		return
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.res.Samples = append(mc.res.Samples, MempoolSample{T: time.Now(), CListNTxs: n})
	if n > mc.res.PeakCList {
		mc.res.PeakCList = n
	}
	mc.res.CListQueryOK = true
	mc.res.FinalCList = n
	// Bound memory for continuous runs: keep only the most recent samples.
	if k := len(mc.res.Samples); k > maxMempoolSamples {
		mc.res.Samples = append(mc.res.Samples[:0], mc.res.Samples[k-maxMempoolSamples:]...)
	}
}

// maxMempoolSamples caps the retained sample ring (continuous-mode safety).
const maxMempoolSamples = 4096

func (mc *MempoolCollector) Result() MempoolResult {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	cp := mc.res
	cp.Samples = append([]MempoolSample(nil), mc.res.Samples...)
	return cp
}
