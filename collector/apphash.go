package collector

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

// NodeRPC identifies a node for app-hash comparison.
type NodeRPC struct {
	Name     string
	CometRPC string
}

// AppHashMismatch is a height where nodes disagreed on app_hash.
type AppHashMismatch struct {
	Height int64
	Hashes map[string]string // node name -> app_hash
}

// AppHashResult is the app-hash collector output.
type AppHashResult struct {
	NodeNames        []string
	CompareAvailable bool // true when >=2 nodes are configured
	HeightsChecked   int64
	Mismatches       []AppHashMismatch
	StallNotes       []string
	// EthLiveness is true when liveness was tracked via EVM eth_blockNumber
	// because no CometRPC was available (JSON-RPC-only target). In that mode a
	// halt is still flagged, but cross-node app-hash compare is not possible.
	EthLiveness bool
}

// AppHashCollector polls each node's app_hash per height and flags divergence
// (direct evidence of BlockSTM/MemIAVL non-determinism) or consensus stall. When
// no CometRPC node is configured it falls back to EVM eth_blockNumber as a
// liveness/halt floor.
type AppHashCollector struct {
	nodes     []NodeRPC
	ethClient *ethclient.Client // EVM JSON-RPC liveness floor when nodes is empty

	mu             sync.Mutex
	res            AppHashResult
	lastChecked    int64
	lastHeight     int64
	lastProgressAt time.Time
	stallNoted     bool
}

// stallThreshold is how long height may stay flat before we flag a possible
// halt. Must exceed normal block time (local chains run ~5s empty blocks) to
// avoid false positives on slow-but-healthy block production.
const stallThreshold = 30 * time.Second

// Continuous-mode memory bounds: a multi-day run on a chain that genuinely
// diverges/stalls could otherwise accumulate these slices without limit (the
// mempool collector already ring-buffers its samples; app-hash did not).
const (
	maxAppHashMismatches = 1024
	maxStallNotes        = 256
)

// NewAppHashCollector builds a collector over the given CometRPC nodes. ethClient
// may be non-nil to provide an EVM-based liveness floor when no CometRPC node is
// configured (JSON-RPC-only target); it is ignored when CometRPC nodes exist.
func NewAppHashCollector(nodes []NodeRPC, ethClient *ethclient.Client) *AppHashCollector {
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.Name
	}
	ethLiveness := len(nodes) == 0 && ethClient != nil
	if ethLiveness {
		names = []string{"evm-jsonrpc"}
	}
	return &AppHashCollector{
		nodes:     nodes,
		ethClient: ethClient,
		res: AppHashResult{
			NodeNames:        names,
			CompareAvailable: len(nodes) >= 2,
			EthLiveness:      ethLiveness,
		},
	}
}

func (ac *AppHashCollector) Run(ctx context.Context, pollInterval time.Duration) {
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ac.poll(ctx)
		}
	}
}

func (ac *AppHashCollector) poll(ctx context.Context) {
	// JSON-RPC-only: no CometRPC nodes. Use EVM block height as a liveness floor
	// so a consensus halt is still flagged (cross-node app-hash compare needs
	// CometBFT data and is not possible here).
	if len(ac.nodes) == 0 {
		if ac.ethClient == nil {
			return
		}
		h, err := ac.ethClient.BlockNumber(ctx)
		if err != nil || h == 0 {
			return
		}
		ac.detectStall(int64(h))
		return
	}

	// Common height = min latest across all reachable nodes.
	common := int64(-1)
	for _, n := range ac.nodes {
		h, err := StatusHeight(ctx, n.CometRPC)
		if err != nil {
			continue
		}
		if common == -1 || h < common {
			common = h
		}
	}
	if common <= 0 {
		return
	}

	ac.detectStall(common)

	start := ac.lastChecked + 1
	if start < 1 {
		start = 1
	}
	for h := start; h <= common; h++ {
		ac.checkHeight(ctx, h)
		ac.lastChecked = h
	}
}

func (ac *AppHashCollector) detectStall(latest int64) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	now := time.Now()
	if latest != ac.lastHeight {
		ac.lastHeight = latest
		ac.lastProgressAt = now
		ac.stallNoted = false
		return
	}
	if ac.lastProgressAt.IsZero() {
		ac.lastProgressAt = now
		return
	}
	// Flag only once, and only after the height has been flat longer than a
	// normal block interval (avoids false positives on slow block production).
	if !ac.stallNoted && now.Sub(ac.lastProgressAt) > stallThreshold {
		ac.res.StallNotes = append(ac.res.StallNotes,
			"height stalled at "+strconv.FormatInt(latest, 10)+
				" for >"+stallThreshold.String()+" (possible halt)")
		if n := len(ac.res.StallNotes); n > maxStallNotes {
			ac.res.StallNotes = append(ac.res.StallNotes[:0], ac.res.StallNotes[n-maxStallNotes:]...)
		}
		ac.stallNoted = true
	}
}

func (ac *AppHashCollector) checkHeight(ctx context.Context, height int64) {
	hashes := make(map[string]string, len(ac.nodes))
	for _, n := range ac.nodes {
		hash, _, err := BlockAppHash(ctx, n.CometRPC, height)
		if err != nil || hash == "" {
			continue
		}
		hashes[n.Name] = hash
	}
	if len(hashes) == 0 {
		return
	}

	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.res.HeightsChecked++

	// Compare: all observed hashes must be equal.
	var ref string
	mismatch := false
	for _, h := range hashes {
		if ref == "" {
			ref = h
			continue
		}
		if h != ref {
			mismatch = true
			break
		}
	}
	if mismatch {
		ac.res.Mismatches = append(ac.res.Mismatches, AppHashMismatch{Height: height, Hashes: hashes})
		if n := len(ac.res.Mismatches); n > maxAppHashMismatches {
			ac.res.Mismatches = append(ac.res.Mismatches[:0], ac.res.Mismatches[n-maxAppHashMismatches:]...)
		}
	}
}

func (ac *AppHashCollector) Result() AppHashResult {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	cp := ac.res
	cp.NodeNames = append([]string(nil), ac.res.NodeNames...)
	cp.Mismatches = append([]AppHashMismatch(nil), ac.res.Mismatches...)
	cp.StallNotes = append([]string(nil), ac.res.StallNotes...)
	return cp
}
