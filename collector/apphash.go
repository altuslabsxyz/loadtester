package collector

import (
	"context"
	"strconv"
	"sync"
	"time"
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
}

// AppHashCollector polls each node's app_hash per height and flags divergence
// (direct evidence of BlockSTM/MemIAVL non-determinism) or consensus stall.
type AppHashCollector struct {
	nodes []NodeRPC

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

func NewAppHashCollector(nodes []NodeRPC) *AppHashCollector {
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.Name
	}
	return &AppHashCollector{
		nodes: nodes,
		res: AppHashResult{
			NodeNames:        names,
			CompareAvailable: len(nodes) >= 2,
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
