// Package report renders the collectors' results into a markdown + JSON report.
package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/stablelabs/loadtester/collector"
	"github.com/stablelabs/loadtester/workload"
)

// Input bundles everything the report renders.
type Input struct {
	TargetName string
	ChainID    uint64
	Continuous bool // continuous-mode snapshot (load still running, no drain)

	Mempool   collector.MempoolResult
	AppHash   collector.AppHashResult
	SentTotal int
	Sent      []workload.KindCount
}

// Markdown renders the human-readable report.
func Markdown(in Input) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Load-Tester Report\n\n")
	fmt.Fprintf(&b, "- Target: `%s` (chainId %d)\n", in.TargetName, in.ChainID)
	fmt.Fprintf(&b, "- Sent txs: %d\n\n", in.SentTotal)

	// Mempool drain.
	fmt.Fprintf(&b, "## Mempool drain (selective recheck / no stuck txs)\n\n")
	fmt.Fprintf(&b, "Peak CList: %d, Peak EVM(pending+queued): %d\n\n", in.Mempool.PeakCList, in.Mempool.PeakEVM)
	evmStr := fmt.Sprintf("%d", in.Mempool.FinalEVM)
	if !in.Mempool.EVMQueryOK {
		evmStr = "n/a (txpool_status unavailable)"
	}
	fmt.Fprintf(&b, "Final CList: %d, Final EVM: %s\n\n", in.Mempool.FinalCList, evmStr)
	switch {
	case in.Continuous:
		fmt.Fprintf(&b, "**LIVE (continuous)**: CList=%d (peak %d). Drain pass/fail is not evaluated while load runs - "+
			"a non-empty mempool under sustained load is expected.\n\n", in.Mempool.FinalCList, in.Mempool.PeakCList)
	case in.Mempool.Drained():
		fmt.Fprintf(&b, "**PASS**: the mempool drained to 0 - no txs left stuck.\n\n")
	case in.Mempool.StillDraining():
		fmt.Fprintf(&b, "**INCOMPLETE (draining, not stuck)**: CList=%d at the drain cap but still trending DOWN "+
			"(peak %d). Raise observe.drainWindowSec or lower inflight to confirm it reaches 0.\n\n",
			in.Mempool.FinalCList, in.Mempool.PeakCList)
	default:
		fmt.Fprintf(&b, "**FAIL**: mempool flat at CList=%d, EVM=%s and NOT draining - likely stuck txs.\n\n",
			in.Mempool.FinalCList, evmStr)
	}

	// Node consistency / non-determinism.
	fmt.Fprintf(&b, "## Node consistency (app-hash / determinism)\n\n")
	fmt.Fprintf(&b, "Nodes: %s\n\n", strings.Join(in.AppHash.NodeNames, ", "))
	fmt.Fprintf(&b, "Heights checked: %d\n\n", in.AppHash.HeightsChecked)
	fmt.Fprintf(&b, "_Note: under CometBFT a committed block's app_hash is agreed by consensus, so cross-node "+
		"agreement is expected and is a liveness/consistency check, not a determinism proof. Non-determinism "+
		"manifests as a consensus HALT or an app-hash-mismatch (a divergent node cannot commit)._\n\n")
	switch {
	case len(in.AppHash.Mismatches) > 0:
		fmt.Fprintf(&b, "**FAIL**: %d height(s) with committed app_hash divergence across nodes:\n\n", len(in.AppHash.Mismatches))
		for _, m := range in.AppHash.Mismatches {
			fmt.Fprintf(&b, "- height %d:\n", m.Height)
			for node, h := range m.Hashes {
				fmt.Fprintf(&b, "    - %s: %s\n", node, h)
			}
		}
		fmt.Fprintf(&b, "\n")
	case len(in.AppHash.StallNotes) > 0:
		fmt.Fprintf(&b, "**REVIEW**: height stall(s) observed (confirm benign vs halt):\n")
		for _, n := range in.AppHash.StallNotes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
		fmt.Fprintf(&b, "\n")
	case !in.AppHash.CompareAvailable:
		fmt.Fprintf(&b, "**NO HALT (weak)**: single node; no cross-node check.\n\n")
	default:
		fmt.Fprintf(&b, "**NO HALT OBSERVED**: %d nodes stayed consistent across %d heights. Absence-of-evidence, not a determinism proof.\n\n",
			len(in.AppHash.NodeNames), in.AppHash.HeightsChecked)
	}

	// Workload summary.
	fmt.Fprintf(&b, "## Workload summary\n\n")
	fmt.Fprintf(&b, "| kind | count |\n|---|---|\n")
	for _, s := range in.Sent {
		fmt.Fprintf(&b, "| %s | %d |\n", s.Kind, s.Count)
	}
	fmt.Fprintf(&b, "\n")
	return b.String()
}

type jsonReport struct {
	TargetName string                  `json:"targetName"`
	ChainID    uint64                  `json:"chainId"`
	Mempool    collector.MempoolResult `json:"mempool"`
	AppHash    collector.AppHashResult `json:"appHash"`
	SentTotal  int                     `json:"sentTotal"`
	Sent       []workload.KindCount    `json:"sent"`
}

// Write writes report.md and report.json into dir, returning the md path.
func Write(dir string, in Input) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	mdPath := filepath.Join(dir, "report.md")
	if err := os.WriteFile(mdPath, []byte(Markdown(in)), 0o644); err != nil {
		return "", err
	}
	raw, err := json.MarshalIndent(jsonReport{
		TargetName: in.TargetName, ChainID: in.ChainID,
		Mempool: in.Mempool, AppHash: in.AppHash,
		SentTotal: in.SentTotal, Sent: in.Sent,
	}, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), raw, 0o644); err != nil {
		return "", err
	}
	return mdPath, nil
}
