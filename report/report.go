// Package report renders the three collectors' results into a markdown + JSON
// QA report keyed to the three verification goals.
package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/stablelabs/loadtester/collector"
	"github.com/stablelabs/loadtester/workload"
)

// Input bundles everything the report renders.
type Input struct {
	TargetName  string
	ChainID     uint64
	GovMode     string
	MaxBlockGas uint64 // chain consensus block.max_gas (0 = unknown/unlimited)
	Continuous  bool   // true = continuous-mode snapshot (load still running, no drain phase)

	Lane      collector.LaneResult
	Mempool   collector.MempoolResult
	AppHash   collector.AppHashResult
	LogScan   collector.LogScanResult
	SentTotal int
	Sent      []workload.KindCount
}

// Markdown renders the human-readable report.
func Markdown(in Input) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Load-Tester QA Report\n\n")
	fmt.Fprintf(&b, "- Target: `%s` (chainId %d)\n", in.TargetName, in.ChainID)
	fmt.Fprintf(&b, "- Governance mode: `%s`\n", in.GovMode)
	fmt.Fprintf(&b, "- Sent txs: %d\n\n", in.SentTotal)

	// Goal 1: Lane quota enforcement.
	fmt.Fprintf(&b, "## Goal 1 - Guaranteed Blockspace (lane quota)\n\n")
	fmt.Fprintf(&b, "Blocks observed: %d\n\n", in.Lane.BlocksObserved)

	// Guard: if block.max_gas is unknown/unlimited, every per-lane quota is 0 and
	// the violation check is meaningless. Do not render a false pass.
	if in.MaxBlockGas == 0 {
		fmt.Fprintf(&b, "**NOT EVALUATED**: chain block.max_gas is unknown or unlimited (read as 0), "+
			"so per-lane quotas are undefined. Set a finite block.max_gas to evaluate Goal 1.\n\n")
	}

	// Ground truth from proposer logs. Distinguish the healthy enforcement path
	// (PrepareProposal skip) from the problem path (ProcessProposal reject).
	if in.LogScan.Available {
		if in.LogScan.LaneQuotaSkips > 0 {
			fmt.Fprintf(&b, "**ENFORCED (ground truth)**: %d \"skip tx: lane gas quota exceeded\" log(s) - "+
				"the proposer actively capped a lane and skipped/overflowed excess txs. Direct proof of enforcement.\n\n",
				in.LogScan.LaneQuotaSkips)
			for _, s := range in.LogScan.LaneQuotaSamples {
				fmt.Fprintf(&b, "    %s\n", s)
			}
			fmt.Fprintf(&b, "\n")
		} else {
			fmt.Fprintf(&b, "**INCONCLUSIVE (ground truth)**: no proposer skip-log seen - "+
				"no lane was oversubscribed past its quota, or load was too light. Increase load / lower block gas.\n\n")
		}
		if in.LogScan.LaneQuotaRejects > 0 {
			fmt.Fprintf(&b, "**WARNING**: %d ProcessProposal \"lane gas quota exceeded\" REJECT(s) - "+
				"a proposer built a quota-violating block that validators rejected. Investigate (proposer/validator disagreement).\n\n",
				in.LogScan.LaneQuotaRejects)
		}
		if in.LogScan.Truncated {
			fmt.Fprintf(&b, "_Note: a log line exceeded the scan buffer; log scan may be incomplete._\n\n")
		}
	} else {
		fmt.Fprintf(&b, "**UNPROVABLE (no logs)**: no node logs configured (logPaths) - on this target Goal 1 "+
			"cannot be proven; the RPC attribution below is only an upper bound, not enforcement evidence.\n\n")
	}

	fmt.Fprintf(&b, "RPC attribution (per-lane sum of tx gas LIMIT by primary lane) vs quota. "+
		"NOTE: this is an UPPER BOUND - overflow moves excess txs to lower lanes, so a lane reading above quota here "+
		"does NOT by itself prove a violation; cross-check the ground-truth log above.\n\n")
	if len(in.Lane.Violations) > 0 {
		fmt.Fprintf(&b, "Blocks where attributed lane gas exceeded quota:\n\n")
		fmt.Fprintf(&b, "| height | lane | gasLimit sum | quota |\n|---|---|---|---|\n")
		for _, v := range in.Lane.Violations {
			fmt.Fprintf(&b, "| %d | %s (%d) | %d | %d |\n", v.Height, v.LaneName, v.LaneID, v.GasUsed, v.Quota)
		}
		fmt.Fprintf(&b, "\n")
	}
	fmt.Fprintf(&b, "Peak attributed gas per lane vs quota:\n\n")
	fmt.Fprintf(&b, "| lane | peak gasLimit sum | quota |\n|---|---|---|\n")
	for _, id := range sortedLaneIDs(in.Lane.PeakLaneGas) {
		fmt.Fprintf(&b, "| %s (%d) | %d | %d |\n", in.Lane.LaneNames[id], id, in.Lane.PeakLaneGas[id], in.Lane.Quota[id])
	}
	fmt.Fprintf(&b, "\n")

	// Goal 2: Mempool drain / selective recheck.
	fmt.Fprintf(&b, "## Goal 2 - Selective-Recheck (mempool drain)\n\n")
	fmt.Fprintf(&b, "Peak CList: %d, Peak EVM(pending+queued): %d\n\n", in.Mempool.PeakCList, in.Mempool.PeakEVM)
	evmStr := fmt.Sprintf("%d", in.Mempool.FinalEVM)
	if !in.Mempool.EVMQueryOK {
		evmStr = "n/a (txpool_status unavailable)"
	}
	fmt.Fprintf(&b, "Final CList: %d, Final EVM: %s\n\n", in.Mempool.FinalCList, evmStr)
	switch {
	case in.Continuous:
		// Load is still running (or just stopped) with no drain phase, so a full
		// mempool is expected. Report live depth as informational, not pass/fail.
		fmt.Fprintf(&b, "**LIVE (continuous)**: CList=%d (peak %d). Drain PASS/FAIL is not evaluated while load runs - "+
			"a non-empty mempool under sustained load is expected. To verify drain, run a one-shot (positive durationSec) "+
			"or stop the load and watch CList return to 0.\n\n", in.Mempool.FinalCList, in.Mempool.PeakCList)
	case in.Mempool.Drained():
		fmt.Fprintf(&b, "**PASS (liveness)**: the mempool drained to 0 - no txs left stuck.\n\n")
	case in.Mempool.StillDraining():
		fmt.Fprintf(&b, "**INCOMPLETE (draining, not stuck)**: CList=%d at the drain cap but still trending DOWN "+
			"(peak was %d). Txs are being worked off, not wedged - the open-loop flood outran the drain cap. "+
			"Raise observe.drainWindowSec or lower workload inflight to confirm it reaches 0.\n\n",
			in.Mempool.FinalCList, in.Mempool.PeakCList)
	default:
		fmt.Fprintf(&b, "**FAIL**: mempool flat at CList=%d, EVM=%s and NOT draining - likely stuck/pending txs.\n\n",
			in.Mempool.FinalCList, evmStr)
	}
	fmt.Fprintf(&b, "_Scope caveat: this workload sends ORDERED txs, so a clean drain shows ordinary recheck works. "+
		"It does NOT isolate selective vs full recheck, and does NOT reproduce the STAB-185 unordered/timeout-tx gap "+
		"(no unordered txs are sent). To target STAB-185, send unordered txs with short timeouts and confirm they are evicted._\n\n")

	// Goal 3: Non-determinism.
	fmt.Fprintf(&b, "## Goal 3 - Non-determinism (app-hash / BlockSTM / MemIAVL)\n\n")
	fmt.Fprintf(&b, "Nodes: %s\n\n", strings.Join(in.AppHash.NodeNames, ", "))
	fmt.Fprintf(&b, "Heights checked: %d\n\n", in.AppHash.HeightsChecked)
	fmt.Fprintf(&b, "_Method note: under CometBFT, a COMMITTED block's app_hash is agreed by consensus - all nodes that "+
		"committed height H necessarily share it. So cross-node agreement on committed hashes is EXPECTED and is a "+
		"liveness/consistency check, not by itself proof of determinism. Genuine non-determinism manifests as a "+
		"consensus HALT or an app-hash-mismatch panic (a divergent node cannot commit). Those, plus node-log scraping, "+
		"are the real signals below._\n\n")

	// Real signal 1: cross-node committed divergence (should be impossible; if seen, severe).
	if len(in.AppHash.Mismatches) > 0 {
		fmt.Fprintf(&b, "**FAIL (severe)**: %d height(s) with committed app_hash divergence across nodes:\n\n", len(in.AppHash.Mismatches))
		for _, m := range in.AppHash.Mismatches {
			fmt.Fprintf(&b, "- height %d:\n", m.Height)
			for node, h := range m.Hashes {
				fmt.Fprintf(&b, "    - %s: %s\n", node, h)
			}
		}
		fmt.Fprintf(&b, "\n")
	}

	// Real signal 2: node-log app-hash mismatch / panic / halt.
	if in.LogScan.Available {
		if len(in.LogScan.AppHashIssues) > 0 {
			fmt.Fprintf(&b, "**FAIL (ground truth)**: node logs show app-hash mismatch / panic (non-determinism or halt):\n\n")
			for _, l := range in.LogScan.AppHashIssues {
				fmt.Fprintf(&b, "    %s\n", l)
			}
			fmt.Fprintf(&b, "\n")
		} else {
			fmt.Fprintf(&b, "Node logs: no app-hash-mismatch / panic lines found.\n\n")
		}
	} else {
		fmt.Fprintf(&b, "_No node logs configured (logPaths); cannot scrape for app-hash-mismatch panics - the strongest determinism signal._\n\n")
	}

	// Real signal 3: consensus halt (height stalled past block time).
	if len(in.AppHash.StallNotes) > 0 {
		fmt.Fprintf(&b, "**Possible halt(s) detected** (investigate - could be determinism-induced):\n")
		for _, n := range in.AppHash.StallNotes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
		fmt.Fprintf(&b, "\n")
	}

	// Verdict. NOTE: this harness can only observe halts/mismatches; it cannot
	// prove determinism (a non-determinism that all nodes resolve identically -
	// same arbitrary result - produces no halt and is invisible here; only
	// re-execution / parallelism-1-vs-N diff would catch that, which is out of
	// scope). So the best positive outcome is "no halt observed", not "PASS".
	switch {
	case len(in.AppHash.Mismatches) > 0 || (in.LogScan.Available && len(in.LogScan.AppHashIssues) > 0):
		fmt.Fprintf(&b, "**Verdict: FAIL** - non-determinism / halt evidence found (see above).\n\n")
	case len(in.AppHash.StallNotes) > 0:
		fmt.Fprintf(&b, "**Verdict: REVIEW** - height stall(s) observed; confirm benign (slow blocks) not a halt.\n\n")
	case !in.AppHash.CompareAvailable && !in.LogScan.Available:
		fmt.Fprintf(&b, "**Verdict: INCONCLUSIVE** - single node and no logs; no determinism signal available.\n\n")
	case !in.AppHash.CompareAvailable:
		fmt.Fprintf(&b, "**Verdict: NO HALT (weak)** - single node; logs show no app-hash panic, but with one node "+
			"there is no cross-node check. Not a determinism proof.\n\n")
	default:
		fmt.Fprintf(&b, "**Verdict: NO HALT OBSERVED** - chain stayed live and %d nodes stayed consistent across %d heights "+
			"under the determinism workload. This is absence-of-evidence (no halt/mismatch), NOT a determinism proof - "+
			"determinism that resolves identically on all nodes would be invisible; only re-execution would catch it.\n\n",
			len(in.AppHash.NodeNames), in.AppHash.HeightsChecked)
	}

	// Workload summary.
	fmt.Fprintf(&b, "## Workload summary\n\n")
	fmt.Fprintf(&b, "| kind | count | expected lane |\n|---|---|---|\n")
	for _, s := range in.Sent {
		fmt.Fprintf(&b, "| %s | %d | %d |\n", s.Kind, s.Count, s.ExpectedLane)
	}
	fmt.Fprintf(&b, "\n")

	return b.String()
}

func sortedLaneIDs(m map[int32]uint64) []int32 {
	ids := make([]int32, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// jsonReport is the machine-readable form.
type jsonReport struct {
	TargetName  string                  `json:"targetName"`
	ChainID     uint64                  `json:"chainId"`
	GovMode     string                  `json:"govMode"`
	MaxBlockGas uint64                  `json:"maxBlockGas"`
	Lane        collector.LaneResult    `json:"lane"`
	Mempool     collector.MempoolResult `json:"mempool"`
	AppHash     collector.AppHashResult `json:"appHash"`
	LogScan     collector.LogScanResult `json:"logScan"`
	SentTotal   int                     `json:"sentTotal"`
	Sent        []workload.KindCount    `json:"sent"`
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
	jr := jsonReport{
		TargetName:  in.TargetName,
		ChainID:     in.ChainID,
		GovMode:     in.GovMode,
		MaxBlockGas: in.MaxBlockGas,
		Lane:        in.Lane,
		Mempool:     in.Mempool,
		AppHash:     in.AppHash,
		LogScan:     in.LogScan,
		SentTotal:   in.SentTotal,
		Sent:        in.Sent,
	}
	raw, err := json.MarshalIndent(jr, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), raw, 0o644); err != nil {
		return "", err
	}
	return mdPath, nil
}
