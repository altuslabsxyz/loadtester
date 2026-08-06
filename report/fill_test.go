package report

import (
	"strings"
	"testing"

	"github.com/stablelabs/loadtester/collector"
)

// fillInput builds a minimal Input carrying only the block-fill data, which is
// all the "Block fill" section reads.
func fillInput(maxGas uint64, samples []collector.BlockFill) Input {
	f := collector.FillResult{Blocks: len(samples)}
	for i, s := range samples {
		if i == 0 || s.Txs < f.TxsMin {
			f.TxsMin = s.Txs
		}
		if s.Txs > f.TxsMax {
			f.TxsMax = s.Txs
		}
		f.TxsTotal += uint64(s.Txs)
	}
	f.Samples = samples
	return Input{
		TargetName:  "t",
		MaxBlockGas: maxGas,
		Lane: collector.LaneResult{
			BlocksObserved: len(samples),
			PeakLaneGas:    map[int32]uint64{},
			Quota:          map[int32]uint64{},
			LaneNames:      map[int32]string{},
			Fill:           f,
		},
	}
}

// TestFillCeilingFromFattestBlock pins the capacity basis. The devnet shape is
// 147M block gas / 21000 gas per native transfer = 7000 tx/block. A near-empty
// block carrying only a cheap system tx must NOT be allowed to set gas-per-tx:
// deriving the divisor from it would inflate the ceiling and silently turn an
// underfilled run into a passing one.
func TestFillCeilingFromFattestBlock(t *testing.T) {
	const maxGas = 147_000_000
	samples := []collector.BlockFill{
		{Height: 1, Txs: 1, Gas: 5_000},            // system-tx-only block: gas/tx = 5000
		{Height: 2, Txs: 7000, Gas: 7000 * 21_000}, // full block: gas/tx = 21000
		{Height: 3, Txs: 2200, Gas: 2200 * 21_000}, // underfilled
	}
	md := Markdown(fillInput(maxGas, samples))
	if !strings.Contains(md, "**7000 tx/block ceiling**") {
		t.Errorf("ceiling not derived from the fattest block; got:\n%s", section(md))
	}
	// 5000 gas/tx would have produced a 29400 ceiling.
	if strings.Contains(md, "29400") {
		t.Error("ceiling was derived from the cheap system-tx block")
	}
}

// TestFillPercentilesAndDiagnostic reproduces the measured 2026-08-05 shape:
// the ceiling IS reached but most blocks fall short. The section must report the
// low percentiles and fire the supply-side diagnostic, because that combination
// is the reap-staleness signature rather than a chain that cannot keep up.
func TestFillPercentilesAndDiagnostic(t *testing.T) {
	const maxGas = 147_000_000
	var samples []collector.BlockFill
	// 10% of blocks full, the rest at ~2200 - bimodal, like the real run.
	for i := 0; i < 100; i++ {
		txs := 2200
		if i%10 == 0 {
			txs = 7000
		}
		samples = append(samples, collector.BlockFill{
			Height: uint64(i), Txs: txs, Gas: uint64(txs) * 21_000,
		})
	}
	md := Markdown(fillInput(maxGas, samples))
	for _, want := range []string{
		"Block fill (throughput stability)",
		// Only 10% of blocks are full, so even p90 sits at the 2200 floor -
		// precisely why max alone must never be read as "we hit 7k".
		"| 2200 | 2200 | 2200 | 2200 | 7000 |", // min p10 p50 p90 max
		"**7000 tx/block ceiling**",
		"Blocks at ceiling: 10/100 (10%)",
		"candidate SUPPLY at proposal time",
		"funding.accountsN",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("missing %q in:\n%s", want, section(md))
		}
	}
}

// TestFillNoDiagnosticWhenStable verifies the diagnostic stays quiet on a run
// that actually filled every block - otherwise it would cry wolf on a pass.
func TestFillNoDiagnosticWhenStable(t *testing.T) {
	const maxGas = 147_000_000
	var samples []collector.BlockFill
	for i := 0; i < 100; i++ {
		samples = append(samples, collector.BlockFill{
			Height: uint64(i), Txs: 7000, Gas: 7000 * 21_000,
		})
	}
	md := Markdown(fillInput(maxGas, samples))
	if !strings.Contains(md, "Blocks at ceiling: 100/100 (100%)") {
		t.Errorf("expected a fully-filled run; got:\n%s", section(md))
	}
	if strings.Contains(md, "candidate SUPPLY at proposal time") {
		t.Error("supply diagnostic fired on a run that filled every block")
	}
}

// TestFillOmittedWhenNoBlocks guards against rendering a divide-by-zero or an
// empty table when no block was ever observed.
func TestFillOmittedWhenNoBlocks(t *testing.T) {
	md := Markdown(fillInput(147_000_000, nil))
	if strings.Contains(md, "Block fill (throughput stability)") {
		t.Error("fill section rendered with zero observed blocks")
	}
}

// section extracts just the block-fill part for readable failures.
func section(md string) string {
	i := strings.Index(md, "## Block fill")
	if i < 0 {
		return md
	}
	rest := md[i:]
	if j := strings.Index(rest, "## Workload"); j > 0 {
		return rest[:j]
	}
	return rest
}
