package workload

import (
	"math/big"
	"testing"
)

const gwei = 1_000_000_000

// TestEstimateSpendReproducesObservedBankruptcy pins the estimator against the
// real 2026-08-05 failure. That run used ramp 0.1 gwei/s over 150s with 40000
// accounts funded at 0.008, and the node logged 22,157 "insufficient funds"
// rejections at a feeCap of ~20.4 gwei. The estimator must judge that config
// unaffordable - if it passes this config, it is useless.
func TestEstimateSpendReproducesObservedBankruptcy(t *testing.T) {
	feeCapNow := big.NewInt(2.2 * gwei) // 2*baseFee(1.1) + tip(~0) on this devnet
	est, ok := EstimateSpend(feeCapNow, big.NewInt(0), 100_000_000 /*0.1 gwei/s*/, 150, 9000, 40000, 21000)
	if !ok {
		t.Fatal("estimate should be computable for a bounded run")
	}
	// 0.1 gwei/s x 150s = 15 gwei premium on top of 2.2.
	if got, want := est.FinalFeeCap, big.NewInt(17.2*gwei); got.Cmp(want) != 0 {
		t.Errorf("final feeCap = %s gwei, want %s", gweiStr(got), gweiStr(want))
	}
	if est.TxsPerAccount < 33 || est.TxsPerAccount > 34 {
		t.Errorf("txs/account = %.2f, want ~33.75", est.TxsPerAccount)
	}
	funded, _ := new(big.Int).SetString("8000000000000000", 10) // 0.008 tokens
	ok, detail := CheckAffordable(est, funded)
	if ok {
		t.Errorf("0.008/account judged affordable, but the real run bankrupted accounts: %s", detail)
	}
	t.Logf("correctly rejected: %s", detail)
}

// TestEstimateSpendGentleRampIsAffordable verifies the fix direction: dropping
// the ramp to 0.02 gwei/s makes the same pool comfortably solvent. The ramp's
// purpose is ordinal (strictly order txs sent ms apart), so the smaller slope
// loses nothing at 20,000 wei of separation per millisecond.
func TestEstimateSpendGentleRampIsAffordable(t *testing.T) {
	feeCapNow := big.NewInt(2.2 * gwei)
	est, ok := EstimateSpend(feeCapNow, big.NewInt(0), 20_000_000 /*0.02 gwei/s*/, 150, 9000, 40000, 21000)
	if !ok {
		t.Fatal("estimate should be computable")
	}
	if got, want := est.FinalFeeCap, big.NewInt(5.2*gwei); got.Cmp(want) != 0 {
		t.Errorf("final feeCap = %s gwei, want %s", gweiStr(got), gweiStr(want))
	}
	funded, _ := new(big.Int).SetString("8000000000000000", 10) // 0.008 tokens
	ok, detail := CheckAffordable(est, funded)
	if !ok {
		t.Errorf("0.008/account should be affordable at 0.02 gwei/s: %s", detail)
	}
	t.Logf("accepted: %s", detail)
}

// TestEstimateSpend9kTargetIsAffordable pins experiments/target-9k.yaml: 36000
// accounts at 0.012, targetTPS 11000 over 300s, ramp 0.003 gwei/s, baseFee 1.0
// gwei (so feeCapNow = 2.0 gwei). It must clear the 2x preflight gate - if a
// future edit to that file or to the fee model drops it below, this fails here
// rather than 20k rejections into a devnet run.
func TestEstimateSpend9kTargetIsAffordable(t *testing.T) {
	est, ok := EstimateSpend(big.NewInt(2*gwei), big.NewInt(0), 3_000_000 /*0.003 gwei/s*/, 300, 11000, 36000, 21000)
	if !ok {
		t.Fatal("estimate should be computable")
	}
	funded, _ := new(big.Int).SetString("12000000000000000", 10) // 0.012 tokens
	affordable, detail := CheckAffordable(est, funded)
	if !affordable {
		t.Errorf("target-9k.yaml must pass the spend preflight: %s", detail)
	}
	t.Logf("target-9k: %s", detail)

	// And the guard rail: a 0.01 gwei/s ramp on the same config must NOT pass,
	// which is what keeps the ramp note in that file honest.
	steep, _ := EstimateSpend(big.NewInt(2*gwei), big.NewInt(0), 10_000_000, 300, 11000, 36000, 21000)
	if ok, _ := CheckAffordable(steep, funded); ok {
		t.Error("0.01 gwei/s at 300s should be rejected at 0.012/account")
	}
}

// TestEstimateSpendRequirementIncludesFinalReserve guards the distinction the
// EVM balance check actually enforces: an account must hold gas x feeCap to send
// its LAST tx, not merely the cumulative amount it will spend. Dropping the
// reserve term would let a run be sized to end with a dust balance that cannot
// pay for the final send.
func TestEstimateSpendRequirementIncludesFinalReserve(t *testing.T) {
	est, ok := EstimateSpend(big.NewInt(2*gwei), big.NewInt(0), 0, 100, 1000, 1000, 21000)
	if !ok {
		t.Fatal("estimate should be computable")
	}
	// Flat tip: mean == final == 2 gwei, 100 txs/account.
	if est.TxsPerAccount != 100 {
		t.Fatalf("txs/account = %.2f, want 100", est.TxsPerAccount)
	}
	cumulative := new(big.Int).Mul(est.MeanSpendPerTx, big.NewInt(100))
	want := new(big.Int).Add(cumulative, est.FinalReserve)
	if est.RequiredPerAcct.Cmp(want) != 0 {
		t.Errorf("required = %s, want cumulative+reserve = %s",
			est.RequiredPerAcct, want)
	}
	if est.FinalReserve.Sign() == 0 {
		t.Error("final reserve must be non-zero")
	}
}

// TestEstimateSpendContinuousIsNotEstimable pins that continuous mode yields no
// estimate: the ramp is unbounded there, so any finite projection would be a lie.
func TestEstimateSpendContinuousIsNotEstimable(t *testing.T) {
	for _, dur := range []int{0, -1} {
		if _, ok := EstimateSpend(big.NewInt(gwei), big.NewInt(0), 100_000_000, dur, 9000, 40000, 21000); ok {
			t.Errorf("durationSec=%d produced an estimate; continuous runs have no bounded cost", dur)
		}
	}
}
