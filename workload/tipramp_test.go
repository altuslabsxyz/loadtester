package workload

import (
	"math/big"
	"testing"
	"time"
)

// The ramp must move feeCap and tip by the SAME amount: an EIP-1559 tx with
// tip > feeCap is invalid, and Fees() derives feeCap from the un-ramped tip.
func TestTipRampRaisesBothFeesEqually(t *testing.T) {
	d := &Driver{}
	d.feeCap.Store(big.NewInt(2_125_000_000))
	d.tip.Store(big.NewInt(125_000_000))

	baseCap, baseTip := d.currentFees()
	if baseCap.Cmp(big.NewInt(2_125_000_000)) != 0 || baseTip.Cmp(big.NewInt(125_000_000)) != 0 {
		t.Fatalf("no ramp configured should pass fees through: got %s/%s", baseCap, baseTip)
	}

	d.SetTipRamp(100_000_000) // 0.1 gwei/s
	d.rampStart = time.Now().Add(-2 * time.Second)
	cap2, tip2 := d.currentFees()

	capDelta := new(big.Int).Sub(cap2, big.NewInt(2_125_000_000))
	tipDelta := new(big.Int).Sub(tip2, big.NewInt(125_000_000))
	if capDelta.Cmp(tipDelta) != 0 {
		t.Errorf("feeCap moved %s but tip moved %s; they must move together", capDelta, tipDelta)
	}
	// ~2s x 0.1 gwei/s = ~0.2 gwei. Allow slack for test scheduling.
	if tipDelta.Cmp(big.NewInt(150_000_000)) < 0 || tipDelta.Cmp(big.NewInt(400_000_000)) > 0 {
		t.Errorf("premium after ~2s = %s wei, want ~200000000", tipDelta)
	}
	if tip2.Cmp(cap2) > 0 {
		t.Errorf("tip %s exceeds feeCap %s - tx would be invalid", tip2, cap2)
	}
}

// Priority is the tip, so a later tx must out-rank an earlier one. Resolution
// has to be sub-second: at a few thousand TPS, one-second bands would lump
// thousands of txs together and leave the FIFO tie-break in charge.
func TestTipRampIsMonotonicAtMillisecondResolution(t *testing.T) {
	d := &Driver{}
	d.feeCap.Store(big.NewInt(2_125_000_000))
	d.tip.Store(big.NewInt(125_000_000))
	d.SetTipRamp(100_000_000) // 0.1 gwei/s => 100000 wei/ms

	start := time.Now()
	var prev *big.Int
	for _, ms := range []int{0, 10, 50, 200, 1000, 2500} {
		d.rampStart = start.Add(-time.Duration(ms) * time.Millisecond)
		_, tip := d.currentFees()
		if prev != nil && tip.Cmp(prev) <= 0 {
			t.Errorf("tip at %dms = %s did not exceed the earlier %s", ms, tip, prev)
		}
		prev = tip
	}

	// One priority unit on this chain is 1e6 wei, so 10ms apart must differ by
	// at least that much or the two txs land in the same band.
	d.rampStart = start
	_, t0 := d.currentFees()
	d.rampStart = start.Add(-10 * time.Millisecond)
	_, t10 := d.currentFees()
	if diff := new(big.Int).Sub(t10, t0); diff.Cmp(big.NewInt(1_000_000)) < 0 {
		t.Errorf("10ms of ramp = %s wei, want >= 1000000 (one priority unit)", diff)
	}
}

func TestSetTipRampZeroDisables(t *testing.T) {
	d := &Driver{}
	d.feeCap.Store(big.NewInt(7))
	d.tip.Store(big.NewInt(3))
	d.SetTipRamp(100)
	d.SetTipRamp(0)
	d.rampStart = time.Now().Add(-time.Hour)
	feeCap, tip := d.currentFees()
	if feeCap.Cmp(big.NewInt(7)) != 0 || tip.Cmp(big.NewInt(3)) != 0 {
		t.Errorf("ramp of 0 should disable the premium: got %s/%s", feeCap, tip)
	}
}
