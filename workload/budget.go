package workload

// budget.go answers, BEFORE a run starts, "can the pool afford this?".
//
// Why it exists: the tip ramp (see Driver.SetTipRamp) is UNBOUNDED - the premium
// is weiPerSec x elapsed - so a tx sent late in a run costs many times what the
// same tx cost at t=0. Nothing in the send path notices until the chain starts
// refusing txs, and then it refuses them per account, forever, one log line at a
// time. A 2026-08-05 run on stable_988-1 produced 22,157 node-side
//
//	sender balance < tx cost (346497053980983 < 428300672058001): insufficient funds
//
// rejections: feeCap had ramped to ~20.4 gwei (21000 x 20.4 gwei = the 4.28e14
// cost above) against accounts funded with 0.008. Each refusal retires an
// account, which shrinks the pool, which drops mempool depth - and mempool depth
// is exactly what a fill-every-block run depends on. So an affordability slip
// does not degrade the result gracefully, it destroys the thing being measured.
//
// The estimate below is deliberately CONSERVATIVE: it prices txs at the send-rate
// CAP rather than at the rate the chain actually achieves, because under-warning
// is what costs a run.

import (
	"fmt"
	"math/big"
)

// SpendEstimate is the per-account balance a run is expected to consume.
//
// Two distinct quantities matter and they are easy to conflate:
//   - what a tx SPENDS: gas x (baseFee + tip), the amount actually deducted.
//   - what a tx RESERVES: gas x feeCap, which is what the EVM balance check
//     compares against. feeCap here is 2*baseFee + tip (accounts.Pool.Fees), so
//     the reserve always exceeds the spend, and the reserve is what produces
//     "insufficient funds". An account therefore needs its whole cumulative
//     spend PLUS one final reserve to send its last tx.
type SpendEstimate struct {
	TxsPerAccount   float64
	FinalFeeCap     *big.Int // feeCap at the end of the ramp
	MeanSpendPerTx  *big.Int
	FinalSpendPerTx *big.Int
	FinalReserve    *big.Int // gas x FinalFeeCap: the last tx's balance check
	RequiredPerAcct *big.Int // cumulative spend + FinalReserve
}

// EstimateSpend projects per-account cost for a bounded run.
//
// feeCapNow/tipNow are the chain's current fees (feeCapNow = 2*base + tipNow).
// The ramp adds rampWeiPerSec x elapsed to BOTH, so the mean premium over the run
// is rampWeiPerSec x durationSec / 2 and the final premium is the full
// rampWeiPerSec x durationSec.
//
// txsPerAccount is derived from targetTPS (the send-rate CAP, not the achieved
// rate) so the estimate errs high; accounts is the in-flight pool size. A
// zero/negative durationSec means continuous mode, where the ramp has no bound
// and no finite estimate exists - callers must treat that separately.
func EstimateSpend(feeCapNow, tipNow *big.Int, rampWeiPerSec int64, durationSec, targetTPS, accounts int, gasPerTx uint64) (SpendEstimate, bool) {
	if durationSec <= 0 || accounts <= 0 || targetTPS <= 0 || gasPerTx == 0 || feeCapNow == nil {
		return SpendEstimate{}, false
	}
	base := new(big.Int).Set(feeCapNow)
	gas := new(big.Int).SetUint64(gasPerTx)
	dur := big.NewInt(int64(durationSec))

	// The spend basis is baseFee+tip, i.e. feeCap - baseFee. Fees() builds
	// feeCap as 2*base + tip, so baseFee = feeCap - tip - baseFee ... which is
	// not recoverable from feeCap alone. Price the SPEND at feeCap too: that is
	// an over-estimate by exactly one baseFee per tx, which is the safe
	// direction and keeps the estimate honest without guessing baseFee.
	fullPremium := new(big.Int).Mul(big.NewInt(rampWeiPerSec), dur)
	meanPremium := new(big.Int).Div(fullPremium, big.NewInt(2))

	finalFeeCap := new(big.Int).Add(base, fullPremium)
	meanFeeCap := new(big.Int).Add(base, meanPremium)

	meanSpend := new(big.Int).Mul(gas, meanFeeCap)
	finalSpend := new(big.Int).Mul(gas, finalFeeCap)
	finalReserve := new(big.Int).Mul(gas, finalFeeCap)

	// txsPerAccount = targetTPS * durationSec / accounts, kept in integer wei
	// math via a scaled numerator so a fractional value is not truncated to 0.
	totalTxs := int64(targetTPS) * int64(durationSec)
	txsPerAcct := float64(totalTxs) / float64(accounts)

	// cumulative = meanSpend * txsPerAccount, done as (meanSpend*totalTxs)/accounts.
	cumulative := new(big.Int).Mul(meanSpend, big.NewInt(totalTxs))
	cumulative.Div(cumulative, big.NewInt(int64(accounts)))

	return SpendEstimate{
		TxsPerAccount:   txsPerAcct,
		FinalFeeCap:     finalFeeCap,
		MeanSpendPerTx:  meanSpend,
		FinalSpendPerTx: finalSpend,
		FinalReserve:    finalReserve,
		RequiredPerAcct: new(big.Int).Add(cumulative, finalReserve),
	}, true
}

// gweiStr renders wei as gwei for logs.
func gweiStr(wei *big.Int) string {
	if wei == nil {
		return "0"
	}
	f := new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(1e9))
	return f.Text('f', 3)
}

// tokenStr renders wei as whole tokens for logs.
func tokenStr(wei *big.Int) string {
	if wei == nil {
		return "0"
	}
	f := new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(1e18))
	return f.Text('f', 9)
}

// SpendSafetyFactor is the headroom demanded over the projected requirement.
// Accounts are not drained uniformly - the ready-queue rotation is only roughly
// fair, and every retirement concentrates more txs onto the survivors - so a run
// sized at exactly 1.0x loses its tail accounts and then spirals.
const SpendSafetyFactor = 2

// CheckAffordable compares the poorest account against the projected
// requirement and returns a human-readable verdict. ok=false means the run is
// expected to bankrupt accounts mid-flight and should not be started.
func CheckAffordable(est SpendEstimate, minBalance *big.Int) (ok bool, detail string) {
	if minBalance == nil || est.RequiredPerAcct == nil {
		return true, ""
	}
	want := new(big.Int).Mul(est.RequiredPerAcct, big.NewInt(SpendSafetyFactor))
	ratio := new(big.Float).Quo(new(big.Float).SetInt(minBalance), new(big.Float).SetInt(est.RequiredPerAcct))
	r, _ := ratio.Float64()
	detail = fmt.Sprintf(
		"poorest account %s tokens vs projected need %s (%.1f txs/account, "+
			"mean %s/tx, final %s/tx at feeCap %s gwei, +%s final reserve) = %.2fx cover; want >=%dx (%s)",
		tokenStr(minBalance), tokenStr(est.RequiredPerAcct), est.TxsPerAccount,
		tokenStr(est.MeanSpendPerTx), tokenStr(est.FinalSpendPerTx), gweiStr(est.FinalFeeCap),
		tokenStr(est.FinalReserve), r, SpendSafetyFactor, tokenStr(want))
	return minBalance.Cmp(want) >= 0, detail
}
