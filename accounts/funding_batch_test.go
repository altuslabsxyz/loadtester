package accounts

import (
	"math/big"
	"testing"
)

func wei(n int64) *big.Int { return big.NewInt(n) }

// TestPartitionRoundKeepsAccountCreationsSerial pins the safety invariant: a
// transfer to an account that does not yet exist on chain CREATES it, and two
// concurrent creations panic the node's CheckTx in the fee path. Only receivers
// already present in the auth store may be broadcast concurrently.
func TestPartitionRoundKeepsAccountCreationsSerial(t *testing.T) {
	endow := []*big.Int{wei(1), wei(1), wei(1), wei(1)}
	exists := []bool{true, false, true, false}
	round := []fundPair{{-1, 0}, {0, 1}, {1, 2}, {2, 3}}

	conc, ser := partitionRound(round, endow, exists)
	if len(conc) != 2 || conc[0].receiver != 0 || conc[1].receiver != 2 {
		t.Errorf("concurrent = %v, want receivers [0 2]", conc)
	}
	if len(ser) != 2 || ser[0].receiver != 1 || ser[1].receiver != 3 {
		t.Errorf("serial = %v, want receivers [1 3]", ser)
	}
}

// TestPartitionRoundDropsZeroEndowments covers the case that makes a re-fund
// fast: an account already holding enough gets no transfer at all, so early
// rounds of a grown seeded pool collapse to no-ops.
func TestPartitionRoundDropsZeroEndowments(t *testing.T) {
	endow := []*big.Int{wei(0), wei(0), wei(5)}
	exists := []bool{true, true, true}
	conc, ser := partitionRound([]fundPair{{-1, 0}, {0, 1}, {1, 2}}, endow, exists)
	if len(ser) != 0 {
		t.Errorf("serial = %v, want empty", ser)
	}
	if len(conc) != 1 || conc[0].receiver != 2 {
		t.Fatalf("concurrent = %v, want only receiver 2", conc)
	}
}

// TestPartitionRoundDefaultsToSerial guards the fail-safe direction: an unknown
// receiver (missing from the exists slice, e.g. a shorter state read) must be
// treated as a creation, because guessing wrong panics the node rather than
// merely slowing the pass.
func TestPartitionRoundDefaultsToSerial(t *testing.T) {
	endow := []*big.Int{wei(1), wei(1)}
	conc, ser := partitionRound([]fundPair{{-1, 0}, {0, 1}}, endow, nil /* nothing known */)
	if len(conc) != 0 {
		t.Errorf("concurrent = %v, want empty when existence is unknown", conc)
	}
	if len(ser) != 2 {
		t.Errorf("serial = %v, want both pairs", ser)
	}
}

// TestPartitionRoundIgnoresOutOfRangeReceivers keeps a malformed schedule from
// indexing past the endowment slice.
func TestPartitionRoundIgnoresOutOfRangeReceivers(t *testing.T) {
	conc, ser := partitionRound([]fundPair{{-1, 7}}, []*big.Int{wei(1)}, []bool{true})
	if len(conc)+len(ser) != 0 {
		t.Errorf("out-of-range receiver produced work: %v %v", conc, ser)
	}
}

// TestFundScheduleDistinctSendersPerRound pins the property the concurrent
// broadcast depends on: within a round every sender is distinct, so no two
// goroutines ever touch the same Account (and the chain's 1-in-flight-per-sender
// rule is never violated).
func TestFundScheduleDistinctSendersPerRound(t *testing.T) {
	for _, n := range []int{1, 2, 7, 15, 1000, 36000} {
		for ri, round := range fundSchedule(n) {
			seen := map[int]bool{}
			for _, fp := range round {
				if seen[fp.sender] {
					t.Fatalf("n=%d round %d: sender %d appears twice", n, ri, fp.sender)
				}
				seen[fp.sender] = true
			}
		}
	}
}

// TestFundScheduleCoversEveryReceiverExactlyOnce guards against a schedule that
// silently under-funds (a receiver never funded) or double-sends to one.
func TestFundScheduleCoversEveryReceiverExactlyOnce(t *testing.T) {
	const n = 36000
	seen := make([]int, n)
	for _, round := range fundSchedule(n) {
		for _, fp := range round {
			seen[fp.receiver]++
		}
	}
	for i, c := range seen {
		if c != 1 {
			t.Fatalf("receiver %d funded %d times, want exactly 1", i, c)
		}
	}
}
