package accounts

import (
	"testing"
)

// TestFundScheduleDistinctSendersPerRound pins the invariant the fan-out relies
// on: within a round every sender is distinct, so the chain's
// 1-in-flight-per-sender rule (a future nonce is rejected, never queued) is
// never violated and the whole round can land in ~one block.
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

// TestFundScheduleSendersAreAlreadyFunded pins the ordering property that makes
// the fan-out correct: a round's sender must have been funded in an EARLIER
// round (or be the master, -1), never in the same round or a later one.
func TestFundScheduleSendersAreAlreadyFunded(t *testing.T) {
	const n = 5000
	fundedBy := map[int]int{} // receiver -> round it was funded in
	for ri, round := range fundSchedule(n) {
		for _, fp := range round {
			if fp.sender >= 0 {
				r, ok := fundedBy[fp.sender]
				if !ok {
					t.Fatalf("round %d: sender %d was never funded", ri, fp.sender)
				}
				if r >= ri {
					t.Fatalf("round %d: sender %d was only funded in round %d", ri, fp.sender, r)
				}
			}
		}
		for _, fp := range round {
			fundedBy[fp.receiver] = ri
		}
	}
}
