package accounts

import (
	"math/big"
	"testing"
)

func wei(n int64) *big.Int { return big.NewInt(n) }

// TestPartitionRoundGatesOnSenderNotReceiver pins the safety invariant and the
// non-obvious half of it: CheckTx runs only the ante, and the ante creates the
// account it charges FEES to (stable-evm ante/evm/06_account_verification.go
// creates `from`). So it is the SENDER that can race two concurrent creations
// into the account-number unique index; the receiver is created later, by block
// execution. Funding a brand-new account is therefore safe to broadcast
// concurrently provided the payer already exists.
func TestPartitionRoundGatesOnSenderNotReceiver(t *testing.T) {
	endow := []*big.Int{wei(1), wei(1), wei(1), wei(1)}
	// Senders 0 and 2 exist; 1 and 3 do not. Receiver existence is irrelevant.
	senderExists := []bool{true, false, true, false}
	round := []fundPair{{0, 1}, {1, 2}, {2, 3}}

	conc, ser := partitionRound(round, endow, senderExists)
	if len(conc) != 2 || conc[0].sender != 0 || conc[1].sender != 2 {
		t.Errorf("concurrent = %v, want senders [0 2]", conc)
	}
	if len(ser) != 1 || ser[0].sender != 1 {
		t.Errorf("serial = %v, want sender [1]", ser)
	}
}

// TestPartitionRoundMasterIsAlwaysConcurrent covers sender -1: the master holds
// the pool's float and has necessarily transacted, so it never needs the serial
// path. Without this the very first round of every pass would be serialized for
// no reason.
func TestPartitionRoundMasterIsAlwaysConcurrent(t *testing.T) {
	conc, ser := partitionRound([]fundPair{{-1, 0}}, []*big.Int{wei(1)}, nil /* nothing known */)
	if len(conc) != 1 || len(ser) != 0 {
		t.Errorf("master pair: concurrent=%v serial=%v, want it concurrent", conc, ser)
	}
}

// TestPartitionRoundFreshSeedIsFullyConcurrent is the case the old
// receiver-based gate got wrong: on the first fund of a brand-new seed NO
// receiver exists, yet every sender is either the master or an account funded in
// an already-committed round, so the whole round may go concurrently. The old
// rule reported "0 parallel" here and serialized 36,000 broadcasts.
func TestPartitionRoundFreshSeedIsFullyConcurrent(t *testing.T) {
	const n = 64
	endow := make([]*big.Int, n)
	for i := range endow {
		endow[i] = wei(1)
	}
	senderExists := make([]bool, n) // nothing exists yet
	var round []fundPair
	// Mirror the tree: senders are the master plus already-funded accounts.
	for i := 0; i < 8; i++ {
		senderExists[i] = true
		round = append(round, fundPair{sender: i, receiver: 8 + i})
	}
	round = append(round, fundPair{sender: -1, receiver: 63})

	conc, ser := partitionRound(round, endow, senderExists)
	if len(ser) != 0 {
		t.Errorf("serial = %v, want none: every payer exists", ser)
	}
	if len(conc) != 9 {
		t.Errorf("concurrent = %d pairs, want 9", len(conc))
	}
}

// TestPartitionRoundDropsZeroEndowments covers the case that makes a re-fund
// fast: an account already holding enough gets no transfer at all, so early
// rounds of a grown seeded pool collapse to no-ops.
func TestPartitionRoundDropsZeroEndowments(t *testing.T) {
	endow := []*big.Int{wei(0), wei(0), wei(5)}
	senderExists := []bool{true, true, true}
	conc, ser := partitionRound([]fundPair{{-1, 0}, {0, 1}, {1, 2}}, endow, senderExists)
	if len(ser) != 0 {
		t.Errorf("serial = %v, want empty", ser)
	}
	if len(conc) != 1 || conc[0].receiver != 2 {
		t.Fatalf("concurrent = %v, want only receiver 2", conc)
	}
}

// TestPartitionRoundDefaultsToSerial guards the fail-safe direction: a sender
// not KNOWN to exist (e.g. one whose own top-up was skipped as wedged) must go
// serial, because guessing wrong panics the node rather than merely slowing the
// pass.
func TestPartitionRoundDefaultsToSerial(t *testing.T) {
	endow := []*big.Int{wei(1), wei(1)}
	conc, ser := partitionRound([]fundPair{{0, 1}}, endow, nil /* nothing known */)
	if len(conc) != 0 {
		t.Errorf("concurrent = %v, want empty when the payer is unknown", conc)
	}
	if len(ser) != 1 {
		t.Errorf("serial = %v, want the pair", ser)
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
