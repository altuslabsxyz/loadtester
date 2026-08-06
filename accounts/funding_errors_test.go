package accounts

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// Real error strings observed against stable_988-1 on 2026-07-27. Each must be
// routed to exactly one recovery path: retry the same nonce, re-sign at a fresh
// nonce, skip the account, or fail the run.
func TestFundingErrorClassification(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		transient bool // retry the SAME nonce
		nonce     bool // re-read committed nonce and re-sign
		occupied  bool // a different tx holds the slot
		broke     bool // sender cannot pay
	}{{
		name: "memiavl commit race",
		err: errors.New("rpc error: code = Unknown desc = codespace sdk code 38: not found: " +
			"failed to load state at height 33247821; historical version not ready: 33247821: " +
			"invalid height (latest height: 33247821)"),
		transient: true,
	}, {
		name:  "nonce below account nonce",
		err:   errors.New("failed to broadcast transaction: got 58, expected 59: txnonce is lower than account nonce"),
		nonce: true,
	}, {
		name:  "nonce above account nonce",
		err:   errors.New("txnonce is higher than account nonce"),
		nonce: true,
	}, {
		name:     "slot held, replacement refused",
		err:      errors.New("tx doesn't fit the replacement rule, oldPriority: 125, newPriority: 125"),
		occupied: true,
	}, {
		name:     "slot held even when out-bid 6x",
		err:      errors.New("tx doesn't fit the replacement rule, oldPriority: 125, newPriority: 784"),
		occupied: true,
	}, {
		name:  "sender cannot pay",
		err:   errors.New("insufficient funds for gas * price + value"),
		broke: true,
	}, {
		name:      "transport flake",
		err:       errors.New("read tcp 10.0.0.1:1234->10.10.30.19:8545: connection reset by peer"),
		transient: true,
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isTransientQueryErr(c.err); got != c.transient {
				t.Errorf("isTransientQueryErr = %v, want %v", got, c.transient)
			}
			if got := isNonceMismatchErr(c.err); got != c.nonce {
				t.Errorf("isNonceMismatchErr = %v, want %v", got, c.nonce)
			}
			if got := isSlotOccupiedErr(c.err); got != c.occupied {
				t.Errorf("isSlotOccupiedErr = %v, want %v", got, c.occupied)
			}
			if got := isBrokeErr(c.err); got != c.broke {
				t.Errorf("isBrokeErr = %v, want %v", got, c.broke)
			}
		})
	}
}

// A caller's own cancellation must never be mistaken for a node-side race, or
// shutdown would spin through the whole retry budget.
func TestContextErrorsAreNotTransient(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded,
		fmt.Errorf("read balance: %w", context.Canceled)} {
		if isTransientQueryErr(err) {
			t.Errorf("isTransientQueryErr(%v) = true, want false", err)
		}
	}
}

// Fund distinguishes skippable senders from fatal errors via errors.Is, so the
// sentinels must survive wrapping.
func TestSkippableSentinelsSurviveWrapping(t *testing.T) {
	wedged := fmt.Errorf("0xabc nonce 3: %w", errSlotWedged)
	if !errors.Is(wedged, errSlotWedged) {
		t.Error("wrapped errSlotWedged not detected by errors.Is")
	}
	if errors.Is(wedged, errSenderBroke) {
		t.Error("errSlotWedged must not match errSenderBroke")
	}
	broke := fmt.Errorf("0xabc: %w", errSenderBroke)
	if !errors.Is(broke, errSenderBroke) {
		t.Error("wrapped errSenderBroke not detected by errors.Is")
	}
}

// retryQuery must stop at the first non-transient error and must not retry a
// success. Both matter: over-retrying a definitive rejection wastes the funding
// window, and re-running a read is only safe because reads are idempotent.
func TestRetryQueryStopsAppropriately(t *testing.T) {
	calls := 0
	_, err := retryQuery(context.Background(), func(context.Context) (int, error) {
		calls++
		return 0, errors.New("execution reverted")
	})
	if err == nil || calls != 1 {
		t.Errorf("non-transient error: calls=%d err=%v, want calls=1 and an error", calls, err)
	}

	calls = 0
	v, err := retryQuery(context.Background(), func(context.Context) (int, error) {
		calls++
		if calls < 3 {
			return 0, errors.New("historical version not ready: 42")
		}
		return 7, nil
	})
	if err != nil || v != 7 || calls != 3 {
		t.Errorf("transient then success: v=%d calls=%d err=%v, want v=7 calls=3 err=nil", v, calls, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls = 0
	if _, err = retryQuery(ctx, func(context.Context) (int, error) {
		calls++
		return 0, errors.New("historical version not ready: 42")
	}); err == nil {
		t.Error("cancelled context: expected an error")
	}
	if calls > 1 {
		t.Errorf("cancelled context: made %d calls, want at most 1", calls)
	}
}
