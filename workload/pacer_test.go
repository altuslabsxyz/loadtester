package workload

import (
	"testing"
	"time"
)

// TestIntegerPacerRate pins the pacer's long-run rate. The credits owed are
// derived from absolute elapsed time, so this is exact and does not depend on
// wall-clock scheduling of the test.
func TestIntegerPacerRate(t *testing.T) {
	for _, target := range []int{1, 50, 200, 1000, 3000, 100000} {
		p, err := newIntegerPacer(target)
		if err != nil {
			t.Fatalf("target %d: %v", target, err)
		}
		base := time.Unix(0, 0)
		if n := p.due(base); n != 0 {
			t.Errorf("target %d: first call emitted %d credits, want 0 (no initial burst)", target, n)
		}
		total := uint64(0)
		for elapsed := p.tick; elapsed <= 10*time.Second; elapsed += p.tick {
			total += p.due(base.Add(elapsed))
		}
		if want := uint64(target) * 10; total != want {
			t.Errorf("target %d: emitted %d credits over 10s, want %d", target, total, want)
		}
	}
}

// TestIntegerPacerNoDriftOnLateWake verifies the self-correcting property: a wake
// that arrives late still emits exactly the backlog it owes, so a stalled
// scheduler cannot permanently depress the rate.
func TestIntegerPacerNoDriftOnLateWake(t *testing.T) {
	p, err := newIntegerPacer(1000)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(0, 0)
	p.due(base)
	// One wake, 2 seconds late.
	if n := p.due(base.Add(2 * time.Second)); n != 2000 {
		t.Errorf("late wake emitted %d credits, want 2000", n)
	}
	// The next on-time wake owes only its own tick.
	if n := p.due(base.Add(2*time.Second + 5*time.Millisecond)); n != 5 {
		t.Errorf("following wake emitted %d credits, want 5", n)
	}
}

// TestIntegerPacerTickChoice checks that low rates keep one wake per credit and
// high rates batch onto the fixed tick.
func TestIntegerPacerTickChoice(t *testing.T) {
	cases := map[int]time.Duration{
		10:   100 * time.Millisecond, // 1 credit per wake
		200:  pacerTick,              // exactly at the boundary
		1000: pacerTick,              // batched: 5 credits per wake
	}
	for target, want := range cases {
		p, err := newIntegerPacer(target)
		if err != nil {
			t.Fatal(err)
		}
		if p.tick != want {
			t.Errorf("target %d: tick %v, want %v", target, p.tick, want)
		}
	}
}

func TestIntegerPacerRejectsOutOfRange(t *testing.T) {
	for _, target := range []int{0, -1, int(time.Second) + 1} {
		if _, err := newIntegerPacer(target); err == nil {
			t.Errorf("target %d: expected an error", target)
		}
	}
}

// TestIntegerPacerNoOverflowOverLongRun guards the split multiply in due():
// target*elapsed as a single product overflows uint64 within an hour at the
// maximum allowed target.
func TestIntegerPacerNoOverflowOverLongRun(t *testing.T) {
	const target = int(time.Second) // 1e9, the maximum accepted
	p, err := newIntegerPacer(target)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(0, 0)
	p.due(base)
	got := p.due(base.Add(time.Hour))
	if want := uint64(target) * 3600; got != want {
		t.Errorf("credits owed after 1h = %d, want %d", got, want)
	}
}
