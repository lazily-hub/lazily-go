package lazily

import (
	"fmt"
	"testing"
)

// Equivalence and ordering tests for Cell observer storage (#lzdartobservercow).
//
// Every test in this file except TestCellObserverFiringOrderIsRegistrationOrder
// and TestCellObserverDisposeDuringNotifySkipsInFlight passes identically
// against the previous map[uint64]func(T) implementation and the slot-list one
// that replaced it; they are the evidence that the perf change altered no
// observable behavior.
//
// TestCellObserverDisposeDuringNotifySkipsInFlight is the later exception: it
// asserted the opposite until lazily-go migrated onto the spec's
// "unsubscribe during notify takes effect immediately" clause. See its own
// doc comment, and the fixture runner in observer_conformance_test.go, which is
// the canonical gate — it executes the lazily-spec JSON directly rather than
// transcribing it.
//
// The ordering test is deliberately NOT an equivalence test. The map
// implementation fired observers in Go's randomized map-iteration order, so it
// had no order to preserve. Registration order is a new, decided guarantee: it
// matches lazily-dart, it falls out of the slot slice for free, and a reactive
// library whose notification order is nondeterministic cannot support
// reproducible tests or deterministic replay downstream.

func TestCellObserverFiringOrderIsRegistrationOrder(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	var fired []int
	for i := 0; i < 64; i++ {
		i := i
		c.Subscribe(func(int) { fired = append(fired, i) })
	}
	c.Set(1)
	if len(fired) != 64 {
		t.Fatalf("fired %d observers, want 64", len(fired))
	}
	for i, got := range fired {
		if got != i {
			t.Fatalf("observer %d fired at position %d; firing order is not registration order: %v", got, i, fired)
		}
	}
}

func TestCellObserverOrderStableAcrossRepeatedNotify(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	for i := 0; i < 32; i++ {
		i := i
		c.Subscribe(func(int) { _ = i })
	}
	var first []int
	for round := 0; round < 5; round++ {
		var fired []int
		c2 := NewCell(ctx, 0)
		for i := 0; i < 32; i++ {
			i := i
			c2.Subscribe(func(int) { fired = append(fired, i) })
		}
		c2.Set(round + 1)
		if first == nil {
			first = fired
			continue
		}
		if fmt.Sprint(first) != fmt.Sprint(fired) {
			t.Fatalf("firing order changed between rounds: %v vs %v", first, fired)
		}
	}
}

func TestCellObserverBasicNotify(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 1)
	var seen []int
	c.Subscribe(func(v int) { seen = append(seen, v) })
	c.Set(2)
	c.Set(3)
	c.Set(3) // PartialEq guard suppresses
	if fmt.Sprint(seen) != "[2 3]" {
		t.Fatalf("seen = %v, want [2 3]", seen)
	}
}

func TestCellObserverDisposeStopsDelivery(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	count := 0
	dispose := c.Subscribe(func(int) { count++ })
	c.Set(1)
	dispose()
	c.Set(2)
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

func TestCellObserverDisposeIsIdempotent(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	count := 0
	keep := c.Subscribe(func(int) { count++ })
	dispose := c.Subscribe(func(int) {})
	dispose()
	dispose()
	dispose()
	c.Set(1)
	if count != 1 {
		t.Fatalf("count = %d, want 1 (repeated dispose must not evict the other observer)", count)
	}
	keep()
}

func TestCellObserverDuplicateRegistrationFiresTwice(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	count := 0
	fn := func(int) { count++ }
	c.Subscribe(fn)
	c.Subscribe(fn)
	c.Set(1)
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
}

func TestCellObserverDuplicateRegistrationDisposesIndependently(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	count := 0
	fn := func(int) { count++ }
	first := c.Subscribe(fn)
	c.Subscribe(fn)
	first()
	c.Set(1)
	if count != 1 {
		t.Fatalf("count = %d, want 1 (disposing one registration must not remove the other)", count)
	}
}

func TestCellObserverSubscribeDuringNotifyDefersToNextNotify(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	inner := 0
	var once bool
	c.Subscribe(func(int) {
		if once {
			return
		}
		once = true
		c.Subscribe(func(int) { inner++ })
	})
	c.Set(1)
	if inner != 0 {
		t.Fatalf("inner = %d, want 0 (a reentrant subscribe must not fire in the in-flight notification)", inner)
	}
	c.Set(2)
	if inner != 1 {
		t.Fatalf("inner = %d, want 1", inner)
	}
}

// TestCellObserverDisposeDuringNotifySkipsInFlight pins the migrated behavior:
// an observer disposed from inside an earlier callback is NOT invoked by the
// notification in flight, even though the loop had not yet reached it.
//
// This test previously asserted the opposite (as
// TestCellObserverDisposeDuringNotifyStillFiresInFlight), which was lazily-go's
// entry in the lazily-spec "Known divergences" table: a stable pre-notification
// snapshot delivered one final call to an observer that had asked to stop. The
// family settled against that — see the rationale under "Unsubscribing during a
// notification takes effect immediately" in docs/reactive-graph.md — because in
// a manually-managed binding unsubscribe is routinely the step immediately
// before freeing the state the callback reads.
//
// The canonical assertion lives in the fixture runner
// (observer_conformance_test.go); this keeps the unit-level pin alongside the
// other Cell observer tests.
func TestCellObserverDisposeDuringNotifySkipsInFlight(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	later := 0
	var disposeLater func()
	c.Subscribe(func(int) { disposeLater() })
	disposeLater = c.Subscribe(func(int) { later++ })
	c.Set(1)
	if later != 0 {
		t.Fatalf("later = %d, want 0 (an observer disposed mid-notification must not be invoked by the pass that removed it)", later)
	}
	c.Set(2)
	if later != 0 {
		t.Fatalf("later = %d, want 0 after the second Set", later)
	}
}

func TestCellObserverSelfDisposeDuringNotify(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	count := 0
	var self func()
	self = c.Subscribe(func(int) {
		count++
		self()
	})
	c.Set(1)
	c.Set(2)
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

func TestCellObserverFullTeardownThenResubscribe(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	for round := 0; round < 4; round++ {
		count := 0
		var disposers []func()
		for i := 0; i < 8; i++ {
			disposers = append(disposers, c.Subscribe(func(int) { count++ }))
		}
		c.Set(round + 1)
		if count != 8 {
			t.Fatalf("round %d: count = %d, want 8", round, count)
		}
		for _, d := range disposers {
			d()
		}
		c.Set(100 + round)
		if count != 8 {
			t.Fatalf("round %d: count = %d after teardown, want 8", round, count)
		}
	}
}

// Interleaved churn that never reaches zero live observers, so it drives the
// compaction path rather than the drop-the-store path.
func TestCellObserverInterleavedChurnDeliversToSurvivors(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	hits := map[int]int{}
	survivors := map[int]func(){}
	for i := 0; i < 4; i++ {
		i := i
		survivors[i] = c.Subscribe(func(int) { hits[i]++ })
	}
	for round := 0; round < 400; round++ {
		id := 1000 + round
		d := c.Subscribe(func(int) { hits[id]++ })
		c.Set(round + 1)
		d()
	}
	for i := 0; i < 4; i++ {
		if hits[i] != 400 {
			t.Fatalf("survivor %d got %d notifications, want 400", i, hits[i])
		}
	}
	// Every transient observer saw exactly the one Set it was live for.
	for round := 0; round < 400; round++ {
		if hits[1000+round] != 1 {
			t.Fatalf("transient %d got %d notifications, want 1", round, hits[1000+round])
		}
	}
	for _, d := range survivors {
		d()
	}
	c.Set(9999)
	for i := 0; i < 4; i++ {
		if hits[i] != 400 {
			t.Fatalf("survivor %d fired after dispose", i)
		}
	}
}

// A compaction that rewrites slot indices must leave every outstanding disposer
// valid — this is the invariant that makes the index-carrying slot safe.
func TestCellObserverDisposersSurviveCompaction(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	const n = 256
	counts := make([]int, n)
	disposers := make([]func(), n)
	for i := 0; i < n; i++ {
		i := i
		disposers[i] = c.Subscribe(func(int) { counts[i]++ })
	}
	// Drop every odd observer: crosses the compaction threshold repeatedly.
	for i := 1; i < n; i += 2 {
		disposers[i]()
	}
	c.Set(1)
	for i := 0; i < n; i++ {
		want := 0
		if i%2 == 0 {
			want = 1
		}
		if counts[i] != want {
			t.Fatalf("observer %d: count = %d, want %d", i, counts[i], want)
		}
	}
	// The surviving disposers must still address their own slots after the
	// index rewrite.
	for i := 0; i < n; i += 2 {
		disposers[i]()
	}
	c.Set(2)
	for i := 0; i < n; i++ {
		want := 0
		if i%2 == 0 {
			want = 1
		}
		if counts[i] != want {
			t.Fatalf("observer %d fired after dispose: count = %d, want %d", i, counts[i], want)
		}
	}
}

func TestCellObserverSurvivorsKeepRegistrationOrderAfterCompaction(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	const n = 128
	var fired []int
	disposers := make([]func(), n)
	for i := 0; i < n; i++ {
		i := i
		disposers[i] = c.Subscribe(func(int) { fired = append(fired, i) })
	}
	for i := 0; i < n; i++ {
		if i%3 != 0 {
			disposers[i]()
		}
	}
	c.Set(1)
	prev := -1
	for _, got := range fired {
		if got <= prev {
			t.Fatalf("compaction reordered survivors: %v", fired)
		}
		prev = got
	}
	if len(fired) != (n+2)/3 {
		t.Fatalf("fired %d survivors, want %d", len(fired), (n+2)/3)
	}
}

func TestCellObserverNoNotifyWithoutSubscribers(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	c.Set(1)
	c.Set(2)
	if c.Get() != 2 {
		t.Fatalf("Get = %d, want 2", c.Get())
	}
}
