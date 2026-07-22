package lazily

import (
	"reflect"
	"sync"
	"testing"
)

// ThreadSafeContext conformance (lazily-py ThreadSafeContext, LazilyFormal
// ThreadSafe). Concurrent writers linearized by the lock converge to a
// deterministic final state; the pure ApplyBatch/FlushBatch kernels coalesce
// the invalidation frontier. The concurrent tests run clean under -race.

// Concurrent read-modify-write increments under WithLock converge to the exact
// total, independent of goroutine interleaving.
func TestThreadSafeConcurrentIncrementsConverge(t *testing.T) {
	tsc := NewThreadSafeContext()
	var counter *Source[int]
	tsc.WithLock(func(ctx *Context) { counter = NewSource(ctx, 0) })

	const goroutines, perGoroutine = 8, 500
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				tsc.WithLock(func(ctx *Context) {
					counter.Set(counter.Peek() + 1)
				})
			}
		}()
	}
	wg.Wait()

	got := Read(tsc, func(ctx *Context) int { return counter.Peek() })
	if want := goroutines * perGoroutine; got != want {
		t.Fatalf("counter = %d, want %d", got, want)
	}
}

// Concurrent TSSetCell writes to distinct cells converge: a slot summing them,
// read under the lock, sees every write deterministically.
func TestThreadSafeConcurrentSetCellsConverge(t *testing.T) {
	tsc := NewThreadSafeContext()
	const n = 16
	var cells [n]*Source[int]
	var sum *Computed[int]
	tsc.WithLock(func(ctx *Context) {
		for i := range cells {
			cells[i] = NewSource(ctx, 0)
		}
		sum = NewSlot(ctx, func(cv *Compute) int {
			total := 0
			for _, c := range cells {
				total += Get(cv, c)
			}
			return total
		})
	})

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			TSSetCell(tsc, cells[i], (i+1)*10)
		}(i)
	}
	wg.Wait()

	got := Read(tsc, func(*Context) int { return sum.Get() })
	want := 0
	for i := 0; i < n; i++ {
		want += (i + 1) * 10
	}
	if got != want {
		t.Fatalf("sum = %d, want %d", got, want)
	}
}

// A Batch under the lock coalesces its cell writes into a single effect rerun.
func TestThreadSafeBatchCoalesces(t *testing.T) {
	tsc := NewThreadSafeContext()
	var a, b *Source[int]
	runs := 0
	tsc.WithLock(func(ctx *Context) {
		a = NewSource(ctx, 1)
		b = NewSource(ctx, 2)
		NewEffect(ctx, func(c *Compute) func() {
			_ = Get(c, a)
			_ = Get(c, b)
			runs++
			return nil
		})
	})
	if got := Read(tsc, func(*Context) int { return runs }); got != 1 {
		t.Fatalf("runs = %d, want 1", got)
	}
	tsc.Batch(func() {
		a.Set(10)
		b.Set(20)
	})
	if got := Read(tsc, func(*Context) int { return runs }); got != 2 {
		t.Fatalf("runs after batch = %d, want 2 (coalesced)", got)
	}
}

// A single TSSetCell outside a batch is observationally identical to Cell.Set:
// it fires the effect exactly once.
func TestThreadSafeSingleWriteEqualsCellSet(t *testing.T) {
	tsc := NewThreadSafeContext()
	var a *Source[int]
	runs := 0
	tsc.WithLock(func(ctx *Context) {
		a = NewSource(ctx, 1)
		NewEffect(ctx, func(c *Compute) func() {
			_ = Get(c, a)
			runs++
			return nil
		})
	})
	TSSetCell(tsc, a, 2)
	if got := Read(tsc, func(*Context) int { return runs }); got != 2 {
		t.Fatalf("runs after single set = %d, want 2", got)
	}
	// Equal write is absorbed (PartialEq guard) — no rerun.
	TSSetCell(tsc, a, 2)
	if got := Read(tsc, func(*Context) int { return runs }); got != 2 {
		t.Fatalf("runs after equal set = %d, want 2", got)
	}
}

// --- pure batch-flush kernel (LazilyFormal.ThreadSafe port) ----------------

func TestThreadSafeApplyBatchGuardAndDedup(t *testing.T) {
	nodes := map[any]NodeEntry{
		"a": {Value: 1, State: "clean"},
		"b": {Value: 2, State: "clean"},
	}
	batch := []BatchWrite{
		{NodeID: "a", Value: 1}, // equal — absorbed by the PartialEq guard
		{NodeID: "a", Value: 5}, // changes a
		{NodeID: "b", Value: 2}, // equal — absorbed
		{NodeID: "a", Value: 5}, // no-op (already 5) — not double-listed
	}
	next, changed := ApplyBatch(nodes, batch)

	if len(changed) != 1 || changed[0] != "a" {
		t.Fatalf("changed = %v, want [a]", changed)
	}
	if got := next["a"]; got.Value != 5 || got.State != "dirty" {
		t.Fatalf("next[a] = %+v, want {5 dirty}", got)
	}
	if got := next["b"]; got.Value != 2 || got.State != "clean" {
		t.Fatalf("next[b] = %+v, want {2 clean}", got)
	}
	// Input map is not mutated (ApplyBatch copies).
	if got := nodes["a"]; got.Value != 1 || got.State != "clean" {
		t.Fatalf("input nodes[a] mutated: %+v", got)
	}
}

func TestThreadSafeApplyBatchMultipleChangesDedup(t *testing.T) {
	nodes := map[any]NodeEntry{"a": {Value: 0, State: "clean"}}
	batch := []BatchWrite{{NodeID: "a", Value: 5}, {NodeID: "a", Value: 6}}
	next, changed := ApplyBatch(nodes, batch)
	if len(changed) != 1 || changed[0] != "a" {
		t.Fatalf("changed = %v, want [a] (source listed once)", changed)
	}
	if got := next["a"]; got.Value != 6 {
		t.Fatalf("next[a].Value = %v, want 6 (last write wins)", got.Value)
	}
}

func TestThreadSafeFlushBatchCoalescedFrontier(t *testing.T) {
	nodes := map[any]NodeEntry{
		"a": {Value: 1, State: "clean"},
		"b": {Value: 2, State: "clean"},
		"x": {Value: 0, State: "clean"},
		"y": {Value: 0, State: "clean"},
	}
	dependents := map[any][]any{
		"a": {"x", "y"},
		"b": {"y"}, // y is a shared dependent of a and b
	}
	batch := []BatchWrite{{NodeID: "a", Value: 10}, {NodeID: "b", Value: 20}}
	next := FlushBatch(nodes, dependents, batch)

	if got := next["a"]; got.Value != 10 || got.State != "dirty" {
		t.Fatalf("next[a] = %+v, want {10 dirty}", got)
	}
	if got := next["b"]; got.Value != 20 || got.State != "dirty" {
		t.Fatalf("next[b] = %+v, want {20 dirty}", got)
	}
	// Both dependents marked dirty exactly once; their values are untouched.
	for _, id := range []string{"x", "y"} {
		if got := next[id]; got.State != "dirty" || got.Value != 0 {
			t.Fatalf("next[%s] = %+v, want {0 dirty}", id, got)
		}
	}
}

func TestThreadSafeUnionDependents(t *testing.T) {
	dependents := map[any][]any{
		"a": {"x", "y"},
		"b": {"y"},
	}
	got := UnionDependents(dependents, []any{"a", "b"})
	want := []any{"x", "y", "y"} // flat union, per the Lean unionDependents
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("UnionDependents = %v, want %v", got, want)
	}
}
