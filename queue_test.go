package lazily

// Reactive-queue conformance suite + unit coverage.
//
// Replays the canonical lazily-spec QueueCell fixtures identically to every
// other binding (lazily-rs tests/queue_conformance.rs, lazily-kt
// QueueCellConformanceTest.kt, lazily-cpp test_queue.cpp, lazily-js
// queue.test.js, lazily-zig queue.zig). Each fixture is COMPUTE: load the
// initial state, replay each step's op, and assert the observable effects —
// reader-kind invalidation (head/len/is_empty/is_full/closed), FIFO order, the
// closure lifecycle (drain on closed, Closed distinct from Empty, idempotent
// terminal close), MPSC per-producer FIFO via batch, and bounded backpressure
// reactivity.
//
// Mirrors the Rust harness: readers are no-op computed slots that read exactly
// one reader-kind cell, observed via Slot.Peek()'s cached flag (warm ==
// survived, cold == invalidated). A reader kind absent from a fixture's
// `invalidates` is not asserted (single-kind fixtures declare only the kind
// under test). Fixtures resolve via loadCollectionFixture (sibling to
// lazily-spec); absent fixtures cause a t.Skip rather than a failure.

import (
	"reflect"
	"testing"
)

// queueReaders holds the five reader-kind observer slots. Each reads exactly
// one reader-kind cell so its cached/uncached state reports that kind's
// invalidation in isolation.
type queueReaders struct {
	head     *FormulaCell[struct{}]
	length   *FormulaCell[int]
	isEmpty  *FormulaCell[bool]
	isFull   *FormulaCell[bool]
	isClosed *FormulaCell[bool]
}

// makeQueueReaders primes one observer slot per reader kind against q.
func makeQueueReaders[T comparable, S QueueStorage[T]](ctx *Context, q *QueueCell[T, S]) queueReaders {
	r := queueReaders{
		head:     NewFormulaCell(ctx, func(*Context) struct{} { _, _ = q.Head(); return struct{}{} }),
		length:   NewFormulaCell(ctx, func(*Context) int { return q.Len() }),
		isEmpty:  NewFormulaCell(ctx, func(*Context) bool { return q.IsEmpty() }),
		isFull:   NewFormulaCell(ctx, func(*Context) bool { return q.IsFull() }),
		isClosed: NewFormulaCell(ctx, func(*Context) bool { return q.IsClosed() }),
	}
	r.head.Get()
	r.length.Get()
	r.isEmpty.Get()
	r.isFull.Get()
	r.isClosed.Get()
	return r
}

// materializeQueueReaders re-caches every reader so the next step measures
// invalidation in isolation.
func materializeQueueReaders(r queueReaders) {
	r.head.Get()
	r.length.Get()
	r.isEmpty.Get()
	r.isFull.Get()
	r.isClosed.Get()
}

// assertQueueInvalidation checks each reader kind present in the fixture's
// `invalidates` map. A kind absent from the fixture is not asserted (matches
// the Rust/Kotlin/C++ harnesses — single-kind fixtures declare only the kind
// under test).
func assertQueueInvalidation(t *testing.T, name string, step int, opType string, r queueReaders, invalidates map[string]any) {
	t.Helper()
	checkHead := func() {
		wantInv, declared := invalidates["head"]
		if !declared {
			return
		}
		_, warm := r.head.Peek()
		expected := wantInv == true
		if expected && warm {
			t.Errorf("%s step %d %s: reader head should have been invalidated but stayed cached", name, step, opType)
		} else if !expected && !warm {
			t.Errorf("%s step %d %s: reader head should have stayed cached but was invalidated", name, step, opType)
		}
	}
	checkLen := func() {
		wantInv, declared := invalidates["len"]
		if !declared {
			return
		}
		_, warm := r.length.Peek()
		expected := wantInv == true
		if expected && warm {
			t.Errorf("%s step %d %s: reader len should have been invalidated but stayed cached", name, step, opType)
		} else if !expected && !warm {
			t.Errorf("%s step %d %s: reader len should have stayed cached but was invalidated", name, step, opType)
		}
	}
	checkBool := func(kind string, reader interface {
		Peek() (bool, bool)
	}) {
		wantInv, declared := invalidates[kind]
		if !declared {
			return
		}
		_, warm := reader.Peek()
		expected := wantInv == true
		if expected && warm {
			t.Errorf("%s step %d %s: reader %q should have been invalidated but stayed cached", name, step, opType, kind)
		} else if !expected && !warm {
			t.Errorf("%s step %d %s: reader %q should have stayed cached but was invalidated", name, step, opType, kind)
		}
	}
	checkHead()
	checkLen()
	checkBool("is_empty", r.isEmpty)
	checkBool("is_full", r.isFull)
	checkBool("closed", r.isClosed)
}

// assertQueueState checks the post-op state declared in expected (each field
// optional — only the declared fields are asserted).
func assertQueueState(t *testing.T, name string, step int, opType string, q *QueueCell[string, *VecDequeStorage[string]], expected map[string]any) {
	t.Helper()
	if elems, ok := expected["elements"]; ok {
		if got := q.Storage().Elements(); !reflect.DeepEqual(got, jsStrList(elems)) {
			t.Errorf("%s step %d %s: elements = %v, want %v", name, step, opType, got, jsStrList(elems))
		}
	}
	if head, ok := expected["head"]; ok {
		got, gotOk := q.Head()
		if head == nil {
			if gotOk {
				t.Errorf("%s step %d %s: head = %q, want none", name, step, opType, got)
			}
		} else if !gotOk || got != jsStr(head) {
			t.Errorf("%s step %d %s: head = %q (ok=%v), want %q", name, step, opType, got, gotOk, jsStr(head))
		}
	}
	if l, ok := expected["len"]; ok {
		if got := q.Len(); got != jsInt(l) {
			t.Errorf("%s step %d %s: len = %d, want %d", name, step, opType, got, jsInt(l))
		}
	}
	if e, ok := expected["is_empty"]; ok {
		if got := q.IsEmpty(); got != (e == true) {
			t.Errorf("%s step %d %s: is_empty = %v, want %v", name, step, opType, got, e == true)
		}
	}
	if f, ok := expected["is_full"]; ok {
		if got := q.IsFull(); got != (f == true) {
			t.Errorf("%s step %d %s: is_full = %v, want %v", name, step, opType, got, f == true)
		}
	}
	if c, ok := expected["closed"]; ok {
		if got := q.IsClosed(); got != (c == true) {
			t.Errorf("%s step %d %s: closed = %v, want %v", name, step, opType, got, c == true)
		}
	}
}

// runQueueFixture replays one queuecell_*.json fixture.
func runQueueFixture(t *testing.T, name string) {
	fixture, ok := loadCollectionFixture(t, name)
	if !ok {
		return
	}
	ctx := NewContext()

	// Build the queue from initial state.
	initial := jsMap(fixture["initial"])
	var q *QueueCell[string, *VecDequeStorage[string]]
	if cap, bounded := initial["capacity"]; bounded && cap != nil {
		q = NewBoundedQueueCell[string](ctx, jsInt(cap))
	} else {
		q = NewQueueCell[string](ctx)
	}
	// Seed initial elements in FIFO order.
	for _, e := range jsStrList(initial["elements"]) {
		if err := q.TryPush(e); !err.Ok() {
			t.Fatalf("%s: seed push %q: %v", name, e, err)
		}
	}

	for i, rawStep := range jsList(fixture["steps"]) {
		step := jsMap(rawStep)
		op := jsMap(step["op"])
		opType := jsStr(op["type"])
		expected := jsMap(step["expected"])
		invalidates := jsMap(expected["invalidates"])
		returns, hasReturns := step["returns"]

		// Prime readers against current state so this step's invalidation is
		// measured in isolation (matches the Rust/Kotlin harnesses).
		readers := makeQueueReaders(ctx, q)

		// Dispatch the op and capture the observable return. `push`/`try_push`
		// and `pop`/`try_pop` map to the same Try* call; the `returns` field
		// (when present) is what distinguishes a success from an error label.
		var gotReturn string
		switch opType {
		case "push", "try_push":
			gotReturn = q.TryPush(jsStr(op["value"])).String()
		case "pop", "try_pop":
			v, err := q.TryPop()
			if err.Ok() {
				gotReturn = v
			} else {
				gotReturn = err.String()
			}
		case "close":
			q.Close()
		case "batch":
			ops := jsList(op["ops"])
			ctx.Batch(func() {
				for _, inner := range ops {
					io := jsMap(inner)
					if jsStr(io["type"]) != "push" {
						t.Fatalf("%s step %d batch: only push is supported inside batch, got %q", name, i, jsStr(io["type"]))
					}
					if err := q.TryPush(jsStr(io["value"])); !err.Ok() {
						t.Fatalf("%s step %d batch push: %v", name, i, err)
					}
				}
			})
		default:
			t.Fatalf("%s step %d: unknown op %q", name, i, opType)
		}

		// returns (when declared): element string or error label.
		if hasReturns {
			if returns == nil {
				// success/no value — nothing concrete to compare (push/close).
			} else if gotReturn != jsStr(returns) {
				t.Errorf("%s step %d %s: returns = %q, want %q", name, i, opType, gotReturn, jsStr(returns))
			}
		}

		assertQueueState(t, name, i, opType, q, expected)
		assertQueueInvalidation(t, name, i, opType, readers, invalidates)
		materializeQueueReaders(readers)
	}
}

func TestQueueConformanceSPSCPushPop(t *testing.T) {
	runQueueFixture(t, "queuecell_spsc_push_pop.json")
}

func TestQueueConformancePoppedHeadObservation(t *testing.T) {
	runQueueFixture(t, "queuecell_popped_head_observation.json")
}

func TestQueueConformanceMPSCMultiWriter(t *testing.T) {
	runQueueFixture(t, "queuecell_mpsc_multi_writer.json")
}

func TestQueueConformanceBoundedBackpressure(t *testing.T) {
	runQueueFixture(t, "queuecell_bounded_backpressure.json")
}

func TestQueueConformanceClosureLifecycle(t *testing.T) {
	runQueueFixture(t, "queuecell_closure_lifecycle.json")
}

// ---------------------------------------------------------------------------
// Direct unit tests (the signature properties beyond fixture replay)
// ---------------------------------------------------------------------------

// SPSC total FIFO: pop order exactly matches push order.
func TestQueueSPSCFIFOOrder(t *testing.T) {
	ctx := NewContext()
	q := NewQueueCell[string](ctx)
	for _, v := range []string{"a", "b", "c", "d"} {
		if err := q.TryPush(v); !err.Ok() {
			t.Fatalf("push %q: %v", v, err)
		}
	}
	var got []string
	for {
		v, err := q.TryPop()
		if !err.Ok() {
			break
		}
		got = append(got, v)
	}
	want := []string{"a", "b", "c", "d"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("drain order = %v, want %v", got, want)
	}
}

// Closure lifecycle: drain on closed, Closed distinct from Empty, push after
// close rejected, close idempotent + terminal.
func TestQueueClosureLifecycle(t *testing.T) {
	ctx := NewContext()
	q := NewQueueCell[int](ctx)
	q.TryPush(1)
	q.TryPush(2)

	// Pop on open non-empty drains.
	if v, err := q.TryPop(); !err.Ok() || v != 1 {
		t.Fatalf("first pop = (%d, %v), want (1, Ok)", v, err)
	}

	q.Close()

	// Pop on closed non-empty keeps draining.
	if v, err := q.TryPop(); !err.Ok() || v != 2 {
		t.Fatalf("pop on closed non-empty = (%d, %v), want (2, Ok)", v, err)
	}
	// Pop on closed empty returns Closed (distinct from Empty).
	if _, err := q.TryPop(); err != QueuePopClosed {
		t.Fatalf("pop on closed empty = %v, want %v", err, QueuePopClosed)
	}
	// Push on closed is rejected.
	if err := q.TryPush(3); err != QueuePushClosed {
		t.Fatalf("push on closed = %v, want %v", err, QueuePushClosed)
	}
	// Close is idempotent and invalidates nothing.
	headReader := NewFormulaCell(ctx, func(*Context) struct{} { _, _ = q.Head(); return struct{}{} })
	headReader.Get()
	closedReader := NewFormulaCell(ctx, func(*Context) bool { return q.IsClosed() })
	closedReader.Get()
	q.Close()
	if _, warm := closedReader.Peek(); !warm {
		t.Errorf("second close invalidated closed reader (should be idempotent)")
	}
	if _, warm := headReader.Peek(); !warm {
		t.Errorf("second close invalidated head reader")
	}
}

// Pop on an open empty queue returns Empty (not Closed).
func TestQueueOpenEmptyReturnsEmpty(t *testing.T) {
	ctx := NewContext()
	q := NewQueueCell[string](ctx)
	if _, err := q.TryPop(); err != QueuePopEmpty {
		t.Fatalf("pop on open empty = %v, want %v", err, QueuePopEmpty)
	}
}

// Reader-kind independence: a push to a non-empty queue does NOT invalidate the
// head reader, but does invalidate len/is_empty. (push_nonempty_preserves_head.)
func TestQueueReaderKindIndependence(t *testing.T) {
	ctx := NewContext()
	q := NewQueueCell[string](ctx)
	q.TryPush("a")

	head := NewFormulaCell(ctx, func(*Context) struct{} { _, _ = q.Head(); return struct{}{} })
	length := NewFormulaCell(ctx, func(*Context) int { return q.Len() })
	empty := NewFormulaCell(ctx, func(*Context) bool { return q.IsEmpty() })
	head.Get()
	length.Get()
	empty.Get()

	q.TryPush("b") // head unchanged ("a" still at head)

	if _, warm := head.Peek(); !warm {
		t.Errorf("head reader invalidated by push to non-empty (should be preserved)")
	}
	if _, warm := length.Peek(); warm {
		t.Errorf("len reader not invalidated by push")
	}
	if _, warm := empty.Peek(); !warm {
		t.Errorf("is_empty reader invalidated by push to non-empty (was false, still false — should NOT invalidate)")
	}
}

// Backpressure reactivity: a consumer pop that makes room wakes a push-side
// effect observing is_full. The signature queue property.
func TestQueueBackpressurePopWakesPushEffect(t *testing.T) {
	ctx := NewContext()
	q := NewBoundedQueueCell[string](ctx, 1)

	runs := 0
	var lastFull *bool
	NewEffect(ctx, func(*Context) func() {
		f := q.IsFull()
		lastFull = &f
		runs++
		return nil
	})
	if runs != 1 {
		t.Fatalf("effect initial runs = %d, want 1", runs)
	}
	if *lastFull {
		t.Fatalf("initial is_full = true, want false (empty bounded queue)")
	}

	// Fill the queue: is_full flips true → effect reruns.
	if err := q.TryPush("x"); !err.Ok() {
		t.Fatalf("push x: %v", err)
	}
	if runs != 2 || !*lastFull {
		t.Fatalf("after fill: runs=%d is_full=%v, want 2 and true", runs, *lastFull)
	}
	// Push at capacity is rejected and invalidates nothing.
	if err := q.TryPush("y"); err != QueuePushFull {
		t.Fatalf("push at capacity = %v, want %v", err, QueuePushFull)
	}
	if runs != 2 {
		t.Fatalf("Full push reran effect: runs=%d, want 2", runs)
	}
	// Consumer pop makes room → is_full flips false → effect reruns (backpressure lift).
	if v, err := q.TryPop(); !err.Ok() || v != "x" {
		t.Fatalf("pop = (%q, %v), want (x, Ok)", v, err)
	}
	if runs != 3 || *lastFull {
		t.Fatalf("after pop at capacity: runs=%d is_full=%v, want 3 and false", runs, *lastFull)
	}
}

// MPSC via batch: multiple producers inside one Context.Batch appear as one
// coalesced transition (per-producer FIFO, one invalidation pass).
func TestQueueMPSCViaBatchIsOneInvalidationPass(t *testing.T) {
	ctx := NewContext()
	q := NewQueueCell[int](ctx)

	length := NewFormulaCell(ctx, func(*Context) int { return q.Len() })
	length.Get()

	// Three pushes from "different producers" in one batch.
	ctx.Batch(func() {
		q.TryPush(10)
		q.TryPush(20)
		q.TryPush(30)
	})
	// The batch coalesces: the len reader was invalidated exactly once across
	// the three pushes (recompute below yields the final state, not three
	// intermediate recomputes).
	if got := q.Len(); got != 3 {
		t.Fatalf("len after batch = %d, want 3", got)
	}
	// Per-producer FIFO: drain yields the batch push order.
	var got []int
	for q.Len() > 0 {
		v, _ := q.TryPop()
		got = append(got, v)
	}
	want := []int{10, 20, 30}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("drain after MPSC batch = %v, want %v", got, want)
	}
}

// Pluggable storage: a custom QueueStorage backend works through the shell.
// A fixed-capacity ring that drops nothing and reports its bound.
type ringStorage[T any] struct {
	buf      []T
	head     int
	count    int
	capacity int
	closed   bool
}

func newRingStorage[T any](capacity int) *ringStorage[T] {
	return &ringStorage[T]{buf: make([]T, capacity), capacity: capacity}
}

func (r *ringStorage[T]) TryPush(value T) QueuePushError {
	if r.closed {
		return QueuePushClosed
	}
	if r.count >= r.capacity {
		return QueuePushFull
	}
	tail := (r.head + r.count) % r.capacity
	r.buf[tail] = value
	r.count++
	return QueuePushOk
}

func (r *ringStorage[T]) TryPop() (T, QueuePopError) {
	var zero T
	if r.count == 0 {
		if r.closed {
			return zero, QueuePopClosed
		}
		return zero, QueuePopEmpty
	}
	v := r.buf[r.head]
	r.buf[r.head] = zero
	r.head = (r.head + 1) % r.capacity
	r.count--
	return v, QueuePopOk
}

func (r *ringStorage[T]) Peek() (T, bool) {
	var zero T
	if r.count == 0 {
		return zero, false
	}
	return r.buf[r.head], true
}

func (r *ringStorage[T]) Len() int              { return r.count }
func (r *ringStorage[T]) Capacity() (int, bool) { return r.capacity, true }
func (r *ringStorage[T]) IsClosed() bool        { return r.closed }
func (r *ringStorage[T]) Close()                { r.closed = true }

// A custom backend (ring buffer) drives the shell with the same observable
// FIFO + backpressure contract as VecDequeStorage. Ring-slot wraparound must
// not cause spurious invalidations.
func TestQueuePluggableStorageRingBackend(t *testing.T) {
	ctx := NewContext()
	ring := newRingStorage[string](2)
	q := NewQueueCellWithStorage[string](ctx, ring)

	if cap, ok := q.Capacity(); !ok || cap != 2 {
		t.Fatalf("capacity = (%d, %v), want (2, true)", cap, ok)
	}
	// Fill, overflow, pop, refill — round-trips through the ring slots.
	q.TryPush("a")
	q.TryPush("b")
	if err := q.TryPush("c"); err != QueuePushFull {
		t.Fatalf("push at capacity = %v, want %v", err, QueuePushFull)
	}
	if v, _ := q.TryPop(); v != "a" {
		t.Fatalf("first pop = %q, want a (FIFO)", v)
	}
	q.TryPush("c") // wraps into the freed slot
	// is_full reflects the wrap correctly.
	if !q.IsFull() {
		t.Errorf("is_full after wrap-refill = false, want true")
	}
	if v, _ := q.TryPop(); v != "b" {
		t.Fatalf("second pop = %q, want b", v)
	}
	if v, _ := q.TryPop(); v != "c" {
		t.Fatalf("third pop = %q, want c", v)
	}
	if _, err := q.TryPop(); err != QueuePopEmpty {
		t.Fatalf("pop on open empty = %v, want %v", err, QueuePopEmpty)
	}
}

// VecDequeStorage.Elements() returns FIFO order (the snapshot shape).
func TestQueueVecDequeStorageElementsIsFIFOOrder(t *testing.T) {
	s := NewVecDequeStorage[int]()
	for _, v := range []int{1, 2, 3} {
		s.TryPush(v)
	}
	if got := s.Elements(); !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Fatalf("Elements() = %v, want [1 2 3]", got)
	}
	s.TryPop()
	if got := s.Elements(); !reflect.DeepEqual(got, []int{2, 3}) {
		t.Fatalf("Elements() after pop = %v, want [2 3]", got)
	}
}

// NewBoundedVecDequeStorage panics on non-positive capacity.
func TestQueueBoundedStorageRejectsZeroCapacity(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for zero capacity")
		}
	}()
	NewBoundedVecDequeStorage[int](0)
}

// ReaderHandles exposes the underlying cells for advanced wiring.
func TestQueueReaderHandlesRoundTrip(t *testing.T) {
	ctx := NewContext()
	q := NewQueueCell[string](ctx)
	h := q.ReaderHandles()
	if h.Len != q.lenCell || h.IsEmpty != q.empty || h.IsFull != q.full ||
		h.IsClosed != q.closed || h.Head != q.head {
		t.Fatal("ReaderHandles returned mismatched cells")
	}
}

// minimalFifoStorage implements ONLY the required QueueStorage contract —
// TryPush / TryPop / Len / IsClosed / Close — with no Peek and no Capacity. It
// is the shape of a raw channel; the shell must treat it as fully conforming
// (Phase 0 #relaycell): no Head reader, never full.
type minimalFifoStorage[T any] struct {
	elems  []T
	closed bool
}

func (s *minimalFifoStorage[T]) TryPush(v T) QueuePushError {
	if s.closed {
		return QueuePushClosed
	}
	s.elems = append(s.elems, v)
	return QueuePushOk
}

func (s *minimalFifoStorage[T]) TryPop() (T, QueuePopError) {
	var zero T
	if len(s.elems) == 0 {
		if s.closed {
			return zero, QueuePopClosed
		}
		return zero, QueuePopEmpty
	}
	v := s.elems[0]
	s.elems = s.elems[1:]
	return v, QueuePopOk
}

func (s *minimalFifoStorage[T]) Len() int       { return len(s.elems) }
func (s *minimalFifoStorage[T]) IsClosed() bool { return s.closed }
func (s *minimalFifoStorage[T]) Close()         { s.closed = true }

// (no Peek, no Capacity)

func TestQueueRawChannelBackendConformsToMinimalContract(t *testing.T) {
	ctx := NewContext()
	q := NewQueueCellWithStorage[int](ctx, &minimalFifoStorage[int]{})

	if !q.IsEmpty() {
		t.Fatalf("new minimal queue not empty")
	}
	if err := q.TryPush(1); !err.Ok() {
		t.Fatalf("push 1: %v", err)
	}
	q.TryPush(2)
	if q.Len() != 2 {
		t.Fatalf("len = %d, want 2", q.Len())
	}

	// No Peek capability → no Head reader (zero, false); no Capacity → never full.
	if v, ok := q.Head(); ok || v != 0 {
		t.Fatalf("Head() = (%d, %v), want (0, false) — no peek capability", v, ok)
	}
	if q.IsFull() {
		t.Fatalf("IsFull() = true, want false (unbounded)")
	}
	if _, ok := q.Capacity(); ok {
		t.Fatalf("Capacity() reported bounded, want unbounded")
	}

	// FIFO drain from TryPop alone.
	if v, _ := q.TryPop(); v != 1 {
		t.Fatalf("pop = %d, want 1", v)
	}
	if v, _ := q.TryPop(); v != 2 {
		t.Fatalf("pop = %d, want 2", v)
	}
	if !q.IsEmpty() {
		t.Fatalf("queue not empty after draining")
	}

	// Closure lifecycle: Closed distinct from Empty; push-after-close rejected.
	q.Close()
	if !q.IsClosed() {
		t.Fatalf("not closed after Close()")
	}
	if err := q.TryPush(3); err != QueuePushClosed {
		t.Fatalf("push after close = %v, want %v", err, QueuePushClosed)
	}
	if _, err := q.TryPop(); err != QueuePopClosed {
		t.Fatalf("pop on closed empty = %v, want %v", err, QueuePopClosed)
	}
}

// A subscribed reader over the minimal backend stays reactive (demand-driven
// len Slot invalidates each op) even without Peek/Capacity.
func TestQueueRawChannelReaderKindsStayReactive(t *testing.T) {
	ctx := NewContext()
	q := NewQueueCellWithStorage[int](ctx, &minimalFifoStorage[int]{})

	var log []int
	NewEffect(ctx, func(*Context) func() {
		log = append(log, q.Len())
		return nil
	})
	if got := []int{0}; !intsEqual(log, got) {
		t.Fatalf("after setup log=%v, want %v", log, got)
	}
	q.TryPush(10)
	if got := []int{0, 1}; !intsEqual(log, got) {
		t.Fatalf("after push log=%v, want %v", log, got)
	}
	q.TryPop()
	if got := []int{0, 1, 0}; !intsEqual(log, got) {
		t.Fatalf("after pop log=%v, want %v", log, got)
	}
}

func intsEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
