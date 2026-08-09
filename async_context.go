// Async reactive context — a Go port of lazily-dart's
// lib/src/async_context.dart (docs/async.md).
//
// This is a separate reactive surface for computations whose values are
// produced by blocking / future-returning functions. It is NOT an overload of
// the synchronous Context (see core.go); it is a distinct graph with its own
// handles, because async computes introduce in-flight state, cancellation,
// stale completion, and dependency tracking across suspension points that the
// synchronous graph does not have. Only resolved slot values would ever cross
// IPC/FFI — this file is compute, not protocol.
//
// # Channel-first architecture (share by communicating)
//
// The whole graph is owned by a SINGLE owner goroutine (AsyncContext.loop).
// Every graph mutation and read is serialized as a command func() sent over the
// command channel; the loop executes them one at a time, so no per-field mutex
// is needed and there are no data races on graph state. Callers that need a
// result capture it in closure variables and wait for the command to run
// (AsyncContext.do). Compute results are posted back to the loop by the compute
// goroutine (AsyncContext.post).
//
// Async compute functions themselves run in their own goroutines (they may
// block), mirroring how Dart's futures run "concurrently" on the event loop
// while all synchronous state transitions happen on the single thread between
// await points. A compute reads its dependencies through an AsyncComputeContext
// whose TrackSource / TrackComputed helpers register dependency edges (via the loop)
// before the awaited read.
//
// # Supersession (Dart's _Superseded) == context cancellation
//
// When a slot's dependency changes, the in-flight compute is superseded: its
// per-compute context.Context is cancelled and every current waiter is told to
// re-resolve (asyncResult.superseded). The re-resolve loop in
// AsyncComputed.GetAsync observes that and starts over from the current slot
// state, exactly like Dart's re-resolve loop catching _Superseded. The stale
// compute goroutine may still finish, but its completion is discarded because
// the slot's in-flight token no longer matches (identity gate in onComplete).
//
// # Lifecycle
//
// DisposeAsync / Close mark the context disposed, cancel every in-flight
// compute, deliver a disposed error to blocked waiters, run and await effect
// cleanups, then tear down the owner goroutine. No goroutine is leaked: compute
// and effect goroutines observe their cancelled context or post through a
// stop-aware channel, and blocked callers unblock on the loop's stop signal.
package lazily

import (
	"context"
	"errors"
	"reflect"
	"sync"
)

// ErrAsyncContextDisposed is returned by async reads once the owning
// AsyncContext has been disposed (Dart threw a StateError here).
var ErrAsyncContextDisposed = errors.New("lazily: async context disposed")

// AsyncComputedState is the public projection of an async computed's
// finite-state machine. The formal model retains the storage-oriented
// AsyncSlotState theorem/module name.
type AsyncComputedState string

const (
	// AsyncComputedEmpty: no cached value, no in-flight computation. Entered on
	// creation and after a hard clear.
	AsyncComputedEmpty AsyncComputedState = "empty"
	// AsyncComputedComputing: a compute is in flight for the current revision.
	// Concurrent GetAsync callers attach as waiters instead of spawning
	// duplicate computations.
	AsyncComputedComputing AsyncComputedState = "computing"
	// AsyncComputedResolved: the cached value is fresh, until dependency
	// invalidation transitions back to computing.
	AsyncComputedResolved AsyncComputedState = "resolved"
	// AsyncComputedError: the last computation failed. Waiters on that attempt
	// receive its error; the error is not cached. The next GetAsync re-spawns
	// (Error -> Computing), per docs/async.md § Async slot state machine and
	// LazilyFormal.AsyncSlotState SlotEvent.retry.
	AsyncComputedError AsyncComputedState = "error"
)

// AsyncSlotState is the deprecated v1 name for AsyncComputedState.
//
// Deprecated: use AsyncComputedState.
type AsyncSlotState = AsyncComputedState

const (
	// Deprecated: use AsyncComputedEmpty.
	AsyncSlotEmpty = AsyncComputedEmpty
	// Deprecated: use AsyncComputedComputing.
	AsyncSlotComputing = AsyncComputedComputing
	// Deprecated: use AsyncComputedResolved.
	AsyncSlotResolved = AsyncComputedResolved
	// Deprecated: use AsyncComputedError.
	AsyncSlotError = AsyncComputedError
)

// Equals is an equality predicate for async memo guards (Dart typedef Equals).
type Equals[T any] func(a, b T) bool

// asyncResult is delivered on a waiter channel when an in-flight compute
// settles or is superseded.
type asyncResult struct {
	value      any
	err        error
	superseded bool
}

// inFlight is the identity token for one compute run. onComplete only publishes
// a result whose token still matches the slot's current token; a superseded /
// cancelled run's token is replaced, so its completion is discarded.
type inFlight struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// dependent is a graph node (slot or effect) that reacts when a dependency it
// read is invalidated.
type dependent interface {
	onDepInvalidated(c *AsyncContext)
}

// depOwner is a node that accumulates dependency edges while it computes.
type depOwner interface {
	dependent
	depSet() map[any]struct{}
}

// asyncSourceNode is the loop-owned state of a mutable input cell.
type asyncSourceNode struct {
	value any
	// policy folds a Merge op into the current value. Held type-erased so this
	// loop-owned struct stays non-generic; see mergeFolder.
	policy   func(cur, op any) any
	disposed bool
}

// asyncComputedNode is the loop-owned state of a computed async slot. All fields are
// touched only inside the owner goroutine.
type asyncComputedNode struct {
	c        *AsyncContext
	compute  func(cc *AsyncComputeContext) (any, error)
	eq       func(a, b any) bool
	state    AsyncComputedState
	revision int
	value    any
	hasValue bool
	inFlight *inFlight
	waiters  []chan asyncResult
	deps     map[any]struct{}
	disposed bool
}

func (s *asyncComputedNode) onDepInvalidated(c *AsyncContext) { c.invalidateSlot(s) }
func (s *asyncComputedNode) depSet() map[any]struct{}         { return s.deps }

func (s *asyncComputedNode) removeWaiter(w chan asyncResult) {
	for i, x := range s.waiters {
		if x == w {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			return
		}
	}
}

// AsyncSource is a mutable input cell on the async graph. Reads registered
// inside an async compute/effect (via TrackSource) create a dependency edge;
// writes invalidate dependents.
type AsyncSource[T any] struct {
	c    *AsyncContext
	node *asyncSourceNode
}

// NewAsyncSource creates a mutable input cell bound to c (Dart AsyncContext.cell).
// Its writes fold under KeepLatest, which is a plain cell.
func NewAsyncSource[T any](c *AsyncContext, value T) *AsyncSource[T] {
	return NewAsyncSourceWithPolicy(c, value, KeepLatest[T]())
}

// NewAsyncSourceWithPolicy creates an input cell on the async graph whose Merge
// folds under policy — the async counterpart of NewSourceWithPolicy. With a
// policy other than KeepLatest it is the accumulator the corpus calls a merge
// cell.
func NewAsyncSourceWithPolicy[T any](c *AsyncContext, value T, policy MergePolicy[T]) *AsyncSource[T] {
	return &AsyncSource[T]{c: c, node: &asyncSourceNode{value: value, policy: mergeFolder(policy)}}
}

// mergeFolder erases MergePolicy[T] to a fold over `any` so the loop-owned node
// can hold it without the node type becoming generic. The cast is safe by
// construction: only NewAsyncSourceWithPolicy[T] installs it, and only
// AsyncSource[T].Merge calls it.
func mergeFolder[T any](policy MergePolicy[T]) func(cur, op any) any {
	return func(cur, op any) any { return policy.Merge(cur.(T), op.(T)) }
}

// Peek returns the current value without registering a dependency
// (non-reactive). Use TrackSource to read reactively inside an async compute.
func (h *AsyncSource[T]) Peek() T {
	v, err := h.TryGet()
	if err != nil {
		panic(err)
	}
	return v
}

// TryGet is the checked read: it returns a *DisposedError instead of panicking
// when this cell has been disposed.
func (h *AsyncSource[T]) TryGet() (T, error) {
	var v T
	var disposed bool
	h.c.do(func() {
		if h.node.disposed {
			disposed = true
			return
		}
		v = h.node.value.(T)
	})
	if disposed {
		var zero T
		return zero, &DisposedError{Kind: "cell"}
	}
	return v, nil
}

// Get returns the current value. It does NOT register a dependency (there is no
// ambient compute outside a goroutine in Go); use TrackSource inside an async
// compute for reactive reads. Kept for parity with the Dart surface.
func (h *AsyncSource[T]) Get() T { return h.Peek() }

// Set assigns a new value. If it differs from the current value, dependent
// async slots/effects are invalidated (or queued when inside Batch).
func (h *AsyncSource[T]) Set(value T) {
	h.c.do(func() {
		if h.c.disposed || h.node.disposed {
			return
		}
		if asyncValueEqual(h.node.value, value) {
			return
		}
		h.node.value = value
		h.c.invalidateDependents(h.node)
	})
}

// Merge folds op into the current value under this cell's policy and writes the
// result. The read-modify-write happens inside ONE loop command, so a fold is
// atomic with respect to every other graph mutation — which is exactly why it
// belongs in the library rather than a Peek-then-Set at the call site.
//
// Like Set, an equal result invalidates nothing (free dedup).
func (h *AsyncSource[T]) Merge(op T) {
	h.c.do(func() {
		if h.c.disposed || h.node.disposed {
			return
		}
		var next any = op
		if h.node.policy != nil {
			next = h.node.policy(h.node.value, op)
		}
		if asyncValueEqual(h.node.value, next) {
			return
		}
		h.node.value = next
		h.c.invalidateDependents(h.node)
	})
}

// AsyncComputed is a computed async slot: a blocking/future-returning
// computation that recomputes when its dependencies change.
type AsyncComputed[T any] struct {
	c    *AsyncContext
	node *asyncComputedNode
}

// AsyncCellHandle is the deprecated v1 name for AsyncSource.
//
// Deprecated: use AsyncSource.
type AsyncCellHandle[T any] = AsyncSource[T]

// AsyncSlotHandle is the deprecated v1 name for AsyncComputed.
//
// Deprecated: use AsyncComputed.
type AsyncSlotHandle[T any] = AsyncComputed[T]

// NewAsyncComputed creates an async computed slot (Dart AsyncContext.computedAsync).
// compute reads its dependencies through the AsyncComputeContext and returns a
// value or an error.
func NewAsyncComputed[T any](c *AsyncContext, compute func(cc *AsyncComputeContext) (T, error)) *AsyncComputed[T] {
	node := &asyncComputedNode{
		c:     c,
		state: AsyncComputedEmpty,
		deps:  map[any]struct{}{},
		compute: func(cc *AsyncComputeContext) (any, error) {
			return compute(cc)
		},
	}
	return &AsyncComputed[T]{c: c, node: node}
}

// NewAsyncComputedWithEquals is like NewAsyncComputed but with an equality memo guard: a recompute
// that yields an equal value (per eq) keeps the cached value and suppresses the
// dependency cascade (Dart AsyncContext.memoAsync).
func NewAsyncComputedWithEquals[T any](c *AsyncContext, compute func(cc *AsyncComputeContext) (T, error), eq Equals[T]) *AsyncComputed[T] {
	h := NewAsyncComputed(c, compute)
	h.node.eq = func(a, b any) bool { return eq(a.(T), b.(T)) }
	return h
}

// NewAsyncCell is the deprecated v1 source constructor.
//
// Deprecated: use NewAsyncSource.
func NewAsyncCell[T any](c *AsyncContext, value T) *AsyncSource[T] {
	return NewAsyncSource(c, value)
}

// NewAsyncSlot is the deprecated v1 computed constructor.
//
// Deprecated: use NewAsyncComputed.
func NewAsyncSlot[T any](
	c *AsyncContext,
	compute func(cc *AsyncComputeContext) (T, error),
) *AsyncComputed[T] {
	return NewAsyncComputed(c, compute)
}

// NewAsyncMemo is the deprecated guarded-computed constructor. Memo is not a
// separate node kind.
//
// Deprecated: use NewAsyncComputedWithEquals.
func NewAsyncMemo[T any](
	c *AsyncContext,
	compute func(cc *AsyncComputeContext) (T, error),
	eq Equals[T],
) *AsyncComputed[T] {
	return NewAsyncComputedWithEquals(c, compute, eq)
}

// NewAsyncComputedRippleWhen is the async mirror of NewComputedRippleWhen
// (#lzcellkernel): a guarded async computed whose downstream propagation is gated
// by an explicit, PURE predicate changed(old, next) — true propagates the
// recompute to dependents, false suppresses it. It installs the engine's equality
// guard as its negation (equal => suppress), so NewAsyncComputedWithEquals(f, eq) and
// NewAsyncComputedRippleWhen(f, func(o, n) bool { return !eq(o, n) }) are the same.
// changed MUST be pure in (old, next); value-carried state is fine, external
// mutable state is not.
func NewAsyncComputedRippleWhen[T any](c *AsyncContext, compute func(cc *AsyncComputeContext) (T, error), changed func(old, next T) bool) *AsyncComputed[T] {
	h := NewAsyncComputed(c, compute)
	h.node.eq = func(a, b any) bool { return !changed(a.(T), b.(T)) }
	return h
}

// State reports the current state-machine state.
func (s *AsyncComputed[T]) State() AsyncComputedState {
	var st AsyncComputedState
	s.c.do(func() { st = s.node.state })
	return st
}

// Revision reports the current revision (incremented on each invalidation; a
// completion whose revision is stale is discarded).
func (s *AsyncComputed[T]) Revision() int {
	var r int
	s.c.do(func() { r = s.node.revision })
	return r
}

// Get is the synchronous cached read (Dart get()): it returns (value, true)
// when the slot is resolved, else (zero, false). It does not spawn a compute.
func (s *AsyncComputed[T]) Get() (T, bool) {
	var v T
	var ok bool
	s.c.do(func() {
		if s.node.state == AsyncComputedResolved {
			v = s.node.value.(T)
			ok = true
		}
	})
	return v, ok
}

// Value returns the cached value when resolved, else (zero, false)
// (Dart value getter).
func (s *AsyncComputed[T]) Value() (T, bool) { return s.Get() }

// GetAsync awaits the slot's value. Resolved slots return immediately;
// otherwise the caller attaches to the in-flight compute (spawning one if none
// is running — in-flight deduplication). ctx cancels this waiter only: dropping
// one waiter never cancels a shared in-flight compute (cancellation contract
// point 1). Supersession causes a transparent re-resolve.
func (s *AsyncComputed[T]) GetAsync(ctx context.Context) (T, error) {
	var zero T
	for {
		var (
			outVal any
			outErr error
			done   bool
			waiter chan asyncResult
		)
		ok := s.c.do(func() {
			n := s.node
			if s.c.disposed {
				outErr = ErrAsyncContextDisposed
				done = true
				return
			}
			if n.disposed {
				outErr = &DisposedError{Kind: "slot"}
				done = true
				return
			}
			switch n.state {
			case AsyncComputedResolved:
				outVal = n.value
				done = true
				return
			}
			// Empty and Error both fall through to the spawn path: an errored
			// slot holds no cached result, so the read re-spawns for the
			// current revision (Error -> Computing). Replaying n.err here
			// would make a transient failure permanent for the slot's
			// lifetime, with no read path able to recover it.
			w := make(chan asyncResult, 1)
			n.waiters = append(n.waiters, w)
			waiter = w
			if n.inFlight == nil {
				s.c.spawnCompute(n)
			}
		})
		if !ok {
			return zero, ErrAsyncContextDisposed
		}
		if done {
			if outErr != nil {
				return zero, outErr
			}
			return outVal.(T), nil
		}
		select {
		case r := <-waiter:
			if r.superseded {
				continue
			}
			if r.err != nil {
				return zero, r.err
			}
			return r.value.(T), nil
		case <-ctx.Done():
			// Waiter cancellation: drop just our waiter; the shared compute
			// keeps running for any remaining waiters and still caches.
			s.c.do(func() { s.node.removeWaiter(waiter) })
			return zero, ctx.Err()
		}
	}
}

// AsyncComputeContext is handed to an async compute/effect body. Dependencies
// are registered through TrackSource / TrackComputed (free functions, because Go
// methods cannot be generic) which record the edge before the awaited read.
type AsyncComputeContext struct {
	c     *AsyncContext
	owner depOwner
	goctx context.Context
}

// Context returns the per-compute cancellation context. It is cancelled when
// this compute is superseded or the AsyncContext is disposed; long-running
// compute bodies should observe Context().Done().
func (cc *AsyncComputeContext) Context() context.Context { return cc.goctx }

// TrackSource reads a cell inside an async compute/effect, registering a
// dependency edge before returning the value (Dart AsyncComputeContext.getCell).
func TrackSource[T any](cc *AsyncComputeContext, cell *AsyncSource[T]) T {
	var v T
	var disposed bool
	cc.c.do(func() {
		if cell.node.disposed {
			disposed = true
			return
		}
		if cc.goctx.Err() == nil {
			cc.c.trackDep(cc.owner, cell.node)
		}
		v = cell.node.value.(T)
	})
	if disposed {
		// Panic, not a returned error: TrackSource returns a bare T so a compute
		// body has no error channel here. runComputeSafe / runBodySafe already
		// recover panics into the compute's error, which is exactly the
		// "errors on next recompute" contract. Mirrors the synchronous
		// Cell.Get panic (see disposal.go).
		panic(&DisposedError{Kind: "cell"})
	}
	return v
}

// TrackComputed awaits a computed inside an async compute/effect, registering a
// dependency edge before the awaited read (Dart AsyncComputeContext.getAsync).
// The nested await uses this compute's cancellation context, so supersession
// unwinds nested reads too.
func TrackComputed[T any](cc *AsyncComputeContext, computed *AsyncComputed[T]) (T, error) {
	cc.c.do(func() {
		if cc.goctx.Err() == nil {
			cc.c.trackDep(cc.owner, computed.node)
		}
	})
	return computed.GetAsync(cc.goctx)
}

// TrackCell is the deprecated v1 source-read helper.
//
// Deprecated: use TrackSource.
func TrackCell[T any](cc *AsyncComputeContext, source *AsyncSource[T]) T {
	return TrackSource(cc, source)
}

// TrackAsync is the deprecated v1 computed-read helper.
//
// Deprecated: use TrackComputed.
func TrackAsync[T any](cc *AsyncComputeContext, computed *AsyncComputed[T]) (T, error) {
	return TrackComputed(cc, computed)
}

// AsyncEffectHandle is an async effect returned by AsyncContext.EffectAsync.
// Reruns are serialized per effect (a rerun does not start until the previous
// cleanup completes), and disposal awaits the current cleanup.
type AsyncEffectHandle struct {
	c              *AsyncContext
	body           func(cc *AsyncComputeContext) func()
	cleanup        func()
	deps           map[any]struct{}
	running        bool
	rerunScheduled bool
	disposed       bool
	cancel         context.CancelFunc
}

func (e *AsyncEffectHandle) onDepInvalidated(c *AsyncContext) { c.scheduleEffectRerun(e) }
func (e *AsyncEffectHandle) depSet() map[any]struct{}         { return e.deps }

// DisposeAsync disposes the effect: cancels any in-flight body and runs its
// pending cleanup. Idempotent.
func (e *AsyncEffectHandle) DisposeAsync() {
	var cleanup func()
	e.c.do(func() {
		if e.disposed {
			return
		}
		e.disposed = true
		if e.rerunScheduled {
			// The rerun will never happen, so it must stop counting towards
			// quiescence or the drain counters never reset.
			e.rerunScheduled = false
			e.c.pendingReruns--
		}
		if e.cancel != nil {
			e.cancel()
		}
		delete(e.c.effects, e)
		for dep := range e.deps {
			e.c.removeDependent(dep, e)
		}
		e.deps = map[any]struct{}{}
		cleanup = e.cleanup
		e.cleanup = nil
	})
	if cleanup != nil {
		runCleanupSafe(cleanup)
	}
}

// Dispose is an alias for DisposeAsync.
func (e *AsyncEffectHandle) Dispose() { e.DisposeAsync() }

// AsyncContext is the async reactive surface: a distinct graph owned by a
// single goroutine that serializes all mutations and reads over a command
// channel. Unlike core Context, AsyncContext is safe for concurrent use.
type AsyncContext struct {
	commands chan func()
	stopReq  chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once

	// Loop-owned state — touched only inside the owner goroutine (loop).
	dependents map[any]map[dependent]struct{}
	computing  map[*asyncComputedNode]struct{}
	effects    map[*AsyncEffectHandle]struct{}
	disposed   bool
	batchDepth int
	batchQueue map[any]struct{}

	// --- bounded effect drain ---
	//
	// The async plane's drain is a goroutine CHAIN, not a flat worklist:
	// onEffectDone re-enters runEffect while rerunScheduled is set. An effect
	// writing into its own dependency cone therefore reschedules itself
	// forever, spawning a goroutine per hop, and the chain's only exit is
	// convergence. Same defect as the sync plane's flushEffects loop, same fix:
	// bound the runs and report rather than hang.
	//
	// Every field here is loop-owned, so no locking — the transitions all
	// happen inside c.do / c.post.
	drainBudget int
	// runningEffects and pendingReruns make quiescence an O(1) test. Scanning
	// c.effects for "is anything still going" would be O(n) on every effect
	// completion, which is the common case and not where that cost belongs.
	runningEffects int
	pendingReruns  int
	// drainIterations counts effect runs since the last quiescent point; it is
	// what the budget bounds.
	drainIterations     int
	drainRuns           map[*AsyncEffectHandle]int
	drainOrder          []*AsyncEffectHandle
	lastDrainExhaustion *DrainExhaustion
}

// DrainBudget reports the current effect-drain iteration budget.
func (c *AsyncContext) DrainBudget() int {
	budget := 0
	c.do(func() { budget = c.drainBudgetLocked() })
	if budget == 0 {
		return DefaultDrainBudget
	}
	return budget
}

func (c *AsyncContext) drainBudgetLocked() int {
	if c.drainBudget <= 0 {
		return DefaultDrainBudget
	}
	return c.drainBudget
}

// SetDrainBudget overrides the effect-drain iteration budget. See
// Context.SetDrainBudget.
func (c *AsyncContext) SetDrainBudget(n int) {
	if n < 1 {
		n = 1
	}
	c.do(func() { c.drainBudget = n })
}

// LastDrainExhaustion returns the most recent drain exhaustion, or nil when
// every drain so far reached quiescence.
func (c *AsyncContext) LastDrainExhaustion() *DrainExhaustion {
	var report *DrainExhaustion
	c.do(func() { report = c.lastDrainExhaustion })
	return report
}

// ClearDrainExhaustion forgets the recorded exhaustion so a later drain can be
// observed independently.
func (c *AsyncContext) ClearDrainExhaustion() {
	c.do(func() { c.lastDrainExhaustion = nil })
}

// noteDrainRun records one effect run and reports whether the budget is now
// exhausted. Loop-owned; called from runEffect before the body is spawned, so
// the run that trips the budget does not also execute.
func (c *AsyncContext) noteDrainRun(e *AsyncEffectHandle) bool {
	if c.drainRuns == nil {
		c.drainRuns = map[*AsyncEffectHandle]int{}
	}
	if c.drainRuns[e] == 0 {
		c.drainOrder = append(c.drainOrder, e)
	}
	c.drainRuns[e]++
	c.drainIterations++
	if c.drainIterations < c.drainBudgetLocked() {
		return false
	}
	c.lastDrainExhaustion = c.asyncDrainExhaustionReport()
	return true
}

// resetDrainIfQuiescent clears the per-drain counters once nothing is running
// and nothing is scheduled, so the next drain is measured on its own.
func (c *AsyncContext) resetDrainIfQuiescent() {
	if c.runningEffects != 0 || c.pendingReruns != 0 {
		return
	}
	c.drainIterations = 0
	clear(c.drainRuns)
	c.drainOrder = c.drainOrder[:0]
}

// asyncDrainExhaustionReport mirrors Context.drainExhaustionReport: busiest
// effects first, ties in first-run order, capped so the head stays visible.
func (c *AsyncContext) asyncDrainExhaustionReport() *DrainExhaustion {
	return &DrainExhaustion{
		Iterations: c.drainIterations,
		Budget:     c.drainBudgetLocked(),
		TopEffects: topEffectRuns(c.drainOrder, c.drainRuns),
	}
}

// NewAsyncContext creates an async reactive context and starts its owner
// goroutine. Call DisposeAsync or Close to tear it down.
func NewAsyncContext() *AsyncContext {
	c := &AsyncContext{
		commands:   make(chan func(), 64),
		stopReq:    make(chan struct{}),
		stopped:    make(chan struct{}),
		dependents: map[any]map[dependent]struct{}{},
		computing:  map[*asyncComputedNode]struct{}{},
		effects:    map[*AsyncEffectHandle]struct{}{},
		batchQueue: map[any]struct{}{},
	}
	go c.loop()
	return c
}

func (c *AsyncContext) loop() {
	for {
		select {
		case cmd := <-c.commands:
			cmd()
		case <-c.stopReq:
			// Drain already-queued commands so pending do()/post() callers
			// observe their command running, then stop.
			for {
				select {
				case cmd := <-c.commands:
					cmd()
				default:
					close(c.stopped)
					return
				}
			}
		}
	}
}

// do runs fn on the owner goroutine and waits for it. It returns false if the
// context's loop has stopped (treated as disposed by callers).
func (c *AsyncContext) do(fn func()) bool {
	done := make(chan struct{})
	select {
	case c.commands <- func() { fn(); close(done) }:
	case <-c.stopReq:
		return false
	}
	select {
	case <-done:
		return true
	case <-c.stopped:
		// The loop stopped; it may still have run our command while draining.
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
}

// post enqueues a fire-and-forget command (compute/effect completions). If the
// loop is stopping the command is dropped — its slot/effect is gone anyway.
func (c *AsyncContext) post(fn func()) {
	select {
	case c.commands <- fn:
	case <-c.stopReq:
	}
}

func (c *AsyncContext) stopLoop() {
	c.stopOnce.Do(func() { close(c.stopReq) })
	<-c.stopped
}

// --- dependency-edge helpers (loop-only) ---

func (c *AsyncContext) addDependent(dep any, d dependent) {
	m := c.dependents[dep]
	if m == nil {
		m = map[dependent]struct{}{}
		c.dependents[dep] = m
	}
	m[d] = struct{}{}
}

func (c *AsyncContext) removeDependent(dep any, d dependent) {
	if m := c.dependents[dep]; m != nil {
		delete(m, d)
		if len(m) == 0 {
			delete(c.dependents, dep)
		}
	}
}

func (c *AsyncContext) trackDep(owner depOwner, dep any) {
	if c.disposed {
		return
	}
	// Never build an edge onto or out of a torn-down node. A compute or effect
	// body runs on its own goroutine and can reach this point *after* its owner
	// was disposed — the disposal already removed the owner's edges, so a late
	// registration would resurrect one and leak it for the life of the context.
	// This is the exact shape #lzspecedgeindex's churn fixture measures. Both
	// this check and the disposal run inside the owner goroutine, so the test
	// is race-free.
	if c.isDisposedNode(dep) || c.isDisposedNode(owner) {
		return
	}
	d := owner.depSet()
	if _, ok := d[dep]; !ok {
		d[dep] = struct{}{}
		c.addDependent(dep, owner)
	}
}

func (c *AsyncContext) invalidateDependents(dep any) {
	if c.disposed {
		return
	}
	if c.batchDepth > 0 {
		c.batchQueue[dep] = struct{}{}
		return
	}
	c.propagate(dep, true)
}

// propagate walks the dependent cone rooted at dep.
//
// schedule distinguishes the two callers. A write (schedule=true) invalidates
// and then reruns the effects it reached — that is a publish. A disposal
// (schedule=false) invalidates and deliberately does NOT rerun them: running an
// effect during teardown re-enters a compute that reads the node being
// disposed, which breaks teardown idempotence. The contract is "errors on next
// recompute", so teardown marks only. Same rule as the synchronous path's
// Context.disposing (see disposal.go).
func (c *AsyncContext) propagate(dep any, schedule bool) {
	// Walk the FULL transitive dependent cone, not just one level. A slot that
	// depends on a slot must itself be invalidated: GetAsync short-circuits on
	// AsyncComputedResolved, so a downstream slot left Resolved keeps serving its
	// cached value forever and the pull chain cannot rescue it. Stopping one
	// level below the written cell is the defect this walk replaces — the sync
	// side never had it (reactiveBase.invalidate in core.go cascades), only the
	// async path.
	//
	// Termination is by edge consumption rather than a visited set: deleting
	// c.dependents[cur] on visit means a revisit finds no entry and the branch
	// dies, so diamonds and cycles terminate. Edges re-register when each slot
	// recomputes (spawnCompute clears s.deps and the compute re-tracks).
	stack := []any{dep}
	// Effects are collected and scheduled AFTER the walk. scheduleEffectRerun
	// calls runEffect inline, and runEffect detaches the effect's dependency
	// edges via removeDependent — mutating c.dependents while the walk is still
	// reading it.
	var effects []*AsyncEffectHandle
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		m := c.dependents[cur]
		if m == nil {
			continue
		}
		delete(c.dependents, cur)
		for d := range m {
			switch n := d.(type) {
			case *AsyncEffectHandle:
				// Frontier leaf: nothing can depend on an effect, so the walk
				// does not continue through one.
				effects = append(effects, n)
			case *asyncComputedNode:
				c.invalidateSlot(n)
				// A slot registers its dependents under its own pointer
				// (trackDep is called with slot.node), so the node is the key
				// for the next level.
				stack = append(stack, n)
			default:
				d.onDepInvalidated(c)
			}
		}
	}
	for _, e := range effects {
		if schedule {
			c.scheduleEffectRerun(e)
			continue
		}
		// Teardown: mark, do not run. The walk above consumed this effect's
		// incoming edges, so re-attach the ones whose dependency is still
		// alive — without this the effect would be silently orphaned from
		// every surviving dependency and never rerun again.
		for d := range e.deps {
			if c.isDisposedNode(d) {
				delete(e.deps, d)
				continue
			}
			c.addDependent(d, e)
		}
	}
}

// --- slot compute lifecycle (loop-only) ---

func (c *AsyncContext) spawnCompute(s *asyncComputedNode) {
	s.state = AsyncComputedComputing
	for dep := range s.deps {
		c.removeDependent(dep, s)
	}
	s.deps = map[any]struct{}{}
	goctx, cancel := context.WithCancel(context.Background())
	inf := &inFlight{ctx: goctx, cancel: cancel}
	s.inFlight = inf
	c.computing[s] = struct{}{}
	compute := s.compute
	cc := &AsyncComputeContext{c: c, owner: s, goctx: goctx}
	go func() {
		value, err := runComputeSafe(cc, compute)
		c.post(func() { c.onComplete(s, inf, value, err) })
	}()
}

func (c *AsyncContext) invalidateSlot(s *asyncComputedNode) {
	s.revision++
	s.state = AsyncComputedComputing
	c.failInFlight(s)
}

// failInFlight cancels the in-flight compute (supersession) and tells current
// waiters to re-resolve. The stale compute's completion is later discarded by
// the identity gate in onComplete.
func (c *AsyncContext) failInFlight(s *asyncComputedNode) {
	inf := s.inFlight
	s.inFlight = nil
	delete(c.computing, s)
	if inf != nil {
		inf.cancel()
	}
	ws := s.waiters
	s.waiters = nil
	for _, w := range ws {
		w <- asyncResult{superseded: true}
	}
}

func (c *AsyncContext) onComplete(s *asyncComputedNode, inf *inFlight, value any, err error) {
	if s.inFlight != inf {
		// Superseded or cancelled: this run's token was replaced. Discard;
		// waiters were already told to re-resolve.
		return
	}
	s.inFlight = nil
	delete(c.computing, s)
	if err != nil {
		// The error is delivered to this attempt's waiters and deliberately not
		// stored: a slot in Error holds no cached result, so the next GetAsync
		// re-spawns (Error -> Computing) instead of replaying a stale failure.
		s.state = AsyncComputedError
		c.deliver(s, asyncResult{err: err})
		return
	}
	if s.eq != nil && s.hasValue && s.eq(s.value, value) {
		// Memo equality suppression: keep the cached value, no cascade.
		s.state = AsyncComputedResolved
		c.deliver(s, asyncResult{value: s.value})
		return
	}
	s.value = value
	s.hasValue = true
	s.state = AsyncComputedResolved
	c.deliver(s, asyncResult{value: value})
}

func (c *AsyncContext) deliver(s *asyncComputedNode, r asyncResult) {
	ws := s.waiters
	s.waiters = nil
	for _, w := range ws {
		w <- r // cap-1 buffered, never blocks the loop
	}
}

// --- effects ---

// EffectAsync creates an async effect. The body receives a compute context and
// returns an optional cleanup callback run before the next body and on disposal.
// Reruns are serialized: a rerun does not start until the prior cleanup runs.
func (c *AsyncContext) EffectAsync(body func(cc *AsyncComputeContext) func()) *AsyncEffectHandle {
	e := &AsyncEffectHandle{c: c, body: body, deps: map[any]struct{}{}}
	c.do(func() {
		if c.disposed {
			return
		}
		c.effects[e] = struct{}{}
		c.scheduleEffectRerun(e)
	})
	return e
}

func (c *AsyncContext) scheduleEffectRerun(e *AsyncEffectHandle) {
	if e.disposed {
		return
	}
	if e.running {
		if !e.rerunScheduled {
			e.rerunScheduled = true
			c.pendingReruns++
		}
		return
	}
	c.runEffect(e)
}

func (c *AsyncContext) runEffect(e *AsyncEffectHandle) {
	// Bound the chain before spawning, so the run that trips the budget does
	// not also execute. Leaving the effect un-run (and un-rescheduled) is the
	// async analogue of the sync drain leaving its worklist in place:
	// exhaustion is reported, not papered over by finishing one more hop.
	if c.noteDrainRun(e) {
		return
	}
	e.running = true
	c.runningEffects++
	for dep := range e.deps {
		c.removeDependent(dep, e)
	}
	e.deps = map[any]struct{}{}
	goctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	cleanup := e.cleanup
	e.cleanup = nil
	body := e.body
	cc := &AsyncComputeContext{c: c, owner: e, goctx: goctx}
	go func() {
		// Cleanup before next body (cancellation contract point 5).
		if cleanup != nil {
			runCleanupSafe(cleanup)
		}
		newCleanup := runBodySafe(cc, body)
		c.post(func() { c.onEffectDone(e, newCleanup) })
	}()
}

func (c *AsyncContext) onEffectDone(e *AsyncEffectHandle, cleanup func()) {
	e.running = false
	c.runningEffects--
	if e.disposed {
		// A cleanup produced after disposal is dropped (matches Dart).
		if e.rerunScheduled {
			e.rerunScheduled = false
			c.pendingReruns--
		}
		c.resetDrainIfQuiescent()
		return
	}
	e.cleanup = cleanup
	if e.rerunScheduled {
		e.rerunScheduled = false
		c.pendingReruns--
		c.runEffect(e)
		return
	}
	c.resetDrainIfQuiescent()
}

// --- batching & lifecycle ---

// Batch delimits a synchronous batch on the calling goroutine. Cell writes made
// during run queue their invalidation roots; at the outermost batch exit the
// queued roots propagate once. Async reruns fire after run returns. Re-entrant.
//
// Note: because the batch flag is loop-owned, cell writes from other goroutines
// during the batch are also coalesced; batch from a single goroutine.
func (c *AsyncContext) Batch(run func()) {
	c.do(func() { c.batchDepth++ })
	defer c.do(func() {
		c.batchDepth--
		if c.batchDepth == 0 {
			q := c.batchQueue
			c.batchQueue = map[any]struct{}{}
			for dep := range q {
				c.invalidateDependents(dep)
			}
		}
	})
	run()
}

// DisposeAsync disposes the context: cancels all in-flight computations,
// delivers a disposed error to blocked waiters, runs and awaits every active
// effect cleanup, then stops the owner goroutine. Subsequent operations are
// no-ops / disposed errors. Idempotent.
func (c *AsyncContext) DisposeAsync() {
	var effects []*AsyncEffectHandle
	ok := c.do(func() {
		if c.disposed {
			return
		}
		c.disposed = true
		for s := range c.computing {
			if s.inFlight != nil {
				s.inFlight.cancel()
				s.inFlight = nil
			}
			ws := s.waiters
			s.waiters = nil
			for _, w := range ws {
				w <- asyncResult{err: ErrAsyncContextDisposed}
			}
		}
		c.computing = map[*asyncComputedNode]struct{}{}
		for e := range c.effects {
			effects = append(effects, e)
		}
	})
	if !ok {
		return // already stopped
	}
	for _, e := range effects {
		e.DisposeAsync()
	}
	c.do(func() {
		c.dependents = map[any]map[dependent]struct{}{}
		c.effects = map[*AsyncEffectHandle]struct{}{}
	})
	c.stopLoop()
}

// Close disposes the context (io.Closer-style). It always returns nil.
func (c *AsyncContext) Close() error {
	c.DisposeAsync()
	return nil
}

// --- helpers ---

func runComputeSafe(cc *AsyncComputeContext, compute func(cc *AsyncComputeContext) (any, error)) (value any, err error) {
	defer func() {
		if r := recover(); r != nil {
			value = nil
			err = asyncPanicError(r)
		}
	}()
	return compute(cc)
}

func runBodySafe(cc *AsyncComputeContext, body func(cc *AsyncComputeContext) func()) (cleanup func()) {
	defer func() {
		if recover() != nil {
			cleanup = nil // body errors are swallowed; effects never publish
		}
	}()
	return body(cc)
}

func runCleanupSafe(f func()) {
	defer func() { _ = recover() }() // cleanup errors are best-effort
	f()
}

func asyncPanicError(r any) error {
	if err, ok := r.(error); ok {
		return err
	}
	return errors.New("lazily: async compute panicked")
}

// asyncValueEqual reports whether two cell values are equal, mirroring Dart's
// `newValue != _value` change guard. Fast paths via a type switch avoid the
// reflect.TypeOf / reflect.DeepEqual cost (each ≈ 1 alloc + 50–100 ns) on the
// common comparable cell payloads (scalars, strings, byte slices). The final
// fallback to reflect.DeepEqual remains for arbitrary non-comparable types
// (slices, maps, funcs) so Set never panics.
func asyncValueEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	switch av := a.(type) {
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case int:
		bv, ok := b.(int)
		return ok && av == bv
	case int64:
		bv, ok := b.(int64)
		return ok && av == bv
	case int32:
		bv, ok := b.(int32)
		return ok && av == bv
	case uint64:
		bv, ok := b.(uint64)
		return ok && av == bv
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case []byte:
		bv, ok := b.([]byte)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if av[i] != bv[i] {
				return false
			}
		}
		return true
	}
	ta := reflect.TypeOf(a)
	if ta != reflect.TypeOf(b) {
		return false
	}
	if ta.Comparable() {
		return a == b
	}
	return reflect.DeepEqual(a, b)
}
