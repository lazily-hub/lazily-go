// Thread-safe reactive context — a lock-serialized batch boundary.
//
// The Go counterpart of lazily-py's ThreadSafeContext and the Lean
// LazilyFormal.ThreadSafe model (lazily-spec § "Concurrency layers are
// required"). The behavioral contract: serializing concurrent cell writes
// through a batch boundary coalesces them into one invalidation pass whose
// result is a deterministic function of the writes — independent of the
// interleaving the lock happened to pick.
//
// Go has real threads (goroutines), so this layer is required (feature row
// "Thread-safe context (lock-backed)" is ✅ for Go, unlike the single-isolate
// JS/Dart runtimes). ThreadSafeContext wraps a single-threaded Context with a
// reentrant lock and reuses the core batch coalescing, so a one-write section
// is observationally identical to a plain Cell.Set — the thread-safe context
// refines the single-threaded kernel.
package lazily

import (
	"runtime"
	"strconv"
	"sync"
)

// ThreadSafeContext serializes all access to an underlying Context behind a
// reentrant lock. Build reactives and read/write cells only inside WithLock or
// Batch (or via TSSetCell); concurrent callers are linearized by the lock, so
// every batch flush is glitch-free.
type ThreadSafeContext struct {
	ctx  *Context
	lock reentrantLock
}

// NewThreadSafeContext creates a lock-backed reactive context.
func NewThreadSafeContext() *ThreadSafeContext {
	return &ThreadSafeContext{ctx: NewContext()}
}

// Context returns the underlying single-threaded Context. Only touch it while
// holding the lock (inside WithLock/Batch) — direct concurrent use is unsafe.
func (t *ThreadSafeContext) Context() *Context { return t.ctx }

// WithLock runs fn while holding the lock, giving it exclusive, race-free access
// to the reactive graph (build nodes, read slots, write cells). Reentrant: fn
// may itself call WithLock/Batch/TSSetCell.
func (t *ThreadSafeContext) WithLock(fn func(ctx *Context)) {
	t.lock.Lock()
	defer t.lock.Unlock()
	fn(t.ctx)
}

// Read runs fn under the lock and returns its result — the read-oriented
// convenience over WithLock.
func Read[T any](t *ThreadSafeContext, fn func(ctx *Context) T) T {
	t.lock.Lock()
	defer t.lock.Unlock()
	return fn(t.ctx)
}

// Batch runs fn under the lock inside an underlying Context batch, so all cell
// writes queued in fn flush in a single coalesced invalidation pass at the
// outermost boundary. Nested Batch calls only flush at the outermost boundary.
func (t *ThreadSafeContext) Batch(fn func()) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.ctx.Batch(fn)
}

// TSSetCell writes a cell's value under the lock. Outside a batch it applies
// immediately (a singleton batch ≡ Cell.Set); inside a Batch it defers to the
// coalesced flush. It is a free function because Go methods cannot be generic.
func TSSetCell[T comparable](t *ThreadSafeContext, cell *Cell[T], value T) {
	t.lock.Lock()
	defer t.lock.Unlock()
	cell.Set(value)
}

// --- pure batch-flush kernel (faithful port of the Lean ThreadSafe model) --- //
//
// These operate on a plain node table so they can be property-tested against
// LazilyFormal.ThreadSafe independently of the live reactive graph.

// NodeEntry is a node's (value, state) pair in the pure kernel. State is one of
// "clean" / "dirty".
type NodeEntry struct {
	Value any
	State string
}

// BatchWrite is a (nodeID, value) write in the pure kernel.
type BatchWrite struct {
	NodeID any
	Value  any
}

// ApplyBatch applies the batch's value updates (with the PartialEq guard) to a
// copy of nodes and returns the new table plus the list of source ids that
// actually changed. A faithful port of the Lean applyBatch.
func ApplyBatch(nodes map[any]NodeEntry, batch []BatchWrite) (map[any]NodeEntry, []any) {
	next := make(map[any]NodeEntry, len(nodes))
	for k, v := range nodes {
		next[k] = v
	}
	var changed []any
	seen := map[any]struct{}{}
	for _, w := range batch {
		cur, ok := next[w.NodeID]
		if !ok {
			continue
		}
		if cur.Value == w.Value {
			continue
		}
		next[w.NodeID] = NodeEntry{Value: w.Value, State: "dirty"}
		if _, dup := seen[w.NodeID]; !dup {
			seen[w.NodeID] = struct{}{}
			changed = append(changed, w.NodeID)
		}
	}
	return next, changed
}

// FlushBatch applies the batch's values, then marks the coalesced union of
// changed sources' dependents dirty in one pass — a faithful port of the Lean
// flushBatch (the coalesced frontier: each dependent appears at most once).
func FlushBatch(nodes map[any]NodeEntry, dependents map[any][]any, batch []BatchWrite) map[any]NodeEntry {
	next, changed := ApplyBatch(nodes, batch)
	var frontier []any
	inFrontier := map[any]struct{}{}
	for _, src := range changed {
		for _, d := range dependents[src] {
			if _, ok := inFrontier[d]; !ok {
				inFrontier[d] = struct{}{}
				frontier = append(frontier, d)
			}
		}
	}
	for _, d := range frontier {
		entry := next[d]
		next[d] = NodeEntry{Value: entry.Value, State: "dirty"}
	}
	return next
}

// UnionDependents is the flat union of dependents over a list of source nodes —
// a faithful port of the Lean unionDependents.
func UnionDependents(dependents map[any][]any, sources []any) []any {
	var out []any
	for _, n := range sources {
		out = append(out, dependents[n]...)
	}
	return out
}

// --- reentrant lock ---------------------------------------------------------- //

// reentrantLock is a mutex that the owning goroutine may re-acquire, so a
// WithLock body can call Batch/TSSetCell without self-deadlock.
type reentrantLock struct {
	gate  sync.Mutex // held for the lifetime of the outermost acquisition
	inner sync.Mutex // guards owner/count
	owner int64
	count int
}

func (r *reentrantLock) Lock() {
	gid := goID()
	r.inner.Lock()
	if r.owner == gid {
		r.count++
		r.inner.Unlock()
		return
	}
	r.inner.Unlock()
	r.gate.Lock()
	r.inner.Lock()
	r.owner = gid
	r.count = 1
	r.inner.Unlock()
}

func (r *reentrantLock) Unlock() {
	r.inner.Lock()
	r.count--
	if r.count == 0 {
		r.owner = 0
		r.inner.Unlock()
		r.gate.Unlock()
		return
	}
	r.inner.Unlock()
}

// goID returns the current goroutine's id (parsed from the runtime stack
// header). Used only to make the lock reentrant; not on any hot path.
func goID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// Header format: "goroutine <id> [status]:"
	s := buf[:n]
	i := 0
	for i < len(s) && s[i] != ' ' {
		i++
	}
	i++ // skip the space after "goroutine"
	j := i
	for j < len(s) && s[j] >= '0' && s[j] <= '9' {
		j++
	}
	id, _ := strconv.ParseInt(string(s[i:j]), 10, 64)
	return id
}
