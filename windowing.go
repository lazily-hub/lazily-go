// Phase 5 of the realtime + distributed primitives plan (#lzwindow) — stream
// windowing.
//
// See lazily-spec/docs/windowing.md and the formal model
// lazily-formal/LazilyFormal/Windowing.lean. Window aggregation *is* a merge, so
// the MergePolicy algebra (Sum/Max/SetUnion/custom) composes: the aggregate of a
// window equals the associative fold of its elements under the policy. Each
// primitive is a pure compute core (window bookkeeping + a MergePolicy fold)
// split from a reactive cell projecting the last emitted aggregate.
//
// Go note: the reactive projection is the last emitted aggregate as an optional,
// held in a Cell. Cell[T] requires `comparable`, so the window element type T is
// `comparable` and the projection is Opt[T] (itself comparable). Emit-only
// invalidation: the output cell is Set only when a window emits, and its
// ==-guard suppresses any cascade when the emitted value is unchanged.

package lazily

// mergeInto folds v into an optional accumulator under policy (identity when
// the accumulator is empty).
func mergeInto[T comparable](policy MergePolicy[T], acc *Opt[T], v T) {
	if acc.Present {
		acc.Value = policy.Merge(acc.Value, v)
	} else {
		acc.Present = true
		acc.Value = v
	}
}

// foldWindow folds a slice of elements under policy (absent for an empty window).
func foldWindow[T comparable](policy MergePolicy[T], items []T) Opt[T] {
	var acc Opt[T]
	for _, v := range items {
		mergeInto(policy, &acc, v)
	}
	return acc
}

// satSub is saturating subtraction on uint64 (a-b, clamped at 0).
func satSub(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}

// ===========================================================================
// Tumbling (count)
// ===========================================================================

// TumblingCountCore is the count-based tumbling window compute core.
type TumblingCountCore[T comparable] struct {
	n      uint64
	policy MergePolicy[T]
	acc    Opt[T]
	count  uint64
}

// NewTumblingCountCore builds a count-tumbling core emitting every n elements.
func NewTumblingCountCore[T comparable](n uint64, policy MergePolicy[T]) *TumblingCountCore[T] {
	if n < 1 {
		n = 1
	}
	return &TumblingCountCore[T]{n: n, policy: policy}
}

// Push accumulates an element; on the n-th it emits the window fold and resets.
func (c *TumblingCountCore[T]) Push(v T) Opt[T] {
	mergeInto(c.policy, &c.acc, v)
	c.count++
	if c.count >= c.n {
		c.count = 0
		out := c.acc
		c.acc = Opt[T]{}
		return out
	}
	return Opt[T]{}
}

// ===========================================================================
// Tumbling (time)
// ===========================================================================

// TumblingTimeCore is the time-based tumbling window compute core.
type TumblingTimeCore[T comparable] struct {
	period uint64
	next   uint64
	policy MergePolicy[T]
	acc    Opt[T]
}

// NewTumblingTimeCore builds a time-tumbling core with the given period.
func NewTumblingTimeCore[T comparable](period uint64, policy MergePolicy[T]) *TumblingTimeCore[T] {
	if period < 1 {
		period = 1
	}
	return &TumblingTimeCore[T]{period: period, next: period, policy: policy}
}

// Push accumulates an element into the current window (no emit).
func (c *TumblingTimeCore[T]) Push(_ uint64, v T) {
	mergeInto(c.policy, &c.acc, v)
}

// Tick emits the window fold at a period boundary (empty window emits absent).
func (c *TumblingTimeCore[T]) Tick(now uint64) Opt[T] {
	if now < c.next {
		return Opt[T]{}
	}
	for c.next <= now {
		c.next += c.period
	}
	out := c.acc
	c.acc = Opt[T]{}
	return out
}

// ===========================================================================
// Sliding (count)
// ===========================================================================

// SlidingCore is the count-based sliding window compute core (fold-recompute,
// correct for any associative merge).
type SlidingCore[T comparable] struct {
	size   int
	slide  uint64
	policy MergePolicy[T]
	buffer []T
	since  uint64
}

// NewSlidingCore builds a sliding core retaining `size` elements, emitting every
// `slide` pushes.
func NewSlidingCore[T comparable](size int, slide uint64, policy MergePolicy[T]) *SlidingCore[T] {
	if size < 1 {
		size = 1
	}
	if slide < 1 {
		slide = 1
	}
	return &SlidingCore[T]{size: size, slide: slide, policy: policy}
}

// Push adds an element; every `slide` pushes emit the fold over the last `size`.
func (c *SlidingCore[T]) Push(v T) Opt[T] {
	c.buffer = append(c.buffer, v)
	for len(c.buffer) > c.size {
		c.buffer = c.buffer[1:]
	}
	c.since++
	if c.since >= c.slide {
		c.since = 0
		return foldWindow(c.policy, c.buffer)
	}
	return Opt[T]{}
}

// ===========================================================================
// Session (gap-based)
// ===========================================================================

// SessionCore is the gap-based sessionization compute core.
type SessionCore[T comparable] struct {
	gap     uint64
	policy  MergePolicy[T]
	acc     Opt[T]
	lastSet bool
	last    uint64
}

// NewSessionCore builds a session core closing sessions after an idle `gap`.
func NewSessionCore[T comparable](gap uint64, policy MergePolicy[T]) *SessionCore[T] {
	return &SessionCore[T]{gap: gap, policy: policy}
}

// Push adds an element; a gap larger than `gap` closes the current session
// (emitting its fold) and opens a new one.
func (c *SessionCore[T]) Push(now uint64, v T) Opt[T] {
	idleBreak := c.lastSet && satSub(now, c.last) > c.gap && c.acc.Present
	if idleBreak {
		emit := c.acc
		c.acc = Opt[T]{Present: true, Value: v}
		c.last = now
		c.lastSet = true
		return emit
	}
	mergeInto(c.policy, &c.acc, v)
	c.last = now
	c.lastSet = true
	return Opt[T]{}
}

// Flush closes the open session if it has been idle longer than `gap`.
func (c *SessionCore[T]) Flush(now uint64) Opt[T] {
	idle := c.lastSet && satSub(now, c.last) > c.gap && c.acc.Present
	if idle {
		out := c.acc
		c.acc = Opt[T]{}
		return out
	}
	return Opt[T]{}
}

// ===========================================================================
// Reactive cells
// ===========================================================================

// TumblingCountWindow is a reactive count-tumbling window projecting the last
// emitted aggregate.
type TumblingCountWindow[T comparable] struct {
	core   *TumblingCountCore[T]
	output *SourceCell[Opt[T]]
}

// TumblingCount constructs a reactive count-tumbling window over ctx.
func TumblingCount[T comparable](ctx *Context, n uint64, policy MergePolicy[T]) *TumblingCountWindow[T] {
	return &TumblingCountWindow[T]{
		core:   NewTumblingCountCore(n, policy),
		output: NewSourceCell(ctx, Opt[T]{}),
	}
}

// Push accumulates an element, projecting the aggregate onto the output cell
// when the window emits. Returns the emitted aggregate (absent if none).
func (w *TumblingCountWindow[T]) Push(v T) Opt[T] {
	e := w.core.Push(v)
	if e.Present {
		w.output.Set(e)
	}
	return e
}

// OutputCell returns the reactive cell holding the last emitted aggregate.
func (w *TumblingCountWindow[T]) OutputCell() *SourceCell[Opt[T]] { return w.output }

// Output reads the last emitted aggregate (subscribes in a computation).
func (w *TumblingCountWindow[T]) Output() Opt[T] { return w.output.Get() }

// TumblingTimeWindow is a reactive time-tumbling window (Push(now,v) + Tick(now)).
type TumblingTimeWindow[T comparable] struct {
	core   *TumblingTimeCore[T]
	output *SourceCell[Opt[T]]
}

// TumblingTime constructs a reactive time-tumbling window over ctx.
func TumblingTime[T comparable](ctx *Context, period uint64, policy MergePolicy[T]) *TumblingTimeWindow[T] {
	return &TumblingTimeWindow[T]{
		core:   NewTumblingTimeCore(period, policy),
		output: NewSourceCell(ctx, Opt[T]{}),
	}
}

// Push accumulates an element into the current window (no emit).
func (w *TumblingTimeWindow[T]) Push(now uint64, v T) { w.core.Push(now, v) }

// Tick emits the window fold at a period boundary, projecting it onto output.
func (w *TumblingTimeWindow[T]) Tick(now uint64) Opt[T] {
	e := w.core.Tick(now)
	if e.Present {
		w.output.Set(e)
	}
	return e
}

// OutputCell returns the reactive cell holding the last emitted aggregate.
func (w *TumblingTimeWindow[T]) OutputCell() *SourceCell[Opt[T]] { return w.output }

// Output reads the last emitted aggregate (subscribes in a computation).
func (w *TumblingTimeWindow[T]) Output() Opt[T] { return w.output.Get() }

// SlidingWindow is a reactive count-based sliding window projecting the last
// emitted aggregate.
type SlidingWindow[T comparable] struct {
	core   *SlidingCore[T]
	output *SourceCell[Opt[T]]
}

// Sliding constructs a reactive sliding window over ctx.
func Sliding[T comparable](ctx *Context, size int, slide uint64, policy MergePolicy[T]) *SlidingWindow[T] {
	return &SlidingWindow[T]{
		core:   NewSlidingCore(size, slide, policy),
		output: NewSourceCell(ctx, Opt[T]{}),
	}
}

// Push adds an element, projecting the fold onto output on a slide boundary.
func (w *SlidingWindow[T]) Push(v T) Opt[T] {
	e := w.core.Push(v)
	if e.Present {
		w.output.Set(e)
	}
	return e
}

// OutputCell returns the reactive cell holding the last emitted aggregate.
func (w *SlidingWindow[T]) OutputCell() *SourceCell[Opt[T]] { return w.output }

// Output reads the last emitted aggregate (subscribes in a computation).
func (w *SlidingWindow[T]) Output() Opt[T] { return w.output.Get() }

// SessionWindow is a reactive gap-based session window (Push(now,v) + Flush(now)).
type SessionWindow[T comparable] struct {
	core   *SessionCore[T]
	output *SourceCell[Opt[T]]
}

// Session constructs a reactive session window over ctx.
func Session[T comparable](ctx *Context, gap uint64, policy MergePolicy[T]) *SessionWindow[T] {
	return &SessionWindow[T]{
		core:   NewSessionCore(gap, policy),
		output: NewSourceCell(ctx, Opt[T]{}),
	}
}

// Push adds an element; a large idle gap closes the session (projecting its fold
// onto output) and opens a new one.
func (w *SessionWindow[T]) Push(now uint64, v T) Opt[T] {
	e := w.core.Push(now, v)
	if e.Present {
		w.output.Set(e)
	}
	return e
}

// Flush closes an idle-open session, projecting its fold onto output.
func (w *SessionWindow[T]) Flush(now uint64) Opt[T] {
	e := w.core.Flush(now)
	if e.Present {
		w.output.Set(e)
	}
	return e
}

// OutputCell returns the reactive cell holding the last emitted aggregate.
func (w *SessionWindow[T]) OutputCell() *SourceCell[Opt[T]] { return w.output }

// Output reads the last emitted aggregate (subscribes in a computation).
func (w *SessionWindow[T]) Output() Opt[T] { return w.output.Get() }
