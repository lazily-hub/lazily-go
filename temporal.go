package lazily

// Temporal source primitives (#lztime): logical-clock-driven reactive sources.
//
// A Go port of lazily-rs `src/time.rs`. Time is modeled by a monotone logical
// clock (a non-decreasing now: uint64 tick), exactly like relay policy — a
// binding drives the sources from its own runtime timer by feeding a
// non-decreasing now.
//
// Each source is a pure compute core (a side-effect-free state machine over
// plain integers) split from a thin reactive cell that projects the core's fire
// edge onto a *Cell so dependents invalidate only on an actual fire/expiry edge
// (the == store-guard). DeadlineCell carries an opaque user value alongside a
// bytes-eligible deadline core. Foundation for leases/expiry/windows/presence.

// TimelineSource is a pure temporal compute core driven by a monotone logical
// clock. A runtime advances any source uniformly via Tick; NextFire lets a
// scheduler compute the delay to the next wake-up.
type TimelineSource interface {
	// Tick advances to logical time now (callers must not go backwards).
	// Returns true on a fire edge — a fire happened on this tick.
	Tick(now uint64) bool
	// NextFire reports the logical time of the next fire; ok is false when the
	// source is exhausted (models Option<u64> as (uint64, bool)).
	NextFire() (fire uint64, ok bool)
}

// ManualClock is a monotone logical clock a manual runtime (game loop, test)
// can own to drive sources. Advance clamps backwards moves so now is always
// non-decreasing.
type ManualClock struct {
	now uint64
}

// NewManualClock creates a clock at logical time 0.
func NewManualClock() *ManualClock { return &ManualClock{} }

// Now reports the current logical time.
func (c *ManualClock) Now() uint64 { return c.now }

// Advance moves to now (monotone: a smaller value is clamped to the current
// time). Returns the effective now a source should be ticked with.
func (c *ManualClock) Advance(now uint64) uint64 {
	if now > c.now {
		c.now = now
	}
	return c.now
}

// ---------------------------------------------------------------------------
// Single-shot timer
// ---------------------------------------------------------------------------

// TimerCore is a single-shot compute core: fires exactly once at the first tick
// with now >= fireAt (idempotent thereafter).
type TimerCore struct {
	fireAt uint64
	fired  bool
}

// NewTimerCore creates a single-shot core firing at fireAt.
func NewTimerCore(fireAt uint64) *TimerCore { return &TimerCore{fireAt: fireAt} }

// Fired reports whether the timer has fired.
func (t *TimerCore) Fired() bool { return t.fired }

// Tick advances to now; returns the fire edge.
func (t *TimerCore) Tick(now uint64) bool {
	if t.fired || now < t.fireAt {
		return false
	}
	t.fired = true
	return true
}

// NextFire reports the fire time, or ok=false once fired.
func (t *TimerCore) NextFire() (uint64, bool) {
	if t.fired {
		return 0, false
	}
	return t.fireAt, true
}

// TimerCell is a reactive single-shot timer: projects TimerCore's fire edge
// onto a cell so HasFired/Value dependents invalidate only on the fire
// (idempotent).
type TimerCell struct {
	core  *TimerCore
	fired *Source[bool]
}

// NewTimerCell creates a reactive single-shot timer firing at fireAt.
func NewTimerCell(ctx *Context, fireAt uint64) *TimerCell {
	return &TimerCell{core: NewTimerCore(fireAt), fired: NewSource(ctx, false)}
}

// Tick advances to logical time now; returns the fire edge. The backing cell is
// set to the projected fired state each tick, so the == store-guard makes a
// repeat tick a no-op and dependents invalidate exactly once (on the edge).
func (t *TimerCell) Tick(now uint64) bool {
	edge := t.core.Tick(now)
	t.fired.Set(t.core.Fired())
	return edge
}

// HasFired reports whether the timer has fired (reactive read).
func (t *TimerCell) HasFired() bool { return t.fired.Get() }

// Value models Option<()>: ok is false before the fire, true after (reactive
// read).
func (t *TimerCell) Value() (struct{}, bool) {
	if t.fired.Get() {
		return struct{}{}, true
	}
	return struct{}{}, false
}

// FiredCell returns the backing cell for dependents that subscribe directly.
func (t *TimerCell) FiredCell() *Source[bool] { return t.fired }

// NextFire reports the next fire time, or ok=false once fired.
func (t *TimerCell) NextFire() (uint64, bool) { return t.core.NextFire() }

// ---------------------------------------------------------------------------
// Periodic interval
// ---------------------------------------------------------------------------

// IntervalCore is a periodic compute core: fire boundaries at period, 2*period,
// … A tick counts every boundary in (frontier, now], so a jump past several
// boundaries counts them all.
type IntervalCore struct {
	period uint64
	next   uint64
	count  uint64
}

// NewIntervalCore creates a periodic core with the given period (clamped to >=1).
func NewIntervalCore(period uint64) *IntervalCore {
	if period < 1 {
		period = 1
	}
	return &IntervalCore{period: period, next: period}
}

// Count reports the total number of fires so far.
func (iv *IntervalCore) Count() uint64 { return iv.count }

// firesThisTick reports the boundaries crossed on a single tick (0 when now is
// below the frontier).
func (iv *IntervalCore) firesThisTick(now uint64) uint64 {
	if now < iv.next {
		return 0
	}
	return (now-iv.next)/iv.period + 1
}

// Tick advances to now; returns whether a boundary fired.
func (iv *IntervalCore) Tick(now uint64) bool {
	fires := iv.firesThisTick(now)
	if fires == 0 {
		return false
	}
	iv.count += fires
	iv.next += fires * iv.period
	return true
}

// NextFire reports the next boundary (always present for an interval).
func (iv *IntervalCore) NextFire() (uint64, bool) { return iv.next, true }

// IntervalCell is a reactive periodic interval: projects IntervalCore's fire
// count onto a cell (invalidates only when count changes).
type IntervalCell struct {
	core  *IntervalCore
	count *Source[uint64]
}

// NewIntervalCell creates a reactive periodic interval with the given period.
func NewIntervalCell(ctx *Context, period uint64) *IntervalCell {
	return &IntervalCell{core: NewIntervalCore(period), count: NewSource[uint64](ctx, 0)}
}

// Tick advances to logical time now; returns whether a boundary fired. The count
// cell mirrors the core's total fire count.
func (iv *IntervalCell) Tick(now uint64) bool {
	edge := iv.core.Tick(now)
	iv.count.Set(iv.core.Count())
	return edge
}

// Count reports the total fires so far (reactive read).
func (iv *IntervalCell) Count() uint64 { return iv.count.Get() }

// CountCell returns the backing count cell.
func (iv *IntervalCell) CountCell() *Source[uint64] { return iv.count }

// NextFire reports the next boundary.
func (iv *IntervalCell) NextFire() (uint64, bool) { return iv.core.NextFire() }

// ---------------------------------------------------------------------------
// Cron pattern
// ---------------------------------------------------------------------------

// countUpto counts m in 1..=n with m mod cycle == o (0 <= o < cycle).
func countUpto(n, o, cycle uint64) uint64 {
	switch {
	case o == 0:
		return n / cycle
	case o <= n:
		return (n-o)/cycle + 1
	default:
		return 0
	}
}

// CronCore is a pattern-periodic compute core: a tick m >= 1 fires iff m mod
// cycle is in offsets. The match count in (cursor, now] is computed
// arithmetically, so a large now jump is O(offsets).
type CronCore struct {
	cycle   uint64
	offsets []uint64
	cursor  uint64
	count   uint64
}

// NewCronCore creates a cron core. offsets are reduced mod cycle, sorted, and
// deduped; cycle is clamped to >=1; empty offsets means the source never fires.
func NewCronCore(cycle uint64, offsets []uint64) *CronCore {
	if cycle < 1 {
		cycle = 1
	}
	reduced := make([]uint64, 0, len(offsets))
	for _, o := range offsets {
		reduced = append(reduced, o%cycle)
	}
	sortUint64(reduced)
	deduped := reduced[:0]
	for i, o := range reduced {
		if i == 0 || o != deduped[len(deduped)-1] {
			deduped = append(deduped, o)
		}
	}
	return &CronCore{cycle: cycle, offsets: deduped}
}

// sortUint64 sorts a slice of uint64 in place (insertion sort; offsets are tiny).
func sortUint64(a []uint64) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j-1] > a[j]; j-- {
			a[j-1], a[j] = a[j], a[j-1]
		}
	}
}

// Count reports the total number of fires so far.
func (c *CronCore) Count() uint64 { return c.count }

func (c *CronCore) matchesIn(lo, hi uint64) uint64 {
	var sum uint64
	for _, o := range c.offsets {
		sum += countUpto(hi, o, c.cycle) - countUpto(lo, o, c.cycle)
	}
	return sum
}

// Tick advances to now; returns whether at least one pattern match fired.
func (c *CronCore) Tick(now uint64) bool {
	if now <= c.cursor {
		// cursor is monotone; now <= cursor leaves it unchanged.
		return false
	}
	fires := c.matchesIn(c.cursor, now)
	c.cursor = now
	if fires == 0 {
		return false
	}
	c.count += fires
	return true
}

// NextFire reports the smallest m > cursor with m mod cycle in offsets, or
// ok=false when offsets is empty.
func (c *CronCore) NextFire() (uint64, bool) {
	if len(c.offsets) == 0 {
		return 0, false
	}
	start := c.cursor + 1
	base := start / c.cycle * c.cycle
	for cyc := uint64(0); cyc < 2; cyc++ {
		block := base + cyc*c.cycle
		for _, o := range c.offsets {
			cand := block + o
			if cand >= start {
				return cand, true
			}
		}
	}
	return 0, false
}

// CronCell is a reactive cron source: same reactive contract as IntervalCell.
type CronCell struct {
	core  *CronCore
	count *Source[uint64]
}

// NewCronCell creates a reactive cron source.
func NewCronCell(ctx *Context, cycle uint64, offsets []uint64) *CronCell {
	return &CronCell{core: NewCronCore(cycle, offsets), count: NewSource[uint64](ctx, 0)}
}

// Tick advances to logical time now; returns whether a match fired.
func (c *CronCell) Tick(now uint64) bool {
	edge := c.core.Tick(now)
	c.count.Set(c.core.Count())
	return edge
}

// Count reports the total fires so far (reactive read).
func (c *CronCell) Count() uint64 { return c.count.Get() }

// CountCell returns the backing count cell.
func (c *CronCell) CountCell() *Source[uint64] { return c.count }

// NextFire reports the next matching time.
func (c *CronCell) NextFire() (uint64, bool) { return c.core.NextFire() }

// ---------------------------------------------------------------------------
// Value + deadline
// ---------------------------------------------------------------------------

// Deadlined pairs a value with a liveness state: Live until its deadline, then
// Expired — the value is preserved across the flip.
type Deadlined[T any] struct {
	expired bool
	value   T
}

// Live wraps a value in the live state.
func Live[T any](v T) Deadlined[T] { return Deadlined[T]{expired: false, value: v} }

// Expired wraps a value in the expired state.
func Expired[T any](v T) Deadlined[T] { return Deadlined[T]{expired: true, value: v} }

// IsExpired reports whether the value's deadline has passed.
func (d Deadlined[T]) IsExpired() bool { return d.expired }

// Value returns the preserved value (present in both Live and Expired).
func (d Deadlined[T]) Value() T { return d.value }

// DeadlineCore is a deadline compute core (bytes-eligible): a TimerCore over the
// deadline. The value lives in the reactive cell.
type DeadlineCore struct {
	timer TimerCore
}

// NewDeadlineCore creates a deadline core expiring at deadline.
func NewDeadlineCore(deadline uint64) *DeadlineCore {
	return &DeadlineCore{timer: TimerCore{fireAt: deadline}}
}

// IsExpired reports whether the deadline has passed.
func (d *DeadlineCore) IsExpired() bool { return d.timer.Fired() }

// Tick advances to now; returns the expiry edge.
func (d *DeadlineCore) Tick(now uint64) bool { return d.timer.Tick(now) }

// NextFire reports the deadline, or ok=false once expired.
func (d *DeadlineCore) NextFire() (uint64, bool) { return d.timer.NextFire() }

// DeadlineCell is a reactive value + deadline: flips Live(v) -> Expired(v) at
// the deadline, preserving the value; the state reader invalidates only on the
// expiry edge.
type DeadlineCell[T any] struct {
	core    *DeadlineCore
	value   T
	expired *Source[bool]
}

// NewDeadlineCell creates a reactive value + deadline pair.
func NewDeadlineCell[T any](ctx *Context, value T, deadline uint64) *DeadlineCell[T] {
	return &DeadlineCell[T]{
		core:    NewDeadlineCore(deadline),
		value:   value,
		expired: NewSource(ctx, false),
	}
}

// Tick advances to logical time now; returns the expiry edge.
func (d *DeadlineCell[T]) Tick(now uint64) bool {
	edge := d.core.Tick(now)
	d.expired.Set(d.core.IsExpired())
	return edge
}

// State returns the current state, preserving the value (reactive read).
func (d *DeadlineCell[T]) State() Deadlined[T] {
	if d.expired.Get() {
		return Expired(d.value)
	}
	return Live(d.value)
}

// IsExpired reports whether the deadline has passed (reactive read).
func (d *DeadlineCell[T]) IsExpired() bool { return d.expired.Get() }

// ExpiredCell returns the backing expiry cell.
func (d *DeadlineCell[T]) ExpiredCell() *Source[bool] { return d.expired }

// NextFire reports the deadline, or ok=false once expired.
func (d *DeadlineCell[T]) NextFire() (uint64, bool) { return d.core.NextFire() }
