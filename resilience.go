package lazily

// Fault-tolerance primitives (#lzresilience): circuit breaker / retry /
// bulkhead / timeout. Each is a pure compute core (a state machine / counter
// over a logical clock) split from a reactive cell projecting the salient
// scalar reader. Ported from lazily-rs/src/resilience.rs.

// ===========================================================================
// Circuit breaker
// ===========================================================================

// BreakerState is the circuit-breaker state.
type BreakerState int

const (
	// BreakerClosed — calls pass; failures accumulate in the window.
	BreakerClosed BreakerState = iota
	// BreakerOpen — fast-fail until the reset deadline.
	BreakerOpen
	// BreakerHalfOpen — allow a single probe.
	BreakerHalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case BreakerClosed:
		return "Closed"
	case BreakerOpen:
		return "Open"
	case BreakerHalfOpen:
		return "HalfOpen"
	default:
		return "Unknown"
	}
}

// CircuitBreakerCore is the circuit-breaker compute core: a sliding window of
// outcomes trips Closed->Open at failureThreshold; Open->HalfOpen at the
// deadline; a HalfOpen success closes, a failure re-opens.
type CircuitBreakerCore struct {
	window           int
	failureThreshold int
	resetTimeout     uint64
	state            BreakerState
	outcomes         []bool // true = success
	openUntil        uint64
}

// NewCircuitBreakerCore builds a core; window and failureThreshold clamp to >= 1.
func NewCircuitBreakerCore(window, failureThreshold int, resetTimeout uint64) *CircuitBreakerCore {
	if window < 1 {
		window = 1
	}
	if failureThreshold < 1 {
		failureThreshold = 1
	}
	return &CircuitBreakerCore{
		window:           window,
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
		state:            BreakerClosed,
	}
}

// State returns the current breaker state.
func (c *CircuitBreakerCore) State() BreakerState { return c.state }

func (c *CircuitBreakerCore) failures() int {
	n := 0
	for _, s := range c.outcomes {
		if !s {
			n++
		}
	}
	return n
}

// Allow reports whether a call is permitted; performs the Open->HalfOpen
// transition at the deadline.
func (c *CircuitBreakerCore) Allow(now uint64) bool {
	switch c.state {
	case BreakerClosed:
		return true
	case BreakerOpen:
		if now >= c.openUntil {
			c.state = BreakerHalfOpen
			return true
		}
		return false
	case BreakerHalfOpen:
		return true
	default:
		return false
	}
}

// Record feeds a call outcome and drives the state machine.
func (c *CircuitBreakerCore) Record(success bool, now uint64) {
	switch c.state {
	case BreakerHalfOpen:
		if success {
			c.state = BreakerClosed
			c.outcomes = c.outcomes[:0]
		} else {
			c.state = BreakerOpen
			c.openUntil = now + c.resetTimeout
		}
	case BreakerClosed:
		c.outcomes = append(c.outcomes, success)
		for len(c.outcomes) > c.window {
			c.outcomes = c.outcomes[1:]
		}
		if c.failures() >= c.failureThreshold {
			c.state = BreakerOpen
			c.openUntil = now + c.resetTimeout
		}
	case BreakerOpen:
		// no-op
	}
}

// CircuitBreakerCell is a reactive circuit breaker: projects the state onto a Cell.
type CircuitBreakerCell struct {
	core  *CircuitBreakerCore
	state *Source[BreakerState]
}

// NewCircuitBreakerCell builds a reactive circuit breaker.
func NewCircuitBreakerCell(ctx *Context, window, failureThreshold int, resetTimeout uint64) *CircuitBreakerCell {
	return &CircuitBreakerCell{
		core:  NewCircuitBreakerCore(window, failureThreshold, resetTimeout),
		state: NewSource[BreakerState](ctx, BreakerClosed),
	}
}

func (c *CircuitBreakerCell) refresh() { c.state.Set(c.core.State()) }

// Allow reports whether a call is permitted, updating the projected state.
func (c *CircuitBreakerCell) Allow(now uint64) bool {
	r := c.core.Allow(now)
	c.refresh()
	return r
}

// Record feeds a call outcome, updating the projected state.
func (c *CircuitBreakerCell) Record(success bool, now uint64) {
	c.core.Record(success, now)
	c.refresh()
}

// State returns the current breaker state.
func (c *CircuitBreakerCell) State() BreakerState { return c.core.State() }

// StateCell returns the reactive state reader.
func (c *CircuitBreakerCell) StateCell() *Source[BreakerState] { return c.state }

// ===========================================================================
// Retry backoff
// ===========================================================================

// RetryPolicyCore is the exponential-backoff compute core:
// delay(attempt) = min(cap, base*2^attempt), saturating to cap on shift overflow.
type RetryPolicyCore struct {
	base    uint64
	cap     uint64
	attempt uint32
}

// NewRetryPolicyCore builds a core.
func NewRetryPolicyCore(base, capacity uint64) *RetryPolicyCore {
	return &RetryPolicyCore{base: base, cap: capacity}
}

// Delay returns the delay for attempt, saturating at cap.
func (r *RetryPolicyCore) Delay(attempt uint32) uint64 {
	// base << attempt, saturating to cap on overflow (>= 64-bit shift or
	// numeric overflow both saturate to cap, mirroring rs checked_shl).
	if attempt >= 64 {
		return r.cap
	}
	shifted := r.base << attempt
	// Detect overflow: if shifting back does not recover base, it overflowed.
	if (shifted >> attempt) != r.base {
		return r.cap
	}
	if shifted < r.cap {
		return shifted
	}
	return r.cap
}

// NextDelay returns the current attempt's delay, then advances.
func (r *RetryPolicyCore) NextDelay() uint64 {
	d := r.Delay(r.attempt)
	if r.attempt != ^uint32(0) {
		r.attempt++
	}
	return d
}

// Reset resets the attempt counter.
func (r *RetryPolicyCore) Reset() { r.attempt = 0 }

// RetryPolicyCell is a reactive retry policy: projects the current delay onto a Cell.
type RetryPolicyCell struct {
	core  *RetryPolicyCore
	delay *Source[uint64]
}

// NewRetryPolicyCell builds a reactive retry policy.
func NewRetryPolicyCell(ctx *Context, base, capacity uint64) *RetryPolicyCell {
	return &RetryPolicyCell{
		core:  NewRetryPolicyCore(base, capacity),
		delay: NewSource[uint64](ctx, 0),
	}
}

// NextDelay returns the current attempt's delay, advances, and projects it.
func (r *RetryPolicyCell) NextDelay() uint64 {
	d := r.core.NextDelay()
	r.delay.Set(d)
	return d
}

// Reset resets the attempt counter and the projected delay.
func (r *RetryPolicyCell) Reset() {
	r.core.Reset()
	r.delay.Set(0)
}

// Delay returns the current projected delay.
func (r *RetryPolicyCell) Delay() uint64 { return r.delay.Get() }

// DelayCell returns the reactive delay reader.
func (r *RetryPolicyCell) DelayCell() *Source[uint64] { return r.delay }

// ===========================================================================
// Bulkhead
// ===========================================================================

// BulkheadCore is a bounded isolation-pool compute core.
type BulkheadCore struct {
	capacity uint64
	inUse    uint64
}

// NewBulkheadCore builds a core.
func NewBulkheadCore(capacity uint64) *BulkheadCore {
	return &BulkheadCore{capacity: capacity}
}

// InUse returns the number of permits in use.
func (b *BulkheadCore) InUse() uint64 { return b.inUse }

// Acquire takes a permit if one is free.
func (b *BulkheadCore) Acquire() bool {
	if b.inUse < b.capacity {
		b.inUse++
		return true
	}
	return false
}

// Release frees a permit.
func (b *BulkheadCore) Release() {
	if b.inUse > 0 {
		b.inUse--
	}
}

// BulkheadCell is a reactive bulkhead: projects permitsInUse onto a Cell.
type BulkheadCell struct {
	core  *BulkheadCore
	inUse *Source[uint64]
}

// NewBulkheadCell builds a reactive bulkhead.
func NewBulkheadCell(ctx *Context, capacity uint64) *BulkheadCell {
	return &BulkheadCell{
		core:  NewBulkheadCore(capacity),
		inUse: NewSource[uint64](ctx, 0),
	}
}

func (b *BulkheadCell) refresh() { b.inUse.Set(b.core.InUse()) }

// Acquire takes a permit if one is free, updating the projection.
func (b *BulkheadCell) Acquire() bool {
	r := b.core.Acquire()
	b.refresh()
	return r
}

// Release frees a permit, updating the projection.
func (b *BulkheadCell) Release() {
	b.core.Release()
	b.refresh()
}

// PermitsInUse returns the projected number of permits in use.
func (b *BulkheadCell) PermitsInUse() uint64 { return b.inUse.Get() }

// PermitsInUseCell returns the reactive permits-in-use reader.
func (b *BulkheadCell) PermitsInUseCell() *Source[uint64] { return b.inUse }

// ===========================================================================
// Timeout
// ===========================================================================

// TimeoutCore is a deadline-bounded call compute core.
type TimeoutCore struct {
	deadline uint64
	armed    bool
	timedOut bool
}

// NewTimeoutCore builds a core.
func NewTimeoutCore() *TimeoutCore { return &TimeoutCore{} }

// Arm arms the timeout with deadline = now + timeout.
func (t *TimeoutCore) Arm(now, timeout uint64) {
	t.deadline = now + timeout
	t.armed = true
	t.timedOut = false
}

// Tick fast-fails when now >= deadline; returns the timeout edge (once).
func (t *TimeoutCore) Tick(now uint64) bool {
	if t.armed && !t.timedOut && now >= t.deadline {
		t.timedOut = true
		return true
	}
	return false
}

// IsTimedOut reports whether the timeout has fired.
func (t *TimeoutCore) IsTimedOut() bool { return t.timedOut }

// TimeoutCell is a reactive timeout: projects isTimedOut onto a Cell.
type TimeoutCell struct {
	core     *TimeoutCore
	timedOut *Source[bool]
}

// NewTimeoutCell builds a reactive timeout.
func NewTimeoutCell(ctx *Context) *TimeoutCell {
	return &TimeoutCell{
		core:     NewTimeoutCore(),
		timedOut: NewSource[bool](ctx, false),
	}
}

func (t *TimeoutCell) refresh() { t.timedOut.Set(t.core.IsTimedOut()) }

// Arm arms the timeout with deadline = now + timeout.
func (t *TimeoutCell) Arm(now, timeout uint64) {
	t.core.Arm(now, timeout)
	t.refresh()
}

// Tick fast-fails when now >= deadline; returns the timeout edge (once).
func (t *TimeoutCell) Tick(now uint64) bool {
	r := t.core.Tick(now)
	t.refresh()
	return r
}

// IsTimedOut reports the projected timeout state.
func (t *TimeoutCell) IsTimedOut() bool { return t.timedOut.Get() }

// IsTimedOutCell returns the reactive is-timed-out reader.
func (t *TimeoutCell) IsTimedOutCell() *Source[bool] { return t.timedOut }
