package lazily

// Distributed coordination (#lzcoord): lease / leader / lock / semaphore /
// barrier + quorum primitives. Each is a pure compute core (a state machine
// over integers / peer ids) split from a reactive cell projecting the salient
// reader onto a Cell. Time is the logical clock; expiry is a tick value the
// runtime drives.
//
// Port of lazily-rs src/coordination.rs. See lazily-spec/docs/coordination.md
// and lazily-spec/conformance/coordination/*.json.

// ===========================================================================
// Lease + fencing token
// ===========================================================================

// LeaseCore is a single-writer lease authority with a monotone fencing token.
type LeaseCore[P comparable] struct {
	holder Opt[P]
	expiry uint64
	fence  uint64
}

// NewLeaseCore returns an empty lease core.
func NewLeaseCore[P comparable]() *LeaseCore[P] {
	return &LeaseCore[P]{}
}

func (c *LeaseCore[P]) isExpired(now uint64) bool {
	return c.holder.Present && now >= c.expiry
}

// IsHeld reports whether the lease is currently held (and not expired at now).
func (c *LeaseCore[P]) IsHeld(now uint64) bool {
	return c.holder.Present && !c.isExpired(now)
}

// holderOpt is the live holder projection at now.
func (c *LeaseCore[P]) holderOpt(now uint64) Opt[P] {
	if c.IsHeld(now) {
		return c.holder
	}
	return Opt[P]{}
}

// Holder returns the live holder at now.
func (c *LeaseCore[P]) Holder(now uint64) (P, bool) {
	h := c.holderOpt(now)
	return h.Value, h.Present
}

// Fence returns the current fencing token.
func (c *LeaseCore[P]) Fence() uint64 { return c.fence }

// Acquire grants if free/expired (new grant increments fence); renew by the
// holder keeps the same fence; held by another -> (0, false). The bool reports
// whether a token was granted.
func (c *LeaseCore[P]) Acquire(peer P, now, ttl uint64) (uint64, bool) {
	free := !c.holder.Present || c.isExpired(now)
	if free {
		c.fence++
		c.holder = Opt[P]{Present: true, Value: peer}
		c.expiry = now + ttl
		return c.fence, true
	}
	if c.holder.Present && c.holder.Value == peer {
		c.expiry = now + ttl // renew keeps fence
		return c.fence, true
	}
	return 0, false
}

// Renew extends the expiry if peer is the live holder.
func (c *LeaseCore[P]) Renew(peer P, now, ttl uint64) bool {
	if c.IsHeld(now) && c.holder.Present && c.holder.Value == peer {
		c.expiry = now + ttl
		return true
	}
	return false
}

// Release drops the grant if peer holds it.
func (c *LeaseCore[P]) Release(peer P) {
	if c.holder.Present && c.holder.Value == peer {
		c.holder = Opt[P]{}
	}
}

// Tick expires the grant when now >= expiry; returns the expiry edge.
func (c *LeaseCore[P]) Tick(now uint64) bool {
	if c.isExpired(now) {
		c.holder = Opt[P]{}
		return true
	}
	return false
}

// LeaseCell is a reactive lease: projects the holder onto a Cell (invalidates
// on holder change).
type LeaseCell[P comparable] struct {
	core   *LeaseCore[P]
	holder *Source[Opt[P]]
}

// NewLeaseCell constructs a reactive lease.
func NewLeaseCell[P comparable](ctx *Context) *LeaseCell[P] {
	return &LeaseCell[P]{
		core:   NewLeaseCore[P](),
		holder: NewSource[Opt[P]](ctx, Opt[P]{}),
	}
}

func (c *LeaseCell[P]) refresh(now uint64) {
	c.holder.Set(c.core.holderOpt(now))
}

// Acquire grants the lease, returning the fencing token (present=false if
// denied).
func (c *LeaseCell[P]) Acquire(peer P, now, ttl uint64) Opt[uint64] {
	f, ok := c.core.Acquire(peer, now, ttl)
	c.refresh(now)
	return Opt[uint64]{Present: ok, Value: f}
}

// Renew extends the expiry if peer is the live holder.
func (c *LeaseCell[P]) Renew(peer P, now, ttl uint64) bool {
	r := c.core.Renew(peer, now, ttl)
	c.refresh(now)
	return r
}

// Release drops the grant if peer holds it.
func (c *LeaseCell[P]) Release(peer P, now uint64) {
	c.core.Release(peer)
	c.refresh(now)
}

// Tick expires the grant when now >= expiry; returns the expiry edge.
func (c *LeaseCell[P]) Tick(now uint64) bool {
	r := c.core.Tick(now)
	c.refresh(now)
	return r
}

// Holder returns the live holder at now.
func (c *LeaseCell[P]) Holder(now uint64) (P, bool) { return c.core.Holder(now) }

// IsHeld reports whether the lease is currently held at now.
func (c *LeaseCell[P]) IsHeld(now uint64) bool { return c.core.IsHeld(now) }

// Fence returns the current fencing token.
func (c *LeaseCell[P]) Fence() uint64 { return c.core.Fence() }

// HolderCell exposes the reactive holder projection.
func (c *LeaseCell[P]) HolderCell() *Source[Opt[P]] { return c.holder }

// ===========================================================================
// Leader / follower / candidate
// ===========================================================================

// LeaderRole is the local node's role, derived from lease ownership.
type LeaderRole int

const (
	// Leader — the local node holds the lease.
	Leader LeaderRole = iota
	// Follower — another peer holds the lease.
	Follower
	// Candidate — the lease is free.
	Candidate
)

// String renders the role name (matches fixture strings).
func (r LeaderRole) String() string {
	switch r {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	default:
		return "Candidate"
	}
}

// LeaderCell is reactive leadership over a lease from node me's perspective.
type LeaderCell[P comparable] struct {
	core          *LeaseCore[P]
	me            P
	currentLeader *Source[Opt[P]]
}

// NewLeaderCell constructs reactive leadership for node me.
func NewLeaderCell[P comparable](ctx *Context, me P) *LeaderCell[P] {
	return &LeaderCell[P]{
		core:          NewLeaseCore[P](),
		me:            me,
		currentLeader: NewSource[Opt[P]](ctx, Opt[P]{}),
	}
}

func (c *LeaderCell[P]) refresh(now uint64) {
	c.currentLeader.Set(c.core.holderOpt(now))
}

// Campaign tries to acquire leadership for me.
func (c *LeaderCell[P]) Campaign(now, ttl uint64) LeaderRole {
	c.core.Acquire(c.me, now, ttl)
	c.refresh(now)
	return c.Role(now)
}

// Contend simulates another peer contending (for tests / co-hosted nodes).
func (c *LeaderCell[P]) Contend(peer P, now, ttl uint64) LeaderRole {
	c.core.Acquire(peer, now, ttl)
	c.refresh(now)
	return c.Role(now)
}

// Tick advances the logical clock, expiring the lease if due.
func (c *LeaderCell[P]) Tick(now uint64) LeaderRole {
	c.core.Tick(now)
	c.refresh(now)
	return c.Role(now)
}

// CurrentLeader returns the live leader at now.
func (c *LeaderCell[P]) CurrentLeader(now uint64) (P, bool) { return c.core.Holder(now) }

// Role derives the local node's role at now.
func (c *LeaderCell[P]) Role(now uint64) LeaderRole {
	h := c.core.holderOpt(now)
	switch {
	case h.Present && h.Value == c.me:
		return Leader
	case h.Present:
		return Follower
	default:
		return Candidate
	}
}

// CurrentLeaderCell exposes the reactive current-leader projection.
func (c *LeaderCell[P]) CurrentLeaderCell() *Source[Opt[P]] { return c.currentLeader }

// ===========================================================================
// Distributed lock + fencing
// ===========================================================================

// LockCell is a reactive distributed mutex over a lease + fencing token.
type LockCell[P comparable] struct {
	core     *LeaseCore[P]
	isLocked *Source[bool]
}

// NewLockCell constructs a reactive distributed lock.
func NewLockCell[P comparable](ctx *Context) *LockCell[P] {
	return &LockCell[P]{
		core:     NewLeaseCore[P](),
		isLocked: NewSource[bool](ctx, false),
	}
}

func (c *LockCell[P]) refresh(now uint64) {
	c.isLocked.Set(c.core.IsHeld(now))
}

// Acquire acquires the lock, returning a fencing token (present=false if held).
func (c *LockCell[P]) Acquire(peer P, now, ttl uint64) Opt[uint64] {
	f, ok := c.core.Acquire(peer, now, ttl)
	c.refresh(now)
	return Opt[uint64]{Present: ok, Value: f}
}

// Release drops the lock if peer holds it.
func (c *LockCell[P]) Release(peer P, now uint64) {
	c.core.Release(peer)
	c.refresh(now)
}

// Tick expires the lock when now >= expiry; returns the expiry edge.
func (c *LockCell[P]) Tick(now uint64) bool {
	r := c.core.Tick(now)
	c.refresh(now)
	return r
}

// Validate reports whether fence is the current (non-stale) fencing token.
func (c *LockCell[P]) Validate(fence uint64) bool {
	return c.core.Fence() == fence
}

// IsLocked reports whether the lock is held at now.
func (c *LockCell[P]) IsLocked(now uint64) bool { return c.core.IsHeld(now) }

// Fence returns the current fencing token.
func (c *LockCell[P]) Fence() uint64 { return c.core.Fence() }

// IsLockedCell exposes the reactive is_locked projection.
func (c *LockCell[P]) IsLockedCell() *Source[bool] { return c.isLocked }

// ===========================================================================
// Semaphore
// ===========================================================================

// SemaphoreCore is a bounded permit pool compute core.
type SemaphoreCore struct {
	capacity uint64
	acquired uint64
}

// NewSemaphoreCore returns a permit pool of the given capacity.
func NewSemaphoreCore(capacity uint64) *SemaphoreCore {
	return &SemaphoreCore{capacity: capacity}
}

// Available returns the number of free permits.
func (c *SemaphoreCore) Available() uint64 { return c.capacity - c.acquired }

// Acquire takes a permit if one is available.
func (c *SemaphoreCore) Acquire() bool {
	if c.acquired < c.capacity {
		c.acquired++
		return true
	}
	return false
}

// Release returns a permit, saturating at capacity.
func (c *SemaphoreCore) Release() {
	if c.acquired > 0 {
		c.acquired--
	}
}

// SemaphoreCell is a reactive semaphore: projects permits_available onto a Cell.
type SemaphoreCell struct {
	core      *SemaphoreCore
	available *Source[uint64]
}

// NewSemaphoreCell constructs a reactive semaphore of the given capacity.
func NewSemaphoreCell(ctx *Context, capacity uint64) *SemaphoreCell {
	return &SemaphoreCell{
		core:      NewSemaphoreCore(capacity),
		available: NewSource[uint64](ctx, capacity),
	}
}

func (c *SemaphoreCell) refresh() {
	c.available.Set(c.core.Available())
}

// Acquire takes a permit if one is available.
func (c *SemaphoreCell) Acquire() bool {
	r := c.core.Acquire()
	c.refresh()
	return r
}

// Release returns a permit, saturating at capacity.
func (c *SemaphoreCell) Release() {
	c.core.Release()
	c.refresh()
}

// PermitsAvailable returns the number of free permits.
func (c *SemaphoreCell) PermitsAvailable() uint64 { return c.core.Available() }

// PermitsAvailableCell exposes the reactive permits_available projection.
func (c *SemaphoreCell) PermitsAvailableCell() *Source[uint64] { return c.available }

// ===========================================================================
// Barrier / quorum
// ===========================================================================

// BarrierCore is a wait-for-N gate compute core over distinct arriving peers.
type BarrierCore[P comparable] struct {
	required uint64
	arrived  map[P]struct{}
}

// NewBarrierCore returns a barrier that opens once required distinct peers
// arrive.
func NewBarrierCore[P comparable](required uint64) *BarrierCore[P] {
	return &BarrierCore[P]{required: required, arrived: map[P]struct{}{}}
}

// Arrive registers a distinct arrival; returns whether the gate is open after.
func (c *BarrierCore[P]) Arrive(peer P) bool {
	c.arrived[peer] = struct{}{}
	return c.IsOpen()
}

// Count returns the number of distinct arrivals.
func (c *BarrierCore[P]) Count() uint64 { return uint64(len(c.arrived)) }

// IsOpen reports whether the gate has opened.
func (c *BarrierCore[P]) IsOpen() bool { return c.Count() >= c.required }

// BarrierCell is a reactive wait-for-N gate. Quorum is a barrier with
// required = total/2 + 1.
type BarrierCell[P comparable] struct {
	core   *BarrierCore[P]
	isOpen *Source[bool]
}

// NewBarrierCell constructs a reactive wait-for-N gate.
func NewBarrierCell[P comparable](ctx *Context, required uint64) *BarrierCell[P] {
	core := NewBarrierCore[P](required)
	return &BarrierCell[P]{
		core:   core,
		isOpen: NewSource[bool](ctx, core.IsOpen()),
	}
}

// Quorum constructs a quorum gate: opens at strict majority of total.
func Quorum[P comparable](ctx *Context, total uint64) *BarrierCell[P] {
	return NewBarrierCell[P](ctx, total/2+1)
}

func (c *BarrierCell[P]) refresh() {
	c.isOpen.Set(c.core.IsOpen())
}

// Arrive registers an arrival / vote; returns whether the gate is open after.
func (c *BarrierCell[P]) Arrive(peer P) bool {
	r := c.core.Arrive(peer)
	c.refresh()
	return r
}

// Count returns the number of distinct arrivals.
func (c *BarrierCell[P]) Count() uint64 { return c.core.Count() }

// IsOpen reports whether the gate has opened.
func (c *BarrierCell[P]) IsOpen() bool { return c.core.IsOpen() }

// IsOpenCell exposes the reactive is_open projection.
func (c *BarrierCell[P]) IsOpenCell() *Source[bool] { return c.isOpen }
