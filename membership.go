package lazily

// Membership + failure detection (#lzmemb): a Phi-accrual failure detector plus
// a SWIM-style peer state machine, projected onto a reactive alive PeerSet.
//
// The pure compute core (PhiAccrual + MembershipCore[P]) is the Phi-accrual math
// and SWIM state machine over plain state; the reactive MembershipCell[P]
// projects the alive set onto a version Cell so PeerSet invalidates only when the
// set changes. The peer id is generic (cmp.Ordered); the distributed plane plugs
// in its own PeerId. This lives below the CRDT plane.

import (
	"cmp"
	"math"
	"slices"
)

// PeerState is the per-peer liveness state (SWIM).
type PeerState int

const (
	// Alive — heartbeats current; a valid CRDT sync target.
	Alive PeerState = iota
	// Suspect — phi crossed the threshold; awaiting a refuting heartbeat or the
	// suspect timeout.
	Suspect
	// Dead — suspect long enough to be declared failed.
	Dead
	// Left — gracefully departed.
	Left
)

// String renders the SWIM state name (matches the conformance fixture labels).
func (s PeerState) String() string {
	switch s {
	case Alive:
		return "Alive"
	case Suspect:
		return "Suspect"
	case Dead:
		return "Dead"
	case Left:
		return "Left"
	default:
		return "Unknown"
	}
}

// PeerChangeKind discriminates the PeerChangeEvent variants.
type PeerChangeKind int

const (
	// PeerJoined — a previously unknown peer joined.
	PeerJoined PeerChangeKind = iota
	// PeerDeparted — a peer gracefully left.
	PeerDeparted
	// PeerStateChanged — a known peer transitioned between states.
	PeerStateChanged
)

// PeerChangeEvent is a diff event over the membership cell. For PeerJoined and
// PeerDeparted, only Peer is meaningful; for PeerStateChanged, From/To carry the
// transition.
type PeerChangeEvent[P cmp.Ordered] struct {
	Kind PeerChangeKind
	Peer P
	From PeerState
	To   PeerState
}

// MembershipConfig holds the failure-detector + SWIM tunables.
type MembershipConfig struct {
	// PhiThreshold — phi > PhiThreshold marks a peer Suspect.
	PhiThreshold float64
	// SuspectTimeout — ticks a peer stays Suspect before being declared Dead.
	SuspectTimeout uint64
	// MaxSamples — sliding window size for heartbeat inter-arrival samples.
	MaxSamples int
	// MinStd — floor on the sample standard deviation (avoids div-by-zero).
	MinStd float64
}

// DefaultMembershipConfig returns the standard tunables.
func DefaultMembershipConfig() MembershipConfig {
	return MembershipConfig{
		PhiThreshold:   8.0,
		SuspectTimeout: 5,
		MaxSamples:     100,
		MinStd:         0.1,
	}
}

// saturatingSubU64 mirrors Rust's u64::saturating_sub.
func saturatingSubU64(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}

// PhiAccrual is a Phi-accrual failure detector over a sliding window of
// heartbeat inter-arrival times. phi is bit-portable across bindings via the
// Akka-style logistic approximation of the normal CDF.
type PhiAccrual struct {
	window        []float64
	maxSamples    int
	minStd        float64
	lastHeartbeat uint64
	hasLast       bool
}

// NewPhiAccrual builds a detector with the given window bound and std floor.
func NewPhiAccrual(maxSamples int, minStd float64) *PhiAccrual {
	if maxSamples < 1 {
		maxSamples = 1
	}
	return &PhiAccrual{
		window:     nil,
		maxSamples: maxSamples,
		minStd:     minStd,
	}
}

// Heartbeat records a heartbeat arrival, appending its inter-arrival sample.
func (d *PhiAccrual) Heartbeat(now uint64) {
	if d.hasLast {
		interval := float64(saturatingSubU64(now, d.lastHeartbeat))
		d.window = append(d.window, interval)
		for len(d.window) > d.maxSamples {
			d.window = d.window[1:]
		}
	}
	d.lastHeartbeat = now
	d.hasLast = true
}

func (d *PhiAccrual) mean() float64 {
	n := float64(len(d.window))
	sum := 0.0
	for _, x := range d.window {
		sum += x
	}
	return sum / n
}

func (d *PhiAccrual) std(mean float64) float64 {
	n := float64(len(d.window))
	varSum := 0.0
	for _, x := range d.window {
		varSum += (x - mean) * (x - mean)
	}
	variance := varSum / n
	return math.Max(math.Sqrt(variance), d.minStd)
}

// Phi is the suspicion level at now. 0.0 when there is no estimate yet.
func (d *PhiAccrual) Phi(now uint64) float64 {
	if !d.hasLast {
		return 0.0
	}
	if len(d.window) == 0 {
		return 0.0
	}
	elapsed := float64(saturatingSubU64(now, d.lastHeartbeat))
	mean := d.mean()
	std := d.std(mean)
	y := (elapsed - mean) / std
	e := math.Exp(-y * (1.5976 + 0.070566*y*y))
	if elapsed > mean {
		return -math.Log10(e / (1.0 + e))
	}
	return -math.Log10(1.0 - 1.0/(1.0+e))
}

// peerRecord is the mutable per-peer SWIM state.
type peerRecord struct {
	state           PeerState
	detector        *PhiAccrual
	suspectSince    uint64
	hasSuspectSince bool
}

// MembershipCore is the pure SWIM state machine over a keyed peer map, driven by
// heartbeats and a logical clock. It emits PeerChangeEvent diffs.
type MembershipCore[P cmp.Ordered] struct {
	config MembershipConfig
	peers  map[P]*peerRecord
}

// NewMembershipCore builds an empty core with the given config.
func NewMembershipCore[P cmp.Ordered](config MembershipConfig) *MembershipCore[P] {
	return &MembershipCore[P]{
		config: config,
		peers:  map[P]*peerRecord{},
	}
}

func (m *MembershipCore[P]) newDetector() *PhiAccrual {
	return NewPhiAccrual(m.config.MaxSamples, m.config.MinStd)
}

// sortedPeers returns the peer keys in ascending order for deterministic
// iteration (mirrors the rs BTreeMap ordering).
func (m *MembershipCore[P]) sortedPeers() []P {
	keys := make([]P, 0, len(m.peers))
	for p := range m.peers {
		keys = append(keys, p)
	}
	slices.Sort(keys)
	return keys
}

// AliveSet returns the current alive peer set as a sorted slice (the reactive
// PeerSet).
func (m *MembershipCore[P]) AliveSet() []P {
	out := make([]P, 0, len(m.peers))
	for _, p := range m.sortedPeers() {
		if m.peers[p].state == Alive {
			out = append(out, p)
		}
	}
	return out
}

// State returns the state of a known peer.
func (m *MembershipCore[P]) State(peer P) (PeerState, bool) {
	r, ok := m.peers[peer]
	if !ok {
		return Alive, false
	}
	return r.state, true
}

// Join adds a peer (or refreshes a re-joining one): Alive with a fresh detector.
func (m *MembershipCore[P]) Join(peer P, now uint64) []PeerChangeEvent[P] {
	detector := m.newDetector()
	detector.Heartbeat(now)
	prev, known := m.peers[peer]
	var prevState PeerState
	if known {
		prevState = prev.state
	}
	m.peers[peer] = &peerRecord{
		state:    Alive,
		detector: detector,
	}
	switch {
	case !known:
		return []PeerChangeEvent[P]{{Kind: PeerJoined, Peer: peer}}
	case prevState == Alive:
		return nil
	default:
		return []PeerChangeEvent[P]{{Kind: PeerStateChanged, Peer: peer, From: prevState, To: Alive}}
	}
}

// Heartbeat records a heartbeat. An unknown peer is a join; a Suspect/Dead peer
// returns to Alive (SWIM refutation).
func (m *MembershipCore[P]) Heartbeat(peer P, now uint64) []PeerChangeEvent[P] {
	record, ok := m.peers[peer]
	if !ok {
		return m.Join(peer, now)
	}
	record.detector.Heartbeat(now)
	from := record.state
	if from != Alive && from != Left {
		record.state = Alive
		record.hasSuspectSince = false
		return []PeerChangeEvent[P]{{Kind: PeerStateChanged, Peer: peer, From: from, To: Alive}}
	}
	return nil
}

// Leave records a graceful departure.
func (m *MembershipCore[P]) Leave(peer P, _ uint64) []PeerChangeEvent[P] {
	record, ok := m.peers[peer]
	if !ok {
		return nil
	}
	if record.state == Left {
		return nil
	}
	record.state = Left
	record.hasSuspectSince = false
	return []PeerChangeEvent[P]{{Kind: PeerDeparted, Peer: peer}}
}

// Tick advances the clock: escalate Alive -> Suspect (phi crossed) and
// Suspect -> Dead (timeout elapsed).
func (m *MembershipCore[P]) Tick(now uint64) []PeerChangeEvent[P] {
	threshold := m.config.PhiThreshold
	timeout := m.config.SuspectTimeout
	var events []PeerChangeEvent[P]
	for _, peer := range m.sortedPeers() {
		record := m.peers[peer]
		switch record.state {
		case Alive:
			if record.detector.Phi(now) > threshold {
				record.state = Suspect
				record.suspectSince = now
				record.hasSuspectSince = true
				events = append(events, PeerChangeEvent[P]{Kind: PeerStateChanged, Peer: peer, From: Alive, To: Suspect})
			}
		case Suspect:
			expired := record.hasSuspectSince && saturatingSubU64(now, record.suspectSince) >= timeout
			if expired {
				record.state = Dead
				events = append(events, PeerChangeEvent[P]{Kind: PeerStateChanged, Peer: peer, From: Suspect, To: Dead})
			}
		default:
			// Dead | Left — terminal.
		}
	}
	return events
}

// MembershipCell is the reactive membership view: it drives a MembershipCore and
// projects the alive set onto a version Cell so PeerSet invalidates only on a set
// change (mirrors the rs Cell<BTreeSet<P>> PartialEq guard).
type MembershipCell[P cmp.Ordered] struct {
	ctx     *Context
	core    *MembershipCore[P]
	version *SourceCell[uint64]
	alive   []P // last projected alive set (sorted)
}

// NewMembershipCell builds a reactive membership cell bound to ctx.
func NewMembershipCell[P cmp.Ordered](ctx *Context, config MembershipConfig) *MembershipCell[P] {
	c := &MembershipCell[P]{
		ctx:     ctx,
		core:    NewMembershipCore[P](config),
		version: NewSourceCell(ctx, uint64(0)),
		alive:   nil,
	}
	return c
}

// refresh recomputes the alive set; it bumps the version only when the set
// structurally changed, matching the rs "invalidate only on change" semantics.
func (c *MembershipCell[P]) refresh() {
	next := c.core.AliveSet()
	if !slices.Equal(c.alive, next) {
		c.alive = next
		c.version.Set(c.version.Peek() + 1)
	}
}

// Join adds/refreshes a peer, then refreshes the projection.
func (c *MembershipCell[P]) Join(peer P, now uint64) []PeerChangeEvent[P] {
	events := c.core.Join(peer, now)
	c.refresh()
	return events
}

// Heartbeat records a heartbeat, then refreshes the projection.
func (c *MembershipCell[P]) Heartbeat(peer P, now uint64) []PeerChangeEvent[P] {
	events := c.core.Heartbeat(peer, now)
	c.refresh()
	return events
}

// Leave records a graceful departure, then refreshes the projection.
func (c *MembershipCell[P]) Leave(peer P, now uint64) []PeerChangeEvent[P] {
	events := c.core.Leave(peer, now)
	c.refresh()
	return events
}

// Tick advances the clock, then refreshes the projection.
func (c *MembershipCell[P]) Tick(now uint64) []PeerChangeEvent[P] {
	events := c.core.Tick(now)
	c.refresh()
	return events
}

// PeerSet returns a fresh snapshot of the alive peer set (sorted). Reading it
// inside a computation subscribes the reader to the alive-set version, so it
// invalidates only when the set changes.
func (c *MembershipCell[P]) PeerSet() []P {
	_ = c.version.Get()
	out := make([]P, len(c.alive))
	copy(out, c.alive)
	return out
}

// VersionCell exposes the backing version Cell for direct subscription.
func (c *MembershipCell[P]) VersionCell() *SourceCell[uint64] { return c.version }

// State returns the state of a known peer.
func (c *MembershipCell[P]) State(peer P) (PeerState, bool) {
	return c.core.State(peer)
}
