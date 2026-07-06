package lazily

// Distributed CRDT plane runtime (protocol.md § Distributed: CRDT Cell Plane).
//
// This module carries the runtime side of the CRDT plane: the per-peer stamp
// frontier (StampFrontier) with its commutative/associative/idempotent Merge and
// the causal-stability watermark (the min over membership), and the CrdtPlane
// that wires the shared Hlc (hlc.go) + a StampFrontier + a live membership set.
//
// The wire mirror of these runtime types is WireStamp / CrdtOp / CrdtSync in
// ipc.go; the two layers never depend on each other's representation, only on
// the shared (wall_time, logical, peer) total order. The HlcStamp <-> WireStamp
// conversion (HlcStamp.ToWire / HlcStampFromWire) lives here.
//
// Mirrors lazily-rs src/crdt.rs (Hlc, StampFrontier, CrdtPlane) and the
// formally-proven invariants in lazily-spec/formal/lean/LazilyFormal/CRDT.lean
// (stampJoin_{comm,assoc,idem}, collectable_implies_observed_everywhere).
//
// Hlc / HlcStamp themselves live in hlc.go and are shared with seq_crdt.go; this
// module does not redefine them. WireStamp and StampFrontierEntry are owned by
// ipc.go and referenced by name here.

import "sort"

// ToWire converts a runtime HlcStamp to its wire mirror (WireStamp, owned by
// ipc.go). The two are isomorphic and convert losslessly at the boundary.
func (s HlcStamp) ToWire() WireStamp {
	return WireStamp{WallTime: s.WallTime, Logical: s.Logical, Peer: s.Peer}
}

// HlcStampFromWire converts a wire WireStamp back to the runtime HlcStamp.
func HlcStampFromWire(stamp WireStamp) HlcStamp {
	return HlcStamp{WallTime: stamp.WallTime, Logical: stamp.Logical, Peer: stamp.Peer}
}

// StampFrontier is the per-peer stamp frontier: the highest HlcStamp observed
// from each peer.
//
// Mirrors lazily-rs StampFrontier (a BTreeMap<PeerId, HlcStamp>). The merge laws
// (commutative, associative, idempotent) are formally proven
// (stampJoin_{comm,assoc,idem}): fold Observe over an incoming frontier in any
// order and the result is identical.
type StampFrontier struct {
	stamps map[PeerId]HlcStamp
}

// NewStampFrontier creates an empty frontier.
func NewStampFrontier() *StampFrontier {
	return &StampFrontier{stamps: map[PeerId]HlcStamp{}}
}

// StampFrontierFromWire builds a frontier from (peer, stamp) wire entries.
func StampFrontierFromWire(entries []StampFrontierEntry) *StampFrontier {
	f := NewStampFrontier()
	for _, e := range entries {
		f.Observe(e.Peer, HlcStampFromWire(e.Stamp))
	}
	return f
}

// Peers returns the set of peers this frontier has observed, sorted by peer id
// for deterministic iteration.
func (f *StampFrontier) Peers() []PeerId {
	peers := make([]PeerId, 0, len(f.stamps))
	for p := range f.stamps {
		peers = append(peers, p)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	return peers
}

// Knows reports whether peer has been observed.
func (f *StampFrontier) Knows(peer PeerId) bool {
	_, ok := f.stamps[peer]
	return ok
}

// Get returns the highest stamp observed for peer, and whether it was observed.
func (f *StampFrontier) Get(peer PeerId) (HlcStamp, bool) {
	s, ok := f.stamps[peer]
	return s, ok
}

// Observe folds one observation: keep the per-peer max. Idempotent — older or
// equal stamps are ignored. Returns whether the frontier changed.
func (f *StampFrontier) Observe(peer PeerId, stamp HlcStamp) bool {
	if cur, ok := f.stamps[peer]; ok && cur.GreaterEqual(stamp) {
		return false
	}
	f.stamps[peer] = stamp
	return true
}

// Merge folds Observe over other. Commutative, associative, idempotent. Returns
// whether anything changed.
func (f *StampFrontier) Merge(other *StampFrontier) bool {
	changed := false
	for peer, stamp := range other.stamps {
		if f.Observe(peer, stamp) {
			changed = true
		}
	}
	return changed
}

// Watermark is the causal-stability watermark: the min over the given
// membership's observed stamps. The second return is false until every member
// has been observed — a single unseen member means the frontier is not yet
// causally complete (formally: collectable_implies_observed_everywhere).
func (f *StampFrontier) Watermark(membership []PeerId) (HlcStamp, bool) {
	var min HlcStamp
	seen := false
	for _, peer := range membership {
		stamp, ok := f.stamps[peer]
		if !ok {
			return HlcStamp{}, false
		}
		if !seen {
			min = stamp
			seen = true
		} else {
			min = MinStamp(min, stamp)
		}
	}
	return min, seen
}

// ToWire emits the wire form: one StampFrontierEntry per observed peer, sorted
// by peer id for deterministic output.
func (f *StampFrontier) ToWire() []StampFrontierEntry {
	peers := f.Peers()
	out := make([]StampFrontierEntry, 0, len(peers))
	for _, p := range peers {
		out = append(out, StampFrontierEntry{Peer: p, Stamp: f.stamps[p].ToWire()})
	}
	return out
}

// CrdtPlane is the CRDT plane: an Hlc + a StampFrontier + the live membership
// set.
//
// This is the runtime hub a `merge: crdt` root cell drives. Local edits (Tick)
// and remote observations (ObserveRemote) both fold into the frontier; the
// StabilityWatermark is what the tombstone-GC contract consumes.
type CrdtPlane struct {
	self       PeerId
	clock      *Hlc
	frontier   *StampFrontier
	membership map[PeerId]struct{}
}

// NewCrdtPlane creates a plane for the given self peer id.
func NewCrdtPlane(self PeerId) *CrdtPlane {
	return &CrdtPlane{
		self:       self,
		clock:      NewHlc(self),
		frontier:   NewStampFrontier(),
		membership: map[PeerId]struct{}{},
	}
}

// Self returns this peer's id.
func (p *CrdtPlane) Self() PeerId { return p.self }

// Clock returns the HLC (wall time is caller-supplied via Tick/ObserveRemote).
func (p *CrdtPlane) Clock() *Hlc { return p.clock }

// Frontier returns the stamp frontier (highest observed stamp per peer).
func (p *CrdtPlane) Frontier() *StampFrontier { return p.frontier }

// Membership returns the live membership set (peers this plane has observed,
// including self), sorted by peer id for deterministic iteration.
func (p *CrdtPlane) Membership() []PeerId {
	peers := make([]PeerId, 0, len(p.membership))
	for m := range p.membership {
		peers = append(peers, m)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	return peers
}

// Tick records a local event: tick the clock and fold the result into the
// frontier. Self is added to the membership on first use.
func (p *CrdtPlane) Tick(nowMicros int64) HlcStamp {
	p.membership[p.self] = struct{}{}
	stamp := p.clock.Tick(nowMicros)
	p.frontier.Observe(p.self, stamp)
	return stamp
}

// ObserveRemote observes a remote stamp: expand membership, fold into the
// frontier, and advance the HLC. Returns the new local stamp.
func (p *CrdtPlane) ObserveRemote(remote HlcStamp, nowMicros int64) HlcStamp {
	p.membership[remote.Peer] = struct{}{}
	p.frontier.Observe(remote.Peer, remote)
	return p.clock.Observe(remote, nowMicros)
}

// StabilityWatermark is the causal-stability watermark: min over membership of
// the frontier. The second return is false until every member has been observed.
func (p *CrdtPlane) StabilityWatermark() (HlcStamp, bool) {
	return p.frontier.Watermark(p.Membership())
}

// IsCollectable reports whether stamp is collectable: its delete stamp is <= the
// stability watermark (so every replica has provably observed it).
func (p *CrdtPlane) IsCollectable(stamp HlcStamp) bool {
	w, ok := p.StabilityWatermark()
	return ok && stamp.LessEqual(w)
}
