package lazily

// Hybrid logical clock (Karger-Shrinkman-Levine) — the shared clock primitive
// used by both the distributed CRDT plane (crdt.go) and the move-aware sequence
// CRDT (seq_crdt.go). Defined once here so both layers share the exact same
// total order (wall_time, logical, peer).
//
// Mirrors lazily-rs `Hlc`/`HlcStamp` and the formally-proven invariants in
// lazily-formal (hlc_send_is_monotonic_and_recv_observes_remote).

// HlcStamp is a runtime HLC stamp — a total order (WallTime, Logical, Peer).
// Order is lexicographic, so equal (wall, logical) from different peers is still
// totally ordered by Peer. HlcStamp is a comparable value type.
type HlcStamp struct {
	WallTime int64 // microseconds
	Logical  int64
	Peer     PeerId
}

// NewHlcStamp constructs a stamp.
func NewHlcStamp(wallTime, logical int64, peer PeerId) HlcStamp {
	return HlcStamp{WallTime: wallTime, Logical: logical, Peer: peer}
}

// Compare returns -1, 0, or +1 for the lexicographic (wall, logical, peer)
// order.
func (s HlcStamp) Compare(other HlcStamp) int {
	switch {
	case s.WallTime != other.WallTime:
		if s.WallTime < other.WallTime {
			return -1
		}
		return 1
	case s.Logical != other.Logical:
		if s.Logical < other.Logical {
			return -1
		}
		return 1
	case s.Peer != other.Peer:
		if s.Peer < other.Peer {
			return -1
		}
		return 1
	default:
		return 0
	}
}

func (s HlcStamp) Less(other HlcStamp) bool      { return s.Compare(other) < 0 }
func (s HlcStamp) LessEqual(other HlcStamp) bool { return s.Compare(other) <= 0 }
func (s HlcStamp) Greater(other HlcStamp) bool   { return s.Compare(other) > 0 }
func (s HlcStamp) GreaterEqual(o HlcStamp) bool  { return s.Compare(o) >= 0 }

// MinStamp returns the lexicographically smaller stamp (stability watermark).
func MinStamp(a, b HlcStamp) HlcStamp {
	if a.LessEqual(b) {
		return a
	}
	return b
}

// MaxStamp returns the lexicographically larger stamp (LWW winner).
func MaxStamp(a, b HlcStamp) HlcStamp {
	if a.GreaterEqual(b) {
		return a
	}
	return b
}

// Hlc is a hybrid logical clock. Wall time is caller-supplied (Tick/Observe take
// nowMicros) so the clock is deterministic and never reads the system clock.
type Hlc struct {
	peer        PeerId
	lastWall    int64
	lastLogical int64
}

// NewHlc creates a clock for the given peer id.
func NewHlc(peer PeerId) *Hlc { return &Hlc{peer: peer} }

// Peer returns this clock's peer id (the final tiebreak component).
func (h *Hlc) Peer() PeerId { return h.peer }

// Tick records a local event, advancing the clock, and returns the new stamp.
// Strictly increasing on this peer.
func (h *Hlc) Tick(nowMicros int64) HlcStamp {
	if nowMicros > h.lastWall {
		h.lastWall = nowMicros
		h.lastLogical = 0
	} else {
		h.lastLogical++
	}
	return HlcStamp{WallTime: h.lastWall, Logical: h.lastLogical, Peer: h.peer}
}

// Observe folds a remote stamp: the returned local stamp is strictly greater
// than remote (the standard HLC recv rule).
func (h *Hlc) Observe(remote HlcStamp, nowMicros int64) HlcStamp {
	wall := max3(h.lastWall, remote.WallTime, nowMicros)
	switch {
	case wall == h.lastWall && wall == remote.WallTime:
		h.lastLogical = max2(h.lastLogical, remote.Logical) + 1
	case wall == h.lastWall:
		h.lastLogical++
	case wall == remote.WallTime:
		h.lastLogical = remote.Logical + 1
	default:
		h.lastLogical = 0
	}
	h.lastWall = wall
	return HlcStamp{WallTime: h.lastWall, Logical: h.lastLogical, Peer: h.peer}
}

func max2(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}

func max3(a, b, c int64) int64 { return max2(max2(a, b), c) }
