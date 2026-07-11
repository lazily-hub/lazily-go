package lazily

// Distributed CRDT plane runtime — state-based anti-entropy.
//
// A Go port of lazily-dart `lib/src/distributed.dart` (`CrdtPlaneRuntime` /
// `ConvergedEntry`), conformant with lazily-spec
// `conformance/distributed/anti_entropy_converge.json`. It also mirrors
// `lazily-js/src/distributed.js`.
//
// The runtime manages a set of per-node LWW cells. Each cell holds the winning
// CrdtOp (greatest WireStamp under lexicographic (wall_time, logical, peer)
// order). Op-log dedup is keyed by (node, stamp), so re-ingesting a frame
// applies zero new ops (state-based CvRDT idempotence). Convergence is
// order-independent: any delivery order of the same op set produces the
// identical winner per node.
//
// Concurrency model (per the port conventions: lean on goroutines + channels
// for the anti-entropy plane). The mutable plane state lives in an unexported
// crdtPlaneState owned exclusively by a single owner goroutine. All access is
// serialized over an inbound command channel; the owner also fans converged
// entries out over an outbound ConvergedEntry stream as ops are applied. This
// is the "share by communicating" pattern — no mutexes guard plane state.
//
// Wire / runtime types are reused as-is: CrdtOp, CrdtSync, WireStamp,
// StampFrontierEntry (ipc.go); StampFrontier and HlcStampFromWire (crdt.go);
// HlcStamp / NodeId / PeerId (hlc.go / types.go).

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// convergedStreamBuffer bounds the outbound converged-entry channel. A slow
// stream consumer applies backpressure to subsequent ingests (the owner
// goroutine blocks on the send) but never loses an entry and never leaks.
const convergedStreamBuffer = 64

// familyNodeBase is the locally-private base for minted family entry node ids
// (#lzfamilysync), set far above any application-registered id so a
// materialized family entry can never collide with an app cell. Mirrors
// lazily-rs FAMILY_NODE_BASE / lazily-zig FAMILY_NODE_BASE.
const familyNodeBase NodeId = 1 << 48

// keyNamespace returns the first `/`-separated segment of a NodeKey path — the
// family namespace (#lzfamilysync).
func keyNamespace(path string) string {
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return path
}

// ConvergedEntry is the converged state of a single node: its id, the winning
// op's optional wire-stable key (the bare NodeKey path string, nil when the
// node is addressed only by id), and the winning op's state payload.
//
// Mirrors Dart `ConvergedEntry`; Key is the `String?` the Dart plane tracks in
// `_nodeToKey`, and State is the winning op's IpcValue (Dart stores the
// already-wire `IpcValue.toWire()` result; ToWire / MarshalJSON here produce
// the identical wire shape via the IpcValue codec).
type ConvergedEntry struct {
	Node  NodeId
	Key   *string
	State IpcValue
}

// ToWire returns the { node, state[, key] } wire map, omitting key when nil
// (mirrors Dart `ConvergedEntry.toWire`).
func (e ConvergedEntry) ToWire() map[string]any {
	m := map[string]any{
		"node":  e.Node,
		"state": e.State,
	}
	if e.Key != nil {
		m["key"] = *e.Key
	}
	return m
}

// MarshalJSON emits { node[, key], state }, omitting key when nil. The State is
// serialized through the IpcValue codec (e.g. {"Inline":[66]}).
func (e ConvergedEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Node  NodeId   `json:"node"`
		Key   *string  `json:"key,omitempty"`
		State IpcValue `json:"state"`
	}{e.Node, e.Key, e.State})
}

func (e ConvergedEntry) String() string {
	key := "<nil>"
	if e.Key != nil {
		key = *e.Key
	}
	return fmt.Sprintf("ConvergedEntry(node=%d, key=%s, state=%v)", e.Node, key, e.State)
}

// opDedupKey is the (node, stamp) op-log dedup key. Two ops are duplicates iff
// they share a node id and an identical WireStamp — matching the Dart string
// key "$node|$wallTime|$logical|$peer".
type opDedupKey struct {
	node    NodeId
	wall    int64
	logical int64
	peer    PeerId
}

// crdtPlaneState is the plane's mutable state. It is NOT safe for concurrent
// use; it is owned by CrdtPlaneRuntime's single owner goroutine and every
// method runs there. This mirrors the Dart `CrdtPlaneRuntime` fields exactly.
type crdtPlaneState struct {
	peer PeerId

	// winning is the per-node winning op (greatest stamp).
	winning map[NodeId]CrdtOp
	// log is the set of applied (node, stamp) dedup keys.
	log map[opDedupKey]struct{}
	// ops is every applied op in insertion order (for delta sync).
	ops []CrdtOp
	// keyToNode / nodeToKey map a wire-stable key to a node id and back; a
	// keyless node maps to a nil key (mirrors Dart Map<int, String?>).
	keyToNode map[string]NodeId
	nodeToKey map[NodeId]*string
	// frontier is the per-peer highest observed stamp; membership is the set
	// of peers ever observed. Both are updated together in observeStamp.
	frontier   *StampFrontier
	membership map[PeerId]struct{}

	// --- Reactive family-granularity sync (#lzfamilysync) ---
	// clock mints local stamps for familySetLww (the base plane only observes
	// remote stamps in the ingest path).
	clock *Hlc
	// families is the set of registered family namespaces. An inbound keyed op
	// whose first NodeKey segment is a member materializes on ingest.
	families map[string]struct{}
	// familyMembers holds each namespace's materialized keys in
	// first-materialization order (present set only grows: deferral-not-dealloc).
	familyMembers map[string][]NodeKey
	// membershipEpoch is the reactive membership signal, bumped whenever a family
	// entry materializes — a derived aggregate over a family reads it so a
	// remote-added key forces a recompute. A plain counter (mirrors lazily-zig).
	membershipEpoch uint64
	// nextFamilyNode is the monotonic allocator for locally-private family node
	// ids.
	nextFamilyNode NodeId
}

func newCrdtPlaneState(peer PeerId) *crdtPlaneState {
	return &crdtPlaneState{
		peer:           peer,
		winning:        map[NodeId]CrdtOp{},
		log:            map[opDedupKey]struct{}{},
		keyToNode:      map[string]NodeId{},
		nodeToKey:      map[NodeId]*string{},
		frontier:       NewStampFrontier(),
		membership:     map[PeerId]struct{}{},
		clock:          NewHlc(peer),
		families:       map[string]struct{}{},
		familyMembers:  map[string][]NodeKey{},
		nextFamilyNode: familyNodeBase,
	}
}

func dedupKeyOf(op CrdtOp) opDedupKey {
	return opDedupKey{
		node:    op.Node,
		wall:    op.Stamp.WallTime,
		logical: op.Stamp.Logical,
		peer:    op.Stamp.Peer,
	}
}

// observeStamp folds a stamp into membership and the per-peer frontier (keep
// per-peer max). Mirrors Dart `_observeStamp`.
func (s *crdtPlaneState) observeStamp(stamp WireStamp) {
	s.membership[stamp.Peer] = struct{}{}
	s.frontier.Observe(stamp.Peer, HlcStampFromWire(stamp))
}

// resolveNode records the key<->node mapping (side-effect only in the ingest
// path; the return value mirrors Dart but is discarded there). Mirrors Dart
// `_resolveNode`.
func (s *crdtPlaneState) resolveNode(op CrdtOp) NodeId {
	if op.Key != nil {
		keyStr := op.Key.ToWire()
		if existing, ok := s.keyToNode[keyStr]; ok {
			return existing
		}
		s.keyToNode[keyStr] = op.Node
		ks := keyStr
		s.nodeToKey[op.Node] = &ks
	} else if _, ok := s.nodeToKey[op.Node]; !ok {
		s.nodeToKey[op.Node] = nil
	}
	return op.Node
}

// ingestOps applies a list of ops, returning the number of newly applied ops
// (0 = idempotent re-delivery) and the ascending set of nodes whose winning op
// changed. Mirrors Dart `ingestOps`.
func (s *crdtPlaneState) ingestOps(ops []CrdtOp) (int, []NodeId) {
	applied := 0
	changed := map[NodeId]struct{}{}
	for _, op := range ops {
		dk := dedupKeyOf(op)
		if _, ok := s.log[dk]; ok {
			continue
		}
		s.log[dk] = struct{}{}
		s.ops = append(s.ops, op)
		applied++

		s.observeStamp(op.Stamp)

		// Key-aware resolution (#lzfamilysync): a keyed op whose namespace is a
		// registered family resolves to a LOCAL family node — an already-known
		// key updates its entry under LWW; an unknown key MATERIALIZES a fresh
		// entry seeded from the op state (materialize-on-ingest) rather than
		// letting the base plane treat the remote's volatile node id as
		// authoritative.
		if op.Key != nil {
			if _, isFamily := s.families[keyNamespace(op.Key.Path())]; isFamily {
				if node, ch := s.ingestFamilyOp(op); ch {
					changed[node] = struct{}{}
				}
				continue
			}
		}

		s.resolveNode(op)

		existing, ok := s.winning[op.Node]
		if !ok || HlcStampFromWire(op.Stamp).Greater(HlcStampFromWire(existing.Stamp)) {
			s.winning[op.Node] = op
			changed[op.Node] = struct{}{}
		}
	}
	nodes := make([]NodeId, 0, len(changed))
	for n := range changed {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return applied, nodes
}

// ingest folds a CrdtSync frame: observe advertised frontier stamps, then apply
// its ops. Mirrors Dart `ingest`.
func (s *crdtPlaneState) ingest(sync CrdtSync) (int, []NodeId) {
	for _, entry := range sync.Frontier {
		s.observeStamp(entry.Stamp)
	}
	return s.ingestOps(sync.Ops)
}

// nodes returns all winning node ids ascending. Mirrors Dart `nodes`.
func (s *crdtPlaneState) nodes() []NodeId {
	out := make([]NodeId, 0, len(s.winning))
	for n := range s.winning {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (s *crdtPlaneState) entryFor(node NodeId) ConvergedEntry {
	return ConvergedEntry{Node: node, Key: s.nodeToKey[node], State: s.winning[node].State}
}

// converged returns one entry per node (ascending). Mirrors Dart `converged`.
func (s *crdtPlaneState) converged() []ConvergedEntry {
	sorted := s.nodes()
	out := make([]ConvergedEntry, 0, len(sorted))
	for _, node := range sorted {
		out = append(out, s.entryFor(node))
	}
	return out
}

// convergedFor returns entries for the given (already ascending) nodes that
// still have a winning op — the outbound-stream projection of a single ingest.
func (s *crdtPlaneState) convergedFor(nodes []NodeId) []ConvergedEntry {
	out := make([]ConvergedEntry, 0, len(nodes))
	for _, node := range nodes {
		if _, ok := s.winning[node]; ok {
			out = append(out, s.entryFor(node))
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Reactive family-granularity sync (#lzfamilysync)
//
// A keyed op for a family entry that is NOT locally registered materializes the
// entry on ingest (seeded from the op's converged register) instead of being
// dropped — so a keyed family syncs as a unit: membership propagates, values are
// adopted, a later last-writer-wins update converges, re-ingest is idempotent,
// and a derived aggregate over the family (e.g. a count of `true` entries)
// converges across replicas. Each entry is a last-writer-wins register keyed by
// namespace/suffix. Proved in lazily-formal FamilySync.lean (applyOp_eq_merge,
// applyOp_present, applyOp_absent_adopts, present_merge, applyOp_idem,
// aggregate_converges); mirrors lazily-rs crdt_plane.rs + lazily-zig.
// ---------------------------------------------------------------------------

// registerFamilyLww registers a last-writer-wins family under namespace, so a
// keyed op an entry of this family produces on a peer materializes an entry here
// on ingest instead of being dropped. Replicas that share a session must
// register the same family namespace.
func (s *crdtPlaneState) registerFamilyLww(namespace string) {
	s.families[namespace] = struct{}{}
	if _, ok := s.familyMembers[namespace]; !ok {
		s.familyMembers[namespace] = nil
	}
}

// bumpMembershipEpoch bumps the reactive membership signal so a derived
// aggregate over a family recomputes when its present set grows.
func (s *crdtPlaneState) bumpMembershipEpoch() { s.membershipEpoch++ }

// mintFamilyNode allocates a locally-private node id for a family entry,
// skipping any id already in use so a family node can never collide with an
// application-registered cell.
func (s *crdtPlaneState) mintFamilyNode() NodeId {
	for {
		candidate := s.nextFamilyNode
		s.nextFamilyNode++
		if _, inUse := s.winning[candidate]; !inUse {
			return candidate
		}
	}
}

// recordFamilyMember records a newly-materialized key in its family's present
// set (dedup so a re-observed key does not duplicate). Only called for a
// genuinely new key.
func (s *crdtPlaneState) recordFamilyMember(namespace string, key NodeKey) {
	members := s.familyMembers[namespace]
	for _, k := range members {
		if k.Path() == key.Path() {
			return
		}
	}
	s.familyMembers[namespace] = append(members, key)
}

// materializeFamilyEntry mints a local node for a fresh family entry seeded from
// state/stamp: record membership, index the key, install the winning op, and
// bump the membership epoch. Returns the local node id.
func (s *crdtPlaneState) materializeFamilyEntry(namespace string, key NodeKey, state IpcValue, stamp WireStamp) NodeId {
	s.recordFamilyMember(namespace, key)
	node := s.mintFamilyNode()
	keyStr := key.ToWire()
	s.keyToNode[keyStr] = node
	ks := keyStr
	s.nodeToKey[node] = &ks
	k := key
	s.winning[node] = CrdtOp{Node: node, Key: &k, Stamp: stamp, State: state}
	s.bumpMembershipEpoch()
	return node
}

// ingestFamilyOp applies a keyed family op: an already-known key updates its
// local entry under LWW (greatest stamp wins); an unknown key materializes a
// fresh local entry seeded from the op state. Returns the local node id and
// whether its winning value changed. Seeding from the op state IS the pointwise
// CRDT merge, so this inherits full semilattice convergence.
func (s *crdtPlaneState) ingestFamilyOp(op CrdtOp) (NodeId, bool) {
	keyStr := op.Key.ToWire()
	if node, known := s.keyToNode[keyStr]; known {
		existing := s.winning[node]
		if HlcStampFromWire(op.Stamp).Greater(HlcStampFromWire(existing.Stamp)) {
			// LWW update: preserve the local node + key; op.Key is a transient
			// wire slice with the same path.
			s.winning[node] = CrdtOp{Node: node, Key: existing.Key, Stamp: op.Stamp, State: op.State}
			return node, true
		}
		return node, false
	}
	return s.materializeFamilyEntry(keyNamespace(op.Key.Path()), *op.Key, op.State, op.Stamp), true
}

// familySetLww inserts or updates a local LWW family entry namespace/<keySuffix>
// to state at nowMicros, returning the CrdtOp to broadcast (recorded in the op
// log so a sync frame carries it) and true, or a zero op and false if the key is
// invalid or the write was stamp-dominated. Materializes the entry (and bumps
// membership) on first insert.
func (s *crdtPlaneState) familySetLww(namespace, keySuffix string, state IpcValue, nowMicros int64) (CrdtOp, bool) {
	key, err := NewNodeKey(namespace + "/" + keySuffix)
	if err != nil {
		return CrdtOp{}, false
	}
	hlc := s.clock.Tick(nowMicros)
	s.observeStamp(hlc.ToWire())
	stamp := hlc.ToWire()
	keyStr := key.ToWire()
	if node, known := s.keyToNode[keyStr]; known {
		existing := s.winning[node]
		if !hlc.Greater(HlcStampFromWire(existing.Stamp)) {
			return CrdtOp{}, false
		}
		op := CrdtOp{Node: node, Key: existing.Key, Stamp: stamp, State: state}
		s.winning[node] = op
		s.recordLocalOp(op)
		return op, true
	}
	node := s.materializeFamilyEntry(namespace, key, state, stamp)
	op := s.winning[node]
	s.recordLocalOp(op)
	return op, true
}

// recordLocalOp records a locally-minted op in the op log + insertion-ordered
// op list (so Ops() / a sync frame carries it), deduped by (node, stamp).
func (s *crdtPlaneState) recordLocalOp(op CrdtOp) {
	dk := dedupKeyOf(op)
	if _, ok := s.log[dk]; ok {
		return
	}
	s.log[dk] = struct{}{}
	s.ops = append(s.ops, op)
}

// familyKeys returns a copy of the materialized keys of family namespace, in
// first-materialization order.
func (s *crdtPlaneState) familyKeys(namespace string) []NodeKey {
	members := s.familyMembers[namespace]
	out := make([]NodeKey, len(members))
	copy(out, members)
	return out
}

// familyValueLww returns the current converged state of family entry
// namespace/<keySuffix>, or (nil, false) if the key is not present.
func (s *crdtPlaneState) familyValueLww(namespace, keySuffix string) (IpcValue, bool) {
	key, err := NewNodeKey(namespace + "/" + keySuffix)
	if err != nil {
		return nil, false
	}
	node, ok := s.keyToNode[key.ToWire()]
	if !ok {
		return nil, false
	}
	op, ok := s.winning[node]
	if !ok {
		return nil, false
	}
	return op.State, true
}

// ---------------------------------------------------------------------------
// CrdtPlaneRuntime — the channel-driven owner goroutine
// ---------------------------------------------------------------------------

// command is one unit of serialized work for the owner goroutine. run mutates
// or reads the owned state and returns any converged entries to fan out on the
// stream; done is closed once run has completed (so a caller's reply is
// available before the — potentially blocking — stream fan-out).
type command struct {
	run  func() []ConvergedEntry
	done chan struct{}
}

// CrdtPlaneRuntime is a state-based CRDT plane runtime with anti-entropy. A
// single owner goroutine owns the plane state; public methods serialize their
// work over an inbound command channel (blocking until the owner replies), and
// converged entries are fanned out over an optional outbound stream
// (ConvergedStream). Call Close (or cancel the context passed to
// NewCrdtPlaneRuntimeWithContext) to stop the owner goroutine; no goroutine is
// leaked.
type CrdtPlaneRuntime struct {
	peer  PeerId
	state *crdtPlaneState

	cmds chan command
	quit chan struct{}

	closeOnce sync.Once

	// stream is the outbound converged-entry channel. It is written and read
	// only by the owner goroutine, so it needs no lock. nil until a caller
	// subscribes via ConvergedStream.
	stream chan ConvergedEntry
}

// NewCrdtPlaneRuntime creates a runtime for the given local peer id and starts
// its owner goroutine. Call Close to release it.
func NewCrdtPlaneRuntime(peer PeerId) *CrdtPlaneRuntime {
	r := &CrdtPlaneRuntime{
		peer:  peer,
		state: newCrdtPlaneState(peer),
		cmds:  make(chan command),
		quit:  make(chan struct{}),
	}
	go r.loop()
	return r
}

// NewCrdtPlaneRuntimeWithContext creates a runtime that also stops when ctx is
// cancelled (in addition to Close). Either signal cleanly tears the owner
// goroutine down.
func NewCrdtPlaneRuntimeWithContext(ctx context.Context, peer PeerId) *CrdtPlaneRuntime {
	r := NewCrdtPlaneRuntime(peer)
	go func() {
		select {
		case <-ctx.Done():
			r.Close()
		case <-r.quit:
		}
	}()
	return r
}

// loop is the owner goroutine: it serializes commands and fans converged
// entries out on the stream. It exits — closing the stream, if any — when quit
// is closed.
func (r *CrdtPlaneRuntime) loop() {
	for {
		select {
		case c := <-r.cmds:
			events := c.run()
			close(c.done)
			for _, e := range events {
				if r.stream == nil {
					break
				}
				select {
				case r.stream <- e:
				case <-r.quit:
					r.closeStream()
					return
				}
			}
		case <-r.quit:
			r.closeStream()
			return
		}
	}
}

func (r *CrdtPlaneRuntime) closeStream() {
	if r.stream != nil {
		close(r.stream)
		r.stream = nil
	}
}

// submit sends run to the owner goroutine and blocks until it has completed
// (its reply is then visible). It is a no-op after Close.
func (r *CrdtPlaneRuntime) submit(run func() []ConvergedEntry) {
	done := make(chan struct{})
	select {
	case r.cmds <- command{run: run, done: done}:
	case <-r.quit:
		return
	}
	select {
	case <-done:
	case <-r.quit:
	}
}

// Close stops the owner goroutine and closes the converged stream (if any). It
// is idempotent and safe to call from any goroutine.
func (r *CrdtPlaneRuntime) Close() {
	r.closeOnce.Do(func() { close(r.quit) })
}

// ConvergedStream returns the outbound channel on which converged entries are
// emitted as ops are applied (one entry per node whose winning op changed in
// each ingest, ascending). Only one subscriber is supported — the most recent
// call wins. The channel is closed when the runtime closes. If the runtime is
// already closed, a closed channel is returned.
func (r *CrdtPlaneRuntime) ConvergedStream() <-chan ConvergedEntry {
	ch := make(chan ConvergedEntry, convergedStreamBuffer)
	subscribed := false
	r.submit(func() []ConvergedEntry {
		r.stream = ch
		subscribed = true
		return nil
	})
	if !subscribed {
		close(ch)
	}
	return ch
}

// Peer returns the local peer id.
func (r *CrdtPlaneRuntime) Peer() PeerId { return r.peer }

// IngestOps applies a batch of ops and returns the number of newly applied ops
// (0 = idempotent re-delivery). Converged entries for changed nodes are fanned
// out on ConvergedStream. Mirrors Dart `ingestOps` (whose unused `nowMicros`
// parameter is omitted).
func (r *CrdtPlaneRuntime) IngestOps(ops []CrdtOp) int {
	var applied int
	r.submit(func() []ConvergedEntry {
		a, changed := r.state.ingestOps(ops)
		applied = a
		return r.state.convergedFor(changed)
	})
	return applied
}

// Ingest folds a CrdtSync frame (observe frontier, then apply ops) and returns
// the number of newly applied ops. Mirrors Dart `ingest`.
func (r *CrdtPlaneRuntime) Ingest(sync CrdtSync) int {
	var applied int
	r.submit(func() []ConvergedEntry {
		a, changed := r.state.ingest(sync)
		applied = a
		return r.state.convergedFor(changed)
	})
	return applied
}

// Converged returns the full converged state: one entry per node (ascending).
// Mirrors Dart `converged`.
func (r *CrdtPlaneRuntime) Converged() []ConvergedEntry {
	var out []ConvergedEntry
	r.submit(func() []ConvergedEntry {
		out = r.state.converged()
		return nil
	})
	return out
}

// WinningOp returns the winning op for node, and whether the node is present.
// Mirrors Dart `winningOp`.
func (r *CrdtPlaneRuntime) WinningOp(node NodeId) (CrdtOp, bool) {
	var op CrdtOp
	var ok bool
	r.submit(func() []ConvergedEntry {
		op, ok = r.state.winning[node]
		return nil
	})
	return op, ok
}

// Value returns the winning state payload for node, and whether the node is
// present. Mirrors Dart `value` (which returns the wire form; here the IpcValue
// itself, whose codec produces that wire form).
func (r *CrdtPlaneRuntime) Value(node NodeId) (IpcValue, bool) {
	var v IpcValue
	var ok bool
	r.submit(func() []ConvergedEntry {
		var op CrdtOp
		op, ok = r.state.winning[node]
		if ok {
			v = op.State
		}
		return nil
	})
	return v, ok
}

// Nodes returns all winning node ids ascending. Mirrors Dart `nodes`.
func (r *CrdtPlaneRuntime) Nodes() []NodeId {
	var out []NodeId
	r.submit(func() []ConvergedEntry {
		out = r.state.nodes()
		return nil
	})
	return out
}

// Size returns the number of converged nodes. Mirrors Dart `size`.
func (r *CrdtPlaneRuntime) Size() int {
	var n int
	r.submit(func() []ConvergedEntry {
		n = len(r.state.winning)
		return nil
	})
	return n
}

// IsEmpty reports whether no node has converged yet. Mirrors Dart `isEmpty`.
func (r *CrdtPlaneRuntime) IsEmpty() bool {
	empty := true
	r.submit(func() []ConvergedEntry {
		empty = len(r.state.winning) == 0
		return nil
	})
	return empty
}

// Membership returns the known peer ids (ascending). Mirrors Dart `membership`.
func (r *CrdtPlaneRuntime) Membership() []PeerId {
	var out []PeerId
	r.submit(func() []ConvergedEntry {
		out = make([]PeerId, 0, len(r.state.membership))
		for p := range r.state.membership {
			out = append(out, p)
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return nil
	})
	return out
}

// FrontierEntries returns the per-peer frontier as wire entries (ascending by
// peer). Mirrors Dart `frontierEntries` (which returns MapEntry pairs; the Go
// wire type StampFrontierEntry carries the same (peer, stamp) pair).
func (r *CrdtPlaneRuntime) FrontierEntries() []StampFrontierEntry {
	var out []StampFrontierEntry
	r.submit(func() []ConvergedEntry {
		out = r.state.frontier.ToWire()
		return nil
	})
	return out
}

// Ops returns a copy of the full op log in insertion order. Mirrors Dart `ops`.
func (r *CrdtPlaneRuntime) Ops() []CrdtOp {
	var out []CrdtOp
	r.submit(func() []ConvergedEntry {
		out = make([]CrdtOp, len(r.state.ops))
		copy(out, r.state.ops)
		return nil
	})
	return out
}

// ---------------------------------------------------------------------------
// Reactive family-granularity sync (#lzfamilysync) — runtime surface
// ---------------------------------------------------------------------------

// RegisterFamilyLww registers a last-writer-wins family under namespace so an
// inbound keyed op for an unregistered entry of this family materializes on
// ingest instead of being dropped. Replicas sharing a session must register the
// same namespace.
func (r *CrdtPlaneRuntime) RegisterFamilyLww(namespace string) {
	r.submit(func() []ConvergedEntry {
		r.state.registerFamilyLww(namespace)
		return nil
	})
}

// FamilySetLww inserts or updates the local LWW family entry
// namespace/<keySuffix> to state at nowMicros, materializing it (and bumping the
// membership epoch) on first insert. Returns the broadcast op and true, or a
// zero op and false if the key is invalid or the write was stamp-dominated. The
// converged entry is fanned out on ConvergedStream.
func (r *CrdtPlaneRuntime) FamilySetLww(namespace, keySuffix string, state IpcValue, nowMicros int64) (CrdtOp, bool) {
	var op CrdtOp
	var ok bool
	r.submit(func() []ConvergedEntry {
		op, ok = r.state.familySetLww(namespace, keySuffix, state, nowMicros)
		if !ok {
			return nil
		}
		return r.state.convergedFor([]NodeId{op.Node})
	})
	return op, ok
}

// FamilyKeys returns the materialized keys of family namespace, in
// first-materialization order.
func (r *CrdtPlaneRuntime) FamilyKeys(namespace string) []NodeKey {
	var out []NodeKey
	r.submit(func() []ConvergedEntry {
		out = r.state.familyKeys(namespace)
		return nil
	})
	return out
}

// FamilyValueLww returns the current converged state of family entry
// namespace/<keySuffix>, and whether the key is present.
func (r *CrdtPlaneRuntime) FamilyValueLww(namespace, keySuffix string) (IpcValue, bool) {
	var v IpcValue
	var ok bool
	r.submit(func() []ConvergedEntry {
		v, ok = r.state.familyValueLww(namespace, keySuffix)
		return nil
	})
	return v, ok
}

// MembershipEpoch returns the reactive membership signal (#lzfamilysync): a
// derived aggregate over a family depends on it so a remote-materialized key
// forces a recompute. Bumped whenever a family entry materializes.
func (r *CrdtPlaneRuntime) MembershipEpoch() uint64 {
	var e uint64
	r.submit(func() []ConvergedEntry {
		e = r.state.membershipEpoch
		return nil
	})
	return e
}
