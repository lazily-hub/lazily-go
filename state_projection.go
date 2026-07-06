package lazily

// State projection / mirror — pull-model helpers.
//
// A Go port of lazily-dart's `lib/src/state_projection.dart`. The value-mirror
// contract from `lazily-spec § Lazy reconciliation`: at flush, the sender
// resolves each invalidated allowlisted slot, so the delta carries concrete
// SlotValues; the receiver holds no compute closures.
//
// This module ships the pure helpers: a StateProjectionMirror that tracks dirty
// slots and produces the minimal flush delta, plus document-hash/event builders
// for the agent-doc state backbone. Epoch sequencing is delegated to DeltaNext
// (ipc.go), which advances the epoch exactly once per flush; the receiver's
// fail-closed resync-on-gap decision lives in Delta.ApplyStatus (ipc.go).

import (
	"sort"
	"unicode/utf16"
)

// StateProjectionMirror tracks which slots are dirty and produces a coalesced
// flush Delta.
//
// The caller marks slots dirty as the reactive graph invalidates them. At
// flush, the mirror collects the resolved values and builds a single Delta
// (DeltaNext) with one DeltaOpSlotValue per resolved slot; slots still dirty at
// flush are emitted as DeltaOpInvalidate (the mirror-lazy path).
//
// Like the Dart original, StateProjectionMirror is not safe for concurrent use.
type StateProjectionMirror struct {
	dirty     map[NodeId]struct{}
	values    map[NodeId]IpcValue
	baseEpoch Epoch
}

// NewStateProjectionMirror constructs an empty mirror at base epoch 0.
func NewStateProjectionMirror() *StateProjectionMirror {
	return &StateProjectionMirror{
		dirty:  map[NodeId]struct{}{},
		values: map[NodeId]IpcValue{},
	}
}

// MarkDirty marks slot node as dirty.
func (m *StateProjectionMirror) MarkDirty(node NodeId) {
	m.dirty[node] = struct{}{}
}

// Resolve records a dirty slot's value (called by the graph at flush time) and
// clears its dirty mark.
func (m *StateProjectionMirror) Resolve(node NodeId, value IpcValue) {
	m.values[node] = value
	delete(m.dirty, node)
}

// IsDirty reports whether node is currently dirty.
func (m *StateProjectionMirror) IsDirty(node NodeId) bool {
	_, ok := m.dirty[node]
	return ok
}

// DirtyNodes returns all dirty node ids, sorted ascending.
func (m *StateProjectionMirror) DirtyNodes() []NodeId {
	out := make([]NodeId, 0, len(m.dirty))
	for node := range m.dirty {
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Flush produces a Delta with one DeltaOpInvalidate per still-dirty slot
// (ascending) followed by one DeltaOpSlotValue per resolved slot (ascending),
// then clears the pending state and advances the base epoch once.
func (m *StateProjectionMirror) Flush() Delta {
	ops := make([]DeltaOp, 0, len(m.dirty)+len(m.values))

	dirty := make([]NodeId, 0, len(m.dirty))
	for node := range m.dirty {
		dirty = append(dirty, node)
	}
	sort.Slice(dirty, func(i, j int) bool { return dirty[i] < dirty[j] })
	for _, node := range dirty {
		ops = append(ops, DeltaOpInvalidate{Node: node})
	}

	resolved := make([]NodeId, 0, len(m.values))
	for node := range m.values {
		resolved = append(resolved, node)
	}
	sort.Slice(resolved, func(i, j int) bool { return resolved[i] < resolved[j] })
	for _, node := range resolved {
		ops = append(ops, DeltaOpSlotValue{Node: node, Payload: m.values[node]})
	}

	m.dirty = map[NodeId]struct{}{}
	m.values = map[NodeId]IpcValue{}

	delta := DeltaNext(m.baseEpoch, ops)
	m.baseEpoch = delta.Epoch
	return delta
}

// BaseEpoch returns the current base epoch.
func (m *StateProjectionMirror) BaseEpoch() Epoch { return m.baseEpoch }

// DocumentHash computes the FNV-1a 64-bit document hash for a file path or
// string.
//
// Cross-language stable (NOT Dart's hashCode). The Dart original hashes over
// String.codeUnits (UTF-16 code units), so this port hashes the UTF-16 encoding
// of path — not its UTF-8 bytes — to reproduce the same digest. Used as the
// canonical document key for the state backbone.
func DocumentHash(path string) uint64 {
	const offset = uint64(0xcbf29ce484222325)
	const prime = uint64(0x100000001b3)

	hash := offset
	for _, unit := range utf16.Encode([]rune(path)) {
		hash ^= uint64(unit)
		hash *= prime
	}
	return hash
}

// BuildStateEvent builds a state-backbone event for the agent-doc ledger. The
// eventType and document_hash seed the fact; entries in fields are merged over
// them (so fields may override), mirroring the Dart map-spread semantics.
func BuildStateEvent(docHash, eventType string, fields map[string]any, eventSuffix string) map[string]any {
	fact := map[string]any{
		"type":          eventType,
		"document_hash": docHash,
	}
	for k, v := range fields {
		fact[k] = v
	}
	return map[string]any{
		"event_id": docHash + ":" + eventSuffix,
		"fact":     fact,
	}
}
