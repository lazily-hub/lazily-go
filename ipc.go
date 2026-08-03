package lazily

// IPC wire protocol for lazily-go.
//
// A Go port of lazily-dart's `lib/src/ipc.dart`, conformant with lazily-spec
// (protocol.md § Shared Types / IPC / Distributed and schemas/{snapshot,delta,
// distributed,defs}.json). This module carries the language-agnostic lazily
// wire protocol: Snapshot / Delta / CrdtSync / DeltaOp / CrdtOp / NodeState /
// IpcValue / IpcMessage / WireStamp, the permission boundary (RemoteOp,
// PeerPermissions, PermissionDenied), and the optional wire-stable NodeKey.
//
// Wire conventions (NORMATIVE, from schemas/*.json):
//   - Externally-tagged unions: a single-key object whose key is the PascalCase
//     variant name (NodeState.Opaque is the bare string "Opaque").
//   - Serialized value bytes are JSON arrays of u8 (NOT base64), so byte
//     payloads are marshaled through bytesWire/parseByteArray rather than Go's
//     default []byte base64 encoding.
//   - snake_case field names (version_vector, base_epoch, type_tag, wall_time,
//     trigger_effect, ...). Field order follows the schema/Dart declaration
//     order via marshaling structs so output is deterministic.
//   - NodeSnapshot / DeltaOpNodeAdd omit `key` when nil; CrdtOp always emits
//     `key` (null when unset), mirroring the lazily-rs derived serde.
//
// The HLC runtime clock (Hlc/HlcStamp) lives in hlc.go; this file owns only the
// codec-stable WireStamp mirror and StampFrontierEntry. HlcStamp<->WireStamp
// conversion is owned by crdt.go.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// ---------------------------------------------------------------------------
// NodeKey (protocol.md § NodeKey, defs.json#/$defs/NodeKey)
// ---------------------------------------------------------------------------

// NodeKey is a validated `/`-joined path addressing a keyed collection entry.
//
// It is an additive, optional wire-stable address: unlike the volatile NodeId
// (which a producer may re-mint after a resync or a remove-then-readd), a key
// is producer-defined and stable across NodeId churn. Serialized as a bare JSON
// string; the containing field is omitted when the key is absent (see
// NodeSnapshot / DeltaOpNodeAdd).
//
// Bounds, enforced on construction:
//   - path <= 1024 bytes (UTF-8);
//   - <= 32 `/`-separated segments;
//   - no empty path and no empty segments (leading/trailing/double `/`).
type NodeKey struct {
	path string
}

// NewNodeKey validates and constructs a NodeKey.
func NewNodeKey(path string) (NodeKey, error) {
	if err := validateNodeKey(path); err != nil {
		return NodeKey{}, err
	}
	return NodeKey{path: path}, nil
}

func validateNodeKey(path string) error {
	if len(path) == 0 {
		return fmt.Errorf("NodeKey path must be non-empty")
	}
	if len(path) > 1024 {
		return fmt.Errorf("NodeKey path exceeds 1024 bytes (was %d)", len(path))
	}
	segments := strings.Split(path, "/")
	if len(segments) > 32 {
		return fmt.Errorf("NodeKey path exceeds 32 segments (was %d)", len(segments))
	}
	for _, segment := range segments {
		if segment == "" {
			return fmt.Errorf(`NodeKey path has an empty segment (leading/trailing/double "/")`)
		}
	}
	return nil
}

// Path returns the canonical path string.
func (k NodeKey) Path() string { return k.path }

// Segments returns the `/`-separated path segments.
func (k NodeKey) Segments() []string { return strings.Split(k.path, "/") }

// ToWire returns the bare path string (the JSON shape of a NodeKey).
func (k NodeKey) ToWire() string { return k.path }

// NodeKeyFromWire parses and re-validates a wire path string.
func NodeKeyFromWire(value string) (NodeKey, error) { return NewNodeKey(value) }

func (k NodeKey) String() string { return fmt.Sprintf("NodeKey(%s)", k.path) }

// MarshalJSON emits the bare path string.
func (k NodeKey) MarshalJSON() ([]byte, error) { return json.Marshal(k.path) }

// UnmarshalJSON parses a JSON string and re-validates the NodeKey bounds.
func (k *NodeKey) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("NodeKey must be a string: %w", err)
	}
	nk, err := NewNodeKey(s)
	if err != nil {
		return err
	}
	*k = nk
	return nil
}

// ---------------------------------------------------------------------------
// BlobBackendKind (defs.json#/$defs/ShmBlobRef.backend) — zero-copy transport
// ---------------------------------------------------------------------------

// BlobBackendKind selects which pluggable blob backend resolves a descriptor
// (cross-process zero-copy transport, #lzzcpy). A receiver routes resolution by
// this discriminator: a `shm` descriptor never resolves in an Arrow backend and
// vice versa (the resolve_wrong_backend theorem). It is the wire mirror of the
// Rust `BlobBackendKind` enum; the `arena` itself is backend-agnostic and does
// not store it — the discriminator is wire-level routing only.
//
// The zero value ("") is the ABSENT discriminator and resolves to Shm, so a
// descriptor minted before the field existed resolves unchanged. That absence is
// the whole of the forward compatibility here (#lzblobbackendstrict): a value
// that is PRESENT but outside the enum is rejected by name, never collapsed to
// Shm — see ParseBlobBackendKind.
type BlobBackendKind string

const (
	// BackendShm is the POSIX shared-memory backend (shm_open + mmap) — the
	// default cross-process backend (same host). Omitted on the wire.
	BackendShm BlobBackendKind = "shm"
	// BackendArrow holds Apache Arrow IPC stream bytes — the descriptor's bytes
	// are an Arrow IPC stream the receiver imports zero-copy.
	BackendArrow BlobBackendKind = "arrow"
	// BackendInProcess is an in-process arena (single address space — the FFI
	// host / an editor plugin loaded in the same process).
	BackendInProcess BlobBackendKind = "in_process"
)

// Normalized resolves the ABSENT discriminator — the zero value, which is what
// an omitted `backend` field decodes to — into the default backend, Shm.
//
// That is the only collapse it performs. A present-but-unrecognised value is
// returned UNCHANGED so that it stays visibly unknown to routerIndex, to the
// encoder, and to String. It used to be folded into Shm here, and this comment
// used to argue that was safe because the router merely declines. It is not: Shm
// is a backend this build genuinely resolves, so folding an unknown kind into it
// routes a non-shm descriptor into the shm table and leaves the
// resolve_wrong_backend guarantee to be discharged probabilistically by a 64-bit
// checksum instead of structurally by the routing rule
// (docs/zero-copy-transport.md, #lzblobbackendstrict).
func (k BlobBackendKind) Normalized() BlobBackendKind {
	if k == "" {
		return BackendShm
	}
	return k
}

// IsKnown reports whether k names a backend this build can route: one of the
// three enum values, or the absent (zero) value, which means Shm.
func (k BlobBackendKind) IsKnown() bool {
	switch k.Normalized() {
	case BackendShm, BackendArrow, BackendInProcess:
		return true
	default:
		return false
	}
}

// ParseBlobBackendKind decodes a `backend` discriminator that is PRESENT on the
// wire, rejecting any value outside the enum and naming the offending token.
//
// Nothing here is a forward-compatibility hazard. A new backend enters the
// protocol by adding an enum value — a spec change carrying its own fixture — so
// a token this build does not know is a corrupt or non-conforming producer, not
// a newer peer. Descriptors that predate the field carry no `backend` at all and
// never reach this function.
//
// The empty string is rejected too: absence is spelled by OMITTING the field,
// and `"backend": ""` names no member of the enum.
func ParseBlobBackendKind(s string) (BlobBackendKind, error) {
	switch BlobBackendKind(s) {
	case BackendShm, BackendArrow, BackendInProcess:
		return BlobBackendKind(s), nil
	default:
		return "", fmt.Errorf(
			"unknown blob backend %q (expected \"shm\", \"arrow\" or \"in_process\")", s)
	}
}

// IsDefault reports whether this is the default backend (Shm), including the
// absent zero value. Used to OMIT the `backend` field on the wire when it is
// `shm`, so a descriptor minted before the field existed round-trips
// byte-identically. An unrecognised kind is not default, so it is emitted rather
// than laundered into a legal-looking `shm` frame.
func (k BlobBackendKind) IsDefault() bool { return k.Normalized() == BackendShm }

// routerIndex is the BlobRouter slot for this backend kind (Shm=0, Arrow=1,
// InProcess=2), matching the Rust `BlobBackendKind as usize` router indexing.
// `ok` is false for a kind this build cannot route; the caller resolves nothing
// rather than falling into slot 0.
func (k BlobBackendKind) routerIndex() (int, bool) {
	switch k.Normalized() {
	case BackendShm:
		return 0, true
	case BackendArrow:
		return 1, true
	case BackendInProcess:
		return 2, true
	default:
		return -1, false
	}
}

// ---------------------------------------------------------------------------
// ShmBlobRef (defs.json#/$defs/ShmBlobRef)
// ---------------------------------------------------------------------------

// ShmBlobRef is a descriptor into a blob backend (zero-copy transport). The
// arena writes a fixed header { generation, epoch, length, checksum } before
// each payload; this struct is the wire mirror of that descriptor. The optional
// Backend discriminator selects which pluggable backend resolves it; it defaults
// to Shm and is omitted on the wire when default, so a descriptor minted before
// the field existed round-trips byte-identically. A present value outside the
// enum is rejected on decode (#lzblobbackendstrict).
type ShmBlobRef struct {
	Offset     int64           `json:"offset"`
	Len        int64           `json:"len"`
	Generation int64           `json:"generation"`
	Epoch      int64           `json:"epoch"`
	Checksum   int64           `json:"checksum"`
	Backend    BlobBackendKind `json:"backend,omitempty"`
}

// WithBackend returns a copy of the descriptor tagged with the given backend
// discriminator (the producer stamps this when spilling to a non-default
// backend; the receiver routes resolution by it).
func (r ShmBlobRef) WithBackend(kind BlobBackendKind) ShmBlobRef {
	r.Backend = kind
	return r
}

// MarshalJSON emits the descriptor with fields in schema order, omitting the
// `backend` field when it is the default (Shm) so a descriptor with no backend
// round-trips byte-identically to the pre-field form.
func (r ShmBlobRef) MarshalJSON() ([]byte, error) {
	type wire struct {
		Offset     int64  `json:"offset"`
		Len        int64  `json:"len"`
		Generation int64  `json:"generation"`
		Epoch      int64  `json:"epoch"`
		Checksum   int64  `json:"checksum"`
		Backend    string `json:"backend,omitempty"`
	}
	w := wire{r.Offset, r.Len, r.Generation, r.Epoch, r.Checksum, ""}
	if !r.Backend.IsDefault() {
		w.Backend = string(r.Backend.Normalized())
	}
	return json.Marshal(w)
}

// NewShmBlobRef constructs a ShmBlobRef, rejecting negative fields.
func NewShmBlobRef(offset, length, generation, epoch, checksum int64) (ShmBlobRef, error) {
	for _, f := range []struct {
		name string
		v    int64
	}{
		{"offset", offset}, {"len", length}, {"generation", generation},
		{"epoch", epoch}, {"checksum", checksum},
	} {
		if f.v < 0 {
			return ShmBlobRef{}, fmt.Errorf("%s must be a non-negative integer (was %d)", f.name, f.v)
		}
	}
	return ShmBlobRef{Offset: offset, Len: length, Generation: generation, Epoch: epoch, Checksum: checksum}, nil
}

// UnmarshalJSON validates non-negative fields to match the Dart fromWire and
// decides the optional `backend` discriminator (#lzblobbackendstrict): ABSENT
// decodes as Shm, and a PRESENT value outside the enum is REJECTED naming the
// token rather than normalized to Shm.
//
// The distinction needs a pointer. Decoding straight into BlobBackendKind makes
// an omitted field and `"backend": ""` indistinguishable, and only the first of
// those is the forward-compat channel.
//
// The pointer carries a THIRD case that is behaviour, not an implementation
// detail: an explicit `"backend": null` leaves it nil, so a null decodes as Shm
// exactly as an omitted field does. That is required, not incidental — a
// serde-style peer that did not apply `skip_serializing_if` to an optional field
// emits null where a conforming encoder omits, so refusing it would be stricter
// than the reference implementation on a frame the reference implementation
// produces (§ NodeKey, `#lzkeynullstrict`). Replacing `*string` with a decode
// that distinguishes null from absent must keep giving them the same answer;
// `backend_null_json` / `backend_null_msgpack` in
// codec/blob_backend_discriminator.json are the fixtures that hold it.
//
// A `backend` that is present and not a string is refused by encoding/json
// itself, as a *json.UnmarshalTypeError — the same returned-error family the
// unknown-token refusal above uses, so one `if err != nil` catches both. The
// msgpack codec bridges into this decoder, so it inherits all three answers.
func (r *ShmBlobRef) UnmarshalJSON(b []byte) error {
	var x struct {
		Offset     int64   `json:"offset"`
		Len        int64   `json:"len"`
		Generation int64   `json:"generation"`
		Epoch      int64   `json:"epoch"`
		Checksum   int64   `json:"checksum"`
		Backend    *string `json:"backend"`
	}
	if err := json.Unmarshal(b, &x); err != nil {
		return err
	}
	if x.Offset < 0 || x.Len < 0 || x.Generation < 0 || x.Epoch < 0 || x.Checksum < 0 {
		return fmt.Errorf("ShmBlobRef fields must be non-negative")
	}
	backend := BackendShm
	if x.Backend != nil {
		parsed, err := ParseBlobBackendKind(*x.Backend)
		if err != nil {
			return fmt.Errorf("ShmBlobRef.backend: %w", err)
		}
		backend = parsed
	}
	*r = ShmBlobRef{
		Offset:     x.Offset,
		Len:        x.Len,
		Generation: x.Generation,
		Epoch:      x.Epoch,
		Checksum:   x.Checksum,
		Backend:    backend,
	}
	return nil
}

func (r ShmBlobRef) String() string {
	return fmt.Sprintf("ShmBlobRef(offset=%d, len=%d, generation=%d, epoch=%d, checksum=%d, backend=%s)",
		r.Offset, r.Len, r.Generation, r.Epoch, r.Checksum, r.Backend.Normalized())
}

// ---------------------------------------------------------------------------
// NodeState (protocol.md § NodeState, defs.json#/$defs/NodeState)
// ---------------------------------------------------------------------------

// NodeState is the body of a NodeSnapshot / NodeAdd. Externally tagged: a
// single-key object keyed by the PascalCase variant name, except Opaque which
// is the bare unit string "Opaque".
type NodeState interface {
	MarshalJSON() ([]byte, error)
	isNodeState()
}

// NodeStatePayload holds concrete serialized value bytes ({"Payload": [u8]}).
type NodeStatePayload struct {
	Bytes []byte
}

func (NodeStatePayload) isNodeState() {}

// MarshalJSON emits {"Payload": [u8]} with bytes as a JSON u8 array (not base64).
func (p NodeStatePayload) MarshalJSON() ([]byte, error) {
	return taggedJSON("Payload", bytesWire(p.Bytes))
}

// NodeStateSharedBlob is a concrete value stored in shared memory
// ({"SharedBlob": ShmBlobRef}).
type NodeStateSharedBlob struct {
	Blob ShmBlobRef
}

func (NodeStateSharedBlob) isNodeState() {}

func (s NodeStateSharedBlob) MarshalJSON() ([]byte, error) {
	return taggedJSON("SharedBlob", s.Blob)
}

// NodeStateOpaque is a visible node whose value cannot be serialized (the bare
// unit string "Opaque").
type NodeStateOpaque struct{}

func (NodeStateOpaque) isNodeState() {}

func (NodeStateOpaque) MarshalJSON() ([]byte, error) { return json.Marshal("Opaque") }

func unmarshalNodeState(raw json.RawMessage) (NodeState, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("NodeState must not be empty")
	}
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return nil, err
		}
		if s == "Opaque" {
			return NodeStateOpaque{}, nil
		}
		return nil, fmt.Errorf("unknown NodeState unit variant: %s", s)
	}
	tag, body, err := splitTagged(trimmed, "NodeState")
	if err != nil {
		return nil, err
	}
	switch tag {
	case "Payload":
		b, err := parseByteArray(body)
		if err != nil {
			return nil, err
		}
		return NodeStatePayload{Bytes: b}, nil
	case "SharedBlob":
		var blob ShmBlobRef
		if err := json.Unmarshal(body, &blob); err != nil {
			return nil, err
		}
		return NodeStateSharedBlob{Blob: blob}, nil
	case "Opaque":
		return NodeStateOpaque{}, nil
	default:
		return nil, fmt.Errorf("unknown NodeState variant: %s", tag)
	}
}

// ---------------------------------------------------------------------------
// IpcValue (protocol.md § IpcValue, defs.json#/$defs/IpcValue)
// ---------------------------------------------------------------------------

// IpcValue is a DeltaOp / CrdtOp cell payload. Externally tagged:
// {"Inline": [u8]} or {"SharedBlob": ShmBlobRef}.
type IpcValue interface {
	MarshalJSON() ([]byte, error)
	isIpcValue()
}

// IpcValueInline is an inline byte-array payload ({"Inline": [u8]}).
type IpcValueInline struct {
	Bytes []byte
}

func (IpcValueInline) isIpcValue() {}

func (v IpcValueInline) MarshalJSON() ([]byte, error) {
	return taggedJSON("Inline", bytesWire(v.Bytes))
}

// IpcValueSharedBlob is a payload descriptor into shared memory
// ({"SharedBlob": ShmBlobRef}).
type IpcValueSharedBlob struct {
	Blob ShmBlobRef
}

func (IpcValueSharedBlob) isIpcValue() {}

func (v IpcValueSharedBlob) MarshalJSON() ([]byte, error) {
	return taggedJSON("SharedBlob", v.Blob)
}

// IpcValueOf normalizes an IpcValue, ShmBlobRef, []byte, or []int into an
// IpcValue (mirrors `IpcValue.of` in the sibling bindings).
func IpcValueOf(value any) (IpcValue, error) {
	switch x := value.(type) {
	case IpcValue:
		return x, nil
	case ShmBlobRef:
		return IpcValueSharedBlob{Blob: x}, nil
	case []byte:
		return IpcValueInline{Bytes: x}, nil
	case []int:
		b := make([]byte, len(x))
		for i, v := range x {
			if v < 0 || v > 255 {
				return nil, fmt.Errorf("byte payload[%d] must be in 0..255 (was %d)", i, v)
			}
			b[i] = byte(v)
		}
		return IpcValueInline{Bytes: b}, nil
	default:
		return nil, fmt.Errorf("cannot coerce %T into an IpcValue", value)
	}
}

func unmarshalIpcValue(raw json.RawMessage) (IpcValue, error) {
	tag, body, err := splitTagged(raw, "IpcValue")
	if err != nil {
		return nil, err
	}
	switch tag {
	case "Inline":
		b, err := parseByteArray(body)
		if err != nil {
			return nil, err
		}
		return IpcValueInline{Bytes: b}, nil
	case "SharedBlob":
		var blob ShmBlobRef
		if err := json.Unmarshal(body, &blob); err != nil {
			return nil, err
		}
		return IpcValueSharedBlob{Blob: blob}, nil
	default:
		return nil, fmt.Errorf("unknown IpcValue variant: %s", tag)
	}
}

// ---------------------------------------------------------------------------
// NodeSnapshot / EdgeSnapshot / Snapshot (snapshot.json)
// ---------------------------------------------------------------------------

// NodeSnapshot is a serialized node in a Snapshot. The optional Key is a
// wire-stable NodeKey, omitted from JSON when nil.
type NodeSnapshot struct {
	Node    NodeId
	TypeTag string
	State   NodeState
	Key     *NodeKey
}

// MarshalJSON emits { node, type_tag, state[, key] }, omitting key when nil.
func (n NodeSnapshot) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Node    NodeId    `json:"node"`
		TypeTag string    `json:"type_tag"`
		State   NodeState `json:"state"`
		Key     *NodeKey  `json:"key,omitempty"`
	}{n.Node, n.TypeTag, n.State, n.Key})
}

func (n *NodeSnapshot) UnmarshalJSON(b []byte) error {
	var w struct {
		Node    NodeId          `json:"node"`
		TypeTag string          `json:"type_tag"`
		State   json.RawMessage `json:"state"`
		Key     *NodeKey        `json:"key"`
	}
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	state, err := unmarshalNodeState(w.State)
	if err != nil {
		return err
	}
	n.Node, n.TypeTag, n.State, n.Key = w.Node, w.TypeTag, state, w.Key
	return nil
}

// EdgeSnapshot is a dependency edge: Dependent reads Dependency.
type EdgeSnapshot struct {
	Dependent  NodeId `json:"dependent"`
	Dependency NodeId `json:"dependency"`
}

// IsReadableBy reports whether both endpoints are readable by peer (so the edge
// is observable).
func (e EdgeSnapshot) IsReadableBy(permissions *PeerPermissions, peer PeerId) bool {
	return permissions.CanRead(peer, e.Dependent) && permissions.CanRead(peer, e.Dependency)
}

func (e EdgeSnapshot) String() string {
	return fmt.Sprintf("Edge(%d -> %d)", e.Dependent, e.Dependency)
}

// Snapshot is the full graph state, sent on connect and on resync.
type Snapshot struct {
	Epoch Epoch
	Nodes []NodeSnapshot
	Edges []EdgeSnapshot
	Roots []NodeId
}

type snapshotWire struct {
	Epoch Epoch          `json:"epoch"`
	Nodes []NodeSnapshot `json:"nodes"`
	Edges []EdgeSnapshot `json:"edges"`
	Roots []NodeId       `json:"roots"`
}

// MarshalJSON emits { epoch, nodes, edges, roots }, always as arrays (never
// null) to match the Dart toWire.
func (s Snapshot) MarshalJSON() ([]byte, error) {
	return json.Marshal(snapshotWire{
		Epoch: s.Epoch,
		Nodes: nonNilSlice(s.Nodes),
		Edges: nonNilSlice(s.Edges),
		Roots: nonNilSlice(s.Roots),
	})
}

func (s *Snapshot) UnmarshalJSON(b []byte) error {
	var w snapshotWire
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	s.Epoch, s.Nodes, s.Edges, s.Roots = w.Epoch, w.Nodes, w.Edges, w.Roots
	return nil
}

// FilterReadable drops non-readable nodes/edges/roots before serialization
// (omission, not redaction — protocol.md § Permission Boundary).
func (s Snapshot) FilterReadable(permissions *PeerPermissions, peer PeerId) Snapshot {
	nodes := make([]NodeSnapshot, 0, len(s.Nodes))
	for _, n := range s.Nodes {
		if permissions.CanRead(peer, n.Node) {
			nodes = append(nodes, n)
		}
	}
	edges := make([]EdgeSnapshot, 0, len(s.Edges))
	for _, e := range s.Edges {
		if e.IsReadableBy(permissions, peer) {
			edges = append(edges, e)
		}
	}
	return Snapshot{
		Epoch: s.Epoch,
		Nodes: nodes,
		Edges: edges,
		Roots: permissions.FilterReadable(peer, s.Roots),
	}
}

// ---------------------------------------------------------------------------
// DeltaOp variants (delta.json#/$defs/DeltaOp)
// ---------------------------------------------------------------------------

// DeltaOp is one incremental operation in a Delta. All variants are externally
// tagged; DeltaOpNodeAdd carries the optional wire-stable NodeKey.
type DeltaOp interface {
	MarshalJSON() ([]byte, error)
	// TargetReadable reports whether peer may read every node this op names. Ops
	// targeting an unreadable node are omitted from a permission-filtered delta.
	TargetReadable(permissions *PeerPermissions, peer PeerId) bool
	isDeltaOp()
}

// DeltaOpCellSet is a changed-value cell write, PartialEq-guarded at the source.
type DeltaOpCellSet struct {
	Node    NodeId
	Payload IpcValue
}

func (DeltaOpCellSet) isDeltaOp() {}
func (o DeltaOpCellSet) MarshalJSON() ([]byte, error) {
	return taggedJSON("CellSet", struct {
		Node    NodeId   `json:"node"`
		Payload IpcValue `json:"payload"`
	}{o.Node, o.Payload})
}
func (o DeltaOpCellSet) TargetReadable(p *PeerPermissions, peer PeerId) bool {
	return p.CanRead(peer, o.Node)
}

// DeltaOpSlotValue signals that a recompute published a new value.
type DeltaOpSlotValue struct {
	Node    NodeId
	Payload IpcValue
}

func (DeltaOpSlotValue) isDeltaOp() {}
func (o DeltaOpSlotValue) MarshalJSON() ([]byte, error) {
	return taggedJSON("SlotValue", struct {
		Node    NodeId   `json:"node"`
		Payload IpcValue `json:"payload"`
	}{o.Node, o.Payload})
}
func (o DeltaOpSlotValue) TargetReadable(p *PeerPermissions, peer PeerId) bool {
	return p.CanRead(peer, o.Node)
}

// DeltaOpInvalidate marks a node dirtied but not yet recomputed (lazy).
type DeltaOpInvalidate struct {
	Node NodeId
}

func (DeltaOpInvalidate) isDeltaOp() {}
func (o DeltaOpInvalidate) MarshalJSON() ([]byte, error) {
	return taggedJSON("Invalidate", nodeBody{o.Node})
}
func (o DeltaOpInvalidate) TargetReadable(p *PeerPermissions, peer PeerId) bool {
	return p.CanRead(peer, o.Node)
}

// DeltaOpNodeAdd adds a new node (optional wire-stable Key, omitted when nil).
type DeltaOpNodeAdd struct {
	Node    NodeId
	TypeTag string
	State   NodeState
	Key     *NodeKey
}

func (DeltaOpNodeAdd) isDeltaOp() {}
func (o DeltaOpNodeAdd) MarshalJSON() ([]byte, error) {
	return taggedJSON("NodeAdd", struct {
		Node    NodeId    `json:"node"`
		TypeTag string    `json:"type_tag"`
		State   NodeState `json:"state"`
		Key     *NodeKey  `json:"key,omitempty"`
	}{o.Node, o.TypeTag, o.State, o.Key})
}
func (o DeltaOpNodeAdd) TargetReadable(p *PeerPermissions, peer PeerId) bool {
	return p.CanRead(peer, o.Node)
}

// DeltaOpNodeRemove removes a node (free-list reuse: Remove then Add).
type DeltaOpNodeRemove struct {
	Node NodeId
}

func (DeltaOpNodeRemove) isDeltaOp() {}
func (o DeltaOpNodeRemove) MarshalJSON() ([]byte, error) {
	return taggedJSON("NodeRemove", nodeBody{o.Node})
}
func (o DeltaOpNodeRemove) TargetReadable(p *PeerPermissions, peer PeerId) bool {
	return p.CanRead(peer, o.Node)
}

// DeltaOpEdgeAdd adds a new dependency edge.
type DeltaOpEdgeAdd struct {
	Dependent  NodeId
	Dependency NodeId
}

func (DeltaOpEdgeAdd) isDeltaOp() {}
func (o DeltaOpEdgeAdd) MarshalJSON() ([]byte, error) {
	return taggedJSON("EdgeAdd", edgeBody{o.Dependent, o.Dependency})
}
func (o DeltaOpEdgeAdd) TargetReadable(p *PeerPermissions, peer PeerId) bool {
	return p.CanRead(peer, o.Dependent) && p.CanRead(peer, o.Dependency)
}

// DeltaOpEdgeRemove removes a dependency edge.
type DeltaOpEdgeRemove struct {
	Dependent  NodeId
	Dependency NodeId
}

func (DeltaOpEdgeRemove) isDeltaOp() {}
func (o DeltaOpEdgeRemove) MarshalJSON() ([]byte, error) {
	return taggedJSON("EdgeRemove", edgeBody{o.Dependent, o.Dependency})
}
func (o DeltaOpEdgeRemove) TargetReadable(p *PeerPermissions, peer PeerId) bool {
	return p.CanRead(peer, o.Dependent) && p.CanRead(peer, o.Dependency)
}

type nodeBody struct {
	Node NodeId `json:"node"`
}

type edgeBody struct {
	Dependent  NodeId `json:"dependent"`
	Dependency NodeId `json:"dependency"`
}

func unmarshalDeltaOp(raw json.RawMessage) (DeltaOp, error) {
	tag, body, err := splitTagged(raw, "DeltaOp")
	if err != nil {
		return nil, err
	}
	switch tag {
	case "CellSet":
		var b struct {
			Node    NodeId          `json:"node"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(body, &b); err != nil {
			return nil, err
		}
		payload, err := unmarshalIpcValue(b.Payload)
		if err != nil {
			return nil, err
		}
		return DeltaOpCellSet{Node: b.Node, Payload: payload}, nil
	case "SlotValue":
		var b struct {
			Node    NodeId          `json:"node"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(body, &b); err != nil {
			return nil, err
		}
		payload, err := unmarshalIpcValue(b.Payload)
		if err != nil {
			return nil, err
		}
		return DeltaOpSlotValue{Node: b.Node, Payload: payload}, nil
	case "Invalidate":
		var b nodeBody
		if err := json.Unmarshal(body, &b); err != nil {
			return nil, err
		}
		return DeltaOpInvalidate{Node: b.Node}, nil
	case "NodeAdd":
		var b struct {
			Node    NodeId          `json:"node"`
			TypeTag string          `json:"type_tag"`
			State   json.RawMessage `json:"state"`
			Key     *NodeKey        `json:"key"`
		}
		if err := json.Unmarshal(body, &b); err != nil {
			return nil, err
		}
		state, err := unmarshalNodeState(b.State)
		if err != nil {
			return nil, err
		}
		return DeltaOpNodeAdd{Node: b.Node, TypeTag: b.TypeTag, State: state, Key: b.Key}, nil
	case "NodeRemove":
		var b nodeBody
		if err := json.Unmarshal(body, &b); err != nil {
			return nil, err
		}
		return DeltaOpNodeRemove{Node: b.Node}, nil
	case "EdgeAdd":
		var b edgeBody
		if err := json.Unmarshal(body, &b); err != nil {
			return nil, err
		}
		return DeltaOpEdgeAdd{Dependent: b.Dependent, Dependency: b.Dependency}, nil
	case "EdgeRemove":
		var b edgeBody
		if err := json.Unmarshal(body, &b); err != nil {
			return nil, err
		}
		return DeltaOpEdgeRemove{Dependent: b.Dependent, Dependency: b.Dependency}, nil
	default:
		return nil, fmt.Errorf("unknown DeltaOp variant: %s", tag)
	}
}

// ---------------------------------------------------------------------------
// DeltaApplyStatus (lean ApplyDecision)
// ---------------------------------------------------------------------------

// DeltaApplyStatus is the outcome of attempting to apply a Delta.
type DeltaApplyStatus interface {
	IsApply() bool
	IsResyncRequired() bool
	isDeltaApplyStatus()
}

// DeltaApplyStatusApply means the delta was sequential and may be applied; the
// new epoch is NewEpoch.
type DeltaApplyStatusApply struct {
	NewEpoch Epoch
}

func (DeltaApplyStatusApply) isDeltaApplyStatus()    {}
func (DeltaApplyStatusApply) IsApply() bool          { return true }
func (DeltaApplyStatusApply) IsResyncRequired() bool { return false }

// DeltaApplyStatusResyncRequired means a gap, reorder, or sender restart was
// detected; request a fresh snapshot.
type DeltaApplyStatusResyncRequired struct {
	LastEpoch Epoch
	BaseEpoch Epoch
	Epoch     Epoch
}

func (DeltaApplyStatusResyncRequired) isDeltaApplyStatus()    {}
func (DeltaApplyStatusResyncRequired) IsApply() bool          { return false }
func (DeltaApplyStatusResyncRequired) IsResyncRequired() bool { return true }

// ---------------------------------------------------------------------------
// Delta (delta.json)
// ---------------------------------------------------------------------------

// Delta is an incremental change set.
type Delta struct {
	BaseEpoch Epoch
	Epoch     Epoch
	Ops       []DeltaOp
}

// DeltaNext builds the next sequential delta after baseEpoch carrying ops.
//
// lean theorem `nextDelta_epoch`: the returned Epoch is always baseEpoch + 1.
func DeltaNext(baseEpoch Epoch, ops []DeltaOp) Delta {
	cp := make([]DeltaOp, len(ops))
	copy(cp, ops)
	return Delta{BaseEpoch: baseEpoch, Epoch: baseEpoch + 1, Ops: cp}
}

// IsNextAfter reports whether this delta continues immediately after lastEpoch
// (lean `isSequentialAfter`).
func (d Delta) IsNextAfter(lastEpoch Epoch) bool {
	return d.BaseEpoch == lastEpoch && d.Epoch == d.BaseEpoch+1
}

// ApplyStatus applies iff sequential, otherwise fails closed (lean `applyDelta`).
func (d Delta) ApplyStatus(lastEpoch Epoch) DeltaApplyStatus {
	if d.IsNextAfter(lastEpoch) {
		return DeltaApplyStatusApply{NewEpoch: d.Epoch}
	}
	return DeltaApplyStatusResyncRequired{LastEpoch: lastEpoch, BaseEpoch: d.BaseEpoch, Epoch: d.Epoch}
}

// FilterReadable drops ops whose target node(s) are unreadable by peer.
func (d Delta) FilterReadable(permissions *PeerPermissions, peer PeerId) Delta {
	ops := make([]DeltaOp, 0, len(d.Ops))
	for _, op := range d.Ops {
		if op.TargetReadable(permissions, peer) {
			ops = append(ops, op)
		}
	}
	return Delta{BaseEpoch: d.BaseEpoch, Epoch: d.Epoch, Ops: ops}
}

type deltaWire struct {
	BaseEpoch Epoch     `json:"base_epoch"`
	Epoch     Epoch     `json:"epoch"`
	Ops       []DeltaOp `json:"ops"`
}

func (d Delta) MarshalJSON() ([]byte, error) {
	return json.Marshal(deltaWire{
		BaseEpoch: d.BaseEpoch,
		Epoch:     d.Epoch,
		Ops:       nonNilSlice(d.Ops),
	})
}

func (d *Delta) UnmarshalJSON(b []byte) error {
	var w struct {
		BaseEpoch Epoch             `json:"base_epoch"`
		Epoch     Epoch             `json:"epoch"`
		Ops       []json.RawMessage `json:"ops"`
	}
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	ops := make([]DeltaOp, len(w.Ops))
	for i, raw := range w.Ops {
		op, err := unmarshalDeltaOp(raw)
		if err != nil {
			return err
		}
		ops[i] = op
	}
	d.BaseEpoch, d.Epoch, d.Ops = w.BaseEpoch, w.Epoch, ops
	return nil
}

// ---------------------------------------------------------------------------
// Distributed CRDT plane wire types (distributed.json)
// ---------------------------------------------------------------------------

// WireStamp is the codec-stable wire mirror of the runtime HLC stamp — a total
// order (wall_time, logical, peer). It is all plain integers so the wire format
// is stable whether or not a peer compiles the CRDT runtime in; the runtime
// representation is HlcStamp (hlc.go). Conversion HlcStamp<->WireStamp is owned
// by crdt.go.
type WireStamp struct {
	WallTime int64  `json:"wall_time"`
	Logical  int64  `json:"logical"`
	Peer     PeerId `json:"peer"`
}

// NewWireStamp constructs a WireStamp.
func NewWireStamp(wallTime, logical int64, peer PeerId) WireStamp {
	return WireStamp{WallTime: wallTime, Logical: logical, Peer: peer}
}

func (s WireStamp) String() string {
	return fmt.Sprintf("WireStamp(wall=%d, logical=%d, peer=%d)", s.WallTime, s.Logical, s.Peer)
}

// CrdtOp is one CRDT cell op on the wire (state-based / CvRDT): the converged
// State for Node, tagged with the WireStamp that produced it and an optional
// wire-stable NodeKey.
//
// Wire note (mirrors lazily-rs derived serde and every sibling): Key is ALWAYS
// present in the wire object (null when unset), unlike NodeSnapshot /
// DeltaOpNodeAdd which omit it. The decoder also accepts an absent field.
type CrdtOp struct {
	Node  NodeId
	Key   *NodeKey
	Stamp WireStamp
	State IpcValue
}

// NewCrdtOp constructs a keyless op (addressed only by node).
func NewCrdtOp(node NodeId, stamp WireStamp, state IpcValue) CrdtOp {
	return CrdtOp{Node: node, Stamp: stamp, State: state}
}

// NewKeyedCrdtOp constructs an op carrying a wire-stable NodeKey.
func NewKeyedCrdtOp(node NodeId, key NodeKey, stamp WireStamp, state IpcValue) CrdtOp {
	return CrdtOp{Node: node, Key: &key, Stamp: stamp, State: state}
}

func (o CrdtOp) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Node  NodeId    `json:"node"`
		Key   *NodeKey  `json:"key"`
		Stamp WireStamp `json:"stamp"`
		State IpcValue  `json:"state"`
	}{o.Node, o.Key, o.Stamp, o.State})
}

func (o *CrdtOp) UnmarshalJSON(b []byte) error {
	var w struct {
		Node  NodeId          `json:"node"`
		Key   *NodeKey        `json:"key"`
		Stamp WireStamp       `json:"stamp"`
		State json.RawMessage `json:"state"`
	}
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	state, err := unmarshalIpcValue(w.State)
	if err != nil {
		return err
	}
	o.Node, o.Key, o.Stamp, o.State = w.Node, w.Key, w.Stamp, state
	return nil
}

// TargetReadable reports whether peer may read Node. Filtered ops are omitted
// (not redacted) from a permission-filtered CrdtSync.
func (o CrdtOp) TargetReadable(permissions *PeerPermissions, peer PeerId) bool {
	return permissions.CanRead(peer, o.Node)
}

// StampFrontierEntry is a (peer, WireStamp) entry in the per-peer stamp
// frontier. On the wire this is a 2-tuple array [peer, stamp] (per
// distributed.json#/$defs/StampFrontierEntry prefixItems), not a {peer, stamp}
// object — mirroring lazily-rs's Vec<(u64, WireStamp)> serde representation.
type StampFrontierEntry struct {
	Peer  PeerId
	Stamp WireStamp
}

func (e StampFrontierEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{e.Peer, e.Stamp})
}

func (e *StampFrontierEntry) UnmarshalJSON(b []byte) error {
	var arr []json.RawMessage
	if err := json.Unmarshal(b, &arr); err != nil {
		return err
	}
	if len(arr) != 2 {
		return fmt.Errorf("StampFrontierEntry must be a 2-element array, got %d elements", len(arr))
	}
	var peer PeerId
	if err := json.Unmarshal(arr[0], &peer); err != nil {
		return fmt.Errorf("frontier peer must be an integer: %w", err)
	}
	if peer < 0 {
		return fmt.Errorf("frontier peer must be a non-negative integer, got %d", peer)
	}
	var stamp WireStamp
	if err := json.Unmarshal(arr[1], &stamp); err != nil {
		return err
	}
	e.Peer, e.Stamp = peer, stamp
	return nil
}

func (e StampFrontierEntry) String() string {
	return fmt.Sprintf("(%d, %s)", e.Peer, e.Stamp)
}

// CrdtSync is a CRDT anti-entropy sync frame (the multi-writer plane). The
// sender advertises its per-peer stamp Frontier (the highest WireStamp observed
// from each peer) and ships a batch of Ops. The exchange is bounded, idempotent,
// and resumable; re-sending a frame the receiver already has is a no-op.
type CrdtSync struct {
	Frontier []StampFrontierEntry
	Ops      []CrdtOp
}

type crdtSyncWire struct {
	Frontier []StampFrontierEntry `json:"frontier"`
	Ops      []CrdtOp             `json:"ops"`
}

func (c CrdtSync) MarshalJSON() ([]byte, error) {
	return json.Marshal(crdtSyncWire{
		Frontier: nonNilSlice(c.Frontier),
		Ops:      nonNilSlice(c.Ops),
	})
}

func (c *CrdtSync) UnmarshalJSON(b []byte) error {
	var w crdtSyncWire
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	c.Frontier, c.Ops = w.Frontier, w.Ops
	return nil
}

// FilterReadable returns a peer-specific frame that omits ops for non-readable
// nodes entirely (omission, not redaction — mirroring Delta.FilterReadable).
// The Frontier advertisement is retained in full: it names peers and stamps,
// not node content, and the receiver needs the whole frontier to compute a
// sound causal-stability watermark.
func (c CrdtSync) FilterReadable(permissions *PeerPermissions, peer PeerId) CrdtSync {
	frontier := make([]StampFrontierEntry, len(c.Frontier))
	copy(frontier, c.Frontier)
	ops := make([]CrdtOp, 0, len(c.Ops))
	for _, op := range c.Ops {
		if op.TargetReadable(permissions, peer) {
			ops = append(ops, op)
		}
	}
	return CrdtSync{Frontier: frontier, Ops: ops}
}

// ---------------------------------------------------------------------------
// Reliable-sync reverse-channel control frames (#lzsync, reliable-sync.json)
// ---------------------------------------------------------------------------

// ResyncRequest is a reliable-sync reverse-channel control frame: request a
// covering Snapshot on a detected gap (#lzsync, spec § ResyncCoordinator). It
// carries no node content, so it is permission-filter- and blob-spill-
// transparent. Wire form: {"from_epoch": N}.
type ResyncRequest struct {
	// FromEpoch is the requesting receiver's last_epoch; the sender replies with
	// a Snapshot { epoch >= from_epoch }.
	FromEpoch Epoch `json:"from_epoch"`
}

// OutboxAck is a reliable-sync reverse-channel control frame: prove receipt
// through ThroughEpoch (#lzsync, spec § DurableOutbox). It advances the
// sender's outbox retention cursor and doubles as the reconnect resume cursor;
// it carries no node content. Wire form: {"through_epoch": N}.
type OutboxAck struct {
	// ThroughEpoch is the highest epoch the receiver has fully applied.
	ThroughEpoch Epoch `json:"through_epoch"`
}

// ---------------------------------------------------------------------------
// IpcMessage (protocol.md § IPC) — the tagged Snapshot/Delta/CrdtSync envelope
// ---------------------------------------------------------------------------

// IpcMessage is a length-prefixed, tagged Snapshot, Delta, CrdtSync, or one of
// the reliable-sync reverse-channel control frames (ResyncRequest / OutboxAck).
// The CrdtSync variant carries multi-writer plane traffic alongside the
// single-producer mirror. Externally tagged: {"Snapshot": ...} / {"Delta": ...}
// / {"CrdtSync": ...} / {"ResyncRequest": ...} / {"OutboxAck": ...}.
type IpcMessage interface {
	MarshalJSON() ([]byte, error)
	// EncodeJSON returns the UTF-8 JSON bytes of the tagged wire form.
	EncodeJSON() ([]byte, error)
	isIpcMessage()
}

// IpcMessageSnapshot wraps a Snapshot.
type IpcMessageSnapshot struct{ Value Snapshot }

func (IpcMessageSnapshot) isIpcMessage()                  {}
func (m IpcMessageSnapshot) MarshalJSON() ([]byte, error) { return taggedJSON("Snapshot", m.Value) }
func (m IpcMessageSnapshot) EncodeJSON() ([]byte, error)  { return m.MarshalJSON() }

// IpcMessageDelta wraps a Delta.
type IpcMessageDelta struct{ Value Delta }

func (IpcMessageDelta) isIpcMessage()                  {}
func (m IpcMessageDelta) MarshalJSON() ([]byte, error) { return taggedJSON("Delta", m.Value) }
func (m IpcMessageDelta) EncodeJSON() ([]byte, error)  { return m.MarshalJSON() }

// IpcMessageCrdtSync wraps a CrdtSync.
type IpcMessageCrdtSync struct{ Value CrdtSync }

func (IpcMessageCrdtSync) isIpcMessage()                  {}
func (m IpcMessageCrdtSync) MarshalJSON() ([]byte, error) { return taggedJSON("CrdtSync", m.Value) }
func (m IpcMessageCrdtSync) EncodeJSON() ([]byte, error)  { return m.MarshalJSON() }

// IpcMessageResyncRequest wraps a ResyncRequest control frame (#lzsync).
type IpcMessageResyncRequest struct{ Value ResyncRequest }

func (IpcMessageResyncRequest) isIpcMessage() {}
func (m IpcMessageResyncRequest) MarshalJSON() ([]byte, error) {
	return taggedJSON("ResyncRequest", m.Value)
}
func (m IpcMessageResyncRequest) EncodeJSON() ([]byte, error) { return m.MarshalJSON() }

// IpcMessageOutboxAck wraps an OutboxAck control frame (#lzsync).
type IpcMessageOutboxAck struct{ Value OutboxAck }

func (IpcMessageOutboxAck) isIpcMessage()                  {}
func (m IpcMessageOutboxAck) MarshalJSON() ([]byte, error) { return taggedJSON("OutboxAck", m.Value) }
func (m IpcMessageOutboxAck) EncodeJSON() ([]byte, error)  { return m.MarshalJSON() }

// IpcMessageFromWire decodes an externally-tagged IpcMessage from JSON bytes.
func IpcMessageFromWire(data []byte) (IpcMessage, error) {
	tag, body, err := splitTagged(data, "IpcMessage")
	if err != nil {
		return nil, err
	}
	switch tag {
	case "Snapshot":
		var s Snapshot
		if err := json.Unmarshal(body, &s); err != nil {
			return nil, err
		}
		return IpcMessageSnapshot{Value: s}, nil
	case "Delta":
		var d Delta
		if err := json.Unmarshal(body, &d); err != nil {
			return nil, err
		}
		return IpcMessageDelta{Value: d}, nil
	case "CrdtSync":
		var c CrdtSync
		if err := json.Unmarshal(body, &c); err != nil {
			return nil, err
		}
		return IpcMessageCrdtSync{Value: c}, nil
	case "ResyncRequest":
		var r ResyncRequest
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, err
		}
		return IpcMessageResyncRequest{Value: r}, nil
	case "OutboxAck":
		var a OutboxAck
		if err := json.Unmarshal(body, &a); err != nil {
			return nil, err
		}
		return IpcMessageOutboxAck{Value: a}, nil
	default:
		return nil, fmt.Errorf("unknown IpcMessage variant: %s", tag)
	}
}

// DecodeIpcMessageJSON decodes UTF-8 JSON bytes into an IpcMessage.
func DecodeIpcMessageJSON(data []byte) (IpcMessage, error) { return IpcMessageFromWire(data) }

// ---------------------------------------------------------------------------
// Permission boundary (protocol.md § Permission Boundary, distributed.json)
// ---------------------------------------------------------------------------

// OpKind is one of the three independently-gated remote operation kinds. A read
// grant never implies write or effect.
type OpKind string

const (
	// OpKindRead is the read grant.
	OpKindRead OpKind = "read"
	// OpKindWrite is the write grant.
	OpKindWrite OpKind = "write"
	// OpKindTriggerEffect is the effect-trigger grant.
	OpKindTriggerEffect OpKind = "trigger_effect"
)

// RemoteOp is a { kind, node } gated remote operation.
type RemoteOp struct {
	Kind OpKind `json:"kind"`
	Node NodeId `json:"node"`
}

// ReadOp constructs a read RemoteOp.
func ReadOp(node NodeId) RemoteOp { return RemoteOp{Kind: OpKindRead, Node: node} }

// WriteOp constructs a write RemoteOp.
func WriteOp(node NodeId) RemoteOp { return RemoteOp{Kind: OpKindWrite, Node: node} }

// TriggerEffectOp constructs a trigger_effect RemoteOp.
func TriggerEffectOp(node NodeId) RemoteOp { return RemoteOp{Kind: OpKindTriggerEffect, Node: node} }

func (o RemoteOp) String() string {
	return fmt.Sprintf("RemoteOp(%s, %d)", o.Kind, o.Node)
}

// PermissionDenied is returned by PeerPermissions.Check when Peer lacks Op.
type PermissionDenied struct {
	Peer PeerId
	Op   RemoteOp
}

func (e *PermissionDenied) Error() string {
	return fmt.Sprintf("PermissionDenied: peer %d denied %s on node %d", e.Peer, e.Op.Kind, e.Op.Node)
}

// PeerPermissions is a default-deny, per-peer allowlist of RemoteOp grants. The
// three OpKinds are gated independently. Non-allowlisted nodes are omitted
// entirely from a permission-filtered snapshot/delta (not redacted).
//
// Like the Dart original, PeerPermissions is not safe for concurrent use.
type PeerPermissions struct {
	peers map[PeerId]map[OpKind]map[NodeId]struct{}
}

// NewPeerPermissions creates an empty allowlist.
func NewPeerPermissions() *PeerPermissions {
	return &PeerPermissions{peers: map[PeerId]map[OpKind]map[NodeId]struct{}{}}
}

func (p *PeerPermissions) ensure() {
	if p.peers == nil {
		p.peers = map[PeerId]map[OpKind]map[NodeId]struct{}{}
	}
}

// Allow grants peer the op; returns whether this added a new grant.
func (p *PeerPermissions) Allow(peer PeerId, op RemoteOp) bool {
	p.ensure()
	byKind := p.peers[peer]
	if byKind == nil {
		byKind = map[OpKind]map[NodeId]struct{}{}
		p.peers[peer] = byKind
	}
	nodes := byKind[op.Kind]
	if nodes == nil {
		nodes = map[NodeId]struct{}{}
		byKind[op.Kind] = nodes
	}
	if _, ok := nodes[op.Node]; ok {
		return false
	}
	nodes[op.Node] = struct{}{}
	return true
}

// AllowMany grants peer every node in nodes for kind.
func (p *PeerPermissions) AllowMany(peer PeerId, kind OpKind, nodes []NodeId) {
	p.ensure()
	byKind := p.peers[peer]
	if byKind == nil {
		byKind = map[OpKind]map[NodeId]struct{}{}
		p.peers[peer] = byKind
	}
	set := byKind[kind]
	if set == nil {
		set = map[NodeId]struct{}{}
		byKind[kind] = set
	}
	for _, n := range nodes {
		set[n] = struct{}{}
	}
}

// Revoke removes a single grant; returns whether anything was removed.
func (p *PeerPermissions) Revoke(peer PeerId, op RemoteOp) bool {
	byKind := p.peers[peer]
	if byKind == nil {
		return false
	}
	nodes := byKind[op.Kind]
	if nodes == nil {
		return false
	}
	if _, ok := nodes[op.Node]; !ok {
		return false
	}
	delete(nodes, op.Node)
	p.prune(peer)
	return true
}

// RevokePeer drops every grant for peer; returns whether the peer was present.
func (p *PeerPermissions) RevokePeer(peer PeerId) bool {
	if _, ok := p.peers[peer]; !ok {
		return false
	}
	delete(p.peers, peer)
	return true
}

// IsAllowed reports whether peer holds op.
func (p *PeerPermissions) IsAllowed(peer PeerId, op RemoteOp) bool {
	byKind := p.peers[peer]
	if byKind == nil {
		return false
	}
	nodes := byKind[op.Kind]
	if nodes == nil {
		return false
	}
	_, ok := nodes[op.Node]
	return ok
}

// CanRead reports whether peer may read node.
func (p *PeerPermissions) CanRead(peer PeerId, node NodeId) bool {
	return p.IsAllowed(peer, ReadOp(node))
}

// Check returns a *PermissionDenied error unless peer holds op.
func (p *PeerPermissions) Check(peer PeerId, op RemoteOp) error {
	if !p.IsAllowed(peer, op) {
		return &PermissionDenied{Peer: peer, Op: op}
	}
	return nil
}

// FilterReadable returns the readable subset of nodes for peer.
func (p *PeerPermissions) FilterReadable(peer PeerId, nodes []NodeId) []NodeId {
	out := make([]NodeId, 0, len(nodes))
	for _, n := range nodes {
		if p.CanRead(peer, n) {
			out = append(out, n)
		}
	}
	return out
}

// PeerCount reports the number of peers with at least one grant.
func (p *PeerPermissions) PeerCount() int { return len(p.peers) }

func (p *PeerPermissions) prune(peer PeerId) {
	byKind := p.peers[peer]
	if byKind == nil {
		return
	}
	for kind, set := range byKind {
		if len(set) == 0 {
			delete(byKind, kind)
		}
	}
	if len(byKind) == 0 {
		delete(p.peers, peer)
	}
}

// ---------------------------------------------------------------------------
// lazily-lean transition helpers (mirrors lazily-formal LazilyFormal/IPC.lean)
// ---------------------------------------------------------------------------

// CellSetOps is lean `cellSetOps` + theorems `equal_cell_set_is_silent` /
// `changed_cell_set_emits_cell_set`: the PartialEq cell guard. An equal write
// emits no op; a changed write emits exactly one DeltaOpCellSet.
func CellSetOps(node NodeId, oldValue, newValue IpcValue) []DeltaOp {
	if ipcValueEqual(oldValue, newValue) {
		return nil
	}
	return []DeltaOp{DeltaOpCellSet{Node: node, Payload: newValue}}
}

// DownstreamInvalidations is lean `downstreamInvalidations`: each downstream
// node becomes a DeltaOpInvalidate, preserving order.
func DownstreamInvalidations(downstream []NodeId) []DeltaOp {
	ops := make([]DeltaOp, len(downstream))
	for i, n := range downstream {
		ops[i] = DeltaOpInvalidate{Node: n}
	}
	return ops
}

// MemoOps is lean `memoOps` + theorems `equal_memo_suppresses_downstream` /
// `changed_memo_publishes_then_invalidates`: memo equality suppression. An
// equal recompute is silent; a changed recompute publishes a DeltaOpSlotValue
// then invalidates the downstream frontier.
func MemoOps(node NodeId, oldValue, newValue IpcValue, downstream []NodeId) []DeltaOp {
	if ipcValueEqual(oldValue, newValue) {
		return nil
	}
	ops := make([]DeltaOp, 0, 1+len(downstream))
	ops = append(ops, DeltaOpSlotValue{Node: node, Payload: newValue})
	ops = append(ops, DownstreamInvalidations(downstream)...)
	return ops
}

// SignalOps is lean `signalOps` + theorems `equal_signal_is_silent` /
// `changed_signal_materializes_slot_value` /
// `signal_never_emits_bare_invalidate`: a changed eager Signal materializes a
// concrete DeltaOpSlotValue for its backing slot — never a bare
// DeltaOpInvalidate.
func SignalOps(node NodeId, oldValue, newValue IpcValue) []DeltaOp {
	if ipcValueEqual(oldValue, newValue) {
		return nil
	}
	return []DeltaOp{DeltaOpSlotValue{Node: node, Payload: newValue}}
}

// BatchFlush is lean `BatchFlush` + theorems `batch_frontier_is_coalesced`,
// `batch_flush_advances_epoch_once`, `batch_flush_ops_are_frontier_invalidations`.
//
// One outermost batch-flush invalidation pass produces a no-duplicate frontier
// (Frontier) and emits exactly one delta that advances the IPC epoch once. The
// frontier is coalesced: a dependent reached through many changed cells appears
// at most once.
type BatchFlush struct {
	// ChangedCells are the cells that changed in this batch (informational; not
	// serialized).
	ChangedCells []NodeId
	// Frontier is the coalesced, duplicate-free invalidation frontier.
	Frontier []NodeId
	// Ops is Frontier mapped to DeltaOpInvalidate (theorem
	// `batch_flush_ops_are_frontier_invalidations`).
	Ops []DeltaOp
}

// NewBatchFlush coalesces frontier and derives the invalidation ops.
func NewBatchFlush(changedCells, frontier []NodeId) BatchFlush {
	dedup := dedupNodes(frontier)
	return BatchFlush{
		ChangedCells: changedCells,
		Frontier:     dedup,
		Ops:          DownstreamInvalidations(dedup),
	}
}

// ToDelta builds the single delta this flush emits, advancing the epoch exactly
// once (theorem `batch_flush_advances_epoch_once`).
func (b BatchFlush) ToDelta(baseEpoch Epoch) Delta { return DeltaNext(baseEpoch, b.Ops) }

func dedupNodes(nodes []NodeId) []NodeId {
	seen := make(map[NodeId]struct{}, len(nodes))
	out := make([]NodeId, 0, len(nodes))
	for _, n := range nodes {
		if _, ok := seen[n]; !ok {
			seen[n] = struct{}{}
			out = append(out, n)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// JSON shape helpers
// ---------------------------------------------------------------------------

// taggedJSON builds an externally-tagged single-key object {"tag": body},
// preserving body's own field order (Go map marshaling would reorder keys).
func taggedJSON(tag string, body any) ([]byte, error) {
	inner, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	key, err := json.Marshal(tag)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteByte('{')
	buf.Write(key)
	buf.WriteByte(':')
	buf.Write(inner)
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// splitTagged extracts the single (key, value) of an externally-tagged object.
func splitTagged(raw json.RawMessage, name string) (string, json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", nil, fmt.Errorf("%s must be a JSON object: %w", name, err)
	}
	if len(m) != 1 {
		return "", nil, fmt.Errorf("%s must be externally tagged (one key), got %d", name, len(m))
	}
	for k, v := range m {
		return k, v, nil
	}
	return "", nil, fmt.Errorf("%s must be externally tagged (one key)", name)
}

// bytesWire converts a byte slice to a JSON u8 array ([]int), matching the
// spec's "arrays of u8, not base64" wire form. An empty slice yields [].
func bytesWire(b []byte) []int {
	out := make([]int, len(b))
	for i, x := range b {
		out[i] = int(x)
	}
	return out
}

// parseByteArray decodes a JSON u8 array into a byte slice, validating 0..255.
func parseByteArray(raw json.RawMessage) ([]byte, error) {
	var ints []int
	if err := json.Unmarshal(raw, &ints); err != nil {
		return nil, fmt.Errorf("byte payload must be an array of integers: %w", err)
	}
	out := make([]byte, len(ints))
	for i, v := range ints {
		if v < 0 || v > 255 {
			return nil, fmt.Errorf("byte payload[%d] must be in 0..255 (was %d)", i, v)
		}
		out[i] = byte(v)
	}
	return out, nil
}

// nonNilSlice returns s, or an empty (non-nil) slice when s is nil, so JSON
// marshaling emits [] instead of null.
func nonNilSlice[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// ipcValueEqual compares two IpcValues structurally (the PartialEq guard
// input). It is a reflect-free type switch over the closed IpcValue variant
// set (Inline + SharedBlob): the inline payload uses bytes.Equal (avoids the
// reflect.Value boxing + per-byte reflection overhead of DeepEqual), and the
// SharedBlob descriptor is plain struct equality. Falls back to a typed-nil
// check then reflect.DeepEqual only for forward-compatibility with
// hypothetical new variants.
func ipcValueEqual(a, b IpcValue) bool {
	switch av := a.(type) {
	case IpcValueInline:
		bv, ok := b.(IpcValueInline)
		return ok && bytes.Equal(av.Bytes, bv.Bytes)
	case IpcValueSharedBlob:
		bv, ok := b.(IpcValueSharedBlob)
		return ok && av.Blob == bv.Blob
	case nil:
		return b == nil
	default:
		return false
	}
}
