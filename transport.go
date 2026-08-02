package lazily

// Cross-process zero-copy transport — pluggable blob backends (#lzzcpy).
//
// Spec:   lazily-spec/docs/zero-copy-transport.md
// Formal: lazily-formal/LazilyFormal/ZeroCopyTransport.lean
// Rust reference: lazily-rs/src/transport.rs
//
// A large payload is not copied through the wire codec. The producer **spills**
// it to a blob backend (the backend mints a ShmBlobRef descriptor) and ships
// only the descriptor; the receiver **resolves** the descriptor against the same
// backend and reads the bytes in place — zero copy. The BlobBackend interface is
// the adapter seam:
//
//   - InProcessBackend wraps a ShmBlobArena — single address space (the FFI host
//     / an editor plugin loaded in the same process).
//   - ArrowBackend holds Apache Arrow IPC stream bytes — the descriptor's bytes
//     are an Arrow IPC stream the receiver imports as an Array / RecordBatch with
//     no copy (bring your own Arrow reader around the resolved []byte).
//   - ShmBackend (transport_shm.go, linux) is a POSIX shm_open + mmap region —
//     the cross-process backend (same host).
//
// Because the formal laws (spill-then-resolve identity, backend isolation, ABA
// generation safety, checksum integrity) are stated only over a backend's
// issued-blob table, they hold uniformly for every backend that maintains the
// BlobBackend contract.

import "fmt"

// DefaultSpillThreshold is the default byte size at or above which SpillValue /
// SpillMessage spill an inline payload to a backend. It is a deployment knob,
// not a protocol constant: payloads below the threshold stay Inline (copying a
// tiny value through the codec is cheaper than a backend round-trip). Callers
// pass their own threshold to the Spill* functions.
const DefaultSpillThreshold = 512

// BlobBackend is the adapter seam: a backend mints descriptors via Write and
// resolves them zero-copy via ReadView. Entries are immutable and stably
// addressed for any descriptor's lifetime, so the transport laws (resolve_write
// identity, backend isolation, ABA generation safety, checksum rejection) hold
// for every backend by construction.
type BlobBackend interface {
	// Kind reports which backend discriminator this adapter serves.
	Kind() BlobBackendKind
	// Write mints a fresh descriptor for bytes: it stores the bytes immutably
	// and returns a descriptor whose Checksum is the bytes' FNV-1a-64, tagged
	// with this backend's Kind.
	Write(bytes []byte) (ShmBlobRef, error)
	// ReadView resolves descriptor zero-copy: it returns the stored bytes and
	// ok=true iff generation + epoch + len + checksum all match; (nil, false)
	// otherwise. No copy, no checksum recompute. The returned slice aliases the
	// backend's storage and is valid only while the backend holds the entry.
	ReadView(descriptor ShmBlobRef) ([]byte, bool)
	// AdvanceEpoch advances the validity epoch. Descriptors minted before an
	// epoch advance no longer resolve (models compaction / restart).
	AdvanceEpoch()
}

// ---------------------------------------------------------------------------
// InProcessBackend — wraps a ShmBlobArena (single address space).
// ---------------------------------------------------------------------------

// InProcessBackend is the default in-process backend: it wraps a ShmBlobArena
// for the single-address-space case (the FFI host ↔ a binding loaded in the same
// process, an editor plugin). Descriptors carry Backend = BackendInProcess. For
// a genuine cross-process store, spill to a ShmBackend (linux) instead.
type InProcessBackend struct {
	arena *ShmBlobArena
}

// NewInProcessBackend creates an in-process backend over a fresh arena at
// epoch 0.
func NewInProcessBackend() *InProcessBackend {
	return &InProcessBackend{arena: NewShmBlobArena(0)}
}

// InProcessBackendFromArena wraps an existing arena.
func InProcessBackendFromArena(arena *ShmBlobArena) *InProcessBackend {
	return &InProcessBackend{arena: arena}
}

// Arena returns the backing arena.
func (b *InProcessBackend) Arena() *ShmBlobArena { return b.arena }

// Epoch returns the backend's current validity epoch.
func (b *InProcessBackend) Epoch() int64 { return b.arena.Epoch() }

// Kind reports BackendInProcess.
func (b *InProcessBackend) Kind() BlobBackendKind { return BackendInProcess }

// Write stores bytes in the arena and stamps the descriptor with the in-process
// backend discriminator.
func (b *InProcessBackend) Write(bytes []byte) (ShmBlobRef, error) {
	ref := b.arena.Write(bytes)
	ref.Backend = BackendInProcess
	return ref, nil
}

// ReadView resolves the descriptor zero-copy against the backing arena.
func (b *InProcessBackend) ReadView(descriptor ShmBlobRef) ([]byte, bool) {
	return b.arena.ReadView(descriptor)
}

// AdvanceEpoch advances the backing arena's epoch, invalidating prior
// descriptors.
func (b *InProcessBackend) AdvanceEpoch() { b.arena.AdvanceEpoch() }

// ---------------------------------------------------------------------------
// ArrowBackend — holds Apache Arrow IPC stream bytes (bring your own Arrow).
// ---------------------------------------------------------------------------

// ArrowBackend is the Apache Arrow blob backend: it holds spilled payloads as
// Arrow IPC stream bytes and resolves a descriptor to the buffer's raw bytes
// with no copy. The descriptor's bytes ARE an Arrow IPC stream — a columnar
// consumer imports them as an Array / RecordBatch zero-copy (the Arrow IPC
// format is itself zero-copy across a shared buffer). This adapter stores the
// raw stream bytes and tags the descriptor Backend = BackendArrow; bring your
// own Arrow reader to wrap the resolved []byte into typed Arrow.
//
// Because Arrow's IPC format is zero-copy over a shared buffer, shm and arrow
// compose: an Arrow batch can live in a ShmBackend region and be resolved by
// either backend. New backends (RDMA/verbs, CUDA IPC) plug in by implementing
// BlobBackend and adding a BlobBackendKind value.
type ArrowBackend struct {
	arena *ShmBlobArena
}

// NewArrowBackend creates an Arrow backend over a fresh arena at epoch 0.
func NewArrowBackend() *ArrowBackend {
	return &ArrowBackend{arena: NewShmBlobArena(0)}
}

// Arena returns the backing arena.
func (b *ArrowBackend) Arena() *ShmBlobArena { return b.arena }

// Epoch returns the backend's current validity epoch.
func (b *ArrowBackend) Epoch() int64 { return b.arena.Epoch() }

// Kind reports BackendArrow.
func (b *ArrowBackend) Kind() BlobBackendKind { return BackendArrow }

// Write stores the Arrow IPC stream bytes and stamps the descriptor with the
// Arrow backend discriminator.
func (b *ArrowBackend) Write(bytes []byte) (ShmBlobRef, error) {
	ref := b.arena.Write(bytes)
	ref.Backend = BackendArrow
	return ref, nil
}

// ReadView resolves the descriptor zero-copy against the backing arena.
func (b *ArrowBackend) ReadView(descriptor ShmBlobRef) ([]byte, bool) {
	return b.arena.ReadView(descriptor)
}

// AdvanceEpoch advances the backing arena's epoch, invalidating prior
// descriptors.
func (b *ArrowBackend) AdvanceEpoch() { b.arena.AdvanceEpoch() }

// ---------------------------------------------------------------------------
// Spill policy: replace large Inline payloads with a SharedBlob descriptor.
// ---------------------------------------------------------------------------

// SpillValue spills an IpcValue to backend when it is Inline and >= threshold
// bytes: it writes the bytes and returns a SharedBlob descriptor value plus the
// number of bytes spilled. Otherwise it returns the value unchanged and 0.
// Payloads below the threshold stay inline — cheaper than a backend round-trip
// for tiny values. A backend write failure leaves the value inline (returns 0).
func SpillValue(value IpcValue, backend BlobBackend, threshold int) (IpcValue, int) {
	inl, ok := value.(IpcValueInline)
	if !ok || len(inl.Bytes) < threshold {
		return value, 0
	}
	ref, err := backend.Write(inl.Bytes)
	if err != nil {
		return value, 0
	}
	return IpcValueSharedBlob{Blob: ref}, len(inl.Bytes)
}

// spillState spills a NodeState Payload above threshold to a SharedBlob
// descriptor, returning the (possibly new) state and the bytes spilled.
func spillState(state NodeState, backend BlobBackend, threshold int) (NodeState, int) {
	p, ok := state.(NodeStatePayload)
	if !ok || len(p.Bytes) < threshold {
		return state, 0
	}
	ref, err := backend.Write(p.Bytes)
	if err != nil {
		return state, 0
	}
	return NodeStateSharedBlob{Blob: ref}, len(p.Bytes)
}

// SpillMessage spills large payloads across an IpcMessage's value/state sites —
// Snapshot node states, Delta CellSet/SlotValue payloads + NodeAdd states, and
// CrdtSync op states — returning a message whose oversized payloads are replaced
// by SharedBlob descriptors, plus the total bytes spilled. The message stays
// small on the wire. Sites already carrying a descriptor are left untouched. The
// input message is not mutated; the returned message shares unspilled substructure.
func SpillMessage(message IpcMessage, backend BlobBackend, threshold int) (IpcMessage, int) {
	total := 0
	switch m := message.(type) {
	case IpcMessageSnapshot:
		snap := m.Value
		nodes := make([]NodeSnapshot, len(snap.Nodes))
		copy(nodes, snap.Nodes)
		for i := range nodes {
			st, n := spillState(nodes[i].State, backend, threshold)
			nodes[i].State = st
			total += n
		}
		snap.Nodes = nodes
		return IpcMessageSnapshot{Value: snap}, total
	case IpcMessageDelta:
		d := m.Value
		ops := make([]DeltaOp, len(d.Ops))
		copy(ops, d.Ops)
		for i := range ops {
			switch op := ops[i].(type) {
			case DeltaOpCellSet:
				p, n := SpillValue(op.Payload, backend, threshold)
				op.Payload = p
				ops[i] = op
				total += n
			case DeltaOpSlotValue:
				p, n := SpillValue(op.Payload, backend, threshold)
				op.Payload = p
				ops[i] = op
				total += n
			case DeltaOpNodeAdd:
				st, n := spillState(op.State, backend, threshold)
				op.State = st
				ops[i] = op
				total += n
			}
		}
		d.Ops = ops
		return IpcMessageDelta{Value: d}, total
	case IpcMessageCrdtSync:
		c := m.Value
		ops := make([]CrdtOp, len(c.Ops))
		copy(ops, c.Ops)
		for i := range ops {
			p, n := SpillValue(ops[i].State, backend, threshold)
			ops[i].State = p
			total += n
		}
		c.Ops = ops
		return IpcMessageCrdtSync{Value: c}, total
	default:
		return message, 0
	}
}

// ResolveValue resolves an IpcValue against a single backend: Inline bytes are
// returned directly (ok=true), a SharedBlob is resolved zero-copy against
// backend. Returns (nil, false) when a SharedBlob fails to resolve
// (unknown / stale / corrupt). The returned slice aliases whichever of value or
// backend owns the bytes.
func ResolveValue(value IpcValue, backend BlobBackend) ([]byte, bool) {
	switch x := value.(type) {
	case IpcValueInline:
		return x.Bytes, true
	case IpcValueSharedBlob:
		return backend.ReadView(x.Blob)
	default:
		return nil, false
	}
}

// ---------------------------------------------------------------------------
// BlobRouter — receiver-side multi-backend resolver.
// ---------------------------------------------------------------------------

// BlobRouter is the receiver-side multi-backend resolver. It holds backends by
// BlobBackendKind and resolves any descriptor by its Backend discriminator — a
// shm descriptor routes to the shm backend, an arrow descriptor to the arrow
// backend, etc. This is the resolve_wrong_backend theorem in practice: a
// descriptor never resolves against a backend of the wrong kind (an unregistered
// kind resolves to nothing).
//
// The zero value is a ready empty router; NewBlobRouter is the explicit
// constructor.
type BlobRouter struct {
	backends [3]BlobBackend
}

// NewBlobRouter creates an empty router with no backends registered.
func NewBlobRouter() *BlobRouter { return &BlobRouter{} }

// Register installs backend for its Kind, replacing any previously-registered
// backend of the same kind. It returns the router for chaining.
func (r *BlobRouter) Register(backend BlobBackend) *BlobRouter {
	idx, ok := backend.Kind().routerIndex()
	if !ok {
		panic(fmt.Sprintf("BlobRouter.Register: backend reports unknown kind %q "+
			"(expected \"shm\", \"arrow\" or \"in_process\")", backend.Kind()))
	}
	r.backends[idx] = backend
	return r
}

// ReadView resolves a descriptor by routing to its Backend kind. Returns
// (nil, false) if the kind is one this build cannot route, if no backend is
// registered for it, or if the descriptor did not resolve.
//
// A kind outside the enum resolves to nothing here rather than falling into slot
// 0. Decoding rejects such a descriptor before it can reach the router
// (#lzblobbackendstrict), so this is the in-process counterpart: a descriptor
// hand-built in Go is not routed into the shm table either.
func (r *BlobRouter) ReadView(descriptor ShmBlobRef) ([]byte, bool) {
	idx, ok := descriptor.Backend.routerIndex()
	if !ok {
		return nil, false
	}
	backend := r.backends[idx]
	if backend == nil {
		return nil, false
	}
	return backend.ReadView(descriptor)
}

// Resolve resolves an IpcValue: Inline bytes are returned directly, a SharedBlob
// is routed by its Backend discriminator and resolved zero-copy.
func (r *BlobRouter) Resolve(value IpcValue) ([]byte, bool) {
	switch x := value.(type) {
	case IpcValueInline:
		return x.Bytes, true
	case IpcValueSharedBlob:
		return r.ReadView(x.Blob)
	default:
		return nil, false
	}
}
