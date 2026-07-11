# Changelog

All notable changes to lazily-go are documented here. This project adheres to
[Semantic Versioning](https://semver.org/) and tracks the shared
[`lazily-spec`](https://github.com/lazily-hub/lazily-spec) protocol version.

## Unreleased

## 0.4.0

### Added

- **Cross-process zero-copy transport (`BlobBackend` / shm / arrow, `#lzzcpy`).**
  Large cell/slot payloads now cross the IPC plane as descriptors, not copies.
  A producer spills an oversized payload to a pluggable `BlobBackend` (minting a
  `ShmBlobRef`) and ships only the descriptor; the receiver resolves it against
  the same backend and reads the bytes in place. Three backends ship:
  `InProcessBackend` (wraps `ShmBlobArena` — single address space),
  `ArrowBackend` (holds Apache Arrow IPC stream bytes), and `ShmBackend` (Linux
  — a genuine POSIX `shm_open` + `mmap` region with an atomic bump allocator,
  resolving cross-mapping). `SpillMessage`/`SpillValue` apply the threshold spill
  policy across `Snapshot`/`Delta`/`CrdtSync`; a receiver-side `BlobRouter`
  resolves any descriptor by its `backend` discriminator. `ShmBlobRef` gains an
  optional `backend` field (`shm` | `arrow` | `in_process`) that defaults to
  `shm` and is omitted on the wire when default, so legacy descriptors validate
  unchanged — the transport is a strict superset of the shared-memory blob path.
  The backend-agnostic laws (spill-then-resolve identity, backend isolation, ABA
  generation safety, checksum integrity) match `lazily-formal`'s
  `ZeroCopyTransport.lean` and the `delta_zero_copy_arrow` conformance fixture.
  Flips the Go **Cross-process zero-copy transport** coverage cell to ✅.

## 0.3.0

### Added

- **Reactive queue (`QueueCell` + `QueueStorage`).** `QueueCell` is a FIFO
  collection composed of cells — not a new cell kind — whose reactive shell
  invalidates by **reader kind** rather than by position: `Head` (the current
  head value), `Len`, `IsEmpty`, `IsFull` (the bounded-queue backpressure
  signal), and `IsClosed`. A push invalidates `Len`/`IsEmpty` (and `Head` when
  transitioning from empty); a pop invalidates `Head`/`Len`/`IsEmpty`; a
  bounded queue's pop that makes room invalidates `IsFull` so a producer
  `Effect` observing it resumes without polling; a no-op (Full / Empty / Closed
  / idempotent close) invalidates nothing. Reader-kind independence comes "for
  free" from the host `Cell` PartialEq guard: the shell re-derives the four
  content cells from storage after each op inside one `Context.Batch`, and any
  cell whose value is unchanged suppresses its cascade (a push to a non-empty
  queue leaves `Head` cached).
  - **SPSC primitive with an MPSC usage rule** — `QueueCell` is a
    single-producer / single-consumer primitive; multi-producer is the same
    primitive used inside a `Context.Batch` boundary (no `MPSCQueueCell` type).
    Per-producer FIFO is preserved; inter-producer order is deterministic within
    a batch.
  - **Pluggable `QueueStorage` backend** — the shell is storage-agnostic; the
    default `VecDequeStorage` (unbounded or bounded) is the reference backend,
    and a custom backend (ring buffer, broker client, consensus log) drops in
    via `NewQueueCellWithStorage`. Distribution is a storage-backend property.
  - **Closure lifecycle** — pop on closed + non-empty drains; pop on closed +
    empty returns `Closed` (distinct from `Empty`); push after close is an
    error; close is idempotent and terminal (the formal
    `Closed_then_stays_Closed` invariant).
  - Conforms with `lazily-spec`/`cell-model.md` § Reactive queues and replays
    all five `lazily-spec`/`conformance/collections/queuecell_*.json` fixtures
    (SPSC push/pop, popped-head observation, MPSC multi-writer, bounded
    backpressure, closure lifecycle), plus direct tests for backpressure-driven
    effect wake-up and a custom ring-buffer backend.

## 0.2.0

### Added

- **Lossless tree CRDT (M1).** `LosslessTreeCrdt` — a single rooted
  concrete-syntax tree whose leaves own every rendered byte (`render == source`
  for valid, invalid, and unknown source). Implements the M1 op vocabulary
  (create / tombstone / intra-parent reorder / leaf-edit / split-leaf /
  merge-adjacent-leaves) with op-based delta sync over a **dotted non-contiguous
  version frontier** (`TreeVersionFrontier` / `TreeDotRange`) that keeps
  delivery holes re-requestable, plus the `prev` causal chain for out-of-order
  text ops. Leaf text embeds `TextCrdt` wholesale; leaf-local wire offsets are
  UTF-8 bytes. Conforms to `schemas/lossless-tree.json` +
  `schemas/lossless-tree-delta.json` and replays all nine
  `conformance/lossless-tree/` compute fixtures (exact round-trip, anti-entropy
  with a hole, concurrent same-parent insert / move+edit / incompatible-shape
  convergence).
- **Command / RPC message plane (`command-plane-v1`).** `CommandProjection`
  (the reducer) + `CommandRpcClient` (the RPC facade) over the four
  externally-tagged frames `CommandSubmit` / `CommandCancel` / `CommandEvents` /
  `CommandProjection`. Terminal authority is the causal receipt, not the event
  or the transport: a unary `call` resolves only when a terminal
  `CausalReceipt` folds in. Implements generation guards, idempotency
  (duplicate command/event/receipt/cancel ids), cancel-before-terminal-only,
  and terminal-conflict fail-closed. Conforms to `schemas/message-passing.json`
  and replays all eight `conformance/message-passing/` fixtures. The plane is
  feature-gated; peers advertise `command-plane-v1` in `CapabilityHandshake`.

### Changed

- **On-node slot cache (perf).** Cached slot values now live on the `Slot`
  itself instead of a shared identity-keyed `Context` map. A read is a direct
  field access on the node, so read latency no longer grows with total graph
  size by probing a whole-graph hash table. On the spreadsheet-scale benchmark
  this flattens viewport recalc from 24.9 µs → **2.70 µs** at 2M cells and
  103 µs → **7.12 µs** at 10M cells, and roughly halves cold/full recalc. No API
  change except `Context.Size()` now reports the number of cached slots (was the
  map length; equivalent); all 153 tests + conformance fixtures unchanged. See
  [BENCHMARKS.md](BENCHMARKS.md).

## 0.1.0

Initial release — the Go binding of the lazily reactive-signals family, at full
feature parity with the other bindings and conformant with `lazily-spec` and
`lazily-formal`.

### Added

- **Reactive graph** — `Context`, `Cell`, `Slot`, `Signal`, `Effect`, `Memo`,
  and `Batch`, with lazy-by-default recompute, the PartialEq guard, eager
  `Signal` materialization, and memo-equality cascade suppression.
- **Thread-safe context** — lock-backed `ThreadSafeContext` for concurrent
  access.
- **Async reactive context** — `AsyncContext`, a channel-serialized reactive
  scope driven by a single owner goroutine, with supersession via context
  cancellation.
- **Flat state machine** (`StateMachine`) and **Harel/SCXML state charts**
  (`StateChart`, `ChartDef`) — compound states, orthogonal regions, shallow and
  deep history, entry/exit/transition actions, and named fail-closed guards.
- **Keyed cell collections** — `CellMap`, `CellTree`, `CellFamily`, with
  value/membership/order reactivity independence and LIS move-minimized
  reconciliation.
- **Memoized semantic tree** (`SemTree`) with ancestor-chain-only recompute.
- **Stable-id alignment** — anchors, content hashes, and word-LCS similarity.
- **CRDTs** — free-text character CRDT (`TextCrdt`) with delta sync, move-aware
  sequence CRDT (`SeqCrdt`), registers (`MvRegister`, `PnCounter`, `CellCrdt`),
  and the hybrid logical clock (`Hlc`, `HlcStamp`, `StampFrontier`).
- **IPC wire protocol** — `Snapshot`, `Delta`, `CrdtSync`, and the shared wire
  primitives, with full JSON round-trip fidelity to the `lazily-spec` fixtures.
- **Shared-memory blob path** (`ShmBlobArena`).
- **Distributed CRDT plane** — `CrdtPlane`, `CrdtPlaneRuntime`, anti-entropy,
  and WebRTC transport + signaling (`SignalingRoom`).
- **State projection / mirror** and **causal receipts** (`CausalReceipts`).
- **C-ABI FFI boundary** (cgo) and **capability negotiation**
  (`CapabilityHandshake`).
- **Instrumentation / benchmarks.**
- Conformance harness replaying the shared `lazily-spec` IPC, collections, and
  state-chart fixtures; CI with `gofmt`/`vet`/`build`/`test` + the race detector.
