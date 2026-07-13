# Changelog

All notable changes to lazily-go are documented here. This project adheres to
[Semantic Versioning](https://semver.org/) and tracks the shared
[`lazily-spec`](https://github.com/lazily-hub/lazily-spec) protocol version.

## Unreleased

## 0.14.0

### Added

- **`WorkQueueCell` competing-consumer delivery (`#lzworkqueue`).** Exclusive
  FIFO claims use stable item ids and fresh delivery ids, worker-scoped
  ack/nack settlement, strict visibility expiry, tail redelivery, bounded
  dead-letter handling, and independent reactive count readers.

## 0.13.1

### Fixed

- **Serialized monotonic outbox cursors.** Every `DurableStoreOutbox` operation
  refreshes the persisted acknowledgement cursor, so a stale handle cannot
  regress replay or retention semantics after another handle advances the same
  durable store.

## 0.13.0

### Added

- **`CrdtTree` (`#lzcrdttree`).** `TextCrdt` now implements the generic
  lossless-document contract with identity-preserving snapshot/delta and merge.
- **Storage-independent durable outbox (`#lzdurableoutbox`).** `OutboxStore`
  provides the five ordered-byte operations, `DurableStoreOutbox` owns cursor and
  replay semantics, and `FileOutboxStore` supplies an append-only crash/restart
  adapter. Ack records fold by `max`, so stale handles cannot regress the cursor.

## 0.12.0

### Added

- **`TopicCell` broadcast topics (`#lztopiccell`).** Independent absolute
  subscriber cursors, durable offline replay, ephemeral disconnect lifecycle,
  per-subscriber reactive invalidation, snapshot restore, and safe prefix GC at
  the slowest durable cursor.

## 0.9.0

### Changed

- **Demand-driven queue reader-kinds + optional `Peek`/`Capacity` (Phase 0,
  `#relaycell`).** `QueueCell` reader-kinds (`Head`/`Len`/`IsEmpty`/`IsFull`) are
  now demand-driven memoized `Slot`s (were eagerly-set `Cell`s): a successful
  push/pop derives no reader value and invalidates only the readers whose value
  provably changed. `Peek`/`Capacity` become optional capabilities via interface
  segregation — the required `QueueStorage` contract is
  `TryPush`/`TryPop`/`Len`/`IsClosed`/`Close`; `PeekableStorage[T]` adds a `Head`
  reader and `BoundedStorage` adds `IsFull`. A raw-channel-style backend conforms
  with neither. Observable semantics are unchanged; all conformance fixtures stay
  green.
- **BREAKING:** `QueueReaderHandles` `Head`/`Len`/`IsEmpty`/`IsFull` are now
  `*Slot` (were `*Cell`); `IsClosed` stays `*Cell`.

## 0.8.0

### Added

- **Reliable Sync (`#lzsync` + `#sync-driver`).** The delivery-reliability layer
  over the `Snapshot`/`Delta`/`CrdtSync` planes (lazily-spec § Reliable Sync), at
  parity with the `lazily-rs`/`lazily-kt`/`lazily-js` references:
  - `ResyncCoordinator` — receiver-side `Apply`/`RequestSnapshot`/`Ignore`
    decision function, multi-epoch-span aware, single-request-per-gap suppression.
  - `DurableOutbox` interface + `InMemoryOutbox` — append-before-send,
    `AckThrough` retention, `ReplayFrom` cursor (at-least-once → exactly-once).
  - `OrSet` (add-wins) + `WireLwwRegister[V]` liveness cells on the CrdtSync plane.
  - `SyncDriver` + `IpcSink`/`IpcSource`/`Clock`/`SnapshotProvider` seams — the
    full-duplex drain → retain-on-fail → receive/route → advertise-ack loop.
  - `ResyncRequest` / `OutboxAck` `IpcMessage` control frames (FFI kinds 4/5).
  Replays the 5 `conformance/reliable-sync/` fixtures + SyncDriver loop-shape
  tests (16 new; 279 total, `-race` clean). Go is now ✅ on both coverage rows.

## 0.7.0

### Changed

- **Keyed-collection unification (`#reactivemap`).** Unified all keyed reactive
  collections on a single generic primitive `ReactiveMap[K, V, H]` (reactive
  membership + order, `GetOrInsertWith` mint-on-access, `Remove`, `Move*`) over
  the entry handle kind `H` (`*Cell[V]` input cells / `*Slot[V]` derived slots),
  mirroring lazily-rs `cell_family.rs`. Two thin specializations embed it with
  the handle fixed: `CellMap[K, V]` (input cells — adds the cell-only `Set` and
  eager value-minting `Entry`/`EntryWith`) and `SlotMap[K, V]` (derived slots —
  `GetOrInsertWith` lazy mint-on-access + `MaterializeAll` eager pre-mint; no
  `Set`). The `Send + Sync` (`ThreadSafeCellMap`/`ThreadSafeSlotMap`) and async
  (`AsyncCellMap`/`AsyncSlotMap`) flavors follow the same shape.
- **Removed the materialization-mode machinery.** Deleted `ReactiveFamily`,
  `ThreadSafeReactiveFamily`, `AsyncReactiveFamily`, `CellFamily`, the
  `MaterializationMode` enum (`Eager`/`Lazy`/`DefaultMaterializationMode`), and
  all `*Family` constructors. There is no longer an eager/lazy mode flag: eager
  is a pre-mint loop (`MaterializeAll`), lazy is mint-on-access
  (`GetOrInsertWith`). Behavior is unchanged; the shared
  `conformance/materialization/*.json` fixtures (now `"model": "SlotMap"`) still
  pass.

## 0.6.0

### Added

- **Thread-safe reactive family (`ThreadSafeReactiveFamily`, `#lzmatmode`).** The
  `Send + Sync` flavor of `ReactiveFamily`: keys `K` map to per-entry reactive
  values of one `EntryKind`, allocated per a `MaterializationMode`, with all
  present-set mutation serialized by an internal `sync.Mutex`. Once built its
  address is stable, so concurrent goroutines may share a `*ThreadSafeReactive
  Family` and serve `Observe`/`Get` from any goroutine with no per-key locking of
  the value axis. Obeys the eager/lazy contract, observational transparency, and
  present-set monotonicity, plus **materialization confluence**: the present set
  and every observed value are independent of the order in which keys are
  materialized under concurrency (`materialize_present_comm` /
  `materialize_observe_comm`). Constructors `ThreadSafeEager/LazySlotFamily` and
  `ThreadSafeEager/LazyCellFamily`; surface `Get`/`Observe`/`Set`/`IsPresent`/
  `PresentKeys`/`PresentCount`/`Mode`/`EntryKind`. Verified under `-race` with an
  N-goroutine confluence soak. Flips the Go **Thread-safe reactive family** cell
  to ✅.

- **Async reactive family (`AsyncReactiveFamily`, `#lzmatmode`).** The async
  flavor of `ReactiveFamily`, adding a **resolution axis** orthogonal to the
  present-set axis: a derived (slot) entry is *pending* until `Drive`n (the
  analog of `AsyncContext.GetAsync`), then *resolved*; input (cell) entries are
  resolved at build. A non-blocking `Observe` returns `(value, ok)` — `(_, false)`
  while pending, `(value, true)` once resolved. The single-threaded transparency
  law weakens to **eventual transparency**: once a node resolves its observed
  value is the canonical value, identical to the synchronous family
  (`eventual_transparency`, `async_resolved_matches_sync`, `observe_pending_is_none`,
  `cell_resolved_at_build`, `resolve_monotone`, `resolve_preserves_observe`).
  Mutex-guarded for cross-goroutine owners. Flips the Go **Async reactive
  family** cell to ✅.

- **Reactive family sync (`#lzfamilysync`).** The distributed CRDT plane
  (`CrdtPlaneRuntime`) now syncs a keyed family as a unit: a keyed op for a
  family entry NOT registered locally **materializes** the entry on ingest
  (seeded from the op's converged register) instead of being dropped —
  membership propagates, values are adopted, a later last-writer-wins update
  converges, re-ingest is idempotent, and a derived aggregate over the family
  (e.g. a count of `true` entries) converges across replicas. Each entry is an
  LWW register addressed by `NodeKey` `namespace/<suffix>`, materialized on a
  locally-private node id (above `familyNodeBase = 1 << 48`) so it never collides
  with an app cell. A `MembershipEpoch` reactive signal bumps whenever an entry
  materializes so a derived aggregate recomputes. Surface `RegisterFamilyLww`/
  `FamilySetLww`/`FamilyKeys`/`FamilyValueLww`/`MembershipEpoch`. Conforms to
  `lazily-formal`'s `FamilySync` module (`applyOp_eq_merge`, `applyOp_present`,
  `applyOp_absent_adopts`, `present_merge`, `applyOp_idem`, `aggregate_converges`)
  and replays `conformance/familysync/materialize_on_ingest.json`. Flips the Go
  **Reactive family sync** cell to ✅ — completing full Go feature parity.

## 0.5.0

### Added

- **Reactive family + materialization mode (`ReactiveFamily`, `#lzmatmode`).** A
  unified keyed reactive family mapping keys `K` to per-entry reactive nodes,
  allocated per a `MaterializationMode`. Entry kind (`EntryKindCell` input
  `*Cell[V]` / `EntryKindSlot` derived `*Slot[V]`) is orthogonal to mode: cell
  entries are always materialized; derived slots are allocated eagerly (default,
  `EagerSlotFamily`) or lazily on first read (`LazySlotFamily` — "materialize on
  pull", never-read nodes never allocated). Materialization mode is never
  observable on the value axis — eager and lazy return identical values
  (observational transparency); it changes allocation timing and memory only.
  Present set grows monotonically (deferral, not de-allocation) and the lazy set
  is a subset of the eager set. `Observe`/`GetCell`/`GetSlot`/`Set`/`IsPresent`/
  `PresentKeys`/`PresentCount`/`Mode`/`EntryKind` surface. Conforms to
  `lazily-formal`'s `Materialization` module and replays the
  `conformance/materialization/*.json` fixtures (observational transparency,
  deferral-not-deallocation, entry-kind orthogonality). The existing `CellFamily`
  is the input-cell collection specialization. Flips the Go **Reactive family**
  coverage cell to ✅.

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
