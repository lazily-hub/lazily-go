# Changelog

All notable changes to lazily-go are documented here. This project adheres to
[Semantic Versioning](https://semver.org/) and tracks the shared
[`lazily-spec`](https://github.com/lazily-hub/lazily-spec) protocol version.

## v0.25.0

### Added

- Portable logical-clock `Timer`, caller-driven `Timeout[T]`, and
  `RevisionBarrier` production APIs with deterministic caller-owned seams.
- Canonical stdlib fixture replay and independently advertised interop peer
  capabilities for all three portable features.

### Fixed

- Portable stdlib parity now rejects and latches regressing barrier clocks,
  preserves terminal results across reentrant cancellation callbacks, and
  retains exact uint64 deadline behavior.

## v0.24.0

### Added

- **The full queue family now ships on all three execution flavors
  (`#lzqfports`).** `ThreadSafeQueueCell` / `AsyncQueueCell`,
  `ThreadSafeTopicCell` / `AsyncTopicCell`, and
  `ThreadSafeWorkQueueCell` / `AsyncWorkQueueCell` bind the same FIFO, cursor,
  lease, and reader-kind laws to their owning graphs. Thread-safe operations
  serialize through `ThreadSafeContext`; async reader derives use the explicit
  `AsyncComputeContext` surface and coalesce changed roots with
  `AsyncContext.Batch`.
- The shared 11-fixture queue-family corpus now replays against every shipped
flavor (31 QueueCell steps, 29 TopicCell steps, and 18 WorkQueueCell steps per
flavor). The runner enforces the 3x3 capability ledger, reads
`steps[].expected.invalidates`, requires positive replay counts, and includes
atomicity, concurrency, exclusive-claim, and deterministic multi-expiry
mutation probes.
- A production interop peer adapter supports capability-negotiated
cross-binding network-suite tests.
- Async handles, constructors, maps, queues, and state projection now use the
canonical `AsyncSource` / `AsyncComputed` / `AsyncComputedState` vocabulary,
with deprecated aliases forwarding to the canonical implementation.

### Fixed

- A first `TopicCell.Subscribe` now invalidates a stable-id reader that was
observed before the subscription existed; its value changes from
`Exists=false` to `Exists=true` even when the unread suffix is empty.
- Signaling rejects malformed frames and conformance coverage rejects corrupt
runtime-manifest fixture ids.

## v0.23.2

### Fixed

- **`ThreadSafeReactiveMap` read paths ran off the context lock — a real data
  race, red under `-race` since v0.23.0.** Mint, the membership/order signal
  bumps, entry disposal, and `Set` all took `tsctx.WithLock`; every *read* went
  straight to the graph. A read is not passive in this kernel: `Get` on a stale
  derived entry runs `refresh` → `recomputeNow` → `Context.newCompute`, which
  bumps `computeGen` and `cachedCount` on the shared single-threaded `Context`,
  so two goroutines reading two *different* keys wrote the same fields. Even a
  `Source` read mutates the dependency edge set when the read surface is a
  `*Compute`. `GetOrInsertWith`, `Observe`, `Keys`, `Len` and `ContainsKey` now
  route through `read`/`track`. The lock is reentrant, so an entry compute that
  reads back into the map is unaffected, and `mu` is still never held across
  graph work, so lock order stays context-then-`mu`.

  `TestTSComputedMapConcurrentMaterializationIsConfluent` — the confluence test
  — was the one failing. It caught the `GetOrInsertWith` path only; the other
  four sites were uncovered, so two new soaks drive them: concurrent tracked
  reads against a reordering mutator, and concurrent `Observe` against a writing
  mutator on a hot key set. All five sites are mutation-checked.

## v0.23.1

No library behaviour changed — no non-test `.go` source differs from v0.23.0.

### Changed

- **Coverage table sync: rs ships the queue family on all three flavors.** Six
  rows in the README support matrix flip to ✅ in the Rust column — `QueueCell`,
  `TopicCell`, and `WorkQueueCell` each gain their thread-safe and async
  flavors. The Go column is unchanged; these rows report another binding's
  status, not this one's.

### Added

- A conformance-coverage guard (`make conformance-coverage`) that fails the
  build when the canonical corpus grows a fixture no test in this repo even
  names, backed by a runtime manifest proving the fixture bytes were really
  read rather than merely mentioned.
- A vendored-fixture drift test that fails the build when a vendored fixture
  diverges from its canonical counterpart.

## v0.23.0

### Added

- **The `ReactiveMap` Core surface now binds every flavor.** Ordering and atomic
  move bound only the single-threaded map; the thread-safe and async maps
  exposed the present set and nothing else. All flavors now carry `keys`, `len`,
  `is_empty`, `contains_key`, `position`, `move_to` / `move_before` /
  `move_after`, and `remove`, each with membership and order signals minted on
  its own graph. A move touches no entry handle and awaits nothing, so it is
  neither thread- nor async-coloured.
- A shared, graph-agnostic `KeyedOrder` core holding the present set, the key
  order, and the move algebra. It deliberately holds no reactivity: membership
  and order invalidation is a graph write, so each flavor owns its own cells.
- The canonical ordering fixtures now replay against all three flavors, with
  invalidation measured by recompute count rather than a cache flag, plus
  directional move coverage the canonical corpus does not provide (its only
  `move_before` step moves a key that already follows its anchor, so the
  `anchor - 1` branch was never exercised).

### Fixed

- The thread-safe and async maps stored plain values with no reactive nodes and
  no context at all. Both are graph-backed now.
- The **single-threaded** map's reactive reads registered no dependency edge:
  `Keys` / `Len` / `ContainsKey` called the zero-argument `Source.Get()`, which
  `core.go` documents as an untracked external read. A slot reading `m.Keys()`
  was never invalidated when a key was added.

### Changed (BREAKING, pre-v1)

- Map constructors take their owning context, and the reactive reads take a
  `ComputeOps` read surface, so a `*Compute` registers the edge and a `*Context`
  does not. `MaterializeAll` / `GetOrInsertWith` / `Observe` / `Keys` / `Len` /
  `ContainsKey` changed arity. Done now, deliberately, while the module is still
  pre-v1 on an unversioned path.

## v0.22.0

### Changed

- **Renamed the keyed maps to `SourceMap` / `ComputedMap`**, finishing the v2 kernel
  migration: the node kinds became `Source` and `Computed`, and the map names now say which
  kind of entry they hold instead of the pre-v2 `Cell` / `Slot` vocabulary.
  `CellMap` -> `SourceMap`, `SlotMap` -> `ComputedMap`, and the `ThreadSafe*` / `Async*`
  variants alongside them.

### Deprecated

- The old names are kept as deprecated aliases of the new ones, so existing callers still
  compile. Conformance runners accept both the old and new `model` spellings in a fixture;
  the corpus emits only the new. Fixture FILE names are unchanged.

## Unreleased

### Added — disposal, teardown scopes, and edge-degree introspection (`#lzspecedgeindex`)

- **`Slot.Dispose`, `Cell.Dispose`, `Memo.DisposeNode`, `Signal.DisposeNode`.**
  A Go handle is a pointer, and dropping the last pointer to a node reclaimed
  nothing: the reverse edge each dependency holds is a strong reference, so a
  long-lived source retained every node that ever read it and a
  subscribe/unsubscribe workload grew without bound in both memory and
  propagation cost. Disposal detaches both edge directions and is idempotent.
  `Signal.Dispose` keeps its older, narrower meaning — deactivate the eager
  puller, stay readable — so graph teardown for a `Signal` is `DisposeNode`.
- **`Context.Scope() *TeardownScope`**, with `Own`, `Len`, `Disarm`, and
  `Close`, plus `Context.WithScope` for the lexical case. Go has no
  destructors, so a scope ends at `defer scope.Close()` rather than at a drop.
  Close tears members down in reverse creation order, which is observable
  through effect cleanups.
- **`Context.DependentCount` / `Context.DependencyCount`**, over a sealed
  `GraphNode` interface. Counts only — no path to the edge sets and no way to
  mutate the graph through them.
- **`Slot.TryGet` / `Cell.TryGet` / `Signal.TryGet`** and `ErrDisposed` /
  `*DisposedError`. A read of a disposed node panics, which is how the error
  crosses a user compute closure that has no error channel; `TryGet` is the
  checked form at the boundary and repairs the tracking stack.
- **The whole surface again on `AsyncContext`**: `DisposeAsync` on slot and cell
  handles, `AsyncTeardownScope` with `OwnAsync`, `DependentCount` /
  `DependencyCount`, `AsyncCellHandle.TryGet`, and
  `AsyncEffectHandle.IsActive`.
- Disposal dirties the surviving dependent cone on **both** the sync and async
  paths — detaching edges without marking dependents leaves a live reader frozen
  on its pre-disposal cache. Effects reached by that walk are marked but never
  scheduled: teardown is not a publish, and running an effect mid-teardown would
  re-enter a compute that reads the node being disposed. The same rule defers
  `Memo`'s equality recompute and `Signal`'s eager re-pull to the next read.

### Fixed

- **AsyncContext: a late dependency registration could resurrect a disposed
  node's edge.** A compute or effect body runs on its own goroutine and could
  reach `trackDep` after its owner was torn down, rebuilding an edge the
  disposal had just removed and leaking it for the life of the context.
  `trackDep` now refuses to build an edge onto or out of a disposed node.

### Testing

- The reactive-graph conformance runner now replays **all nine** fixtures
  against both `Context` and `AsyncContext` — previously one of nine, with the
  other eight recorded as blocked on the missing public API. The runner gained
  the `dispose`, `fanout`, `dispose_fanout`, `churn`, `begin_scope`,
  `end_scope`, `disarm`, and `dispose_stale_handle` ops, the `dependents_of`,
  `dependencies_of`, `readable`, `error`, `observed_by`, `observed_count`,
  `cleanup_order`, and `scope_owned_count` assertions, and the `scenarios`
  fixture shape with its `observationally_equal` relation. A divergence ledger
  asserts in both directions, so a new divergence and a stale entry both fail.

### Removed — BREAKING: the `Cell` observer API

- **`Cell.Subscribe` and its disposer are gone**, along with the observer slot
  storage, the notification snapshot, and the compaction machinery. lazily will
  not carry a `Cell` observer API in any binding: observation in a reactive graph
  is a declared dependency edge, not a registered callback. A callback registry
  on `Cell` bypasses the graph, ignores batching, breaks glitch-freedom, and
  costs memory on *every* cell whether or not anyone subscribes. Four of the
  eight bindings (rs, cpp, js, kt) never had it.
- **Migration:** read the cell inside an `Effect` — the `Get` is what subscribes,
  and `Effect.Dispose` replaces the disposer. Where you need every individual
  transition delivered as a stream, that is a `TopicCell`, which is unaffected.
- **`TopicCell`/`QueueCell` `Subscribe` is unchanged** — the stream primitive is
  the supported answer here, not a casualty of this removal.

### Changed

- **`StateMachine.OnTransition` is now backed by an `Effect`** rather than a cell
  observer, mirroring lazily-rs `StateMachine::on_transition`. The signature is
  unchanged (handler of `(old, new)`, not called on registration, returns a
  disposer). **Behavior change:** an effect reruns once per settled cascade, so a
  batch that walks `A -> B -> C` now reports the single transition `(A, C)`
  rather than `(A, B)` and `(B, C)`. This is intended — a batch asserts
  atomicity, and the intermediate `B` was never an observable state of the graph.

## 0.19.0 - 2026-07-17

### Changed — performance investigation (`#lzgoslotmap`)

- **Dense-node-arena investigation landed as measurement + deferred design.**
  The `scale` benchmarks (BENCHMARKS.md → "Note on viewport scaling")
  document a residual ~2.6× viewport-read growth from 2M→10M cells vs
  lazily-rs's flatter slotmap-packed curve. The strategic fix is a dense
  node arena (flat `[]reactiveNode` indexed by `nodeID`) — but a full
  arena refactor of lazily-go's public reactive types (`*Cell[T]`,
  `*Slot[T]`, `*Signal[T]`, `*Effect`, `*Memo[T]` each embed
  `reactiveBase` directly and are returned by reference from the public
  API) touches every collection, async, and distributed layer and is not
  a single-turn refactor.
- **Smaller-scope alternatives were prototyped, measured, and rejected**
  (full table with before/after numbers in BENCHMARKS.md →
  "`#lzgoslotmap` investigation"). In summary: lazy edge-map allocation
  is a net wash (it shifts the same hmap allocations from build into the
  cold-read path); the inline-stack-snapshot optimization adds stack-frame
  overhead for no gain (Go's escape analysis already stack-allocates the
  small-fan-out case); leaving `Cell.dependencies` nil gives a real
  build-time alloc win but produces a repeatable ~16% regression on
  `ScaleViewportRecalc` from an unexplained heap-layout effect.
- **New default-`make-bench`-runnable pointer-chase benchmarks.**
  `BenchmarkSlotmapChaseViewport` (~2,090 ns/op, 0 allocs) and
  `BenchmarkSlotmapChaseEdit` (~6.35 ns/op, 0 allocs) in `bench_test.go`
  capture the slotmap-relevant cost on a 200K-node spreadsheet-shaped
  graph. The existing `scale_bench_test.go` benchmarks are gated behind
  the `scalebench` build tag (the heavy multi-million-node build would
  dominate a default `make bench`); the new benchmarks make the cost
  visible without opting into the heavy build, and provide a stable
  baseline future arena work can regress-test against.

## 0.18.0 - 2026-07-17

### Changed — performance (Phase 2 quick wins)

- **Reflect-free equality guards (`#lzgono reflect`).** Replaced the three
  remaining `reflect.DeepEqual` / `TypeOf` sites on the IPC / async-cell /
  presence hot paths with typed comparators:
  - `ipcValueEqual` (`ipc.go:1499`) is now a closed type switch over the two
    `IpcValue` variants: `bytes.Equal` for `IpcValueInline` (avoiding reflect's
    per-byte comparison overhead) and struct `==` for `IpcValueSharedBlob`'s
    comparable `ShmBlobRef` descriptor. Drops the `reflect` import from `ipc.go`.
  - `asyncValueEqual` (`async_context.go:790`) adds fast type-switch paths for
    the common comparable cell payloads (`string`, `int`, `int32`, `int64`,
    `uint64`, `float64`, `bool`, `[]byte`) before falling back to the existing
    reflect-based comparable/non-comparable dispatch. `AsyncCell.Set` on a
    scalar cell no longer pays the ~50–100 ns `reflect.TypeOf` round-trip per
    write.
  - `presentReader.refresh` (`presence.go:190`) replaces `reflect.DeepEqual` on
    the live-view `map[K]V` projection with a new generic
    `comparableMapEqual[K, V comparable]` helper (direct map lookup + `==`).
    Drops the `reflect` import from `presence.go`.
  - New `Phase2*` benchmarks in `bench_test.go` cover all three comparators and
    the higher-level `AsyncCell.Set` and `PresenceCell.Heartbeat` steady-state
    paths.
- **Slice preallocation tidy-ups.** Two append-from-filtered-map sites now
  preallocate the upper bound: `WorkQueueCell.ReapExpired`
  (`work_queue.go:195`) uses `make([]uint64, 0, len(q.inFlight))`, and the
  signaling-room welcome roster (`signaling.go:842`) uses
  `make([]PeerId, 0, len(o.peerToConn))`. Eliminates the reallocations that
  previously occurred as the filtered output grew.
- **`#lzgosecondary-index` audit.** Surveyed the remaining O(N) scans for
  membership-test / index-style wins comparable to Phase 1's
  `childrenByParent` (`lossless_tree_crdt.go:736`). No clear algorithmic-class
  wins were found: the surviving linear scans are either inherent to the
  operation (`ReactiveMap.removeFromOrder` already needs an O(N) slice shift —
  a `map[K]int` index would trade comparisons for hash updates with no
  complexity-class improvement), over tiny slices (`CronCore.offsets`), part
  of a full reduction (`PhiAccrual.mean`/`std`), in serialization/snapshot
  paths rather than apply/merge hot paths, or already covered by Phase 1
  indexing (`liveChildren`, `orderedIds` caches, the per-call `byOrigin`
  bucketing in `TextCrdt.orderedIds`). Audit-only — no changes.

## 0.17.0 - 2026-07-17

### Changed — performance (CRDT plane, Phase 1 of `tasks/agent-doc/plans/lazily-perf-memory-audit.md`)

- **`TextCrdt.orderedIds` cache (`#lztextordcache`).** The DFS pre-order is now
  memoized and invalidated on every mutation that changes the element set
  (`Insert`/`InsertStr`/`Merge`/`ApplyDelta`/`GcWith`). Repeated `Text()` /
  `Len()` between mutations drops from O(N log N) per call to O(N) total
  (one rebuild + N filter passes) instead of N × O(N log N). Tombstone flips
  (`Delete`) only invalidate the live cache (the full DFS includes tombstones).
- **`TextCrdt.InsertStr` origin chaining (`#lztextinsertchain`).** `InsertStr`
  rewrites from N per-char `Insert` calls to one `orderedIds()` pass + N chain
  appends. Drops O(N² log N) → O(N log N); concurrent inserts still sort by
  peer tiebreak.
- **`LosslessTreeCrdt` parent→children index (`#lzlivelchildidx`).** A new
  `childrenByParent` map replaces the O(N) full-scan in `liveChildren`.
  `Render()` drops from O(N²) to O(N). Maintained at every `CreateNode` /
  `SplitLeaf` site (idempotent under apply replay); tombstones stay in the
  bucket (logical delete); `Fork` rebuilds from the copied node map.
- The Go `TextCrdt` already keyed `elems` by `OpId` directly (Go struct values
  are comparable), so `#lzopidkeytuple` was a no-op here.

## 0.16.0 - 2026-07-16

### Changed

- **Reactive-core allocation reduction.** Ported the lazily-rs edge-lifetime
  patterns into the Go core so per-cascade work no longer rebuilds edge maps:
  dependency/dependent maps are `clear()`-ed in place (reusing backing buckets)
  instead of re-allocated, `track()` skips re-writing an existing edge, and
  `Cell` observers drop the `*func` indirection with an early return when there
  are none. Measured on the hot paths (`make bench`): `SlotRecompute` 4→0
  allocs/op (512→0 B, ~1.9× faster), `MemoEqualityGuard` 4→0 allocs/op (512→0 B,
  ~1.9× faster), `BatchCoalesce` 31→1 allocs/op (4,144→160 B, ~2× faster). The
  zero-alloc steady state (`CellReadWrite`, `CellMapInsertRead`) is unchanged.
- **O(1) effect-queue drain.** `pendingEffects` is now a head-pointer ring that
  pops in O(1) and compacts when drained, instead of front re-slicing (which
  leaked the backing array) plus a linear splice on dispose. Fixes the
  unbounded growth of the effect queue under long-lived contexts.
- **`ThreadSafeReactiveMap` read/write split.** The keyed-map lock is now an
  `sync.RWMutex`: `Observe` / `IsPresent` / `PresentCount` / `PresentKeys` take
  a read lock; only materialization (`GetOrInsertWith` / `Set`) takes a write
  lock. Concurrency semantics are unchanged and stay clean under `-race`.
- **Fast goroutine-id for `ThreadSafeContext`.** The reentrant lock no longer
  calls `runtime.Stack` (walk + format) on every acquisition. Added
  `internal/goid`: on amd64 it reads the runtime `g` pointer from TLS and the
  `goid` field directly (offset probed once at first use, with a two-goroutine
  consensus that removes false positives); non-amd64 and any probe failure fall
  back to the portable `runtime.Stack` parse. All unsafe reads live in assembly
  so `go vet` and `-race`/checkptr stay clean. Verified against `runtime.Stack`
  across goroutines, under GC, and under `-race`.

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
