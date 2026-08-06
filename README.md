# lazily (Go)

Lazy reactive primitives for Go — the **Cell kernel** (`Source` / `Computed` /
`Effect`, all cells guarded; eager via `Computed.Eager()`) with automatic
dependency tracking and cache invalidation, plus the full [`lazily-spec`][spec]
wire protocol, CRDT collection types, keyed cell collections, Harel state
charts, and the distributed CRDT plane.

A Go port of the lazily reactive family ([`lazily-rs`][rs], [`lazily-py`][py],
[`lazily-kt`][kt], [`lazily-js`][js], [`lazily-dart`][dart], [`lazily-zig`][zig])
— conformant with [`lazily-spec`][spec] and [`lazily-formal`][formal]. The
concurrency surfaces (async reactive context, signaling room, CRDT anti-entropy
plane) are built on **goroutines and channels**: share state by communicating,
not by locking.

```bash
go get github.com/lazily-hub/lazily-go
```

## The reactive family — the Cell kernel

Two value kinds, named without collision (design `#lzcellkernel`). **`Cell` is a
conceptual word for a value-bearing reactive node, not a Go type** — the two kinds
are two concrete handle structs, and write protection lives in the type:

- **`Source[T]`** — a value written from outside the graph; the only kind with
  `Set`/`Merge`. It folds writes under a `MergePolicy` (KeepLatest by default = a
  plain cell; Sum/Max = the former `MergeCell`). `Cell ≡ Source<KeepLatest>`.
  Constructors: `NewSource` / `NewSourceWithPolicy`.
- **`Computed[T]`** — a value computed from upstream; lazily cached and
  dependency-tracking, with **neither `Set` nor `Merge`**. `NewComputed(f)` is
  **guarded** (an `==`-guard suppresses equal recomputes) — the sole derived
  constructor now that the former `Memo` is removed: a `Computed` *is* the guarded
  form. `computed.Eager()` makes it *eager* (which replaces the former `Signal`),
  returning the same handle; `computed.Lazy()` reverses it.
- **`Effect`** — a side-effect sink (`ctx.effect`), outside the Cell hierarchy.

The `T comparable` bound is what the guard needs. For a value type that is **not
comparable**, drop to **`NewSlot(f)`** — the bound-free storage-sense primitive
(`T any`, no guard), the escape hatch mirroring `lazily-rs`'s `slot()`.

The v1 `Cell[T]` read-genus interface is **dropped**: no Go generic code used it
as a bound, and v2 no longer needs a genus for write protection.

Write protection lives in the types: because `Computed` has no `Set`,
`computed.Set(…)` does not compile. Values are **lazy by default**; call `Eager`
on a `Computed` for eager push-style semantics. Use `Effect` for side effects (a
sink, outside the Cell hierarchy).

## Usage

```go
import lazily "github.com/lazily-hub/lazily-go"

ctx := lazily.NewContext()
a := lazily.NewSource(ctx, 2)
b := lazily.NewSource(ctx, 3)

// Lazy: computes on first read, caches, recomputes only when a or b changes.
sum := lazily.NewComputed(ctx, func(c *lazily.Context) int { return a.Get() + b.Get() })
sum.Get() // 5

a.Set(10)
sum.Get() // 13

// Eager: Eager() attaches a puller so the computed re-materializes on every change.
parity := lazily.NewComputed(ctx, func(c *lazily.Context) string {
	if a.Get()%2 == 0 {
		return "even"
	}
	return "odd"
}).Eager()
parity.Get() // "even"
a.Set(11)
parity.Get() // "odd" (already updated before the read)
```

To react to a `Cell` from outside the graph (the hook for UI bridges), declare a
dependency edge with an `Effect` — a `Cell` has no callback registry:

```go
count := lazily.NewSource(ctx, 0)
effect := lazily.NewEffect(ctx, func(*lazily.Context) func() {
	fmt.Println("now", count.Get()) // the Get is what subscribes
	return nil
})
count.Set(1) // prints "now 1"
effect.Dispose()
```

An `Effect` runs once at creation and then once per settled cascade, so under
`Batch` it observes the settled value rather than each intermediate write. When
you need every individual transition delivered as a stream, use `TopicCell`.

Batch coalesces cascades so dependent `Effect`s flush once:

```go
ctx.Batch(func() {
	a.Set(1)
	b.Set(2)
}) // a single coalesced cascade
```

## Competing-consumer work queue

`WorkQueueCell[T]` provides exclusive FIFO claims, visibility deadlines,
worker-scoped acknowledgements, tail retries, and bounded dead-letter handling.
Item ids remain stable across retries; every claim gets a fresh delivery id.
The same contract is available as `ThreadSafeWorkQueueCell` for competing
goroutines and `AsyncWorkQueueCell` for composition on an `AsyncContext`; queue
and topic equivalents follow the same `ThreadSafe*` / `Async*` naming.

```go
work := lazily.NewWorkQueueCell[string](ctx, 10, 3)
work.Push("job")
delivery, _ := work.Claim("worker-a", 100)
if !work.Ack("worker-a", delivery.DeliveryID) {
    panic("ack rejected")
}
```

## Context

All reactives that react to each other must share a `Context`. It holds an
identity-keyed cache and the computation stack used for automatic dependency
tracking. `Context` is single-goroutine; for concurrent access use the
lock-backed `ThreadSafeContext` or drive the graph from one owner goroutine via
the channel-serialized `AsyncContext`.

## Reactive members on a struct

Go has no decorators, so there is no direct analog of lazily-py's `@slot` /
`@cell` on a method. The idiomatic Go equivalent of a lazily-*decorated method*
is to wire the reactive members as `Source` / `Computed` fields in the
constructor and expose thin accessor methods. The accessor reads like a plain
method but is lazy, cached, and dependency-tracked:

```go
type Greeter struct {
	Name     *lazily.Source[string]
	greeting *lazily.Computed[string] // the "decorated" lazy member
}

func NewGreeter(ctx *lazily.Context) *Greeter {
	g := &Greeter{Name: lazily.NewSource(ctx, "")}
	// greeting tracks Name automatically; it recomputes only after Name changes.
	g.greeting = lazily.NewComputed(ctx, func(*lazily.Context) string {
		return "Hello, " + g.Name.Get() + "!"
	})
	return g
}

// Greeting reads like a normal method but is lazy + cached + reactive.
func (g *Greeter) Greeting() string { return g.greeting.Get() }
```

```go
ctx := lazily.NewContext()
g := NewGreeter(ctx)
g.Name.Set("World")
g.Greeting() // "Hello, World!" (computed on first read, then cached)
g.Name.Set("Go")
g.Greeting() // "Hello, Go!"  (recomputed once, because Name changed)
```

`NewComputed` is guarded by default (it suppresses the downstream cascade when the
recomputed value is unchanged); chain `.Eager()` for eager recomputation. A
runnable version of this pattern lives in
[`example_test.go`](example_test.go).

## State machine

`StateMachine` is a finite state machine backed by a `Source`, so any
`Computed` reading its state is invalidated on transition.

## State chart

`StateChart` is a full Harel/SCXML **hierarchical** state machine — the native
counterpart of [`lazily-formal`][formal]'s `LazilyFormal.StateChart`. It is
**compute, not protocol** (never serialized as a distinct wire kind). Built from
the declarative JSON form via `ChartDefFromJSON`. Implements compound states,
orthogonal (parallel) regions, shallow and deep history, entry/exit/transition
actions, and named fail-closed guards.

## Collections & CRDTs

Keyed cell collections (`SourceMap`, `SourceTree`) with LIS move-minimized
reconciliation, the memoized semantic tree (`SemTree`), stable-id alignment, the
**reactive queue** (`QueueCell` — a FIFO collection whose shell invalidates by
reader kind: a push invalidates `Len`/`IsEmpty` (and `Head` when transitioning
from empty), a pop invalidates `Head`/`Len`/`IsEmpty`, and a bounded queue's
`IsFull` is the reactive backpressure signal; SPSC primitive with MPSC via
`Batch`, over a pluggable `QueueStorage` backend with the default
`VecDequeStorage`), and the CRDT family: free-text character CRDT (`TextCrdt`,
with delta sync), move-aware sequence CRDT (`SeqCrdt`), the **lossless tree
CRDT** (`LosslessTreeCrdt` — a single rooted concrete-syntax tree whose leaves
own every rendered byte, with op-based delta sync over a dotted non-contiguous
version frontier), registers (`MvRegister`, `PnCounter`, `CellCrdt`), and the
distributed CRDT plane (`CrdtPlane`, `CrdtPlaneRuntime`) with anti-entropy and
WebRTC transport + signaling.

## Keyed reactive maps

`ReactiveMap[K, V, H]` is the unified keyed reactive collection (`#reactivemap`):
keys map to independently-tracked per-entry reactive nodes over a handle kind `H`
(`*Source[V]` input cells or `*Computed[V]` derived computeds), with **reactive membership
and order**. Go generics can't add methods to a type alias, so its two
specializations are thin distinct structs embedding `*ReactiveMap` with the
handle kind fixed:

- **`SourceMap[K, V]`** — input-cell entries. Adds the source-only `Set` and eager
  value-minting (`Entry` / `EntryWith`). Every entry is a writable `*Source[V]`.
- **`ComputedMap[K, V]`** — derived-slot entries. `GetOrInsertWith` mints a slot
  on first access (**lazy materialization**); `MaterializeAll` pre-mints the
  keyset (**eager**). A slot's value is derived, so `ComputedMap` has **no
  `Set`**. There is **no eager/lazy mode flag** — eager is a pre-mint loop, lazy
  is mint-on-access.

These were named `CellMap` / `SlotMap` before the v2 kernel renamed the node
kinds to `Source` and `Computed`. The old spellings remain as **deprecated**
generic type aliases (`CellMap` = `SourceMap`, `SlotMap` = `ComputedMap`, plus
the `Async*` / `ThreadSafe*` flavors, and `CellTree` = `SourceTree` for the
ordered keyed tree) with deprecated constructor wrappers, so existing callers
keep compiling. Generic type aliases require **Go 1.24+**.

The shared surface — `GetOrInsertWith` / `Remove` / `Move*` / `Keys` / `Len` /
`ContainsKey` / membership + order signals — lives on the generic `ReactiveMap`.

```go
ctx := lazily.NewContext()
// Lazy derived-slot map over a large keyed space; only read keys are allocated.
sheet := lazily.NewComputedMap[Key, int](ctx)
sheet.GetOrInsertWith(k, func(k Key) int { return recompute(k) }) // mint on first pull
sheet.PresentCount()                                              // grows only with reads
```

Eager (`MaterializeAll` pre-mint) and lazy (`GetOrInsertWith` mint-on-access)
return **identical values** for every key (observational transparency); the
strategy changes allocation timing and memory, never results. The laws —
`observe_canonical`, `eager_lazy_observationally_equivalent`,
`materialize_present_monotone` / `lazy_present_subset_eager`, and entry-kind
orthogonality (`cell_entries_materialized_in_every_mode` /
`slot_entries_deferred_under_lazy`) — are proven in [`lazily-formal`][formal]'s
`Materialization` module and pinned by the `conformance/materialization/*.json`
fixtures. The `Send + Sync` (`ThreadSafeSourceMap` / `ThreadSafeComputedMap`) and async
(`AsyncSourceMap` / `AsyncComputedMap`) flavors mirror the same surface.

## lazily-spec IPC

The IPC types (`Snapshot`, `Delta`, `CrdtSync`, `NodeState`, ...) implement the
language-agnostic [`lazily-spec`][spec] wire protocol so a Go graph's state can
be mirrored to remote observers across processes and languages. They round-trip
the canonical fixtures from [`lazily-spec`][spec]/`conformance/`. The C-ABI FFI
boundary (cgo) exposes the state plane to in-process native embedders.

The additive **command / RPC message plane** (`command-plane-v1`) —
`CommandSubmit` / `CommandCancel` / `CommandEvents` / `CommandProjection` plus
the `CommandRpcClient` facade — rides the same wire envelope. Terminal command
authority folds through a `CausalReceipt`, so a unary `call` resolves only on a
terminal receipt (never on a transport ACK or `accepted`/queued event).

## Cross-process zero-copy transport

Large cell/slot payloads cross the IPC plane as **descriptors**, not copies
(`#lzzcpy`). The producer **spills** an oversized payload to a pluggable
[`BlobBackend`](transport.go) and ships a small `ShmBlobRef` descriptor; the
receiver **resolves** the descriptor against the same backend and reads the
bytes in place — no copy, no checksum recompute. Three backends ship:

- **`InProcessBackend`** wraps a `ShmBlobArena` — the single-address-space case
  (the cgo FFI host / an in-process embedder).
- **`ArrowBackend`** holds Apache Arrow IPC stream bytes — the descriptor's bytes
  *are* an Arrow IPC stream a columnar consumer imports zero-copy.
- **`ShmBackend`** (Linux) is a genuine POSIX `shm_open` + `mmap` region with an
  atomic bump allocator — the cross-process backend: a descriptor minted by one
  mapping resolves zero-copy against an independent mapping of the same region.

`SpillMessage` replaces oversized `Inline`/`Payload` sites across a
`Snapshot`/`Delta`/`CrdtSync` with descriptors above a deployment threshold; a
receiver-side `BlobRouter` resolves any descriptor by its `backend`
discriminator (a `shm` descriptor never resolves in an Arrow backend, and vice
versa). The `backend` field is optional and defaults to `shm`, so legacy
descriptors validate unchanged — the transport is a strict superset of the
shared-memory blob path. The backend-agnostic invariants (spill-then-resolve
identity, backend isolation, ABA generation safety, checksum integrity) are
proven in [`lazily-formal`][spec]'s `ZeroCopyTransport.lean` and pinned by the
`delta_zero_copy_arrow` conformance fixture.

## Conformance

lazily-go replays the shared [`lazily-spec`][spec] conformance fixtures (IPC,
keyed collections, Harel state charts, the lossless-tree CRDT, and the
command-plane message family) — asserting identical behavior to every other
binding. Run `make check` (fmt + vet + build + test) locally; CI also runs the
race detector.

## Benchmarks

See [BENCHMARKS.md](BENCHMARKS.md) for micro-benchmark results on the hot paths
— reactive core read/write, slot/memo recompute, batch coalescing, keyed
collections, and CRDT construction — with `ns/op` / `B/op` / `allocs/op` and
what each case measures. The reactive steady state (`Cell` read/write, `SourceMap`
insert/read) is zero-allocation. Benchmarks are defined as Go `testing.B` cases
in [`bench_test.go`](bench_test.go) (mirroring the in-library `RunBenchmarkSuite`)
and reproducible with `make bench` (`go test -bench=. -benchmem ./...`).

BENCHMARKS.md also includes a **spreadsheet-scale** benchmark (`make bench-scale`)
on a graph of `N` input cells + `N` formula slots (`=A_i + A_{i-1}`): ~2M nodes
at the default `N=1M`, up to a full **10M-cell Google Sheets workbook** at
`LAZILY_SCALE_N=5000000`. It builds the full workbook in under a second and — via
the lazy pull-based model — a one-cell edit + bounded-viewport read recomputes
only the viewport (~2 formulas), staying orders of magnitude cheaper than a full
recalc regardless of sheet size.

## Feature coverage

The full `lazily` capability set across every binding. Legend: ✅ shipped ·
`~` partial · `—` absent or not applicable. The canonical matrix with per-cell
notes and platform carve-outs lives in
[`lazily-spec` § Cross-Language Coverage](https://github.com/lazily-hub/lazily-spec/blob/main/docs/coverage.md).

<!-- coverage-table:start -->
| Feature | Rust | Python | Kotlin | JS | Dart | Zig | Go | C++ | C# |
| --------- | :----: | :------: | :------: | :--: | :----: | :---: | :--: | :---: | :--: |
| Reactive graph [^reactive-graph] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Keyed-map materialization [^keyed-map-materialization] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Thread-safe keyed map [^thread-safe-keyed-map] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Async keyed map [^async-keyed-map] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Keyed-map sync [^keyed-map-sync] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Thread-safe context [^thread-safe-context] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Async reactive context [^async-reactive-context] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Flat state machine [^flat-state-machine] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Harel state charts [^harel-state-charts] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Keyed reactive maps [^keyed-reactive-maps] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| ReactiveMap core — single-threaded [^reactivemap-core-single-threaded] | ✅ | ✅ | ✅ | ✅ | ✅ | ~ | ✅ | ✅ | ✅ |
| ReactiveMap core — thread-safe [^reactivemap-core-thread-safe] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| ReactiveMap core — async [^reactivemap-core-async] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Exact-key dependency availability [^exact-key-dependency-availability] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Atomic ordered move [^atomic-ordered-move] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Memoized semantic tree [^memoized-semantic-tree] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Stable-id alignment [^stable-id-alignment] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Reactive queue core — single-threaded [^reactive-queue-core-single-threaded] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Reactive queue core — thread-safe [^reactive-queue-core-thread-safe] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Reactive queue core — async [^reactive-queue-core-async] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Broadcast topic core — single-threaded [^broadcast-topic-core-single-threaded] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Broadcast topic core — thread-safe [^broadcast-topic-core-thread-safe] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Broadcast topic core — async [^broadcast-topic-core-async] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Work queue core — single-threaded [^work-queue-core-single-threaded] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Work queue core — thread-safe [^work-queue-core-thread-safe] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Work queue core — async [^work-queue-core-async] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Merge algebra [^merge-algebra] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| RelayCell [^relaycell] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Free-text character CRDT [^free-text-character-crdt] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| TextCrdt delta sync [^textcrdt-delta-sync] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| CrdtTree lossless document [^crdttree-lossless-document] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Move-aware sequence CRDT [^move-aware-sequence-crdt] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Lossless tree CRDT core [^lossless-tree-crdt-core] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Lossless tree — anti-entropy [^lossless-tree-anti-entropy] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Lossless tree — merge convergence [^lossless-tree-merge-convergence] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Registers (LWW/MV) + PnCounter [^registers] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| IPC wire — Snapshot/Delta/CrdtSync [^ipc-wire] | ✅ | ✅ | ✅ | ✅ | ~ | ✅ | ✅ | ✅ | ✅ |
| Frame codec — json [^frame-codec-json] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Frame codec — msgpack [^frame-codec-msgpack] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Frame codec — postcard [^frame-codec-postcard] | ✅ | — | — | — | — | — | — | — | — |
| NodeId/PeerId exact-representation [^nodeid-peerid-exact-representation] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| NodeKey null-leniency [^nodekey-null-leniency] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Shared-memory blob path [^shared-memory-blob-path] | ✅ | ✅ | ✅ | ~ | ~ | ✅ | ✅ | ~ | ✅ |
| Cross-process zero-copy transport [^cross-process-zero-copy-transport] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Distributed CRDT plane [^distributed-crdt-plane] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Reliable sync [^reliable-sync] | ~ | ~ | ~ | ~ | ~ | ~ | ~ | ~ | ~ |
| Storage-independent durable outbox [^storage-independent-durable-outbox] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Reliable-sync transport seam [^reliable-sync-transport-seam] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Distributed plane — WebRTC [^distributed-plane-webrtc] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| State projection / mirror [^state-projection-mirror] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Causal receipts [^causal-receipts] | ~ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Message-passing + RPC command plane [^message-passing-rpc-command-plane] | ✅ | ✅ | ✅ | ✅ | ✅ | ~ | ✅ | ✅ | ✅ |
| C-ABI FFI boundary [^c-abi-ffi-boundary] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Permission boundary [^permission-boundary] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Capability negotiation [^capability-negotiation] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Instrumentation / benchmarks [^instrumentation-benchmarks] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Temporal sources [^temporal-sources] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Rate-shaping operators [^rate-shaping-operators] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Membership + failure detection [^membership-failure-detection] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Distributed coordination [^distributed-coordination] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Presence + ephemeral plane [^presence-ephemeral-plane] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Stream windowing [^stream-windowing] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Fault tolerance [^fault-tolerance] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Portable stdlib Timer [^portable-stdlib-timer] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Portable stdlib Timeout [^portable-stdlib-timeout] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Portable stdlib RevisionBarrier [^portable-stdlib-revision-barrier] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Embedded-service plane [^embedded-service-plane] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Reactive ingress [^reactive-ingress] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Ingress — thread-safe [^ingress-thread-safe] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Ingress — async [^ingress-async] | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

[^reactive-graph]: Reactive graph — two cell kinds (nodes `SourceCell` / `ComputedCell`; handles `Source<T, M>` / `Computed<T>`) + `Effect` sink + eager `Computed` (`computed().eager()`) / all cells guarded / batch
[^keyed-map-materialization]: Keyed-map materialization (`ComputedMap`) — mint-on-access derived slots: transparency + deferral (`#lzmatmode`)
[^thread-safe-keyed-map]: Thread-safe keyed map (`ThreadSafeComputedMap`) — `Send + Sync` + materialization confluence (`#lzmatmode`)
[^async-keyed-map]: Async keyed map (`AsyncComputedMap`) — eventual transparency (`#lzmatmode`)
[^keyed-map-sync]: Keyed-map sync — membership propagation + materialize-on-ingest + derived-aggregate transparency (`#lzfamilysync`)
[^thread-safe-context]: Thread-safe context (lock-backed)
[^async-reactive-context]: Async reactive context
[^flat-state-machine]: Flat state machine
[^harel-state-charts]: Harel state charts
[^keyed-reactive-maps]: Keyed reactive maps (`ReactiveMap`: `SourceMap` / `ComputedMap`) + `SourceTree` + reconcile
[^reactivemap-core-single-threaded]: `ReactiveMap` **Core surface** — single-threaded flavor (cell-model.md § Core surface vs. binding extensions)
[^reactivemap-core-thread-safe]: `ReactiveMap` **Core surface** — thread-safe flavor (ordering + membership reactivity)
[^reactivemap-core-async]: `ReactiveMap` **Core surface** — async flavor (ordering + membership reactivity)
[^exact-key-dependency-availability]: Exact-key dependency availability (`DependencyMap`: observe before publish, unrelated-key isolation, stable identity; `#lzdependencyavailability`)
[^atomic-ordered-move]: Atomic ordered move replayed against **all three flavors** (`cellmap_atomic_move` + `cellmap_independence`)
[^memoized-semantic-tree]: Memoized semantic tree (`SemTree`)
[^stable-id-alignment]: Stable-id alignment (manufactured identity)
[^reactive-queue-core-single-threaded]: Reactive queue (`QueueCell` SPSC/MPSC + `QueueStorage` adapter) **Core surface** — single-threaded flavor
[^reactive-queue-core-thread-safe]: Reactive queue (`QueueCell` SPSC/MPSC + `QueueStorage` adapter) **Core surface** — thread-safe flavor (reader kinds + closure lifecycle)
[^reactive-queue-core-async]: Reactive queue (`QueueCell` SPSC/MPSC + `QueueStorage` adapter) **Core surface** — async flavor (reader kinds + eventual transparency)
[^broadcast-topic-core-single-threaded]: Broadcast topic (`TopicCell`) **Core surface** — single-threaded flavor — independent cursors + durable replay + safe GC (`#lztopiccell`)
[^broadcast-topic-core-thread-safe]: Broadcast topic (`TopicCell`) **Core surface** — thread-safe flavor (reader kinds + closure lifecycle)
[^broadcast-topic-core-async]: Broadcast topic (`TopicCell`) **Core surface** — async flavor (reader kinds + eventual transparency)
[^work-queue-core-single-threaded]: Competing-consumer work queue (`WorkQueueCell`) **Core surface** — single-threaded flavor — exclusive leases + ack/nack + redelivery + DLQ (`#lzworkqueue`)
[^work-queue-core-thread-safe]: Competing-consumer work queue (`WorkQueueCell`) **Core surface** — thread-safe flavor (reader kinds + closure lifecycle)
[^work-queue-core-async]: Competing-consumer work queue (`WorkQueueCell`) **Core surface** — async flavor (reader kinds + eventual transparency)
[^merge-algebra]: Merge algebra + `Source<T, M>` — associative `MergePolicy` (`KeepLatest`/`Sum`/`Max`/`SetUnion`/`RawFifo`), `Cell ≡ Source<KeepLatest>`, read-any-cell/write-`Source` split (`#relaycell`)
[^relaycell]: RelayCell — conflating relay + `BackpressurePolicy` + `SpillStore` + `Transport` + Inbox/Outbox + Rate/Window/Expiry/Priority/keyed policies (`#relaycell`)
[^free-text-character-crdt]: Free-text character CRDT (`TextCrdt`)
[^textcrdt-delta-sync]: `TextCrdt` delta sync (`version_vector` / `delta_since` / `apply_delta`)
[^crdttree-lossless-document]: `CrdtTree` lossless document contract (`#lzcrdttree`)
[^move-aware-sequence-crdt]: Move-aware sequence CRDT (`SeqCrdt`)
[^lossless-tree-crdt-core]: Lossless tree CRDT core (`LosslessTreeCrdt`, M1)
[^lossless-tree-anti-entropy]: Lossless tree — dotted-frontier anti-entropy
[^lossless-tree-merge-convergence]: Lossless tree — concurrent merge convergence
[^registers]: Registers (LWW / MV) + `PnCounter` + `CellCrdt`
[^ipc-wire]: IPC wire — `Snapshot` + `Delta` + `CrdtSync`
[^frame-codec-json]: Frame codec — `json` **reference codec**: dependency-free interop floor, FFI baseline form, byte-canonical (**MUST**) — executable round-trip obligation (`conformance/codec/frame_roundtrip_json.json`, `#lzmsgpackparity`)
[^frame-codec-msgpack]: Frame codec — `msgpack` **cross-language binary default**: externally-tagged frame over named-field maps, semantic (not byte-identical) round-trip (**MUST**) — executable round-trip obligation (`conformance/codec/frame_roundtrip_msgpack.json`, `#lzmsgpackparity`). Shipping *a* MessagePack codec does not earn this mark: lazily-cpp read `~` here while its private internally-tagged framing wore the token, and only flipped once it shipped the spec wire (`#lzcppmsgpackwire`)
[^frame-codec-postcard]: Frame codec — `postcard` positional same-schema fast path: smallest + byte-canonical, not cross-language (**MAY**)
[^nodeid-peerid-exact-representation]: `NodeId` / `PeerId` exact-representation bound (**MUST**) — a decoder that cannot represent a received identifier exactly rejects the frame rather than rounding it (`conformance/codec/nodeid_exact_range.json`, `#lzspecdecoderbound`). A binding's exact range MAY be narrower than the `u64` wire type; ✅ means it refuses outside that range instead of substituting a neighbouring id, not that it carries the full `u64`. Exact ranges: full `u64` in Rust / Zig / C#, unbounded in Python, `[0, 2^63)` in Kotlin / Go / C++, `[0, 2^53)` in JS, and platform-split in Dart (63-bit on the VM, 53-bit on web). protocol.md stated only the PRODUCER half until this audit, and two C++ decoders were substituting rather than refusing.
[^nodekey-null-leniency]: `NodeKey` null-leniency on decode (**MUST**) — omit-when-absent binds the ENCODER; a decoder reads both an omitted `key` and an explicit `key: null` as absent, refusing neither and constructing a key from neither (`conformance/codec/nodekey_null_leniency.json`, `#lzkeynullstrict`). Replayed on BOTH optional-key sites (`NodeSnapshot`, the `NodeAdd` delta op) in both codecs, and the fixture pins the RE-ENCODED field set as well: reading null as absent and writing it back out is a correct decode with a non-conforming encoder. Before the audit lazily-py and lazily-zig refused the null form, and lazily-kt decoded it into a real key named `null` — all three had the same field right on `CrdtOp`, in the same file.
[^shared-memory-blob-path]: Shared-memory blob path (`ShmBlobArena`)
[^cross-process-zero-copy-transport]: Cross-process zero-copy transport (`BlobBackend` / shm / arrow)
[^distributed-crdt-plane]: Distributed CRDT plane (`CrdtPlaneRuntime` / anti-entropy)
[^reliable-sync]: Reliable sync — resync coordinator + at-least-once durable outbox + OR-set/LWW liveness (`#lzsync`)
[^storage-independent-durable-outbox]: Storage-independent durable outbox (`OutboxStore` + shared outbox protocol; SQLite/Room/IndexedDB/file adapters)
[^reliable-sync-transport-seam]: Reliable-sync transport seam + full-duplex `SyncDriver` loop (`IpcSink`/`IpcSource`, `#sync-driver`)
[^distributed-plane-webrtc]: Distributed plane — WebRTC transport + signaling
[^state-projection-mirror]: State projection / mirror
[^causal-receipts]: Causal receipts (`CausalReceipts` outcome projection)
[^message-passing-rpc-command-plane]: Message-passing + RPC command plane (`command-plane-v1`)
[^c-abi-ffi-boundary]: C-ABI FFI boundary
[^permission-boundary]: Permission boundary (`PeerPermissions` / `RemoteOp`)
[^capability-negotiation]: Capability negotiation (`SessionHandshake`)
[^instrumentation-benchmarks]: Instrumentation / benchmarks
[^temporal-sources]: Temporal sources — `TimerCell` / `IntervalCell` / `CronCell` / `DeadlineCell` over a logical clock (`#lztime`)
[^rate-shaping-operators]: Rate-shaping operators — `DebounceCell` / `ThrottleCell` / `SampleCell` / `ProbabilisticSampleCell` (`#lzrateshape`)
[^membership-failure-detection]: Membership + failure detection — `MembershipCell` (SWIM + Phi-accrual) / `PeerSet` / `PeerChangeEvent` (`#lzmemb`)
[^distributed-coordination]: Distributed coordination — `LeaseCell` / `LeaderCell` / `LockCell` / `SemaphoreCell` / `BarrierCell`+`QuorumCell` (`#lzcoord`)
[^presence-ephemeral-plane]: Presence + ephemeral plane — `PresenceCell` / `AwarenessCell` / `EphemeralCell` + `Ephemeral`/`Durable` markers (`#lzpresence`)
[^stream-windowing]: Stream windowing — `TumblingWindow` / `SlidingWindow` / `SessionWindow` over the merge algebra (`#lzwindow`)
[^fault-tolerance]: Fault tolerance — `CircuitBreakerCell` / `RetryPolicyCell` / `BulkheadCell` / `TimeoutCell` (`#lzresilience`)
[^portable-stdlib-timer]: Portable stdlib `Timer` (`stdlib_timer_v1`) — canonical fixture + mutation-gate verified
[^portable-stdlib-timeout]: Portable stdlib caller-driven `Timeout<T>` (`stdlib_timeout_v1`) — distinct from reactive `TimeoutCell`
[^portable-stdlib-revision-barrier]: Portable stdlib `RevisionBarrier` (`stdlib_revision_barrier_v1`) — register/recheck lost-wakeup guard
[^embedded-service-plane]: Embedded-service plane — `HealthCell` / `ReadinessCell` / `DiscoveryCell` / `ServiceRegistry` (`#lzservice`)
[^reactive-ingress]: Transport-agnostic reactive ingress (`IngressCell`) — keyed lifecycle scopes, generation/sequence/freshness envelopes, reorder buffer, accepted/dropped/error receipt readers (`#designimplementtransport`)
[^ingress-thread-safe]: Ingress family — `Send + Sync` flavor (`ThreadSafeIngressCell`): one frontier walk per admission (`#designimplementtransport`)
[^ingress-async]: Ingress family — async flavor (`AsyncIngressCell`): admission is not async-coloured (`#designimplementtransport`)
<!-- coverage-table:end -->

## The lazily family

lazily is one reactive model implemented across many languages — the same cell
kernel, the same keyed collections and CRDTs, and the same wire protocol — so
peers written in different languages talk to each other without a translation
layer.

- [`lazily-spec`][spec] — the language-agnostic wire protocol, the
  cross-language feature matrix, and the conformance corpus every binding
  replays, lazily-go included.
- [`lazily-formal`][formal] — the Lean 4 formal model every binding inherits its
  proofs from.

| Repo | Language |
|---|---|
| [`lazily-rs`][rs] | Rust — the reference implementation |
| [`lazily-py`][py] | Python |
| **`lazily-go`** | Go — you are here |
| [`lazily-kt`][kt] | Kotlin / JVM |
| [`lazily-js`][js] | JavaScript / TypeScript |
| [`lazily-cs`][cs] | C# / .NET |
| [`lazily-cpp`][cpp] | C++ |
| [`lazily-zig`][zig] | Zig |
| [`lazily-dart`][dart] | Dart / Flutter |
| [`lazily-react`][react] | React / Preact bindings layered over `lazily-js` (not a separate language binding) |

[spec]: https://github.com/lazily-hub/lazily-spec
[formal]: https://github.com/lazily-hub/lazily-formal
[rs]: https://github.com/lazily-hub/lazily-rs
[py]: https://github.com/lazily-hub/lazily-py
[kt]: https://github.com/lazily-hub/lazily-kt
[js]: https://github.com/lazily-hub/lazily-js
[dart]: https://github.com/lazily-hub/lazily-dart
[zig]: https://github.com/lazily-hub/lazily-zig
[cs]: https://github.com/lazily-hub/lazily-cs
[cpp]: https://github.com/lazily-hub/lazily-cpp
[react]: https://github.com/lazily-hub/lazily-react
