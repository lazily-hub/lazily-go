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
| Reactive graph — two cell kinds (nodes `SourceCell` / `ComputedCell`; handles `Source<T, M>` / `Computed<T>`) + `Effect` sink + eager `Computed` (`computed().eager()`) / all cells guarded / batch | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Keyed-map materialization (`ComputedMap`) — mint-on-access derived slots: transparency + deferral (`#lzmatmode`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Thread-safe keyed map (`ThreadSafeComputedMap`) — `Send + Sync` + materialization confluence (`#lzmatmode`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Async keyed map (`AsyncComputedMap`) — eventual transparency (`#lzmatmode`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Keyed-map sync — membership propagation + materialize-on-ingest + derived-aggregate transparency (`#lzfamilysync`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Thread-safe context (lock-backed) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Async reactive context | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Flat state machine | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Harel state charts | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Keyed reactive maps (`ReactiveMap`: `SourceMap` / `ComputedMap`) + `SourceTree` + reconcile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `ReactiveMap` **Core surface** — single-threaded flavor (cell-model.md § Core surface vs. binding extensions) | ✅ | ✅ | ✅ | ✅ | ✅ | ~ | ✅ | ✅ | ✅ |
| `ReactiveMap` **Core surface** — thread-safe flavor (ordering + membership reactivity) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `ReactiveMap` **Core surface** — async flavor (ordering + membership reactivity) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Atomic ordered move replayed against **all three flavors** (`cellmap_atomic_move` + `cellmap_independence`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Memoized semantic tree (`SemTree`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Stable-id alignment (manufactured identity) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Reactive queue (`QueueCell` SPSC/MPSC + `QueueStorage` adapter) **Core surface** — single-threaded flavor | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Reactive queue (`QueueCell` SPSC/MPSC + `QueueStorage` adapter) **Core surface** — thread-safe flavor (reader kinds + closure lifecycle) | ✅ | ✅ | ✅ | — | ✅ | — | ✅ | ✅ | — |
| Reactive queue (`QueueCell` SPSC/MPSC + `QueueStorage` adapter) **Core surface** — async flavor (reader kinds + eventual transparency) | ✅ | ✅ | ✅ | — | ✅ | — | ✅ | ✅ | — |
| Broadcast topic (`TopicCell`) **Core surface** — single-threaded flavor — independent cursors + durable replay + safe GC (`#lztopiccell`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Broadcast topic (`TopicCell`) **Core surface** — thread-safe flavor (reader kinds + closure lifecycle) | ✅ | ✅ | ✅ | — | ✅ | — | ✅ | ✅ | — |
| Broadcast topic (`TopicCell`) **Core surface** — async flavor (reader kinds + eventual transparency) | ✅ | ✅ | ✅ | — | ✅ | — | ✅ | ✅ | — |
| Competing-consumer work queue (`WorkQueueCell`) **Core surface** — single-threaded flavor — exclusive leases + ack/nack + redelivery + DLQ (`#lzworkqueue`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Competing-consumer work queue (`WorkQueueCell`) **Core surface** — thread-safe flavor (reader kinds + closure lifecycle) | ✅ | ✅ | ✅ | — | ✅ | — | ✅ | ✅ | — |
| Competing-consumer work queue (`WorkQueueCell`) **Core surface** — async flavor (reader kinds + eventual transparency) | ✅ | ✅ | ✅ | — | ✅ | — | ✅ | ✅ | — |
| Merge algebra + `Source<T, M>` — associative `MergePolicy` (`KeepLatest`/`Sum`/`Max`/`SetUnion`/`RawFifo`), `Cell ≡ Source<KeepLatest>`, read-any-cell/write-`Source` split (`#relaycell`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| RelayCell — conflating relay + `BackpressurePolicy` + `SpillStore` + `Transport` + Inbox/Outbox + Rate/Window/Expiry/Priority/keyed policies (`#relaycell`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Free-text character CRDT (`TextCrdt`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `TextCrdt` delta sync (`version_vector` / `delta_since` / `apply_delta`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `CrdtTree` lossless document contract (`#lzcrdttree`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Move-aware sequence CRDT (`SeqCrdt`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Lossless tree CRDT core (`LosslessTreeCrdt`, M1) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Lossless tree — dotted-frontier anti-entropy | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Lossless tree — concurrent merge convergence | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Registers (LWW / MV) + `PnCounter` + `CellCrdt` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| IPC wire — `Snapshot` + `Delta` + `CrdtSync` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Shared-memory blob path (`ShmBlobArena`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Cross-process zero-copy transport (`BlobBackend` / shm / arrow) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Distributed CRDT plane (`CrdtPlaneRuntime` / anti-entropy) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Reliable sync — resync coordinator + at-least-once durable outbox + OR-set/LWW liveness (`#lzsync`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Storage-independent durable outbox (`OutboxStore` + shared outbox protocol; SQLite/Room/IndexedDB/file adapters) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Reliable-sync transport seam + full-duplex `SyncDriver` loop (`IpcSink`/`IpcSource`, `#sync-driver`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Distributed plane — WebRTC transport + signaling | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| State projection / mirror | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Causal receipts (`CausalReceipts` outcome projection) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Message-passing + RPC command plane (`command-plane-v1`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C-ABI FFI boundary | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Permission boundary (`PeerPermissions` / `RemoteOp`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Capability negotiation (`SessionHandshake`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Instrumentation / benchmarks | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Temporal sources — `TimerCell` / `IntervalCell` / `CronCell` / `DeadlineCell` over a logical clock (`#lztime`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Rate-shaping operators — `DebounceCell` / `ThrottleCell` / `SampleCell` / `ProbabilisticSampleCell` (`#lzrateshape`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Membership + failure detection — `MembershipCell` (SWIM + Phi-accrual) / `PeerSet` / `PeerChangeEvent` (`#lzmemb`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Distributed coordination — `LeaseCell` / `LeaderCell` / `LockCell` / `SemaphoreCell` / `BarrierCell`+`QuorumCell` (`#lzcoord`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Presence + ephemeral plane — `PresenceCell` / `AwarenessCell` / `EphemeralCell` + `Ephemeral`/`Durable` markers (`#lzpresence`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Stream windowing — `TumblingWindow` / `SlidingWindow` / `SessionWindow` over the merge algebra (`#lzwindow`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Fault tolerance — `CircuitBreakerCell` / `RetryPolicyCell` / `BulkheadCell` / `TimeoutCell` (`#lzresilience`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Portable stdlib `Timer` (`stdlib_timer_v1`) — canonical fixture + mutation-gate verified | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Portable stdlib caller-driven `Timeout<T>` (`stdlib_timeout_v1`) — distinct from reactive `TimeoutCell` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Portable stdlib `RevisionBarrier` (`stdlib_revision_barrier_v1`) — register/recheck lost-wakeup guard | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Embedded-service plane — `HealthCell` / `ReadinessCell` / `DiscoveryCell` / `ServiceRegistry` (`#lzservice`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Transport-agnostic reactive ingress (`IngressCell`) — keyed lifecycle scopes, generation/sequence/freshness envelopes, reorder buffer, accepted/dropped/error receipt readers (`#designimplementtransport`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Ingress family — `Send + Sync` flavor (`ThreadSafeIngressCell`): one frontier walk per admission (`#designimplementtransport`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Ingress family — async flavor (`AsyncIngressCell`): admission is not async-coloured (`#designimplementtransport`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
<!-- coverage-table:end -->

[spec]: https://github.com/lazily-hub/lazily-spec
[formal]: https://github.com/lazily-hub/lazily-formal
[rs]: https://github.com/lazily-hub/lazily-rs
[py]: https://github.com/lazily-hub/lazily-py
[kt]: https://github.com/lazily-hub/lazily-kt
[js]: https://github.com/lazily-hub/lazily-js
[dart]: https://github.com/lazily-hub/lazily-dart
[zig]: https://github.com/lazily-hub/lazily-zig
