# lazily (Go)

Lazy reactive primitives for Go — **Slots, Cells, and Signals** with automatic
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

## The reactive family

- **Slot** — a lazily-computed cached value that automatically tracks its
  dependencies and recomputes only when read after an upstream change.
- **Cell** — a mutable source value that invalidates dependent Slots/Signals
  when it changes.
- **Signal** — an *eager* derived value that recomputes the instant a dependency
  changes, with no intermediate unset value.

Values are **lazy by default**. When you need eager push-style semantics, reach
for `Signal`. Use `Effect` for side effects and `Memo` for an equality-guarded
derived value.

## Usage

```go
import lazily "github.com/lazily-hub/lazily-go"

ctx := lazily.NewContext()
a := lazily.NewCell(ctx, 2)
b := lazily.NewCell(ctx, 3)

// Lazy: computes on first read, caches, recomputes only when a or b changes.
sum := lazily.NewSlot(ctx, func(c *lazily.Context) int { return a.Get() + b.Get() })
sum.Get() // 5

a.Set(10)
sum.Get() // 13

// Eager: recomputes immediately when a dependency changes.
parity := lazily.NewSignal(ctx, func(c *lazily.Context) string {
	if a.Get()%2 == 0 {
		return "even"
	}
	return "odd"
})
parity.Get() // "even"
a.Set(11)
parity.Get() // "odd" (already updated before the read)
```

A `Cell` also supports persistent observers (the hook for UI bridges):

```go
count := lazily.NewCell(ctx, 0)
dispose := count.Subscribe(func(v int) { fmt.Println("now", v) })
count.Set(1) // prints "now 1"
dispose()
```

Batch coalesces cascades so dependent `Effect`s flush once:

```go
ctx.Batch(func() {
	a.Set(1)
	b.Set(2)
}) // a single coalesced cascade
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
is to wire the reactive members as `Cell` / `Slot` / `Memo` / `Signal` fields in
the constructor and expose thin accessor methods. The accessor reads like a
plain method but is lazy, cached, and dependency-tracked:

```go
type Greeter struct {
	Name     *lazily.Cell[string]
	greeting *lazily.Slot[string] // the "decorated" lazy member
}

func NewGreeter(ctx *lazily.Context) *Greeter {
	g := &Greeter{Name: lazily.NewCell(ctx, "")}
	// greeting tracks Name automatically; it recomputes only after Name changes.
	g.greeting = lazily.NewSlot(ctx, func(*lazily.Context) string {
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

Use `NewMemo` for the equality-guarded variant (suppress the downstream cascade
when the recomputed value is unchanged) or `NewSignal` for eager recomputation.
A runnable version of this pattern lives in
[`example_test.go`](example_test.go).

## State machine

`StateMachine` is a finite state machine backed by a `Cell`, so any slot or
signal reading its state is invalidated on transition.

## State chart

`StateChart` is a full Harel/SCXML **hierarchical** state machine — the native
counterpart of [`lazily-formal`][formal]'s `LazilyFormal.StateChart`. It is
**compute, not protocol** (never serialized as a distinct wire kind). Built from
the declarative JSON form via `ChartDefFromJSON`. Implements compound states,
orthogonal (parallel) regions, shallow and deep history, entry/exit/transition
actions, and named fail-closed guards.

## Collections & CRDTs

Keyed cell collections (`CellMap`, `CellTree`) with LIS move-minimized
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
what each case measures. The reactive steady state (`Cell` read/write, `CellMap`
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
[`lazily-spec` § Cross-Language Coverage](../lazily-spec/docs/coverage.md).

<!-- coverage-table:start -->
| Feature | Rust | Python | Kotlin | JS | Dart | Zig | Go | C++ |
| --------- | :----: | :------: | :------: | :--: | :----: | :---: | :--: | :---: |
| Reactive graph — `Cell` / `Slot` / `Signal` / `Effect` / memo / batch | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Thread-safe context (lock-backed) | ✅ | ✅ | ✅ | — | — | ✅ | ✅ | ✅ |
| Async reactive context | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Flat state machine | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Harel state charts | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Keyed cell collections (`CellMap` / `CellTree`) + reconcile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Memoized semantic tree (`SemTree`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Stable-id alignment (manufactured identity) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Reactive queue (`QueueCell` SPSC/MPSC + `QueueStorage` adapter) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Free-text character CRDT (`TextCrdt`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `TextCrdt` delta sync (`version_vector` / `delta_since` / `apply_delta`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Move-aware sequence CRDT (`SeqCrdt`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Lossless tree CRDT core (`LosslessTreeCrdt`, M1) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Lossless tree — dotted-frontier anti-entropy | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Lossless tree — concurrent merge convergence | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Registers (LWW / MV) + `PnCounter` + `CellCrdt` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| IPC wire — `Snapshot` + `Delta` + `CrdtSync` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Shared-memory blob path (`ShmBlobArena`) | ✅ | ✅ | ✅ | ~ | ~ | ✅ | ✅ | ✅ |
| Distributed CRDT plane (`CrdtPlaneRuntime` / anti-entropy) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Distributed plane — WebRTC transport + signaling | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| State projection / mirror | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Causal receipts (`CausalReceipts` outcome projection) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Message-passing + RPC command plane (`command-plane-v1`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C-ABI FFI boundary | ✅ | ✅ | ✅ | — | ✅ | ✅ | ✅ | ✅ |
| Permission boundary (`PeerPermissions` / `RemoteOp`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Capability negotiation (`SessionHandshake`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Instrumentation / benchmarks | ✅ | ✅ | ✅ | — | ✅ | ✅ | ✅ | ✅ |
<!-- coverage-table:end -->

[spec]: https://github.com/lazily-hub/lazily-spec
[formal]: https://github.com/lazily-hub/lazily-formal
[rs]: https://github.com/lazily-hub/lazily-rs
[py]: https://github.com/lazily-hub/lazily-py
[kt]: https://github.com/lazily-hub/lazily-kt
[js]: https://github.com/lazily-hub/lazily-js
[dart]: https://github.com/lazily-hub/lazily-dart
[zig]: https://github.com/lazily-hub/lazily-zig
