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
reconciliation, the memoized semantic tree (`SemTree`), stable-id alignment, and
the CRDT family: free-text character CRDT (`TextCrdt`, with delta sync),
move-aware sequence CRDT (`SeqCrdt`), registers (`MvRegister`, `PnCounter`,
`CellCrdt`), and the distributed CRDT plane (`CrdtPlane`, `CrdtPlaneRuntime`)
with anti-entropy and WebRTC transport + signaling.

## lazily-spec IPC

The IPC types (`Snapshot`, `Delta`, `CrdtSync`, `NodeState`, ...) implement the
language-agnostic [`lazily-spec`][spec] wire protocol so a Go graph's state can
be mirrored to remote observers across processes and languages. They round-trip
the canonical fixtures from [`lazily-spec`][spec]/`conformance/`. The C-ABI FFI
boundary (cgo) exposes the state plane to in-process native embedders.

## Conformance

lazily-go replays the shared [`lazily-spec`][spec] conformance fixtures (IPC,
keyed collections, and Harel state charts) — asserting identical behavior to
every other binding. Run `make check` (fmt + vet + build + test) locally; CI
also runs the race detector.

## Feature coverage

The full `lazily` capability set across every binding. Legend: ✅ shipped ·
`~` partial · `—` absent or not applicable. The canonical matrix with per-cell
notes and platform carve-outs lives in
[`lazily-spec` § Cross-Language Coverage](../lazily-spec/docs/coverage.md).

<!-- coverage-table:start -->
| Feature | Rust | Python | Kotlin | JS | Dart | Zig | Go |
| --------- | :----: | :------: | :------: | :--: | :----: | :---: | :--: |
| Reactive graph — `Cell` / `Slot` / `Signal` / `Effect` / memo / batch | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Thread-safe context (lock-backed) | ✅ | ✅ | ✅ | — | — | ✅ | ✅ |
| Async reactive context | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Flat state machine | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Harel state charts | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Keyed cell collections (`CellMap` / `CellTree`) + reconcile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Memoized semantic tree (`SemTree`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Stable-id alignment (manufactured identity) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Free-text character CRDT (`TextCrdt`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `TextCrdt` delta sync (`version_vector` / `delta_since` / `apply_delta`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Move-aware sequence CRDT (`SeqCrdt`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Registers (LWW / MV) + `PnCounter` + `CellCrdt` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| IPC wire — `Snapshot` + `Delta` + `CrdtSync` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Shared-memory blob path (`ShmBlobArena`) | ✅ | ✅ | ✅ | ~ | ~ | ✅ | ✅ |
| Distributed CRDT plane (`CrdtPlaneRuntime` / anti-entropy) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Distributed plane — WebRTC transport + signaling | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| State projection / mirror | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Causal receipts (`CausalReceipts` outcome projection) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C-ABI FFI boundary | ✅ | ✅ | ✅ | — | ✅ | ✅ | ✅ |
| Permission boundary (`PeerPermissions` / `RemoteOp`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Capability negotiation (`SessionHandshake`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Instrumentation / benchmarks | ✅ | ✅ | — | — | ✅ | ✅ | ✅ |
<!-- coverage-table:end -->

[spec]: https://github.com/lazily-hub/lazily-spec
[formal]: https://github.com/lazily-hub/lazily-formal
[rs]: https://github.com/lazily-hub/lazily-rs
[py]: https://github.com/lazily-hub/lazily-py
[kt]: https://github.com/lazily-hub/lazily-kt
[js]: https://github.com/lazily-hub/lazily-js
[dart]: https://github.com/lazily-hub/lazily-dart
[zig]: https://github.com/lazily-hub/lazily-zig
