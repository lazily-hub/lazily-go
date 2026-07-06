# Changelog

All notable changes to lazily-go are documented here. This project adheres to
[Semantic Versioning](https://semver.org/) and tracks the shared
[`lazily-spec`](https://github.com/lazily-hub/lazily-spec) protocol version.

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
