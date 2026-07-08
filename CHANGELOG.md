# Changelog

All notable changes to lazily-go are documented here. This project adheres to
[Semantic Versioning](https://semver.org/) and tracks the shared
[`lazily-spec`](https://github.com/lazily-hub/lazily-spec) protocol version.

## Unreleased

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
