# lazily-go Benchmarks

Micro-benchmarks for the lazily-go hot paths, defined as Go `testing.B`
benchmarks in [`bench_test.go`](bench_test.go). They mirror the in-library
`RunBenchmarkSuite` scenarios (`instrumentation.go`) so results are reproducible
by anyone.

## Reproduce

```bash
make bench
# or, directly:
go test -run '^$' -bench=. -benchmem ./...
```

Each row reports `ns/op` (nanoseconds per iteration), `B/op` (bytes allocated
per iteration), and `allocs/op` (heap allocations per iteration). Lower is
better on all three.

## Results

Measured on the following machine — treat the absolute numbers as indicative;
the shapes (relative costs, alloc counts) are what matter across runs.

### Hardware / environment

| | |
|---|---|
| CPU | AMD Ryzen 9 9950X3D (16 cores / 32 threads) |
| RAM | 186 GiB |
| OS | Linux 7.1.1 (CachyOS), x86-64 |
| Go | go1.26.4 |

```
goos: linux
goarch: amd64
cpu: AMD Ryzen 9 9950X3D 16-Core Processor
```

| Benchmark | ns/op | B/op | allocs/op | What it measures |
|-----------|------:|-----:|----------:|------------------|
| `CellReadWrite` | 7.58 | 0 | 0 | `Cell.Set` (PartialEq guard) + `Cell.Get` round trip — the core mutation path, alloc-free. |
| `CellMapInsertRead` | 15.7 | 0 | 0 | `CellMap.Set` + `Read` on a keyed collection — alloc-free steady state. |
| `MemoEqualityGuard` | 96.6 | 0 | 0 | `Memo` recompute that yields an equal value, suppressing the downstream cascade (an `Effect` stays put). |
| `SlotRecompute` | 98.6 | 0 | 0 | Invalidate a `Cell`, then re-pull a dependent `Slot` (edge re-tracking + recompute). |
| `BatchCoalesce` | 981 | 160 | 1 | 10 cell writes inside one `Batch`, coalesced into a single invalidation pass, then one `Slot` recompute. |
| `SeqCrdtInsert` | 73,992 | 38,865 | 1,810 | Build a move-aware `SeqCrdt` of 100 elements (fractional-index positions + LWW registers). |
| `TextCrdtInsert` | 687,160 | 1,054,422 | 11,444 | Build a `TextCrdt` of 100 characters (Fugue/RGA ordering rebuilt per insert). |
| `Phase2IpcValueEqual` | 3.42 | 0 | 0 | Reflect-free `IpcValue` PartialEq guard (alternates `IpcValueInline` `bytes.Equal` and `IpcValueSharedBlob` struct ==). |
| `Phase2AsyncValueEqual` | 10.6 | 16* | 0 | Reflect-free `asyncValueEqual` over `string`/`int`/`[]byte` (the fast type-switch paths added in 0.18.0). |
| `Phase2AsyncCellStringSet` | 380 | 200 | 4 | `AsyncCell.Set` on a string cell where the equality guard short-circuits — exercises the new string fast path end-to-end. |
| `Phase2PresenceRefreshSteadyState` | 113 | 256 | 2 | `PresenceCell.Heartbeat` with unchanged value — the reflect-free `comparableMapEqual` projection guard. |
| `SlotmapChaseViewport` | 2,090 | 0 | 0 | Steady-state "edit one input, read a 1,000-cell viewport" on a 200K-node spreadsheet graph. Pure pointer-chase latency (no allocation); the cost a dense-arena refactor would address. |
| `SlotmapChaseEdit` | 6.35 | 0 | 0 | Just the invalidation cascade after a single input edit on the same 200K-node graph (no viewport read). Isolates the invalidate path. |

*The `16 B/op` for `Phase2AsyncValueEqual` shows up as non-zero bytes with zero
allocs because the `[]byte` test fixtures' backing arrays live in the bench
function's scope; `runtime.ReadMemStats` attributes some heap-growth accounting
to per-op cost even though no allocation happens per iteration (verified by a
standalone run). The actual comparator allocates nothing.

## Notes

- The reactive core steady-state (`CellReadWrite`, `CellMapInsertRead`) is
  **zero-allocation** — reads and equality-guarded writes don't touch the heap.
- `SlotRecompute` / `MemoEqualityGuard` are now **zero-allocation** per cascade
  as well: dependency-edge maps are `clear()`-reused in place instead of being
  re-allocated each recompute, and `track()` skips re-writing an existing edge.
  (Previously 512 B / 4 allocs per op.) The memo guard's win remains behavioral —
  it aborts the downstream cascade when the recomputed value is unchanged.
- `BatchCoalesce` drops to a single allocation (the coalesced invalidation
  snapshot); the cell-write coalescing map and the pending-effect queue are now
  reused across batches via in-place `clear()` and a head-pointer ring that
  compacts on drain. (Previously 4,144 B / 31 allocs per op.)
- The CRDT builders (`TextCrdtInsert`, `SeqCrdtInsert`) measure *whole-document
  construction* (100 ops), not per-op cost — divide by 100 for per-insert. They
  are the heaviest paths because visible order is recomputed as a pure function
  of the element set on each mutation, matching the spec's determinism
  requirement. Their allocation profile is unchanged by the reactive-core work.
- These are single-threaded micro-benchmarks. The concurrency surfaces
  (`AsyncContext`, `SignalingRoom`, `CrdtPlaneRuntime`) are correctness-tested
  under the race detector rather than benchmarked here; `ThreadSafeContext`
  avoids a `runtime.Stack` walk per lock via the fast goid lookup in
  `internal/goid` (see CHANGELOG).
- **0.18.0 — Phase 2 quick wins.** The three remaining reflect-based equality
  guards on the IPC / async-cell / presence hot paths are replaced with typed
  comparators: a closed type switch over `IpcValue` variants (`bytes.Equal` +
  struct `==`), a fast-path type switch over common scalar/`[]byte` payloads
  before the reflect fallback in `asyncValueEqual`, and a generic
  `comparableMapEqual` helper in place of `reflect.DeepEqual` on the
  `presentReader.refresh` map projection. The new `Phase2*` benchmarks above
  exercise each. `ipcValueEqual` is now ~3.4 ns/op with zero allocation; the
  string fast path in `asyncValueEqual` avoids the ~50–100 ns `reflect.TypeOf`
  round-trip per `AsyncCell.Set`. Slice preallocation was also tightened at
  two append-from-filtered-map sites (`work_queue.ReapExpired`,
  `signaling.welcome`).

## Scale (≥1M cells) — spreadsheet-shaped graph

Replicates the lazily-rs `scale` group ([`scale.rs`][rs-scale]) on a
spreadsheet-shaped graph: `N` input cells + `N` formula slots where
`formula[i] = input[i] + input[i-1]` (local fan-in, like a column of
`=A_i + A_{i-1}`). With the default `N = 1,000,000` that is **~2,000,000 reactive
nodes**. Defined in [`scale_bench_test.go`](scale_bench_test.go), gated behind
the `scalebench` build tag so a plain `make bench` skips the heavy build.

Reproduce (same hardware as above):

```bash
make bench-scale
# or a specific size:
LAZILY_SCALE_N=1000000 go test -tags scalebench -run '^$' -bench=Scale -benchmem ./...
LAZILY_SCALE_N=5000000 go test -tags scalebench -run '^$' -bench=Scale -benchmem ./...
```

> **A "cell count" here counts two cells per row** — the graph models a column of
> formulas `=A_i + A_{i-1}`, so each row is **one input cell `A_i` plus one
> formula cell**. `N` rows ⇒ `N` inputs + `N` formulas = `2N` cells.

### 1,000,000 rows (~2M cells)

| Benchmark | Time | Per cell | What it measures |
|-----------|-----:|---------:|------------------|
| `ScaleBuild` | 189 ms | ~95 ns | Construct all 2N nodes (formulas lazy, not yet computed). |
| `ScaleColdFullRecalc` | 212 ms | ~106 ns | First read of every formula — forces every compute + edge-tracking. |
| `ScaleViewportRecalc` | **2.70 µs** | — | Edit one input, read only a 1,000-cell viewport. ~78,000× cheaper than a full recalc. |
| `ScaleFullRecalcInvalidateAll` | 467 ms | ~234 ns | Touch every input, then read every formula (worst-case full-sheet edit). |

### 5,000,000 rows (10M cells — a full Google Sheets workbook)

Google Sheets caps a workbook at **10,000,000 cells**. Modeled as 5,000,000
input cells + 5,000,000 formula cells (`LAZILY_SCALE_N=5000000`):

| Benchmark | Time | Per cell | What it measures |
|-----------|-----:|---------:|------------------|
| `ScaleBuild` | 955 ms | ~95 ns | Build the full 10M-cell workbook. |
| `ScaleColdFullRecalc` | 1.04 s | ~208 ns | Compute all 5M formulas cold. |
| `ScaleViewportRecalc` | **7.12 µs** | — | Edit one input, read a 1,000-cell viewport. ~146,000× cheaper than a full recalc. |
| `ScaleFullRecalcInvalidateAll` | 2.20 s | ~220 ns | Re-edit every input, recompute the whole workbook. |

So lazily-go backs a **full-capacity Google Sheets workbook**: build under a
second, full cold recompute ~1 s, and a one-cell edit + bounded-viewport read
stays in the **single-digit-µs** range — because the lazy pull-based model leaves
off-viewport formulas dirty and never recomputes them (only ~2 formulas actually
recompute per edit, regardless of sheet size — the property a viewport-rendered
spreadsheet needs).

### Spreadsheet cell-count context

| Spreadsheet | Documented limit | Cells |
|-------------|------------------|------:|
| Google Sheets | 10,000,000 cells per workbook (18,278 columns max) | 10,000,000 |
| Microsoft Excel | 1,048,576 rows × 16,384 columns per worksheet | 17,179,869,184 |

The `LAZILY_SCALE_N=5000000` run above covers a full Google Sheets workbook. A
grid-complete Excel worksheet (17 billion cells) is unrepresentative — real
sheets populate a tiny fraction of the grid, and lazily stores only the cells
you create, so the `scale` group measures the populated-cell path that matters.

### A note on viewport scaling — on-node cache

Cached slot values live **on the node itself** (`Slot.value`/`cached` fields),
not in a shared `Context` hash map. So a viewport read of ~1,000 formulas is
~1,000 direct field reads on the nodes you already hold — it never probes a
whole-graph structure. This is the design that keeps the viewport curve nearly
flat: **2.70 µs at 2M cells → 7.12 µs at 10M cells** (vs the shared-map design
this replaced, which degraded 24.9 µs → 103 µs from cache/TLB pressure probing a
multi-GB map). The **number of recomputes stays viewport-bounded (~2, fully
independent of sheet size)**; the residual ~2.6× growth is pointer-chasing
latency — the viewport's nodes are scattered across a larger heap at 10M cells,
so more of the 1,000 reads miss cache. That is memory-hierarchy, not algorithmic:
still ~146,000× cheaper than a full recalc at 10M cells, and off-viewport
formulas are never recomputed.

lazily-rs holds the flattest curve of the family (~11.5 µs at both sizes) because
its slotmap packs values into a contiguous array indexed by a dense handle, so
even the pointer-chase is largely sequential; a contiguous arena keyed by a dense
node id would close the remaining lazily-go gap. Reported with real before/after
numbers rather than a claimed flat line.

## `#lzgoslotmap` investigation — dense node arena (0.19.0)

The BENCHMARKS.md note above identifies the win — a contiguous arena keyed by
a dense node id — but a full slotmap refactor of lazily-go is a deep change:
the public reactive types (`*Cell[T]`, `*Slot[T]`, `*Signal[T]`, `*Effect`,
`*Memo[T]`) each embed `reactiveBase` directly and are returned by reference
from the public API, so the per-node storage cannot be relocated into a flat
`[]reactiveNode` without either changing every public constructor's return
type or introducing a wide-reaching ID→`*T` indirection that every
`Cell.Get`/`Slot.Get`/`Signal.Get` would have to dereference. That is the
right long-term design, but it touches every collection, async, and
distributed layer (collections.go, async_context.go, reactive_map.go,
registers.go, thread_safe*.go, etc.) and is not a single-turn refactor.

For 0.19.0 the smaller-scope alternatives were prototyped, measured on the
`scalebench` suite (`LAZILY_SCALE_N=2000000`, 4M reactive nodes), and
rejected because none was a safe net win:

| Attempt | Build allocs | Build time | Cold-read allocs | Cold-read time | Viewport ns | Outcome |
|---|---:|---:|---:|---:|---:|---|
| baseline (0.18.0) | 14.0M | 345 ms | 4.0M | 410 ms | 1,960 | — |
| lazy edge maps (both dirs nil until first write) | 6.0M (-57%) | 166 ms (-52%) | 8.0M (+100%) | 466 ms (+13%) | 2,336 | Net wash — shifts the same hmap allocations from build into the cold-read path. Reverted. |
| inline-stack snapshot for `invalidate()` | 14.0M | 413 ms | 4.0M | 498 ms | 2,237 | No win — Go's escape analysis already stack-allocates `make([]T, 0, n)` for the small fan-out case; the inline array just grew the recursive stack frame. Reverted. |
| leave `Cell.dependencies` nil (cells never push onto the stack so the upstream map is never written) | 12.0M (-14%) | 298 ms (-14%) | 4.0M (no change) | 366 ms | 2,282 | Build-time alloc win, but a repeatable ~16% regression on `ScaleViewportRecalc` (1,960 → 2,270 ns) with no change to the steady-state allocation profile — a heap-layout / cache-line effect from removing 2M hmap structs we cannot explain. Not shippable as a perf win when an established benchmark regresses. Reverted. |
| `make(map, hint≥7)` bucket preallocation | +8M build, −4M cold | worse | 0 | better | — | Trades cold-read time for build time one-for-one (same total alloc count, just relocated). A wash on the spreadsheet workload. Not shipped. |

Two durable deliverables did land for 0.19.0:

- **`BenchmarkSlotmapChaseViewport` / `BenchmarkSlotmapChaseEdit`** in
  `bench_test.go` capture the pointer-chase cost in the default
  `make bench` run. The existing `scale_bench_test.go` benchmarks that
  originally measured the 2.6× viewport growth are gated behind the
  `scalebench` build tag (the heavy multi-million-node build would
  dominate a default bench run); the new `SlotmapChase*` benchmarks
  construct a 200K-node spreadsheet-shaped graph that fits comfortably
  in `make bench` and exhibits the same shape, so the slotmap cost is
  visible and re-measurable without opting into the heavy build. Numbers
  above (~2,090 ns viewport, ~6.35 ns edit-only) become the baseline
  future arena work can regress-test against.
- **The investigation table above** records *why* each smaller-scope
  attempt was rejected, with before/after numbers — so the next attempt
  doesn't re-walk the same ground.

What a full arena refactor would need to do (deferred):

1. Introduce a `nodeID` (uint32) and a `Context.arena []arenaNode` flat
   array. Each `reactiveBase` becomes `(id nodeID, ctx *Context)`; the
   `dependents`/`dependencies` maps move into `arenaNode` keyed by
   `nodeID` (so a map bucket entry drops from 16-byte `reactiveNode`
   interface to 4-byte `uint32`).
2. `Cell.Get`/`Slot.Get`/etc. keep their existing `*T` return types (so
   the public API is preserved), but the per-node storage lives in the
   arena and the public `*T` becomes a handle into it.
3. Every site that embeds `reactiveBase` (collections.go, async_context.go,
  reactive_map.go, registers.go, thread_safe*.go, signalSlot, Memo) needs
   its constructors migrated to the arena path.

That is the work that would actually close the slotmap gap. It is the
recommended next step for `#lzgoslotmap`.

[rs-scale]: https://github.com/lazily-hub/lazily-rs/blob/main/benches/scale.rs
