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
| `CellReadWrite` | 9.35 | 0 | 0 | `Cell.Set` (PartialEq guard) + `Cell.Get` round trip — the core mutation path, alloc-free. |
| `CellMapInsertRead` | 16.58 | 0 | 0 | `CellMap.Set` + `Read` on a keyed collection — alloc-free steady state. |
| `MemoEqualityGuard` | 185.0 | 512 | 4 | `Memo` recompute that yields an equal value, suppressing the downstream cascade (an `Effect` stays put). |
| `SlotRecompute` | 189.1 | 512 | 4 | Invalidate a `Cell`, then re-pull a dependent `Slot` (edge re-tracking + recompute). |
| `BatchCoalesce` | 1864 | 4144 | 31 | 10 cell writes inside one `Batch`, coalesced into a single invalidation pass, then one `Slot` recompute. |
| `SeqCrdtInsert` | 79,162 | 38,863 | 1,809 | Build a move-aware `SeqCrdt` of 100 elements (fractional-index positions + LWW registers). |
| `TextCrdtInsert` | 687,793 | 1,054,417 | 11,444 | Build a `TextCrdt` of 100 characters (Fugue/RGA ordering rebuilt per insert). |

## Notes

- The reactive core steady-state (`CellReadWrite`, `CellMapInsertRead`) is
  **zero-allocation** — reads and equality-guarded writes don't touch the heap.
- `SlotRecompute` / `MemoEqualityGuard` allocate a small, constant amount per
  cascade (dependency-edge sets rebuilt on recompute); the memo guard's win is
  behavioral — it aborts the downstream cascade when the recomputed value is
  unchanged, not shown in per-op bytes here.
- The CRDT builders (`TextCrdtInsert`, `SeqCrdtInsert`) measure *whole-document
  construction* (100 ops), not per-op cost — divide by 100 for per-insert. They
  are the heaviest paths because visible order is recomputed as a pure function
  of the element set on each mutation, matching the spec's determinism
  requirement.
- These are single-threaded micro-benchmarks. The concurrency surfaces
  (`AsyncContext`, `SignalingRoom`, `CrdtPlaneRuntime`) are correctness-tested
  under the race detector rather than benchmarked here.

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

[rs-scale]: https://github.com/lazily-hub/lazily-rs/blob/main/benches/scale.rs
