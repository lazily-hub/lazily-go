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

```
goos: linux
goarch: amd64
cpu: AMD Ryzen 9 9950X3D 16-Core Processor
go: go1.26.4
```

| Benchmark | ns/op | B/op | allocs/op | What it measures |
|-----------|------:|-----:|----------:|------------------|
| `CellReadWrite` | 8.65 | 0 | 0 | `Cell.Set` (PartialEq guard) + `Cell.Get` round trip — the core mutation path, alloc-free. |
| `CellMapInsertRead` | 15.58 | 0 | 0 | `CellMap.Set` + `Read` on a keyed collection — alloc-free steady state. |
| `MemoEqualityGuard` | 209.9 | 512 | 4 | `Memo` recompute that yields an equal value, suppressing the downstream cascade (an `Effect` stays put). |
| `SlotRecompute` | 240.9 | 520 | 4 | Invalidate a `Cell`, then re-pull a dependent `Slot` (edge re-tracking + recompute). |
| `BatchCoalesce` | 1998 | 4152 | 31 | 10 cell writes inside one `Batch`, coalesced into a single invalidation pass, then one `Slot` recompute. |
| `SeqCrdtInsert` | 76,489 | 38,870 | 1,810 | Build a move-aware `SeqCrdt` of 100 elements (fractional-index positions + LWW registers). |
| `TextCrdtInsert` | 687,792 | 1,054,418 | 11,444 | Build a `TextCrdt` of 100 characters (Fugue/RGA ordering rebuilt per insert). |

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
