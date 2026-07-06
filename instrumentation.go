// Instrumentation — benchmark harness for reactive operations.
//
// Lightweight micro-benchmarks for the reactive core, keyed collections, and
// CRDT types. This is the in-library instrumentation API (not Go `testing.B`
// benchmarks); drive it from a `main` or a tool.
//
// Ported from lazily-dart lib/src/instrumentation.dart. Semantics match the
// Dart harness: each scenario is executed `iterations` times and the total
// wall-clock time is recorded in microseconds.
package lazily

import (
	"fmt"
	"time"
)

// DefaultBenchmarkIterations is the iteration count used when a caller does not
// specify one, matching the Dart harness default.
const DefaultBenchmarkIterations = 10000

// BenchmarkResult is a single benchmark measurement.
type BenchmarkResult struct {
	Name       string
	Iterations int
	// TotalMicros is the total elapsed time across all iterations, in
	// microseconds.
	TotalMicros int64
}

// AvgMicros returns the average time per iteration in microseconds.
func (r BenchmarkResult) AvgMicros() float64 {
	return float64(r.TotalMicros) / float64(r.Iterations)
}

// OpsPerSecond returns the operations per second.
func (r BenchmarkResult) OpsPerSecond() float64 {
	return float64(r.Iterations) / (float64(r.TotalMicros) / 1000000)
}

// String renders the result the same way the Dart toString does.
func (r BenchmarkResult) String() string {
	return fmt.Sprintf("%s: %.2fµs/op, %.0f ops/s (%d iters)",
		r.Name, r.AvgMicros(), r.OpsPerSecond(), r.Iterations)
}

// Benchmark runs body iterations times and measures the total elapsed time.
func Benchmark(name string, body func(), iterations int) BenchmarkResult {
	start := time.Now()
	for i := 0; i < iterations; i++ {
		body()
	}
	elapsed := time.Since(start)
	return BenchmarkResult{
		Name:        name,
		Iterations:  iterations,
		TotalMicros: elapsed.Microseconds(),
	}
}

// RunBenchmarkSuite runs the full benchmark suite and returns every result.
// Pass DefaultBenchmarkIterations to match the Dart default.
func RunBenchmarkSuite(iterations int) []BenchmarkResult {
	return []BenchmarkResult{
		Benchmark("Cell read/write", func() {
			ctx := NewContext()
			c := NewCell[int](ctx, 0)
			c.Set(42)
			c.Get()
		}, iterations),
		Benchmark("Slot recompute", func() {
			ctx := NewContext()
			a := NewCell[int](ctx, 1)
			b := NewCell[int](ctx, 2)
			sum := NewSlot[int](ctx, func(_ *Context) int { return a.Get() + b.Get() })
			a.Set(10)
			sum.Get()
		}, iterations),
		Benchmark("Memo equality guard (cache hit)", func() {
			ctx := NewContext()
			src := NewCell[int](ctx, 4)
			parity := NewMemo[string](ctx, func(_ *Context) string {
				if src.Get()%2 == 0 {
					return "even"
				}
				return "odd"
			})
			src.Set(6) // still even — memo suppresses
			parity.Get()
		}, iterations),
		Benchmark("batch coalesce (10 cells)", func() {
			ctx := NewContext()
			cells := make([]*Cell[int], 10)
			for i := 0; i < 10; i++ {
				cells[i] = NewCell[int](ctx, i)
			}
			NewEffect(ctx, func(_ *Context) func() {
				for _, c := range cells {
					c.Get()
				}
				return nil
			})
			ctx.Batch(func() {
				for i := 0; i < 10; i++ {
					cells[i].Set(i + 1)
				}
			})
		}, iterations),
		Benchmark("CellMap insert + read", func() {
			ctx := NewContext()
			m := NewCellMap[string, int](ctx)
			for i := 0; i < 10; i++ {
				m.Set(fmt.Sprintf("k%d", i), i)
			}
			m.Read("k5")
		}, iterations),
		Benchmark("TextCrdt insert 100 chars", func() {
			crdt := NewTextCrdt(1)
			for i := 0; i < 100; i++ {
				crdt.Insert(i, "a")
			}
		}, iterations/10),
		Benchmark("SeqCrdt insert 100 elements", func() {
			seq := NewSeqCrdt[int, int](1)
			for i := 0; i < 100; i++ {
				seq.InsertBack(i, i, int64(i))
			}
		}, iterations/10),
	}
}
