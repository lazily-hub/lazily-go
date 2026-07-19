package lazily

import (
	"fmt"
	"testing"
)

// Two measurement arms for the Cell observer storage change (#lzdartobservercow).
//
// Method, per this workstream's standard:
//   - Total work is held fixed at observerBenchTotalOps and only the fan-out
//     width W varies, so ns/op is directly comparable across rungs and the
//     assertion is on the wide/narrow ratio, not on absolute growth.
//   - Each W has a "wide" shape (one cell, W observers) and a "narrow" control
//     at equal node count (W cells, one observer each). Same total observers,
//     same total subscribe/dispose ops, same total invocations.
//   - Every arm counts the work it actually performed and fails the benchmark
//     if the count does not match, so an inert harness cannot be mistaken for a
//     flat result.
//   - Both arms run an untimed warmup iteration before b.ResetTimer, because a
//     cold first iteration lands GC and allocator growth inside the timed
//     region and manufactures phantom regressions.
//
// The churn arm is the "must not hurt" guard: the map implementation this
// replaced already had O(1) subscribe and unsubscribe, so the slot list must
// not give that up. The publish arm is where the defect was — the map had to
// materialize an O(W) snapshot slice on every notification. allocs/op is the
// load-immune evidence there and should read 0 at every W.

const observerBenchTotalOps = 16384

var observerBenchWidths = []int{2, 16, 128, 1024, 16384}

// An arm is a prepare step (untimed: context and cell construction) returning
// the closure that performs exactly observerBenchTotalOps units of the work
// under test. Keeping construction out of the timed region is what makes
// allocs/op read the notification path rather than fixture setup.
type observerArm func(w int) func() int

// churnWide subscribes and disposes W observers on a single cell, repeated
// until observerBenchTotalOps subscriptions have been made.
func churnWide(w int) func() int {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	rounds := observerBenchTotalOps / w
	disposers := make([]func(), 0, w)
	return func() int {
		ops := 0
		for r := 0; r < rounds; r++ {
			disposers = disposers[:0]
			for i := 0; i < w; i++ {
				disposers = append(disposers, c.Subscribe(func(int) {}))
				ops++
			}
			for _, d := range disposers {
				d()
			}
		}
		return ops
	}
}

// churnNarrow is the equal-node-count control: W cells with one observer each,
// same total subscribe/dispose count as churnWide.
func churnNarrow(w int) func() int {
	ctx := NewContext()
	cells := make([]*Cell[int], w)
	for i := range cells {
		cells[i] = NewCell(ctx, 0)
	}
	rounds := observerBenchTotalOps / w
	disposers := make([]func(), 0, w)
	return func() int {
		ops := 0
		for r := 0; r < rounds; r++ {
			disposers = disposers[:0]
			for i := 0; i < w; i++ {
				disposers = append(disposers, cells[i].Subscribe(func(int) {}))
				ops++
			}
			for _, d := range disposers {
				d()
			}
		}
		return ops
	}
}

// publishWide holds a stable set of W observers on one cell and notifies it
// until observerBenchTotalOps observer invocations have happened.
func publishWide(w int) func() int {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	fired := 0
	for i := 0; i < w; i++ {
		c.Subscribe(func(int) { fired++ })
	}
	notifies := observerBenchTotalOps / w
	n := 0
	return func() int {
		before := fired
		for i := 0; i < notifies; i++ {
			n++
			c.Set(n)
		}
		return fired - before
	}
}

// publishNarrow is the equal-node-count control: W cells with one observer
// each, notified so that the total invocation count matches publishWide.
func publishNarrow(w int) func() int {
	ctx := NewContext()
	cells := make([]*Cell[int], w)
	fired := 0
	for i := range cells {
		cells[i] = NewCell(ctx, 0)
		cells[i].Subscribe(func(int) { fired++ })
	}
	notifies := observerBenchTotalOps / w
	n := 0
	return func() int {
		before := fired
		for i := 0; i < notifies; i++ {
			n++
			for _, c := range cells {
				c.Set(n)
			}
		}
		return fired - before
	}
}

func runObserverArm(b *testing.B, arm observerArm, w int) {
	run := arm(w)
	// Warmup outside the timed region: the first iterations pay allocator and
	// GC growth that would otherwise be attributed to the change under test.
	for i := 0; i < 3; i++ {
		if got := run(); got != observerBenchTotalOps {
			b.Fatalf("inert harness: warmup performed %d units, want %d", got, observerBenchTotalOps)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	total := 0
	for i := 0; i < b.N; i++ {
		total += run()
	}
	b.StopTimer()
	if want := observerBenchTotalOps * b.N; total != want {
		b.Fatalf("inert harness: performed %d units, want %d", total, want)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(total), "ns/unit")
}

func BenchmarkObserverChurn(b *testing.B) {
	for _, w := range observerBenchWidths {
		b.Run(fmt.Sprintf("W=%d/wide", w), func(b *testing.B) { runObserverArm(b, churnWide, w) })
		b.Run(fmt.Sprintf("W=%d/narrow", w), func(b *testing.B) { runObserverArm(b, churnNarrow, w) })
	}
}

func BenchmarkObserverPublish(b *testing.B) {
	for _, w := range observerBenchWidths {
		b.Run(fmt.Sprintf("W=%d/wide", w), func(b *testing.B) { runObserverArm(b, publishWide, w) })
		b.Run(fmt.Sprintf("W=%d/narrow", w), func(b *testing.B) { runObserverArm(b, publishNarrow, w) })
	}
}
