//go:build scalebench

// #lzscalebench — large-graph scale benchmark for lazily-go, replicating the
// lazily-rs `scale` group (benches/scale.rs).
//
// Models a spreadsheet-shaped graph: N input cells plus N formula slots, where
// formula[i] = input[i] + input[i-1] (local fan-in, like a column of
// =A_i + A_{i-1}). With the default N = 1_000_000 that is ~2M reactive nodes.
// Four benchmarks cover the spreadsheet lifecycle:
//
//   - ScaleBuild                  — construct all 2N nodes.
//   - ScaleColdFullRecalc         — first read of every formula (forces every compute).
//   - ScaleViewportRecalc         — edit one input, read only a bounded viewport
//     (the lazy-pull win: off-viewport formulas stay
//     dirty and never recompute).
//   - ScaleFullRecalcInvalidateAll — touch every input, then read every formula.
//
// Gated behind the `scalebench` build tag so a plain `make bench` skips the
// heavy 2M-node build. Run on demand or at a different size:
//
//	make bench-scale
//	go test -tags scalebench -run '^$' -bench=Scale -benchmem ./...
//	LAZILY_SCALE_N=2000000 go test -tags scalebench -run '^$' -bench=Scale ./...
//	LAZILY_SCALE_N=5000000 go test -tags scalebench -run '^$' -bench=Scale ./...  # Google Sheets 10M-cell workbook
package lazily

import (
	"os"
	"runtime"
	"strconv"
	"testing"
)

func scaleN() int {
	if v, err := strconv.Atoi(os.Getenv("LAZILY_SCALE_N")); err == nil && v > 0 {
		return v
	}
	return 1_000_000
}

func scaleViewport(n int) int {
	v := 1_000
	if x, err := strconv.Atoi(os.Getenv("LAZILY_SCALE_VIEWPORT")); err == nil && x > 0 {
		v = x
	}
	if v > n {
		v = n
	}
	return v
}

type scaleGraph struct {
	ctx      *Context
	inputs   []*Cell[int64]
	formulas []*Slot[int64]
}

// buildScaleGraph constructs the spreadsheet-shaped graph (formulas not yet
// computed — lazy until first read).
func buildScaleGraph(n int) scaleGraph {
	ctx := NewContext()
	inputs := make([]*Cell[int64], n)
	for i := 0; i < n; i++ {
		inputs[i] = NewCell(ctx, int64(i))
	}
	formulas := make([]*Slot[int64], n)
	for i := 0; i < n; i++ {
		a := inputs[i]
		prev := i - 1
		if prev < 0 {
			prev = 0
		}
		b := inputs[prev]
		formulas[i] = NewSlot(ctx, func(*Context) int64 { return a.Get() + b.Get() })
	}
	return scaleGraph{ctx: ctx, inputs: inputs, formulas: formulas}
}

func readAllFormulas(g scaleGraph) int64 {
	var acc int64
	for _, f := range g.formulas {
		acc += f.Get()
	}
	return acc
}

// scaleSink defeats dead-code elimination of benchmarked reads.
var scaleSink int64

func BenchmarkScaleBuild(b *testing.B) {
	n := scaleN()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		g := buildScaleGraph(n)
		runtime.KeepAlive(g)
	}
}

func BenchmarkScaleColdFullRecalc(b *testing.B) {
	n := scaleN()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		g := buildScaleGraph(n)
		b.StartTimer()
		scaleSink = readAllFormulas(g)
	}
}

func BenchmarkScaleViewportRecalc(b *testing.B) {
	n := scaleN()
	vp := scaleViewport(n)
	g := buildScaleGraph(n)
	scaleSink = readAllFormulas(g) // warm the whole sheet once
	mid := n / 2
	lo := mid - vp/2
	if lo < 0 {
		lo = 0
	}
	hi := lo + vp
	if hi > n {
		hi = n
	}
	var tick int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tick++
		g.inputs[mid].Set(tick) // edit one input; PartialEq guard passes (toggling value)
		var acc int64
		for _, f := range g.formulas[lo:hi] {
			acc += f.Get()
		}
		scaleSink = acc
	}
}

func BenchmarkScaleFullRecalcInvalidateAll(b *testing.B) {
	n := scaleN()
	g := buildScaleGraph(n)
	scaleSink = readAllFormulas(g) // warm once
	var tick int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tick++
		base := tick
		for j, c := range g.inputs {
			c.Set(base + int64(j))
		}
		scaleSink = readAllFormulas(g)
	}
}
