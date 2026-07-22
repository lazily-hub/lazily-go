package lazily

import (
	"runtime"
	"testing"
)

// Go testing.B benchmarks for the lazily-go hot paths. These mirror the
// in-library RunBenchmarkSuite scenarios (instrumentation.go) so the numbers in
// BENCHMARKS.md are reproducible with `go test -bench=. -benchmem`.

func BenchmarkCellReadWrite(b *testing.B) {
	ctx := NewContext()
	c := NewSource(ctx, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(i)
		_ = c.Get()
	}
}

func BenchmarkSlotRecompute(b *testing.B) {
	ctx := NewContext()
	a := NewSource(ctx, 0)
	sum := NewSlot(ctx, func(c *Compute) int { return Get(c, a) * 2 })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Set(i) // invalidate
		_ = sum.Get()
	}
}

func BenchmarkMemoEqualityGuard(b *testing.B) {
	ctx := NewContext()
	width := NewSource(ctx, 0)
	parity := NewComputed(ctx, func(c *Compute) bool { return Get(c, width)%2 == 0 })
	downstream := 0
	NewEffect(ctx, func(c *Compute) func() {
		if Get(c, parity) {
			downstream++
		}
		return nil
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		width.Set(width.Peek() + 2) // stays even — memo suppresses cascade
	}
}

func BenchmarkBatchCoalesce(b *testing.B) {
	ctx := NewContext()
	const n = 10
	cells := make([]*Source[int], n)
	for i := range cells {
		cells[i] = NewSource(ctx, 0)
	}
	sum := NewSlot(ctx, func(cv *Compute) int {
		total := 0
		for _, c := range cells {
			total += Get(cv, c)
		}
		return total
	})
	_ = sum.Get()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.Batch(func() {
			for _, c := range cells {
				c.Set(i)
			}
		})
		_ = sum.Get()
	}
}

func BenchmarkCellMapInsertRead(b *testing.B) {
	ctx := NewContext()
	m := NewCellMap[int, int](ctx)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(i%256, i)
		_, _ = m.Read(i % 256)
	}
}

func BenchmarkTextCrdtInsert(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := NewTextCrdt(1)
		for j := 0; j < 100; j++ {
			t.Insert(j, "a")
		}
	}
}

func BenchmarkSeqCrdtInsert(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := NewSeqCrdt[int, int](1)
		for j := 0; j < 100; j++ {
			s.InsertBack(j, j, int64(j))
		}
	}
}

// ---------------------------------------------------------------------------
// Phase 2 perf quick wins (#lzgono reflect, #lzgosecondary-index audit).
//
// These exercise the three reflect-free equality hot paths added in 0.18.0
// (ipc.go, async_context.go, presence.go). The two `Equal` benchmarks
// stress the comparators in isolation; the higher-level `AsyncCell*` and
// `PresenceCell*` benchmarks exercise them through the reactive API.
// ---------------------------------------------------------------------------

// BenchmarkPhase2IpcValueEqual covers both IpcValue variants through the
// PartialEq guard (ipcValueEqual). Inline uses bytes.Equal; SharedBlob uses
// plain struct ==.
func BenchmarkPhase2IpcValueEqual(b *testing.B) {
	inlineA := IpcValueInline{Bytes: []byte("hello lazily-go phase 2 reflect-free path")}
	inlineB := IpcValueInline{Bytes: []byte("hello lazily-go phase 2 reflect-free path")}
	blobA := IpcValueSharedBlob{Blob: ShmBlobRef{Offset: 42, Len: 1024, Generation: 7, Epoch: 3, Checksum: 999}}
	blobB := IpcValueSharedBlob{Blob: ShmBlobRef{Offset: 42, Len: 1024, Generation: 7, Epoch: 3, Checksum: 999}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Alternate variants so the type switch hits both branches.
		if i&1 == 0 {
			_ = ipcValueEqual(inlineA, inlineB)
		} else {
			_ = ipcValueEqual(blobA, blobB)
		}
	}
}

// BenchmarkPhase2AsyncValueEqual exercises the fast type-switch paths in
// asyncValueEqual (string / int / []byte), which previously paid
// reflect.TypeOf on every Set.
func BenchmarkPhase2AsyncValueEqual(b *testing.B) {
	sA, sB := "phase2", "phase2"
	iA, iB := 17, 17
	bA, bB := []byte("phase2-bytes"), []byte("phase2-bytes")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 3 {
		case 0:
			_ = asyncValueEqual(sA, sB)
		case 1:
			_ = asyncValueEqual(iA, iB)
		case 2:
			_ = asyncValueEqual(bA, bB)
		}
	}
}

// BenchmarkPhase2AsyncCellStringSet drives AsyncCell.Set on a string cell.
// The PartialEq guard previously paid reflect.TypeOf per write; the new fast
// path uses a string type switch and zero reflection.
func BenchmarkPhase2AsyncCellStringSet(b *testing.B) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	c := NewAsyncCell(ctx, "")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set("same") // equality guard short-circuits: no dependents to invalidate.
	}
}

// BenchmarkPhase2PresenceRefreshSteadyState heartbeats the same peer with the
// same value. The refresh path compares the live map to the last projection;
// previously reflect.DeepEqual, now the typed comparableMapEqual helper.
func BenchmarkPhase2PresenceRefreshSteadyState(b *testing.B) {
	ctx := NewContext()
	cell := NewPresenceCell[int, string](ctx, 1_000)
	// Seed one peer so refresh compares two single-entry maps each tick.
	cell.Heartbeat(1, "online", 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cell.Heartbeat(1, "online", uint64(i)) // value unchanged -> projection equal.
	}
}

// ---------------------------------------------------------------------------
// #lzgoslotmap — pointer-chase cost characterization
//
// The scale benchmarks in scale_bench_test.go (gated behind the `scalebench`
// build tag so a plain `make bench` skips the heavy multi-million-node build)
// measure the slotmap locality gap end-to-end: the viewport read grows ~2.6×
// from 2M→10M cells because nodes are scattered across a larger heap. The
// benchmarks below capture the same shape at a size that fits in the default
// `make bench` run, so the cost is visible without opting into the heavy
// build tag. They construct a fan-in-2 spreadsheet-shaped graph (input cell
// + previous-cell formula) of a tunable size, warm it once, then time a
// steady-state "edit one input, read a 1,000-cell viewport" iteration —
// exactly the workload the slotmap dense-arena would address.
//
// See BENCHMARKS.md → "#lzgoslotmap investigation" for the analysis of why a
// full arena refactor is the path that closes the gap (and why smaller-scope
// alternatives were measured and rejected for this turn).
// ---------------------------------------------------------------------------

// slotmapChaseSize is the default row count for the pointer-chase benchmarks.
// 100_000 rows ⇒ 200_000 reactive nodes — large enough for the heap to span
// many allocator spans (so cache effects show up) but small enough that the
// default `make bench` finishes in well under a second.
const slotmapChaseSize = 100_000

// buildSlotmapChase constructs a fan-in-2 spreadsheet-shaped graph with n
// rows: input[i] + formula[i] = input[i] + input[i-1]. Returns the cells,
// formulas, and the editable "middle" input index used by the viewport edit.
func buildSlotmapChase(n int) (cells []*Source[int64], formulas []*Computed[int64], mid int) {
	ctx := NewContext()
	cells = make([]*Source[int64], n)
	for i := 0; i < n; i++ {
		cells[i] = NewSource(ctx, int64(i))
	}
	formulas = make([]*Computed[int64], n)
	for i := 0; i < n; i++ {
		a := cells[i]
		prev := i - 1
		if prev < 0 {
			prev = 0
		}
		b := cells[prev]
		formulas[i] = NewSlot(ctx, func(c *Compute) int64 { return Get(c, a) + Get(c, b) })
	}
	for _, f := range formulas {
		_ = f.Get() // warm: establish edges and cache
	}
	mid = n / 2
	return
}

// benchSink is a package-level sink that defeats dead-code elimination of
// benchmarked reads in this file. (scale_bench_test.go has its own scaleSink
// behind the scalebench tag.)
var benchSink int64

// BenchmarkSlotmapChaseViewport is the default-`make-bench`-runnable version
// of `BenchmarkScaleViewportRecalc`. Edit one input, read a 1,000-cell
// viewport around the middle, repeat. Stays at zero allocations in the
// steady state — the timed cost is pure pointer-chase latency across the
// reactive graph, which is what a dense-arena refactor would address.
func BenchmarkSlotmapChaseViewport(b *testing.B) {
	cells, formulas, mid := buildSlotmapChase(slotmapChaseSize)
	lo := mid - 500
	if lo < 0 {
		lo = 0
	}
	hi := lo + 1000
	if hi > len(formulas) {
		hi = len(formulas)
	}
	var tick int64
	var acc int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tick++
		cells[mid].Set(tick)
		for _, f := range formulas[lo:hi] {
			acc += f.Get()
		}
	}
	benchSink = acc
}

// BenchmarkSlotmapChaseEdit measures just the invalidation cascade after a
// single input edit (no viewport read). In a fan-in-2 graph each edit
// invalidates exactly 2 formulas; the rest of the cost is downstream-cascade
// overhead. Useful for isolating the invalidate path from the read path.
func BenchmarkSlotmapChaseEdit(b *testing.B) {
	cells, _, mid := buildSlotmapChase(slotmapChaseSize)
	var tick int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tick++
		cells[mid].Set(tick)
	}
	runtime.KeepAlive(cells)
}
