package lazily

import "testing"

// Go testing.B benchmarks for the lazily-go hot paths. These mirror the
// in-library RunBenchmarkSuite scenarios (instrumentation.go) so the numbers in
// BENCHMARKS.md are reproducible with `go test -bench=. -benchmem`.

func BenchmarkCellReadWrite(b *testing.B) {
	ctx := NewContext()
	c := NewCell(ctx, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(i)
		_ = c.Get()
	}
}

func BenchmarkSlotRecompute(b *testing.B) {
	ctx := NewContext()
	a := NewCell(ctx, 0)
	sum := NewSlot(ctx, func(*Context) int { return a.Get() * 2 })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Set(i) // invalidate
		_ = sum.Get()
	}
}

func BenchmarkMemoEqualityGuard(b *testing.B) {
	ctx := NewContext()
	width := NewCell(ctx, 0)
	parity := NewMemo(ctx, func(*Context) bool { return width.Get()%2 == 0 })
	downstream := 0
	NewEffect(ctx, func(*Context) func() {
		if parity.Get() {
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
	cells := make([]*Cell[int], n)
	for i := range cells {
		cells[i] = NewCell(ctx, 0)
	}
	sum := NewSlot(ctx, func(*Context) int {
		total := 0
		for _, c := range cells {
			total += c.Get()
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
