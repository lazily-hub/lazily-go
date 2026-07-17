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
