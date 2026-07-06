package lazily

import "testing"

func TestSlotLazyRecompute(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 2)
	b := NewCell(ctx, 3)
	calls := 0
	sum := NewSlot(ctx, func(*Context) int {
		calls++
		return a.Get() + b.Get()
	})
	if got := sum.Get(); got != 5 {
		t.Fatalf("sum = %d, want 5", got)
	}
	if got := sum.Get(); got != 5 { // cached
		t.Fatalf("cached sum = %d, want 5", got)
	}
	if calls != 1 {
		t.Fatalf("compute called %d times, want 1", calls)
	}
	a.Set(10)
	if got := sum.Get(); got != 13 {
		t.Fatalf("sum after set = %d, want 13", got)
	}
	if calls != 2 {
		t.Fatalf("compute called %d times, want 2", calls)
	}
}

func TestCellPartialEqGuard(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 1)
	calls := 0
	NewEffect(ctx, func(*Context) func() {
		_ = a.Get()
		calls++
		return nil
	})
	if calls != 1 {
		t.Fatalf("effect ran %d times initially, want 1", calls)
	}
	a.Set(1) // equal — no cascade
	if calls != 1 {
		t.Fatalf("effect ran %d times after equal set, want 1", calls)
	}
	a.Set(2)
	if calls != 2 {
		t.Fatalf("effect ran %d times after change, want 2", calls)
	}
}

func TestSignalEager(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 2)
	parity := NewSignal(ctx, func(*Context) string {
		if a.Get()%2 == 0 {
			return "even"
		}
		return "odd"
	})
	if got := parity.Get(); got != "even" {
		t.Fatalf("parity = %q, want even", got)
	}
	a.Set(11)
	if got := parity.Get(); got != "odd" {
		t.Fatalf("parity = %q, want odd", got)
	}
}

func TestMemoEqualitySuppression(t *testing.T) {
	ctx := NewContext()
	width := NewCell(ctx, 10)
	parity := NewMemo(ctx, func(*Context) string {
		if width.Get()%2 == 0 {
			return "even"
		}
		return "odd"
	})
	downstream := 0
	NewEffect(ctx, func(*Context) func() {
		_ = parity.Get()
		downstream++
		return nil
	})
	if downstream != 1 {
		t.Fatalf("downstream ran %d times initially, want 1", downstream)
	}
	width.Set(12) // still even — memo suppresses
	if downstream != 1 {
		t.Fatalf("downstream ran %d times after even->even, want 1", downstream)
	}
	width.Set(13) // odd — cascade
	if downstream != 2 {
		t.Fatalf("downstream ran %d times after even->odd, want 2", downstream)
	}
}

func TestBatchCoalesces(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 1)
	b := NewCell(ctx, 1)
	runs := 0
	NewEffect(ctx, func(*Context) func() {
		_ = a.Get()
		_ = b.Get()
		runs++
		return nil
	})
	if runs != 1 {
		t.Fatalf("runs = %d, want 1", runs)
	}
	ctx.Batch(func() {
		a.Set(2)
		b.Set(2)
	})
	if runs != 2 {
		t.Fatalf("runs after batch = %d, want 2 (coalesced)", runs)
	}
}
