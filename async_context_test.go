package lazily

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Ported from lazily-dart test/async_context_test.dart, plus explicit
// supersession/cancellation and Close coverage. These run clean under -race:
// every shared counter is atomic or mutex-guarded, and compute/effect bodies
// gate on channels or their cancellation context rather than raw sleeps.

// waitFor polls cond until it is true or the deadline elapses.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

func TestAsyncV2CompatibilityAliasesShareCanonicalTypes(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()

	var source *AsyncSource[int] = NewAsyncCell(ctx, 2)
	var legacySource *AsyncCellHandle[int] = source
	var computed *AsyncComputed[int] = NewAsyncSlot(
		ctx,
		func(cc *AsyncComputeContext) (int, error) {
			return TrackCell(cc, legacySource) * 3, nil
		},
	)
	var legacyComputed *AsyncSlotHandle[int] = computed
	outer := NewAsyncSlot(
		ctx,
		func(cc *AsyncComputeContext) (int, error) {
			return TrackAsync(cc, legacyComputed)
		},
	)
	if got, err := outer.GetAsync(context.Background()); err != nil || got != 6 {
		t.Fatalf("legacy computed = (%d, %v), want (6, nil)", got, err)
	}

	memo := NewAsyncMemo(
		ctx,
		func(cc *AsyncComputeContext) (int, error) { return 7, nil },
		func(left, right int) bool { return left == right },
	)
	var canonicalMemo *AsyncComputed[int] = memo
	if got, err := canonicalMemo.GetAsync(context.Background()); err != nil || got != 7 {
		t.Fatalf("legacy memo = (%d, %v), want (7, nil)", got, err)
	}

	var legacyState AsyncSlotState = AsyncSlotResolved
	var canonicalState AsyncComputedState = legacyState
	if canonicalState != AsyncComputedResolved {
		t.Fatalf("state alias = %q, want %q", canonicalState, AsyncComputedResolved)
	}
}

func TestAsyncEmptyComputingResolved(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	a := NewAsyncSource(ctx, 2)
	b := NewAsyncSource(ctx, 3)
	sum := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		return TrackSource(cc, a) + TrackSource(cc, b), nil
	})
	if got := sum.State(); got != AsyncComputedEmpty {
		t.Fatalf("initial state = %q, want empty", got)
	}
	v, err := sum.GetAsync(context.Background())
	if err != nil || v != 5 {
		t.Fatalf("GetAsync = (%d, %v), want (5, nil)", v, err)
	}
	if got := sum.State(); got != AsyncComputedResolved {
		t.Fatalf("state after resolve = %q, want resolved", got)
	}
}

func TestAsyncResolvedComputingResolvedOnDepChange(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	a := NewAsyncSource(ctx, 1)
	slot := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		return TrackSource(cc, a) * 10, nil
	})
	if v, err := slot.GetAsync(context.Background()); err != nil || v != 10 {
		t.Fatalf("first GetAsync = (%d, %v), want (10, nil)", v, err)
	}
	a.Set(5)
	// Synchronous cached read is unavailable while the recompute is pending.
	if _, ok := slot.Get(); ok {
		t.Fatal("Get() should report not-resolved while recompute pending")
	}
	if v, err := slot.GetAsync(context.Background()); err != nil || v != 50 {
		t.Fatalf("GetAsync after dep change = (%d, %v), want (50, nil)", v, err)
	}
}

func TestAsyncComputingErrorAndRetry(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	boom := errors.New("boom")
	var calls int32
	slot := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return 0, boom
		}
		return 7, nil
	})
	if _, err := slot.GetAsync(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("GetAsync err = %v, want boom", err)
	}
	if got := slot.State(); got != AsyncComputedError {
		t.Fatalf("state = %q, want error", got)
	}
	// Error -> Computing: a slot in Error holds no cached result, so the next
	// read re-spawns rather than replaying the stored error. Without this a
	// transient failure is permanent for the slot's lifetime and no read path
	// can recover it (docs/async.md § Async slot state machine;
	// LazilyFormal.AsyncSlotState SlotEvent.retry).
	if v, err := slot.GetAsync(context.Background()); err != nil || v != 7 {
		t.Fatalf("retry GetAsync = (%d, %v), want (7, nil)", v, err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("compute calls = %d, want 2 (the retry must re-run the body)", got)
	}
	if got := slot.State(); got != AsyncComputedResolved {
		t.Fatalf("state after retry = %q, want resolved", got)
	}
	// The runtime keeps running after a failing slot: a fresh slot resolves.
	ok := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) { return 42, nil })
	if v, err := ok.GetAsync(context.Background()); err != nil || v != 42 {
		t.Fatalf("fresh slot GetAsync = (%d, %v), want (42, nil)", v, err)
	}
}

// A slot whose compute keeps failing keeps re-spawning: the error is delivered
// to each caller, never cached and replayed. This is the half of the retry
// contract that a sticky-error binding also gets "right" by accident, so it is
// asserted on the compute counter, not just the returned error.
func TestAsyncErrorRetriesEveryRead(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	boom := errors.New("boom")
	var calls int32
	slot := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		atomic.AddInt32(&calls, 1)
		return 0, boom
	})
	for i := 0; i < 3; i++ {
		if _, err := slot.GetAsync(context.Background()); !errors.Is(err, boom) {
			t.Fatalf("read %d err = %v, want boom", i, err)
		}
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("compute calls = %d, want 3 (one per read; a cached error would report 1)", got)
	}
}

func TestAsyncStaleCompletionDiscarded(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	step := NewAsyncSource(ctx, 0)
	var calls int32
	slot := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		n := atomic.AddInt32(&calls, 1)
		s := TrackSource(cc, step)
		if n == 1 {
			// Block the first run until it is superseded (its context is
			// cancelled by the dependency change below), then finish stale.
			<-cc.Context().Done()
		}
		return s, nil
	})

	type res struct {
		v   int
		err error
	}
	done := make(chan res, 1)
	go func() {
		v, err := slot.GetAsync(context.Background())
		done <- res{v, err}
	}()

	// Wait until the first compute is in flight, then supersede it.
	waitFor(t, func() bool { return atomic.LoadInt32(&calls) >= 1 })
	step.Set(1)

	r := <-done
	if r.err != nil || r.v != 1 {
		t.Fatalf("pending GetAsync = (%d, %v), want (1, nil)", r.v, r.err)
	}
	if got := atomic.LoadInt32(&calls); got <= 1 {
		t.Fatalf("calls = %d, want > 1 (a fresh compute was spawned)", got)
	}
}

func TestAsyncInFlightDeduplication(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	a := NewAsyncSource(ctx, 1)
	var calls int32
	gate := make(chan struct{})
	slot := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		atomic.AddInt32(&calls, 1)
		select {
		case <-gate:
		case <-cc.Context().Done():
		}
		return TrackSource(cc, a), nil
	})

	var wg sync.WaitGroup
	results := make([]int, 2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = slot.GetAsync(context.Background())
		}(i)
	}
	// Both callers attach to the one in-flight compute before we release it.
	waitFor(t, func() bool { return atomic.LoadInt32(&calls) == 1 })
	time.Sleep(20 * time.Millisecond) // let the second GetAsync attach
	close(gate)
	wg.Wait()

	for i := 0; i < 2; i++ {
		if errs[i] != nil || results[i] != 1 {
			t.Fatalf("waiter %d = (%d, %v), want (1, nil)", i, results[i], errs[i])
		}
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("calls = %d, want 1 (one in-flight compute per revision)", got)
	}
}

func TestAsyncMemoEqualRecomputeSuppresses(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	trigger := NewAsyncSource(ctx, 0)
	var calls int32
	slot := NewAsyncComputedWithEquals(ctx, func(cc *AsyncComputeContext) (string, error) {
		atomic.AddInt32(&calls, 1)
		_ = TrackSource(cc, trigger)
		return "constant", nil
	}, func(a, b string) bool { return a == b })

	if v, err := slot.GetAsync(context.Background()); err != nil || v != "constant" {
		t.Fatalf("first GetAsync = (%q, %v), want (constant, nil)", v, err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
	trigger.Set(1)
	if v, err := slot.GetAsync(context.Background()); err != nil || v != "constant" {
		t.Fatalf("GetAsync after trigger = (%q, %v), want (constant, nil)", v, err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("calls = %d, want 2 (recompute still runs; suppression is on publish)", got)
	}
}

func TestAsyncEffectCleanupBeforeBody(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	trigger := NewAsyncSource(ctx, 1)
	var mu sync.Mutex
	var log []string
	appendLog := func(s string) { mu.Lock(); log = append(log, s); mu.Unlock() }
	snapshot := func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), log...) }

	effect := ctx.EffectAsync(func(cc *AsyncComputeContext) func() {
		v := TrackSource(cc, trigger)
		appendLog("body")
		_ = v
		return func() { appendLog("cleanup") }
	})
	waitFor(t, func() bool { return len(snapshot()) >= 1 })
	trigger.Set(2)
	// Wait for: body, cleanup, body (in that order).
	waitFor(t, func() bool { return len(snapshot()) >= 3 })

	l := snapshot()
	// Find cleanup and the second body; cleanup must precede second body.
	cleanupIdx, secondBodyIdx := -1, -1
	bodies := 0
	for i, e := range l {
		if e == "cleanup" && cleanupIdx == -1 {
			cleanupIdx = i
		}
		if e == "body" {
			bodies++
			if bodies == 2 {
				secondBodyIdx = i
			}
		}
	}
	if cleanupIdx == -1 || secondBodyIdx == -1 || cleanupIdx > secondBodyIdx {
		t.Fatalf("log = %v, want cleanup before second body", l)
	}
	effect.DisposeAsync()
}

func TestAsyncBatchCoalesces(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	a := NewAsyncSource(ctx, 1)
	b := NewAsyncSource(ctx, 2)
	var calls int32
	slot := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		atomic.AddInt32(&calls, 1)
		return TrackSource(cc, a) + TrackSource(cc, b), nil
	})
	if v, err := slot.GetAsync(context.Background()); err != nil || v != 3 {
		t.Fatalf("first GetAsync = (%d, %v), want (3, nil)", v, err)
	}
	before := atomic.LoadInt32(&calls)
	ctx.Batch(func() {
		a.Set(10)
		b.Set(20)
	})
	if v, err := slot.GetAsync(context.Background()); err != nil || v != 30 {
		t.Fatalf("GetAsync after batch = (%d, %v), want (30, nil)", v, err)
	}
	if got := atomic.LoadInt32(&calls); got != before+1 {
		t.Fatalf("calls = %d, want %d (one rerun from the batch)", got, before+1)
	}
}

func TestAsyncDisposalWritesAreNoOps(t *testing.T) {
	ctx := NewAsyncContext()
	a := NewAsyncSource(ctx, 1)
	slot := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		return TrackSource(cc, a), nil
	})
	if v, err := slot.GetAsync(context.Background()); err != nil || v != 1 {
		t.Fatalf("GetAsync = (%d, %v), want (1, nil)", v, err)
	}
	if err := ctx.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	// No panic and no hang: the invalidation path is a no-op after disposal.
	a.Set(999)
	// Reads after disposal return the disposed error.
	if _, err := slot.GetAsync(context.Background()); !errors.Is(err, ErrAsyncContextDisposed) {
		t.Fatalf("post-dispose GetAsync err = %v, want ErrAsyncContextDisposed", err)
	}
}

// --- explicit cancellation / supersession / Close coverage -----------------

// A cancelled waiter's context drops only that waiter; the shared compute keeps
// running and still resolves for the remaining waiter.
func TestAsyncWaiterCancellationDropsOnlyThatWaiter(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	a := NewAsyncSource(ctx, 7)
	var calls int32
	gate := make(chan struct{})
	slot := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		atomic.AddInt32(&calls, 1)
		select {
		case <-gate:
		case <-cc.Context().Done():
		}
		return TrackSource(cc, a), nil
	})

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancelledErr := make(chan error, 1)
	go func() {
		_, err := slot.GetAsync(cancelCtx)
		cancelledErr <- err
	}()
	keepErr := make(chan error, 1)
	keepVal := make(chan int, 1)
	go func() {
		v, err := slot.GetAsync(context.Background())
		keepVal <- v
		keepErr <- err
	}()

	waitFor(t, func() bool { return atomic.LoadInt32(&calls) == 1 })
	time.Sleep(20 * time.Millisecond) // let both waiters attach
	cancel()
	if err := <-cancelledErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter err = %v, want context.Canceled", err)
	}
	// The shared compute still resolves for the remaining waiter.
	close(gate)
	if v, err := <-keepVal, <-keepErr; err != nil || v != 7 {
		t.Fatalf("remaining waiter = (%d, %v), want (7, nil)", v, err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("calls = %d, want 1 (cancellation never re-spawned)", got)
	}
}

// Closing the context while a compute is in flight unblocks pending waiters
// with the disposed error and does not leak the compute goroutine.
func TestAsyncCloseUnblocksPendingWaiter(t *testing.T) {
	ctx := NewAsyncContext()
	a := NewAsyncSource(ctx, 1)
	started := make(chan struct{})
	slot := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		close(started)
		<-cc.Context().Done() // block until disposed cancels us
		return TrackSource(cc, a), nil
	})
	errCh := make(chan error, 1)
	go func() {
		_, err := slot.GetAsync(context.Background())
		errCh <- err
	}()
	<-started
	if err := ctx.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	if err := <-errCh; !errors.Is(err, ErrAsyncContextDisposed) {
		t.Fatalf("pending waiter err = %v, want ErrAsyncContextDisposed", err)
	}
	// Close is idempotent.
	if err := ctx.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
}
