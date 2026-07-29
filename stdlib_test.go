package lazily

import "testing"

func TestRevisionBarrierRejectsClockRegressionBeforeMutationOrCancellation(t *testing.T) {
	barrier := NewRevisionBarrier(0, 1, nil)
	cancellationCalls := 0
	if observation := barrier.Observe(10, false, func() TimeoutCancellation {
		cancellationCalls++
		return CancellationPending
	}); observation.Outcome != "pending" {
		t.Fatalf("initial observation = %#v", observation)
	}

	cancellationCalls = 0
	regression := barrier.Observe(9, true, func() TimeoutCancellation {
		cancellationCalls++
		return CancellationCancelled
	})
	if regression.Outcome != "unavailable" ||
		regression.Reason != string(TimerClockRegression) ||
		regression.Revision != 0 ||
		regression.Generation != 0 {
		t.Fatalf("regression = %#v", regression)
	}
	if cancellationCalls != 0 {
		t.Fatalf("regressing observation called cancellation %d time(s)", cancellationCalls)
	}

	registered := NewRevisionBarrier(0, 1, nil)
	registered.RegisterRecheck(10, 0, false)
	registerRegression := registered.RegisterRecheck(9, 7, true)
	if registerRegression.Outcome != "unavailable" ||
		registerRegression.Reason != string(TimerClockRegression) ||
		registerRegression.Revision != 0 ||
		registerRegression.Generation != 0 {
		t.Fatalf("register regression = %#v", registerRegression)
	}
}

func TestRevisionBarrierPreservesFirstTerminalAcrossReentrantCancellation(t *testing.T) {
	barrier := NewRevisionBarrier(0, 1, nil)
	observation := barrier.Observe(0, false, func() TimeoutCancellation {
		barrier.Dispose()
		return CancellationCancelled
	})
	if observation.Outcome != "disposed" {
		t.Fatalf("reentrant terminal = %#v", observation)
	}
}
