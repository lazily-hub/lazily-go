package lazily

import "testing"

func TestConcurrencyScopesAreExplicitAcrossInProcessSurfaces(t *testing.T) {
	cases := []struct {
		name  string
		value ConcurrencyScopedCapability
		want  ConcurrencyScope
	}{
		{"Context", (*Context)(nil), ConcurrencyScopeSingleGoroutine},
		{"ThreadSafeContext", (*ThreadSafeContext)(nil), ConcurrencyScopeSingleProcessSerialized},
		{"AsyncContext", (*AsyncContext)(nil), ConcurrencyScopeSingleProcessSerialized},
		{"QueueCell", (*QueueCell[int, *VecDequeStorage[int]])(nil), ConcurrencyScopeSingleGoroutine},
		{"TopicCell", (*TopicCell[int])(nil), ConcurrencyScopeSingleGoroutine},
		{"WorkQueueCell", (*WorkQueueCell[int])(nil), ConcurrencyScopeSingleGoroutine},
		{"ThreadSafeQueueCell", (*ThreadSafeQueueCell[int, *VecDequeStorage[int]])(nil), ConcurrencyScopeSingleProcessSerialized},
		{"ThreadSafeTopicCell", (*ThreadSafeTopicCell[int])(nil), ConcurrencyScopeSingleProcessSerialized},
		{"ThreadSafeWorkQueueCell", (*ThreadSafeWorkQueueCell[int])(nil), ConcurrencyScopeSingleProcessSerialized},
		{"AsyncQueueCell", (*AsyncQueueCell[int, *VecDequeStorage[int]])(nil), ConcurrencyScopeSingleProcessSerialized},
		{"AsyncTopicCell", (*AsyncTopicCell[int])(nil), ConcurrencyScopeSingleProcessSerialized},
		{"AsyncWorkQueueCell", (*AsyncWorkQueueCell[int])(nil), ConcurrencyScopeSingleProcessSerialized},
		{"LatestDurableProjectionCore", (*LatestDurableProjectionCore[string, string])(nil), ConcurrencyScopeSingleGoroutine},
		{"LatestDurableProjection", (*LatestDurableProjection[string, string])(nil), ConcurrencyScopeSingleGoroutine},
		{"ThreadSafeLatestDurableProjection", (*ThreadSafeLatestDurableProjection[string, string])(nil), ConcurrencyScopeSingleProcessSerialized},
		{"AsyncLatestDurableProjection", (*AsyncLatestDurableProjection[string, string])(nil), ConcurrencyScopeSingleProcessSerialized},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := test.value.ConcurrencyScope(); got != test.want || !got.Valid() {
				t.Fatalf("ConcurrencyScope() = %q, want valid %q", got, test.want)
			}
			if _, advertised := test.value.(DatabaseProjectionMaintenanceBarrier); advertised {
				t.Fatalf("in-process capability advertises DatabaseProjectionMaintenanceBarrier")
			}
		})
	}
}

func TestPostgresProjectionBarrierAdvertisesDurableCrossProcessScope(t *testing.T) {
	var barrier DatabaseProjectionMaintenanceBarrier = (*PostgresProjectionBarrier)(nil)
	if got := barrier.ConcurrencyScope(); got != ConcurrencyScopeDurableCrossProcess {
		t.Fatalf("ConcurrencyScope() = %q, want %q", got, ConcurrencyScopeDurableCrossProcess)
	}
}
