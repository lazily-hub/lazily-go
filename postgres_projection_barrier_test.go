package lazily

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestPostgresProjectionLocksHaveOneTotalOrder(t *testing.T) {
	locks, err := orderedPostgresProjectionLocks([]PostgresProjectionLock{
		{Namespace: 2, Key: 1},
		{Namespace: 1, Key: 3},
		{Namespace: 1, Key: 2},
		{Namespace: 1, Key: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []PostgresProjectionLock{
		{Namespace: 1, Key: 2},
		{Namespace: 1, Key: 3},
		{Namespace: 2, Key: 1},
	}
	if !reflect.DeepEqual(locks, want) {
		t.Fatalf("ordered locks = %#v, want %#v", locks, want)
	}
}

func TestPostgresProjectionBarrierRejectsUnboundedOrMissingWork(t *testing.T) {
	if _, err := orderedPostgresProjectionLocks(nil); !errors.Is(err, ErrPostgresProjectionBarrierConfig) {
		t.Fatalf("empty lock set error = %v", err)
	}
	if _, err := NewPostgresProjectionBarrier(nil, PostgresProjectionBarrierOptions{}); !errors.Is(err, ErrPostgresProjectionBarrierConfig) {
		t.Fatalf("nil database error = %v", err)
	}
	if got := postgresProjectionTimeoutSetting(time.Nanosecond); got != "1ms" {
		t.Fatalf("sub-millisecond timeout = %q, want 1ms", got)
	}
}

func TestInProcessProjectionTypesDoNotAdvertiseDatabaseBarrier(t *testing.T) {
	var threadSafe any = (*ThreadSafeLatestDurableProjection[string, string])(nil)
	var async any = (*AsyncLatestDurableProjection[string, string])(nil)
	if _, ok := threadSafe.(DatabaseProjectionMaintenanceBarrier); ok {
		t.Fatal("ThreadSafeLatestDurableProjection must not advertise a database barrier")
	}
	if _, ok := async.(DatabaseProjectionMaintenanceBarrier); ok {
		t.Fatal("AsyncLatestDurableProjection must not advertise a database barrier")
	}
	var _ DatabaseProjectionMaintenanceBarrier = (*PostgresProjectionBarrier)(nil)
}
