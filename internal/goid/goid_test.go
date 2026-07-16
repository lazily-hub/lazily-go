package goid

import (
	"runtime"
	"sync"
	"testing"
)

// Get() must agree with the runtime.Stack-derived id for every goroutine,
// including under -race and after GC. Guards the fast amd64 TLS read in
// goid_amd64.go (the slow path is used on other arches, where this is trivial).
func TestGetMatchesRuntimeStack(t *testing.T) {
	const goroutines = 200
	var wg sync.WaitGroup
	var mismatch int32
	var mu sync.Mutex
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if Get() != stackGID() {
				mu.Lock()
				mismatch++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if mismatch != 0 {
		t.Fatalf("%d goroutines reported Get() != stackGID", mismatch)
	}
	runtime.GC()
	if Get() != stackGID() {
		t.Fatal("Get() != stackGID after GC on test goroutine")
	}
}

// Get must be stable across calls on the same goroutine.
func TestGetStable(t *testing.T) {
	first := Get()
	for i := 0; i < 50; i++ {
		if got := Get(); got != first {
			t.Fatalf("Get() changed on same goroutine: %d -> %d", first, got)
		}
		runtime.Gosched()
	}
}
