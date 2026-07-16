//go:build amd64

package goid

import (
	"sync"
	"unsafe"
)

// Fast goroutine id for amd64: read the runtime g pointer (from TLS) and the
// goid field directly, avoiding a runtime.Stack walk+format on every call.
//
// The goid field offset inside runtime.g is not part of Go's stable ABI, so it
// is probed once at first use: two helper goroutines each derive their own goid
// via runtime.Stack (stackGID) and scan their own g struct for a matching int64.
// They agree on the same offset only at the real goid field (any other field
// holds one fixed value that can equal at most one of the two distinct probed
// goids), which removes false positives. All unsafe reads live in assembly
// (goid_amd64.s) so checkptr and `go vet` see no Go-side pointer arithmetic.
//
// On probe failure (offset 0) Get falls back to slow.

func getg() unsafe.Pointer

func readGoid(base unsafe.Pointer, offset uintptr) int64

func scanGoid(base unsafe.Pointer, target int64, max uintptr) uintptr

var goidProbe = struct {
	once   sync.Once
	offset uintptr
}{}

func probeGoidOffset() uintptr {
	c := make(chan uintptr, 2)
	for i := 0; i < 2; i++ {
		go func() {
			gid := stackGID()
			c <- scanGoid(getg(), gid, 2048)
		}()
	}
	a, b := <-c, <-c
	if a == b && a != 0 {
		return a
	}
	return 0
}

// Get returns the current goroutine's runtime id.
func Get() int64 {
	goidProbe.once.Do(func() { goidProbe.offset = probeGoidOffset() })
	if goidProbe.offset == 0 {
		return slow()
	}
	return readGoid(getg(), goidProbe.offset)
}
