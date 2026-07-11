//go:build linux

package lazily

// POSIX shared-memory backend conformance (linux). Mirrors the Rust
// shm_backend_round_trip / shm_backend_cross_process tests: cross-mapping
// resolution is proven by opening the same /dev/shm region from a second,
// independent handle (each mmap is a distinct mapping of the same shared pages —
// exactly what two processes get) and resolving a descriptor minted by the first.

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func shmTestName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("/lazily_go_shm_test_%d_%s", os.Getpid(), t.Name())
}

func TestShmBackendRoundTrip(t *testing.T) {
	name := shmTestName(t)
	UnlinkShmBackend(name)
	b, err := CreateShmBackend(name, 1<<20)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { b.Close(); UnlinkShmBackend(name) }()

	payload := make([]byte, 1000)
	for i := range payload {
		payload[i] = byte(i*7 + 1)
	}
	desc, err := b.Write(payload)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if desc.Backend != BackendShm {
		t.Fatalf("backend = %q, want shm", desc.Backend)
	}
	if v, ok := b.ReadView(desc); !ok || !bytes.Equal(v, payload) {
		t.Fatalf("round-trip mismatch (ok=%v)", ok)
	}
	// epoch advance invalidates.
	b.AdvanceEpoch()
	if _, ok := b.ReadView(desc); ok {
		t.Fatalf("descriptor resolved after epoch advance")
	}
}

// Cross-mapping resolution: a descriptor minted by the creating handle resolves
// zero-copy against an independent Open handle mapping the same region — the
// genuine cross-process property (distinct mappings, shared pages).
func TestShmBackendCrossMapping(t *testing.T) {
	name := shmTestName(t)
	UnlinkShmBackend(name)
	payload := make([]byte, 1000)
	for i := range payload {
		payload[i] = byte(i*7 + 1)
	}

	parent, err := CreateShmBackend(name, 1<<20)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { parent.Close(); UnlinkShmBackend(name) }()
	desc, err := parent.Write(payload)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	child, err := OpenShmBackend(name)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer child.Close()
	if v, ok := child.ReadView(desc); !ok || !bytes.Equal(v, payload) {
		t.Fatalf("cross-mapping resolve mismatch (ok=%v)", ok)
	}

	// A descriptor the child mints is likewise resolvable by the parent.
	childPayload := []byte("written from the second mapping")
	desc2, err := child.Write(childPayload)
	if err != nil {
		t.Fatalf("child write: %v", err)
	}
	if v, ok := parent.ReadView(desc2); !ok || !bytes.Equal(v, childPayload) {
		t.Fatalf("parent could not resolve child-minted descriptor (ok=%v)", ok)
	}
}

// A ShmBackend routes as a shm-kind backend in a BlobRouter.
func TestShmBackendRouting(t *testing.T) {
	name := shmTestName(t)
	UnlinkShmBackend(name)
	b, err := CreateShmBackend(name, 1<<20)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { b.Close(); UnlinkShmBackend(name) }()

	desc, _ := b.Write([]byte("shm routed payload"))
	router := NewBlobRouter().Register(b).Register(NewArrowBackend())
	if v, ok := router.ReadView(desc); !ok || !bytes.Equal(v, []byte("shm routed payload")) {
		t.Fatalf("shm routing failed (ok=%v)", ok)
	}
}
