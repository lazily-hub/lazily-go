//go:build linux

package lazily

// POSIX shared-memory blob backend (linux) — the genuine cross-process backend
// for the zero-copy transport (#lzzcpy). Rust reference: the `shm` module in
// lazily-rs/src/transport.rs (gated behind the `shm` feature).
//
// A ShmBackend is a fixed-capacity shm_open + mmap region with an atomic bump
// allocator. On Linux a POSIX shared-memory object is a file under /dev/shm, so
// the region is created with Open + Ftruncate and mapped MAP_SHARED; a distinct
// process (or a second handle) opens the same name and maps the same pages, so a
// descriptor minted by the creator resolves zero-copy against the opener's
// mapping — the bytes are read in place from shared memory, never copied across
// the process boundary.
//
// The header counters (bump / generation / epoch) live inside the mapped region
// and are updated with address-free atomics, so concurrent multi-writer use
// across mappings is lock-free. Entries are immutable once published (Write
// never rewrites a slot), satisfying the BlobBackend stable-address contract.
//
// Limitations: no reclamation (bumps until capacity, then BlobTooLarge); Linux
// only. A managed region with reclamation plugs in behind the same BlobBackend
// interface.

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	shmMagic     uint64 = 0x4c5a5348424c4f42 // "LZSHBLOB"
	shmHeaderLen        = 40                 // magic, capacity, bump, generation, epoch (5 × u64)
	shmSlotLen          = 24                 // per-slot header: generation, len, checksum (3 × u64)

	shmOffMagic      = 0
	shmOffCapacity   = 8
	shmOffBump       = 16
	shmOffGeneration = 24
	shmOffEpoch      = 32
)

// ShmBackend is a POSIX shared-memory blob backend backed by a named /dev/shm
// region. It implements BlobBackend with cross-process resolution: a descriptor
// minted by Create's handle resolves against any Open handle mapping the same
// region. Not safe to use after Close.
type ShmBackend struct {
	name   string
	fd     int
	region []byte
}

// shmPath maps a POSIX shm name ("foo" or "/foo") to its /dev/shm file path.
func shmPath(name string) string {
	for len(name) > 0 && name[0] == '/' {
		name = name[1:]
	}
	return "/dev/shm/" + name
}

func shmHeaderU64(region []byte, off int) *int64 {
	return (*int64)(unsafe.Pointer(&region[off]))
}

// CreateShmBackend creates (or truncates) a named POSIX shared-memory region of
// capacity bytes and maps it MAP_SHARED. The caller owns unlink timing — call
// UnlinkShmBackend(name) once no further readers/writers remain.
func CreateShmBackend(name string, capacity int) (*ShmBackend, error) {
	if capacity <= shmHeaderLen+shmSlotLen {
		return nil, fmt.Errorf("shm capacity %d too small (need > %d)", capacity, shmHeaderLen+shmSlotLen)
	}
	path := shmPath(name)
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CREAT, 0o600)
	if err != nil {
		return nil, fmt.Errorf("shm_open create %s: %w", path, err)
	}
	if err := syscall.Ftruncate(fd, int64(capacity)); err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("ftruncate %s: %w", path, err)
	}
	region, err := syscall.Mmap(fd, 0, capacity, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("mmap %s: %w", path, err)
	}
	b := &ShmBackend{name: name, fd: fd, region: region}
	atomic.StoreInt64(shmHeaderU64(region, shmOffMagic), int64(shmMagic))
	atomic.StoreInt64(shmHeaderU64(region, shmOffCapacity), int64(capacity))
	atomic.StoreInt64(shmHeaderU64(region, shmOffBump), int64(shmHeaderLen))
	atomic.StoreInt64(shmHeaderU64(region, shmOffGeneration), 0)
	atomic.StoreInt64(shmHeaderU64(region, shmOffEpoch), 0)
	return b, nil
}

// OpenShmBackend opens (without creating) an existing named POSIX
// shared-memory region and maps it at the capacity recorded in its header. A
// distinct process uses this to resolve descriptors minted by the creator.
func OpenShmBackend(name string) (*ShmBackend, error) {
	path := shmPath(name)
	fd, err := syscall.Open(path, syscall.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("shm_open %s: %w", path, err)
	}
	// Map the header first to read the real capacity, then remap the whole region.
	probe, err := syscall.Mmap(fd, 0, shmHeaderLen, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("mmap header %s: %w", path, err)
	}
	if uint64(atomic.LoadInt64(shmHeaderU64(probe, shmOffMagic))) != shmMagic {
		capacity := atomic.LoadInt64(shmHeaderU64(probe, shmOffCapacity))
		_ = capacity
		syscall.Munmap(probe)
		syscall.Close(fd)
		return nil, fmt.Errorf("shm region %s has a bad magic header", path)
	}
	capacity := int(atomic.LoadInt64(shmHeaderU64(probe, shmOffCapacity)))
	syscall.Munmap(probe)
	if capacity <= shmHeaderLen {
		syscall.Close(fd)
		return nil, fmt.Errorf("shm region %s reports invalid capacity %d", path, capacity)
	}
	region, err := syscall.Mmap(fd, 0, capacity, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("mmap %s: %w", path, err)
	}
	return &ShmBackend{name: name, fd: fd, region: region}, nil
}

// UnlinkShmBackend removes the named region so it is reclaimed once all mappings
// are unmapped. It is a no-op if the region does not exist.
func UnlinkShmBackend(name string) {
	_ = syscall.Unlink(shmPath(name))
}

// Close unmaps the region and closes the descriptor. It does not unlink the
// name; call UnlinkShmBackend for that.
func (b *ShmBackend) Close() error {
	var err error
	if b.region != nil {
		err = syscall.Munmap(b.region)
		b.region = nil
	}
	if b.fd >= 0 {
		if cerr := syscall.Close(b.fd); err == nil {
			err = cerr
		}
		b.fd = -1
	}
	return err
}

// Capacity returns the region's total byte capacity.
func (b *ShmBackend) Capacity() int { return len(b.region) }

// Kind reports BackendShm.
func (b *ShmBackend) Kind() BlobBackendKind { return BackendShm }

// Epoch returns the backend's current validity epoch.
func (b *ShmBackend) Epoch() int64 {
	return atomic.LoadInt64(shmHeaderU64(b.region, shmOffEpoch))
}

// Write bump-allocates a slot, copies bytes into shared memory, and returns a
// descriptor tagged BackendShm. The bump/generation counters are advanced with
// atomics, so concurrent writers across mappings never overlap.
func (b *ShmBackend) Write(bytes []byte) (ShmBlobRef, error) {
	need := shmSlotLen + len(bytes)
	off := atomic.AddInt64(shmHeaderU64(b.region, shmOffBump), int64(need)) - int64(need)
	if int(off)+need > len(b.region) {
		return ShmBlobRef{}, fmt.Errorf("shm blob too large: need %d at offset %d, capacity %d", need, off, len(b.region))
	}
	generation := atomic.AddInt64(shmHeaderU64(b.region, shmOffGeneration), 1)
	epoch := atomic.LoadInt64(shmHeaderU64(b.region, shmOffEpoch))
	csum := fnv1a(bytes)
	slot := b.region[off : off+int64(shmSlotLen)]
	binary.LittleEndian.PutUint64(slot[0:8], uint64(generation))
	binary.LittleEndian.PutUint64(slot[8:16], uint64(len(bytes)))
	binary.LittleEndian.PutUint64(slot[16:24], uint64(csum))
	copy(b.region[off+int64(shmSlotLen):], bytes)
	return ShmBlobRef{
		Offset:     off + int64(shmSlotLen),
		Len:        int64(len(bytes)),
		Generation: generation,
		Epoch:      epoch,
		Checksum:   csum,
		Backend:    BackendShm,
	}, nil
}

// ReadView resolves a descriptor zero-copy against the shared region: it returns
// a slice aliasing the mapped bytes iff the slot header's generation / len /
// checksum and the region's current epoch all match; (nil, false) otherwise.
func (b *ShmBackend) ReadView(descriptor ShmBlobRef) ([]byte, bool) {
	off := descriptor.Offset
	slotOff := off - int64(shmSlotLen)
	if slotOff < int64(shmHeaderLen) || off+descriptor.Len > int64(len(b.region)) {
		return nil, false
	}
	slot := b.region[slotOff:off]
	if int64(binary.LittleEndian.Uint64(slot[0:8])) != descriptor.Generation {
		return nil, false
	}
	if int64(binary.LittleEndian.Uint64(slot[8:16])) != descriptor.Len {
		return nil, false
	}
	if int64(binary.LittleEndian.Uint64(slot[16:24])) != descriptor.Checksum {
		return nil, false
	}
	if atomic.LoadInt64(shmHeaderU64(b.region, shmOffEpoch)) != descriptor.Epoch {
		return nil, false
	}
	return b.region[off : off+descriptor.Len], true
}

// AdvanceEpoch advances the region's validity epoch, invalidating every prior
// descriptor across all mappings.
func (b *ShmBackend) AdvanceEpoch() {
	atomic.AddInt64(shmHeaderU64(b.region, shmOffEpoch), 1)
}
