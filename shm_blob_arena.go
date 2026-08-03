package lazily

// Shared-memory blob arena — in-process blob storage with header validation.
//
// A Go port of lazily-dart's `lib/src/shm_blob_arena.dart`, conformant with
// lazily-spec (protocol.md § Shared-memory IPC / § Shared-memory payload path).
// The arena writes a fixed header before each payload — { generation, epoch,
// length, checksum } — and readers validate that header before accepting a
// ShmBlobRef descriptor. The large-payload path carries a ShmBlobRef
// (NodeStateSharedBlob / IpcValueSharedBlob in ipc.go) instead of inline bytes.
//
// Unlike the JS/Dart bindings (isolate/Worker model — no shared address space,
// so `shared_memory: partial` and large payloads fall back to Inline), Go hosts
// a genuine in-process arena: goroutines share the process address space, so a
// descriptor handed to any goroutine resolves against the same backing store.
// The backing store here is a plain byte-arena (a slice of entries) rather than
// an OS mmap/shm segment — that matches the Dart semantics while still exposing
// the ShmBlobRef indirection callers see for a real shared-memory arena.
//
// Beyond the faithful Dart surface (Write/Read/Update/AdvanceEpoch), this port
// adds genuine-arena refcount/free semantics (Retain/Free): a written blob
// starts with one reference; Free drops a reference and reclaims the slot when
// the count reaches zero, after which the descriptor is stale. When no caller
// ever calls Retain/Free the behavior is identical to the Dart original.
//
// The arena is safe for concurrent use (a real shared-memory arena is shared
// across goroutines); the Dart original was single-isolate and unsynchronized.

import (
	"encoding/binary"
	"fmt"
	"sync"
)

// ShmBlobHeaderLen is the byte length of the canonical in-process LZSH arena
// header: magic (u32), version (u16), header length (u16), generation, epoch,
// payload length, and checksum (four u64 values), all little-endian.
const ShmBlobHeaderLen = 40

const (
	shmBlobMagic   uint32 = 0x4c5a5348 // "LZSH" when rendered big-endian
	shmBlobVersion uint16 = 1
)

// shmArenaEntry is a blob arena entry: header fields + payload. Mirrors the
// Dart `_ArenaEntry`, extended with refCount for the free path.
type shmArenaEntry struct {
	generation int64
	epoch      int64
	payload    []byte
	refCount   int
}

// checksum is the FNV-1a hash of the payload (the header's checksum field).
func (e *shmArenaEntry) checksum() int64 { return fnv1a(e.payload) }

// toRef builds the descriptor for this entry at the given arena offset.
func (e *shmArenaEntry) toRef(offset int64) ShmBlobRef {
	return ShmBlobRef{
		Offset:     offset,
		Len:        int64(len(e.payload)),
		Generation: e.generation,
		Epoch:      e.epoch,
		Checksum:   e.checksum(),
	}
}

// ShmBlobArena is an in-process shared-memory blob arena. It manages blob
// storage with generation/epoch tracking and header validation, handing out
// ShmBlobRef descriptors that callers exchange over the control transport in
// place of large inline payloads.
//
// The zero value is not usable; construct one with NewShmBlobArena.
type ShmBlobArena struct {
	mu         sync.Mutex
	epoch      int64
	entries    []*shmArenaEntry // offset == index; a freed slot is nil
	generation int64

	// hostBytes enables the fixed-capacity, byte-compatible arena host created
	// by NewShmBlobArenaWithCapacity. The existing entry arena remains the
	// default used by Write/Read/Update/Retain/Free.
	hostBytes          []byte
	hostWriteOffset    int
	hostNextGeneration uint64
}

// NewShmBlobArena creates an empty arena starting at the given epoch (Dart's
// `ShmBlobArena({this.epoch = 0})`; pass 0 for the default).
func NewShmBlobArena(epoch int64) *ShmBlobArena {
	return &ShmBlobArena{epoch: epoch}
}

// NewShmBlobArenaWithCapacity creates the canonical fixed-capacity LZSH arena
// host used by lazily-spec/conformance/arena_blob.json. WriteBlob writes the
// 40-byte header immediately before each payload and returns a descriptor whose
// offset points at that header. When the remaining tail cannot hold a write,
// the cursor wraps to zero and the new generation makes overwritten
// descriptors stale.
func NewShmBlobArenaWithCapacity(capacity int) (*ShmBlobArena, error) {
	if capacity < ShmBlobHeaderLen+1 {
		return nil, fmt.Errorf("arena capacity %d < minimum %d", capacity, ShmBlobHeaderLen+1)
	}
	return &ShmBlobArena{
		hostBytes:          make([]byte, capacity),
		hostNextGeneration: 1,
	}, nil
}

// Capacity returns the fixed host arena's backing capacity, or zero for the
// entry-oriented arena created by NewShmBlobArena.
func (a *ShmBlobArena) Capacity() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.hostBytes)
}

// Bytes returns the fixed host arena's backing bytes. The returned slice aliases
// arena storage so a transport can expose the shared region without copying.
// Callers must not mutate the header or payload bytes.
func (a *ShmBlobArena) Bytes() []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.hostBytes
}

// WriteBlob writes one payload into the canonical fixed-capacity LZSH arena.
// The epoch is supplied per write because it is part of the serialized header.
func (a *ShmBlobArena) WriteBlob(epoch int64, payload []byte) (ShmBlobRef, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.hostBytes) == 0 {
		return ShmBlobRef{}, fmt.Errorf("WriteBlob requires NewShmBlobArenaWithCapacity")
	}
	if len(payload) > len(a.hostBytes)-ShmBlobHeaderLen {
		return ShmBlobRef{}, fmt.Errorf(
			"payload %d bytes exceeds arena max %d",
			len(payload), len(a.hostBytes)-ShmBlobHeaderLen,
		)
	}
	total := ShmBlobHeaderLen + len(payload)
	if a.hostWriteOffset+total > len(a.hostBytes) {
		a.hostWriteOffset = 0
	}
	generation := a.hostNextGeneration
	a.hostNextGeneration++
	if a.hostNextGeneration == 0 {
		return ShmBlobRef{}, fmt.Errorf("arena generation counter overflowed")
	}

	offset := a.hostWriteOffset
	checksum := uint64(fnv1a(payload))
	ref := ShmBlobRef{
		Offset:     int64(offset),
		Len:        int64(len(payload)),
		Generation: int64(generation),
		Epoch:      epoch,
		Checksum:   int64(checksum),
	}
	header := a.hostBytes[offset : offset+ShmBlobHeaderLen]
	binary.LittleEndian.PutUint32(header[0:4], shmBlobMagic)
	binary.LittleEndian.PutUint16(header[4:6], shmBlobVersion)
	binary.LittleEndian.PutUint16(header[6:8], ShmBlobHeaderLen)
	binary.LittleEndian.PutUint64(header[8:16], generation)
	binary.LittleEndian.PutUint64(header[16:24], uint64(epoch))
	binary.LittleEndian.PutUint64(header[24:32], uint64(len(payload)))
	binary.LittleEndian.PutUint64(header[32:40], checksum)
	copy(a.hostBytes[offset+ShmBlobHeaderLen:offset+total], payload)
	a.hostWriteOffset += total
	if a.hostWriteOffset == len(a.hostBytes) {
		a.hostWriteOffset = 0
	}
	return ref, nil
}

// ReadBlob validates a canonical LZSH header and returns a defensive copy of
// its payload.
func (a *ShmBlobArena) ReadBlob(ref ShmBlobRef) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.hostBytes) == 0 {
		return nil, fmt.Errorf("ReadBlob requires NewShmBlobArenaWithCapacity")
	}
	if ref.Offset < 0 || ref.Len < 0 ||
		ref.Offset+ShmBlobHeaderLen+ref.Len > int64(len(a.hostBytes)) {
		return nil, fmt.Errorf("descriptor offset=%d len=%d outside arena capacity=%d",
			ref.Offset, ref.Len, len(a.hostBytes))
	}
	offset := int(ref.Offset)
	header := a.hostBytes[offset : offset+ShmBlobHeaderLen]
	if binary.LittleEndian.Uint32(header[0:4]) != shmBlobMagic {
		return nil, fmt.Errorf("arena descriptor/header mismatch: magic")
	}
	if binary.LittleEndian.Uint16(header[4:6]) != shmBlobVersion {
		return nil, fmt.Errorf("arena descriptor/header mismatch: version")
	}
	if binary.LittleEndian.Uint16(header[6:8]) != ShmBlobHeaderLen {
		return nil, fmt.Errorf("arena descriptor/header mismatch: header_len")
	}
	if int64(binary.LittleEndian.Uint64(header[8:16])) != ref.Generation {
		return nil, fmt.Errorf("arena descriptor/header mismatch: generation")
	}
	if int64(binary.LittleEndian.Uint64(header[16:24])) != ref.Epoch {
		return nil, fmt.Errorf("arena descriptor/header mismatch: epoch")
	}
	if int64(binary.LittleEndian.Uint64(header[24:32])) != ref.Len {
		return nil, fmt.Errorf("arena descriptor/header mismatch: len")
	}
	if int64(binary.LittleEndian.Uint64(header[32:40])) != ref.Checksum {
		return nil, fmt.Errorf("arena descriptor/header mismatch: checksum")
	}
	from := offset + ShmBlobHeaderLen
	payload := append([]byte(nil), a.hostBytes[from:from+int(ref.Len)]...)
	if fnv1a(payload) != ref.Checksum {
		return nil, fmt.Errorf("arena payload checksum mismatch")
	}
	return payload, nil
}

// Epoch returns the arena's current epoch. All descriptors carry the epoch that
// was current when they were minted; AdvanceEpoch invalidates them.
func (a *ShmBlobArena) Epoch() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.epoch
}

// Length returns the number of live (unfreed) stored blobs. With no Free calls
// this equals the number of Write calls, matching the Dart `length` getter.
func (a *ShmBlobArena) Length() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.liveCount()
}

// IsEmpty reports whether the arena holds no live blobs (Dart `isEmpty`).
func (a *ShmBlobArena) IsEmpty() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.liveCount() == 0
}

func (a *ShmBlobArena) liveCount() int {
	n := 0
	for _, e := range a.entries {
		if e != nil {
			n++
		}
	}
	return n
}

// Write allocates a blob and returns its descriptor. The payload bytes are
// copied into the arena; the returned ShmBlobRef is the header a reader
// validates. The blob starts with a reference count of one (mirrors Dart's
// `write`, which has no free path).
func (a *ShmBlobArena) Write(bytes []byte) ShmBlobRef {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.generation++
	payload := make([]byte, len(bytes))
	copy(payload, bytes)
	entry := &shmArenaEntry{
		generation: a.generation,
		epoch:      a.epoch,
		payload:    payload,
		refCount:   1,
	}
	offset := int64(len(a.entries))
	a.entries = append(a.entries, entry)
	return entry.toRef(offset)
}

// Read returns a blob's payload by descriptor, or nil if header validation
// fails (out-of-range offset, freed slot, or a mismatched generation, epoch,
// length, or checksum). The returned slice is a defensive copy. Mirrors Dart
// `read`, which returns null on validation failure.
func (a *ShmBlobArena) Read(ref ShmBlobRef) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry := a.validEntry(ref)
	if entry == nil {
		return nil
	}
	out := make([]byte, len(entry.payload))
	copy(out, entry.payload)
	return out
}

// ReadView resolves a descriptor zero-copy: it returns the arena's own backing
// payload slice (NOT a defensive copy) and ok=true iff the descriptor passes
// full header validation (offset in range, slot live, matching generation /
// epoch / len / checksum); otherwise (nil, false). This is the transport
// read_view primitive — the caller reads the backend's bytes in place. The
// returned slice aliases arena storage; the arena entry is immutable for the
// lifetime a descriptor may reference it (Write never mutates a stored buffer;
// Update/Free bump the generation, invalidating the descriptor), so the view is
// stable. Callers that intend to retain the bytes past a possible Free should
// copy. Contrast Read, which always copies.
func (a *ShmBlobArena) ReadView(ref ShmBlobRef) ([]byte, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry := a.validEntry(ref)
	if entry == nil {
		return nil, false
	}
	return entry.payload, true
}

// Update rewrites an existing blob in place (bumping its generation) and
// returns the new descriptor, or nil if ref is stale. Mirrors Dart `update`:
// the payload buffer is overwritten from offset 0 and keeps its original
// length, so the new descriptor's Len matches the stored buffer, not len(bytes).
// bytes longer than the stored payload are truncated (Go copy semantics)
// instead of raising, keeping the operation recoverable.
func (a *ShmBlobArena) Update(ref ShmBlobRef, bytes []byte) *ShmBlobRef {
	a.mu.Lock()
	defer a.mu.Unlock()
	// Update validates generation + epoch (like the Dart original) but not
	// length/checksum, permitting a same-slot rewrite from a fresh reader.
	if ref.Offset < 0 || ref.Offset >= int64(len(a.entries)) {
		return nil
	}
	entry := a.entries[ref.Offset]
	if entry == nil {
		return nil
	}
	if entry.generation != ref.Generation {
		return nil
	}
	if entry.epoch != ref.Epoch {
		return nil
	}
	a.generation++
	entry.generation = a.generation
	copy(entry.payload, bytes)
	newRef := entry.toRef(ref.Offset)
	return &newRef
}

// Retain increments a live blob's reference count and reports success. It fails
// (returns false) when ref is stale or the slot is freed. This is a
// genuine-arena extension with no Dart equivalent.
func (a *ShmBlobArena) Retain(ref ShmBlobRef) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry := a.validEntry(ref)
	if entry == nil {
		return false
	}
	entry.refCount++
	return true
}

// Free drops one reference to a blob and reports whether the reference was
// released. When the reference count reaches zero the slot is reclaimed (its
// descriptor becomes permanently stale); the offset index is preserved so other
// descriptors keep resolving to the correct slots. Returns false if ref is
// stale or already freed. This is a genuine-arena extension with no Dart
// equivalent (Dart's arena never frees).
func (a *ShmBlobArena) Free(ref ShmBlobRef) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry := a.validEntry(ref)
	if entry == nil {
		return false
	}
	entry.refCount--
	if entry.refCount <= 0 {
		a.entries[ref.Offset] = nil
	}
	return true
}

// AdvanceEpoch bumps the arena epoch and restamps every live entry, so all
// previously-minted descriptors become stale (their Epoch no longer matches).
// Mirrors Dart `advanceEpoch`.
func (a *ShmBlobArena) AdvanceEpoch() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.epoch++
	for _, e := range a.entries {
		if e != nil {
			e.epoch = a.epoch
		}
	}
}

// validEntry returns the entry ref resolves to only if it passes full header
// validation (offset in range, slot live, matching generation/epoch/len/
// checksum); otherwise nil. Callers must hold a.mu.
func (a *ShmBlobArena) validEntry(ref ShmBlobRef) *shmArenaEntry {
	if ref.Offset < 0 || ref.Offset >= int64(len(a.entries)) {
		return nil
	}
	entry := a.entries[ref.Offset]
	if entry == nil {
		return nil
	}
	if entry.generation != ref.Generation {
		return nil
	}
	if entry.epoch != ref.Epoch {
		return nil
	}
	if int64(len(entry.payload)) != ref.Len {
		return nil
	}
	if entry.checksum() != ref.Checksum {
		return nil
	}
	return entry
}

// ValidateBlobRef validates a ShmBlobRef descriptor against expected bounds:
// all header fields must be non-negative, and Len must not exceed maxLen when
// maxLen is non-nil. Mirrors Dart `validateBlobRef({int? maxLen})`.
func ValidateBlobRef(ref ShmBlobRef, maxLen *int64) bool {
	if ref.Offset < 0 {
		return false
	}
	if ref.Len < 0 {
		return false
	}
	if ref.Generation < 0 {
		return false
	}
	if ref.Epoch < 0 {
		return false
	}
	if ref.Checksum < 0 {
		return false
	}
	if maxLen != nil && ref.Len > *maxLen {
		return false
	}
	return true
}

// fnv1a is the 64-bit FNV-1a hash of the payload, matching the Dart `_fnv1a`
// used for the arena header checksum. The Dart implementation accumulates in a
// 64-bit signed int with wraparound; Go computes the same in uint64 and
// reinterprets the result as int64 (two's-complement), so the high-bit-set
// hashes land on the identical signed value and Checksum comparisons agree.
func fnv1a(bytes []byte) int64 {
	const (
		offset uint64 = 0xcbf29ce484222325
		prime  uint64 = 0x100000001b3
	)
	hash := offset
	for _, b := range bytes {
		hash ^= uint64(b)
		hash *= prime
	}
	return int64(hash)
}
