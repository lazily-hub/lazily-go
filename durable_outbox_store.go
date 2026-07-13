package lazily

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// OutboxStore is dumb ordered byte storage for the durable outbox protocol.
// Serialization, cursor monotonicity, pruning, and replay ordering belong to
// DurableStoreOutbox; adapters implement only these five operations.
type OutboxStore interface {
	Put(epoch Epoch, frame []byte)
	DeleteThrough(epoch Epoch)
	ScanAfter(cursor Epoch) []StoredOutboxEntry
	LoadCursor() Epoch
	SaveCursor(epoch Epoch)
}

// StoredOutboxEntry is one serialized frame returned by an OutboxStore.
type StoredOutboxEntry struct {
	Epoch Epoch
	Frame []byte
}

// DurableStoreOutbox is Go's storage-independent outbox. The longer name avoids
// colliding with RelayCell's established Outbox role facade.
type DurableStoreOutbox[S OutboxStore] struct {
	store        S
	ackedThrough Epoch
	err          error
}

// NewDurableStoreOutbox loads the durable cursor from store.
func NewDurableStoreOutbox[S OutboxStore](store S) *DurableStoreOutbox[S] {
	return &DurableStoreOutbox[S]{store: store, ackedThrough: store.LoadCursor()}
}

// Store returns the byte adapter owned by the shared protocol.
func (o *DurableStoreOutbox[S]) Store() S { return o.store }

// AckedThrough returns the highest loaded or observed peer acknowledgement.
func (o *DurableStoreOutbox[S]) AckedThrough() Epoch {
	persisted := o.store.LoadCursor()
	if persisted > o.ackedThrough {
		o.ackedThrough = persisted
	}
	return o.ackedThrough
}

// Err returns the most recent frame serialization/decoding error.
func (o *DurableStoreOutbox[S]) Err() error { return o.err }

// Append serializes and stores a frame before transport send.
func (o *DurableStoreOutbox[S]) Append(epoch Epoch, msg IpcMessage) {
	frame, err := msg.MarshalJSON()
	if err != nil {
		o.err = err
		return
	}
	o.store.Put(epoch, frame)
}

// AckThrough advances the monotonic cursor and prunes the acknowledged prefix.
func (o *DurableStoreOutbox[S]) AckThrough(epoch Epoch) {
	target := o.AckedThrough()
	if epoch > target {
		target = epoch
	}
	if target > o.ackedThrough {
		o.store.SaveCursor(target)
		o.ackedThrough = target
	}
	o.store.DeleteThrough(target)
}

// ReplayFrom returns decoded frames after both the caller and durable cursors.
func (o *DurableStoreOutbox[S]) ReplayFrom(cursor Epoch) []OutboxEntry {
	if ackedThrough := o.AckedThrough(); ackedThrough > cursor {
		cursor = ackedThrough
	}
	out := make([]OutboxEntry, 0)
	for _, stored := range o.store.ScanAfter(cursor) {
		msg, err := DecodeIpcMessageJSON(stored.Frame)
		if err != nil {
			o.err = err
			continue
		}
		out = append(out, OutboxEntry{Epoch: stored.Epoch, Msg: msg})
	}
	return out
}

// RetainedEpochs lists the unacknowledged suffix in ascending order.
func (o *DurableStoreOutbox[S]) RetainedEpochs() []Epoch {
	stored := o.store.ScanAfter(o.AckedThrough())
	out := make([]Epoch, len(stored))
	for i, entry := range stored {
		out[i] = entry.Epoch
	}
	return out
}

// InMemoryStore is an ordered process-local OutboxStore.
type InMemoryStore struct {
	entries map[Epoch][]byte
	cursor  Epoch
}

// NewInMemoryStore returns an empty byte store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{entries: map[Epoch][]byte{}}
}

func (s *InMemoryStore) Put(epoch Epoch, frame []byte) {
	s.entries[epoch] = append([]byte(nil), frame...)
}

func (s *InMemoryStore) DeleteThrough(epoch Epoch) {
	for stored := range s.entries {
		if stored <= epoch {
			delete(s.entries, stored)
		}
	}
}

func (s *InMemoryStore) ScanAfter(cursor Epoch) []StoredOutboxEntry {
	epochs := make([]Epoch, 0, len(s.entries))
	for epoch := range s.entries {
		if epoch > cursor {
			epochs = append(epochs, epoch)
		}
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	out := make([]StoredOutboxEntry, 0, len(epochs))
	for _, epoch := range epochs {
		out = append(out, StoredOutboxEntry{Epoch: epoch, Frame: append([]byte(nil), s.entries[epoch]...)})
	}
	return out
}

func (s *InMemoryStore) LoadCursor() Epoch { return s.cursor }

func (s *InMemoryStore) SaveCursor(epoch Epoch) {
	if epoch > s.cursor {
		s.cursor = epoch
	}
}

// InMemoryOutbox preserves the established default constructor and API while
// delegating all protocol behavior to DurableStoreOutbox.
type InMemoryOutbox struct {
	*DurableStoreOutbox[*InMemoryStore]
}

// NewInMemoryOutbox returns an empty outbox.
func NewInMemoryOutbox() *InMemoryOutbox {
	return &InMemoryOutbox{NewDurableStoreOutbox(NewInMemoryStore())}
}

type fileOutboxRecord struct {
	Op    string `json:"op"`
	Epoch Epoch  `json:"epoch"`
	Frame []byte `json:"frame,omitempty"`
}

// FileOutboxStore is a durable append-only journal adapter. Cursor records are
// folded with max, rather than overwritten, so stale writers cannot regress the
// persisted acknowledgement. O_APPEND also gives separate handles one serialized
// record boundary without a shared in-memory lock.
type FileOutboxStore struct {
	path string
	mu   sync.Mutex
	err  error
}

// NewFileOutboxStore opens (or creates) an append-only outbox journal.
func NewFileOutboxStore(path string) (*FileOutboxStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	err = file.Close()
	if err != nil {
		return nil, err
	}
	return &FileOutboxStore{path: path}, nil
}

// Err returns the latest journal I/O or decoding error.
func (s *FileOutboxStore) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *FileOutboxStore) appendRecord(record fileOutboxRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	frame, err := json.Marshal(record)
	if err != nil {
		s.err = err
		return
	}
	frame = append(frame, '\n')
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err == nil {
		_, err = file.Write(frame)
	}
	if err == nil {
		err = file.Sync()
	}
	if file != nil {
		err = errors.Join(err, file.Close())
	}
	s.err = err
}

func (s *FileOutboxStore) records() []fileOutboxRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.Open(s.path)
	if err != nil {
		s.err = err
		return nil
	}
	defer file.Close()
	var records []fileOutboxRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record fileOutboxRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			// A crash may leave one incomplete trailing record; earlier synced
			// records remain authoritative and replayable.
			continue
		}
		records = append(records, record)
	}
	s.err = scanner.Err()
	return records
}

func (s *FileOutboxStore) Put(epoch Epoch, frame []byte) {
	s.appendRecord(fileOutboxRecord{Op: "put", Epoch: epoch, Frame: frame})
}

func (s *FileOutboxStore) DeleteThrough(epoch Epoch) {
	s.appendRecord(fileOutboxRecord{Op: "delete", Epoch: epoch})
}

func (s *FileOutboxStore) ScanAfter(cursor Epoch) []StoredOutboxEntry {
	entries := map[Epoch][]byte{}
	var deletedThrough Epoch
	for _, record := range s.records() {
		switch record.Op {
		case "put":
			entries[record.Epoch] = append([]byte(nil), record.Frame...)
		case "delete":
			if record.Epoch > deletedThrough {
				deletedThrough = record.Epoch
			}
		}
	}
	if deletedThrough > cursor {
		cursor = deletedThrough
	}
	epochs := make([]Epoch, 0, len(entries))
	for epoch := range entries {
		if epoch > cursor {
			epochs = append(epochs, epoch)
		}
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	out := make([]StoredOutboxEntry, 0, len(epochs))
	for _, epoch := range epochs {
		out = append(out, StoredOutboxEntry{Epoch: epoch, Frame: entries[epoch]})
	}
	return out
}

func (s *FileOutboxStore) LoadCursor() Epoch {
	var cursor Epoch
	for _, record := range s.records() {
		if record.Op == "cursor" && record.Epoch > cursor {
			cursor = record.Epoch
		}
	}
	return cursor
}

func (s *FileOutboxStore) SaveCursor(epoch Epoch) {
	s.appendRecord(fileOutboxRecord{Op: "cursor", Epoch: epoch})
}

// FileOutbox is the ready-to-use durable filesystem adapter.
type FileOutbox struct {
	*DurableStoreOutbox[*FileOutboxStore]
}

// NewFileOutbox opens a durable outbox journal.
func NewFileOutbox(path string) (*FileOutbox, error) {
	store, err := NewFileOutboxStore(path)
	if err != nil {
		return nil, err
	}
	return &FileOutbox{NewDurableStoreOutbox(store)}, nil
}

var _ DurableOutbox = (*InMemoryOutbox)(nil)
var _ DurableOutbox = (*FileOutbox)(nil)
