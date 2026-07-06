package lazily

// The C-ABI FFI boundary (protocol.md § FFI Boundary, schemas/ffi.json).
//
// A Go port of lazily-dart's `lib/src/ffi.dart`, mirroring `lazily-rs/src/ffi.rs`
// and `lazily-zig/src/lazily/ffi.zig`. Go is a compiled native runtime with a
// shared in-process address space, so this binding declares `ffi = full`: the
// language-agnostic FFI value types live here in pure Go (buildable with or
// without cgo), and the real `//export` C-ABI adapter lives in `ffi_cgo.go`
// (guarded by `//go:build cgo`). `ffi_nocgo.go` supplies the non-cgo companion
// so `go build` succeeds under both `CGO_ENABLED=1` and `CGO_ENABLED=0`.
//
// A frame is *just serialized `IpcMessage` bytes* — there is no custom header
// and no separate discriminant byte. The message kind is derived by *decoding*
// the frame and matching on the variant (see LazilyFfiKindJSON). A channel
// accepts a frame, decodes it as IpcMessage, and re-encodes canonical JSON
// bytes (the "decode + re-encode canonical JSON" contract pin).
//
// LazilyFfiMessageKind includes CrdtSync = 3 per the protocol's normative
// requirement: the FFI message kind discriminant MUST include CrdtSync (the
// multi-writer plane rides the same transport).

import "sync"

// ---------------------------------------------------------------------------
// LazilyFfiStatus (schemas/ffi.json#/$defs/LazilyFfiStatus)
// ---------------------------------------------------------------------------

// LazilyFfiStatus is the FFI operation status code. Errors return one of the
// non-zero codes; recovered panics surface as LazilyFfiStatusPanic before
// crossing the C ABI. The integer values are the normative C-ABI wire
// discriminants (0..5).
type LazilyFfiStatus int

const (
	// LazilyFfiStatusOk is success.
	LazilyFfiStatusOk LazilyFfiStatus = 0
	// LazilyFfiStatusEmpty means no message was available (empty channel read).
	LazilyFfiStatusEmpty LazilyFfiStatus = 1
	// LazilyFfiStatusNullPointer means a required pointer argument was null.
	LazilyFfiStatusNullPointer LazilyFfiStatus = 2
	// LazilyFfiStatusInvalidMessage means the frame did not decode as a valid
	// IpcMessage.
	LazilyFfiStatusInvalidMessage LazilyFfiStatus = 3
	// LazilyFfiStatusEncodeFailed means the frame decoded but could not be
	// re-encoded as canonical bytes.
	LazilyFfiStatusEncodeFailed LazilyFfiStatus = 4
	// LazilyFfiStatusPanic means a panic was caught before crossing the C ABI.
	LazilyFfiStatusPanic LazilyFfiStatus = 5
)

// IsOk reports whether this status represents success.
func (s LazilyFfiStatus) IsOk() bool { return s == LazilyFfiStatusOk }

// LazilyFfiStatusFromCode decodes the integer discriminant, returning ok=false
// for an unknown value (mirrors the Dart/Rust enum's strictness on
// out-of-range discriminants).
func LazilyFfiStatusFromCode(code int) (LazilyFfiStatus, bool) {
	switch LazilyFfiStatus(code) {
	case LazilyFfiStatusOk, LazilyFfiStatusEmpty, LazilyFfiStatusNullPointer,
		LazilyFfiStatusInvalidMessage, LazilyFfiStatusEncodeFailed, LazilyFfiStatusPanic:
		return LazilyFfiStatus(code), true
	default:
		return LazilyFfiStatusOk, false
	}
}

// ---------------------------------------------------------------------------
// LazilyFfiMessageKind (schemas/ffi.json#/$defs/LazilyFfiMessageKind)
// ---------------------------------------------------------------------------

// LazilyFfiMessageKind is the IPC message kind discriminant, derived by
// decoding a frame as IpcMessage and matching on the variant. CrdtSync = 3 is
// normative.
type LazilyFfiMessageKind int

const (
	// LazilyFfiMessageKindUnknown is the unknown / unset zero value.
	LazilyFfiMessageKindUnknown LazilyFfiMessageKind = 0
	// LazilyFfiMessageKindSnapshot classifies an IpcMessageSnapshot.
	LazilyFfiMessageKindSnapshot LazilyFfiMessageKind = 1
	// LazilyFfiMessageKindDelta classifies an IpcMessageDelta.
	LazilyFfiMessageKindDelta LazilyFfiMessageKind = 2
	// LazilyFfiMessageKindCrdtSync classifies an IpcMessageCrdtSync (the
	// multi-writer CRDT plane).
	LazilyFfiMessageKindCrdtSync LazilyFfiMessageKind = 3
)

// LazilyFfiMessageKindFromCode decodes the integer discriminant, returning
// LazilyFfiMessageKindUnknown for an out-of-range value (matches the C enum's
// zero-default).
func LazilyFfiMessageKindFromCode(code int) LazilyFfiMessageKind {
	switch LazilyFfiMessageKind(code) {
	case LazilyFfiMessageKindSnapshot, LazilyFfiMessageKindDelta, LazilyFfiMessageKindCrdtSync:
		return LazilyFfiMessageKind(code)
	default:
		return LazilyFfiMessageKindUnknown
	}
}

// ---------------------------------------------------------------------------
// LazilyFfiBytes (schemas/ffi.json#/$defs/LazilyFfiBytes)
// ---------------------------------------------------------------------------

// LazilyFfiBytes is an owned byte buffer crossing the FFI boundary. On the real
// C ABI this is `{ uint8_t* ptr; size_t len; }` with explicit allocation
// ownership: the caller owns input bytes; the host owns output buffers until
// the paired free function (`lazily_ffi_bytes_free`) is called. This pure-Go
// mirror carries the bytes inline; the ownership contract is realized in
// ffi_cgo.go, which marshals to/from C-allocated buffers.
type LazilyFfiBytes struct {
	// Bytes is the owned byte buffer.
	Bytes []byte
}

// NewLazilyFfiBytes copies b into a newly-owned LazilyFfiBytes buffer.
func NewLazilyFfiBytes(b []byte) LazilyFfiBytes {
	out := make([]byte, len(b))
	copy(out, b)
	return LazilyFfiBytes{Bytes: out}
}

// LazilyFfiBytesFromOwned wraps an already-owned buffer without copying.
func LazilyFfiBytesFromOwned(b []byte) LazilyFfiBytes { return LazilyFfiBytes{Bytes: b} }

// Len returns the buffer length in bytes.
func (b LazilyFfiBytes) Len() int { return len(b.Bytes) }

// AsJSON decodes the buffer as UTF-8 JSON text.
func (b LazilyFfiBytes) AsJSON() string { return string(b.Bytes) }

// ---------------------------------------------------------------------------
// Classification / clone results
// ---------------------------------------------------------------------------

// LazilyFfiClassification is the result of a frame classification: the status
// plus (on success) the decoded message kind.
type LazilyFfiClassification struct {
	Status LazilyFfiStatus
	Kind   LazilyFfiMessageKind
}

// IsOk reports whether classification succeeded.
func (c LazilyFfiClassification) IsOk() bool { return c.Status.IsOk() }

// LazilyFfiCloneResult is the result of cloning a frame through the channel:
// decode as IpcMessage, then re-encode canonical JSON bytes. Output is set iff
// Status is ok.
type LazilyFfiCloneResult struct {
	Status LazilyFfiStatus
	// Output is the re-encoded canonical JSON bytes (nil unless Status is ok).
	Output *LazilyFfiBytes
}

// ---------------------------------------------------------------------------
// Pure-Go FFI logic (mirrors the C-ABI functions in ffi_cgo.go)
// ---------------------------------------------------------------------------

// LazilyFfiValidateJSON validates a frame: decode the bytes as IpcMessage and
// confirm the result is well-formed. Returns LazilyFfiStatusOk on success.
// Mirrors `lazily_ffi_ipc_message_validate_json`.
func LazilyFfiValidateJSON(frame LazilyFfiBytes) (status LazilyFfiStatus) {
	defer func() {
		if recover() != nil {
			status = LazilyFfiStatusPanic
		}
	}()
	if _, err := DecodeIpcMessageJSON(frame.Bytes); err != nil {
		return LazilyFfiStatusInvalidMessage
	}
	return LazilyFfiStatusOk
}

// LazilyFfiKindJSON classifies a frame: decode it and return the variant kind.
// On a decode failure the status is LazilyFfiStatusInvalidMessage and the kind
// is LazilyFfiMessageKindUnknown. Mirrors `lazily_ffi_ipc_message_kind_json`.
func LazilyFfiKindJSON(frame LazilyFfiBytes) (result LazilyFfiClassification) {
	defer func() {
		if recover() != nil {
			result = LazilyFfiClassification{Status: LazilyFfiStatusPanic, Kind: LazilyFfiMessageKindUnknown}
		}
	}()
	message, err := DecodeIpcMessageJSON(frame.Bytes)
	if err != nil {
		return LazilyFfiClassification{Status: LazilyFfiStatusInvalidMessage, Kind: LazilyFfiMessageKindUnknown}
	}
	return LazilyFfiClassification{Status: LazilyFfiStatusOk, Kind: ipcMessageKind(message)}
}

// LazilyFfiCloneJSON clones a frame: decode the bytes as IpcMessage, then
// re-encode canonical JSON bytes. Mirrors `lazily_ffi_ipc_message_clone_json`.
// This is the contract pin: the channel decodes each accepted frame and
// re-encodes canonical JSON bytes.
func LazilyFfiCloneJSON(frame LazilyFfiBytes) (result LazilyFfiCloneResult) {
	defer func() {
		if recover() != nil {
			result = LazilyFfiCloneResult{Status: LazilyFfiStatusPanic}
		}
	}()
	message, err := DecodeIpcMessageJSON(frame.Bytes)
	if err != nil {
		return LazilyFfiCloneResult{Status: LazilyFfiStatusInvalidMessage}
	}
	reencoded, err := message.EncodeJSON()
	if err != nil {
		return LazilyFfiCloneResult{Status: LazilyFfiStatusEncodeFailed}
	}
	out := LazilyFfiBytesFromOwned(reencoded)
	return LazilyFfiCloneResult{Status: LazilyFfiStatusOk, Output: &out}
}

// ipcMessageKind matches an IpcMessage to its FFI discriminant.
func ipcMessageKind(message IpcMessage) LazilyFfiMessageKind {
	switch message.(type) {
	case IpcMessageSnapshot:
		return LazilyFfiMessageKindSnapshot
	case IpcMessageDelta:
		return LazilyFfiMessageKindDelta
	case IpcMessageCrdtSync:
		return LazilyFfiMessageKindCrdtSync
	default:
		return LazilyFfiMessageKindUnknown
	}
}

// ---------------------------------------------------------------------------
// LazilyFfiChannel — the in-process send -> recv relay
// ---------------------------------------------------------------------------

// LazilyFfiChannel is an in-process FFI message channel that mirrors the C-ABI
// `lazily_ffi_channel_send_json` / `lazily_ffi_channel_recv_json` pair. Each
// accepted frame is decoded as IpcMessage and re-encoded to canonical JSON
// bytes on the way in, so a round-trip exercises the same "decode + re-encode
// canonical JSON" contract regardless of the sender's codec.
//
// It is a local ownership/ABI adapter, not a second graph-state model. Unlike
// the Dart original, this channel is safe for concurrent use (the C-ABI handle
// may be shared across goroutines).
type LazilyFfiChannel struct {
	mu    sync.Mutex
	queue [][]byte
}

// NewLazilyFfiChannel creates an empty channel. Mirrors
// `lazily_ffi_channel_new`.
func NewLazilyFfiChannel() *LazilyFfiChannel { return &LazilyFfiChannel{} }

// Send encodes message to canonical JSON and queues it. Returns
// LazilyFfiStatusOk on success or LazilyFfiStatusEncodeFailed if encoding
// fails. Mirrors `lazily_ffi_channel_send_json` with an already-typed message.
func (c *LazilyFfiChannel) Send(message IpcMessage) LazilyFfiStatus {
	encoded, err := message.EncodeJSON()
	if err != nil {
		return LazilyFfiStatusEncodeFailed
	}
	c.mu.Lock()
	c.queue = append(c.queue, encoded)
	c.mu.Unlock()
	return LazilyFfiStatusOk
}

// SendJSONFrame accepts raw frame bytes, decoding and re-encoding to canonical
// form on the way in so the recv side always sees canonical bytes. Mirrors
// `lazily_ffi_channel_send_json`.
func (c *LazilyFfiChannel) SendJSONFrame(frame LazilyFfiBytes) (status LazilyFfiStatus) {
	defer func() {
		if recover() != nil {
			status = LazilyFfiStatusPanic
		}
	}()
	message, err := DecodeIpcMessageJSON(frame.Bytes)
	if err != nil {
		return LazilyFfiStatusInvalidMessage
	}
	encoded, err := message.EncodeJSON()
	if err != nil {
		return LazilyFfiStatusEncodeFailed
	}
	c.mu.Lock()
	c.queue = append(c.queue, encoded)
	c.mu.Unlock()
	return LazilyFfiStatusOk
}

// Recv dequeues and decodes the next message. Returns LazilyFfiStatusEmpty (and
// a nil message) if the queue is empty. Mirrors `lazily_ffi_channel_recv_json`.
func (c *LazilyFfiChannel) Recv() (IpcMessage, LazilyFfiStatus) {
	frame, status := c.RecvJSONFrame()
	if status != LazilyFfiStatusOk {
		return nil, status
	}
	message, err := DecodeIpcMessageJSON(frame.Bytes)
	if err != nil {
		return nil, LazilyFfiStatusInvalidMessage
	}
	return message, LazilyFfiStatusOk
}

// RecvJSONFrame dequeues the next canonical frame bytes. Returns
// LazilyFfiStatusEmpty when the queue is empty. Mirrors
// `lazily_ffi_channel_recv_json`.
func (c *LazilyFfiChannel) RecvJSONFrame() (LazilyFfiBytes, LazilyFfiStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.queue) == 0 {
		return LazilyFfiBytes{}, LazilyFfiStatusEmpty
	}
	frame := c.queue[0]
	c.queue = c.queue[1:]
	return LazilyFfiBytesFromOwned(frame), LazilyFfiStatusOk
}

// Len returns the number of queued frames. Mirrors `lazily_ffi_channel_len`.
func (c *LazilyFfiChannel) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.queue)
}

// IsEmpty reports whether the channel has no pending frame.
func (c *LazilyFfiChannel) IsEmpty() bool { return c.Len() == 0 }
