//go:build cgo

package lazily

// C-ABI export layer for the lazily FFI boundary (protocol.md § FFI Boundary,
// schemas/ffi.json). Compiled only when cgo is enabled; the pure-Go types and
// logic it wraps live in ffi.go and are available in both build modes.
//
// The C surface is deliberately minimal but real: opaque channel handles and
// owned byte buffers only. Foreign runtimes exchange serialized IpcMessage
// values without ever receiving Go pointers, closures, or typed handles.
//
// cgo pointer-rules note: a *LazilyFfiChannel is never handed to C directly.
// Channel handles are minted with runtime/cgo.Handle (an opaque uintptr-sized
// token) so no live Go pointer crosses the boundary. Output buffers are
// C-allocated (C.CBytes / C.malloc) and owned by the C caller until freed via
// lazily_ffi_bytes_free.

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
    uint8_t *ptr;
    size_t   len;
} LazilyFfiBytes;
*/
import "C"

import (
	"runtime/cgo"
	"unsafe"
)

// FFIHasCABI reports whether the native C-ABI export layer is compiled into
// this build. True under CGO_ENABLED=1.
const FFIHasCABI = true

// ffiGuard runs op, catching any panic and reporting it as
// LazilyFfiStatusPanic so a Go panic never unwinds across the C ABI.
func ffiGuard(op func() LazilyFfiStatus) (status LazilyFfiStatus) {
	defer func() {
		if recover() != nil {
			status = LazilyFfiStatusPanic
		}
	}()
	return op()
}

// channelFromHandle resolves an opaque handle to its *LazilyFfiChannel,
// reporting LazilyFfiStatusNullPointer for a zero or invalid handle.
func channelFromHandle(handle C.uintptr_t) (ch *LazilyFfiChannel, status LazilyFfiStatus) {
	if handle == 0 {
		return nil, LazilyFfiStatusNullPointer
	}
	defer func() {
		if recover() != nil {
			ch, status = nil, LazilyFfiStatusNullPointer
		}
	}()
	value := cgo.Handle(uintptr(handle)).Value()
	channel, ok := value.(*LazilyFfiChannel)
	if !ok {
		return nil, LazilyFfiStatusNullPointer
	}
	return channel, LazilyFfiStatusOk
}

// goBytesFromRaw copies a C byte range into a Go slice. A zero length yields an
// empty (nil) slice; a null pointer with a non-zero length is a NullPointer
// error (mirrors lazily-rs bytes_from_raw).
func goBytesFromRaw(ptr *C.uint8_t, length C.size_t) ([]byte, LazilyFfiStatus) {
	if length == 0 {
		return nil, LazilyFfiStatusOk
	}
	if ptr == nil {
		return nil, LazilyFfiStatusNullPointer
	}
	return C.GoBytes(unsafe.Pointer(ptr), C.int(length)), LazilyFfiStatusOk
}

// writeOutBytes stores data in a freshly C-allocated buffer and publishes it
// through out. An empty payload leaves a null/zero buffer. The C caller owns
// the buffer and must release it via lazily_ffi_bytes_free.
func writeOutBytes(out *C.LazilyFfiBytes, data []byte) {
	out.ptr = nil
	out.len = 0
	if len(data) == 0 {
		return
	}
	out.ptr = (*C.uint8_t)(C.CBytes(data))
	out.len = C.size_t(len(data))
}

//export lazily_ffi_channel_new
func lazily_ffi_channel_new() C.uintptr_t {
	handle := cgo.NewHandle(NewLazilyFfiChannel())
	return C.uintptr_t(uintptr(handle))
}

//export lazily_ffi_channel_free
func lazily_ffi_channel_free(handle C.uintptr_t) {
	if handle == 0 {
		return
	}
	defer func() { _ = recover() }()
	cgo.Handle(uintptr(handle)).Delete()
}

//export lazily_ffi_channel_send_json
func lazily_ffi_channel_send_json(handle C.uintptr_t, ptr *C.uint8_t, length C.size_t) C.int {
	return C.int(ffiGuard(func() LazilyFfiStatus {
		channel, status := channelFromHandle(handle)
		if status != LazilyFfiStatusOk {
			return status
		}
		bytes, status := goBytesFromRaw(ptr, length)
		if status != LazilyFfiStatusOk {
			return status
		}
		return channel.SendJSONFrame(LazilyFfiBytesFromOwned(bytes))
	}))
}

//export lazily_ffi_channel_recv_json
func lazily_ffi_channel_recv_json(handle C.uintptr_t, out *C.LazilyFfiBytes) C.int {
	return C.int(ffiGuard(func() LazilyFfiStatus {
		if out == nil {
			return LazilyFfiStatusNullPointer
		}
		out.ptr = nil
		out.len = 0
		channel, status := channelFromHandle(handle)
		if status != LazilyFfiStatusOk {
			return status
		}
		frame, status := channel.RecvJSONFrame()
		if status != LazilyFfiStatusOk {
			return status
		}
		writeOutBytes(out, frame.Bytes)
		return LazilyFfiStatusOk
	}))
}

//export lazily_ffi_channel_len
func lazily_ffi_channel_len(handle C.uintptr_t, outLen *C.size_t) C.int {
	return C.int(ffiGuard(func() LazilyFfiStatus {
		if outLen == nil {
			return LazilyFfiStatusNullPointer
		}
		channel, status := channelFromHandle(handle)
		if status != LazilyFfiStatusOk {
			return status
		}
		*outLen = C.size_t(channel.Len())
		return LazilyFfiStatusOk
	}))
}

//export lazily_ffi_ipc_message_validate_json
func lazily_ffi_ipc_message_validate_json(ptr *C.uint8_t, length C.size_t) C.int {
	return C.int(ffiGuard(func() LazilyFfiStatus {
		bytes, status := goBytesFromRaw(ptr, length)
		if status != LazilyFfiStatusOk {
			return status
		}
		return LazilyFfiValidateJSON(LazilyFfiBytesFromOwned(bytes))
	}))
}

//export lazily_ffi_ipc_message_kind_json
func lazily_ffi_ipc_message_kind_json(ptr *C.uint8_t, length C.size_t, outKind *C.int) C.int {
	return C.int(ffiGuard(func() LazilyFfiStatus {
		if outKind == nil {
			return LazilyFfiStatusNullPointer
		}
		*outKind = C.int(LazilyFfiMessageKindUnknown)
		bytes, status := goBytesFromRaw(ptr, length)
		if status != LazilyFfiStatusOk {
			return status
		}
		classification := LazilyFfiKindJSON(LazilyFfiBytesFromOwned(bytes))
		if classification.Status == LazilyFfiStatusOk {
			*outKind = C.int(classification.Kind)
		}
		return classification.Status
	}))
}

//export lazily_ffi_ipc_message_clone_json
func lazily_ffi_ipc_message_clone_json(ptr *C.uint8_t, length C.size_t, out *C.LazilyFfiBytes) C.int {
	return C.int(ffiGuard(func() LazilyFfiStatus {
		if out == nil {
			return LazilyFfiStatusNullPointer
		}
		out.ptr = nil
		out.len = 0
		bytes, status := goBytesFromRaw(ptr, length)
		if status != LazilyFfiStatusOk {
			return status
		}
		result := LazilyFfiCloneJSON(LazilyFfiBytesFromOwned(bytes))
		if result.Status == LazilyFfiStatusOk && result.Output != nil {
			writeOutBytes(out, result.Output.Bytes)
		}
		return result.Status
	}))
}

//export lazily_ffi_bytes_free
func lazily_ffi_bytes_free(bytes C.LazilyFfiBytes) {
	if bytes.ptr != nil {
		C.free(unsafe.Pointer(bytes.ptr))
	}
}
