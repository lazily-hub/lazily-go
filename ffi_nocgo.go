//go:build !cgo

package lazily

// Non-cgo companion to ffi_cgo.go. Under CGO_ENABLED=0 the native `//export`
// C-ABI layer cannot be compiled, so the exported adapter functions
// (lazily_ffi_channel_new, lazily_ffi_ipc_message_clone_json, ...) are absent.
// The pure-Go FFI types and logic in ffi.go remain fully available; use
// LazilyFfiChannel / LazilyFfiValidateJSON / LazilyFfiKindJSON /
// LazilyFfiCloneJSON directly for in-process interop.

// FFIHasCABI reports whether the native C-ABI export layer is compiled into
// this build. False under CGO_ENABLED=0.
const FFIHasCABI = false
