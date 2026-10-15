//go:build cgo

// Command lazily-ffi-shared packages lazily-go's native host boundary as a
// c-shared artifact. Importing the root package links the //export functions in
// ffi_cgo.go into this main package's shared library; the command intentionally
// defines no second ABI surface.
package main

import _ "github.com/lazily-hub/lazily-go"

func main() {}
