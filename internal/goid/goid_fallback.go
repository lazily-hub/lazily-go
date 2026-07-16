//go:build !amd64

package goid

// Fallback Get for non-amd64 arches: the portable runtime.Stack path.
// goid_amd64.go provides a faster TLS-based implementation on amd64.

// Get returns the current goroutine's runtime id.
func Get() int64 { return slow() }
