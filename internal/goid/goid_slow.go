// Package goid provides a fast goroutine-id lookup for reentrant locking.
//
// The public entry point is Get, which returns the current goroutine's runtime
// id. On amd64 it reads the runtime g pointer (from TLS) and the goid field
// directly, avoiding a runtime.Stack walk+format on every call. On other arches
// it falls back to parsing the runtime.Stack header.
package goid

import "runtime"

// stackGID returns the current goroutine's id by parsing the runtime stack
// header. Portable fallback (non-amd64 arches and the offset probe's source of
// truth on first use).
func stackGID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// Header format: "goroutine <id> [status]:"
	s := buf[:n]
	i := 0
	for i < len(s) && s[i] != ' ' {
		i++
	}
	i++ // skip the space after "goroutine"
	j := i
	for j < len(s) && s[j] >= '0' && s[j] <= '9' {
		j++
	}
	var id int64
	for k := i; k < j; k++ {
		id = id*10 + int64(s[k]-'0')
	}
	return id
}

// slow is the runtime.Stack-backed implementation, used as the fallback when the
// fast amd64 path is unavailable or its offset probe failed.
func slow() int64 { return stackGID() }
