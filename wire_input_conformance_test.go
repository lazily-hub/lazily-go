package lazily

import "fmt"

// wireInputFNV1a64 fingerprints the exact byte slice handed to a codec
// decoder. The corpus carries the expected lowercase, zero-padded digest so a
// runner cannot satisfy wire_encoding by decoding a reconstructed proxy.
func wireInputFNV1a64(raw []byte) string {
	const (
		offset uint64 = 0xcbf29ce484222325
		prime  uint64 = 0x100000001b3
	)
	hash := offset
	for _, b := range raw {
		hash ^= uint64(b)
		hash *= prime
	}
	return fmt.Sprintf("%016x", hash)
}
