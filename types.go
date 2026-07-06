package lazily

// Shared wire identity aliases (protocol.md § Shared Types). These mirror the
// lazily-spec typedefs `NodeId` / `PeerId` / `Epoch`. They are Go type aliases
// (not distinct types) so they interoperate freely with int64 arithmetic and
// JSON numbers.
type (
	// NodeId identifies a reactive node in a Snapshot/Delta graph.
	NodeId = int64
	// PeerId identifies a replica/peer in the distributed plane.
	PeerId = int64
	// Epoch is the monotonically increasing snapshot/delta sequence number.
	Epoch = int64
)
