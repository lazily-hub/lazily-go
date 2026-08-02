package lazily

import (
	"encoding/json"
	"strings"
	"testing"
)

// Fail-open dispatch audit of the LIBRARY sources (not the conformance
// runners). Every site the audit called out gets one of two tests here:
//
//   - a REJECTION test, for a site converted to fail closed; and
//   - a PINNING test, for a site whose leniency IS the wire contract.
//
// The second kind is the load-bearing one. An undocumented default and a
// deliberate one are indistinguishable from outside the package; the pin is
// what makes them distinguishable, and what makes a later "tidy-up" that
// removes the leniency show up as a red test rather than a silent wire break.

// ---------------------------------------------------------------------------
// FAIL CLOSED — text-CRDT delta-sync wire decode (text_crdt.go)
// ---------------------------------------------------------------------------

// TestOpIdFromWireRejectsNonIntegerFields proves the decode path no longer
// coerces a malformed `counter`/`peer` to 0.
//
// Zero is not a neutral default here: nextId mints counters from 1, so
// OpId{0,0} is exactly textRootKey — the byOrigin bucket for elements whose
// origin is the document start. Before the conversion, `{"counter":"x"}` and
// `{}` both produced the document-root sentinel and merged into visible text.
func TestOpIdFromWireRejectsNonIntegerFields(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"counter is a string", map[string]any{"counter": "7", "peer": 1.0}, "OpId.counter"},
		{"counter is absent", map[string]any{"peer": 1.0}, "OpId.counter"},
		{"counter is null", map[string]any{"counter": nil, "peer": 1.0}, "OpId.counter"},
		{"counter is a bool", map[string]any{"counter": true, "peer": 1.0}, "OpId.counter"},
		{"counter is fractional", map[string]any{"counter": 1.5, "peer": 1.0}, "OpId.counter"},
		{"peer is a string", map[string]any{"counter": 1.0, "peer": "me"}, "OpId.peer"},
		{"peer is absent", map[string]any{"counter": 1.0}, "OpId.peer"},
		{"not an object", []any{1, 2}, "must be a JSON object"},
		{"json.Number is fractional", map[string]any{
			"counter": json.Number("1.5"), "peer": json.Number("1"),
		}, "OpId.counter"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := OpIdFromWire(c.in)
			if err == nil {
				t.Fatalf("OpIdFromWire(%v) accepted, returning %+v; want a rejection", c.in, got)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not name %q", err.Error(), c.want)
			}
			// The rejection must not hand back the root sentinel either.
			if got != (OpId{}) {
				t.Fatalf("rejected decode returned %+v, want the zero value", got)
			}
			if got == textRootKey && err == nil {
				t.Fatal("a malformed OpId resolved to textRootKey")
			}
		})
	}
}

// TestOpIdFromWireAcceptsEveryNumericEncoding pins the ACCEPTING half: the
// three shapes a JSON decoder can hand us for an integer all still decode.
// Without this, "reject everything" would pass the rejection test above.
func TestOpIdFromWireAcceptsEveryNumericEncoding(t *testing.T) {
	cases := []struct {
		name string
		in   any
	}{
		{"float64 (encoding/json default)", map[string]any{"counter": 42.0, "peer": 7.0}},
		{"json.Number (UseNumber)", map[string]any{"counter": json.Number("42"), "peer": json.Number("7")}},
		{"native int64", map[string]any{"counter": int64(42), "peer": int64(7)}},
		{"native int", map[string]any{"counter": 42, "peer": 7}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := OpIdFromWire(c.in)
			if err != nil {
				t.Fatalf("OpIdFromWire(%v): %v", c.in, err)
			}
			if got != (OpId{Counter: 42, Peer: 7}) {
				t.Fatalf("got %+v, want {42 7}", got)
			}
		})
	}
}

// TestTextOpFromWireRejectsMalformedOps covers the op envelope around OpId:
// a non-object op, a missing/non-string `ch`, and a malformed `origin` or
// `deleted`. `origin`/`deleted` stay lenient for an explicit null, which is
// the wire spelling of "document start" / "live".
func TestTextOpFromWireRejectsMalformedOps(t *testing.T) {
	goodID := map[string]any{"counter": 1.0, "peer": 2.0}
	bad := map[string]any{"counter": "nope", "peer": 2.0}

	cases := []struct {
		name string
		in   any
		want string
	}{
		{"not an object", "op", "must be a JSON object"},
		{"id is malformed", map[string]any{"id": bad, "ch": "a"}, "TextOp.id"},
		{"ch is absent", map[string]any{"id": goodID}, "TextOp.ch"},
		{"ch is not a string", map[string]any{"id": goodID, "ch": 3.0}, "TextOp.ch"},
		{"origin is malformed", map[string]any{"id": goodID, "ch": "a", "origin": bad}, "TextOp.origin"},
		{"deleted is malformed", map[string]any{"id": goodID, "ch": "a", "deleted": bad}, "TextOp.deleted"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if _, err := TextOpFromWire(c.in); err == nil {
				t.Fatalf("TextOpFromWire(%v) accepted; want a rejection", c.in)
			} else if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not name %q", err.Error(), c.want)
			}
		})
	}
}

// TestTextOpWireRoundTripSurvives is the accepting counterpart: a well-formed
// op still round-trips through ToWire/FromWire, including the null origin and
// deleted spellings.
func TestTextOpWireRoundTripSurvives(t *testing.T) {
	origin := OpId{Counter: 3, Peer: 9}
	deleted := OpId{Counter: 11, Peer: 4}
	for _, op := range []TextOp{
		{Id: OpId{Counter: 1, Peer: 2}, Ch: "a"},
		{Id: OpId{Counter: 5, Peer: 6}, Ch: "b", Origin: &origin},
		{Id: OpId{Counter: 7, Peer: 8}, Ch: "c", Origin: &origin, Deleted: &deleted},
	} {
		wire := op.ToWire()
		// Force the op through a real JSON round trip so the numeric fields
		// arrive as float64 the way a peer's frame would.
		encoded, err := json.Marshal(wire)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var decoded any
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		got, err := TextOpFromWire(decoded)
		if err != nil {
			t.Fatalf("TextOpFromWire(%s): %v", encoded, err)
		}
		if got.Id != op.Id || got.Ch != op.Ch {
			t.Fatalf("round trip of %s lost the id/ch: %+v", encoded, got)
		}
		if (got.Origin == nil) != (op.Origin == nil) ||
			(got.Origin != nil && *got.Origin != *op.Origin) {
			t.Fatalf("round trip of %s lost origin: %+v", encoded, got.Origin)
		}
		if (got.Deleted == nil) != (op.Deleted == nil) ||
			(got.Deleted != nil && *got.Deleted != *op.Deleted) {
			t.Fatalf("round trip of %s lost deleted: %+v", encoded, got.Deleted)
		}
	}
}

// ---------------------------------------------------------------------------
// INTENTIONAL — state-chart `kind` derivation (state_chart.go parseState)
// ---------------------------------------------------------------------------

// TestStateChartKindLeniencyIsPinned pins the deliberate leniency in
// parseState: an unrecognised `kind` annotation, and a non-string `history`,
// both fall through to the structural derivation instead of failing the
// document. See the block comment at the site for the wire reason.
//
// This mirrors lazily-rs `parse_state`, which ends the same ladder in
// `Kind::Atomic` and reads `history` through `as_str()`. If a future change
// makes lazily-go strict here, the same chart document would load in one
// binding and be refused by another — this test is what turns that into a
// red build instead of a field report.
func TestStateChartKindLeniencyIsPinned(t *testing.T) {
	t.Run("unknown kind annotation derives Atomic", func(t *testing.T) {
		def := mustChart(t, `{
			"initial": "root",
			"states": {
				"root":  {"initial": "leaf"},
				"leaf":  {"parent": "root", "kind": "vendor-annotation-from-a-newer-spec"}
			}
		}`)
		if got := def.kind("leaf"); got != kindAtomic {
			t.Fatalf("unknown kind derived %v, want kindAtomic", got)
		}
		// Bounded consequence: an Atomic state is a real active leaf.
		chart := NewStateChart(NewContext(), def)
		if leaves := chart.ActiveLeaves(); len(leaves) != 1 || leaves[0] != "leaf" {
			t.Fatalf("active leaves %v, want [leaf]", leaves)
		}
	})

	t.Run("unknown kind does not defeat structural derivation", func(t *testing.T) {
		def := mustChart(t, `{
			"initial": "root",
			"states": {
				"root":  {"kind": "machine", "initial": "mid"},
				"mid":   {"parent": "root", "kind": "whatever", "initial": "leaf"},
				"leaf":  {"parent": "mid"}
			}
		}`)
		if got := def.kind("root"); got != kindCompound {
			t.Fatalf("root derived %v, want kindCompound", got)
		}
		if got := def.kind("mid"); got != kindCompound {
			t.Fatalf("mid derived %v, want kindCompound", got)
		}
	})

	t.Run("parallel outranks an unknown kind", func(t *testing.T) {
		def := mustChart(t, `{
			"initial": "root",
			"states": {
				"root": {"parallel": true, "kind": "unrecognised"},
				"a":    {"parent": "root"},
				"b":    {"parent": "root"}
			}
		}`)
		if got := def.kind("root"); got != kindParallel {
			t.Fatalf("root derived %v, want kindParallel", got)
		}
	})

	t.Run("non-string history falls through instead of erroring", func(t *testing.T) {
		def := mustChart(t, `{
			"initial": "root",
			"states": {
				"root": {"initial": "leaf"},
				"leaf": {"parent": "root", "history": 7}
			}
		}`)
		if got := def.kind("leaf"); got != kindAtomic {
			t.Fatalf("non-string history derived %v, want kindAtomic", got)
		}
	})

	// The counterpart that is NOT lenient, pinned in the same place so the two
	// halves of the contract cannot drift apart: a `history` that IS a string
	// but names an unknown mode is rejected, because the history mode decides
	// what gets restored and guessing silently loses state.
	t.Run("unknown string history is rejected", func(t *testing.T) {
		_, err := ChartDefFromJSON([]byte(`{
			"initial": "root",
			"states": {
				"root": {"initial": "leaf"},
				"leaf": {"parent": "root", "history": "medium"}
			}
		}`))
		if err == nil {
			t.Fatal("history:\"medium\" was accepted; want a rejection")
		}
		if !strings.Contains(err.Error(), "unknown history kind") {
			t.Fatalf("error %q does not name the offending history kind", err.Error())
		}
	})
}

func mustChart(t *testing.T, doc string) *ChartDef {
	t.Helper()
	def, err := ChartDefFromJSON([]byte(doc))
	if err != nil {
		t.Fatalf("ChartDefFromJSON: %v", err)
	}
	return def
}

// ---------------------------------------------------------------------------
// INTENTIONAL — FFI message-kind discriminant (ffi.go)
// ---------------------------------------------------------------------------

// TestFFIMessageKindLeniencyIsPinned records the wire reason for
// LazilyFfiMessageKindFromCode's unknown-code default, alongside the existing
// discriminant-stability assertion in capability_ffi_test.go.
//
// Wire reason: the code crosses a C ABI as a plain `int`. A C caller built
// against an older header has no `LazilyFfiMessageKind` member for a frame
// kind added later, and the C enum's own zero value IS "unknown", so mapping
// an out-of-range code to Unknown is what keeps an older host loadable against
// a newer library. The consequence is bounded: Unknown is not a routable kind,
// so a caller that switches on it falls into its own unhandled branch rather
// than misrouting the frame as a Snapshot.
func TestFFIMessageKindLeniencyIsPinned(t *testing.T) {
	for _, code := range []int{-1, 6, 99, 1 << 30} {
		if got := LazilyFfiMessageKindFromCode(code); got != LazilyFfiMessageKindUnknown {
			t.Fatalf("FromCode(%d) = %v, want Unknown", code, got)
		}
	}
	// Known codes must still decode, or "always Unknown" would pass the above.
	for code, want := range map[int]LazilyFfiMessageKind{
		1: LazilyFfiMessageKindSnapshot,
		2: LazilyFfiMessageKindDelta,
		3: LazilyFfiMessageKindCrdtSync,
		4: LazilyFfiMessageKindResyncRequest,
		5: LazilyFfiMessageKindOutboxAck,
	} {
		if got := LazilyFfiMessageKindFromCode(code); got != want {
			t.Fatalf("FromCode(%d) = %v, want %v", code, got, want)
		}
	}
	// The paired status decoder is the fail-closed counterpart: it reports the
	// unknown code through `ok` rather than defaulting.
	if _, ok := LazilyFfiStatusFromCode(99); ok {
		t.Fatal("LazilyFfiStatusFromCode(99) reported ok; the status decoder is strict by contract")
	}
}

// ---------------------------------------------------------------------------
// INTENTIONAL — blob backend discriminator (ipc.go)
// ---------------------------------------------------------------------------

// TestBlobBackendLeniencyIsPinned pins the unknown-backend default, which
// transport_test.go exercises only through ShmBlobRef unmarshaling.
//
// Wire reason: `backend` is an OPTIONAL, omitted-when-default field, so a
// legacy descriptor minted before the field existed carries no backend at all
// and must still resolve as Shm. A producer on a newer binding may stamp a
// backend this binding has no code for; normalizing it to Shm rather than
// erroring keeps the descriptor parseable, and the router then declines it
// safely — routerIndex sends it to slot 0, and if no Shm backend is
// registered ReadView returns (nil, false). It never resolves against a
// backend of the wrong kind.
func TestBlobBackendLeniencyIsPinned(t *testing.T) {
	for _, unknown := range []BlobBackendKind{"", "rdma", "cuda-ipc", "Shm", "ARROW"} {
		if got := unknown.Normalized(); got != BackendShm {
			t.Fatalf("BlobBackendKind(%q).Normalized() = %q, want %q", unknown, got, BackendShm)
		}
		if !unknown.IsDefault() {
			t.Fatalf("BlobBackendKind(%q).IsDefault() = false, want true", unknown)
		}
		if got := unknown.routerIndex(); got != 0 {
			t.Fatalf("BlobBackendKind(%q).routerIndex() = %d, want 0", unknown, got)
		}
	}
	// Known kinds must still round-trip, or "always Shm" would pass the above.
	for kind, wantIndex := range map[BlobBackendKind]int{
		BackendShm: 0, BackendArrow: 1, BackendInProcess: 2,
	} {
		if got := kind.Normalized(); got != kind {
			t.Fatalf("%q.Normalized() = %q, want itself", kind, got)
		}
		if got := kind.routerIndex(); got != wantIndex {
			t.Fatalf("%q.routerIndex() = %d, want %d", kind, got, wantIndex)
		}
	}
	// The bounded consequence: an unknown backend resolves to nothing rather
	// than to some other backend's bytes.
	router := NewBlobRouter()
	if _, ok := router.ReadView(ShmBlobRef{Backend: "rdma", Len: 4}); ok {
		t.Fatal("an unknown backend resolved against an empty router")
	}
}
