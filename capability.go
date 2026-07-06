package lazily

// Capability negotiation (protocol.md § Capability Negotiation).
//
// A Go port of lazily-dart's `lib/src/capability.dart`. Each non-local session
// starts with a compatibility handshake. If the peers disagree on
// `protocol_major_version`, `codec`, `ordered_reliable`, or required features,
// they fail closed before applying any Snapshot or Delta (or CrdtSync). The
// handshake is a STANDALONE frame — a plain (NOT externally tagged) JSON object
// — not an IpcMessage variant.
//
// Mirrors `lazily-rs::CapabilityHandshake` (struct + 5-conjunct
// `is_compatible_with`) and `lazily-js::SessionHandshake.checkCompatible`
// (structured `{ok, field, reason}` diagnostics). The constants ProtocolID /
// ProtocolMajorVersion are the canonical wire values every peer must advertise.
//
// Wire shape (field order matches the Dart toWire and the spec fixture):
//
//	{
//	  "protocol_id": "lazily-ipc",
//	  "protocol_major_version": 1,
//	  "codec": "json",
//	  "max_frame_size": 1048576,
//	  "fragmentation_supported": false,
//	  "ordered_reliable": true,
//	  "peer_id": 1,
//	  "session_id": "abc-123",
//	  "features": ["shared-blob", "signaling-relay"]
//	}

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ProtocolID is the protocol identifier every lazily-ipc peer must advertise.
const ProtocolID = "lazily-ipc"

// ProtocolMajorVersion is the current protocol major version.
const ProtocolMajorVersion = 1

// DefaultCodec is the default codec negotiation token.
const DefaultCodec = "json"

// DefaultMaxFrameSize is the default maximum frame size (1 MiB).
const DefaultMaxFrameSize int64 = 1 << 20

// ---------------------------------------------------------------------------
// FfiCapability (protocol.md § C-ABI FFI is required)
// ---------------------------------------------------------------------------

// FfiCapability is the `ffi` capability declaration. Its string value is the
// wire token.
type FfiCapability string

const (
	// FfiCapabilityHost means this binding hosts a native C ABI and may be
	// loaded in-process.
	FfiCapabilityHost FfiCapability = "host"

	// FfiCapabilityNone means this binding's runtime cannot host a native C ABI
	// (e.g. browser/Worker JS). It conforms to the interop contract but NOT the
	// in-process embedding contract, and MUST NOT advertise itself as
	// embeddable.
	FfiCapabilityNone FfiCapability = "none"
)

// Wire returns the wire token for this capability.
func (f FfiCapability) Wire() string { return string(f) }

// ParseFfiCapability parses a wire token into an FfiCapability.
func ParseFfiCapability(s string) (FfiCapability, error) {
	switch FfiCapability(s) {
	case FfiCapabilityHost:
		return FfiCapabilityHost, nil
	case FfiCapabilityNone:
		return FfiCapabilityNone, nil
	default:
		return "", fmt.Errorf("unknown FfiCapability wire token: %q", s)
	}
}

// ---------------------------------------------------------------------------
// BindingCapabilities (protocol.md § Binding Conformance Matrix)
// ---------------------------------------------------------------------------

// BindingName is this binding's name.
const BindingName = "lazily-go"

// BindingCapabilities is the lazily-go binding's conformance declaration
// (protocol.md § Binding Conformance Matrix). This binding implements every
// MUST layer.
//
// The Dart original models this as a class of static consts; in Go the
// canonical declaration is a value returned by NewBindingCapabilities. The
// `Ffi` capability is `host` because Go can host a native C ABI via cgo; never
// `none` (the `none` carve-out is reserved for platforms with no shared
// in-process address space, e.g. browser/Worker JS).
type BindingCapabilities struct {
	Binding               string
	Ffi                   FfiCapability
	ReactiveCore          bool
	Collections           bool
	StateMachine          bool
	StateCharts           bool
	Ipc                   bool
	Crdt                  bool
	Permissions           bool
	CapabilityNegotiation bool
	Async                 bool
}

// NewBindingCapabilities returns the canonical lazily-go conformance
// declaration (every MUST layer implemented, ffi = host).
func NewBindingCapabilities() BindingCapabilities {
	return BindingCapabilities{
		Binding:               BindingName,
		Ffi:                   FfiCapabilityHost,
		ReactiveCore:          true,
		Collections:           true,
		StateMachine:          true,
		StateCharts:           true,
		Ipc:                   true,
		Crdt:                  true,
		Permissions:           true,
		CapabilityNegotiation: true,
		Async:                 true,
	}
}

type bindingCapabilitiesWire struct {
	Binding               string `json:"binding"`
	Ffi                   string `json:"ffi"`
	ReactiveCore          bool   `json:"reactive_core"`
	Collections           bool   `json:"collections"`
	StateMachine          bool   `json:"state_machine"`
	StateCharts           bool   `json:"state_charts"`
	Ipc                   bool   `json:"ipc"`
	Crdt                  bool   `json:"crdt"`
	Permissions           bool   `json:"permissions"`
	CapabilityNegotiation bool   `json:"capability_negotiation"`
	Async                 bool   `json:"async"`
}

func (b BindingCapabilities) wire() bindingCapabilitiesWire {
	return bindingCapabilitiesWire{
		Binding:               b.Binding,
		Ffi:                   b.Ffi.Wire(),
		ReactiveCore:          b.ReactiveCore,
		Collections:           b.Collections,
		StateMachine:          b.StateMachine,
		StateCharts:           b.StateCharts,
		Ipc:                   b.Ipc,
		Crdt:                  b.Crdt,
		Permissions:           b.Permissions,
		CapabilityNegotiation: b.CapabilityNegotiation,
		Async:                 b.Async,
	}
}

// ToWire returns the JSON object (as an ordered marshaling struct) a peer
// introspects at build/link time.
func (b BindingCapabilities) ToWire() any { return b.wire() }

// MarshalJSON emits the conformance declaration with the spec field order.
func (b BindingCapabilities) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.wire())
}

// ---------------------------------------------------------------------------
// CapabilityCheck
// ---------------------------------------------------------------------------

// CapabilityCheck is the result of CapabilityHandshake.CheckCompatible. On
// failure, Field is the offending handshake field and Reason the
// human-readable fail-closed cause; both are empty on success.
type CapabilityCheck struct {
	Ok     bool
	Field  string
	Reason string
}

// CapabilityCheckOk builds a successful check.
func CapabilityCheckOk() CapabilityCheck { return CapabilityCheck{Ok: true} }

// CapabilityCheckFail builds a failed check for the offending field.
func CapabilityCheckFail(field, reason string) CapabilityCheck {
	return CapabilityCheck{Ok: false, Field: field, Reason: reason}
}

// IsOk reports whether the handshake is compatible.
func (c CapabilityCheck) IsOk() bool { return c.Ok }

func (c CapabilityCheck) String() string {
	if c.Ok {
		return "CapabilityCheck.ok"
	}
	return fmt.Sprintf("CapabilityCheck.fail(%s: %s)", c.Field, c.Reason)
}

// ---------------------------------------------------------------------------
// CapabilityHandshake
// ---------------------------------------------------------------------------

// CapabilityHandshake is the compatibility handshake exchanged before any graph
// state flows (protocol.md § Capability Negotiation). It is serialized as a
// plain JSON object (NOT externally tagged — a standalone frame, not an
// IpcMessage variant).
type CapabilityHandshake struct {
	ProtocolID             string
	ProtocolMajorVersion   int
	Codec                  string
	MaxFrameSize           int64
	FragmentationSupported bool
	OrderedReliable        bool
	PeerID                 PeerId
	SessionID              string
	Features               []string
}

// NewCapabilityHandshake builds a handshake with protocol defaults (JSON codec,
// 1 MiB frame, ordered-reliable, no features). Mirrors the Dart
// `CapabilityHandshake.defaults` factory; customize with the With* builders.
func NewCapabilityHandshake(peerID PeerId, sessionID string) CapabilityHandshake {
	return CapabilityHandshake{
		ProtocolID:             ProtocolID,
		ProtocolMajorVersion:   ProtocolMajorVersion,
		Codec:                  DefaultCodec,
		MaxFrameSize:           DefaultMaxFrameSize,
		FragmentationSupported: false,
		OrderedReliable:        true,
		PeerID:                 peerID,
		SessionID:              sessionID,
		Features:               []string{},
	}
}

// WithCodec returns a copy with the codec negotiation token set.
func (h CapabilityHandshake) WithCodec(codec string) CapabilityHandshake {
	h.Codec = codec
	return h
}

// WithMaxFrameSize returns a copy with the max frame size set.
func (h CapabilityHandshake) WithMaxFrameSize(maxFrameSize int64) CapabilityHandshake {
	h.MaxFrameSize = maxFrameSize
	return h
}

// WithFeatures returns a copy with the features list set.
func (h CapabilityHandshake) WithFeatures(features []string) CapabilityHandshake {
	cp := make([]string, len(features))
	copy(cp, features)
	h.Features = cp
	return h
}

// WithFragmentation returns a copy with fragmentation support set.
func (h CapabilityHandshake) WithFragmentation(supported bool) CapabilityHandshake {
	h.FragmentationSupported = supported
	return h
}

// WithOrderedReliable returns a copy with ordered-reliable set.
func (h CapabilityHandshake) WithOrderedReliable(orderedReliable bool) CapabilityHandshake {
	h.OrderedReliable = orderedReliable
	return h
}

// HasFeature reports whether this peer advertises feature.
func (h CapabilityHandshake) HasFeature(feature string) bool {
	for _, f := range h.Features {
		if f == feature {
			return true
		}
	}
	return false
}

// IsCompatibleWith reports whether this handshake is mutually compatible with
// other.
//
// Peers are compatible when both advertise ProtocolID, both advertise
// ProtocolMajorVersion, their major versions and codecs agree, and both require
// ordered-reliable delivery. Feature negotiation is caller-driven via
// HasFeature / CheckCompatible's requiredFeatures argument.
func (h CapabilityHandshake) IsCompatibleWith(other CapabilityHandshake) bool {
	return h.CheckCompatible(other).IsOk()
}

// CheckCompatible is a structured compatibility check. It returns the offending
// field (and reason) on mismatch so a caller can produce a clean fail-closed
// diagnostic — mirrors `lazily-js::SessionHandshake.checkCompatible`.
//
// requiredFeatures are checked against the OTHER peer's offered set: if this
// peer requires a feature the other does not offer, the handshake fails closed
// on `features`.
func (h CapabilityHandshake) CheckCompatible(other CapabilityHandshake, requiredFeatures ...string) CapabilityCheck {
	if h.ProtocolID != ProtocolID {
		return CapabilityCheckFail("protocol_id", "local protocol_id != lazily-ipc")
	}
	if other.ProtocolID != ProtocolID {
		return CapabilityCheckFail("protocol_id", "remote protocol_id != lazily-ipc")
	}
	if h.ProtocolMajorVersion != ProtocolMajorVersion {
		return CapabilityCheckFail("protocol_major_version",
			fmt.Sprintf("local major != %d", ProtocolMajorVersion))
	}
	if other.ProtocolMajorVersion != ProtocolMajorVersion {
		return CapabilityCheckFail("protocol_major_version",
			fmt.Sprintf("remote major != %d", ProtocolMajorVersion))
	}
	if h.ProtocolMajorVersion != other.ProtocolMajorVersion {
		return CapabilityCheckFail("protocol_major_version", "major version mismatch")
	}
	if h.Codec != other.Codec {
		return CapabilityCheckFail("codec",
			fmt.Sprintf("codec mismatch (%s vs %s)", h.Codec, other.Codec))
	}
	if !h.OrderedReliable || !other.OrderedReliable {
		return CapabilityCheckFail("ordered_reliable",
			"both peers must require ordered-reliable delivery")
	}
	offered := make(map[string]struct{}, len(other.Features))
	for _, f := range other.Features {
		offered[f] = struct{}{}
	}
	for _, required := range requiredFeatures {
		if _, ok := offered[required]; !ok {
			return CapabilityCheckFail("features",
				fmt.Sprintf("required feature %q not offered by peer", required))
		}
	}
	return CapabilityCheckOk()
}

type capabilityHandshakeWire struct {
	ProtocolID             string   `json:"protocol_id"`
	ProtocolMajorVersion   int      `json:"protocol_major_version"`
	Codec                  string   `json:"codec"`
	MaxFrameSize           int64    `json:"max_frame_size"`
	FragmentationSupported bool     `json:"fragmentation_supported"`
	OrderedReliable        bool     `json:"ordered_reliable"`
	PeerID                 PeerId   `json:"peer_id"`
	SessionID              string   `json:"session_id"`
	Features               []string `json:"features"`
}

// ToWire returns the plain-JSON wire shape (a standalone frame, NOT externally
// tagged) as an ordered marshaling struct.
func (h CapabilityHandshake) ToWire() any {
	return capabilityHandshakeWire{
		ProtocolID:             h.ProtocolID,
		ProtocolMajorVersion:   h.ProtocolMajorVersion,
		Codec:                  h.Codec,
		MaxFrameSize:           h.MaxFrameSize,
		FragmentationSupported: h.FragmentationSupported,
		OrderedReliable:        h.OrderedReliable,
		PeerID:                 h.PeerID,
		SessionID:              h.SessionID,
		Features:               nonNilSlice(h.Features),
	}
}

// MarshalJSON emits the plain-JSON wire object with the spec field order,
// always rendering `features` as an array (never null).
func (h CapabilityHandshake) MarshalJSON() ([]byte, error) {
	return json.Marshal(h.ToWire())
}

// UnmarshalJSON decodes a plain-JSON wire object. It defaults
// fragmentation_supported = false, ordered_reliable = true, codec = "json",
// protocol_id = "lazily-ipc", protocol_major_version = 1,
// max_frame_size = 1 MiB, and features = [] when absent (mirrors the lazily-rs
// serde defaults and the Dart fromWire). peer_id is required and must be a
// non-negative integer; max_frame_size, when present, must be non-negative.
func (h *CapabilityHandshake) UnmarshalJSON(b []byte) error {
	var w struct {
		ProtocolID             *string  `json:"protocol_id"`
		ProtocolMajorVersion   *int     `json:"protocol_major_version"`
		Codec                  *string  `json:"codec"`
		MaxFrameSize           *int64   `json:"max_frame_size"`
		FragmentationSupported *bool    `json:"fragmentation_supported"`
		OrderedReliable        *bool    `json:"ordered_reliable"`
		PeerID                 *PeerId  `json:"peer_id"`
		SessionID              *string  `json:"session_id"`
		Features               []string `json:"features"`
	}
	if err := json.Unmarshal(b, &w); err != nil {
		return fmt.Errorf("CapabilityHandshake must be a JSON object: %w", err)
	}

	if w.PeerID == nil || *w.PeerID < 0 {
		return fmt.Errorf("peer_id must be a non-negative integer")
	}
	maxFrameSize := DefaultMaxFrameSize
	if w.MaxFrameSize != nil {
		if *w.MaxFrameSize < 0 {
			return fmt.Errorf("max_frame_size must be a non-negative integer, got %d", *w.MaxFrameSize)
		}
		maxFrameSize = *w.MaxFrameSize
	}

	out := CapabilityHandshake{
		ProtocolID:             ProtocolID,
		ProtocolMajorVersion:   ProtocolMajorVersion,
		Codec:                  DefaultCodec,
		MaxFrameSize:           maxFrameSize,
		FragmentationSupported: false,
		OrderedReliable:        true,
		PeerID:                 *w.PeerID,
		SessionID:              "",
		Features:               []string{},
	}
	if w.ProtocolID != nil {
		out.ProtocolID = *w.ProtocolID
	}
	if w.ProtocolMajorVersion != nil {
		out.ProtocolMajorVersion = *w.ProtocolMajorVersion
	}
	if w.Codec != nil {
		out.Codec = *w.Codec
	}
	if w.FragmentationSupported != nil {
		out.FragmentationSupported = *w.FragmentationSupported
	}
	if w.OrderedReliable != nil {
		out.OrderedReliable = *w.OrderedReliable
	}
	if w.SessionID != nil {
		out.SessionID = *w.SessionID
	}
	if w.Features != nil {
		out.Features = w.Features
	}
	*h = out
	return nil
}

// EncodeJSON returns the UTF-8 JSON bytes of the plain-JSON wire form.
func (h CapabilityHandshake) EncodeJSON() ([]byte, error) { return json.Marshal(h) }

// DecodeCapabilityHandshakeJSON decodes UTF-8 JSON bytes into a handshake.
func DecodeCapabilityHandshakeJSON(data []byte) (CapabilityHandshake, error) {
	var h CapabilityHandshake
	if err := json.Unmarshal(data, &h); err != nil {
		return CapabilityHandshake{}, err
	}
	return h, nil
}

func (h CapabilityHandshake) String() string {
	return fmt.Sprintf("CapabilityHandshake(peer=%d, session=%s, codec=%s, v%d, features=[%s])",
		h.PeerID, h.SessionID, h.Codec, h.ProtocolMajorVersion, strings.Join(h.Features, " "))
}
