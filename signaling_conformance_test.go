package lazily

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// Cross-language conformance tests for the WebRTC signaling protocol, replaying
// the canonical fixtures every binding replays.
//
//   - frames.json: every client->server and server->client frame variant. Each
//     frame's `wire` is parsed into the Go typed message, its fields are
//     asserted, and it is re-marshaled and checked for a canonical
//     (semantic-JSON) round-trip.
//   - anti_spoof_session.json: a routing transcript replayed through a live
//     SignalingRoom. The load-bearing invariant is anti-spoof: the server
//     forwards a directed frame stamping `from` with the SENDER's registered
//     peer id, never a client-supplied value; the welcome roster excludes the
//     joining peer.
//
// Fixtures mirror lazily-spec/conformance/signaling/ byte-identically. When that
// source tree is reachable on disk (sibling repo), it is used; otherwise the
// test is skipped so drift in an absent spec tree cannot fail the build.

// signalingSpecDir resolves the sibling lazily-spec conformance directory.
func signalingSpecDir() string {
	return filepath.Join("..", "lazily-spec", "conformance", "signaling")
}

// loadSignalingFixture reads and JSON-decodes a fixture, returning ok=false when
// the spec tree is absent so the caller can t.Skip.
func loadSignalingFixture(t *testing.T, name string, v any) bool {
	t.Helper()
	path := filepath.Join(signalingSpecDir(), name)
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("decode fixture %s: %v", name, err)
	}
	return true
}

// jsonSemanticEqual reports whether two JSON documents are structurally equal,
// ignoring key order and insignificant formatting.
func jsonSemanticEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		t.Fatalf("decode json a %s: %v", a, err)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		t.Fatalf("decode json b %s: %v", b, err)
	}
	return reflect.DeepEqual(av, bv)
}

// ---------------------------------------------------------------------------
// frames.json — per-variant parse / field-assert / round-trip
// ---------------------------------------------------------------------------

type framesFixture struct {
	Kind   string        `json:"kind"`
	Frames []signalFrame `json:"frames"`
}

type signalFrame struct {
	Label      string          `json:"label"`
	Direction  string          `json:"direction"`
	Variant    string          `json:"variant"`
	Assertions frameAssertions `json:"assertions"`
	Wire       json.RawMessage `json:"wire"`
}

type frameAssertions struct {
	Peer            *int64   `json:"peer"`
	To              *int64   `json:"to"`
	From            *int64   `json:"from"`
	Code            *string  `json:"code"`
	HasCapabilities *bool    `json:"has_capabilities"`
	Capabilities    []string `json:"capabilities"`
	Peers           *[]int64 `json:"peers"`
}

func TestSignalingFramesConformance(t *testing.T) {
	var fx framesFixture
	if !loadSignalingFixture(t, "frames.json", &fx) {
		t.Skipf("lazily-spec fixture absent: %s", filepath.Join(signalingSpecDir(), "frames.json"))
	}
	if fx.Kind != "SignalingFrames" {
		t.Fatalf("unexpected fixture kind %q", fx.Kind)
	}
	if len(fx.Frames) == 0 {
		t.Fatal("frames.json has no frames")
	}

	for _, fr := range fx.Frames {
		fr := fr
		t.Run(fr.Label, func(t *testing.T) {
			switch fr.Direction {
			case "client":
				msg, err := ParseClientMessage(fr.Wire)
				if err != nil {
					t.Fatalf("ParseClientMessage(%s): %v", fr.Wire, err)
				}
				if msg.Type() != fr.Variant {
					t.Fatalf("client type = %q, want %q", msg.Type(), fr.Variant)
				}
				assertClientFrame(t, fr, msg)
				got, err := json.Marshal(msg)
				if err != nil {
					t.Fatalf("marshal client %s: %v", fr.Label, err)
				}
				if !jsonSemanticEqual(t, got, fr.Wire) {
					t.Fatalf("client round-trip mismatch\n got: %s\nwant: %s", got, fr.Wire)
				}
			case "server":
				msg, err := ParseServerMessage(fr.Wire)
				if err != nil {
					t.Fatalf("ParseServerMessage(%s): %v", fr.Wire, err)
				}
				if msg.Type() != fr.Variant {
					t.Fatalf("server type = %q, want %q", msg.Type(), fr.Variant)
				}
				assertServerFrame(t, fr, msg)
				got, err := json.Marshal(msg)
				if err != nil {
					t.Fatalf("marshal server %s: %v", fr.Label, err)
				}
				if !jsonSemanticEqual(t, got, fr.Wire) {
					t.Fatalf("server round-trip mismatch\n got: %s\nwant: %s", got, fr.Wire)
				}
			default:
				t.Fatalf("unknown direction %q", fr.Direction)
			}
		})
	}
}

func assertClientFrame(t *testing.T, fr signalFrame, msg ClientMessage) {
	t.Helper()
	a := fr.Assertions
	switch m := msg.(type) {
	case ClientJoin:
		if a.Peer != nil && m.Peer != PeerId(*a.Peer) {
			t.Fatalf("join peer = %d, want %d", m.Peer, *a.Peer)
		}
		if a.HasCapabilities != nil {
			has := m.Capabilities != nil
			if has != *a.HasCapabilities {
				t.Fatalf("join has_capabilities = %v, want %v", has, *a.HasCapabilities)
			}
		}
		if a.Capabilities != nil && !reflect.DeepEqual(m.Capabilities, a.Capabilities) {
			t.Fatalf("join capabilities = %v, want %v", m.Capabilities, a.Capabilities)
		}
	case ClientOffer:
		assertTo(t, a, int64(m.To))
	case ClientAnswer:
		assertTo(t, a, int64(m.To))
	case ClientIce:
		assertTo(t, a, int64(m.To))
	case ClientRelay:
		assertTo(t, a, int64(m.To))
	case ClientLeave:
		// no fields
	default:
		t.Fatalf("unexpected client message type %T", msg)
	}
}

func assertServerFrame(t *testing.T, fr signalFrame, msg ServerMessage) {
	t.Helper()
	a := fr.Assertions
	switch m := msg.(type) {
	case ServerWelcome:
		if a.Peer != nil && m.Peer != PeerId(*a.Peer) {
			t.Fatalf("welcome peer = %d, want %d", m.Peer, *a.Peer)
		}
		if a.Peers != nil {
			want := make([]PeerId, len(*a.Peers))
			for i, p := range *a.Peers {
				want[i] = PeerId(p)
			}
			got := nonNilPeers(m.Peers)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("welcome peers = %v, want %v", got, want)
			}
		}
	case ServerPeerJoined:
		if a.Peer != nil && m.Peer != PeerId(*a.Peer) {
			t.Fatalf("peer-joined peer = %d, want %d", m.Peer, *a.Peer)
		}
	case ServerPeerLeft:
		if a.Peer != nil && m.Peer != PeerId(*a.Peer) {
			t.Fatalf("peer-left peer = %d, want %d", m.Peer, *a.Peer)
		}
	case ServerOffer:
		assertFrom(t, a, int64(m.From))
	case ServerAnswer:
		assertFrom(t, a, int64(m.From))
	case ServerIce:
		assertFrom(t, a, int64(m.From))
	case ServerRelay:
		assertFrom(t, a, int64(m.From))
	case ServerError:
		if a.Code != nil && m.Code != *a.Code {
			t.Fatalf("error code = %q, want %q", m.Code, *a.Code)
		}
	default:
		t.Fatalf("unexpected server message type %T", msg)
	}
}

func assertTo(t *testing.T, a frameAssertions, got int64) {
	t.Helper()
	if a.To != nil && got != *a.To {
		t.Fatalf("to = %d, want %d", got, *a.To)
	}
}

func assertFrom(t *testing.T, a frameAssertions, got int64) {
	t.Helper()
	if a.From != nil && got != *a.From {
		t.Fatalf("from = %d, want %d", got, *a.From)
	}
}

func nonNilPeers(p []PeerId) []PeerId {
	if p == nil {
		return []PeerId{}
	}
	return p
}

// ---------------------------------------------------------------------------
// anti_spoof_session.json — replay through a live SignalingRoom
// ---------------------------------------------------------------------------

type sessionFixture struct {
	Kind  string        `json:"kind"`
	Mode  string        `json:"mode"`
	Steps []sessionStep `json:"steps"`
}

type sessionStep struct {
	Input  sessionInput    `json:"input"`
	Expect []expectedFrame `json:"expect"`
}

type sessionInput struct {
	Conn string          `json:"conn"`
	Recv json.RawMessage `json:"recv"`
}

type expectedFrame struct {
	To    string          `json:"to"`
	Frame json.RawMessage `json:"frame"`
}

func TestSignalingAntiSpoofSession(t *testing.T) {
	var fx sessionFixture
	if !loadSignalingFixture(t, "anti_spoof_session.json", &fx) {
		t.Skipf("lazily-spec fixture absent: %s", filepath.Join(signalingSpecDir(), "anti_spoof_session.json"))
	}
	if fx.Kind != "SignalingSession" {
		t.Fatalf("unexpected fixture kind %q", fx.Kind)
	}

	mode := SignalingMode(fx.Mode)
	if mode == "" {
		mode = SignalingModeOpen
	}
	room := NewSignalingRoom(mode)
	defer room.Close()

	// Connect every distinct connection label the transcript touches, up front.
	conns := map[string]*ClientConn{}
	for _, step := range fx.Steps {
		if _, ok := conns[step.Input.Conn]; !ok {
			c, err := room.Connect(step.Input.Conn)
			if err != nil {
				t.Fatalf("Connect(%q): %v", step.Input.Conn, err)
			}
			conns[step.Input.Conn] = c
		}
	}

	for i, step := range fx.Steps {
		conn, ok := conns[step.Input.Conn]
		if !ok {
			t.Fatalf("step %d: unknown conn %q", i, step.Input.Conn)
		}
		msg, err := ParseClientMessage(step.Input.Recv)
		if err != nil {
			t.Fatalf("step %d: ParseClientMessage(%s): %v", i, step.Input.Recv, err)
		}
		select {
		case conn.Inbound() <- msg:
		case <-time.After(2 * time.Second):
			t.Fatalf("step %d: timed out sending inbound on conn %q", i, step.Input.Conn)
		}

		// Each expected frame targets a specific connection. Per-connection
		// ordering is preserved, so read one frame per expectation from its
		// target's outbound channel.
		for j, exp := range step.Expect {
			target, ok := conns[exp.To]
			if !ok {
				t.Fatalf("step %d expect %d: unknown target conn %q", i, j, exp.To)
			}
			got := recvServerFrame(t, target, i, j)
			gotBytes, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("step %d expect %d: marshal routed frame: %v", i, j, err)
			}
			if !jsonSemanticEqual(t, gotBytes, exp.Frame) {
				t.Fatalf("step %d expect %d (to %q) mismatch\n got: %s\nwant: %s",
					i, j, exp.To, gotBytes, exp.Frame)
			}
		}
	}
}

func recvServerFrame(t *testing.T, c *ClientConn, step, idx int) ServerMessage {
	t.Helper()
	select {
	case m, ok := <-c.Outbound():
		if !ok {
			t.Fatalf("step %d expect %d: outbound closed", step, idx)
		}
		return m
	case <-time.After(2 * time.Second):
		t.Fatalf("step %d expect %d: timed out waiting for outbound frame", step, idx)
		return nil
	}
}
