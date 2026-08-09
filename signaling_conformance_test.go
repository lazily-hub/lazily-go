package lazily

import (
	"encoding/json"
	"fmt"
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
	return specPath("signaling")
}

// loadSignalingFixture reads and JSON-decodes a fixture, returning ok=false when
// the spec tree is absent so the caller can t.Skip.
func loadSignalingFixture(t *testing.T, name string, v any) bool {
	t.Helper()
	path := filepath.Join(signalingSpecDir(), name)
	data, err := specReadFile(path)
	if err != nil {
		return false
	}
	mustStrictJSON(t, name, data, v)
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
	conformanceDoc
	ProtocolVersion int           `json:"protocol_version"`
	Kind            string        `json:"kind"`
	Frames          []signalFrame `json:"frames"`
	Rejects         []rejectFrame `json:"rejects"`
}

type rejectFrame struct {
	Label     string          `json:"label"`
	Direction string          `json:"direction"`
	Reason    string          `json:"reason"`
	Wire      json.RawMessage `json:"wire"`
	Input     *sessionInput   `json:"input"`
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
	// The two anti-spoof discriminators. Both used to fall through unread: the
	// welcome roster was compared to a literal list without ever asking whether
	// it excluded the recipient, and the forwarded frames were never asked
	// whether their route was server-stamped rather than client-supplied.
	RosterExcludesSelf *bool `json:"roster_excludes_self"`
	ServerStampedFrom  *bool `json:"server_stamped_from"`
}

// assertServerStampedFrom checks the wire shape a server-stamped route must
// have: `from` present, `to` absent. A client-supplied route is a `to`, so a
// forwarded frame carrying one would be exactly the spoof this asserts against.
func assertServerStampedFrom(t *testing.T, fr signalFrame, from PeerId) {
	t.Helper()
	if fr.Assertions.ServerStampedFrom == nil {
		return
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(fr.Wire, &obj); err != nil {
		t.Fatalf("%s: decode wire: %v", fr.Label, err)
	}
	_, hasFrom := obj["from"]
	_, hasTo := obj["to"]
	stamped := hasFrom && !hasTo && from != 0
	if stamped != *fr.Assertions.ServerStampedFrom {
		t.Fatalf("%s: server_stamped_from = %v (from=%v to=%v id=%d), want %v",
			fr.Label, stamped, hasFrom, hasTo, from, *fr.Assertions.ServerStampedFrom)
	}
}

// assertRosterExcludesSelf checks a welcome roster omits its own recipient.
func assertRosterExcludesSelf(t *testing.T, label string, want *bool, self PeerId, roster []PeerId) {
	t.Helper()
	if want == nil {
		return
	}
	excludes := true
	for _, p := range roster {
		if p == self {
			excludes = false
			break
		}
	}
	if excludes != *want {
		t.Fatalf("%s: roster_excludes_self = %v (peer %d in %v), want %v", label, excludes, self, roster, *want)
	}
}

// rosterIsAscending reports whether a welcome roster is in ascending peer order.
//
// A PREDICATE, not an assertion. Its two callers compare the result against the
// value the corpus declares, so a fixture that flips `roster_sorted_ascending`
// to false is contradicted by the run rather than quietly turning the check off
// (#lznullformblind).
func rosterIsAscending(roster []PeerId) bool {
	for i := 1; i < len(roster); i++ {
		if roster[i-1] >= roster[i] {
			return false
		}
	}
	return true
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
	for _, reject := range fx.Rejects {
		var err error
		switch reject.Direction {
		case "client":
			_, err = ParseClientMessage(reject.Wire)
		case "server":
			_, err = ParseServerMessage(reject.Wire)
		default:
			t.Fatalf("%s: unknown reject direction %q", reject.Label, reject.Direction)
		}
		if err == nil {
			t.Errorf("%s: malformed signaling frame was accepted", reject.Label)
		}
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
		assertRosterExcludesSelf(t, fr.Label, a.RosterExcludesSelf, m.Peer, m.Peers)
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
		assertServerStampedFrom(t, fr, m.From)
	case ServerAnswer:
		assertFrom(t, a, int64(m.From))
		assertServerStampedFrom(t, fr, m.From)
	case ServerIce:
		assertFrom(t, a, int64(m.From))
		assertServerStampedFrom(t, fr, m.From)
	case ServerRelay:
		assertFrom(t, a, int64(m.From))
		assertServerStampedFrom(t, fr, m.From)
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
	conformanceDoc
	ProtocolVersion int               `json:"protocol_version"`
	Kind            string            `json:"kind"`
	Mode            string            `json:"mode"`
	Assertions      sessionAssertions `json:"assertions"`
	Steps           []sessionStep     `json:"steps"`
	Rejects         []rejectFrame     `json:"rejects"`
}

// sessionAssertions are the transcript-wide anti-spoof invariants. They are
// stated once for the whole session rather than per frame, and until now the
// runner replayed the transcript without ever asking any of them.
type sessionAssertions struct {
	RosterExcludesSelf              *bool `json:"roster_excludes_self"`
	ForwardedFromIsServerRegistered *bool `json:"forwarded_from_is_server_registered"`
	RosterSortedAscending           *bool `json:"roster_sorted_ascending"`
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

	// conn label -> the peer id that connection joined with. The anti-spoof
	// invariant is that a forwarded frame's `from` is this id and never the
	// value the client put on the wire, so the transcript's own joins are the
	// only admissible source of truth for it.
	registered := map[string]PeerId{}

	// A transcript-wide invariant is only checked where a frame of the right
	// shape turns up, so an invariant the corpus DECLARES can go the whole
	// replay without ever being evaluated and the test still reports green
	// (#lznullformblind). That is the shape of the blindness the corpus itself
	// warns about: `forwarded_from_is_server_registered` is the anti-spoof rule
	// this fixture exists for, and it fires only inside `if forwarded` — so a
	// `forwardedFrom` that stopped recognising a routed variant, or a transcript
	// that lost its forwarded frames, would silence the whole invariant without
	// silencing the test. These count the evaluations; the gate after the loop
	// requires each declared invariant to have been exercised at least once.
	var rosterChecks, sortedChecks, forwardedChecks int

	for i, step := range fx.Steps {
		conn, ok := conns[step.Input.Conn]
		if !ok {
			t.Fatalf("step %d: unknown conn %q", i, step.Input.Conn)
		}
		msg, err := ParseClientMessage(step.Input.Recv)
		if err != nil {
			t.Fatalf("step %d: ParseClientMessage(%s): %v", i, step.Input.Recv, err)
		}
		if join, ok := msg.(ClientJoin); ok {
			registered[step.Input.Conn] = join.Peer
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

			// The transcript-wide assertions, evaluated against the frames the
			// room actually produced. Comparing each frame to its literal
			// `expect` says the bytes match; these say *why* they match, which is
			// the property the fixture exists for.
			label := fmt.Sprintf("step %d expect %d", i, j)
			if w, ok := got.(ServerWelcome); ok {
				if fx.Assertions.RosterExcludesSelf != nil {
					assertRosterExcludesSelf(t, label, fx.Assertions.RosterExcludesSelf, w.Peer, w.Peers)
					rosterChecks++
				}
				// Compared against the DECLARED value, both directions. Gating
				// on `*want == true` meant a fixture that flipped the key to
				// false turned the check off and stayed green — the key was
				// read, and still bound nothing (#lznullformblind).
				if want := fx.Assertions.RosterSortedAscending; want != nil {
					sortedChecks++
					if got := rosterIsAscending(w.Peers); got != *want {
						t.Fatalf("%s: roster_sorted_ascending = %v (%v), want %v",
							label, got, w.Peers, *want)
					}
				}
			}
			if want := fx.Assertions.ForwardedFromIsServerRegistered; want != nil {
				if from, forwarded := forwardedFrom(got); forwarded {
					forwardedChecks++
					registeredID, joined := registered[step.Input.Conn]
					if !joined {
						t.Fatalf("%s: forwarded frame from a conn %q that never joined", label, step.Input.Conn)
					}
					// Same both-directions rule, and this is the one that
					// matters most: the anti-spoof invariant this fixture exists
					// for used to evaluate only when the corpus said `true`, so
					// flipping it to false silently retired the rule.
					if got := from == registeredID; got != *want {
						t.Fatalf("%s: forwarded `from` = %d and the sender's server-registered "+
							"id is %d, so forwarded_from_is_server_registered = %v, want %v",
							label, from, registeredID, got, *want)
					}
				}
			}
		}
	}
	// Every DECLARED transcript-wide invariant must have been evaluated. A count
	// of zero means the corpus stated a rule and this replay never once asked it.
	if fx.Assertions.RosterExcludesSelf != nil && rosterChecks == 0 {
		t.Error("the fixture declares `roster_excludes_self` and the replay produced no " +
			"ServerWelcome to evaluate it against — the invariant was never asked")
	}
	if fx.Assertions.RosterSortedAscending != nil && sortedChecks == 0 {
		t.Error("the fixture declares `roster_sorted_ascending` and the replay produced no " +
			"ServerWelcome to evaluate it against — the invariant was never asked")
	}
	if fx.Assertions.ForwardedFromIsServerRegistered != nil && forwardedChecks == 0 {
		t.Error("the fixture declares `forwarded_from_is_server_registered` — the anti-spoof " +
			"rule this fixture exists for — and the replay produced no forwarded frame to " +
			"evaluate it against, so the rule was never asked")
	}

	for _, reject := range fx.Rejects {
		if reject.Input == nil {
			t.Fatalf("%s: reject has no input", reject.Label)
		}
		if _, err := ParseClientMessage(reject.Input.Recv); err == nil {
			t.Errorf("%s: malformed client signaling frame was accepted", reject.Label)
		}
	}
}

// forwardedFrom returns the server-stamped `from` of a routed frame, and
// whether the frame is one of the forwarded variants that carries one.
func forwardedFrom(m ServerMessage) (PeerId, bool) {
	switch v := m.(type) {
	case ServerOffer:
		return v.From, true
	case ServerAnswer:
		return v.From, true
	case ServerIce:
		return v.From, true
	case ServerRelay:
		return v.From, true
	default:
		return 0, false
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
