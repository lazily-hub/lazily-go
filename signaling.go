package lazily

// WebRTC signaling protocol — room routing + client/server frames.
//
// A Go port of lazily-dart's `lib/src/signaling.dart`, conformant with
// lazily-spec (protocol.md § Signaling Protocol (WebSocket) and
// schemas/signaling.json). Feature rows: "Distributed plane — WebRTC transport
// + signaling".
//
// The signaling server brokers peer discovery for the distributed CRDT plane.
// Peers join a session, learn the roster, and exchange the WebRTC SDP/ICE
// handshake (or relay opaque payloads). The signaling layer never interprets
// CRDT state.
//
// Wire conventions (NORMATIVE, from schemas/signaling.json + protocol.md):
//   - Each frame is internally tagged by a bare "type" discriminant string
//     (join/offer/answer/ice/relay/leave for client frames;
//     welcome/peer-joined/peer-left/offer/answer/ice/relay/error for server
//     frames). Field names are snake_case / bare as the schema declares them.
//   - `peer`/`to`/`from` ids are bare JSON integers (PeerId).
//   - `from` on every forwarded server frame is the sender connection's
//     registered peer id, never client-supplied (anti-spoofing).
//   - Relay `payload` is arbitrary JSON, preserved byte-for-byte via
//     json.RawMessage so a relayed frame round-trips unchanged.
//
// Channel architecture (the concurrency model): SignalingRoom is a single owner
// goroutine that owns all room/peer state. Callers Connect a connection to get
// a *ClientConn with a per-client inbound channel (transport -> room) and a
// per-client outbound `<-chan ServerMessage` (room -> transport). The owner
// serializes every mutation, and broadcast/relay is performed by sending frames
// on the target connections' outbound channels. Lifecycle is explicit: each
// connection has an inbound reader goroutine and an outbound pump goroutine that
// both exit on Disconnect or Close, and Close blocks until every goroutine has
// stopped, so no goroutines leak.

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

// SignalingErrorCode is the `code` of a ServerError frame. The string value is
// the wire form (snake_case), matching lazily-dart's enum wire tags.
type SignalingErrorCode string

const (
	// SignalingErrorBadMessage is an unparseable / malformed client frame.
	SignalingErrorBadMessage SignalingErrorCode = "bad_message"
	// SignalingErrorNotJoined is a signaling frame from a connection that has
	// not joined the session.
	SignalingErrorNotJoined SignalingErrorCode = "not_joined"
	// SignalingErrorAlreadyJoined is a join from a connection that already
	// joined.
	SignalingErrorAlreadyJoined SignalingErrorCode = "already_joined"
	// SignalingErrorDuplicatePeer is a join for a peer id already present in the
	// session.
	SignalingErrorDuplicatePeer SignalingErrorCode = "duplicate_peer"
	// SignalingErrorUnknownTarget is a directed frame to a peer not in the
	// session.
	SignalingErrorUnknownTarget SignalingErrorCode = "unknown_target"
	// SignalingErrorPermissionDenied is an allowlist-gated join or directed
	// frame that was not granted.
	SignalingErrorPermissionDenied SignalingErrorCode = "permission_denied"
)

// Wire returns the on-wire string form of the error code.
func (c SignalingErrorCode) Wire() string { return string(c) }

// SignalingMode is the permission mode for a signaling room.
type SignalingMode string

const (
	// SignalingModeOpen lets any peer join and signal any other joined peer.
	SignalingModeOpen SignalingMode = "open"
	// SignalingModeAllowlist is default-deny: peers require explicit grants, and
	// directed frames only reach allowed targets.
	SignalingModeAllowlist SignalingMode = "allowlist"
)

// ---------------------------------------------------------------------------
// Client -> Server messages (internally tagged union)
// ---------------------------------------------------------------------------

// ClientMessage is a client -> server signaling frame. It is a sealed union
// (mirroring the Dart `sealed class ClientMessage`); concrete variants are
// ClientJoin/ClientOffer/ClientAnswer/ClientIce/ClientRelay/ClientLeave. Decode
// wire bytes with ParseClientMessage; each variant implements MarshalJSON.
type ClientMessage interface {
	// Type returns the wire discriminant.
	Type() string
	MarshalJSON() ([]byte, error)
	isClientMessage()
}

// ClientJoin registers a connection with the session under a peer id. The
// optional Capabilities list is omitted from the wire when nil (an explicit
// empty list is preserved).
type ClientJoin struct {
	Peer         PeerId
	Capabilities []string
}

func (ClientJoin) isClientMessage() {}
func (ClientJoin) Type() string     { return "join" }

func (m ClientJoin) MarshalJSON() ([]byte, error) {
	if m.Capabilities == nil {
		return json.Marshal(struct {
			Type string `json:"type"`
			Peer PeerId `json:"peer"`
		}{"join", m.Peer})
	}
	return json.Marshal(struct {
		Type         string   `json:"type"`
		Peer         PeerId   `json:"peer"`
		Capabilities []string `json:"capabilities"`
	}{"join", m.Peer, m.Capabilities})
}

// ClientOffer carries a WebRTC SDP offer to a target peer.
type ClientOffer struct {
	To  PeerId
	Sdp string
}

func (ClientOffer) isClientMessage() {}
func (ClientOffer) Type() string     { return "offer" }

func (m ClientOffer) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		To   PeerId `json:"to"`
		Sdp  string `json:"sdp"`
	}{"offer", m.To, m.Sdp})
}

// ClientAnswer carries a WebRTC SDP answer to a target peer.
type ClientAnswer struct {
	To  PeerId
	Sdp string
}

func (ClientAnswer) isClientMessage() {}
func (ClientAnswer) Type() string     { return "answer" }

func (m ClientAnswer) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		To   PeerId `json:"to"`
		Sdp  string `json:"sdp"`
	}{"answer", m.To, m.Sdp})
}

// ClientIce carries an ICE candidate to a target peer.
type ClientIce struct {
	To        PeerId
	Candidate string
}

func (ClientIce) isClientMessage() {}
func (ClientIce) Type() string     { return "ice" }

func (m ClientIce) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string `json:"type"`
		To        PeerId `json:"to"`
		Candidate string `json:"candidate"`
	}{"ice", m.To, m.Candidate})
}

// ClientRelay relays an opaque JSON payload to a target peer. Payload is kept as
// json.RawMessage so it round-trips byte-for-byte through the server.
type ClientRelay struct {
	To      PeerId
	Payload json.RawMessage
}

func (ClientRelay) isClientMessage() {}
func (ClientRelay) Type() string     { return "relay" }

func (m ClientRelay) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type    string          `json:"type"`
		To      PeerId          `json:"to"`
		Payload json.RawMessage `json:"payload"`
	}{"relay", m.To, relayPayload(m.Payload)})
}

// ClientLeave disconnects the connection from the session.
type ClientLeave struct{}

func (ClientLeave) isClientMessage() {}
func (ClientLeave) Type() string     { return "leave" }

func (ClientLeave) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
	}{"leave"})
}

// ParseClientMessage decodes an internally-tagged client frame from JSON bytes.
func ParseClientMessage(data []byte) (ClientMessage, error) {
	var disc struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &disc); err != nil {
		return nil, fmt.Errorf("ClientMessage must be a JSON object with a type: %w", err)
	}
	switch disc.Type {
	case "join":
		var w struct {
			Peer         PeerId   `json:"peer"`
			Capabilities []string `json:"capabilities"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return ClientJoin{Peer: w.Peer, Capabilities: w.Capabilities}, nil
	case "offer":
		var w struct {
			To  PeerId `json:"to"`
			Sdp string `json:"sdp"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return ClientOffer{To: w.To, Sdp: w.Sdp}, nil
	case "answer":
		var w struct {
			To  PeerId `json:"to"`
			Sdp string `json:"sdp"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return ClientAnswer{To: w.To, Sdp: w.Sdp}, nil
	case "ice":
		var w struct {
			To        PeerId `json:"to"`
			Candidate string `json:"candidate"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return ClientIce{To: w.To, Candidate: w.Candidate}, nil
	case "relay":
		var w struct {
			To      PeerId          `json:"to"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return ClientRelay{To: w.To, Payload: w.Payload}, nil
	case "leave":
		return ClientLeave{}, nil
	default:
		return nil, fmt.Errorf("unknown ClientMessage type: %q", disc.Type)
	}
}

// ---------------------------------------------------------------------------
// Server -> Client messages (internally tagged union)
// ---------------------------------------------------------------------------

// ServerMessage is a server -> client signaling frame. It is a sealed union
// (mirroring the Dart `sealed class ServerMessage`); concrete variants are
// ServerWelcome/ServerPeerJoined/ServerPeerLeft/ServerOffer/ServerAnswer/
// ServerIce/ServerRelay/ServerError. Decode wire bytes with ParseServerMessage;
// each variant implements MarshalJSON.
type ServerMessage interface {
	// Type returns the wire discriminant.
	Type() string
	MarshalJSON() ([]byte, error)
	isServerMessage()
}

// ServerWelcome is sent to a joiner with the roster (excluding self). Peers is
// always emitted, as [] when empty.
type ServerWelcome struct {
	Peer  PeerId
	Peers []PeerId
}

func (ServerWelcome) isServerMessage() {}
func (ServerWelcome) Type() string     { return "welcome" }

func (m ServerWelcome) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type  string   `json:"type"`
		Peer  PeerId   `json:"peer"`
		Peers []PeerId `json:"peers"`
	}{"welcome", m.Peer, nonNilSlice(m.Peers)})
}

// ServerPeerJoined notifies existing peers that a new peer joined.
type ServerPeerJoined struct {
	Peer PeerId
}

func (ServerPeerJoined) isServerMessage() {}
func (ServerPeerJoined) Type() string     { return "peer-joined" }

func (m ServerPeerJoined) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		Peer PeerId `json:"peer"`
	}{"peer-joined", m.Peer})
}

// ServerPeerLeft notifies remaining peers that a peer disconnected.
type ServerPeerLeft struct {
	Peer PeerId
}

func (ServerPeerLeft) isServerMessage() {}
func (ServerPeerLeft) Type() string     { return "peer-left" }

func (m ServerPeerLeft) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		Peer PeerId `json:"peer"`
	}{"peer-left", m.Peer})
}

// ServerOffer is a forwarded WebRTC SDP offer, stamped with the sender's From.
type ServerOffer struct {
	From PeerId
	Sdp  string
}

func (ServerOffer) isServerMessage() {}
func (ServerOffer) Type() string     { return "offer" }

func (m ServerOffer) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		From PeerId `json:"from"`
		Sdp  string `json:"sdp"`
	}{"offer", m.From, m.Sdp})
}

// ServerAnswer is a forwarded WebRTC SDP answer, stamped with the sender's From.
type ServerAnswer struct {
	From PeerId
	Sdp  string
}

func (ServerAnswer) isServerMessage() {}
func (ServerAnswer) Type() string     { return "answer" }

func (m ServerAnswer) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		From PeerId `json:"from"`
		Sdp  string `json:"sdp"`
	}{"answer", m.From, m.Sdp})
}

// ServerIce is a forwarded ICE candidate, stamped with the sender's From.
type ServerIce struct {
	From      PeerId
	Candidate string
}

func (ServerIce) isServerMessage() {}
func (ServerIce) Type() string     { return "ice" }

func (m ServerIce) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string `json:"type"`
		From      PeerId `json:"from"`
		Candidate string `json:"candidate"`
	}{"ice", m.From, m.Candidate})
}

// ServerRelay is a forwarded opaque payload, stamped with the sender's From.
type ServerRelay struct {
	From    PeerId
	Payload json.RawMessage
}

func (ServerRelay) isServerMessage() {}
func (ServerRelay) Type() string     { return "relay" }

func (m ServerRelay) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type    string          `json:"type"`
		From    PeerId          `json:"from"`
		Payload json.RawMessage `json:"payload"`
	}{"relay", m.From, relayPayload(m.Payload)})
}

// ServerError reports a rejected client frame. Code is the wire string form of
// a SignalingErrorCode.
type ServerError struct {
	Code    string
	Message string
}

func (ServerError) isServerMessage() {}
func (ServerError) Type() string     { return "error" }

func (m ServerError) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type    string `json:"type"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}{"error", m.Code, m.Message})
}

// newServerError builds a ServerError from a typed code.
func newServerError(code SignalingErrorCode, message string) ServerError {
	return ServerError{Code: code.Wire(), Message: message}
}

// ParseServerMessage decodes an internally-tagged server frame from JSON bytes.
func ParseServerMessage(data []byte) (ServerMessage, error) {
	var disc struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &disc); err != nil {
		return nil, fmt.Errorf("ServerMessage must be a JSON object with a type: %w", err)
	}
	switch disc.Type {
	case "welcome":
		var w struct {
			Peer  PeerId   `json:"peer"`
			Peers []PeerId `json:"peers"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return ServerWelcome{Peer: w.Peer, Peers: w.Peers}, nil
	case "peer-joined":
		var w struct {
			Peer PeerId `json:"peer"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return ServerPeerJoined{Peer: w.Peer}, nil
	case "peer-left":
		var w struct {
			Peer PeerId `json:"peer"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return ServerPeerLeft{Peer: w.Peer}, nil
	case "offer":
		var w struct {
			From PeerId `json:"from"`
			Sdp  string `json:"sdp"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return ServerOffer{From: w.From, Sdp: w.Sdp}, nil
	case "answer":
		var w struct {
			From PeerId `json:"from"`
			Sdp  string `json:"sdp"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return ServerAnswer{From: w.From, Sdp: w.Sdp}, nil
	case "ice":
		var w struct {
			From      PeerId `json:"from"`
			Candidate string `json:"candidate"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return ServerIce{From: w.From, Candidate: w.Candidate}, nil
	case "relay":
		var w struct {
			From    PeerId          `json:"from"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return ServerRelay{From: w.From, Payload: w.Payload}, nil
	case "error":
		var w struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return ServerError{Code: w.Code, Message: w.Message}, nil
	default:
		return nil, fmt.Errorf("unknown ServerMessage type: %q", disc.Type)
	}
}

// relayPayload normalizes a nil relay payload to JSON null so the required
// `payload` field is always present on the wire.
func relayPayload(p json.RawMessage) json.RawMessage {
	if len(p) == 0 {
		return json.RawMessage("null")
	}
	return p
}

// ---------------------------------------------------------------------------
// RoutedFrame
// ---------------------------------------------------------------------------

// RoutedFrame pairs a target connection id with the ServerMessage to deliver on
// that connection. ConnID is an opaque, caller-supplied handle (mirroring the
// Dart `Object connId`); it must be a comparable value (usable as a map key).
type RoutedFrame struct {
	ConnID  any
	Message ServerMessage
}

// NewRoutedFrame constructs a RoutedFrame.
func NewRoutedFrame(connID any, message ServerMessage) RoutedFrame {
	return RoutedFrame{ConnID: connID, Message: message}
}

// ---------------------------------------------------------------------------
// SignalingRoom — channel-first owner goroutine
// ---------------------------------------------------------------------------

// ErrSignalingRoomClosed is returned by SignalingRoom methods after Close.
var ErrSignalingRoomClosed = errors.New("signaling room is closed")

// ErrConnAlreadyExists is returned by Connect when connID is already connected.
var ErrConnAlreadyExists = errors.New("signaling connection already exists")

// ClientConn is a per-connection handle into a SignalingRoom. Send client
// frames on the inbound channel (Inbound) and read server frames from the
// outbound channel (Outbound). The outbound channel is closed when the
// connection is disconnected or the room is closed.
type ClientConn struct {
	connID any
	room   *SignalingRoom

	inbound chan ClientMessage // transport -> room
	feed    chan ServerMessage // owner -> pump (owner never blocks; pump is always ready)
	out     chan ServerMessage // pump -> transport (public read side)
	closed  chan struct{}      // closed on teardown to stop the reader/pump
}

// ConnID returns the opaque connection id.
func (c *ClientConn) ConnID() any { return c.connID }

// Inbound is the send side for client -> room frames.
func (c *ClientConn) Inbound() chan<- ClientMessage { return c.inbound }

// Outbound is the receive side for room -> client frames. It is closed when the
// connection is torn down.
func (c *ClientConn) Outbound() <-chan ServerMessage { return c.out }

// SignalingRoom is a transport-agnostic signaling room. A single owner
// goroutine owns all room and per-connection state; callers interact only
// through channels and the (channel-backed) methods below. It never interprets
// CRDT state.
type SignalingRoom struct {
	cmds      chan func(*roomOwner)
	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// roomOwner is the state owned exclusively by the owner goroutine. It is never
// touched outside a command closure, so it needs no locks.
type roomOwner struct {
	room *SignalingRoom
	mode SignalingMode

	connToPeer    map[any]PeerId
	peerToConn    map[PeerId]any
	joinAllowed   map[PeerId]struct{}
	signalAllowed map[PeerId]map[PeerId]struct{}

	conns map[any]*ClientConn
}

// NewSignalingRoom starts a signaling room in the given mode and launches its
// owner goroutine. Call Close to stop the room and release its goroutines.
func NewSignalingRoom(mode SignalingMode) *SignalingRoom {
	if mode == "" {
		mode = SignalingModeOpen
	}
	r := &SignalingRoom{
		cmds: make(chan func(*roomOwner)),
		done: make(chan struct{}),
	}
	o := &roomOwner{
		room:          r,
		mode:          mode,
		connToPeer:    map[any]PeerId{},
		peerToConn:    map[PeerId]any{},
		joinAllowed:   map[PeerId]struct{}{},
		signalAllowed: map[PeerId]map[PeerId]struct{}{},
		conns:         map[any]*ClientConn{},
	}
	r.wg.Add(1)
	go r.run(o)
	return r
}

func (r *SignalingRoom) run(o *roomOwner) {
	defer r.wg.Done()
	for {
		select {
		case fn := <-r.cmds:
			fn(o)
		case <-r.done:
			o.shutdown()
			return
		}
	}
}

// dispatch submits a command to the owner goroutine. It returns false if the
// room has been closed.
func (r *SignalingRoom) dispatch(fn func(*roomOwner)) bool {
	select {
	case r.cmds <- fn:
		return true
	case <-r.done:
		return false
	}
}

// Connect registers a connection under connID and returns its *ClientConn. The
// connID must be a comparable value. It errors if the room is closed or connID
// is already connected.
func (r *SignalingRoom) Connect(connID any) (*ClientConn, error) {
	type result struct {
		conn *ClientConn
		err  error
	}
	reply := make(chan result, 1)
	if !r.dispatch(func(o *roomOwner) {
		conn, err := o.connect(connID)
		reply <- result{conn, err}
	}) {
		return nil, ErrSignalingRoomClosed
	}
	select {
	case res := <-reply:
		return res.conn, res.err
	case <-r.done:
		return nil, ErrSignalingRoomClosed
	}
}

// Disconnect tears down the connection connID, emitting peer-left to remaining
// peers and closing the connection's outbound channel.
func (r *SignalingRoom) Disconnect(connID any) error {
	done := make(chan struct{}, 1)
	if !r.dispatch(func(o *roomOwner) {
		o.disconnect(connID)
		done <- struct{}{}
	}) {
		return ErrSignalingRoomClosed
	}
	select {
	case <-done:
		return nil
	case <-r.done:
		return ErrSignalingRoomClosed
	}
}

// AllowJoin grants peer permission to join in allowlist mode (no-op semantics in
// open mode, where every peer may already join).
func (r *SignalingRoom) AllowJoin(peer PeerId) error {
	return r.mutate(func(o *roomOwner) { o.joinAllowed[peer] = struct{}{} })
}

// AllowSignal grants from permission to send directed frames to target in
// allowlist mode.
func (r *SignalingRoom) AllowSignal(from, target PeerId) error {
	return r.mutate(func(o *roomOwner) {
		set := o.signalAllowed[from]
		if set == nil {
			set = map[PeerId]struct{}{}
			o.signalAllowed[from] = set
		}
		set[target] = struct{}{}
	})
}

func (r *SignalingRoom) mutate(fn func(*roomOwner)) error {
	done := make(chan struct{}, 1)
	if !r.dispatch(func(o *roomOwner) {
		fn(o)
		done <- struct{}{}
	}) {
		return ErrSignalingRoomClosed
	}
	select {
	case <-done:
		return nil
	case <-r.done:
		return ErrSignalingRoomClosed
	}
}

// Roster returns the joined peer ids, sorted ascending.
func (r *SignalingRoom) Roster() []PeerId {
	reply := make(chan []PeerId, 1)
	if !r.dispatch(func(o *roomOwner) { reply <- o.roster() }) {
		return nil
	}
	select {
	case res := <-reply:
		return res
	case <-r.done:
		return nil
	}
}

// Size returns the number of joined peers.
func (r *SignalingRoom) Size() int {
	reply := make(chan int, 1)
	if !r.dispatch(func(o *roomOwner) { reply <- len(o.peerToConn) }) {
		return 0
	}
	select {
	case res := <-reply:
		return res
	case <-r.done:
		return 0
	}
}

// Close stops the room. It tears down every connection (closing outbound
// channels) and blocks until all owner/reader/pump goroutines have exited.
func (r *SignalingRoom) Close() {
	r.closeOnce.Do(func() { close(r.done) })
	r.wg.Wait()
}

// ---------------------------------------------------------------------------
// Owner-goroutine state transitions (mirror lazily-dart SignalingRoom)
// ---------------------------------------------------------------------------

func (o *roomOwner) connect(connID any) (*ClientConn, error) {
	if _, exists := o.conns[connID]; exists {
		return nil, ErrConnAlreadyExists
	}
	c := &ClientConn{
		connID:  connID,
		room:    o.room,
		inbound: make(chan ClientMessage),
		feed:    make(chan ServerMessage),
		out:     make(chan ServerMessage),
		closed:  make(chan struct{}),
	}
	o.conns[connID] = c
	o.room.wg.Add(2)
	go c.pump(&o.room.wg)
	go c.readLoop(&o.room.wg)
	return c, nil
}

// receive processes an inbound client frame and delivers the resulting frames.
func (o *roomOwner) receive(connID any, message ClientMessage) {
	o.dispatchFrames(o.route(connID, message))
}

// disconnect removes a connection, delivers peer-left to survivors, and tears
// the connection down.
func (o *roomOwner) disconnect(connID any) {
	o.dispatchFrames(o.leave(connID))
	if c, ok := o.conns[connID]; ok {
		delete(o.conns, connID)
		c.teardown()
	}
}

// route is the pure routing logic (lazily-dart SignalingRoom.receive): it
// mutates peer state and returns the frames to deliver.
func (o *roomOwner) route(connID any, message ClientMessage) []RoutedFrame {
	var out []RoutedFrame
	switch m := message.(type) {
	case ClientJoin:
		out = o.join(out, connID, m.Peer)
	case ClientLeave:
		out = o.leaveInto(out, connID)
	case ClientOffer:
		out = o.forward(out, connID, m.To, func(from PeerId) ServerMessage {
			return ServerOffer{From: from, Sdp: m.Sdp}
		})
	case ClientAnswer:
		out = o.forward(out, connID, m.To, func(from PeerId) ServerMessage {
			return ServerAnswer{From: from, Sdp: m.Sdp}
		})
	case ClientIce:
		out = o.forward(out, connID, m.To, func(from PeerId) ServerMessage {
			return ServerIce{From: from, Candidate: m.Candidate}
		})
	case ClientRelay:
		out = o.forward(out, connID, m.To, func(from PeerId) ServerMessage {
			return ServerRelay{From: from, Payload: m.Payload}
		})
	}
	return out
}

// leave handles an external disconnection (lazily-dart SignalingRoom.disconnect).
func (o *roomOwner) leave(connID any) []RoutedFrame {
	var out []RoutedFrame
	return o.leaveInto(out, connID)
}

func (o *roomOwner) join(out []RoutedFrame, connID any, peer PeerId) []RoutedFrame {
	if _, joined := o.connToPeer[connID]; joined {
		return o.errFrame(out, connID, SignalingErrorAlreadyJoined, "connection already joined")
	}
	if o.mode == SignalingModeAllowlist {
		if _, ok := o.joinAllowed[peer]; !ok {
			return o.errFrame(out, connID, SignalingErrorPermissionDenied,
				fmt.Sprintf("peer %d is not allowed to join", peer))
		}
	}
	if _, dup := o.peerToConn[peer]; dup {
		return o.errFrame(out, connID, SignalingErrorDuplicatePeer,
			fmt.Sprintf("peer %d is already in this session", peer))
	}
	o.connToPeer[connID] = peer
	o.peerToConn[peer] = connID

	// Welcome the joiner (roster excludes self).
	others := make([]PeerId, 0, len(o.peerToConn))
	for _, p := range o.roster() {
		if p != peer {
			others = append(others, p)
		}
	}
	out = append(out, RoutedFrame{ConnID: connID, Message: ServerWelcome{Peer: peer, Peers: others}})

	// Notify all other peers.
	for _, otherPeer := range o.roster() {
		if otherPeer == peer {
			continue
		}
		out = append(out, RoutedFrame{ConnID: o.peerToConn[otherPeer], Message: ServerPeerJoined{Peer: peer}})
	}
	return out
}

func (o *roomOwner) leaveInto(out []RoutedFrame, connID any) []RoutedFrame {
	peer, ok := o.connToPeer[connID]
	if !ok {
		return out
	}
	delete(o.connToPeer, connID)
	delete(o.peerToConn, peer)
	for otherConn := range o.connToPeer {
		out = append(out, RoutedFrame{ConnID: otherConn, Message: ServerPeerLeft{Peer: peer}})
	}
	return out
}

func (o *roomOwner) forward(out []RoutedFrame, connID any, to PeerId, build func(from PeerId) ServerMessage) []RoutedFrame {
	from, ok := o.connToPeer[connID]
	if !ok {
		return o.errFrame(out, connID, SignalingErrorNotJoined, "join before signaling")
	}
	if o.mode == SignalingModeAllowlist {
		allowed, has := o.signalAllowed[from]
		if !has {
			return o.errFrame(out, connID, SignalingErrorPermissionDenied,
				fmt.Sprintf("peer %d is not allowed to signal peer %d", from, to))
		}
		if _, ok := allowed[to]; !ok {
			return o.errFrame(out, connID, SignalingErrorPermissionDenied,
				fmt.Sprintf("peer %d is not allowed to signal peer %d", from, to))
		}
	}
	targetConn, ok := o.peerToConn[to]
	if !ok {
		return o.errFrame(out, connID, SignalingErrorUnknownTarget,
			fmt.Sprintf("peer %d is not in this session", to))
	}
	return append(out, RoutedFrame{ConnID: targetConn, Message: build(from)})
}

func (o *roomOwner) errFrame(out []RoutedFrame, connID any, code SignalingErrorCode, msg string) []RoutedFrame {
	return append(out, RoutedFrame{ConnID: connID, Message: newServerError(code, msg)})
}

func (o *roomOwner) roster() []PeerId {
	peers := make([]PeerId, 0, len(o.peerToConn))
	for p := range o.peerToConn {
		peers = append(peers, p)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	return peers
}

// dispatchFrames delivers each RoutedFrame onto its target connection's feed.
// The owner never blocks: each connection's pump is always ready to receive on
// feed. Frames for unknown connections are dropped (their consumer is gone).
func (o *roomOwner) dispatchFrames(frames []RoutedFrame) {
	for _, f := range frames {
		if c, ok := o.conns[f.ConnID]; ok {
			c.feed <- f.Message
		}
	}
}

// shutdown tears down every connection when the room closes.
func (o *roomOwner) shutdown() {
	for connID, c := range o.conns {
		delete(o.conns, connID)
		c.teardown()
	}
}

// teardown stops a connection's reader and pump. Closing feed makes the pump
// drain and close out; closing closed stops the reader.
func (c *ClientConn) teardown() {
	close(c.closed)
	close(c.feed)
}

// ---------------------------------------------------------------------------
// Per-connection goroutines
// ---------------------------------------------------------------------------

// readLoop forwards inbound client frames to the owner as receive commands. It
// exits when the connection is torn down or the room closes.
func (c *ClientConn) readLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case msg, ok := <-c.inbound:
			if !ok {
				return
			}
			c.room.dispatch(func(o *roomOwner) { o.receive(c.connID, msg) })
		case <-c.closed:
			return
		case <-c.room.done:
			return
		}
	}
}

// pump copies frames from the owner-fed queue to the public outbound channel.
// It uses an internal FIFO buffer so the owner never blocks on a slow consumer.
// When feed is closed (teardown) it closes out and exits, so it never leaks.
func (c *ClientConn) pump(wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(c.out)
	var buf []ServerMessage
	for {
		if len(buf) == 0 {
			m, ok := <-c.feed
			if !ok {
				return
			}
			buf = append(buf, m)
			continue
		}
		select {
		case m, ok := <-c.feed:
			if !ok {
				return
			}
			buf = append(buf, m)
		case c.out <- buf[0]:
			buf = buf[1:]
		}
	}
}
