// Command lazily-interop-peer is NDJSON test infrastructure for the
// cross-binding Lazily interoperability suite. It delegates IPC parsing and all
// CRDT ordering/dedup decisions to the production lazily package.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	lazily "github.com/lazily-hub/lazily-go"
)

const protocolVersion = 1

type request struct {
	Cmd             string          `json:"cmd"`
	Peer            lazily.PeerId   `json:"peer"`
	ProtocolVersion int             `json:"protocol_version"`
	Node            lazily.NodeId   `json:"node"`
	Key             *string         `json:"key"`
	State           json.RawMessage `json:"state"`
	At              int64           `json:"at"`
	Frame           json.RawMessage `json:"frame"`
}

type peer struct {
	peerID  lazily.PeerId
	logical int64
	runtime *lazily.CrdtPlaneRuntime
}

func (p *peer) close() {
	if p.runtime != nil {
		p.runtime.Close()
		p.runtime = nil
	}
}

func (p *peer) handle(req request) (any, error) {
	switch req.Cmd {
	case "hello":
		return p.hello(req), nil
	case "local_set":
		return p.localSet(req)
	case "deliver":
		return p.deliver(req)
	case "snapshot":
		return p.snapshot()
	case "bye":
		return map[string]any{"ok": true}, nil
	case "link_open", "link_send", "link_recv", "link_close", "link_stats":
		return map[string]any{
			"ok":          false,
			"error":       "unsupported channel",
			"unsupported": true,
		}, nil
	default:
		return map[string]any{"ok": false, "error": "unknown command"}, nil
	}
}

func (p *peer) hello(req request) any {
	if req.ProtocolVersion != protocolVersion {
		return map[string]any{
			"ok":    false,
			"error": "unsupported protocol_version",
		}
	}
	p.close()
	p.peerID = req.Peer
	p.logical = 0
	p.runtime = lazily.NewCrdtPlaneRuntime(req.Peer)
	return map[string]any{
		"ok":               true,
		"binding":          "lazily-go",
		"version":          "0.23.2",
		"protocol_version": protocolVersion,
		"features":         []string{"distributed_crdt"},
		"codecs":           []string{"json"},
		"channels":         []string{},
		"channel_variants": map[string][]string{},
		"platform_profile": "portable",
		"carve_outs":       []string{"msgpack", "transport_links"},
	}
}

func (p *peer) localSet(req request) (any, error) {
	runtime, err := p.ready()
	if err != nil {
		return nil, err
	}
	p.logical++
	opWire := struct {
		Node  lazily.NodeId    `json:"node"`
		Key   *string          `json:"key"`
		Stamp lazily.WireStamp `json:"stamp"`
		State json.RawMessage  `json:"state"`
	}{
		Node:  req.Node,
		Key:   req.Key,
		Stamp: lazily.NewWireStamp(req.At, p.logical, p.peerID),
		State: req.State,
	}
	encoded, err := json.Marshal(opWire)
	if err != nil {
		return nil, err
	}
	var op lazily.CrdtOp
	if err := json.Unmarshal(encoded, &op); err != nil {
		return nil, err
	}
	if runtime.Ingest(lazily.CrdtSync{Ops: []lazily.CrdtOp{op}}) != 1 {
		return nil, errors.New("production runtime rejected fresh local op")
	}
	message := lazily.IpcMessageCrdtSync{Value: lazily.CrdtSync{
		Frontier: runtime.FrontierEntries(),
		Ops:      []lazily.CrdtOp{op},
	}}
	frame, err := message.EncodeJSON()
	if err != nil {
		return nil, err
	}
	return struct {
		OK    bool            `json:"ok"`
		Frame json.RawMessage `json:"frame"`
	}{true, frame}, nil
}

func (p *peer) deliver(req request) (any, error) {
	runtime, err := p.ready()
	if err != nil {
		return nil, err
	}
	message, err := lazily.DecodeIpcMessageJSON(req.Frame)
	if err != nil {
		return nil, err
	}
	syncMessage, ok := message.(lazily.IpcMessageCrdtSync)
	if !ok {
		return nil, errors.New("deliver requires CrdtSync")
	}
	return map[string]any{
		"ok":      true,
		"applied": runtime.Ingest(syncMessage.Value),
	}, nil
}

func (p *peer) snapshot() (any, error) {
	runtime, err := p.ready()
	if err != nil {
		return nil, err
	}
	type cell struct {
		Node  lazily.NodeId   `json:"node"`
		Key   *string         `json:"key"`
		State lazily.IpcValue `json:"state"`
	}
	entries := runtime.Converged()
	cells := make([]cell, 0, len(entries))
	for _, entry := range entries {
		cells = append(cells, cell{
			Node: entry.Node, Key: entry.Key, State: entry.State,
		})
	}
	return struct {
		OK    bool   `json:"ok"`
		Cells []cell `json:"cells"`
	}{true, cells}, nil
}

func (p *peer) ready() (*lazily.CrdtPlaneRuntime, error) {
	if p.runtime == nil {
		return nil, errors.New("hello must run first")
	}
	return p.runtime, nil
}

func selfCheck() error {
	var p peer
	defer p.close()
	hello, err := p.handle(request{
		Cmd: "hello", Peer: 1, ProtocolVersion: protocolVersion,
	})
	if err != nil {
		return err
	}
	helloBytes, _ := json.Marshal(hello)
	if !json.Valid(helloBytes) {
		return errors.New("hello self-check failed")
	}
	local, err := p.handle(request{
		Cmd: "local_set", Node: 7, State: json.RawMessage(`{"Inline":[65]}`), At: 10,
	})
	if err != nil {
		return err
	}
	localBytes, err := json.Marshal(local)
	if err != nil {
		return err
	}
	var response struct {
		Frame json.RawMessage `json:"frame"`
	}
	if err := json.Unmarshal(localBytes, &response); err != nil {
		return err
	}
	duplicate, err := p.handle(request{Cmd: "deliver", Frame: response.Frame, At: 11})
	if err != nil {
		return err
	}
	duplicateBytes, _ := json.Marshal(duplicate)
	if string(duplicateBytes) != `{"applied":0,"ok":true}` {
		return fmt.Errorf("duplicate self-check failed: %s", duplicateBytes)
	}
	return nil
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--self-check" {
		if err := selfCheck(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "lazily-go interop peer self-check: ok")
		return
	}

	var p peer
	defer p.close()
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	output := bufio.NewWriter(os.Stdout)
	for scanner.Scan() {
		var req request
		var response any
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			response = map[string]any{"ok": false, "error": err.Error()}
		} else {
			var err error
			response, err = p.handle(req)
			if err != nil {
				response = map[string]any{"ok": false, "error": err.Error()}
			}
		}
		encoded, _ := json.Marshal(response)
		_, _ = output.Write(encoded)
		_ = output.WriteByte('\n')
		_ = output.Flush()
		if req.Cmd == "bye" {
			return
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
