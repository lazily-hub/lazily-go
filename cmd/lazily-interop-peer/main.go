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
	"strconv"

	lazily "github.com/lazily-hub/lazily-go"
)

const protocolVersion = 1

type request struct {
	Cmd             string          `json:"cmd"`
	Feature         string          `json:"feature"`
	Step            json.RawMessage `json:"step"`
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
	stdlib  map[string]*stdlibFeature
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
	case "feature_reset":
		return p.featureReset(req)
	case "feature_step":
		return p.featureStep(req)
	case "feature_observe":
		return p.featureObserve(req)
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
	p.stdlib = make(map[string]*stdlibFeature)
	return map[string]any{
		"ok":               true,
		"binding":          "lazily-go",
		"version":          "0.23.2",
		"protocol_version": protocolVersion,
		"features": []string{
			"distributed_crdt",
			"stdlib_timer_v1",
			"stdlib_timeout_v1",
			"stdlib_revision_barrier_v1",
		},
		"codecs":           []string{"json"},
		"channels":         []string{},
		"channel_variants": map[string][]string{},
		"platform_profile": "portable",
		"carve_outs":       []string{"msgpack", "transport_links"},
	}
}

type wireUint64 uint64

func (value *wireUint64) UnmarshalJSON(data []byte) error {
	text := string(data)
	if len(data) > 0 && data[0] == '"' {
		if err := json.Unmarshal(data, &text); err != nil {
			return err
		}
	}
	parsed, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return fmt.Errorf("expected unsigned decimal integer: %w", err)
	}
	*value = wireUint64(parsed)
	return nil
}

type featureStep struct {
	Op               string      `json:"op"`
	Now              wireUint64  `json:"now"`
	Duration         wireUint64  `json:"duration"`
	Operation        string      `json:"operation"`
	Value            string      `json:"value"`
	Cancellation     string      `json:"cancellation"`
	Revision         wireUint64  `json:"revision"`
	RequiredRevision wireUint64  `json:"required_revision"`
	ObservedRevision wireUint64  `json:"observed_revision"`
	Deadline         *wireUint64 `json:"deadline"`
	Predicate        bool        `json:"predicate"`
	Key              string      `json:"key"`
}

func (step featureStep) deadlineValue() *uint64 {
	if step.Deadline == nil {
		return nil
	}
	value := uint64(*step.Deadline)
	return &value
}

type stdlibFeature struct {
	name     string
	timer    *lazily.Timer
	timeout  *lazily.Timeout[string]
	barrier  *lazily.RevisionBarrier
	deadline uint64
	last     map[string]any
}

func supportedFeature(feature string) bool {
	switch feature {
	case "stdlib_timer_v1", "stdlib_timeout_v1", "stdlib_revision_barrier_v1":
		return true
	default:
		return false
	}
}

func (p *peer) featureReset(req request) (any, error) {
	if !supportedFeature(req.Feature) {
		return map[string]any{
			"ok": false, "error": "unsupported feature " + req.Feature, "unsupported": true,
		}, nil
	}
	if p.stdlib == nil {
		p.stdlib = make(map[string]*stdlibFeature)
	}
	p.stdlib[req.Feature] = &stdlibFeature{name: req.Feature}
	return map[string]any{"ok": true, "feature": req.Feature}, nil
}

func (p *peer) featureStep(req request) (any, error) {
	feature := p.stdlib[req.Feature]
	if feature == nil {
		return nil, fmt.Errorf("feature %s must be reset before stepping", req.Feature)
	}
	var step featureStep
	if err := json.Unmarshal(req.Step, &step); err != nil {
		return nil, fmt.Errorf("invalid feature step: %w", err)
	}
	observation, err := feature.step(step)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"ok": true, "feature": req.Feature, "observation": observation,
	}, nil
}

func (p *peer) featureObserve(req request) (any, error) {
	feature := p.stdlib[req.Feature]
	if feature == nil {
		return nil, fmt.Errorf("feature %s must be reset before observation", req.Feature)
	}
	if feature.last == nil {
		return nil, fmt.Errorf("feature %s has no observation", req.Feature)
	}
	return map[string]any{
		"ok": true, "feature": req.Feature, "observation": feature.last,
	}, nil
}

func (f *stdlibFeature) step(step featureStep) (map[string]any, error) {
	var (
		observation map[string]any
		err         error
	)
	switch f.name {
	case "stdlib_timer_v1":
		observation, err = f.timerStep(step)
	case "stdlib_timeout_v1":
		observation, err = f.timeoutStep(step)
	case "stdlib_revision_barrier_v1":
		observation, err = f.barrierStep(step)
	default:
		err = fmt.Errorf("unsupported feature %s", f.name)
	}
	if err == nil {
		f.last = observation
	}
	return observation, err
}

func (f *stdlibFeature) timerStep(step featureStep) (map[string]any, error) {
	switch step.Op {
	case "start":
		timer, err := lazily.NewTimer(uint64(step.Now), uint64(step.Duration))
		if err == lazily.TimerDeadlineOverflow {
			f.timer = nil
			return map[string]any{
				"outcome": "unavailable", "reason": string(lazily.TimerDeadlineOverflow),
			}, nil
		}
		if err != nil {
			return nil, err
		}
		f.timer = timer
		f.deadline, _ = lazily.CheckedDeadline(uint64(step.Now), uint64(step.Duration))
		return map[string]any{"outcome": "pending", "deadline": f.deadline}, nil
	case "observe":
		if f.timer == nil {
			return nil, errors.New("timer feature is not started")
		}
		value, err := f.timer.Observe(uint64(step.Now))
		if err != nil {
			return map[string]any{
				"outcome": "unavailable", "reason": err.Error(), "deadline": value.Deadline,
			}, nil
		}
		if value.Outcome == "fired" {
			return map[string]any{"outcome": "fired", "fired_at": value.FiredAt}, nil
		}
		return map[string]any{"outcome": "pending", "deadline": value.Deadline}, nil
	default:
		return nil, fmt.Errorf("unsupported timer feature step %s", step.Op)
	}
}

func (f *stdlibFeature) timeoutStep(step featureStep) (map[string]any, error) {
	switch step.Op {
	case "start":
		timeout, err := lazily.NewTimeout[string](uint64(step.Now), uint64(step.Duration))
		if err != nil {
			return nil, err
		}
		f.timeout = timeout
		f.deadline, _ = lazily.CheckedDeadline(uint64(step.Now), uint64(step.Duration))
		return map[string]any{"outcome": "pending", "deadline": f.deadline}, nil
	case "poll":
		if f.timeout == nil {
			return nil, errors.New("timeout feature is not started")
		}
		operationCalls, cancellationCalls := 0, 0
		value := f.timeout.Poll(uint64(step.Now), func() lazily.TimeoutOperation[string] {
			operationCalls++
			switch step.Operation {
			case "pending":
				return lazily.PendingOperation[string]()
			case "completed":
				return lazily.CompletedOperation(step.Value)
			case "unavailable":
				return lazily.UnavailableOperation[string]()
			default:
				return lazily.UnavailableOperation[string]()
			}
		}, func() lazily.TimeoutCancellation {
			cancellationCalls++
			return lazily.TimeoutCancellation(step.Cancellation)
		})
		observation := map[string]any{
			"outcome":         value.Outcome,
			"operation_calls": operationCalls, "cancellation_calls": cancellationCalls,
		}
		if value.Outcome == "pending" {
			observation["deadline"] = value.Deadline
		}
		if value.Outcome == "completed" {
			observation["value"] = value.Value
		}
		if value.Reason != "" {
			observation["reason"] = value.Reason
		}
		return observation, nil
	default:
		return nil, fmt.Errorf("unsupported timeout feature step %s", step.Op)
	}
}

func (f *stdlibFeature) barrierStep(step featureStep) (map[string]any, error) {
	var (
		value             lazily.RevisionBarrierObservation
		cancellationCalls int
	)
	switch step.Op {
	case "start":
		f.barrier = lazily.NewRevisionBarrier(
			uint64(step.Revision),
			uint64(step.RequiredRevision),
			step.deadlineValue(),
		)
		value = f.barrier.Receipt("")
	case "observe":
		if f.barrier == nil {
			return nil, errors.New("barrier feature is not started")
		}
		value = f.barrier.Observe(uint64(step.Now), step.Predicate, func() lazily.TimeoutCancellation {
			cancellationCalls++
			return lazily.TimeoutCancellation(step.Cancellation)
		})
	case "register_recheck":
		value = f.barrier.RegisterRecheck(
			uint64(step.Now),
			uint64(step.ObservedRevision),
			step.Predicate,
		)
	case "advance":
		value = f.barrier.Advance(uint64(step.Revision), step.Predicate)
	case "dispose":
		value = f.barrier.Dispose()
	case "receipt":
		value = f.barrier.Receipt(step.Key)
	default:
		return nil, fmt.Errorf("unsupported revision barrier feature step %s", step.Op)
	}
	observation := map[string]any{
		"outcome":  value.Outcome,
		"revision": value.Revision, "generation": value.Generation,
	}
	if value.Reason != "" {
		observation["reason"] = value.Reason
	}
	if step.Op == "observe" {
		observation["cancellation_calls"] = cancellationCalls
	}
	return observation, nil
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
	featureCases := []struct {
		feature string
		steps   []json.RawMessage
		outcome string
	}{
		{
			"stdlib_timer_v1",
			[]json.RawMessage{
				json.RawMessage(`{"op":"start","now":0,"duration":0}`),
				json.RawMessage(`{"op":"observe","now":0}`),
			},
			"fired",
		},
		{
			"stdlib_timeout_v1",
			[]json.RawMessage{
				json.RawMessage(`{"op":"start","now":0,"duration":1}`),
				json.RawMessage(`{"op":"poll","now":0,"operation":"completed","value":"ok","cancellation":"pending"}`),
			},
			"completed",
		},
		{
			"stdlib_revision_barrier_v1",
			[]json.RawMessage{
				json.RawMessage(`{"op":"start","revision":1,"required_revision":1,"deadline":null}`),
				json.RawMessage(`{"op":"observe","now":0,"predicate":true,"cancellation":"pending"}`),
			},
			"satisfied",
		},
	}
	for _, test := range featureCases {
		if _, err := p.handle(request{Cmd: "feature_reset", Feature: test.feature}); err != nil {
			return fmt.Errorf("%s reset self-check: %w", test.feature, err)
		}
		for _, step := range test.steps {
			if _, err := p.handle(request{
				Cmd: "feature_step", Feature: test.feature, Step: step,
			}); err != nil {
				return fmt.Errorf("%s step self-check: %w", test.feature, err)
			}
		}
		observed, err := p.handle(request{Cmd: "feature_observe", Feature: test.feature})
		if err != nil {
			return fmt.Errorf("%s observe self-check: %w", test.feature, err)
		}
		raw, _ := json.Marshal(observed)
		var response struct {
			Observation struct {
				Outcome string `json:"outcome"`
			} `json:"observation"`
		}
		if err := json.Unmarshal(raw, &response); err != nil || response.Observation.Outcome != test.outcome {
			return fmt.Errorf("%s observation self-check failed: %s", test.feature, raw)
		}
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
