package lazily

import (
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"
)

// SimWorld is Lazily's deterministic, single-threaded execution world. It is
// deliberately a small scheduler, not an operating-system scheduler: every
// source of work is an explicit action, logical time is advanced manually, and
// actors are invoked synchronously one at a time.
const (
	SimContractVersion   = "lazily.sim/1.0"
	SimTraceSchema       = 1
	SimRNGAlgorithm      = "lazily-sim-rng-v1"
	SimDigestAlgorithm   = "sha256/v1"
	SimSchedulerKind     = "lazily.sim.queue"
	SimSchedulerVersion  = "1"
	SimActionVersion     = "1"
	SimTraceEntryVersion = "1"
)

var ErrSimulation = errors.New("lazily: deterministic simulation failed")

type simBoundError struct{ limit string }

func (e simBoundError) Error() string { return "simulation bound exhausted: " + e.limit }
func (e simBoundError) Unwrap() error { return ErrSimulation }

func simErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrSimulation}, args...)...)
}

// SimSeed is the 32-byte root identity from which named random streams derive.
type SimSeed [32]byte

func ParseSimSeed(value string) (SimSeed, error) {
	var seed SimSeed
	if len(value) != hex.EncodedLen(len(seed)) {
		return seed, simErrorf("seed must be exactly 32 bytes of lowercase hexadecimal")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || value != strings.ToLower(value) {
		return seed, simErrorf("seed must be exactly 32 bytes of lowercase hexadecimal")
	}
	copy(seed[:], decoded)
	return seed, nil
}

func (s SimSeed) String() string { return hex.EncodeToString(s[:]) }

// SimRandomStream is a deterministic named counter stream. Streams do not use
// math/rand, process state, time, or concurrency.
type SimRandomStream struct {
	key       [32]byte
	nextBlock uint64
	block     [32]byte
	offset    int
	exhausted bool
	active    func() bool
}

func newSimRandomStream(seed SimSeed, labels []string) (*SimRandomStream, error) {
	h := sha256.New()
	_, _ = h.Write([]byte("lazily.sim.rng.v1\x00"))
	_, _ = h.Write(seed[:])
	var size [4]byte
	for _, label := range labels {
		if label == "" || !utf8.ValidString(label) || uint64(len(label)) > math.MaxUint32 {
			return nil, simErrorf("random-stream labels must be nonempty valid UTF-8 strings of at most %d bytes", uint64(math.MaxUint32))
		}
		binary.BigEndian.PutUint32(size[:], uint32(len(label)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(label))
	}
	stream := &SimRandomStream{offset: sha256.Size}
	copy(stream.key[:], h.Sum(nil))
	return stream, nil
}

func (s *SimRandomStream) refill() error {
	if s.exhausted {
		return simErrorf("random-stream block counter exhausted")
	}
	h := sha256.New()
	_, _ = h.Write([]byte("lazily.sim.block.v1\x00"))
	_, _ = h.Write(s.key[:])
	var counter [8]byte
	binary.BigEndian.PutUint64(counter[:], s.nextBlock)
	_, _ = h.Write(counter[:])
	copy(s.block[:], h.Sum(nil))
	if s.nextBlock == math.MaxUint64 {
		s.exhausted = true
	} else {
		s.nextBlock++
	}
	s.offset = 0
	return nil
}

func (s *SimRandomStream) Read(dst []byte) (int, error) {
	if s.active != nil && !s.active() {
		return 0, simErrorf("random stream belongs to a terminal world")
	}
	written := 0
	for len(dst) > 0 {
		if s.offset == len(s.block) {
			if err := s.refill(); err != nil {
				return written, err
			}
		}
		n := copy(dst, s.block[s.offset:])
		s.offset += n
		dst = dst[n:]
		written += n
	}
	return written, nil
}

func (s *SimRandomStream) Uint64() (uint64, error) {
	var raw [8]byte
	if _, err := io.ReadFull(s, raw[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(raw[:]), nil
}

// Uint64N returns a uniform value in [0, bound), using rejection sampling.
func (s *SimRandomStream) Uint64N(bound uint64) (uint64, error) {
	if bound == 0 {
		return 0, simErrorf("a bounded random draw needs a nonzero bound")
	}
	threshold := -bound % bound
	for {
		value, err := s.Uint64()
		if err != nil {
			return 0, err
		}
		if value >= threshold {
			return value % bound, nil
		}
	}
}

type SimAction struct {
	ID      string
	ActorID string
	Kind    string
	Version string
	Payload any
	CauseID string
}

// SimMessage is an action addressed to an actor. The alias makes message-style
// call sites legible without creating a second scheduling model.
type SimMessage = SimAction

type SimScheduledAction struct {
	At     uint64
	Action SimAction
}

type SimObservation struct {
	ID      string
	Kind    string
	Version string
	Value   any
}

// SimDecision is the ordered result of one production decision invocation.
type SimDecision struct {
	Accepted     bool
	Schedule     []SimScheduledAction
	Observations []SimObservation
}

// SimActor is a narrow adapter around production behavior. Decide must invoke
// the same reducer/decision path as production; Observe must be side-effect-free.
// Decide runs synchronously. Returning an error promises that actor state was
// left unchanged. A returned accepted Decision may mutate production state;
// that acceptance is traced and replay-projected even if later validation
// terminalizes the world. The simulator cannot roll back arbitrary adapters.
type SimActor interface {
	Decide(SimAction) (SimDecision, error)
	Observe() map[string]any
}

// SimActorFactory constructs a fresh actor. Durable state can be captured by
// the factory; actor-local ephemeral state is discarded on crash and rebuilt
// by calling the factory on restart.
type SimActorFactory func() (SimActor, error)

type SimActorFuncs struct {
	DecideFunc  func(SimAction) (SimDecision, error)
	ObserveFunc func() map[string]any
}

func (a SimActorFuncs) Decide(action SimAction) (SimDecision, error) {
	if a.DecideFunc == nil {
		return SimDecision{}, simErrorf("actor has no production decision function")
	}
	return a.DecideFunc(action)
}

func (a SimActorFuncs) Observe() map[string]any {
	if a.ObserveFunc == nil {
		return map[string]any{}
	}
	return a.ObserveFunc()
}

// SimStateMachineActor adapts the existing production StateMachine without
// reproducing its transition logic in the simulator. Context here is Lazily's
// reactive graph owner, not context.Context; the simulation has no wall-clock
// cancellation or deadline surface.
type SimStateMachineActor[S comparable, E comparable] struct {
	ctx     *Context
	machine *StateMachine[S, E]
	decode  func(SimAction) (E, error)
	observe func(S) map[string]any
}

func NewSimStateMachineActor[S comparable, E comparable](
	ctx *Context,
	initial S,
	transition Transition[S, E],
	decode func(SimAction) (E, error),
	observe func(S) map[string]any,
) *SimStateMachineActor[S, E] {
	if ctx == nil {
		ctx = NewContext()
	}
	return &SimStateMachineActor[S, E]{
		ctx: ctx, machine: NewStateMachine(ctx, initial, transition), decode: decode, observe: observe,
	}
}

func (a *SimStateMachineActor[S, E]) Machine() *StateMachine[S, E] { return a.machine }

func (a *SimStateMachineActor[S, E]) Decide(action SimAction) (SimDecision, error) {
	if a.decode == nil {
		return SimDecision{}, simErrorf("state-machine actor has no action decoder")
	}
	event, err := a.decode(action)
	if err != nil {
		return SimDecision{}, err
	}
	accepted := false
	a.ctx.Batch(func() { accepted = a.machine.Send(event) })
	return SimDecision{Accepted: accepted}, nil
}

func (a *SimStateMachineActor[S, E]) Observe() map[string]any {
	state := a.machine.State()
	if a.observe != nil {
		return a.observe(state)
	}
	return map[string]any{"state": state}
}

type SimActorStatus string

const (
	SimActorRegistered SimActorStatus = "registered"
	SimActorRunning    SimActorStatus = "running"
	SimActorCrashed    SimActorStatus = "crashed"
	SimActorStopped    SimActorStatus = "stopped"
)

const (
	simLifecycleCrash   = "sim.actor.crash"
	simLifecycleRestart = "sim.actor.restart"
	simLifecycleStop    = "sim.actor.stop"
)

type SimScheduledItem struct {
	At      uint64
	Ordinal uint64
	Action  SimAction
}

type simQueue []SimScheduledItem

func (q simQueue) Len() int { return len(q) }
func (q simQueue) Less(i, j int) bool {
	if q[i].At != q[j].At {
		return q[i].At < q[j].At
	}
	return q[i].Ordinal < q[j].Ordinal
}
func (q simQueue) Swap(i, j int)   { q[i], q[j] = q[j], q[i] }
func (q *simQueue) Push(value any) { *q = append(*q, value.(SimScheduledItem)) }
func (q *simQueue) Pop() any {
	old := *q
	last := len(old) - 1
	item := old[last]
	old[last] = SimScheduledItem{}
	*q = old[:last]
	return item
}

type SimWorldSpec struct {
	InitialTime     uint64
	Seed            SimSeed
	SubjectKind     string
	SubjectVersion  string
	MaxQueue        uint64
	MaxTraceEntries uint64
}

type simActorRecord struct {
	actor   SimActor
	factory SimActorFactory
	status  SimActorStatus
}

type SimTraceHeader struct {
	ContractVersion  string
	TraceSchema      uint64
	RNGAlgorithm     string
	DigestAlgorithm  string
	SchedulerKind    string
	SchedulerVersion string
	SeedHex          string
	SubjectKind      string
	SubjectVersion   string
}

// SimTraceEntry intentionally contains only canonical scalar fields and
// digests. The bytes are a stable, Go-local proof surface; ReplayCanonicalBytes
// remains binding-local and is not advertised as the future portable codec.
type SimTraceEntry struct {
	TraceSeq         uint64
	Now              uint64
	Kind             string
	Version          string
	At               uint64
	Ordinal          uint64
	ActionID         string
	ActorID          string
	ActionKind       string
	ActionVersion    string
	CauseID          string
	Outcome          string
	Detail           string
	PayloadDigest    string
	DecisionDigest   string
	ObservationID    string
	ValueDigest      string
	CheckpointDigest string
	WorldDigest      string
}

type SimTerminal struct {
	Status      SimRunStatus
	Limit       string
	Steps       uint64
	Now         uint64
	WorldDigest string
}

type SimTrace struct {
	Header      SimTraceHeader
	Entries     []SimTraceEntry
	HasTerminal bool
	Terminal    SimTerminal
}

func (t SimTrace) CanonicalBytes() ([]byte, error) { return ReplayCanonicalBytes(t) }
func (t SimTrace) Digest() (string, error)         { return ReplayCanonicalDigest(t) }

type SimObservedValue struct {
	ID     string
	Digest string
}

type SimCheckpoint struct {
	TraceSeq    uint64
	Now         uint64
	Values      []SimObservedValue
	Digest      string
	WorldDigest string
}

type SimStepResult struct {
	Executed   bool
	Item       SimScheduledItem
	Decision   SimDecision
	Checkpoint SimCheckpoint
}

type SimRunStatus string

const (
	SimRunIdle           SimRunStatus = "idle"
	SimRunBoundExhausted SimRunStatus = "bound_exhausted"
	SimRunError          SimRunStatus = "error"
)

type SimRunResult struct {
	Status SimRunStatus
	Limit  string
	Steps  uint64
	Now    uint64
}

type SimWorld struct {
	clock *ManualClock
	seed  SimSeed

	actors       map[string]simActorRecord
	queue        simQueue
	nextOrdinal  uint64
	ordinalSpent bool
	actionIDs    map[string]struct{}

	streams      map[string]*SimRandomStream
	streamLabels map[string][]string
	observations map[string]func() any

	trace    SimTrace
	executed []ReplayEvent
	steps    uint64
	maxQueue uint64
	maxTrace uint64
	finished bool
}

func NewSimWorld(seed SimSeed) *SimWorld {
	world, err := BuildSimWorld(SimWorldSpec{Seed: seed})
	if err != nil {
		panic(err)
	}
	return world
}

func BuildSimWorld(spec SimWorldSpec) (*SimWorld, error) {
	clock := NewManualClock()
	clock.Advance(spec.InitialTime)
	w := &SimWorld{
		clock: clock, seed: spec.Seed,
		actors: map[string]simActorRecord{}, actionIDs: map[string]struct{}{},
		streams: map[string]*SimRandomStream{}, streamLabels: map[string][]string{},
		observations: map[string]func() any{}, maxQueue: spec.MaxQueue, maxTrace: spec.MaxTraceEntries,
	}
	heap.Init(&w.queue)
	w.trace.Header = SimTraceHeader{
		ContractVersion: SimContractVersion, TraceSchema: SimTraceSchema,
		RNGAlgorithm: SimRNGAlgorithm, DigestAlgorithm: SimDigestAlgorithm,
		SchedulerKind: SimSchedulerKind, SchedulerVersion: SimSchedulerVersion,
		SeedHex: spec.Seed.String(), SubjectKind: spec.SubjectKind, SubjectVersion: spec.SubjectVersion,
	}
	if _, err := w.worldDigest(nil); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *SimWorld) Now() uint64   { return w.clock.Now() }
func (w *SimWorld) Pending() int  { return w.queue.Len() }
func (w *SimWorld) Steps() uint64 { return w.steps }

// Scheduled returns the pending queue in execution order.
func (w *SimWorld) Scheduled() []SimScheduledItem {
	items := append([]SimScheduledItem(nil), w.queue...)
	for i := range items {
		items[i].Action = cloneSimAction(items[i].Action)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].At != items[j].At {
			return items[i].At < items[j].At
		}
		return items[i].Ordinal < items[j].Ordinal
	})
	return items
}

func (w *SimWorld) RegisterActor(id string, actor SimActor) error {
	return w.registerActor(id, actor, nil)
}

// RegisterActorFactory opts into reconstructive lifecycle semantics. Plain
// RegisterActor remains an explicit status-only lifecycle adapter.
func (w *SimWorld) RegisterActorFactory(id string, factory SimActorFactory) error {
	if w.finished {
		return simErrorf("cannot register actor after terminal status")
	}
	if factory == nil {
		return simErrorf("actor %q has a nil factory", id)
	}
	actor, err := factory()
	if err != nil {
		return fmt.Errorf("%w: construct actor %q: %v", ErrSimulation, id, err)
	}
	return w.registerActor(id, actor, factory)
}

func (w *SimWorld) registerActor(id string, actor SimActor, factory SimActorFactory) error {
	if w.finished {
		return simErrorf("cannot register actor after terminal status")
	}
	if err := w.ensureTraceCapacity(1); err != nil {
		return err
	}
	if !validSimID(id) {
		return simErrorf("invalid actor id %q", id)
	}
	if actor == nil {
		return simErrorf("actor %q is nil", id)
	}
	if _, exists := w.actors[id]; exists {
		return simErrorf("duplicate actor id %q", id)
	}
	w.actors[id] = simActorRecord{actor: actor, factory: factory, status: SimActorRunning}
	mode := "status_only"
	if factory != nil {
		mode = "reconstructive"
	}
	if err := w.appendEntry(SimTraceEntry{Kind: "actor_lifecycle", Version: SimTraceEntryVersion, ActorID: id, Outcome: string(SimActorRunning), Detail: mode}); err != nil {
		delete(w.actors, id)
		return err
	}
	return nil
}

func (w *SimWorld) ActorStatus(id string) (SimActorStatus, bool) {
	record, ok := w.actors[id]
	return record.status, ok
}

func (w *SimWorld) RegisterObservation(id string, observe func() any) error {
	if w.finished {
		return simErrorf("cannot register observation after terminal status")
	}
	if !validSimID(id) || observe == nil {
		return simErrorf("observation needs a valid id and function")
	}
	if _, exists := w.observations[id]; exists {
		return simErrorf("duplicate observation id %q", id)
	}
	if _, err := ReplayCanonicalBytes(observe()); err != nil {
		return fmt.Errorf("%w: observation %q: %v", ErrSimulation, id, err)
	}
	w.observations[id] = observe
	return nil
}

func (w *SimWorld) RandomStream(labels ...string) (*SimRandomStream, error) {
	if w.finished {
		return nil, simErrorf("cannot access random stream after terminal status")
	}
	key := simLabelsKey(labels)
	if stream := w.streams[key]; stream != nil {
		return stream, nil
	}
	stream, err := newSimRandomStream(w.seed, labels)
	if err != nil {
		return nil, err
	}
	stream.active = func() bool { return !w.finished }
	w.streams[key] = stream
	w.streamLabels[key] = append([]string(nil), labels...)
	return stream, nil
}

func simLabelsKey(labels []string) string {
	var b strings.Builder
	for _, label := range labels {
		fmt.Fprintf(&b, "%d:%s", len(label), label)
	}
	return b.String()
}

func (w *SimWorld) Submit(action SimAction) (SimScheduledItem, error) {
	return w.Schedule(w.Now(), action)
}

// Send is the message-oriented spelling of Schedule.
func (w *SimWorld) Send(at uint64, message SimMessage) (SimScheduledItem, error) {
	return w.Schedule(at, message)
}

func (w *SimWorld) Schedule(at uint64, action SimAction) (SimScheduledItem, error) {
	if w.finished {
		return SimScheduledItem{}, simErrorf("cannot enqueue after terminal status")
	}
	if err := w.ensureTraceCapacity(1); err != nil {
		return SimScheduledItem{}, err
	}
	queueBefore := append(simQueue(nil), w.queue...)
	nextOrdinalBefore, ordinalSpentBefore := w.nextOrdinal, w.ordinalSpent
	item, entry, err := w.enqueue(at, action)
	if err != nil {
		_ = w.appendEntry(SimTraceEntry{Kind: "error", Version: SimTraceEntryVersion, At: at, ActionID: action.ID, ActorID: action.ActorID, Detail: err.Error()})
		return SimScheduledItem{}, err
	}
	if err := w.appendEntry(entry); err != nil {
		w.queue = queueBefore
		heap.Init(&w.queue)
		w.nextOrdinal, w.ordinalSpent = nextOrdinalBefore, ordinalSpentBefore
		delete(w.actionIDs, action.ID)
		return SimScheduledItem{}, err
	}
	item.Action = cloneSimAction(item.Action)
	return item, nil
}

func (w *SimWorld) ScheduleLifecycle(at uint64, actorID string, status SimActorStatus) (SimScheduledItem, error) {
	kind := ""
	switch status {
	case SimActorCrashed:
		kind = simLifecycleCrash
	case SimActorRunning:
		kind = simLifecycleRestart
	case SimActorStopped:
		kind = simLifecycleStop
	default:
		return SimScheduledItem{}, simErrorf("unsupported actor lifecycle target %q", status)
	}
	action := SimAction{
		ID:      fmt.Sprintf("lifecycle:%s:%s:%d", actorID, status, w.nextOrdinal),
		ActorID: actorID, Kind: kind, Version: SimActionVersion,
	}
	return w.Schedule(at, action)
}

func (w *SimWorld) enqueue(at uint64, action SimAction) (SimScheduledItem, SimTraceEntry, error) {
	if w.finished {
		return SimScheduledItem{}, SimTraceEntry{}, simErrorf("cannot enqueue after terminal status")
	}
	if at < w.Now() {
		return SimScheduledItem{}, SimTraceEntry{}, simErrorf("cannot schedule action %q at past tick %d (now %d)", action.ID, at, w.Now())
	}
	if err := validateSimAction(action); err != nil {
		return SimScheduledItem{}, SimTraceEntry{}, err
	}
	if _, exists := w.actors[action.ActorID]; !exists {
		return SimScheduledItem{}, SimTraceEntry{}, simErrorf("action %q references unknown actor %q", action.ID, action.ActorID)
	}
	if _, exists := w.actionIDs[action.ID]; exists {
		return SimScheduledItem{}, SimTraceEntry{}, simErrorf("duplicate action id %q", action.ID)
	}
	if action.CauseID != "" {
		if _, exists := w.actionIDs[action.CauseID]; !exists {
			return SimScheduledItem{}, SimTraceEntry{}, simErrorf("action %q references unresolved cause %q", action.ID, action.CauseID)
		}
	}
	if w.maxQueue > 0 && uint64(w.queue.Len()) >= w.maxQueue {
		return SimScheduledItem{}, SimTraceEntry{}, simBoundError{limit: "max_queue"}
	}
	if w.ordinalSpent {
		return SimScheduledItem{}, SimTraceEntry{}, simBoundError{limit: "enqueue_ordinal"}
	}
	payloadDigest, err := ReplayCanonicalDigest(action.Payload)
	if err != nil {
		return SimScheduledItem{}, SimTraceEntry{}, fmt.Errorf("%w: action %q payload: %v", ErrSimulation, action.ID, err)
	}
	action.Payload, err = freezeSimValue(action.Payload)
	if err != nil {
		return SimScheduledItem{}, SimTraceEntry{}, fmt.Errorf("%w: action %q payload ownership: %v", ErrSimulation, action.ID, err)
	}
	if action.Version == "" {
		action.Version = SimActionVersion
	}
	item := SimScheduledItem{At: at, Ordinal: w.nextOrdinal, Action: action}
	if w.nextOrdinal == math.MaxUint64 {
		w.ordinalSpent = true
	} else {
		w.nextOrdinal++
	}
	heap.Push(&w.queue, item)
	w.actionIDs[action.ID] = struct{}{}
	entry := SimTraceEntry{
		Kind: "enqueue", Version: SimTraceEntryVersion, At: at, Ordinal: item.Ordinal,
		ActionID: action.ID, ActorID: action.ActorID, ActionKind: action.Kind,
		ActionVersion: action.Version, CauseID: action.CauseID, PayloadDigest: payloadDigest,
	}
	return item, entry, nil
}

func validateSimAction(action SimAction) error {
	if !validSimID(action.ID) || !validSimID(action.ActorID) || !validSimID(action.Kind) {
		return simErrorf("action id, actor id, and kind must be stable identifiers")
	}
	if action.CauseID != "" && !validSimID(action.CauseID) {
		return simErrorf("invalid cause id %q", action.CauseID)
	}
	return nil
}

func freezeSimValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	cloned, err := cloneSimReflect(reflect.ValueOf(value))
	if err != nil {
		return nil, err
	}
	return cloned.Interface(), nil
}

func cloneSimReflect(src reflect.Value) (reflect.Value, error) {
	if !src.IsValid() {
		return src, nil
	}
	switch src.Kind() {
	case reflect.Interface:
		if src.IsNil() {
			return reflect.Zero(src.Type()), nil
		}
		value, err := cloneSimReflect(src.Elem())
		if err != nil {
			return reflect.Value{}, err
		}
		dst := reflect.New(src.Type()).Elem()
		dst.Set(value)
		return dst, nil
	case reflect.Pointer:
		if src.IsNil() {
			return reflect.Zero(src.Type()), nil
		}
		value, err := cloneSimReflect(src.Elem())
		if err != nil {
			return reflect.Value{}, err
		}
		dst := reflect.New(src.Type().Elem())
		dst.Elem().Set(value)
		return dst, nil
	case reflect.Slice:
		if src.IsNil() {
			return reflect.Zero(src.Type()), nil
		}
		dst := reflect.MakeSlice(src.Type(), src.Len(), src.Len())
		for i := 0; i < src.Len(); i++ {
			value, err := cloneSimReflect(src.Index(i))
			if err != nil {
				return reflect.Value{}, err
			}
			dst.Index(i).Set(value)
		}
		return dst, nil
	case reflect.Map:
		if src.IsNil() {
			return reflect.Zero(src.Type()), nil
		}
		dst := reflect.MakeMapWithSize(src.Type(), src.Len())
		iter := src.MapRange()
		for iter.Next() {
			key, err := cloneSimReflect(iter.Key())
			if err != nil {
				return reflect.Value{}, err
			}
			value, err := cloneSimReflect(iter.Value())
			if err != nil {
				return reflect.Value{}, err
			}
			dst.SetMapIndex(key, value)
		}
		return dst, nil
	case reflect.Array:
		dst := reflect.New(src.Type()).Elem()
		for i := 0; i < src.Len(); i++ {
			value, err := cloneSimReflect(src.Index(i))
			if err != nil {
				return reflect.Value{}, err
			}
			dst.Index(i).Set(value)
		}
		return dst, nil
	case reflect.Struct:
		dst := reflect.New(src.Type()).Elem()
		for i := 0; i < src.NumField(); i++ {
			if !dst.Field(i).CanSet() {
				return reflect.Value{}, simErrorf("payload type %s has inaccessible fields", src.Type())
			}
			value, err := cloneSimReflect(src.Field(i))
			if err != nil {
				return reflect.Value{}, err
			}
			dst.Field(i).Set(value)
		}
		return dst, nil
	default:
		return src, nil
	}
}

func cloneSimAction(action SimAction) SimAction {
	if value, err := freezeSimValue(action.Payload); err == nil {
		action.Payload = value
	}
	return action
}

func cloneSimDecision(decision SimDecision) SimDecision {
	decision.Schedule = append([]SimScheduledAction(nil), decision.Schedule...)
	for i := range decision.Schedule {
		decision.Schedule[i].Action = cloneSimAction(decision.Schedule[i].Action)
	}
	decision.Observations = append([]SimObservation(nil), decision.Observations...)
	for i := range decision.Observations {
		if value, err := freezeSimValue(decision.Observations[i].Value); err == nil {
			decision.Observations[i].Value = value
		}
	}
	return decision
}

// validateScheduleBatch mirrors enqueue validation without mutating the world.
// Actions in one decision may causally reference an earlier action in that same
// ordered batch, but never a later or unknown action.
func (w *SimWorld) validateScheduleBatch(scheduled []SimScheduledAction) error {
	admitted := make(map[string]struct{}, len(scheduled))
	nextOrdinal, ordinalSpent := w.nextOrdinal, w.ordinalSpent
	queueSize := uint64(w.queue.Len())
	for _, candidate := range scheduled {
		action := candidate.Action
		if candidate.At < w.Now() {
			return simErrorf("cannot schedule action %q at past tick %d (now %d)", action.ID, candidate.At, w.Now())
		}
		if err := validateSimAction(action); err != nil {
			return err
		}
		if _, exists := w.actors[action.ActorID]; !exists {
			return simErrorf("action %q references unknown actor %q", action.ID, action.ActorID)
		}
		if _, exists := w.actionIDs[action.ID]; exists {
			return simErrorf("duplicate action id %q", action.ID)
		}
		if _, exists := admitted[action.ID]; exists {
			return simErrorf("duplicate action id %q", action.ID)
		}
		if action.CauseID != "" {
			_, existing := w.actionIDs[action.CauseID]
			_, earlier := admitted[action.CauseID]
			if !existing && !earlier {
				return simErrorf("action %q references unresolved cause %q", action.ID, action.CauseID)
			}
		}
		if w.maxQueue > 0 && queueSize >= w.maxQueue {
			return simBoundError{limit: "max_queue"}
		}
		if ordinalSpent {
			return simBoundError{limit: "enqueue_ordinal"}
		}
		if _, err := ReplayCanonicalDigest(action.Payload); err != nil {
			return fmt.Errorf("%w: action %q payload: %v", ErrSimulation, action.ID, err)
		}
		if _, err := freezeSimValue(action.Payload); err != nil {
			return fmt.Errorf("%w: action %q payload ownership: %v", ErrSimulation, action.ID, err)
		}
		admitted[action.ID] = struct{}{}
		queueSize++
		if nextOrdinal == math.MaxUint64 {
			ordinalSpent = true
		} else {
			nextOrdinal++
		}
	}
	return nil
}

func validSimID(id string) bool {
	if len(id) == 0 || len(id) > 128 || id[0] < 'a' || id[0] > 'z' {
		return false
	}
	for i := 1; i < len(id); i++ {
		c := id[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == ':' || c == '-' {
			continue
		}
		return false
	}
	return true
}

func (w *SimWorld) Step() (SimStepResult, error) {
	if w.finished {
		return SimStepResult{}, simErrorf("world has terminal status")
	}
	if w.queue.Len() == 0 {
		return SimStepResult{}, nil
	}
	// Reserve dequeue, clock, result, and checkpoint entries before invoking an
	// actor. The exact observation and enqueue count is checked after Decide.
	if err := w.ensureTraceCapacity(4); err != nil {
		return SimStepResult{}, err
	}
	item := heap.Pop(&w.queue).(SimScheduledItem)
	if item.At < w.Now() {
		return SimStepResult{}, simErrorf("queue order moved backward from %d to %d", w.Now(), item.At)
	}
	w.clock.Advance(item.At)
	w.steps++
	entries := []SimTraceEntry{{
		Kind: "dequeue", Version: SimTraceEntryVersion, At: item.At, Ordinal: item.Ordinal,
		ActionID: item.Action.ID, ActorID: item.Action.ActorID, ActionKind: item.Action.Kind,
		ActionVersion: item.Action.Version, CauseID: item.Action.CauseID,
	}, {
		Kind: "clock_advance", Version: SimTraceEntryVersion, At: item.At, Ordinal: item.Ordinal,
		ActionID: item.Action.ID, ActorID: item.Action.ActorID,
	}}

	decision, err := w.execute(item)
	if err != nil {
		return w.failStep(item, entries, err)
	}
	if decision.Accepted {
		decision.Schedule = append([]SimScheduledAction(nil), decision.Schedule...)
		for i := range decision.Schedule {
			if decision.Schedule[i].Action.CauseID == "" {
				decision.Schedule[i].Action.CauseID = item.Action.ID
			}
			if decision.Schedule[i].Action.Version == "" {
				decision.Schedule[i].Action.Version = SimActionVersion
			}
		}
	}
	outcome := "rejected"
	if decision.Accepted {
		outcome = "accepted"
	}
	decisionDigest, digestErr := ReplayCanonicalDigest(decision)
	detail := ""
	if isSimLifecycle(item.Action.Kind) {
		detail = "status_only"
		if w.actors[item.Action.ActorID].factory != nil {
			detail = "reconstructive"
		}
	}
	entries = append(entries, SimTraceEntry{
		Kind: "action_" + outcome, Version: SimTraceEntryVersion,
		At: item.At, Ordinal: item.Ordinal, ActionID: item.Action.ID,
		ActorID: item.Action.ActorID, ActionKind: item.Action.Kind,
		ActionVersion: item.Action.Version, CauseID: item.Action.CauseID,
		Outcome: outcome, Detail: detail, DecisionDigest: decisionDigest,
	})
	if digestErr != nil {
		return w.failStep(item, entries, fmt.Errorf("%w: decision for action %q: %v", ErrSimulation, item.Action.ID, digestErr))
	}
	if decision.Accepted {
		if err := w.validateScheduleBatch(decision.Schedule); err != nil {
			return w.failStep(item, entries, err)
		}
	}
	observed, err := w.Observe()
	if err != nil {
		return w.failStep(item, entries, err)
	}
	for _, observation := range decision.Observations {
		id := observation.ID
		if !validSimID(id) {
			return w.failStep(item, entries, simErrorf("invalid decision observation id %q", id))
		}
		if _, duplicate := observed[id]; duplicate {
			return w.failStep(item, entries, simErrorf("duplicate observation id %q", id))
		}
		if _, err := ReplayCanonicalBytes(observation.Value); err != nil {
			return w.failStep(item, entries, fmt.Errorf("%w: observation %q: %v", ErrSimulation, id, err))
		}
		observed[id] = observation.Value
	}
	entryCount := 4 + len(observed)
	if decision.Accepted {
		entryCount += len(decision.Schedule)
	}
	if err := w.ensureTraceCapacity(entryCount); err != nil {
		return w.failStep(item, entries, err)
	}
	if decision.Accepted && !isSimLifecycle(item.Action.Kind) {
		acceptedSeq := uint64(len(w.trace.Entries)) + 2
		if acceptedSeq > math.MaxInt64 {
			return w.failStep(item, entries, simErrorf("accepted replay sequence exceeds int64"))
		}
	}
	if decision.Accepted {
		for _, scheduled := range decision.Schedule {
			_, enqueueEntry, enqueueErr := w.enqueue(scheduled.At, scheduled.Action)
			if enqueueErr != nil {
				return w.failStep(item, entries, enqueueErr)
			}
			entries = append(entries, enqueueEntry)
		}
	}
	checkpoint, err := w.checkpoint(observed)
	if err != nil {
		return w.failStep(item, entries, err)
	}
	ids := make([]string, 0, len(observed))
	for id := range observed {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		digest, digestErr := ReplayCanonicalDigest(observed[id])
		if digestErr != nil {
			return w.failStep(item, entries, digestErr)
		}
		entries = append(entries, SimTraceEntry{Kind: "observation", Version: SimTraceEntryVersion, ActionID: item.Action.ID, ActorID: item.Action.ActorID, ObservationID: id, ValueDigest: digest})
	}
	entries = append(entries, SimTraceEntry{Kind: "checkpoint", Version: SimTraceEntryVersion, ActionID: item.Action.ID, ActorID: item.Action.ActorID, CheckpointDigest: checkpoint.Digest})
	firstSeq := uint64(len(w.trace.Entries))
	if err := w.appendEntries(entries); err != nil {
		return w.failStep(item, nil, err)
	}
	checkpoint.TraceSeq = uint64(len(w.trace.Entries) - 1)
	checkpoint.WorldDigest = w.trace.Entries[len(w.trace.Entries)-1].WorldDigest
	if decision.Accepted && !isSimLifecycle(item.Action.Kind) {
		acceptedSeq := firstSeq + 2
		w.executed = append(w.executed, ReplayEvent{
			Seq: int64(acceptedSeq), Name: item.Action.Kind,
			Payload: SimReplayAction{Action: cloneSimAction(item.Action), At: item.At, Ordinal: item.Ordinal},
		})
	}
	item.Action = cloneSimAction(item.Action)
	return SimStepResult{Executed: true, Item: item, Decision: cloneSimDecision(decision), Checkpoint: checkpoint}, nil
}

func (w *SimWorld) failStep(item SimScheduledItem, entries []SimTraceEntry, cause error) (SimStepResult, error) {
	firstSeq := uint64(len(w.trace.Entries))
	acceptedOffset := -1
	for i := range entries {
		if entries[i].Kind == "action_accepted" {
			acceptedOffset = i
			break
		}
	}
	entries = append(entries, SimTraceEntry{Kind: "error", Version: SimTraceEntryVersion, ActionID: item.Action.ID, ActorID: item.Action.ActorID, Detail: cause.Error()})
	recorded := w.appendEntries(entries) == nil
	// A malformed post-reducer observation can make the normal world digest
	// unavailable. Preserve the decision/error evidence with an empty digest;
	// the terminal record below still makes the run visibly erroneous.
	if !recorded && w.ensureTraceCapacity(len(entries)) == nil {
		for i := range entries {
			entries[i].TraceSeq = uint64(len(w.trace.Entries))
			entries[i].Now = w.Now()
			w.trace.Entries = append(w.trace.Entries, entries[i])
		}
		recorded = true
	}
	if recorded && acceptedOffset >= 0 && !isSimLifecycle(item.Action.Kind) {
		seq := firstSeq + uint64(acceptedOffset)
		if seq <= math.MaxInt64 {
			w.executed = append(w.executed, ReplayEvent{Seq: int64(seq), Name: item.Action.Kind, Payload: SimReplayAction{Action: cloneSimAction(item.Action), At: item.At, Ordinal: item.Ordinal}})
		}
	}
	digest, _ := w.worldDigest(nil)
	w.finished = true
	w.trace.HasTerminal = true
	status, limit := SimRunError, ""
	var bound simBoundError
	if errors.As(cause, &bound) {
		status, limit = SimRunBoundExhausted, bound.limit
	}
	w.trace.Terminal = SimTerminal{Status: status, Limit: limit, Steps: w.steps, Now: w.Now(), WorldDigest: digest}
	item.Action = cloneSimAction(item.Action)
	return SimStepResult{Executed: true, Item: item}, cause
}

func (w *SimWorld) execute(item SimScheduledItem) (SimDecision, error) {
	record := w.actors[item.Action.ActorID]
	if isSimLifecycle(item.Action.Kind) {
		accepted := false
		switch item.Action.Kind {
		case simLifecycleCrash:
			accepted = record.status == SimActorRunning
			if accepted {
				record.status = SimActorCrashed
				if record.factory != nil {
					record.actor = nil
				}
			}
		case simLifecycleRestart:
			accepted = record.status == SimActorCrashed
			if accepted {
				if record.factory != nil {
					actor, err := record.factory()
					if err != nil {
						return SimDecision{}, fmt.Errorf("%w: restart actor %q: %v", ErrSimulation, item.Action.ActorID, err)
					}
					if actor == nil {
						return SimDecision{}, simErrorf("restart actor %q returned nil", item.Action.ActorID)
					}
					record.actor = actor
				}
				record.status = SimActorRunning
			}
		case simLifecycleStop:
			accepted = record.status != SimActorStopped
			if accepted {
				record.status = SimActorStopped
			}
		}
		w.actors[item.Action.ActorID] = record
		return SimDecision{Accepted: accepted}, nil
	}
	if record.status != SimActorRunning {
		return SimDecision{Accepted: false}, nil
	}
	return record.actor.Decide(cloneSimAction(item.Action))
}

func isSimLifecycle(kind string) bool {
	return kind == simLifecycleCrash || kind == simLifecycleRestart || kind == simLifecycleStop
}

func (w *SimWorld) Observe() (map[string]any, error) {
	out := map[string]any{}
	actorIDs := make([]string, 0, len(w.actors))
	for id := range w.actors {
		actorIDs = append(actorIDs, id)
	}
	sort.Strings(actorIDs)
	for _, actorID := range actorIDs {
		record := w.actors[actorID]
		if record.actor == nil {
			continue
		}
		values := record.actor.Observe()
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			id := actorID + "." + key
			if !validSimID(id) {
				return nil, simErrorf("invalid actor observation id %q", id)
			}
			if _, duplicate := out[id]; duplicate {
				return nil, simErrorf("duplicate actor observation id %q", id)
			}
			if _, err := ReplayCanonicalBytes(values[key]); err != nil {
				return nil, err
			}
			out[id] = values[key]
		}
	}
	ids := make([]string, 0, len(w.observations))
	for id := range w.observations {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if _, duplicate := out[id]; duplicate {
			return nil, simErrorf("duplicate observation id %q", id)
		}
		value := w.observations[id]()
		if _, err := ReplayCanonicalBytes(value); err != nil {
			return nil, err
		}
		out[id] = value
	}
	return out, nil
}

func (w *SimWorld) Checkpoint() (SimCheckpoint, error) {
	observed, err := w.Observe()
	if err != nil {
		return SimCheckpoint{}, err
	}
	return w.checkpoint(observed)
}

func (w *SimWorld) checkpoint(observed map[string]any) (SimCheckpoint, error) {
	ids := make([]string, 0, len(observed))
	for id := range observed {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	values := make([]SimObservedValue, 0, len(ids))
	for _, id := range ids {
		digest, err := ReplayCanonicalDigest(observed[id])
		if err != nil {
			return SimCheckpoint{}, err
		}
		values = append(values, SimObservedValue{ID: id, Digest: digest})
	}
	digest, err := ReplayCanonicalDigest(struct {
		Now    uint64
		Values []SimObservedValue
	}{w.Now(), values})
	if err != nil {
		return SimCheckpoint{}, err
	}
	worldDigest, err := w.worldDigest(observed)
	if err != nil {
		return SimCheckpoint{}, err
	}
	return SimCheckpoint{Now: w.Now(), Values: values, Digest: digest, WorldDigest: worldDigest}, nil
}

func (w *SimWorld) appendEntry(entry SimTraceEntry) error {
	return w.appendEntries([]SimTraceEntry{entry})
}

func (w *SimWorld) ensureTraceCapacity(needed int) error {
	if needed < 0 {
		return simErrorf("negative trace reservation")
	}
	if w.maxTrace == 0 {
		return nil
	}
	used := uint64(len(w.trace.Entries))
	if used > w.maxTrace || uint64(needed) > w.maxTrace-used {
		return simBoundError{limit: "max_trace_entries"}
	}
	return nil
}

func (w *SimWorld) appendEntries(entries []SimTraceEntry) error {
	if err := w.ensureTraceCapacity(len(entries)); err != nil {
		return err
	}
	digest, err := w.worldDigest(nil)
	if err != nil {
		return err
	}
	for i := range entries {
		entries[i].TraceSeq = uint64(len(w.trace.Entries))
		entries[i].Now = w.Now()
		entries[i].WorldDigest = digest
		w.trace.Entries = append(w.trace.Entries, entries[i])
	}
	return nil
}

type simActorSnapshot struct {
	ID           string
	Status       SimActorStatus
	Observations []SimObservedValue
}
type simQueueSnapshot struct {
	At, Ordinal                                              uint64
	ActionID, ActorID, Kind, Version, CauseID, PayloadDigest string
}
type simStreamSnapshot struct {
	Labels    []string
	NextBlock uint64
	Offset    uint64
	Exhausted bool
}

func (w *SimWorld) worldDigest(observed map[string]any) (string, error) {
	if observed == nil {
		var err error
		observed, err = w.Observe()
		if err != nil {
			return "", err
		}
	}
	actorIDs := make([]string, 0, len(w.actors))
	for id := range w.actors {
		actorIDs = append(actorIDs, id)
	}
	sort.Strings(actorIDs)
	actors := make([]simActorSnapshot, 0, len(actorIDs))
	for _, id := range actorIDs {
		prefix := id + "."
		values := []SimObservedValue{}
		for key, value := range observed {
			if strings.HasPrefix(key, prefix) {
				digest, err := ReplayCanonicalDigest(value)
				if err != nil {
					return "", err
				}
				values = append(values, SimObservedValue{ID: key, Digest: digest})
			}
		}
		sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
		actors = append(actors, simActorSnapshot{ID: id, Status: w.actors[id].status, Observations: values})
	}
	observationIDs := make([]string, 0, len(observed))
	for id := range observed {
		observationIDs = append(observationIDs, id)
	}
	sort.Strings(observationIDs)
	observations := make([]SimObservedValue, 0, len(observationIDs))
	for _, id := range observationIDs {
		digest, err := ReplayCanonicalDigest(observed[id])
		if err != nil {
			return "", err
		}
		observations = append(observations, SimObservedValue{ID: id, Digest: digest})
	}
	queued := append([]SimScheduledItem(nil), w.queue...)
	sort.Slice(queued, func(i, j int) bool {
		if queued[i].At != queued[j].At {
			return queued[i].At < queued[j].At
		}
		return queued[i].Ordinal < queued[j].Ordinal
	})
	queue := make([]simQueueSnapshot, 0, len(queued))
	for _, item := range queued {
		payloadDigest, err := ReplayCanonicalDigest(item.Action.Payload)
		if err != nil {
			return "", err
		}
		queue = append(queue, simQueueSnapshot{item.At, item.Ordinal, item.Action.ID, item.Action.ActorID, item.Action.Kind, item.Action.Version, item.Action.CauseID, payloadDigest})
	}
	actionIDs := make([]string, 0, len(w.actionIDs))
	for id := range w.actionIDs {
		actionIDs = append(actionIDs, id)
	}
	sort.Strings(actionIDs)
	streamKeys := make([]string, 0, len(w.streams))
	for key := range w.streams {
		streamKeys = append(streamKeys, key)
	}
	sort.Strings(streamKeys)
	streams := make([]simStreamSnapshot, 0, len(streamKeys))
	for _, key := range streamKeys {
		stream := w.streams[key]
		streams = append(streams, simStreamSnapshot{append([]string(nil), w.streamLabels[key]...), stream.nextBlock, uint64(stream.offset), stream.exhausted})
	}
	return ReplayCanonicalDigest(struct {
		Now, NextOrdinal uint64
		Steps            uint64
		OrdinalSpent     bool
		Actors           []simActorSnapshot
		Observations     []SimObservedValue
		Queue            []simQueueSnapshot
		ActionIDs        []string
		Streams          []simStreamSnapshot
	}{w.Now(), w.nextOrdinal, w.steps, w.ordinalSpent, actors, observations, queue, actionIDs, streams})
}

func (w *SimWorld) Trace() SimTrace {
	trace := w.trace
	trace.Entries = append([]SimTraceEntry(nil), w.trace.Entries...)
	return trace
}

func (w *SimWorld) TraceBytes() ([]byte, error)  { return w.Trace().CanonicalBytes() }
func (w *SimWorld) TraceDigest() (string, error) { return w.Trace().Digest() }
func (w *SimWorld) WorldDigest() (string, error) {
	if w.trace.HasTerminal {
		return w.trace.Terminal.WorldDigest, nil
	}
	return w.worldDigest(nil)
}

// SimReplayAction is the binding-local ReplayLog payload for an accepted action.
type SimReplayAction struct {
	Action      SimAction
	At, Ordinal uint64
}

// ReplayLog projects accepted executed domain actions in trace execution order,
// ready for the existing ReplayHarness. Lifecycle controls are intentionally not
// projected into a subject graph's domain log.
func (w *SimWorld) ReplayLog() (*ReplayLog, error) {
	events := append([]ReplayEvent(nil), w.executed...)
	for i := range events {
		if payload, ok := events[i].Payload.(SimReplayAction); ok {
			payload.Action = cloneSimAction(payload.Action)
			events[i].Payload = payload
		}
	}
	return NewReplayLog(events...)
}

func (w *SimWorld) RunSteps(maxSteps uint64) (SimRunResult, error) {
	return w.run(maxSteps)
}

func (w *SimWorld) RunUntilIdle(maxSteps uint64) (SimRunResult, error) {
	return w.run(maxSteps)
}

func (w *SimWorld) run(maxSteps uint64) (SimRunResult, error) {
	if w.queue.Len() == 0 {
		return w.finish(SimRunIdle, "", 0)
	}
	if maxSteps == 0 {
		return w.finish(SimRunBoundExhausted, "max_steps", 0)
	}
	var ran uint64
	for ran < maxSteps && w.queue.Len() > 0 {
		if _, err := w.Step(); err != nil {
			if w.finished {
				result := SimRunResult{Status: w.trace.Terminal.Status, Limit: w.trace.Terminal.Limit, Steps: ran + 1, Now: w.Now()}
				if result.Status == SimRunBoundExhausted {
					return result, nil
				}
				return result, err
			}
			var bound simBoundError
			if errors.As(err, &bound) {
				result, finishErr := w.finish(SimRunBoundExhausted, bound.limit, ran)
				if finishErr != nil {
					return result, finishErr
				}
				return result, nil
			}
			result, finishErr := w.finish(SimRunError, "", ran)
			if finishErr != nil {
				return result, finishErr
			}
			return result, err
		}
		ran++
	}
	if w.queue.Len() == 0 {
		return w.finish(SimRunIdle, "", ran)
	}
	return w.finish(SimRunBoundExhausted, "max_steps", ran)
}

func (w *SimWorld) finish(status SimRunStatus, limit string, ran uint64) (SimRunResult, error) {
	if w.finished {
		return SimRunResult{}, simErrorf("world already has terminal status")
	}
	digest, err := w.worldDigest(nil)
	if err != nil {
		return SimRunResult{}, err
	}
	w.finished = true
	w.trace.HasTerminal = true
	w.trace.Terminal = SimTerminal{Status: status, Limit: limit, Steps: w.steps, Now: w.Now(), WorldDigest: digest}
	return SimRunResult{Status: status, Limit: limit, Steps: ran, Now: w.Now()}, nil
}
