package lazily

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

var ErrSimOracle = errors.New("lazily: simulation oracle failed")

func simOracleErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrSimOracle}, args...)...)
}

type SimOracleSnapshot struct {
	Step         uint64
	Now          uint64
	Action       SimAction
	Accepted     bool
	Observations map[string]any
}

type SimSafetyAssertion struct {
	ID    string
	Check func(SimOracleSnapshot) error
}

// SimBoundedLivenessAssertion starts an obligation when When becomes true and
// requires Satisfied within Bound subsequently executed actions. At idle, an
// active unsatisfied obligation fails even if its numeric bound was not reached.
type SimBoundedLivenessAssertion struct {
	ID        string
	Bound     uint64
	When      func(SimOracleSnapshot) bool
	Satisfied func(SimOracleSnapshot) bool
}

type SimReferenceModel interface {
	Apply(SimStepResult) error
	Observe() map[string]any
}

// SimOracleSourceKind records why a comparative oracle is independent enough to
// catch defects in the simulated subject. A simulation-only subject may be useful
// as an additional differential, but it cannot be the sole comparative authority.
type SimOracleSourceKind string

const (
	SimOracleSourceIndependentModel  SimOracleSourceKind = "independent_model"
	SimOracleSourceProductionReducer SimOracleSourceKind = "production_reducer"
	SimOracleSourceRealAdapter       SimOracleSourceKind = "real_adapter"
	SimOracleSourceSimulationOnly    SimOracleSourceKind = "simulation_only"
)

type SimOracleSource struct {
	ID   string
	Kind SimOracleSourceKind
}

type SimDifferentialSubject struct {
	ID      string
	Source  SimOracleSource
	Observe func() map[string]any
}

type SimHistoryOperation struct {
	Step       uint64
	At         uint64
	InvokeAt   uint64
	ReturnAt   uint64
	ActionID   string
	ActorID    string
	Kind       string
	Accepted   bool
	CauseID    string
	Checkpoint string
}

type SimHistoryAssertion struct {
	ID    string
	Check func(history []SimHistoryOperation, snapshot SimOracleSnapshot) error
}

type SimOracleConfig struct {
	Safety          []SimSafetyAssertion
	Liveness        []SimBoundedLivenessAssertion
	Reference       SimReferenceModel
	ReferenceSource SimOracleSource
	Differential    []SimDifferentialSubject
	History         []SimHistoryAssertion
}

type simLivenessState struct {
	active bool
	since  uint64
}

type SimOracleObservation struct {
	Step   uint64
	Now    uint64
	Values []SimOracleValue
}

type SimOracleValue struct {
	ID        string
	Canonical []byte
}

type SimOracle struct {
	world        *SimWorld
	config       SimOracleConfig
	liveness     map[string]simLivenessState
	history      []SimHistoryOperation
	observed     []SimOracleObservation
	firstFailure error
}

func NewSimOracle(world *SimWorld, config SimOracleConfig) (*SimOracle, error) {
	if world == nil {
		return nil, simOracleErrorf("oracle needs a simulation world")
	}
	config.Safety = append([]SimSafetyAssertion(nil), config.Safety...)
	config.Liveness = append([]SimBoundedLivenessAssertion(nil), config.Liveness...)
	config.Differential = append([]SimDifferentialSubject(nil), config.Differential...)
	config.History = append([]SimHistoryAssertion(nil), config.History...)
	sort.Slice(config.Safety, func(i, j int) bool { return config.Safety[i].ID < config.Safety[j].ID })
	sort.Slice(config.Liveness, func(i, j int) bool { return config.Liveness[i].ID < config.Liveness[j].ID })
	sort.Slice(config.Differential, func(i, j int) bool { return config.Differential[i].ID < config.Differential[j].ID })
	sort.Slice(config.History, func(i, j int) bool { return config.History[i].ID < config.History[j].ID })
	ids := map[string]struct{}{}
	validate := func(id string, callbackOK bool) error {
		if !validSimID(id) || !callbackOK {
			return simOracleErrorf("oracle assertion needs a stable id and callback")
		}
		if _, duplicate := ids[id]; duplicate {
			return simOracleErrorf("duplicate oracle id %q", id)
		}
		ids[id] = struct{}{}
		return nil
	}
	for _, assertion := range config.Safety {
		if err := validate(assertion.ID, assertion.Check != nil); err != nil {
			return nil, err
		}
	}
	for _, assertion := range config.Liveness {
		if err := validate(assertion.ID, assertion.When != nil && assertion.Satisfied != nil); err != nil {
			return nil, err
		}
	}
	hasComparativeOracle := config.Reference != nil || len(config.Differential) > 0
	hasIndependentOracle := false
	if config.Reference != nil {
		if err := validateSimOracleSource(config.ReferenceSource); err != nil {
			return nil, fmt.Errorf("reference source: %w", err)
		}
		hasIndependentOracle = config.ReferenceSource.Kind != SimOracleSourceSimulationOnly
	} else if config.ReferenceSource != (SimOracleSource{}) {
		return nil, simOracleErrorf("reference source was supplied without a reference model")
	}
	for _, subject := range config.Differential {
		if err := validate(subject.ID, subject.Observe != nil); err != nil {
			return nil, err
		}
		if err := validateSimOracleSource(subject.Source); err != nil {
			return nil, fmt.Errorf("differential %q source: %w", subject.ID, err)
		}
		if subject.Source.Kind != SimOracleSourceSimulationOnly {
			hasIndependentOracle = true
		}
	}
	if hasComparativeOracle && !hasIndependentOracle {
		return nil, simOracleErrorf("a simulation-only implementation cannot be the sole comparative oracle")
	}
	for _, assertion := range config.History {
		if err := validate(assertion.ID, assertion.Check != nil); err != nil {
			return nil, err
		}
	}
	return &SimOracle{world: world, config: config, liveness: map[string]simLivenessState{}}, nil
}

func validateSimOracleSource(source SimOracleSource) error {
	if !validSimID(source.ID) {
		return simOracleErrorf("oracle source needs a stable id")
	}
	switch source.Kind {
	case SimOracleSourceIndependentModel,
		SimOracleSourceProductionReducer,
		SimOracleSourceRealAdapter,
		SimOracleSourceSimulationOnly:
		return nil
	default:
		return simOracleErrorf("oracle source %q has unknown kind %q", source.ID, source.Kind)
	}
}

// Step is the oracle execution boundary. Callers must execute through Step (or
// RunUntilIdle) when claiming stepwise checking; running the world directly
// intentionally cannot make that claim.
func (o *SimOracle) Step() (SimStepResult, error) {
	if o.firstFailure != nil {
		return SimStepResult{}, o.firstFailure
	}
	result, err := o.world.Step()
	if err != nil || !result.Executed {
		return result, err
	}
	if o.config.Reference != nil {
		if err := o.config.Reference.Apply(result); err != nil {
			return result, o.fail("reference apply after action %q: %v", result.Item.Action.ID, err)
		}
	}
	observed, err := o.world.Observe()
	if err != nil {
		return result, o.fail("observe after action %q: %v", result.Item.Action.ID, err)
	}
	snapshot := SimOracleSnapshot{Step: o.world.Steps(), Now: o.world.Now(), Action: cloneSimAction(result.Item.Action), Accepted: result.Decision.Accepted, Observations: cloneOracleValues(observed)}
	operation := SimHistoryOperation{Step: snapshot.Step, At: result.Item.At, InvokeAt: result.Item.At, ReturnAt: result.Item.At, ActionID: result.Item.Action.ID, ActorID: result.Item.Action.ActorID, Kind: result.Item.Action.Kind, Accepted: result.Decision.Accepted, CauseID: result.Item.Action.CauseID, Checkpoint: result.Checkpoint.Digest}
	o.history = append(o.history, operation)
	encoded, err := encodeOracleObservation(snapshot.Step, snapshot.Now, observed)
	if err != nil {
		return result, o.fail("encode observations: %v", err)
	}
	o.observed = append(o.observed, encoded)
	if o.config.Reference != nil {
		if err := compareOracleMaps("reference", observed, o.config.Reference.Observe()); err != nil {
			return result, o.fail("%v", err)
		}
	}
	for _, subject := range o.config.Differential {
		if err := compareOracleMaps("differential "+subject.ID, observed, subject.Observe()); err != nil {
			return result, o.fail("%v", err)
		}
	}
	for _, assertion := range o.config.Safety {
		if err := assertion.Check(cloneOracleSnapshot(snapshot)); err != nil {
			return result, o.fail("safety %s: %v", assertion.ID, err)
		}
	}
	for _, assertion := range o.config.Liveness {
		state := o.liveness[assertion.ID]
		if assertion.Satisfied(cloneOracleSnapshot(snapshot)) {
			state = simLivenessState{}
		} else {
			if !state.active && assertion.When(cloneOracleSnapshot(snapshot)) {
				state = simLivenessState{active: true, since: snapshot.Step}
			}
			if state.active && snapshot.Step-state.since >= assertion.Bound {
				return result, o.fail("bounded liveness %s exceeded %d actions", assertion.ID, assertion.Bound)
			}
		}
		o.liveness[assertion.ID] = state
	}
	history := append([]SimHistoryOperation(nil), o.history...)
	for _, assertion := range o.config.History {
		if err := assertion.Check(history, cloneOracleSnapshot(snapshot)); err != nil {
			return result, o.fail("history %s: %v", assertion.ID, err)
		}
	}
	return result, nil
}

func (o *SimOracle) RunUntilIdle(maxSteps uint64) (SimRunResult, error) {
	if o.world.Pending() == 0 {
		if err := o.finalizeLiveness(); err != nil {
			return SimRunResult{}, err
		}
		return o.world.finish(SimRunIdle, "", 0)
	}
	if maxSteps == 0 {
		return o.world.finish(SimRunBoundExhausted, "max_steps", 0)
	}
	var ran uint64
	for ran < maxSteps && o.world.Pending() > 0 {
		if _, err := o.Step(); err != nil {
			return SimRunResult{Status: SimRunError, Steps: ran + 1, Now: o.world.Now()}, err
		}
		ran++
	}
	if o.world.Pending() > 0 {
		return o.world.finish(SimRunBoundExhausted, "max_steps", ran)
	}
	if err := o.finalizeLiveness(); err != nil {
		return SimRunResult{}, err
	}
	return o.world.finish(SimRunIdle, "", ran)
}

func (o *SimOracle) finalizeLiveness() error {
	for _, assertion := range o.config.Liveness {
		if o.liveness[assertion.ID].active {
			return o.fail("bounded liveness %s remained pending at idle", assertion.ID)
		}
	}
	return nil
}

func (o *SimOracle) fail(format string, args ...any) error {
	if o.firstFailure == nil {
		o.firstFailure = simOracleErrorf(format, args...)
	}
	return o.firstFailure
}

func (o *SimOracle) History() []SimHistoryOperation {
	return append([]SimHistoryOperation(nil), o.history...)
}
func (o *SimOracle) FirstFailure() error { return o.firstFailure }

// RecordHistoryOperation adds an explicitly timed operation from a modeled port
// or client. This is the route for histories with overlapping intervals.
func (o *SimOracle) RecordHistoryOperation(operation SimHistoryOperation) error {
	if !validSimID(operation.ActionID) || !validSimID(operation.Kind) || operation.InvokeAt > operation.ReturnAt {
		return simOracleErrorf("history operation needs stable ids and invoke_at <= return_at")
	}
	operation.Step = o.world.Steps()
	o.history = append(o.history, operation)
	return nil
}

func compareOracleMaps(label string, expected, actual map[string]any) error {
	keys := make([]string, 0, len(expected))
	for key := range expected {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(expected) != len(actual) {
		return simOracleErrorf("%s observation count differs: world=%d subject=%d", label, len(expected), len(actual))
	}
	for _, key := range keys {
		actualValue, ok := actual[key]
		if !ok {
			return simOracleErrorf("%s is missing observation %q", label, key)
		}
		expectedBytes, err := ReplayCanonicalBytes(expected[key])
		if err != nil {
			return err
		}
		actualBytes, err := ReplayCanonicalBytes(actualValue)
		if err != nil {
			return err
		}
		if !bytes.Equal(expectedBytes, actualBytes) {
			return simOracleErrorf("%s observation %q differs", label, key)
		}
	}
	return nil
}

func cloneOracleValues(values map[string]any) map[string]any {
	out := make(map[string]any, len(values))
	for key, value := range values {
		if cloned, err := freezeSimValue(value); err == nil {
			out[key] = cloned
		} else {
			out[key] = value
		}
	}
	return out
}

func cloneOracleSnapshot(snapshot SimOracleSnapshot) SimOracleSnapshot {
	snapshot.Action = cloneSimAction(snapshot.Action)
	snapshot.Observations = cloneOracleValues(snapshot.Observations)
	return snapshot
}

func encodeOracleObservation(step, now uint64, values map[string]any) (SimOracleObservation, error) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	encoded := SimOracleObservation{Step: step, Now: now, Values: make([]SimOracleValue, 0, len(keys))}
	for _, key := range keys {
		value, err := ReplayCanonicalBytes(values[key])
		if err != nil {
			return SimOracleObservation{}, err
		}
		encoded.Values = append(encoded.Values, SimOracleValue{ID: key, Canonical: append([]byte(nil), value...)})
	}
	return encoded, nil
}

type SimOracleFingerprint struct {
	TraceDigest             string
	ReplayLogDigest         string
	ReplayFingerprintDigest string
	ReplayFingerprint       ReplayFingerprintWire
	Observations            []SimOracleObservation
	Digest                  string
}

func (o *SimOracle) RecordFingerprint(harness *ReplayHarness) (SimOracleFingerprint, error) {
	if harness == nil {
		return SimOracleFingerprint{}, simOracleErrorf("oracle fingerprint needs a replay harness")
	}
	if err := o.fingerprintReady(); err != nil {
		return SimOracleFingerprint{}, err
	}
	traceDigest, err := o.world.TraceDigest()
	if err != nil {
		return SimOracleFingerprint{}, err
	}
	log, err := o.world.ReplayLog()
	if err != nil {
		return SimOracleFingerprint{}, err
	}
	replayFingerprint, err := harness.Record(log)
	if err != nil {
		return SimOracleFingerprint{}, err
	}
	fingerprint := SimOracleFingerprint{TraceDigest: traceDigest, ReplayLogDigest: log.Digest(), ReplayFingerprintDigest: replayFingerprint.Digest(), ReplayFingerprint: replayFingerprint.Wire(), Observations: cloneOracleObservations(o.observed)}
	digest, err := ReplayCanonicalDigest(fingerprintWithoutDigest(fingerprint))
	if err != nil {
		return SimOracleFingerprint{}, err
	}
	fingerprint.Digest = digest
	return fingerprint, nil
}

// VerifyFingerprint validates trace, log, and replay-fingerprint identities
// before comparing any observation bytes.
func (o *SimOracle) VerifyFingerprint(expected SimOracleFingerprint, harness *ReplayHarness) error {
	if harness == nil {
		return simOracleErrorf("oracle fingerprint needs a replay harness")
	}
	if err := o.fingerprintReady(); err != nil {
		return err
	}
	digest, err := ReplayCanonicalDigest(fingerprintWithoutDigest(expected))
	if err != nil {
		return err
	}
	if digest != expected.Digest {
		return simOracleErrorf("oracle fingerprint identity mismatch")
	}
	traceDigest, err := o.world.TraceDigest()
	if err != nil {
		return err
	}
	if traceDigest != expected.TraceDigest {
		return simOracleErrorf("simulation trace digest mismatch")
	}
	log, err := o.world.ReplayLog()
	if err != nil {
		return err
	}
	if log.Digest() != expected.ReplayLogDigest {
		return simOracleErrorf("simulation replay log digest mismatch")
	}
	replayExpected, err := ReplayFingerprintFromWire(expected.ReplayFingerprint)
	if err != nil {
		return err
	}
	if replayExpected.Digest() != expected.ReplayFingerprintDigest {
		return simOracleErrorf("replay fingerprint identity mismatch")
	}
	if _, err := harness.Verify(log, replayExpected); err != nil {
		return err
	}
	actual := cloneOracleObservations(o.observed)
	if err := compareOracleObservationSequence(expected.Observations, actual); err != nil {
		return err
	}
	return nil
}

func (o *SimOracle) fingerprintReady() error {
	if o.firstFailure != nil {
		return simOracleErrorf("cannot fingerprint a failed oracle: %v", o.firstFailure)
	}
	if uint64(len(o.observed)) != o.world.Steps() {
		return simOracleErrorf("world executed %d actions but oracle checked %d; execution bypassed the oracle boundary", o.world.Steps(), len(o.observed))
	}
	return nil
}

func fingerprintWithoutDigest(f SimOracleFingerprint) any {
	return struct {
		TraceDigest, ReplayLogDigest, ReplayFingerprintDigest string
		ReplayFingerprint                                     ReplayFingerprintWire
		Observations                                          []SimOracleObservation
	}{f.TraceDigest, f.ReplayLogDigest, f.ReplayFingerprintDigest, f.ReplayFingerprint, f.Observations}
}

func cloneOracleObservations(in []SimOracleObservation) []SimOracleObservation {
	out := append([]SimOracleObservation(nil), in...)
	for i := range out {
		out[i].Values = append([]SimOracleValue(nil), out[i].Values...)
		for j := range out[i].Values {
			out[i].Values[j].Canonical = append([]byte(nil), out[i].Values[j].Canonical...)
		}
	}
	return out
}

func compareOracleObservationSequence(expected, actual []SimOracleObservation) error {
	if len(expected) != len(actual) {
		return simOracleErrorf("oracle observation checkpoint count differs: expected=%d actual=%d", len(expected), len(actual))
	}
	for i := range expected {
		if expected[i].Step != actual[i].Step || expected[i].Now != actual[i].Now || len(expected[i].Values) != len(actual[i].Values) {
			return simOracleErrorf("oracle observation checkpoint %d differs", i)
		}
		for j := range expected[i].Values {
			if expected[i].Values[j].ID != actual[i].Values[j].ID || !bytes.Equal(expected[i].Values[j].Canonical, actual[i].Values[j].Canonical) {
				return simOracleErrorf("oracle observation checkpoint %d value %d differs", i, j)
			}
		}
	}
	return nil
}

// SimLinearizabilityCapability is deliberately unconstructable outside this
// package. Claiming it is an explicit assertion that the subject publishes a
// sequential specification and operation intervals suitable for checking.
type SimLinearizabilityCapability struct {
	subject, version string
	claimed          bool
}

func ClaimSimLinearizability(subject, version string) (SimLinearizabilityCapability, error) {
	if !validSimID(subject) || version == "" {
		return SimLinearizabilityCapability{}, simOracleErrorf("linearizability claim needs a stable subject and version")
	}
	return SimLinearizabilityCapability{subject: subject, version: version, claimed: true}, nil
}

type SimLinearizabilityChecker func(history []SimHistoryOperation) error

func (o *SimOracle) CheckLinearizability(capability SimLinearizabilityCapability, checker SimLinearizabilityChecker) error {
	if !capability.claimed || checker == nil {
		return simOracleErrorf("linearizability checking requires an explicit claimed capability and checker")
	}
	if err := checker(o.History()); err != nil {
		return o.fail("linearizability %s/%s: %v", capability.subject, capability.version, err)
	}
	return nil
}
