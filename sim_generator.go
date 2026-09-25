package lazily

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

var ErrSimGeneration = errors.New("lazily: deterministic simulation generation failed")

func simGenerationErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrSimGeneration}, args...)...)
}

// SimGeneratedAction retains the model command that produced an ordinary
// SimAction. Command names are shrink-time authority; action kinds need not map
// one-to-one to model commands.
type SimGeneratedAction struct {
	Command string
	Action  SimAction
}

type SimGeneratedScenario struct {
	GeneratorName    string
	GeneratorVersion string
	ScenarioIndex    uint64
	SeedHex          string
	Actions          []SimGeneratedAction
	CoverageLabels   []string
}

// SimModelCommand describes one stateful generator choice. All callbacks must
// be deterministic and side-effect-free except Apply's returned model value.
type SimModelCommand[M any] struct {
	Name         string
	Weight       uint64
	Precondition func(model M, history []SimGeneratedAction) bool
	Build        func(random *SimRandomStream, model M, ordinal uint64) (SimAction, error)
	Apply        func(model M, action SimAction) (M, error)
	Coverage     func(before M, action SimAction, after M) []string
}

type SimGeneratorSpec[M any] struct {
	Name         string
	Version      string
	InitialModel M
	// CloneModel is required when M contains mutable reference state. Value-like
	// models may leave it nil.
	CloneModel        func(M) M
	Commands          []SimModelCommand[M]
	MaxActions        uint64
	MaxHistory        uint64
	DeclaredScenarios []string
	Coverage          *SimCoverageAudit
}

type SimGenerator[M any] struct {
	seed SimSeed
	spec SimGeneratorSpec[M]
}

func NewSimGenerator[M any](world *SimWorld, spec SimGeneratorSpec[M]) (*SimGenerator[M], error) {
	if world == nil || !validSimID(spec.Name) || spec.Version == "" || spec.MaxActions == 0 || spec.MaxActions > uint64(math.MaxInt) {
		return nil, simGenerationErrorf("generator needs a world, stable name/version, and nonzero action bound")
	}
	seen := map[string]struct{}{}
	for _, command := range spec.Commands {
		if !validSimID(command.Name) || command.Build == nil || command.Apply == nil {
			return nil, simGenerationErrorf("command %q needs a stable name, Build, and Apply", command.Name)
		}
		if _, duplicate := seen[command.Name]; duplicate {
			return nil, simGenerationErrorf("duplicate command %q", command.Name)
		}
		seen[command.Name] = struct{}{}
	}
	if spec.Coverage == nil {
		spec.Coverage = NewSimCoverageAudit()
	}
	spec.Commands = append([]SimModelCommand[M](nil), spec.Commands...)
	spec.DeclaredScenarios = append([]string(nil), spec.DeclaredScenarios...)
	for name := range seen {
		spec.Coverage.DeclareAction(name)
	}
	for _, label := range spec.DeclaredScenarios {
		if err := spec.Coverage.DeclareScenario(label); err != nil {
			return nil, err
		}
	}
	return &SimGenerator[M]{seed: world.seed, spec: spec}, nil
}

func (g *SimGenerator[M]) Coverage() *SimCoverageAudit { return g.spec.Coverage }

func (g *SimGenerator[M]) AuditCoverage() (SimCoverageReport, error) {
	return g.spec.Coverage.Audit()
}

// Generate derives fresh index-and-ordinal-isolated streams using SimWorld's
// named RNG algorithm. Selection and payload streams are separate, so Build's
// draw count cannot perturb later command selection. Repeating an index returns
// the same scenario without consuming or changing runtime-world RNG state.
func (g *SimGenerator[M]) Generate(index uint64) (SimGeneratedScenario, error) {
	model := g.spec.InitialModel
	if g.spec.CloneModel != nil {
		model = g.spec.CloneModel(model)
	}
	actions := make([]SimGeneratedAction, 0, g.spec.MaxActions)
	labels := map[string]struct{}{}
	actionCoverage := map[string]uint64{}
	seenIDs := map[string]struct{}{}
	for ordinal := uint64(0); ordinal < g.spec.MaxActions; ordinal++ {
		ordinalLabel := strconv.FormatUint(ordinal, 10)
		selectStream, err := newSimRandomStream(g.seed, []string{"generator", g.spec.Name, g.spec.Version, strconv.FormatUint(index, 10), "select", ordinalLabel})
		if err != nil {
			return SimGeneratedScenario{}, err
		}
		history := boundedSimHistory(actions, g.spec.MaxHistory)
		eligible := make([]int, 0, len(g.spec.Commands))
		var total uint64
		for i, command := range g.spec.Commands {
			if command.Weight == 0 {
				continue
			}
			if command.Precondition != nil && !command.Precondition(model, history) {
				continue
			}
			if math.MaxUint64-total < command.Weight {
				return SimGeneratedScenario{}, simGenerationErrorf("eligible command weights overflow uint64")
			}
			total += command.Weight
			eligible = append(eligible, i)
		}
		if len(eligible) == 0 {
			break
		}
		sort.Slice(eligible, func(i, j int) bool { return g.spec.Commands[eligible[i]].Name < g.spec.Commands[eligible[j]].Name })
		draw, err := selectStream.Uint64N(total)
		if err != nil {
			return SimGeneratedScenario{}, err
		}
		selected := eligible[len(eligible)-1]
		for _, i := range eligible {
			if draw < g.spec.Commands[i].Weight {
				selected = i
				break
			}
			draw -= g.spec.Commands[i].Weight
		}
		command := g.spec.Commands[selected]
		payloadStream, err := newSimRandomStream(g.seed, []string{"generator", g.spec.Name, g.spec.Version, strconv.FormatUint(index, 10), "payload", command.Name, ordinalLabel})
		if err != nil {
			return SimGeneratedScenario{}, err
		}
		action, err := command.Build(payloadStream, model, ordinal)
		if err != nil {
			return SimGeneratedScenario{}, fmt.Errorf("%w: build command %q: %v", ErrSimGeneration, command.Name, err)
		}
		if err := validateSimAction(action); err != nil {
			return SimGeneratedScenario{}, err
		}
		if _, duplicate := seenIDs[action.ID]; duplicate {
			return SimGeneratedScenario{}, simGenerationErrorf("command %q generated duplicate action id %q", command.Name, action.ID)
		}
		if action.CauseID != "" {
			if _, resolved := seenIDs[action.CauseID]; !resolved {
				return SimGeneratedScenario{}, simGenerationErrorf("command %q generated unresolved cause %q", command.Name, action.CauseID)
			}
		}
		if _, err := ReplayCanonicalBytes(action.Payload); err != nil {
			return SimGeneratedScenario{}, fmt.Errorf("%w: command %q payload: %v", ErrSimGeneration, command.Name, err)
		}
		payload, err := freezeSimValue(action.Payload)
		if err != nil {
			return SimGeneratedScenario{}, fmt.Errorf("%w: command %q payload ownership: %v", ErrSimGeneration, command.Name, err)
		}
		action.Payload = payload
		frozen := cloneSimAction(action)
		after, err := command.Apply(model, cloneSimAction(frozen))
		if err != nil {
			return SimGeneratedScenario{}, fmt.Errorf("%w: apply command %q: %v", ErrSimGeneration, command.Name, err)
		}
		actions = append(actions, SimGeneratedAction{Command: command.Name, Action: frozen})
		seenIDs[action.ID] = struct{}{}
		actionCoverage[command.Name]++
		if command.Coverage != nil {
			for _, label := range command.Coverage(model, cloneSimAction(frozen), after) {
				if _, declared := g.spec.Coverage.declaredScenarios[label]; !declared {
					return SimGeneratedScenario{}, simGenerationErrorf("scenario coverage label %q was not declared", label)
				}
				labels[label] = struct{}{}
			}
		}
		model = after
	}
	coverage := make([]string, 0, len(labels))
	for label := range labels {
		coverage = append(coverage, label)
	}
	sort.Strings(coverage)
	for name, count := range actionCoverage {
		if math.MaxUint64-g.spec.Coverage.actionCounts[name] < count {
			return SimGeneratedScenario{}, simGenerationErrorf("action coverage count overflow for %q", name)
		}
	}
	for _, label := range coverage {
		if g.spec.Coverage.scenarioCounts[label] == math.MaxUint64 {
			return SimGeneratedScenario{}, simGenerationErrorf("scenario coverage count overflow for %q", label)
		}
	}
	for name, count := range actionCoverage {
		g.spec.Coverage.actionCounts[name] += count
	}
	for _, label := range coverage {
		g.spec.Coverage.scenarioCounts[label]++
	}
	return SimGeneratedScenario{g.spec.Name, g.spec.Version, index, g.seed.String(), actions, coverage}, nil
}

func boundedSimHistory(actions []SimGeneratedAction, max uint64) []SimGeneratedAction {
	if max == 0 {
		return nil
	}
	start := 0
	if uint64(len(actions)) > max {
		start = len(actions) - int(max)
	}
	out := append([]SimGeneratedAction(nil), actions[start:]...)
	for i := range out {
		out[i].Action = cloneSimAction(out[i].Action)
	}
	return out
}

type SimCoverageAudit struct {
	declaredActions   map[string]struct{}
	declaredScenarios map[string]struct{}
	actionCounts      map[string]uint64
	scenarioCounts    map[string]uint64
}

func NewSimCoverageAudit() *SimCoverageAudit {
	return &SimCoverageAudit{map[string]struct{}{}, map[string]struct{}{}, map[string]uint64{}, map[string]uint64{}}
}

func (a *SimCoverageAudit) DeclareAction(name string) {
	a.ensureMaps()
	a.declaredActions[name] = struct{}{}
}
func (a *SimCoverageAudit) DeclareScenario(label string) error {
	a.ensureMaps()
	if !validSimID(label) {
		return simGenerationErrorf("invalid scenario coverage label %q", label)
	}
	a.declaredScenarios[label] = struct{}{}
	return nil
}
func (a *SimCoverageAudit) RecordAction(name string) {
	a.ensureMaps()
	a.actionCounts[name]++
}
func (a *SimCoverageAudit) RecordScenario(label string) error {
	a.ensureMaps()
	if _, declared := a.declaredScenarios[label]; !declared {
		return simGenerationErrorf("scenario coverage label %q was not declared", label)
	}
	a.scenarioCounts[label]++
	return nil
}

type SimCoverageReport struct {
	ActionCounts     map[string]uint64
	ScenarioCounts   map[string]uint64
	MissingActions   []string
	MissingScenarios []string
}

func (a *SimCoverageAudit) Audit() (SimCoverageReport, error) {
	a.ensureMaps()
	report := SimCoverageReport{map[string]uint64{}, map[string]uint64{}, nil, nil}
	for name := range a.declaredActions {
		report.ActionCounts[name] = a.actionCounts[name]
		if a.actionCounts[name] == 0 {
			report.MissingActions = append(report.MissingActions, name)
		}
	}
	for label := range a.declaredScenarios {
		report.ScenarioCounts[label] = a.scenarioCounts[label]
		if a.scenarioCounts[label] == 0 {
			report.MissingScenarios = append(report.MissingScenarios, label)
		}
	}
	sort.Strings(report.MissingActions)
	sort.Strings(report.MissingScenarios)
	if len(report.MissingActions)+len(report.MissingScenarios) > 0 {
		return report, simGenerationErrorf("coverage incomplete: missing actions=%v scenarios=%v", report.MissingActions, report.MissingScenarios)
	}
	return report, nil
}

func (a *SimCoverageAudit) ensureMaps() {
	if a.declaredActions == nil {
		a.declaredActions = map[string]struct{}{}
	}
	if a.declaredScenarios == nil {
		a.declaredScenarios = map[string]struct{}{}
	}
	if a.actionCounts == nil {
		a.actionCounts = map[string]uint64{}
	}
	if a.scenarioCounts == nil {
		a.scenarioCounts = map[string]uint64{}
	}
}

type SimShrinkCommand[M any] struct {
	Precondition func(model M, history []SimGeneratedAction, action SimAction) bool
	Apply        func(model M, action SimAction) (M, error)
	// Dependencies returns additional action IDs required by this command.
	// CauseID is always treated as a dependency without listing it here.
	Dependencies func(action SimAction) []string
}

type SimCausalShrinker[M any] struct {
	InitialModel M
	CloneModel   func(M) M
	Commands     map[string]SimShrinkCommand[M]
	MaxHistory   uint64
	MaxAttempts  uint64
}

// Shrink returns a dependency-closed, command-valid, one-removal-minimal
// failing sequence. IDs are never rewritten, so causal identities remain useful
// as committed regression fixtures. fails must recognize the specific target
// failure (not merely any error) when failure identity matters.
func (s SimCausalShrinker[M]) Shrink(input []SimGeneratedAction, fails func([]SimGeneratedAction) (bool, error)) ([]SimGeneratedAction, error) {
	if fails == nil {
		return nil, simGenerationErrorf("shrinker needs a failure predicate")
	}
	current := cloneGeneratedActions(input)
	if !s.valid(current) {
		return nil, simGenerationErrorf("initial sequence is not command-valid")
	}
	ok, err := fails(cloneGeneratedActions(current))
	if err != nil || !ok {
		if err != nil {
			return nil, err
		}
		return nil, simGenerationErrorf("initial sequence does not fail")
	}
	limit := s.MaxAttempts
	if limit == 0 {
		limit = math.MaxUint64
	}
	var attempts uint64
	chunk := len(current) / 2
	if chunk == 0 && len(current) > 0 {
		chunk = 1
	}
	for chunk >= 1 && attempts < limit {
		changed := false
		for start := 0; start < len(current) && attempts < limit; start += chunk {
			end := start + chunk
			if end > len(current) {
				end = len(current)
			}
			candidate := s.causalDelete(current, start, end)
			if len(candidate) == len(current) || !s.valid(candidate) {
				continue
			}
			attempts++
			failed, err := fails(cloneGeneratedActions(candidate))
			if err != nil {
				return nil, err
			}
			if failed {
				current, changed = candidate, true
				break
			}
		}
		if !changed {
			if chunk == 1 {
				break
			}
			chunk /= 2
		}
		if changed && chunk > len(current) {
			chunk = len(current)
		}
	}
	return cloneGeneratedActions(current), nil
}

func (s SimCausalShrinker[M]) valid(actions []SimGeneratedAction) bool {
	model := s.InitialModel
	if s.CloneModel != nil {
		model = s.CloneModel(model)
	}
	seen := map[string]struct{}{}
	for i, generated := range actions {
		if _, duplicate := seen[generated.Action.ID]; duplicate {
			return false
		}
		if generated.Action.CauseID != "" {
			if _, ok := seen[generated.Action.CauseID]; !ok {
				return false
			}
		}
		command, ok := s.Commands[generated.Command]
		if !ok || command.Apply == nil {
			return false
		}
		if command.Dependencies != nil {
			for _, dependency := range command.Dependencies(cloneSimAction(generated.Action)) {
				if _, ok := seen[dependency]; dependency == "" || !ok {
					return false
				}
			}
		}
		history := boundedSimHistory(actions[:i], s.MaxHistory)
		if command.Precondition != nil && !command.Precondition(model, history, cloneSimAction(generated.Action)) {
			return false
		}
		next, err := command.Apply(model, cloneSimAction(generated.Action))
		if err != nil {
			return false
		}
		model = next
		seen[generated.Action.ID] = struct{}{}
	}
	return true
}

func (s SimCausalShrinker[M]) causalDelete(actions []SimGeneratedAction, start, end int) []SimGeneratedAction {
	removed := map[string]struct{}{}
	for _, action := range actions[start:end] {
		removed[action.Action.ID] = struct{}{}
	}
	changed := true
	for changed {
		changed = false
		for _, action := range actions {
			if _, gone := removed[action.Action.ID]; gone {
				continue
			}
			if _, causeGone := removed[action.Action.CauseID]; action.Action.CauseID != "" && causeGone {
				removed[action.Action.ID] = struct{}{}
				changed = true
				continue
			}
			command := s.Commands[action.Command]
			if command.Dependencies != nil {
				for _, dependency := range command.Dependencies(cloneSimAction(action.Action)) {
					if _, dependencyGone := removed[dependency]; dependencyGone {
						removed[action.Action.ID] = struct{}{}
						changed = true
						break
					}
				}
			}
		}
	}
	out := make([]SimGeneratedAction, 0, len(actions)-len(removed))
	for _, action := range actions {
		if _, gone := removed[action.Action.ID]; !gone {
			out = append(out, action)
		}
	}
	return cloneGeneratedActions(out)
}

func cloneGeneratedActions(actions []SimGeneratedAction) []SimGeneratedAction {
	out := append([]SimGeneratedAction(nil), actions...)
	for i := range out {
		out[i].Action = cloneSimAction(out[i].Action)
	}
	return out
}

type SimCounterexample struct {
	Scenario SimGeneratedScenario
	Failure  string
}

type SimPersistedCounterexample struct{ RegressionPath, FuzzCorpusPath, Digest string }

// PersistSimCounterexample writes caller-encoded reversible bytes as a normal
// regression fixture and as a []byte Go fuzz seed. File names are content based.
// Requiring a codec avoids pretending ReplayCanonicalBytes is decodable.
func PersistSimCounterexample(regressionDir, fuzzCorpusDir string, counterexample SimCounterexample, encode func(SimCounterexample) ([]byte, error)) (SimPersistedCounterexample, error) {
	if encode == nil {
		return SimPersistedCounterexample{}, simGenerationErrorf("counterexample persistence needs a reversible encoder")
	}
	bytes, err := encode(counterexample)
	if err != nil {
		return SimPersistedCounterexample{}, err
	}
	if len(bytes) == 0 {
		return SimPersistedCounterexample{}, simGenerationErrorf("counterexample encoder returned empty bytes")
	}
	sum := sha256.Sum256(bytes)
	digest := hex.EncodeToString(sum[:])
	if err := os.MkdirAll(regressionDir, 0o755); err != nil {
		return SimPersistedCounterexample{}, err
	}
	if err := os.MkdirAll(fuzzCorpusDir, 0o755); err != nil {
		return SimPersistedCounterexample{}, err
	}
	regression := filepath.Join(regressionDir, digest+".simcase")
	corpus := filepath.Join(fuzzCorpusDir, digest)
	if err := writeSimContentAddressed(regression, bytes); err != nil {
		return SimPersistedCounterexample{}, err
	}
	fuzz := []byte("go test fuzz v1\n[]byte(" + strconv.Quote(string(bytes)) + ")\n")
	if err := writeSimContentAddressed(corpus, fuzz); err != nil {
		return SimPersistedCounterexample{}, err
	}
	return SimPersistedCounterexample{regression, corpus, digest}, nil
}

func writeSimContentAddressed(path string, content []byte) error {
	if existing, err := os.ReadFile(path); err == nil {
		if !bytes.Equal(existing, content) {
			return simGenerationErrorf("content-addressed path %q already contains different bytes", path)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".sim-counterexample-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// Link publishes atomically without overwriting an existing digest path.
	if err := os.Link(tmpPath, path); err != nil {
		if existing, readErr := os.ReadFile(path); readErr == nil {
			if bytes.Equal(existing, content) {
				return nil
			}
			return simGenerationErrorf("content-addressed path %q raced with different bytes", path)
		}
		return err
	}
	return nil
}
