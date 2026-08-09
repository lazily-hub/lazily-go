package lazily

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

// Cross-language conformance tests for the full Harel state-chart spec,
// replaying the canonical fixtures every binding replays. Each test loads a
// chart, asserts initial_active (and initial_actions when present), replays the
// steps, and asserts accepted, active, matches, and actions after each step.
//
// Fixtures mirror lazily-spec/conformance/statechart/ byte-identically. When
// that source tree is reachable on disk (sibling repo), it is used; otherwise
// the test is skipped so drift in an absent spec tree cannot fail the build.

var statechartFixtureNames = []string{
	"flat_cycle.json",
	"hierarchical_player.json",
	"guarded_door.json",
	"parallel_regions.json",
	"history_shallow.json",
	"history_deep.json",
	"entry_exit_actions.json",
}

// statechartSpecDir resolves the sibling lazily-spec conformance directory.
func statechartSpecDir() string {
	return specPath("statechart")
}

type statechartStep struct {
	conformanceDoc
	Event    string          `json:"event"`
	Guards   map[string]bool `json:"guards"`
	Accepted bool            `json:"accepted"`
	Active   json.RawMessage `json:"active"`
	Matches  map[string]bool `json:"matches"`
	Actions  *[]string       `json:"actions"`
}

type statechartFixture struct {
	conformanceMeta
	Chart          json.RawMessage  `json:"chart"`
	InitialActive  json.RawMessage  `json:"initial_active"`
	InitialActions []string         `json:"initial_actions"`
	Steps          []statechartStep `json:"steps"`
}

// activeExpected normalizes the fixture's `active`/`initial_active` (string or
// array of strings) into a sorted slice.
func activeExpected(t *testing.T, raw json.RawMessage) []string {
	t.Helper()
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return []string{single}
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("active must be a string or array: %v", err)
	}
	sort.Strings(list)
	return list
}

func loadStatechartFixture(t *testing.T, name string) (statechartFixture, bool) {
	t.Helper()
	path := filepath.Join(statechartSpecDir(), name)
	data, err := specReadFile(path)
	if err != nil {
		return statechartFixture{}, false
	}
	var fx statechartFixture
	mustStrictJSON(t, name, data, &fx)
	return fx, true
}

func replayStatechartFixture(t *testing.T, name string, fx statechartFixture) {
	t.Helper()

	def, err := ChartDefFromJSON(fx.Chart)
	if err != nil {
		t.Fatalf("%s: ChartDefFromJSON: %v", name, err)
	}
	ctx := NewContext()
	chart := NewStateChart(ctx, def)

	// initial_active (asserted once before any step).
	wantInitial := activeExpected(t, fx.InitialActive)
	if got := chart.ActiveLeaves(); !reflect.DeepEqual(got, wantInitial) {
		t.Fatalf("%s: initial_active: got %v, want %v", name, got, wantInitial)
	}

	// initial_actions (optional).
	if len(fx.InitialActions) > 0 {
		if got := chart.LastActions(); !reflect.DeepEqual(got, fx.InitialActions) {
			t.Fatalf("%s: initial_actions: got %v, want %v", name, got, fx.InitialActions)
		}
	}

	for i, step := range fx.Steps {
		accepted := chart.Send(step.Event, step.Guards)
		if accepted != step.Accepted {
			t.Fatalf("%s: step %d %q accepted: got %v, want %v", name, i, step.Event, accepted, step.Accepted)
		}

		wantActive := activeExpected(t, step.Active)
		if got := chart.ActiveLeaves(); !reflect.DeepEqual(got, wantActive) {
			t.Fatalf("%s: step %d %q active: got %v, want %v", name, i, step.Event, got, wantActive)
		}

		for id, want := range step.Matches {
			if got := chart.Matches(id); got != want {
				t.Fatalf("%s: step %d %q matches(%s): got %v, want %v", name, i, step.Event, id, got, want)
			}
		}

		if step.Actions != nil {
			want := *step.Actions
			got := chart.LastActions()
			// Normalize nil vs empty slice for equality.
			if len(want) == 0 && len(got) == 0 {
				continue
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: step %d %q actions: got %v, want %v", name, i, step.Event, got, want)
			}
		}
	}
}

func TestStatechartConformance(t *testing.T) {
	for _, name := range statechartFixtureNames {
		name := name
		t.Run(name, func(t *testing.T) {
			fx, ok := loadStatechartFixture(t, name)
			if !ok {
				t.Skipf("lazily-spec fixture absent: %s", filepath.Join(statechartSpecDir(), name))
			}
			replayStatechartFixture(t, name, fx)
		})
	}
}

func TestMalformedStatechartCorpusIsRejected(t *testing.T) {
	data, err := specReadFile(filepath.Join(statechartSpecDir(), "malformed_rejected.json"))
	if err != nil {
		t.Fatalf("malformed statechart fixture missing from lazily-spec: %v", err)
	}
	var fixture struct {
		Cases []struct {
			Name  string          `json:"name"`
			Chart json.RawMessage `json:"chart"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Cases) == 0 {
		t.Fatal("malformed corpus must contain cases")
	}
	for _, scenario := range fixture.Cases {
		if _, err := ChartDefFromJSON(scenario.Chart); err == nil {
			t.Fatalf("malformed statechart case %q was accepted", scenario.Name)
		}
	}
}
