package lazily

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// Cross-language conformance for stream windowing (#lzwindow) — see
// lazily-spec/docs/windowing.md and lazily-spec/conformance/windowing/*.json.
// All fixtures use a Sum (uint64) aggregate for determinism. Each fixture
// replays its op stream, asserting the emitted aggregate (`returns`), the
// projected output reader (`expected.output`), and emit-only reader
// invalidation (`expected.invalidates.output`).

func windowingSpecDir() string {
	return filepath.Join("..", "lazily-spec", "conformance", "windowing")
}

type windowingStep struct {
	Op struct {
		Type  string  `json:"type"`
		Now   *uint64 `json:"now"`
		Value *uint64 `json:"value"`
	} `json:"op"`
	Returns  *uint64 `json:"returns"`
	Expected struct {
		Output      *uint64 `json:"output"`
		Invalidates struct {
			Output bool `json:"output"`
		} `json:"invalidates"`
	} `json:"expected"`
}

type windowingFixture struct {
	Config struct {
		N      *uint64 `json:"n"`
		Period *uint64 `json:"period"`
		Size   *uint64 `json:"size"`
		Slide  *uint64 `json:"slide"`
		Gap    *uint64 `json:"gap"`
	} `json:"config"`
	Steps []windowingStep `json:"steps"`
}

// loadWindowingFixture returns (fixture, true) when the sibling spec file is
// reachable; otherwise (zero, false) so an absent spec tree skips rather than
// fails (mirrors the rs `present()` guard and the statechart loader).
func loadWindowingFixture(t *testing.T, name string) (windowingFixture, bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(windowingSpecDir(), name))
	if err != nil {
		return windowingFixture{}, false
	}
	var fx windowingFixture
	if err := json.Unmarshal(data, &fx); err != nil {
		t.Fatalf("%s: unmarshal fixture: %v", name, err)
	}
	return fx, true
}

// optEq compares an emitted/projected Opt[uint64] against a nullable fixture
// value (nil == absent).
func optEq(o Opt[uint64], want *uint64) bool {
	if want == nil {
		return !o.Present
	}
	return o.Present && o.Value == *want
}

func optStr(o Opt[uint64]) string {
	if !o.Present {
		return "null"
	}
	return strconv.FormatUint(o.Value, 10)
}

func wantStr(p *uint64) string {
	if p == nil {
		return "null"
	}
	return strconv.FormatUint(*p, 10)
}

// windowingCheck asserts the emit edge, the projected output, and reader
// invalidation for one step. `observed` wraps the window's output cell; its
// warmth after the op tells us whether the op invalidated the reader.
func windowingCheck(t *testing.T, i int, observed *FormulaCell[Opt[uint64]], step windowingStep, emitted, output Opt[uint64]) {
	t.Helper()
	if !optEq(emitted, step.Returns) {
		t.Fatalf("step %d: emit got %s, want %s", i, optStr(emitted), wantStr(step.Returns))
	}
	if !optEq(output, step.Expected.Output) {
		t.Fatalf("step %d: output got %s, want %s", i, optStr(output), wantStr(step.Expected.Output))
	}
	_, warm := observed.Peek()
	observed.Get() // re-prime for the next step
	if inval := !warm; inval != step.Expected.Invalidates.Output {
		t.Fatalf("step %d: invalidates got %v, want %v", i, inval, step.Expected.Invalidates.Output)
	}
}

func TestWindowingConformance(t *testing.T) {
	t.Run("tumbling_count.json", func(t *testing.T) {
		fx, ok := loadWindowingFixture(t, "tumbling_count.json")
		if !ok {
			t.Skip("lazily-spec conformance/windowing not reachable")
		}
		ctx := NewContext()
		w := TumblingCount(ctx, *fx.Config.N, Sum[uint64]())
		oc := w.OutputCell()
		observed := NewFormulaCell(ctx, func(_ *Context) Opt[uint64] { return oc.Get() })
		observed.Get() // prime
		for i, step := range fx.Steps {
			emitted := w.Push(*step.Op.Value)
			windowingCheck(t, i, observed, step, emitted, w.Output())
		}
	})

	t.Run("tumbling_time.json", func(t *testing.T) {
		fx, ok := loadWindowingFixture(t, "tumbling_time.json")
		if !ok {
			t.Skip("lazily-spec conformance/windowing not reachable")
		}
		ctx := NewContext()
		w := TumblingTime(ctx, *fx.Config.Period, Sum[uint64]())
		oc := w.OutputCell()
		observed := NewFormulaCell(ctx, func(_ *Context) Opt[uint64] { return oc.Get() })
		observed.Get()
		for i, step := range fx.Steps {
			var emitted Opt[uint64]
			if step.Op.Type == "push" {
				w.Push(*step.Op.Now, *step.Op.Value)
			} else {
				emitted = w.Tick(*step.Op.Now)
			}
			windowingCheck(t, i, observed, step, emitted, w.Output())
		}
	})

	t.Run("sliding_count.json", func(t *testing.T) {
		fx, ok := loadWindowingFixture(t, "sliding_count.json")
		if !ok {
			t.Skip("lazily-spec conformance/windowing not reachable")
		}
		ctx := NewContext()
		w := Sliding(ctx, int(*fx.Config.Size), *fx.Config.Slide, Sum[uint64]())
		oc := w.OutputCell()
		observed := NewFormulaCell(ctx, func(_ *Context) Opt[uint64] { return oc.Get() })
		observed.Get()
		for i, step := range fx.Steps {
			emitted := w.Push(*step.Op.Value)
			windowingCheck(t, i, observed, step, emitted, w.Output())
		}
	})

	t.Run("session.json", func(t *testing.T) {
		fx, ok := loadWindowingFixture(t, "session.json")
		if !ok {
			t.Skip("lazily-spec conformance/windowing not reachable")
		}
		ctx := NewContext()
		w := Session(ctx, *fx.Config.Gap, Sum[uint64]())
		oc := w.OutputCell()
		observed := NewFormulaCell(ctx, func(_ *Context) Opt[uint64] { return oc.Get() })
		observed.Get()
		for i, step := range fx.Steps {
			var emitted Opt[uint64]
			if step.Op.Type == "push" {
				emitted = w.Push(*step.Op.Now, *step.Op.Value)
			} else {
				emitted = w.Flush(*step.Op.Now)
			}
			windowingCheck(t, i, observed, step, emitted, w.Output())
		}
	})
}
