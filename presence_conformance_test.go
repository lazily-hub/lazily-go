package lazily

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

// Cross-language conformance for the presence + ephemeral plane (#lzpresence) —
// see lazily-spec/conformance/presence/*.json. Mirrors lazily-rs
// tests/presence_conformance.rs: presence heartbeat/evict/TTL, awareness
// last-writer, ephemeral value expiry, and live-view reader invalidation.
//
// Fixtures mirror lazily-spec/conformance/presence/ byte-identically. When that
// sibling tree is reachable the test runs; otherwise it skips.

func presenceSpecDir() string {
	return filepath.Join("..", "lazily-spec", "conformance", "presence")
}

type presenceOp struct {
	Type  string `json:"type"`
	Peer  uint64 `json:"peer"`
	Value string `json:"value"`
	Now   uint64 `json:"now"`
	TTL   uint64 `json:"ttl"`
}

type presenceExpected struct {
	Present     map[string]string `json:"present"`
	Value       *string           `json:"value"`
	Invalidates map[string]bool   `json:"invalidates"`
}

type presenceStep struct {
	Op       presenceOp       `json:"op"`
	Expected presenceExpected `json:"expected"`
}

type presenceFixture struct {
	Config struct {
		TTL uint64 `json:"ttl"`
	} `json:"config"`
	Steps []presenceStep `json:"steps"`
}

func loadPresenceFixture(t *testing.T, name string) (presenceFixture, bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(presenceSpecDir(), name))
	if err != nil {
		return presenceFixture{}, false
	}
	var fx presenceFixture
	if err := json.Unmarshal(data, &fx); err != nil {
		t.Fatalf("%s: unmarshal fixture: %v", name, err)
	}
	return fx, true
}

// wantMap converts the fixture's string-keyed present map into uint64 keys.
func wantMap(t *testing.T, m map[string]string) map[uint64]string {
	t.Helper()
	out := map[uint64]string{}
	for k, v := range m {
		key, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			t.Fatalf("bad peer key %q: %v", k, err)
		}
		out[key] = v
	}
	return out
}

func TestPresenceConformance(t *testing.T) {
	t.Run("presence", func(t *testing.T) {
		fx, ok := loadPresenceFixture(t, "presence.json")
		if !ok {
			t.Skipf("lazily-spec fixture absent: %s", filepath.Join(presenceSpecDir(), "presence.json"))
		}
		ctx := NewContext()
		cell := NewPresenceCell[uint64, string](ctx, fx.Config.TTL)
		observed := NewFormulaCell(ctx, func(_ *Context) int { _ = cell.Present(); return 0 })
		observed.Get() // prime

		for i, step := range fx.Steps {
			op := step.Op
			switch op.Type {
			case "heartbeat":
				cell.Heartbeat(op.Peer, op.Value, op.Now)
			case "evict":
				cell.Evict(op.Peer, op.Now)
			case "tick":
				cell.Tick(op.Now)
			default:
				t.Fatalf("unknown op %q", op.Type)
			}

			want := wantMap(t, step.Expected.Present)
			if got := cell.Present(); !reflect.DeepEqual(got, want) {
				t.Fatalf("step %d %q present: got %v, want %v", i, op.Type, got, want)
			}

			_, warm := observed.Peek()
			observed.Get() // re-prime
			if inval := !warm; inval != step.Expected.Invalidates["present"] {
				t.Fatalf("step %d %q inval present: got %v, want %v", i, op.Type, inval, step.Expected.Invalidates["present"])
			}
		}
	})

	t.Run("awareness", func(t *testing.T) {
		fx, ok := loadPresenceFixture(t, "awareness.json")
		if !ok {
			t.Skipf("lazily-spec fixture absent: %s", filepath.Join(presenceSpecDir(), "awareness.json"))
		}
		ctx := NewContext()
		cell := NewAwarenessCell[uint64, string](ctx, fx.Config.TTL)
		observed := NewFormulaCell(ctx, func(_ *Context) int { _ = cell.Present(); return 0 })
		observed.Get() // prime

		for i, step := range fx.Steps {
			op := step.Op
			switch op.Type {
			case "set":
				cell.Set(op.Peer, op.Value, op.Now)
			case "tick":
				cell.Tick(op.Now)
			default:
				t.Fatalf("unknown op %q", op.Type)
			}

			want := wantMap(t, step.Expected.Present)
			if got := cell.Present(); !reflect.DeepEqual(got, want) {
				t.Fatalf("step %d %q present: got %v, want %v", i, op.Type, got, want)
			}

			_, warm := observed.Peek()
			observed.Get() // re-prime
			if inval := !warm; inval != step.Expected.Invalidates["present"] {
				t.Fatalf("step %d %q inval present: got %v, want %v", i, op.Type, inval, step.Expected.Invalidates["present"])
			}
		}
	})

	t.Run("ephemeral", func(t *testing.T) {
		fx, ok := loadPresenceFixture(t, "ephemeral.json")
		if !ok {
			t.Skipf("lazily-spec fixture absent: %s", filepath.Join(presenceSpecDir(), "ephemeral.json"))
		}
		ctx := NewContext()
		cell := NewEphemeralCell[string](ctx)
		vc := cell.ValueCell()
		observed := NewFormulaCell(ctx, func(_ *Context) Opt[string] { return vc.Get() })
		observed.Get() // prime

		for i, step := range fx.Steps {
			op := step.Op
			switch op.Type {
			case "set":
				cell.Set(op.Value, op.Now, op.TTL)
			case "tick":
				cell.Tick(op.Now)
			default:
				t.Fatalf("unknown op %q", op.Type)
			}

			gotV, gotPresent := cell.Value()
			if step.Expected.Value == nil {
				if gotPresent {
					t.Fatalf("step %d %q value: got %q, want null", i, op.Type, gotV)
				}
			} else {
				if !gotPresent || gotV != *step.Expected.Value {
					t.Fatalf("step %d %q value: got (%q,%v), want %q", i, op.Type, gotV, gotPresent, *step.Expected.Value)
				}
			}

			_, warm := observed.Peek()
			observed.Get() // re-prime
			if inval := !warm; inval != step.Expected.Invalidates["value"] {
				t.Fatalf("step %d %q inval value: got %v, want %v", i, op.Type, inval, step.Expected.Invalidates["value"])
			}
		}
	})
}
