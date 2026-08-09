package lazily

import (
	"path/filepath"
	"reflect"
	"testing"
)

// Cross-language conformance for the embedded-service plane (#lzservice) — see
// lazily-spec/docs/service.md and lazily-spec/conformance/service/*.json. Mirrors
// lazily-rs tests/service_conformance.rs: for each fixture, replay the steps and
// after each op assert the returned value (returns), the projected reader value
// (expected.health/ready/discovery/projection), and reader invalidation
// (expected.invalidates.<reader>).
//
// When the sibling lazily-spec tree is unreachable the subtest is skipped so an
// absent spec tree cannot fail the build.

func serviceSpecDir() string {
	return specPath("service")
}

type serviceOp struct {
	Type     string `json:"type"`
	Name     string `json:"name"`
	Up       bool   `json:"up"`
	Critical bool   `json:"critical"`
	Ready    bool   `json:"ready"`
	Service  string `json:"service"`
	Endpoint string `json:"endpoint"`
	Peer     uint64 `json:"peer"`
}

type serviceExpected struct {
	Health      *string           `json:"health"`
	Ready       *bool             `json:"ready"`
	Discovery   map[string]string `json:"discovery"`
	Projection  map[string]string `json:"projection"`
	Invalidates map[string]bool   `json:"invalidates"`
}

type serviceStep struct {
	Op       serviceOp       `json:"op"`
	Returns  *string         `json:"returns"`
	Expected serviceExpected `json:"expected"`
}

type serviceFixture struct {
	conformanceMeta
	Steps []serviceStep `json:"steps"`
}

func loadServiceFixture(t *testing.T, name string) (serviceFixture, bool) {
	t.Helper()
	path := filepath.Join(serviceSpecDir(), name)
	data, err := specReadFile(path)
	if err != nil {
		return serviceFixture{}, false
	}
	var fx serviceFixture
	mustStrictJSON(t, name, data, &fx)
	return fx, true
}

func TestServiceConformance(t *testing.T) {
	t.Run("health.json", func(t *testing.T) {
		fx, ok := loadServiceFixture(t, "health.json")
		if !ok {
			t.Skipf("lazily-spec fixture absent: %s", filepath.Join(serviceSpecDir(), "health.json"))
		}
		ctx := NewContext()
		h := NewHealthCell(ctx)
		rc := h.HealthCell()
		observed := NewSlot(ctx, func(c *Compute) Health { return Get(c, rc) })
		observed.Get()

		for i, step := range fx.Steps {
			h.Set(step.Op.Name, step.Op.Up, step.Op.Critical)
			if step.Expected.Health == nil {
				t.Fatalf("step %d: missing expected.health", i)
			}
			if got := h.Health().String(); got != *step.Expected.Health {
				t.Fatalf("step %d: health got %q, want %q", i, got, *step.Expected.Health)
			}
			_, warm := observed.Peek()
			observed.Get()
			if got := !warm; got != step.Expected.Invalidates["health"] {
				t.Fatalf("step %d: invalidates.health got %v, want %v", i, got, step.Expected.Invalidates["health"])
			}
		}
	})

	t.Run("readiness.json", func(t *testing.T) {
		fx, ok := loadServiceFixture(t, "readiness.json")
		if !ok {
			t.Skipf("lazily-spec fixture absent: %s", filepath.Join(serviceSpecDir(), "readiness.json"))
		}
		ctx := NewContext()
		r := NewReadinessCell(ctx)
		rc := r.ReadyCell()
		observed := NewSlot(ctx, func(c *Compute) bool { return Get(c, rc) })
		observed.Get()

		for i, step := range fx.Steps {
			r.Set(step.Op.Name, step.Op.Ready)
			if step.Expected.Ready == nil {
				t.Fatalf("step %d: missing expected.ready", i)
			}
			if got := r.Ready(); got != *step.Expected.Ready {
				t.Fatalf("step %d: ready got %v, want %v", i, got, *step.Expected.Ready)
			}
			_, warm := observed.Peek()
			observed.Get()
			if got := !warm; got != step.Expected.Invalidates["ready"] {
				t.Fatalf("step %d: invalidates.ready got %v, want %v", i, got, step.Expected.Invalidates["ready"])
			}
		}
	})

	t.Run("discovery.json", func(t *testing.T) {
		fx, ok := loadServiceFixture(t, "discovery.json")
		if !ok {
			t.Skipf("lazily-spec fixture absent: %s", filepath.Join(serviceSpecDir(), "discovery.json"))
		}
		ctx := NewContext()
		d := NewDiscoveryCell[uint64](ctx)
		observed := NewSlot(ctx, func(c *Compute) int { Get(c, d.DiscoveryCell()); return 0 })
		observed.Get()

		for i, step := range fx.Steps {
			switch step.Op.Type {
			case "register":
				d.Register(step.Op.Service, step.Op.Endpoint, step.Op.Peer)
			case "deregister":
				d.Deregister(step.Op.Service)
			case "evict":
				d.Evict(step.Op.Peer)
			case "resolve":
				got, ok := d.Resolve(step.Op.Service)
				var gotPtr *string
				if ok {
					gotPtr = &got
				}
				if !eqStrPtr(gotPtr, step.Returns) {
					t.Fatalf("step %d: resolve returns got %v, want %v", i, ptrStr(gotPtr), ptrStr(step.Returns))
				}
			default:
				t.Fatalf("step %d: unknown op %q", i, step.Op.Type)
			}
			want := step.Expected.Discovery
			if want == nil {
				want = map[string]string{}
			}
			if got := d.Discovery(); !reflect.DeepEqual(got, want) {
				t.Fatalf("step %d: discovery got %v, want %v", i, got, want)
			}
			_, warm := observed.Peek()
			observed.Get()
			if got := !warm; got != step.Expected.Invalidates["discovery"] {
				t.Fatalf("step %d: invalidates.discovery got %v, want %v", i, got, step.Expected.Invalidates["discovery"])
			}
		}
	})

	t.Run("service_registry.json", func(t *testing.T) {
		fx, ok := loadServiceFixture(t, "service_registry.json")
		if !ok {
			t.Skipf("lazily-spec fixture absent: %s", filepath.Join(serviceSpecDir(), "service_registry.json"))
		}
		ctx := NewContext()
		reg := NewServiceRegistry(ctx)
		observed := NewSlot(ctx, func(c *Compute) int { Get(c, reg.ProjectionCell()); return 0 })
		observed.Get()

		for i, step := range fx.Steps {
			switch step.Op.Type {
			case "register":
				reg.Register(step.Op.Service, step.Op.Endpoint)
			case "deregister":
				reg.Deregister(step.Op.Service)
			case "replay":
				reg.Replay()
			default:
				t.Fatalf("step %d: unknown op %q", i, step.Op.Type)
			}
			want := step.Expected.Projection
			if want == nil {
				want = map[string]string{}
			}
			if got := reg.Projection(); !reflect.DeepEqual(got, want) {
				t.Fatalf("step %d: projection got %v, want %v", i, got, want)
			}
			_, warm := observed.Peek()
			observed.Get()
			if got := !warm; got != step.Expected.Invalidates["projection"] {
				t.Fatalf("step %d: invalidates.projection got %v, want %v", i, got, step.Expected.Invalidates["projection"])
			}
		}
	})
}

func eqStrPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func ptrStr(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}
