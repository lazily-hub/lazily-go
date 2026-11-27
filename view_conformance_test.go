package lazily

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestViewConformance(t *testing.T) {
	dir := specPath("view")
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		t.Skipf("conformance/view is absent at %s", dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, entry := range entries {
		if name := entry.Name(); len(name) > 5 && name[len(name)-5:] == ".json" {
			found = append(found, name)
		}
	}
	sort.Strings(found)
	want := []string{"batch_send.json", "counter.json", "field_change.json", "malformed_rejected.json"}
	if !reflect.DeepEqual(found, want) {
		t.Fatalf("view fixture set = %v, want %v", found, want)
	}
	for _, name := range []string{"counter.json", "field_change.json", "batch_send.json"} {
		t.Run(name, func(t *testing.T) {
			replayViewFixture(t, name)
		})
	}
	t.Run("malformed_rejected.json", func(t *testing.T) {
		doc := loadViewFixture(t, "malformed_rejected.json")
		cases, _ := doc["cases"].([]any)
		if len(cases) == 0 {
			t.Fatal("malformed corpus must contain cases")
		}
		for _, raw := range cases {
			scenario := raw.(map[string]any)
			name, _ := scenario["name"].(string)
			_, err := InstallView(NewContext(), scenario["view"])
			if err == nil {
				t.Fatalf("%s must be rejected", name)
			}
		}
	})
}

func loadViewFixture(t *testing.T, name string) map[string]any {
	t.Helper()
	data, err := specReadFile(filepath.Join(specPath("view"), name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return doc
}

func replayViewFixture(t *testing.T, name string) {
	t.Helper()
	doc := loadViewFixture(t, name)
	if doc["kind"] != "View" {
		t.Fatalf("%s kind = %v", name, doc["kind"])
	}
	view, err := InstallView(NewContext(), doc["view"])
	if err != nil {
		t.Fatalf("%s install: %v", name, err)
	}
	assertViewJSON(t, name+" signals", view.SignalsJSON(), doc["signals"])
	assertViewJSON(t, name+" expressions", view.ExpressionsJSON(), doc["expressions"])
	assertViewJSON(t, name+" layout", view.LayoutJSON(), doc["layout"])
	assertViewJSON(t, name+" trace", doc["trace"], []any{})
	assertViewJSON(t, name+" chart", view.ChartJSON(), doc["chart"])
	steps, _ := doc["steps"].([]any)
	for i, raw := range steps {
		step := raw.(map[string]any)
		node, _ := step["node"].(string)
		event, _ := step["event"].(string)
		trace, err := view.DispatchJSON(node, event, step["value"])
		if err != nil {
			t.Fatalf("%s step %d: %v", name, i, err)
		}
		label := name + " step"
		assertViewJSON(t, label+" signals", view.SignalsJSON(), step["signals"])
		assertViewJSON(t, label+" expressions", view.ExpressionsJSON(), step["expressions"])
		assertViewJSON(t, label+" trace", trace, step["trace"])
		assertViewJSON(t, label+" chart", view.ChartJSON(), step["chart"])
	}
}

func assertViewJSON(t *testing.T, label string, got, want any) {
	t.Helper()
	gb, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("%s marshal got: %v", label, err)
	}
	wb, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("%s marshal want: %v", label, err)
	}
	var g, w any
	if err := json.Unmarshal(gb, &g); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(wb, &w); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("%s:\n got %s\nwant %s", label, gb, wb)
	}
}
