package lazily

import (
	"sort"
	"testing"
)

const boundaryIngressFixture = "ingress/boundary_ingress_adapter.json"

func jsNumber(value any) float64 { return value.(float64) }

type boundaryDelivery struct {
	id      string
	targets map[string]struct{}
	acked   map[string]struct{}
}

type boundaryModel struct {
	maxBuffered     int
	freshnessWindow float64
	phase           string
	generation      float64
	cursor          *float64
	buffered        map[float64]map[string]any
	sourceKeys      map[string]struct{}
	members         map[string]struct{}
	validation      string
	replayFrom      *float64
	staleEvents     float64
	delivery        *boundaryDelivery
	lastStampedAt   *float64
	now             float64
	revision        float64
}

func newBoundaryModel(policy map[string]any) *boundaryModel {
	return &boundaryModel{
		maxBuffered:     int(jsNumber(policy["max_buffered"])),
		freshnessWindow: jsNumber(policy["freshness_horizon"]),
		phase:           "detached",
		buffered:        map[float64]map[string]any{},
		sourceKeys:      map[string]struct{}{},
		members:         map[string]struct{}{},
		validation:      "valid",
	}
}

func (m *boundaryModel) changed() { m.revision++ }

func (m *boundaryModel) applyPayload(event map[string]any) {
	switch event["action"] {
	case "upsert":
		m.sourceKeys[event["key"].(string)] = struct{}{}
	case "remove":
		delete(m.sourceKeys, event["key"].(string))
	case "validate":
		m.validation = event["validation"].(string)
	default:
		panic("unknown boundary event action")
	}
	cursor := jsNumber(event["cursor"])
	stampedAt := jsNumber(event["stamped_at"])
	m.cursor = &cursor
	m.lastStampedAt = &stampedAt
	if m.validation == "valid" {
		m.phase = "live"
	} else {
		m.phase = "invalid"
	}
	m.replayFrom = nil
}

func (m *boundaryModel) drain() {
	for m.cursor != nil {
		next := *m.cursor + 1
		event, ok := m.buffered[next]
		if !ok {
			break
		}
		delete(m.buffered, next)
		m.applyPayload(event)
	}
	if len(m.buffered) > 0 {
		m.phase = "replay_required"
		next := *m.cursor + 1
		m.replayFrom = &next
	}
}

func (m *boundaryModel) apply(op map[string]any) {
	switch op["type"] {
	case "subscribe":
		generation := jsNumber(op["generation"])
		if generation < m.generation {
			return
		}
		m.generation = generation
		m.cursor = nil
		m.buffered = map[float64]map[string]any{}
		m.sourceKeys = map[string]struct{}{}
		m.members = map[string]struct{}{}
		m.validation = "valid"
		m.replayFrom = nil
		m.phase = "bootstrapping"
		m.changed()
	case "snapshot":
		generation := jsNumber(op["generation"])
		if generation < m.generation {
			m.staleEvents++
			m.changed()
			return
		}
		if generation > m.generation {
			m.generation = generation
			m.buffered = map[float64]map[string]any{}
		}
		cursor := jsNumber(op["cursor"])
		stampedAt := jsNumber(op["stamped_at"])
		m.cursor = &cursor
		m.lastStampedAt = &stampedAt
		m.sourceKeys = stringsToSet(jsList(op["source_keys"]))
		m.members = stringsToSet(jsList(op["members"]))
		m.validation = op["validation"].(string)
		if m.validation == "valid" {
			m.phase = "live"
		} else {
			m.phase = "invalid"
		}
		m.replayFrom = nil
		for bufferedCursor := range m.buffered {
			if bufferedCursor <= cursor {
				delete(m.buffered, bufferedCursor)
			}
		}
		m.drain()
		m.changed()
	case "event":
		generation := jsNumber(op["generation"])
		eventCursor := jsNumber(op["cursor"])
		if generation < m.generation {
			m.staleEvents++
			m.changed()
			return
		}
		if generation > m.generation {
			m.generation = generation
			m.cursor = nil
			m.buffered = map[float64]map[string]any{}
			m.sourceKeys = map[string]struct{}{}
			m.members = map[string]struct{}{}
			m.phase = "bootstrapping"
			m.replayFrom = nil
		}
		if m.cursor == nil {
			if _, duplicate := m.buffered[eventCursor]; !duplicate && len(m.buffered) >= m.maxBuffered {
				m.phase = "backpressured"
				zero := float64(0)
				m.replayFrom = &zero
				m.changed()
				return
			}
			if _, duplicate := m.buffered[eventCursor]; !duplicate {
				m.buffered[eventCursor] = op
				m.changed()
			}
			return
		}
		if eventCursor <= *m.cursor {
			return
		}
		if _, duplicate := m.buffered[eventCursor]; duplicate {
			return
		}
		if eventCursor == *m.cursor+1 {
			m.applyPayload(op)
			m.drain()
			m.changed()
			return
		}
		if len(m.buffered) >= m.maxBuffered {
			m.phase = "backpressured"
			next := *m.cursor + 1
			m.replayFrom = &next
			m.changed()
			return
		}
		m.buffered[eventCursor] = op
		m.phase = "replay_required"
		next := *m.cursor + 1
		m.replayFrom = &next
		m.changed()
	case "member_join":
		member := op["member"].(string)
		if _, exists := m.members[member]; exists {
			return
		}
		m.members[member] = struct{}{}
		if m.delivery != nil && len(m.delivery.targets) == 0 {
			m.delivery.targets[member] = struct{}{}
		}
		m.changed()
	case "member_leave":
		member := op["member"].(string)
		if _, exists := m.members[member]; exists {
			delete(m.members, member)
			m.changed()
		}
	case "open_receipt":
		m.delivery = &boundaryDelivery{
			id:      op["receipt_id"].(string),
			targets: cloneSet(m.members),
			acked:   map[string]struct{}{},
		}
		m.changed()
	case "ack":
		if m.delivery == nil || m.delivery.id != op["receipt_id"].(string) {
			return
		}
		member := op["member"].(string)
		_, target := m.delivery.targets[member]
		_, duplicate := m.delivery.acked[member]
		if target && !duplicate {
			m.delivery.acked[member] = struct{}{}
			m.changed()
		}
	case "tick":
		before := m.fresh()
		m.now = jsNumber(op["now"])
		if m.fresh() != before {
			m.changed()
		}
	default:
		panic("unknown boundary ingress op")
	}
}

func (m *boundaryModel) fresh() bool {
	return m.lastStampedAt != nil && m.now-*m.lastStampedAt <= m.freshnessWindow
}

func (m *boundaryModel) projection() map[string]any {
	var delivery any
	if m.delivery != nil {
		converged := len(m.delivery.targets) > 0
		for target := range m.delivery.targets {
			if _, ok := m.delivery.acked[target]; !ok {
				converged = false
			}
		}
		delivery = map[string]any{
			"receipt_id": m.delivery.id,
			"targets":    sortedSet(m.delivery.targets),
			"acked":      sortedSet(m.delivery.acked),
			"converged":  converged,
		}
	}
	var cursor any
	if m.cursor != nil {
		cursor = *m.cursor
	}
	var replayFrom any
	if m.replayFrom != nil {
		replayFrom = *m.replayFrom
	}
	buffered := make([]any, 0, len(m.buffered))
	for value := range m.buffered {
		buffered = append(buffered, value)
	}
	sort.Slice(buffered, func(i, j int) bool { return buffered[i].(float64) < buffered[j].(float64) })
	return map[string]any{
		"phase":                m.phase,
		"generation":           m.generation,
		"cursor":               cursor,
		"buffered_cursors":     buffered,
		"source_keys":          sortedSet(m.sourceKeys),
		"members":              sortedSet(m.members),
		"validation":           m.validation,
		"replay_from":          replayFrom,
		"stale_events":         m.staleEvents,
		"delivery":             delivery,
		"ready":                m.phase == "live" && m.validation == "valid",
		"fresh":                m.fresh(),
		"observation_revision": m.revision,
		"revision":             m.revision,
	}
}

func stringsToSet(values []any) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		out[value.(string)] = struct{}{}
	}
	return out
}

func cloneSet(values map[string]struct{}) map[string]struct{} {
	out := map[string]struct{}{}
	for value := range values {
		out[value] = struct{}{}
	}
	return out
}

func sortedSet(values map[string]struct{}) []any {
	sorted := make([]string, 0, len(values))
	for value := range values {
		sorted = append(sorted, value)
	}
	sort.Strings(sorted)
	out := make([]any, len(sorted))
	for i, value := range sorted {
		out[i] = value
	}
	return out
}

func TestBoundaryIngressAdapterCanonicalContract(t *testing.T) {
	data := loadConformanceFixture(t, "ingress", "boundary_ingress_adapter.json")
	var fixture map[string]any
	mustStrictJSON(t, boundaryIngressFixture, data, &fixture)
	consumeFixtureKeys(t, boundaryIngressFixture, fixture,
		"schema_version", "model", "transport", "policy", "scenarios")
	excuseKeys(t, fixture, "contract metadata or replay input",
		"schema_version", "model", "transport", "policy", "scenarios")

	replayed := 0
	for _, view := range scenarioViews(boundaryIngressFixture, jsList(fixture["scenarios"])) {
		scenario := view.Map()
		policy := map[string]any{}
		for key, value := range jsMap(fixture["policy"]) {
			policy[key] = value
		}
		if raw, ok := scenario["policy"]; ok {
			for key, value := range jsMap(raw) {
				policy[key] = value
			}
		}
		model := newBoundaryModel(policy)
		for index, raw := range jsList(scenario["steps"]) {
			step := jsMap(raw)
			model.apply(jsMap(step["op"]))
			actual := model.projection()
			expected := jsMap(step["expected"])
			for key := range expected {
				assertKey(t, expected, key, actual[key])
			}
			replayed++
			_ = index
		}
	}
	if replayed == 0 {
		t.Fatal("canonical boundary-ingress fixture replayed zero steps")
	}
}
