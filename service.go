package lazily

// Embedded-service plane (#lzservice): HealthCell / ReadinessCell /
// DiscoveryCell / ServiceRegistry, each a pure compute core (an aggregation or
// keyed map) split from a reactive cell projecting the composed view. Ported
// from lazily-rs src/service.rs; see lazily-spec/docs/service.md and the formal
// model lazily-formal/LazilyFormal/Service.lean.
//
// Scalar readers (health, ready) project onto a comparable Cell, so the Cell's
// != guard gives correct "invalidate only on change" for free. Collection
// readers (discovery, projection) cannot live in a Cell (maps are not
// comparable), so they use the internal version-cell pattern: recompute the
// projection after each op and bump a *Cell[uint64] only when the projected map
// structurally changes.

// ===========================================================================
// Health
// ===========================================================================

// Health is the composed health status (worst component dominates).
type Health int

const (
	Healthy Health = iota
	Degraded
	Unhealthy
)

func (h Health) String() string {
	switch h {
	case Healthy:
		return "Healthy"
	case Degraded:
		return "Degraded"
	case Unhealthy:
		return "Unhealthy"
	default:
		return "Unknown"
	}
}

// probe records a single liveness probe: whether it is up and whether it is
// critical.
type probe struct {
	up       bool
	critical bool
}

// HealthCore is the composed liveness-probe core. Each probe reports up and
// whether it is critical.
type HealthCore struct {
	probes map[string]probe
}

// NewHealthCore creates an empty health core.
func NewHealthCore() *HealthCore {
	return &HealthCore{probes: map[string]probe{}}
}

// Set sets or refreshes a probe.
func (c *HealthCore) Set(name string, up bool, critical bool) {
	c.probes[name] = probe{up: up, critical: critical}
}

// Health is the aggregate: Unhealthy if any critical probe is down, else
// Degraded if any is down, else Healthy.
func (c *HealthCore) Health() Health {
	anyDown := false
	for _, p := range c.probes {
		if !p.up {
			anyDown = true
			if p.critical {
				return Unhealthy
			}
		}
	}
	if anyDown {
		return Degraded
	}
	return Healthy
}

// HealthCell is the reactive health projection onto a Cell for /health.
type HealthCell struct {
	core   *HealthCore
	health *Cell[Health]
}

// NewHealthCell creates a reactive health cell bound to ctx.
func NewHealthCell(ctx *Context) *HealthCell {
	return &HealthCell{
		core:   NewHealthCore(),
		health: NewCell(ctx, Healthy),
	}
}

func (h *HealthCell) refresh() {
	h.health.Set(h.core.Health())
}

// Set sets or refreshes a probe and refreshes the projection.
func (h *HealthCell) Set(name string, up bool, critical bool) {
	h.core.Set(name, up, critical)
	h.refresh()
}

// Health returns the current aggregate health.
func (h *HealthCell) Health() Health {
	return h.core.Health()
}

// HealthCell returns the underlying reactive cell for /health.
func (h *HealthCell) HealthCell() *Cell[Health] {
	return h.health
}

// ===========================================================================
// Readiness
// ===========================================================================

// ReadinessCore is the composed readiness-probe core: ready iff every condition
// holds.
type ReadinessCore struct {
	conditions map[string]bool
}

// NewReadinessCore creates an empty readiness core.
func NewReadinessCore() *ReadinessCore {
	return &ReadinessCore{conditions: map[string]bool{}}
}

// Set sets or refreshes a condition.
func (c *ReadinessCore) Set(name string, ready bool) {
	c.conditions[name] = ready
}

// Ready reports whether every condition is ready.
func (c *ReadinessCore) Ready() bool {
	for _, r := range c.conditions {
		if !r {
			return false
		}
	}
	return true
}

// ReadinessCell is the reactive readiness projection onto a Cell for /ready.
type ReadinessCell struct {
	core  *ReadinessCore
	ready *Cell[bool]
}

// NewReadinessCell creates a reactive readiness cell bound to ctx.
func NewReadinessCell(ctx *Context) *ReadinessCell {
	return &ReadinessCell{
		core:  NewReadinessCore(),
		ready: NewCell(ctx, true),
	}
}

func (r *ReadinessCell) refresh() {
	r.ready.Set(r.core.Ready())
}

// Set sets or refreshes a condition and refreshes the projection.
func (r *ReadinessCell) Set(name string, ready bool) {
	r.core.Set(name, ready)
	r.refresh()
}

// Ready reports whether the service is ready.
func (r *ReadinessCell) Ready() bool {
	return r.core.Ready()
}

// ReadyCell returns the underlying reactive cell for /ready.
func (r *ReadinessCell) ReadyCell() *Cell[bool] {
	return r.ready
}

// ===========================================================================
// Discovery
// ===========================================================================

// discoveryEntry records a discovered endpoint and its owning peer.
type discoveryEntry[P comparable] struct {
	endpoint string
	owner    P
}

// DiscoveryCore is the service-discovery core: service -> (endpoint, owner). A
// peer's departure (Evict) removes its endpoints.
type DiscoveryCore[P comparable] struct {
	entries map[string]discoveryEntry[P]
}

// NewDiscoveryCore creates an empty discovery core.
func NewDiscoveryCore[P comparable]() *DiscoveryCore[P] {
	return &DiscoveryCore[P]{entries: map[string]discoveryEntry[P]{}}
}

// Register records a service endpoint owned by peer.
func (c *DiscoveryCore[P]) Register(service string, endpoint string, peer P) {
	c.entries[service] = discoveryEntry[P]{endpoint: endpoint, owner: peer}
}

// Deregister removes a service.
func (c *DiscoveryCore[P]) Deregister(service string) {
	delete(c.entries, service)
}

// Evict removes all endpoints owned by peer (membership loss).
func (c *DiscoveryCore[P]) Evict(peer P) {
	for service, e := range c.entries {
		if e.owner == peer {
			delete(c.entries, service)
		}
	}
}

// Resolve returns the endpoint for a service, if present.
func (c *DiscoveryCore[P]) Resolve(service string) (string, bool) {
	e, ok := c.entries[service]
	if !ok {
		return "", false
	}
	return e.endpoint, true
}

// Discovery returns the live service -> endpoint map.
func (c *DiscoveryCore[P]) Discovery() map[string]string {
	out := make(map[string]string, len(c.entries))
	for s, e := range c.entries {
		out[s] = e.endpoint
	}
	return out
}

// DiscoveryCell is the reactive service discovery. The discovery map is a
// collection reader, so it uses the version-cell pattern: bump version only when
// the projected map structurally changes.
type DiscoveryCell[P comparable] struct {
	core    *DiscoveryCore[P]
	version *Cell[uint64]
	last    map[string]string
}

// NewDiscoveryCell creates a reactive discovery cell bound to ctx.
func NewDiscoveryCell[P comparable](ctx *Context) *DiscoveryCell[P] {
	return &DiscoveryCell[P]{
		core:    NewDiscoveryCore[P](),
		version: NewCell[uint64](ctx, 0),
		last:    map[string]string{},
	}
}

func (d *DiscoveryCell[P]) refresh() {
	next := d.core.Discovery()
	if !sameStringMap(d.last, next) {
		d.last = next
		d.version.Set(d.version.Peek() + 1)
	}
}

// Register records a service endpoint owned by peer and refreshes the map.
func (d *DiscoveryCell[P]) Register(service string, endpoint string, peer P) {
	d.core.Register(service, endpoint, peer)
	d.refresh()
}

// Deregister removes a service and refreshes the map.
func (d *DiscoveryCell[P]) Deregister(service string) {
	d.core.Deregister(service)
	d.refresh()
}

// Evict removes all endpoints owned by peer and refreshes the map.
func (d *DiscoveryCell[P]) Evict(peer P) {
	d.core.Evict(peer)
	d.refresh()
}

// Resolve returns the endpoint for a service without changing the map.
func (d *DiscoveryCell[P]) Resolve(service string) (string, bool) {
	return d.core.Resolve(service)
}

// Discovery returns the live service -> endpoint map, subscribing the reader to
// the version cell.
func (d *DiscoveryCell[P]) Discovery() map[string]string {
	_ = d.version.Get()
	return d.core.Discovery()
}

// DiscoveryCell returns the underlying version cell (the reactive handle).
func (d *DiscoveryCell[P]) DiscoveryCell() *Cell[uint64] {
	return d.version
}

// ===========================================================================
// Service registry (durable)
// ===========================================================================

// registryOpKind discriminates a durable registry log entry.
type registryOpKind int

const (
	opRegister registryOpKind = iota
	opDeregister
)

// registryOp is a durable registry op (the ordered log entry).
type registryOp struct {
	kind     registryOpKind
	service  string
	endpoint string
}

// ServiceRegistryCore is the durable service-registry core: an ordered log (the
// DurableOutbox pattern) whose left-fold is the projection, so replay
// reconstructs it.
type ServiceRegistryCore struct {
	log        []registryOp
	projection map[string]string
}

// NewServiceRegistryCore creates an empty durable registry core.
func NewServiceRegistryCore() *ServiceRegistryCore {
	return &ServiceRegistryCore{
		log:        nil,
		projection: map[string]string{},
	}
}

func applyRegistryOp(projection map[string]string, op registryOp) {
	switch op.kind {
	case opRegister:
		projection[op.service] = op.endpoint
	case opDeregister:
		delete(projection, op.service)
	}
}

// Register appends a register op to the log and updates the projection.
func (c *ServiceRegistryCore) Register(service string, endpoint string) {
	op := registryOp{kind: opRegister, service: service, endpoint: endpoint}
	applyRegistryOp(c.projection, op)
	c.log = append(c.log, op)
}

// Deregister appends a deregister op to the log and updates the projection.
func (c *ServiceRegistryCore) Deregister(service string) {
	op := registryOp{kind: opDeregister, service: service}
	applyRegistryOp(c.projection, op)
	c.log = append(c.log, op)
}

// Replay rebuilds the projection from the durable log (restart / crash-replay).
func (c *ServiceRegistryCore) Replay() {
	projection := map[string]string{}
	for _, op := range c.log {
		applyRegistryOp(projection, op)
	}
	c.projection = projection
}

// Projection returns a snapshot of the current projection.
func (c *ServiceRegistryCore) Projection() map[string]string {
	out := make(map[string]string, len(c.projection))
	for k, v := range c.projection {
		out[k] = v
	}
	return out
}

// ServiceRegistry is the reactive durable service registry. The projection is a
// collection reader, so it uses the version-cell pattern.
type ServiceRegistry struct {
	core    *ServiceRegistryCore
	version *Cell[uint64]
	last    map[string]string
}

// NewServiceRegistry creates a reactive durable registry bound to ctx.
func NewServiceRegistry(ctx *Context) *ServiceRegistry {
	return &ServiceRegistry{
		core:    NewServiceRegistryCore(),
		version: NewCell[uint64](ctx, 0),
		last:    map[string]string{},
	}
}

func (r *ServiceRegistry) refresh() {
	next := r.core.Projection()
	if !sameStringMap(r.last, next) {
		r.last = next
		r.version.Set(r.version.Peek() + 1)
	}
}

// Register appends a register op and refreshes the projection.
func (r *ServiceRegistry) Register(service string, endpoint string) {
	r.core.Register(service, endpoint)
	r.refresh()
}

// Deregister appends a deregister op and refreshes the projection.
func (r *ServiceRegistry) Deregister(service string) {
	r.core.Deregister(service)
	r.refresh()
}

// Replay rebuilds the projection from the durable log and refreshes.
func (r *ServiceRegistry) Replay() {
	r.core.Replay()
	r.refresh()
}

// Projection returns the current projection, subscribing the reader to the
// version cell.
func (r *ServiceRegistry) Projection() map[string]string {
	_ = r.version.Get()
	return r.core.Projection()
}

// ProjectionCell returns the underlying version cell (the reactive handle).
func (r *ServiceRegistry) ProjectionCell() *Cell[uint64] {
	return r.version
}

// sameStringMap reports whether two string maps are structurally equal.
func sameStringMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || va != vb {
			return false
		}
	}
	return true
}
