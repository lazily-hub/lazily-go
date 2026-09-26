package lazily

// ConcurrencyScope classifies the strongest coordination boundary a capability
// itself provides. It does not infer database atomicity from thread safety or
// durability from an in-memory name.
type ConcurrencyScope string

const (
	ConcurrencyScopeSingleGoroutine         ConcurrencyScope = "single_goroutine"
	ConcurrencyScopeSingleProcessSerialized ConcurrencyScope = "single_process_serialized"
	ConcurrencyScopeDurableCrossProcess     ConcurrencyScope = "durable_cross_process"
	ConcurrencyScopeDistributedFenced       ConcurrencyScope = "distributed_fenced"
)

// Valid reports whether scope is one of the specification-defined values.
func (scope ConcurrencyScope) Valid() bool {
	switch scope {
	case ConcurrencyScopeSingleGoroutine, ConcurrencyScopeSingleProcessSerialized,
		ConcurrencyScopeDurableCrossProcess, ConcurrencyScopeDistributedFenced:
		return true
	default:
		return false
	}
}

// ConcurrencyScopedCapability exposes a capability's coordination boundary for
// conformance checks and dependency validation.
type ConcurrencyScopedCapability interface {
	ConcurrencyScope() ConcurrencyScope
}

func (*Context) ConcurrencyScope() ConcurrencyScope { return ConcurrencyScopeSingleGoroutine }

func (*ThreadSafeContext) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleProcessSerialized
}

func (*AsyncContext) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleProcessSerialized
}

func (*QueueCell[T, S]) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleGoroutine
}

func (*TopicCell[T]) ConcurrencyScope() ConcurrencyScope { return ConcurrencyScopeSingleGoroutine }

func (*WorkQueueCell[T]) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleGoroutine
}

func (*ThreadSafeQueueCell[T, S]) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleProcessSerialized
}

func (*ThreadSafeTopicCell[T]) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleProcessSerialized
}

func (*ThreadSafeWorkQueueCell[T]) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleProcessSerialized
}

func (*AsyncQueueCell[T, S]) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleProcessSerialized
}

func (*AsyncTopicCell[T]) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleProcessSerialized
}

func (*AsyncWorkQueueCell[T]) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleProcessSerialized
}

func (*LatestDurableProjectionCore[K, V]) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleGoroutine
}

func (*LatestDurableProjection[K, V]) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleGoroutine
}

func (*ThreadSafeLatestDurableProjection[K, V]) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleProcessSerialized
}

func (*AsyncLatestDurableProjection[K, V]) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeSingleProcessSerialized
}
