package lazily

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"
)

var (
	// ErrPostgresProjectionBarrierConfig reports a missing database, lock key,
	// callback, or an invalid timeout/retry option.
	ErrPostgresProjectionBarrierConfig = errors.New("invalid Postgres projection barrier configuration")
	// ErrPostgresProjectionBarrierTimeout reports PostgreSQL SQLSTATE 55P03
	// while acquiring a projection barrier lock.
	ErrPostgresProjectionBarrierTimeout = errors.New("Postgres projection barrier lock timeout")
	// ErrPostgresProjectionBarrierRetryExhausted reports that every complete
	// SERIALIZABLE transaction attempt ended with SQLSTATE 40001 or 40P01.
	ErrPostgresProjectionBarrierRetryExhausted = errors.New("Postgres projection barrier retry exhausted")
)

const (
	defaultPostgresProjectionLockTimeout = 5 * time.Second
	defaultPostgresProjectionMaxAttempts = 4
)

// DatabaseProjectionMaintenanceBarrier marks cross-process exclusion owned by
// the durable database. In-process Context, ThreadSafeContext, AsyncContext,
// and LatestDurableProjection values intentionally do not implement it.
type DatabaseProjectionMaintenanceBarrier interface {
	ConcurrencyScopedCapability
	DatabaseProjectionMaintenanceBarrier()
}

// PostgresProjectionLock names one cooperative transaction-scoped advisory
// lock. Callers must allocate collision-free namespace and key values.
type PostgresProjectionLock struct {
	Namespace int32
	Key       int32
}

// PostgresProjectionBarrierOptions bounds lock waiting and complete
// SERIALIZABLE transaction retries. Zero fields select documented defaults.
type PostgresProjectionBarrierOptions struct {
	LockTimeout time.Duration
	MaxAttempts int
}

// PostgresProjectionBarrier serializes projection source writes and full
// rebuilds across every process connected to the same PostgreSQL database.
//
// Every participant must use the same advisory keys. Callbacks can run more
// than once and therefore must keep all non-idempotent work inside the supplied
// transaction; external effects do not belong in a retryable callback.
type PostgresProjectionBarrier struct {
	db          *sql.DB
	lockTimeout time.Duration
	maxAttempts int
}

var _ DatabaseProjectionMaintenanceBarrier = (*PostgresProjectionBarrier)(nil)

func NewPostgresProjectionBarrier(db *sql.DB, options PostgresProjectionBarrierOptions) (*PostgresProjectionBarrier, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrPostgresProjectionBarrierConfig)
	}
	if options.LockTimeout < 0 {
		return nil, fmt.Errorf("%w: lock timeout is negative", ErrPostgresProjectionBarrierConfig)
	}
	if options.MaxAttempts < 0 {
		return nil, fmt.Errorf("%w: max attempts is negative", ErrPostgresProjectionBarrierConfig)
	}
	if options.LockTimeout == 0 {
		options.LockTimeout = defaultPostgresProjectionLockTimeout
	}
	if options.MaxAttempts == 0 {
		options.MaxAttempts = defaultPostgresProjectionMaxAttempts
	}
	return &PostgresProjectionBarrier{
		db:          db,
		lockTimeout: options.LockTimeout,
		maxAttempts: options.MaxAttempts,
	}, nil
}

func (*PostgresProjectionBarrier) DatabaseProjectionMaintenanceBarrier() {}

func (*PostgresProjectionBarrier) ConcurrencyScope() ConcurrencyScope {
	return ConcurrencyScopeDurableCrossProcess
}

// ApplySourceWrite runs one accepted source mutation and its incremental
// projection update behind the same database-owned barrier used by rebuilds.
// The callback must persist both sides before returning nil.
func (barrier *PostgresProjectionBarrier) ApplySourceWrite(
	ctx context.Context,
	locks []PostgresProjectionLock,
	apply func(context.Context, *sql.Tx) error,
) error {
	if barrier == nil || apply == nil {
		return fmt.Errorf("%w: barrier and source-write callback are required", ErrPostgresProjectionBarrierConfig)
	}
	return barrier.run(ctx, locks, apply)
}

// MaintainPostgresProjection performs scan and replacement in one transaction
// behind the same ordered barrier used by source writers. A serialization or
// deadlock retry reruns both callbacks from the beginning.
func MaintainPostgresProjection[T any](
	ctx context.Context,
	barrier *PostgresProjectionBarrier,
	locks []PostgresProjectionLock,
	scan func(context.Context, *sql.Tx) (T, error),
	replace func(context.Context, *sql.Tx, T) error,
) error {
	if barrier == nil || scan == nil || replace == nil {
		return fmt.Errorf("%w: barrier, scan, and replace are required", ErrPostgresProjectionBarrierConfig)
	}
	return barrier.run(ctx, locks, func(ctx context.Context, tx *sql.Tx) error {
		replacement, err := scan(ctx, tx)
		if err != nil {
			return err
		}
		return replace(ctx, tx, replacement)
	})
}

func (barrier *PostgresProjectionBarrier) run(
	ctx context.Context,
	locks []PostgresProjectionLock,
	operation func(context.Context, *sql.Tx) error,
) error {
	ordered, err := orderedPostgresProjectionLocks(locks)
	if err != nil {
		return err
	}
	var lastRetry error
	for attempt := 1; attempt <= barrier.maxAttempts; attempt++ {
		err := barrier.runOnce(ctx, ordered, operation)
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		switch postgresSQLState(err) {
		case "55P03":
			return fmt.Errorf("%w: %v", ErrPostgresProjectionBarrierTimeout, err)
		case "40001", "40P01":
			lastRetry = err
			continue
		default:
			return err
		}
	}
	return fmt.Errorf("%w after %d attempts: %v", ErrPostgresProjectionBarrierRetryExhausted, barrier.maxAttempts, lastRetry)
}

func (barrier *PostgresProjectionBarrier) runOnce(
	ctx context.Context,
	locks []PostgresProjectionLock,
	operation func(context.Context, *sql.Tx) error,
) error {
	tx, err := barrier.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(
		ctx,
		"SELECT set_config('lock_timeout', $1, true)",
		postgresProjectionTimeoutSetting(barrier.lockTimeout),
	); err != nil {
		return err
	}
	for _, lock := range locks {
		if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1, $2)", lock.Namespace, lock.Key); err != nil {
			return err
		}
	}
	if err := operation(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func orderedPostgresProjectionLocks(locks []PostgresProjectionLock) ([]PostgresProjectionLock, error) {
	if len(locks) == 0 {
		return nil, fmt.Errorf("%w: at least one advisory lock is required", ErrPostgresProjectionBarrierConfig)
	}
	ordered := append([]PostgresProjectionLock(nil), locks...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Namespace != ordered[j].Namespace {
			return ordered[i].Namespace < ordered[j].Namespace
		}
		return ordered[i].Key < ordered[j].Key
	})
	deduplicated := ordered[:0]
	for _, lock := range ordered {
		if len(deduplicated) == 0 || deduplicated[len(deduplicated)-1] != lock {
			deduplicated = append(deduplicated, lock)
		}
	}
	return deduplicated, nil
}

func postgresProjectionTimeoutSetting(timeout time.Duration) string {
	milliseconds := timeout / time.Millisecond
	if timeout%time.Millisecond != 0 {
		milliseconds++
	}
	return fmt.Sprintf("%dms", milliseconds)
}

type postgresSQLStateError interface {
	SQLState() string
}

func postgresSQLState(err error) string {
	var state postgresSQLStateError
	if errors.As(err, &state) {
		return state.SQLState()
	}
	return ""
}
