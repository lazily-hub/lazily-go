package lazily

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"os"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

var simProjectionLocks = []PostgresProjectionLock{{Namespace: 1733, Key: 2}, {Namespace: 1733, Key: 1}}

var errSimProjectionCrashBeforeCommit = errors.New("synthetic crash before commit")

type postgresProjectionCorpus struct {
	url           string
	db            *sql.DB
	peer          *sql.DB
	barrier       *PostgresProjectionBarrier
	peerBarrier   *PostgresProjectionBarrier
	retryAttempts atomic.Int32
}

func openProjectionCorpusDB(t *testing.T, url string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(t.Context()); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db
}

func newPostgresProjectionCorpus(t *testing.T) *postgresProjectionCorpus {
	t.Helper()
	url := os.Getenv("LAZILY_POSTGRES_URL")
	if url == "" {
		t.Skip("LAZILY_POSTGRES_URL is required for PostgreSQL integration tests")
	}
	corpus := &postgresProjectionCorpus{url: url}
	corpus.db = openProjectionCorpusDB(t, url)
	corpus.peer = openProjectionCorpusDB(t, url)
	corpus.rebuildBarriers(t)
	t.Cleanup(func() {
		corpus.db.Close()
		corpus.peer.Close()
	})
	return corpus
}

func (corpus *postgresProjectionCorpus) rebuildBarriers(t *testing.T) {
	t.Helper()
	var err error
	corpus.barrier, err = NewPostgresProjectionBarrier(corpus.db, PostgresProjectionBarrierOptions{LockTimeout: 2 * time.Second, MaxAttempts: 4})
	if err != nil {
		t.Fatal(err)
	}
	corpus.peerBarrier, err = NewPostgresProjectionBarrier(corpus.peer, PostgresProjectionBarrierOptions{LockTimeout: 2 * time.Second, MaxAttempts: 4})
	if err != nil {
		t.Fatal(err)
	}
}

func (corpus *postgresProjectionCorpus) reset(ctx context.Context) error {
	corpus.retryAttempts.Store(0)
	_, err := corpus.db.ExecContext(ctx, `
		DROP FUNCTION IF EXISTS lazily_sim_projection_fail_once() CASCADE;
		DROP SEQUENCE IF EXISTS lazily_sim_projection_retry_once;
		DROP TABLE IF EXISTS lazily_sim_projection_view;
		DROP TABLE IF EXISTS lazily_sim_projection_event;
		CREATE TABLE lazily_sim_projection_event (
			ordinal BIGSERIAL PRIMARY KEY,
			event_id TEXT NOT NULL UNIQUE,
			event_kind TEXT NOT NULL,
			item_key TEXT NOT NULL,
			item_value TEXT NOT NULL
		);
		CREATE TABLE lazily_sim_projection_view (
			item_key TEXT PRIMARY KEY,
			item_value TEXT NOT NULL
		);
	`)
	return err
}

func scanMaterializedProjectionHistory(ctx context.Context, queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) ([]MaterializedProjectionEvent, error) {
	rows, err := queryer.QueryContext(ctx, `
		SELECT event_id, event_kind, item_key, item_value
		FROM lazily_sim_projection_event ORDER BY ordinal
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var history []MaterializedProjectionEvent
	for rows.Next() {
		var event MaterializedProjectionEvent
		var kind string
		if err := rows.Scan(&event.ID, &kind, &event.Key, &event.Value); err != nil {
			return nil, err
		}
		event.Kind = MaterializedProjectionEventKind(kind)
		history = append(history, event)
	}
	return history, rows.Err()
}

func replaceMaterializedProjection(ctx context.Context, tx *sql.Tx, projection map[string]string) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM lazily_sim_projection_view"); err != nil {
		return err
	}
	keys := make([]string, 0, len(projection))
	for key := range projection {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO lazily_sim_projection_view (item_key, item_value) VALUES ($1, $2)",
			key, projection[key],
		); err != nil {
			return err
		}
	}
	return nil
}

func scanAndReduceMaterializedProjection(ctx context.Context, tx *sql.Tx) (map[string]string, error) {
	history, err := scanMaterializedProjectionHistory(ctx, tx)
	if err != nil {
		return nil, err
	}
	return ReduceMaterializedProjectionHistory(history)
}

func appendMaterializedProjectionEvent(ctx context.Context, tx *sql.Tx, event MaterializedProjectionEvent) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO lazily_sim_projection_event (event_id, event_kind, item_key, item_value)
		VALUES ($1, $2, $3, $4)
	`, event.ID, event.Kind, event.Key, event.Value); err != nil {
		return err
	}
	projection, err := scanAndReduceMaterializedProjection(ctx, tx)
	if err != nil {
		return err
	}
	return replaceMaterializedProjection(ctx, tx, projection)
}

func (corpus *postgresProjectionCorpus) applyEvent(ctx context.Context, barrier *PostgresProjectionBarrier, event MaterializedProjectionEvent, attempts *atomic.Int32) error {
	return barrier.ApplySourceWrite(ctx, simProjectionLocks, func(ctx context.Context, tx *sql.Tx) error {
		if attempts != nil {
			attempts.Add(1)
		}
		return appendMaterializedProjectionEvent(ctx, tx, event)
	})
}

func (corpus *postgresProjectionCorpus) rebuild(ctx context.Context, barrier *PostgresProjectionBarrier) error {
	return MaintainPostgresProjection(ctx, barrier, simProjectionLocks, scanAndReduceMaterializedProjection, replaceMaterializedProjection)
}

func (corpus *postgresProjectionCorpus) holdBarrier(ctx context.Context) (<-chan struct{}, chan<- struct{}, <-chan error) {
	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- corpus.peerBarrier.ApplySourceWrite(ctx, simProjectionLocks, func(ctx context.Context, _ *sql.Tx) error {
			close(locked)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	return locked, release, done
}

func (corpus *postgresProjectionCorpus) applyConcurrentRebuild(ctx context.Context, event MaterializedProjectionEvent) error {
	scanned := make(chan struct{})
	release := make(chan struct{})
	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- MaintainPostgresProjection(ctx, corpus.barrier, simProjectionLocks,
			func(ctx context.Context, tx *sql.Tx) (map[string]string, error) {
				projection, err := scanAndReduceMaterializedProjection(ctx, tx)
				if err != nil {
					return nil, err
				}
				close(scanned)
				select {
				case <-release:
					return projection, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}, replaceMaterializedProjection)
	}()
	select {
	case <-scanned:
	case err := <-rebuildDone:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
	writeDone := make(chan error, 1)
	go func() { writeDone <- corpus.applyEvent(ctx, corpus.peerBarrier, event, nil) }()
	select {
	case <-writeDone:
		close(release)
		<-rebuildDone
		return errors.New("source write crossed a held projection rebuild barrier")
	case <-time.After(40 * time.Millisecond):
	case <-ctx.Done():
		close(release)
		return ctx.Err()
	}
	close(release)
	if err := <-rebuildDone; err != nil {
		return err
	}
	return <-writeDone
}

func (corpus *postgresProjectionCorpus) applyRejectedWait(ctx context.Context, action SimProjectionMaintenanceAction) error {
	locked, release, holderDone := corpus.holdBarrier(ctx)
	select {
	case <-locked:
	case err := <-holderDone:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		close(release)
		<-holderDone
	}()
	if action.Kind == SimProjectionMaintenanceLockTimeout {
		barrier, err := NewPostgresProjectionBarrier(corpus.db, PostgresProjectionBarrierOptions{LockTimeout: 35 * time.Millisecond, MaxAttempts: 1})
		if err != nil {
			return err
		}
		err = corpus.applyEvent(ctx, barrier, action.Event, nil)
		if !errors.Is(err, ErrPostgresProjectionBarrierTimeout) {
			return errors.New("bounded lock wait did not return the timeout sentinel")
		}
		return nil
	}
	cancelCtx, cancel := context.WithTimeout(ctx, 35*time.Millisecond)
	defer cancel()
	err := corpus.applyEvent(cancelCtx, corpus.barrier, action.Event, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		return errors.New("cancelled lock wait did not return context deadline exceeded")
	}
	return nil
}

func (corpus *postgresProjectionCorpus) installRetryTrigger(ctx context.Context) error {
	_, err := corpus.db.ExecContext(ctx, `
		DROP FUNCTION IF EXISTS lazily_sim_projection_fail_once() CASCADE;
		DROP SEQUENCE IF EXISTS lazily_sim_projection_retry_once;
		CREATE SEQUENCE lazily_sim_projection_retry_once START 1;
		CREATE FUNCTION lazily_sim_projection_fail_once() RETURNS trigger AS $$
		BEGIN
			IF nextval('lazily_sim_projection_retry_once') = 1 THEN
				RAISE EXCEPTION 'synthetic serialization retry' USING ERRCODE = '40001';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER lazily_sim_projection_retry_trigger
		BEFORE INSERT ON lazily_sim_projection_event
		FOR EACH ROW EXECUTE FUNCTION lazily_sim_projection_fail_once();
	`)
	return err
}

func (corpus *postgresProjectionCorpus) removeRetryTrigger(ctx context.Context) error {
	_, err := corpus.db.ExecContext(ctx, "DROP FUNCTION IF EXISTS lazily_sim_projection_fail_once() CASCADE")
	return err
}

func (corpus *postgresProjectionCorpus) reconnect(t *testing.T) error {
	if err := corpus.db.Close(); err != nil {
		return err
	}
	db, err := sql.Open("pgx", corpus.url)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(t.Context()); err != nil {
		db.Close()
		return err
	}
	corpus.db = db
	barrier, err := NewPostgresProjectionBarrier(db, PostgresProjectionBarrierOptions{LockTimeout: 2 * time.Second, MaxAttempts: 4})
	if err != nil {
		return err
	}
	corpus.barrier = barrier
	return nil
}

func (corpus *postgresProjectionCorpus) apply(t *testing.T, ctx context.Context, action SimProjectionMaintenanceAction) (SimProjectionMaintenanceOutcome, error) {
	outcome, err := expectedSimProjectionOutcome(action)
	if err != nil {
		return outcome, err
	}
	switch action.Kind {
	case SimProjectionMaintenanceEvent:
		err = corpus.applyEvent(ctx, corpus.barrier, action.Event, nil)
	case SimProjectionMaintenanceFullRebuild:
		err = corpus.rebuild(ctx, corpus.barrier)
	case SimProjectionMaintenanceConcurrentRebuild:
		err = corpus.applyConcurrentRebuild(ctx, action.Event)
	case SimProjectionMaintenanceLockTimeout, SimProjectionMaintenanceCancellation:
		err = corpus.applyRejectedWait(ctx, action)
	case SimProjectionMaintenanceRetry:
		if err = corpus.installRetryTrigger(ctx); err == nil {
			err = corpus.applyEvent(ctx, corpus.barrier, action.Event, &corpus.retryAttempts)
		}
		removeErr := corpus.removeRetryTrigger(ctx)
		if err == nil {
			err = removeErr
		}
	case SimProjectionMaintenanceCrashBeforeCommit:
		err = corpus.barrier.ApplySourceWrite(ctx, simProjectionLocks, func(ctx context.Context, tx *sql.Tx) error {
			if err := appendMaterializedProjectionEvent(ctx, tx, action.Event); err != nil {
				return err
			}
			return errSimProjectionCrashBeforeCommit
		})
		if errors.Is(err, errSimProjectionCrashBeforeCommit) {
			err = nil
		}
	case SimProjectionMaintenanceCrashAfterCommit:
		if err = corpus.applyEvent(ctx, corpus.barrier, action.Event, nil); err == nil {
			err = corpus.reconnect(t)
		}
	}
	return outcome, err
}

func (corpus *postgresProjectionCorpus) observe(ctx context.Context) (SimProjectionMaintenanceObservation, error) {
	history, err := scanMaterializedProjectionHistory(ctx, corpus.db)
	if err != nil {
		return SimProjectionMaintenanceObservation{}, err
	}
	rows, err := corpus.db.QueryContext(ctx, "SELECT item_key, item_value FROM lazily_sim_projection_view ORDER BY item_key")
	if err != nil {
		return SimProjectionMaintenanceObservation{}, err
	}
	defer rows.Close()
	projection := map[string]string{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return SimProjectionMaintenanceObservation{}, err
		}
		projection[key] = value
	}
	return SimProjectionMaintenanceObservation{History: history, Projection: projection}, rows.Err()
}

func TestSimProjectionMaintenancePostgresRaceCorpus(t *testing.T) {
	corpus := newPostgresProjectionCorpus(t)
	memory, _ := newSimConsumerTestAdapter("memory", SimConsumerAdapterInMemory, 0)
	postgres, _ := newSimConsumerTestAdapter("postgres.race", SimConsumerAdapterPostgres, 0)
	memory.ProductionReducerID, memory.ReducerID = "materialized.projection.reducer.v1", "materialized.projection.reducer.v1"
	postgres.ProductionReducerID, postgres.ReducerID = memory.ProductionReducerID, memory.ReducerID
	memory.ProjectionMaintenance, _ = newMemoryProjectionPort()
	postgres.Probe = func() error { return corpus.db.PingContext(t.Context()) }
	postgres.ProjectionMaintenance = &SimProjectionMaintenanceAdapter{
		Reset: corpus.reset,
		Apply: func(ctx context.Context, action SimProjectionMaintenanceAction) (SimProjectionMaintenanceOutcome, error) {
			return corpus.apply(t, ctx, action)
		},
		Observe: corpus.observe,
	}
	kit, err := NewSimConsumerTestkit(SimConsumerTestkitSpec{
		SimulationAdapterID: "memory", RequiredRealAdapters: []SimConsumerAdapterKind{SimConsumerAdapterPostgres},
		Adapters: []SimConsumerAdapter{postgres, memory},
	})
	if err != nil {
		t.Fatal(err)
	}
	scenario := projectionScenario(
		projectionGeneratedAction("action.append.alpha", SimProjectionMaintenanceEvent, MaterializedProjectionEvent{ID: "event.append.alpha", Kind: MaterializedProjectionAppend, Key: "item.alpha", Value: "one"}, ""),
		projectionGeneratedAction("action.rebuild.first", SimProjectionMaintenanceFullRebuild, MaterializedProjectionEvent{}, ""),
		projectionGeneratedAction("action.amend.alpha", SimProjectionMaintenanceEvent, MaterializedProjectionEvent{ID: "event.amend.alpha", Kind: MaterializedProjectionAmend, Key: "item.alpha", Value: "two"}, "action.append.alpha"),
		projectionGeneratedAction("action.concurrent.beta", SimProjectionMaintenanceConcurrentRebuild, MaterializedProjectionEvent{ID: "event.append.beta", Kind: MaterializedProjectionAppend, Key: "item.beta", Value: "other"}, ""),
		projectionGeneratedAction("action.timeout.gamma", SimProjectionMaintenanceLockTimeout, MaterializedProjectionEvent{ID: "event.timeout.gamma", Kind: MaterializedProjectionAppend, Key: "item.gamma", Value: "lost"}, ""),
		projectionGeneratedAction("action.retry.gamma", SimProjectionMaintenanceRetry, MaterializedProjectionEvent{ID: "event.append.gamma", Kind: MaterializedProjectionAppend, Key: "item.gamma", Value: "three"}, ""),
		projectionGeneratedAction("action.cancel.gamma", SimProjectionMaintenanceCancellation, MaterializedProjectionEvent{ID: "event.cancel.gamma", Kind: MaterializedProjectionAmend, Key: "item.gamma", Value: "lost"}, "action.retry.gamma"),
		projectionGeneratedAction("action.crash.before", SimProjectionMaintenanceCrashBeforeCommit, MaterializedProjectionEvent{ID: "event.crash.before", Kind: MaterializedProjectionRetract, Key: "item.beta"}, "action.concurrent.beta"),
		projectionGeneratedAction("action.crash.after", SimProjectionMaintenanceCrashAfterCommit, MaterializedProjectionEvent{ID: "event.retract.beta", Kind: MaterializedProjectionRetract, Key: "item.beta"}, "action.concurrent.beta"),
		projectionGeneratedAction("action.rebuild.final", SimProjectionMaintenanceFullRebuild, MaterializedProjectionEvent{}, ""),
	)
	result, err := kit.RunProjectionMaintenance(t.Context(), scenario)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.FinalHistory) != 5 || !maps.Equal(result.FinalProjection, map[string]string{"item.alpha": "two", "item.gamma": "three"}) {
		t.Fatalf("result = %+v", result)
	}
	if attempts := corpus.retryAttempts.Load(); attempts != 2 {
		t.Fatalf("serialization retry callback attempts = %d, want 2", attempts)
	}
}
