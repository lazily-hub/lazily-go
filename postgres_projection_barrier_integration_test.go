package lazily

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type projectionRow struct {
	ID    int
	Value string
}

var projectionLocks = []PostgresProjectionLock{
	{Namespace: 1701, Key: 2},
	{Namespace: 1701, Key: 1},
}

func openProjectionDatabase(t *testing.T) *sql.DB {
	t.Helper()
	url := os.Getenv("LAZILY_POSTGRES_URL")
	if url == "" {
		t.Skip("LAZILY_POSTGRES_URL is required for PostgreSQL integration tests")
	}
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func resetProjectionDatabase(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `
		DROP FUNCTION IF EXISTS lazily_projection_fail_once() CASCADE;
		DROP SEQUENCE IF EXISTS lazily_projection_retry_once;
		DROP TABLE IF EXISTS lazily_projection_view;
		DROP TABLE IF EXISTS lazily_projection_source;
		CREATE TABLE lazily_projection_source (
			item_id INTEGER PRIMARY KEY,
			item_value TEXT NOT NULL
		);
		CREATE TABLE lazily_projection_view (
			item_id INTEGER PRIMARY KEY,
			item_value TEXT NOT NULL
		);
	`)
	if err != nil {
		t.Fatal(err)
	}
}

func projectionBarrier(t *testing.T, db *sql.DB, timeout time.Duration) *PostgresProjectionBarrier {
	t.Helper()
	barrier, err := NewPostgresProjectionBarrier(db, PostgresProjectionBarrierOptions{
		LockTimeout: timeout,
		MaxAttempts: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	return barrier
}

func applyProjectionRow(ctx context.Context, barrier *PostgresProjectionBarrier, row projectionRow) error {
	return barrier.ApplySourceWrite(ctx, projectionLocks, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO lazily_projection_source (item_id, item_value) VALUES ($1, $2)",
			row.ID, row.Value,
		); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO lazily_projection_view (item_id, item_value) VALUES ($1, $2)
			ON CONFLICT (item_id) DO UPDATE SET item_value = EXCLUDED.item_value
		`, row.ID, row.Value)
		return err
	})
}

func scanProjectionRows(ctx context.Context, tx *sql.Tx) ([]projectionRow, error) {
	rows, err := tx.QueryContext(ctx,
		"SELECT item_id, item_value FROM lazily_projection_source ORDER BY item_id",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []projectionRow
	for rows.Next() {
		var row projectionRow
		if err := rows.Scan(&row.ID, &row.Value); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func replaceProjectionRows(ctx context.Context, tx *sql.Tx, rows []projectionRow) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM lazily_projection_view"); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO lazily_projection_view (item_id, item_value) VALUES ($1, $2)",
			row.ID, row.Value,
		); err != nil {
			return err
		}
	}
	return nil
}

func readProjectionRows(t *testing.T, db *sql.DB, table string) []projectionRow {
	t.Helper()
	query := fmt.Sprintf("SELECT item_id, item_value FROM %s ORDER BY item_id", table)
	rows, err := db.QueryContext(t.Context(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []projectionRow
	for rows.Next() {
		var row projectionRow
		if err := rows.Scan(&row.ID, &row.Value); err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestPostgresProjectionBarrierTwoConnectionMultiHostNoAcceptedWriteLost(t *testing.T) {
	rebuildDB := openProjectionDatabase(t)
	writerDB := openProjectionDatabase(t)
	resetProjectionDatabase(t, rebuildDB)
	rebuilder := projectionBarrier(t, rebuildDB, 2*time.Second)
	writer := projectionBarrier(t, writerDB, 2*time.Second)

	if err := applyProjectionRow(t.Context(), writer, projectionRow{ID: 1, Value: "alpha"}); err != nil {
		t.Fatal(err)
	}

	scanned := make(chan struct{})
	releaseRebuild := make(chan struct{})
	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- MaintainPostgresProjection(
			context.Background(),
			rebuilder,
			projectionLocks,
			func(ctx context.Context, tx *sql.Tx) ([]projectionRow, error) {
				rows, err := scanProjectionRows(ctx, tx)
				if err != nil {
					return nil, err
				}
				close(scanned)
				select {
				case <-releaseRebuild:
					return rows, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
			replaceProjectionRows,
		)
	}()
	<-scanned

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- applyProjectionRow(context.Background(), writer, projectionRow{ID: 2, Value: "beta"})
	}()
	select {
	case err := <-writerDone:
		t.Fatalf("source writer crossed a held rebuild barrier: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseRebuild)
	if err := <-rebuildDone; err != nil {
		t.Fatal(err)
	}
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}

	source := readProjectionRows(t, rebuildDB, "lazily_projection_source")
	projection := readProjectionRows(t, writerDB, "lazily_projection_view")
	if !reflect.DeepEqual(projection, source) {
		t.Fatalf("projection rows = %#v, accepted source rows = %#v", projection, source)
	}
}

func TestPostgresProjectionBarrierBoundsWaitAndHonorsCancellation(t *testing.T) {
	holderDB := openProjectionDatabase(t)
	waiterDB := openProjectionDatabase(t)
	resetProjectionDatabase(t, holderDB)
	holder := projectionBarrier(t, holderDB, 2*time.Second)

	locked := make(chan struct{})
	release := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- holder.ApplySourceWrite(context.Background(), projectionLocks, func(ctx context.Context, _ *sql.Tx) error {
			close(locked)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	<-locked

	timed := projectionBarrier(t, waiterDB, 40*time.Millisecond)
	err := timed.ApplySourceWrite(t.Context(), projectionLocks, func(context.Context, *sql.Tx) error { return nil })
	if !errors.Is(err, ErrPostgresProjectionBarrierTimeout) {
		t.Fatalf("bounded lock wait error = %v", err)
	}

	cancelled := projectionBarrier(t, waiterDB, 2*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	err = cancelled.ApplySourceWrite(ctx, projectionLocks, func(context.Context, *sql.Tx) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled lock wait error = %v", err)
	}

	close(release)
	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
}

func TestPostgresProjectionBarrierRetriesCompleteSerializableTransaction(t *testing.T) {
	db := openProjectionDatabase(t)
	resetProjectionDatabase(t, db)
	_, err := db.ExecContext(t.Context(), `
		CREATE SEQUENCE lazily_projection_retry_once START 1;
		CREATE FUNCTION lazily_projection_fail_once() RETURNS trigger AS $$
		BEGIN
			IF nextval('lazily_projection_retry_once') = 1 THEN
				RAISE EXCEPTION 'deterministic serialization retry' USING ERRCODE = '40001';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER lazily_projection_retry_trigger
		BEFORE INSERT ON lazily_projection_source
		FOR EACH ROW EXECUTE FUNCTION lazily_projection_fail_once();
	`)
	if err != nil {
		t.Fatal(err)
	}

	barrier := projectionBarrier(t, db, time.Second)
	var attempts atomic.Int32
	err = barrier.ApplySourceWrite(t.Context(), projectionLocks, func(ctx context.Context, tx *sql.Tx) error {
		attempts.Add(1)
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO lazily_projection_source (item_id, item_value) VALUES (3, 'gamma')",
		); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx,
			"INSERT INTO lazily_projection_view (item_id, item_value) VALUES (3, 'gamma')",
		)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("transaction callback attempts = %d, want 2", got)
	}
	if source, projection := readProjectionRows(t, db, "lazily_projection_source"), readProjectionRows(t, db, "lazily_projection_view"); !reflect.DeepEqual(source, projection) {
		t.Fatalf("source rows = %#v, projection rows = %#v", source, projection)
	}
}
