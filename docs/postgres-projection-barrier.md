# PostgreSQL projection-maintenance barrier

`PostgresProjectionBarrier` is the cross-process coordination capability for a
source table that has both incremental writers and a full projection rebuild.
Every accepted source writer and every rebuild must use the same advisory lock
keys. A source writer persists the source mutation and its incremental
projection change in its supplied transaction. A rebuild uses
`MaintainPostgresProjection` so source scan and projection replacement cannot
escape that same transaction.

The barrier deliberately combines two PostgreSQL mechanisms:

1. Transaction-scoped advisory locks provide a logical, per-projection mutex
   and are released automatically at transaction end. They are cooperative, so
   bypassing the barrier forfeits the guarantee. All keys are sorted by
   `(namespace, key)` and deduplicated before acquisition.
2. `SERIALIZABLE` validates the rest of the transaction's read/write shape. A
   `40001` serialization failure or `40P01` deadlock retries the complete
   callback, including a rebuild's scan. Callback code must therefore keep
   non-idempotent work inside the supplied transaction.

Each attempt applies a transaction-local `lock_timeout`. Context cancellation
also propagates through every database call. A `55P03` lock timeout is returned
as `ErrPostgresProjectionBarrierTimeout`; exhausting the bounded serialization
retry count returns `ErrPostgresProjectionBarrierRetryExhausted`.

## Why this strategy

PostgreSQL documents three relevant strategies:

| Strategy | Property | Tradeoff and decision |
|---|---|---|
| Table lock | `SHARE ROW EXCLUSIVE` can admit only one rebuilding writer and conflicts with ordinary row-exclusive writes. | Correct when every affected table is locked first, but unnecessarily blocks unrelated projections and requires a global table order. Not the reference strategy. |
| Transaction advisory lock | Application-defined identity, automatically released with the transaction. | Narrowest fit for one logical projection. Chosen, with a collision-free caller-owned namespace and mandatory participation by writers. |
| `SERIALIZABLE` | Successfully committed transactions have a serial explanation; anomalies abort with `40001`. | Necessary backstop, not a mutex. The entire transaction must be retried, so it complements the advisory barrier. |

PostgreSQL recommends acquiring multiple locks in a consistent order and using
the most restrictive required mode first. It otherwise permits lock waits to be
unbounded. The implementation's total advisory-key order and transaction-local
timeout make both requirements explicit.

Conceptual sources are limited to PostgreSQL's official documentation:

- [Explicit locking and advisory locks](https://www.postgresql.org/docs/current/explicit-locking.html)
- [`LOCK TABLE`](https://www.postgresql.org/docs/current/sql-lock.html)
- [`SERIALIZABLE` isolation and whole-transaction retry](https://www.postgresql.org/docs/current/transaction-iso.html)
- [`lock_timeout` and transaction timeout settings](https://www.postgresql.org/docs/current/runtime-config-client.html)

`ThreadSafeContext`, `AsyncContext`, and the in-memory latest-durable projection
types coordinate goroutines or one process only. They intentionally do not
implement `DatabaseProjectionMaintenanceBarrier` and cannot substitute for this
database-owned capability.
