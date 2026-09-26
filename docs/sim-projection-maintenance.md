# Projection-maintenance simulation corpus

`SimConsumerTestkit.RunProjectionMaintenance` exercises projection maintenance
as a generated history rather than as a collection of unrelated examples. The
history language is deliberately generic: synthetic keys receive append,
amend, and retract events while rebuild and failure-window commands change how
an event is attempted. No application schema, policy, incident, fixture, or
consumer name is embedded in the library corpus.

The selected adapters have distinct execution boundaries:

- The in-memory adapter is the reference host.
- The PostgreSQL adapter is explicitly selected, probed, and connected to a
  real service. Its writers and rebuilds use `PostgresProjectionBarrier`.
- Both adapters invoke `ReduceMaterializedProjectionHistory`; there is no
  test-only projection algorithm to agree with itself.

After every command, the runner compares the exact ordered history of accepted
events with every adapter. It then reduces that complete history from empty
state and requires each materialized projection to equal the result. A timeout,
cancellation, or crash before commit must add nothing. A successful write,
retry, concurrent rebuild/write, or crash after commit must remain in both the
durable history and the rebuilt result. This establishes the two corpus
invariants: no accepted update is lost, and final state is equivalent to replay
of the complete accepted history.

## Commands and race windows

| Command | Required result |
|---|---|
| `event` | Event and projection commit together. |
| `full_rebuild` | Complete history is scanned and replaces the projection. |
| `concurrent_rebuild` | A source writer waits behind an already-scanned rebuild, then remains visible after both commit. |
| `lock_timeout` | A bounded advisory-lock wait fails and rolls back. |
| `serialization_retry` | PostgreSQL raises `40001` once and the entire callback runs again. |
| `cancellation` | Context expiry interrupts a lock wait and rolls back. |
| `crash_before_commit` | The callback aborts after writes but before commit, leaving no event. |
| `crash_after_commit` | The connection pool is discarded after commit; a fresh pool must recover the event and projection. |

`ShrinkSimProjectionMaintenanceFailure` delegates to `SimCausalShrinker`.
Removal candidates remain command-valid and dependency-closed: an action's
`CauseID` cannot survive without its cause, while append/amend/retract
preconditions are re-evaluated against the candidate prefix. The returned
sequence is a one-removal-minimal reproduction under the supplied failure
predicate.

The fuzz target keeps its input transformation fast and deterministic, derives
only valid histories, and lets Go retain minimized failing inputs as regression
seeds. These choices follow the official [Go fuzzing security
guidance](https://go.dev/doc/security/fuzz/) and [fuzzing
tutorial](https://go.dev/doc/tutorial/fuzz).

The database race shapes use only behavior described by PostgreSQL's primary
documentation: transaction advisory locks are cooperative and released at
transaction end, consistent lock ordering avoids deadlocks, `lock_timeout`
bounds an otherwise indefinite wait, and `SERIALIZABLE` failures require a
complete transaction retry. See [explicit and advisory
locking](https://www.postgresql.org/docs/current/explicit-locking.html),
[`LOCK TABLE`](https://www.postgresql.org/docs/current/sql-lock.html),
[transaction isolation](https://www.postgresql.org/docs/current/transaction-iso.html),
and [client timeout settings](https://www.postgresql.org/docs/current/runtime-config-client.html).
