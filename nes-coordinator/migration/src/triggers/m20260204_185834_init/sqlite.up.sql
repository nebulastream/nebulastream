-- Enforce the query lifecycle and reject illegal transitions. Terminal
-- states (Completed/Stopped/Failed) cannot transition out; rollbacks
-- to Pending from Registered/Running are allowed so the application
-- can reset a query.
CREATE TRIGGER IF NOT EXISTS validate_query_state_transition
    BEFORE UPDATE OF state
    ON query
    WHEN NEW.state != OLD.state
BEGIN
    SELECT CASE
        WHEN OLD.state = 'Pending' AND NEW.state NOT IN ('Registered', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid state transition: Pending must transition to one of (Registered, Stopped, Failed)')

        WHEN OLD.state = 'Registered' AND NEW.state NOT IN ('Pending', 'Running', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid state transition: Registered must transition to one of (Pending, Running, Stopped, Failed)')

        WHEN OLD.state = 'Running' AND NEW.state NOT IN ('Pending', 'Stopped', 'Completed', 'Failed') THEN
            RAISE(ABORT, 'Invalid state transition: Running must transition to one of (Pending, Stopped, Completed, Failed)')

        WHEN OLD.state IN ('Completed', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid state transition: Cannot transition from a terminal state')
    END;
END;

-- Same transition rules as for query.state, applied per fragment. The
-- two triggers must stay aligned: any state a fragment can reach must
-- also be a state the derived query state is allowed to reach.
CREATE TRIGGER IF NOT EXISTS validate_fragment_state_transition
    BEFORE UPDATE OF current_state
    ON fragment
    WHEN NEW.current_state != OLD.current_state
BEGIN
    SELECT CASE
        WHEN OLD.current_state = 'Pending' AND NEW.current_state NOT IN ('Registered', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid fragment state transition: Pending must transition to one of (Registered, Stopped, Failed)')

        WHEN OLD.current_state = 'Registered' AND NEW.current_state NOT IN ('Pending', 'Running', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid fragment state transition: Registered must transition to one of (Pending, Running, Stopped, Failed)')

        WHEN OLD.current_state = 'Running' AND NEW.current_state NOT IN ('Pending', 'Completed', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid fragment state transition: Running must transition to one of (Pending, Completed, Stopped, Failed)')

        WHEN OLD.current_state IN ('Completed', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid fragment state transition: Cannot transition from a terminal state')
    END;
END;

-- Reserve operator slots on the hosting worker at fragment insert. A
-- NULL max_operators means unbounded capacity, so we skip the update;
-- combined with the `max_operators >= 0` CHECK, this rejects placements
-- that would overflow capacity in the same transaction as the insert.
CREATE TRIGGER IF NOT EXISTS acquire_worker_capacity
    AFTER INSERT ON fragment
BEGIN
    UPDATE worker
    SET max_operators = max_operators - NEW.num_operators
    WHERE host_addr = NEW.host_addr
    AND max_operators IS NOT NULL;
END;

-- Counterpart to the acquire above: when a fragment enters a terminal
-- state, give its slots back. The OLD-state guard prevents double-
-- crediting if a terminal fragment is saved again.
CREATE TRIGGER IF NOT EXISTS release_fragment_capacity
    AFTER UPDATE OF current_state ON fragment
    WHEN NEW.current_state IN ('Completed', 'Stopped', 'Failed')
    AND OLD.current_state NOT IN ('Completed', 'Stopped', 'Failed')
BEGIN
    UPDATE worker
    SET max_operators = max_operators + NEW.num_operators
    WHERE host_addr = NEW.host_addr
    AND max_operators IS NOT NULL;
END;

-- Single source of truth for query.state, the start/stop timestamps,
-- and the per-host error JSON. Runs after any fragment state change
-- and does three things in order:
--   1. Recompute query.state from the multiset of fragment states.
--      Rule order matters: any Failed fragment fails the query;
--      otherwise any Pending fragment keeps it Pending; otherwise the
--      'all-in-{X}' rules fall through to the most-advanced reachable
--      state. COALESCE preserves the previous state when no rule fires.
--   2. Propagate timestamps: start = MIN(fragment.start), stop =
--      MAX(fragment.stop), each gated on the derived query state so
--      timestamps never leak into states that should not carry them.
--   3. If the query ended up Failed, aggregate per-host fragment
--      errors into a {host_addr: message} JSON object.
CREATE TRIGGER IF NOT EXISTS derive_query_state_on_fragment_update
    AFTER UPDATE OF current_state ON fragment
BEGIN
    UPDATE query
    SET state = COALESCE(
        (SELECT CASE
            WHEN EXISTS (SELECT 1 FROM fragment WHERE query_id = NEW.query_id AND current_state = 'Failed')
                THEN 'Failed'
            WHEN EXISTS (SELECT 1 FROM fragment WHERE query_id = NEW.query_id AND current_state = 'Pending')
                THEN 'Pending'
            WHEN NOT EXISTS (SELECT 1 FROM fragment WHERE query_id = NEW.query_id AND current_state != 'Completed')
                THEN 'Completed'
            WHEN NOT EXISTS (SELECT 1 FROM fragment WHERE query_id = NEW.query_id AND current_state NOT IN ('Completed', 'Stopped'))
                THEN 'Stopped'
            WHEN NOT EXISTS (SELECT 1 FROM fragment WHERE query_id = NEW.query_id AND current_state NOT IN ('Running', 'Completed'))
                THEN 'Running'
            WHEN NOT EXISTS (SELECT 1 FROM fragment WHERE query_id = NEW.query_id AND current_state NOT IN ('Registered', 'Running', 'Completed'))
                THEN 'Registered'
            ELSE NULL
        END),
        (SELECT state FROM query WHERE id = NEW.query_id)
    )
    WHERE id = NEW.query_id;

    UPDATE query SET
        start_timestamp = CASE
            WHEN (SELECT state FROM query WHERE id = NEW.query_id)
                 IN ('Running', 'Completed', 'Stopped', 'Failed')
            THEN COALESCE(
                (SELECT MIN(start_timestamp) FROM fragment WHERE query_id = NEW.query_id),
                (SELECT start_timestamp FROM query WHERE id = NEW.query_id)
            )
            ELSE (SELECT start_timestamp FROM query WHERE id = NEW.query_id)
        END,
        stop_timestamp = CASE
            WHEN (SELECT state FROM query WHERE id = NEW.query_id)
                 IN ('Completed', 'Stopped', 'Failed')
            THEN COALESCE(
                (SELECT MAX(stop_timestamp) FROM fragment WHERE query_id = NEW.query_id),
                (SELECT stop_timestamp FROM query WHERE id = NEW.query_id)
            )
            ELSE (SELECT stop_timestamp FROM query WHERE id = NEW.query_id)
        END
    WHERE id = NEW.query_id;

    UPDATE query SET
        error = (
            SELECT json_group_object(
                f.host_addr,
                COALESCE(
                    json_extract(f.error, '$.Internal.msg'),
                    json_extract(f.error, '$.Transport.msg')
                )
            )
            FROM fragment f
            WHERE f.query_id = NEW.query_id AND f.error IS NOT NULL
        )
    WHERE id = NEW.query_id
    AND (SELECT state FROM query WHERE id = NEW.query_id) = 'Failed';
END;

-- When the derive trigger above flips a query to Failed (because of
-- one fragment), force every other non-terminal fragment of that
-- query to stop. Forceful: a failed query is not worth draining
-- gracefully.
CREATE TRIGGER IF NOT EXISTS cascade_query_failed_to_fragments
    AFTER UPDATE OF state ON query
    WHEN NEW.state = 'Failed' AND OLD.state != 'Failed'
BEGIN
    UPDATE fragment
    SET desired_state = 'Stopped',
        stop_mode = 'Forceful'
    WHERE query_id = NEW.id
    AND current_state NOT IN ('Completed', 'Stopped', 'Failed');
END;

-- When a worker is marked for removal, every non-terminal fragment on
-- that host is unrecoverable, so mark them Failed. The derive trigger
-- above then propagates the failure to the owning queries. We must
-- set `stop_timestamp` and `error` together with `current_state`,
-- otherwise the row violates the application's invariant that a
-- Failed fragment carries both, and the per-host error aggregation
-- would silently drop this host.
CREATE TRIGGER IF NOT EXISTS cascade_worker_removed_to_fragments
    AFTER UPDATE OF desired_state ON worker
    WHEN NEW.desired_state = 'Removed' AND OLD.desired_state != 'Removed'
BEGIN
    UPDATE fragment
    SET current_state = 'Failed',
        stop_timestamp = CURRENT_TIMESTAMP,
        error = json_object('Transport', json_object('msg', 'worker removed'))
    WHERE host_addr = NEW.host_addr
    AND current_state NOT IN ('Completed', 'Stopped', 'Failed');
END;

-- Query-owned (Inline/Internal) sources are tied to exactly one query.
-- When the last query_source row referencing them goes away (the FK
-- cascade fires when the owning query is deleted), drop the source
-- too. Shared sources are user-managed and never auto-cleaned.
CREATE TRIGGER IF NOT EXISTS cleanup_orphaned_source
    AFTER DELETE ON query_source
BEGIN
    DELETE FROM physical_source
    WHERE kind != 'Shared'
      AND id = OLD.source_id
      AND NOT EXISTS (SELECT 1 FROM query_source WHERE source_id = OLD.source_id);
END;

-- Refuse to drop a physical source while any non-terminal query still
-- references it; the query would otherwise keep running with a
-- dangling source id. Wait for the query to finish, or drop it first.
CREATE TRIGGER IF NOT EXISTS prevent_physical_source_drop_with_active_queries
    BEFORE DELETE ON physical_source
BEGIN
    SELECT CASE
        WHEN EXISTS (
            SELECT 1 FROM query_source qs
            JOIN query q ON q.id = qs.query_id
            WHERE qs.source_id = OLD.id
            AND q.state NOT IN ('Completed', 'Stopped', 'Failed')
        ) THEN
            RAISE(ABORT, 'Cannot drop physical source: non-terminal queries still reference it')
    END;
END;

-- Sink counterpart of the physical-source drop guard above.
CREATE TRIGGER IF NOT EXISTS prevent_sink_drop_with_active_queries
    BEFORE DELETE ON sink
BEGIN
    SELECT CASE
        WHEN EXISTS (
            SELECT 1 FROM query_sink qs
            JOIN query q ON q.id = qs.query_id
            WHERE qs.sink_id = OLD.id
            AND q.state NOT IN ('Completed', 'Stopped', 'Failed')
        ) THEN
            RAISE(ABORT, 'Cannot drop sink: non-terminal queries still reference it')
    END;
END;

-- Sink counterpart of the source orphan cleanup above.
CREATE TRIGGER IF NOT EXISTS cleanup_orphaned_sink
    AFTER DELETE ON query_sink
BEGIN
    DELETE FROM sink
    WHERE kind != 'Shared'
      AND id = OLD.sink_id
      AND NOT EXISTS (SELECT 1 FROM query_sink WHERE sink_id = OLD.sink_id);
END;
