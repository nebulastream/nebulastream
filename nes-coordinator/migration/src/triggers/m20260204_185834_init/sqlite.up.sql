--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--      https://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.

-- Enforce the query lifecycle and reject illegal transitions.
-- Terminal states (Completed/Stopped/Failed) cannot transition out.
-- Rollbacks to Pending from Started/Running are allowed so the application can reset a query.
--
-- Started may go straight to Completed.
-- These states record what a worker was last seen doing,
-- and a query short enough to finish between two observations is never seen running.
--
-- The state names here mirror the model's state enums
-- (the CHECK constraints on the state columns list the same values);
-- keep them in sync when adding or renaming a state.
CREATE TRIGGER IF NOT EXISTS validate_query_state_transition
    BEFORE UPDATE OF state
    ON query
    WHEN NEW.state != OLD.state
BEGIN
    SELECT CASE
        WHEN OLD.state = 'Pending' AND NEW.state NOT IN ('Started', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid state transition: Pending must transition to one of (Started, Stopped, Failed)')

        WHEN OLD.state = 'Started' AND NEW.state NOT IN ('Pending', 'Running', 'Completed', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid state transition: Started must transition to one of (Pending, Running, Completed, Stopped, Failed)')

        WHEN OLD.state = 'Running' AND NEW.state NOT IN ('Pending', 'Stopped', 'Completed', 'Failed') THEN
            RAISE(ABORT, 'Invalid state transition: Running must transition to one of (Pending, Stopped, Completed, Failed)')

        WHEN OLD.state IN ('Completed', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid state transition: Cannot transition from a terminal state')
    END;
END;

-- Same transition rules as for query.state, applied per query_fragment.
-- The two triggers must stay aligned:
-- every state that a query_fragment can reach must also be reachable by the derived query state.
CREATE TRIGGER IF NOT EXISTS validate_query_fragment_state_transition
    BEFORE UPDATE OF current_state
    ON query_fragment
    WHEN NEW.current_state != OLD.current_state
BEGIN
    SELECT CASE
        WHEN OLD.current_state = 'Pending' AND NEW.current_state NOT IN ('Started', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid query_fragment state transition: Pending must transition to one of (Started, Stopped, Failed)')

        WHEN OLD.current_state = 'Started' AND NEW.current_state NOT IN ('Pending', 'Running', 'Completed', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid query_fragment state transition: Started must transition to one of (Pending, Running, Completed, Stopped, Failed)')

        WHEN OLD.current_state = 'Running' AND NEW.current_state NOT IN ('Pending', 'Completed', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid query_fragment state transition: Running must transition to one of (Pending, Completed, Stopped, Failed)')

        WHEN OLD.current_state IN ('Completed', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid query_fragment state transition: Cannot transition from a terminal state')
    END;
END;

-- Reserve operator slots on the hosting worker when a query_fragment is inserted.
-- A NULL max_operators means unbounded capacity, so the update is skipped.
-- Combined with the `max_operators >= 0` CHECK,
-- this rejects placements that would overflow capacity in the same transaction as the insert.
CREATE TRIGGER IF NOT EXISTS acquire_worker_capacity
    AFTER INSERT ON query_fragment
BEGIN
    UPDATE worker
    SET max_operators = max_operators - NEW.num_operators
    WHERE host_addr = NEW.host_addr
    AND max_operators IS NOT NULL;
END;

-- Counterpart to the acquire above:
-- when a query_fragment enters a terminal state, give its slots back.
-- The OLD-state guard prevents double crediting if a terminal query_fragment is saved again.
CREATE TRIGGER IF NOT EXISTS release_query_fragment_capacity
    AFTER UPDATE OF current_state ON query_fragment
    WHEN NEW.current_state IN ('Completed', 'Stopped', 'Failed')
    AND OLD.current_state NOT IN ('Completed', 'Stopped', 'Failed')
BEGIN
    UPDATE worker
    SET max_operators = max_operators + NEW.num_operators
    WHERE host_addr = NEW.host_addr
    AND max_operators IS NOT NULL;
END;

-- Delete-side counterpart to the release above.
-- A hard delete of a query_fragment (for example the cascade from deleting its query)
-- is a DELETE, not an UPDATE, so the release trigger never fires and the slots would leak.
-- The OLD-state guard skips query_fragments that already reached a terminal state and released their slots.
CREATE TRIGGER IF NOT EXISTS release_capacity_on_query_fragment_delete
    AFTER DELETE ON query_fragment
    WHEN OLD.current_state NOT IN ('Completed', 'Stopped', 'Failed')
BEGIN
    UPDATE worker
    SET max_operators = max_operators + OLD.num_operators
    WHERE host_addr = OLD.host_addr
    AND max_operators IS NOT NULL;
END;

-- Single source of truth for query.state, the start/stop timestamps, and the per-host error JSON.
-- Runs after any query_fragment state change and does three things in order:
--   1. Recompute query.state from the multiset of query_fragment states.
--      Rule order matters: any Failed query_fragment fails the query;
--      otherwise any Pending query_fragment keeps it Pending;
--      otherwise the 'all-in-{X}' rules fall through to the most advanced reachable state.
--      COALESCE preserves the previous state when no rule fires.
--   2. Propagate timestamps: start = MIN(query_fragment.start), stop = MAX(query_fragment.stop).
--      Each is written only once the derived query state can have it,
--      so no timestamp leaks into an earlier state.
--   3. If the query ended up Failed and carries no error yet,
--      copy the first failed query_fragment's error and host onto it, as a {host_addr, error} JSON object.
--      The earliest stop_timestamp is the first failure; a later one is a consequence of it
--      (the other query_fragments being stopped, or a worker removed), so the assignment is final.
CREATE TRIGGER IF NOT EXISTS derive_query_state_on_query_fragment_update
    AFTER UPDATE OF current_state ON query_fragment
BEGIN
    UPDATE query
    SET state = COALESCE(
        (SELECT CASE
            WHEN EXISTS (SELECT 1 FROM query_fragment WHERE query_id = NEW.query_id AND current_state = 'Failed')
                THEN 'Failed'
            WHEN EXISTS (SELECT 1 FROM query_fragment WHERE query_id = NEW.query_id AND current_state = 'Pending')
                THEN 'Pending'
            WHEN NOT EXISTS (SELECT 1 FROM query_fragment WHERE query_id = NEW.query_id AND current_state != 'Completed')
                THEN 'Completed'
            WHEN NOT EXISTS (SELECT 1 FROM query_fragment WHERE query_id = NEW.query_id AND current_state NOT IN ('Completed', 'Stopped'))
                THEN 'Stopped'
            WHEN NOT EXISTS (SELECT 1 FROM query_fragment WHERE query_id = NEW.query_id AND current_state NOT IN ('Running', 'Completed'))
                THEN 'Running'
            WHEN NOT EXISTS (SELECT 1 FROM query_fragment WHERE query_id = NEW.query_id AND current_state NOT IN ('Started', 'Running', 'Completed'))
                THEN 'Started'
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
                (SELECT MIN(start_timestamp) FROM query_fragment WHERE query_id = NEW.query_id),
                (SELECT start_timestamp FROM query WHERE id = NEW.query_id)
            )
            ELSE (SELECT start_timestamp FROM query WHERE id = NEW.query_id)
        END,
        stop_timestamp = CASE
            WHEN (SELECT state FROM query WHERE id = NEW.query_id)
                 IN ('Completed', 'Stopped', 'Failed')
            THEN COALESCE(
                (SELECT MAX(stop_timestamp) FROM query_fragment WHERE query_id = NEW.query_id),
                (SELECT stop_timestamp FROM query WHERE id = NEW.query_id)
            )
            ELSE (SELECT stop_timestamp FROM query WHERE id = NEW.query_id)
        END
    WHERE id = NEW.query_id;

    UPDATE query SET
        error = (
            SELECT json_object('host_addr', f.host_addr, 'error', json(f.error))
            FROM query_fragment f
            WHERE f.query_id = NEW.query_id AND f.current_state = 'Failed' AND f.error IS NOT NULL
            ORDER BY f.stop_timestamp, f.id
            LIMIT 1
        )
    WHERE id = NEW.query_id
    AND (SELECT state FROM query WHERE id = NEW.query_id) = 'Failed'
    AND error IS NULL;
END;

-- When the derive trigger above sets a query to Failed because one query_fragment failed,
-- stop every other non-terminal query_fragment of that query.
CREATE TRIGGER IF NOT EXISTS cascade_query_failed_to_query_fragments
    AFTER UPDATE OF state ON query
    WHEN NEW.state = 'Failed' AND OLD.state != 'Failed'
BEGIN
    UPDATE query_fragment
    SET desired_state = 'Stopped'
    WHERE query_id = NEW.id
    AND current_state NOT IN ('Completed', 'Stopped', 'Failed');
END;

-- When a worker is marked for removal,
-- every non-terminal query_fragment on that host is unrecoverable, so mark them Failed.
-- The derive trigger above then propagates the failure to the owning queries.
-- stop_timestamp and error are set together with current_state,
-- otherwise the row violates the application's invariant that a Failed query_fragment has both.
CREATE TRIGGER IF NOT EXISTS cascade_worker_removed_to_query_fragments
    AFTER UPDATE OF desired_state ON worker
    WHEN NEW.desired_state = 'Removed' AND OLD.desired_state != 'Removed'
BEGIN
    UPDATE query_fragment
    SET current_state = 'Failed',
        -- SQLite has no datetime type:
        -- the column is declared date_time() and sea-orm stores DateTime<Utc> values as ISO-8601 text,
        -- so the trigger writes the same encoding
        -- and the derive trigger's MIN/MAX text comparison stays chronological.
        -- CURRENT_TIMESTAMP would emit a value with a space separator and no offset,
        -- which does not compare correctly against app-written timestamps.
        stop_timestamp = strftime('%Y-%m-%dT%H:%M:%f', 'now') || '+00:00',
        error = json_object('Transport', json_object('msg', 'worker removed'))
    WHERE host_addr = NEW.host_addr
    AND current_state NOT IN ('Completed', 'Stopped', 'Failed');
END;

-- Query-owned (Anonymous/Internal) sources are tied to exactly one query.
-- When the last query_source row that references them is deleted
-- (the FK cascade does this when the owning query is deleted), drop the source too.
-- Shared sources are user-managed and never auto-cleaned.
CREATE TRIGGER IF NOT EXISTS cleanup_orphaned_source
    AFTER DELETE ON query_source
BEGIN
    DELETE FROM physical_source
    WHERE kind != 'Shared'
      AND id = OLD.source_id
      AND NOT EXISTS (SELECT 1 FROM query_source WHERE source_id = OLD.source_id);
END;

-- Refuse to drop a physical source while any non-terminal query still references it;
-- the query would otherwise keep running with a dangling source id.
-- Wait for the query to finish, or drop it first.
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
