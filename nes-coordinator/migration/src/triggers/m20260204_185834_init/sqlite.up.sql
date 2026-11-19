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

-- Rejects a change out of a terminal state (Completed, Stopped, Failed), and a change from Running back to Started.
-- Every other change is legal, and a move back to Pending lets the application reset a query.
-- The CHECK constraint on the column rejects any value outside the state names.
--
-- These states record what a worker was last seen doing,
-- and a query short enough to finish between two observations is never seen running.
-- If the reply to a query's start was lost, the query is still Pending when it is first seen running or completed.
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
        WHEN OLD.state IN ('Completed', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid query state transition: cannot leave a terminal state')
        WHEN OLD.state = 'Running' AND NEW.state = 'Started' THEN
            RAISE(ABORT, 'Invalid query state transition: cannot go from Running back to Started')
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
        WHEN OLD.current_state IN ('Completed', 'Stopped', 'Failed') THEN
            RAISE(ABORT, 'Invalid query_fragment state transition: cannot leave a terminal state')
        WHEN OLD.current_state = 'Running' AND NEW.current_state = 'Started' THEN
            RAISE(ABORT, 'Invalid query_fragment state transition: cannot go from Running back to Started')
    END;
END;

-- Reserve operator slots on the hosting worker when a query_fragment is inserted.
-- A NULL max_operators means unbounded capacity, so the update is skipped.
-- Combined with the `max_operators >= 0` CHECK,
-- this rejects placements that would overflow capacity in the same transaction as the insert.
CREATE TRIGGER IF NOT EXISTS acquire_capacity_on_query_fragment_insert
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
CREATE TRIGGER IF NOT EXISTS release_capacity_on_query_fragment_termination
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

-- Derives a query's state, its start and stop timestamps, and its error from its query_fragments.
CREATE TRIGGER IF NOT EXISTS derive_query_state_on_query_fragment_update
    AFTER UPDATE OF current_state ON query_fragment
    WHEN NEW.current_state != OLD.current_state
BEGIN
    -- The first matching row wins, and the query keeps its state when no row matches:
    --   any Failed                            Failed
    --   any Pending                           Pending
    --   all Completed                         Completed
    --   all in {Completed, Stopped}           Stopped
    --   all in {Running, Completed}           Running
    --   all in {Started, Running, Completed}  Started
    -- For example, {Running, Stopped} matches no row.
    -- MAX over a condition is true when any query_fragment meets it, and MIN when every query_fragment does.
    -- NOTE: The statement writes query.state also when the state stays the same, and the drop trigger below relies on that.
    UPDATE query
    SET state = COALESCE(
        (SELECT CASE
            WHEN MAX(current_state = 'Failed') THEN 'Failed'
            WHEN MAX(current_state = 'Pending') THEN 'Pending'
            WHEN MIN(current_state = 'Completed') THEN 'Completed'
            WHEN MIN(current_state IN ('Completed', 'Stopped')) THEN 'Stopped'
            WHEN MIN(current_state IN ('Running', 'Completed')) THEN 'Running'
            WHEN MIN(current_state IN ('Started', 'Running', 'Completed')) THEN 'Started'
        END
        FROM query_fragment
        WHERE query_id = NEW.query_id),
        state
    )
    WHERE id = NEW.query_id;

    -- A timestamp is copied only once the new state can have it,
    -- so a Started query gets no start_timestamp while one of its query_fragments is already Running.
    -- The error comes from the query_fragment with the earliest stop_timestamp, which failed first.
    -- A later failure is a consequence of it (another query_fragment was stopped, or a worker was removed),
    -- so the error is set once and never replaced.
    UPDATE query SET
        start_timestamp = COALESCE(
            CASE WHEN state IN ('Running', 'Completed', 'Stopped', 'Failed') THEN
                (SELECT MIN(start_timestamp) FROM query_fragment WHERE query_id = NEW.query_id)
            END,
            start_timestamp
        ),
        stop_timestamp = COALESCE(
            CASE WHEN state IN ('Completed', 'Stopped', 'Failed') THEN
                (SELECT MAX(stop_timestamp) FROM query_fragment WHERE query_id = NEW.query_id)
            END,
            stop_timestamp
        ),
        error = COALESCE(
            error,
            CASE WHEN state = 'Failed' THEN (
                SELECT json_object('host_addr', f.host_addr, 'error', json(f.error))
                FROM query_fragment f
                WHERE f.query_id = NEW.query_id AND f.current_state = 'Failed' AND f.error IS NOT NULL
                ORDER BY f.stop_timestamp, f.id
                LIMIT 1
            ) END
        )
    WHERE id = NEW.query_id;
END;

-- When the derive trigger above sets a query to Failed because one query_fragment failed,
-- set the desired state of every other non-terminal query_fragment of that query to Stopped.
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
        -- SQLite has no datetime type, and sea-orm stores DateTime<Utc> values as ISO-8601 text.
        -- The trigger writes the same encoding, so the MIN/MAX text comparison in the derive trigger stays chronological.
        -- NOTE: CURRENT_TIMESTAMP does not compare correctly against the timestamps that the application writes:
        -- `2026-02-04 18:58:35` sorts before `2026-02-04T18:58:34+00:00`, because a space sorts before `T`.
        stop_timestamp = strftime('%Y-%m-%dT%H:%M:%f', 'now') || '+00:00',
        -- The serde encoding of the model's Transport fragment error.
        -- The model finds the query_fragments that this trigger failed by this message, so change both together.
        error = json_object('Transport', json_object('msg', 'worker removed'))
    WHERE host_addr = NEW.host_addr
    AND current_state NOT IN ('Completed', 'Stopped', 'Failed');
END;

-- A dropped query is deleted once none of its fragments can run any more,
-- and the foreign keys delete its fragments, its links, and the sources and sinks that it owns.
-- The query state is not enough: a query is Failed while its other fragments are still being stopped.
-- The trigger fires when the query is marked dropped, so a query that has already terminated is deleted immediately.
-- It also fires on every fragment state change, through the derive trigger above.
CREATE TRIGGER IF NOT EXISTS delete_dropped_query_on_termination
    AFTER UPDATE OF state, dropped ON query
    WHEN NEW.dropped
    AND NOT EXISTS (
        SELECT 1 FROM query_fragment
        WHERE query_id = NEW.id
        AND current_state NOT IN ('Completed', 'Stopped', 'Failed')
    )
BEGIN
    DELETE FROM query WHERE id = NEW.id;
END;

-- Reject the drop of a physical source while a non-terminal query still references it,
-- because the query would otherwise keep running with a dangling source id.
CREATE TRIGGER IF NOT EXISTS prevent_physical_source_drop_with_active_queries
    BEFORE DELETE ON physical_source
    WHEN EXISTS (
        SELECT 1 FROM query_source qs
        JOIN query q ON q.id = qs.query_id
        WHERE qs.source_id = OLD.id
        AND q.state NOT IN ('Completed', 'Stopped', 'Failed')
    )
BEGIN
    SELECT RAISE(ABORT, 'Cannot drop physical source: non-terminal queries still reference it');
END;

-- Sink counterpart of the physical-source drop guard above.
CREATE TRIGGER IF NOT EXISTS prevent_sink_drop_with_active_queries
    BEFORE DELETE ON sink
    WHEN EXISTS (
        SELECT 1 FROM query_sink qs
        JOIN query q ON q.id = qs.query_id
        WHERE qs.sink_id = OLD.id
        AND q.state NOT IN ('Completed', 'Stopped', 'Failed')
    )
BEGIN
    SELECT RAISE(ABORT, 'Cannot drop sink: non-terminal queries still reference it');
END;
