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

CREATE TRIGGER IF NOT EXISTS acquire_worker_capacity
    AFTER INSERT ON fragment
BEGIN
    UPDATE worker
    SET max_operators = max_operators - NEW.num_operators
    WHERE host_addr = NEW.host_addr
    AND max_operators IS NOT NULL;
END;

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
                    json_extract(f.error, '$.WorkerInternal.msg'),
                    json_extract(f.error, '$.WorkerCommunication.msg')
                )
            )
            FROM fragment f
            WHERE f.query_id = NEW.query_id AND f.error IS NOT NULL
        )
    WHERE id = NEW.query_id
    AND (SELECT state FROM query WHERE id = NEW.query_id) = 'Failed';
END;

CREATE TRIGGER IF NOT EXISTS prevent_worker_drop_with_active_fragments
    BEFORE UPDATE OF desired_state ON worker
    WHEN NEW.desired_state = 'Removed' AND OLD.desired_state != 'Removed'
BEGIN
    SELECT CASE
        WHEN EXISTS (
            SELECT 1 FROM fragment
            WHERE host_addr = NEW.host_addr
            AND current_state NOT IN ('Completed', 'Stopped', 'Failed')
        ) THEN
            RAISE(ABORT, 'Cannot drop worker: non-terminal fragments still reference it')
    END;
END;

CREATE TRIGGER IF NOT EXISTS cleanup_orphaned_source
    AFTER DELETE ON query_source
BEGIN
    DELETE FROM physical_source
    WHERE kind != 'Shared'
      AND id = OLD.source_id
      AND NOT EXISTS (SELECT 1 FROM query_source WHERE source_id = OLD.source_id);
END;

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

CREATE TRIGGER IF NOT EXISTS cleanup_orphaned_sink
    AFTER DELETE ON query_sink
BEGIN
    DELETE FROM sink
    WHERE kind != 'Shared'
      AND id = OLD.sink_id
      AND NOT EXISTS (SELECT 1 FROM query_sink WHERE sink_id = OLD.sink_id);
END;
