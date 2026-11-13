-- Reads the task events of a remote worker that was started with --enable_task_statistics.
CREATE WORKER 'worker-node:8080' SET ('worker-node:9090' AS DATA);

-- A Generator query, purely to give the engine something to do.
CREATE LOGICAL SOURCE endless(ts UINT64);
CREATE PHYSICAL SOURCE FOR endless TYPE Generator SET(
       'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
       'CSV' as INPUT_FORMATTER."TYPE",
       30000 AS "SOURCE".MAX_RUNTIME_MS,
       'worker-node:8080' AS "SOURCE"."HOST",
       'emit_rate 100' AS "SOURCE".GENERATOR_RATE_CONFIG,
       1 AS "SOURCE".SEED,
       'SEQUENCE UINT64 0 10000000 1' AS "SOURCE".GENERATOR_SCHEMA);

-- The engine's own task events. The source is placed on the worker whose events it reads, because the
-- feed only exists inside that worker's process.
CREATE LOGICAL SOURCE engineStats(
       event_type VARSIZED, ts_us UINT64, thread_id UINT64, query_id INT64,
       pipeline_id UINT64, task_id UINT64, tuples UINT64);
CREATE PHYSICAL SOURCE FOR engineStats TYPE EngineEvents SET(
       'CSV' as INPUT_FORMATTER."TYPE",
       'worker-node:8080' AS "SOURCE"."HOST");
CREATE SINK statsSink(
       event_type VARSIZED, ts_us UINT64, thread_id UINT64, query_id INT64,
       pipeline_id UINT64, task_id UINT64, tuples UINT64) TYPE File SET(
       'stats.csv' as "SINK".FILE_PATH,
       'CSV' as "SINK".OUTPUT_FORMAT,
       'worker-node:8080' AS "SINK"."HOST");

-- The observer goes first, so that it sees the load query start up.
SELECT event_type, ts_us, thread_id, query_id, pipeline_id, task_id, tuples
       FROM engineStats INTO statsSink;
SELECT TS FROM ENDLESS INTO Void('worker-node:8080' AS "SINK"."HOST");
