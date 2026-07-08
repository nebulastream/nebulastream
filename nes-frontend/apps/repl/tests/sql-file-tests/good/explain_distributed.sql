CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL, value UINT64 NOT NULL, timestamp UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR stream TYPE Generator SET(
'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
'CSV' as INPUT_FORMATTER."TYPE",
'source-node:8080' AS "SOURCE"."HOST",
'SEQUENCE UINT32 0 10000000 1, SEQUENCE UINT64 0 10000000 1, SEQUENCE UINT64 0 10000000 1' AS "SOURCE".GENERATOR_SCHEMA);


CREATE LOGICAL SOURCE stream2(id2 UINT64 NOT NULL, value2 UINT64 NOT NULL, timestamp2 UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR stream2 TYPE Generator SET(
'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
'CSV' as INPUT_FORMATTER."TYPE",
'source-node:8080' AS "SOURCE"."HOST",
'SEQUENCE UINT32 0 10000000 1, SEQUENCE UINT64 0 10000000 1, SEQUENCE UINT64 0 10000000 1, SEQUENCE UINT64 0 10000000 1' AS "SOURCE".GENERATOR_SCHEMA);


CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA);
CREATE WORKER 'source-node:8080' SET ('source-node:9090' AS DATA, 'sink-node:8080' AS "DOWNSTREAM", 2 AS "CAPACITY");

EXPLAIN (LOGICAL) FORMAT TEXT SELECT start, end, id, value, timestamp, id2, value2, timestamp2 FROM ( SELECT * FROM (SELECT * FROM stream) INNER JOIN (SELECT * FROM stream2) ON (id = id2) WINDOW TUMBLING (timestamp, timestamp2, size 1 sec)) INTO Void('sink-node:8080' AS "SINK"."HOST");
EXPLAIN (OPTIMIZED) FORMAT TEXT SELECT start, end, id, value, timestamp, id2, value2, timestamp2 FROM ( SELECT * FROM (SELECT * FROM stream) INNER JOIN (SELECT * FROM stream2) ON (id = id2) WINDOW TUMBLING (timestamp, timestamp2, size 1 sec)) INTO Void('sink-node:8080' AS "SINK"."HOST");
EXPLAIN (DISTRIBUTED) FORMAT TEXT SELECT start, end, id, value, timestamp, id2, value2, timestamp2 FROM ( SELECT * FROM (SELECT * FROM stream) INNER JOIN (SELECT * FROM stream2) ON (id = id2) WINDOW TUMBLING (timestamp, timestamp2, size 1 sec)) INTO Void('sink-node:8080' AS "SINK"."HOST");

EXPLAIN (LOGICAL) FORMAT VISUAL SELECT start, end, id, value, timestamp, id2, value2, timestamp2 FROM ( SELECT * FROM (SELECT * FROM stream) INNER JOIN (SELECT * FROM stream2) ON (id = id2) WINDOW TUMBLING (timestamp, timestamp2, size 1 sec)) INTO Void('sink-node:8080' AS "SINK"."HOST");
EXPLAIN (OPTIMIZED) FORMAT VISUAL SELECT start, end, id, value, timestamp, id2, value2, timestamp2 FROM ( SELECT * FROM (SELECT * FROM stream) INNER JOIN (SELECT * FROM stream2) ON (id = id2) WINDOW TUMBLING (timestamp, timestamp2, size 1 sec)) INTO Void('sink-node:8080' AS "SINK"."HOST");
EXPLAIN (DISTRIBUTED) FORMAT VISUAL SELECT start, end, id, value, timestamp, id2, value2, timestamp2 FROM ( SELECT * FROM (SELECT * FROM stream) INNER JOIN (SELECT * FROM stream2) ON (id = id2) WINDOW TUMBLING (timestamp, timestamp2, size 1 sec)) INTO Void('sink-node:8080' AS "SINK"."HOST");
