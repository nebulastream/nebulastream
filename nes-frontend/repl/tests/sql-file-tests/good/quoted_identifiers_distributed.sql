CREATE WORKER 'worker-node:8080' SET ('worker-node:9090' AS DATA);
CREATE LOGICAL SOURCE "quotedSource" ("mixedValue" UINT64, "A" UINT64);
CREATE PHYSICAL SOURCE FOR "quotedSource" TYPE Generator SET(
    'ALL' AS "SOURCE"."STOP_GENERATOR_WHEN_SEQUENCE_FINISHES",
    'CSV' AS "INPUT_FORMATTER"."TYPE",
    'worker-node:8080' AS "SOURCE"."HOST",
    'SEQUENCE UINT64 0 10000000 1, SEQUENCE UINT64 0 10000000 1' AS "SOURCE"."GENERATOR_SCHEMA");
CREATE SINK "quotedSink" ("projectedValue" UINT64, A UINT64) TYPE Void SET('worker-node:8080' AS "SINK"."HOST");
SELECT "mixedValue" AS "projectedValue", A FROM "quotedSource" INTO "quotedSink";
