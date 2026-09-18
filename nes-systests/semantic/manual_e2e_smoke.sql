-- Manual smoke script for SEM_MAP DDL/EXPLAIN, exercised through nes-repl-embedded (plain
-- nes-repl has no worker, so EXPLAIN fails with "non-existing worker" — plan §4 M3 as-built note).
-- Not a systest: SEM_MAP(sentiment, ...) here points at a placeholder BASE_URL/FILE_PATH, so only
-- the EXPLAIN/DDL/error-shape lines are meaningful; the two non-EXPLAIN SEM_MAP lines will fail or
-- hang against a real endpoint. For a real end-to-end run against a live Ollama, see the M5
-- as-built note in docs/sem-map-implementation-plan.md §4 for the query shape and CLI flags used.
--
-- Usage: cat nes-systests/semantic/manual_e2e_smoke.sql | ./build-docker/nes-frontend/apps/nes-repl-embedded

CREATE LOGICAL SOURCE reviews (description VARSIZED NOT NULL, rating UINT32);
CREATE PHYSICAL SOURCE FOR reviews TYPE File SET (0 as "SOURCE".MAX_INFLIGHT_BUFFERS, '/dev/null' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE", '\n' AS INPUT_FORMATTER.TUPLE_DELIMITER, ',' AS INPUT_FORMATTER.FIELD_DELIMITER);
CREATE SINK out (description VARSIZED NOT NULL, rating UINT32, sentiment VARSIZED NOT NULL) TYPE File SET ('/dev/null' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SEMANTIC MODEL sentiment SET ('http://localhost:11434/v1' AS BASE_URL, 'gemma3:27b' AS MODEL, 'Classify' AS PROMPT) INPUT (description VARSIZED) OUTPUT (sentiment VARSIZED);
EXPLAIN SELECT *, SEM_MAP(sentiment, description) AS sentiment FROM reviews INTO out;
EXPLAIN SELECT *, SEM_MAP(nosuch, description) AS s FROM reviews INTO out;
EXPLAIN SELECT *, SEM_MAP(sentiment, missingcol) AS s FROM reviews INTO out;
EXPLAIN SELECT *, SEM_MAP(sentiment, rating) AS s FROM reviews INTO out;
EXPLAIN SELECT *, SEM_MAP(sentiment) AS s FROM reviews INTO out;
SELECT SEM_MAP(sentiment, description) FROM reviews;
DROP SEMANTIC MODEL WHERE NAME = 'sentiment';
SHOW SEMANTIC MODELS;
