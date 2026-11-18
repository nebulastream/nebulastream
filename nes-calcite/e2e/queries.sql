-- Each ANTLR query (sink *_antlr) has a Calcite twin (anonymous File sink *_calcite.csv, which takes the query's schema);
-- run.sh checks that both produce the same rows. Calcite types AVG of integers as an integer, hence the CAST below.
CREATE LOGICAL SOURCE events(id UINT64 NOT NULL, val UINT64 NOT NULL, ts UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR events TYPE Generator SET(
    'ALL' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
    'CSV' AS INPUT_FORMATTER."TYPE",
    1 AS "SOURCE".SEED,
    'SEQUENCE UINT64 1 5 1, SEQUENCE UINT64 10 50 10, SEQUENCE UINT64 0 20000 10' AS "SOURCE".GENERATOR_SCHEMA);
CREATE LOGICAL SOURCE events2(id UINT64 NOT NULL, val UINT64 NOT NULL, ts UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR events2 TYPE Generator SET(
    'ALL' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
    'CSV' AS INPUT_FORMATTER."TYPE",
    2 AS "SOURCE".SEED,
    'SEQUENCE UINT64 1 7 1, SEQUENCE UINT64 1 9 2, SEQUENCE UINT64 0 5000 10' AS "SOURCE".GENERATOR_SCHEMA);

CREATE SINK filter_antlr(id UINT64 NOT NULL, v2 UINT64 NOT NULL) TYPE File SET('filter_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK union_antlr(id UINT64 NOT NULL, val UINT64 NOT NULL) TYPE File SET('union_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK tumble_antlr(start UINT64 NOT NULL, end UINT64 NOT NULL, id UINT64 NOT NULL, s UINT64 NOT NULL) TYPE File SET('tumble_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK slide_antlr(start UINT64 NOT NULL, end UINT64 NOT NULL, c UINT64 NOT NULL, a FLOAT64 NOT NULL) TYPE File SET('slide_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK expr_antlr(start UINT64 NOT NULL, end UINT64 NOT NULL, m UINT64 NOT NULL) TYPE File SET('expr_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);

SELECT id, val * 2 AS v2 FROM events WHERE val > 20 INTO filter_antlr;
CALCITE SELECT id, val * 2 AS v2 FROM events WHERE val > 20 INTO File('filter_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT * FROM (SELECT id, val FROM events UNION SELECT id, val FROM events2) INTO union_antlr;
CALCITE SELECT id, val FROM events UNION ALL SELECT id, val FROM events2 INTO File('union_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT start, end, id, SUM(val) AS s FROM events GROUP BY id WINDOW TUMBLING(ts, SIZE 1 SEC) INTO tumble_antlr;
CALCITE SELECT window_start AS "START", window_end AS "END", id, SUM(val) AS s
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND))
GROUP BY window_start, window_end, id INTO File('tumble_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT start, end, COUNT(*) AS c, AVG(val) AS a FROM events WINDOW SLIDING(ts, SIZE 2 SEC, ADVANCE BY 500 MS) INTO slide_antlr;
CALCITE SELECT window_start AS "START", window_end AS "END", COUNT(*) AS c, AVG(CAST(val AS DOUBLE)) AS a
FROM TABLE(HOP(TABLE events, DESCRIPTOR(ts), INTERVAL '0.5' SECOND, INTERVAL '2' SECOND))
GROUP BY window_start, window_end INTO File('slide_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT start, end, MAX(val * 2) AS m FROM events WHERE id > 2 WINDOW TUMBLING(ts, SIZE 1 SEC) INTO expr_antlr;
CALCITE SELECT window_start AS "START", window_end AS "END", MAX(val * 2) AS m
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND))
WHERE id > 2 GROUP BY window_start, window_end INTO File('expr_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

-- Pushdown: for Calcite plans it is done by Calcite, the NebulaStream pushdown rules are skipped.
CREATE SINK keyfilter_antlr(start UINT64 NOT NULL, end UINT64 NOT NULL, id UINT64 NOT NULL, s UINT64 NOT NULL) TYPE File SET('keyfilter_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK unionfilter_antlr(id UINT64 NOT NULL, val UINT64 NOT NULL) TYPE File SET('unionfilter_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK unused_antlr(start UINT64 NOT NULL, end UINT64 NOT NULL, s UINT64 NOT NULL) TYPE File SET('unused_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);

SELECT * FROM (SELECT start, end, id, SUM(val) AS s FROM events GROUP BY id WINDOW TUMBLING(ts, SIZE 1 SEC))
WHERE id > 2 AND s > 100 INTO keyfilter_antlr;
CALCITE SELECT * FROM (SELECT window_start AS "START", window_end AS "END", id, SUM(val) AS s
    FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND)) GROUP BY window_start, window_end, id)
WHERE id > 2 AND s > 100 INTO File('keyfilter_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT * FROM (SELECT id, val FROM events UNION SELECT id, val FROM events2) WHERE val > 5 INTO unionfilter_antlr;
CALCITE SELECT * FROM (SELECT id, val FROM events UNION ALL SELECT id, val FROM events2) WHERE val > 5 INTO File('unionfilter_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT start, end, s FROM (SELECT start, end, SUM(val) AS s, COUNT(*) AS c FROM events WINDOW TUMBLING(ts, SIZE 1 SEC))
INTO unused_antlr;
CALCITE SELECT "START", "END", s FROM (SELECT window_start AS "START", window_end AS "END", SUM(val) AS s, COUNT(*) AS c
    FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND)) GROUP BY window_start, window_end)
INTO File('unused_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

-- Windowed joins. Calcite picks the implementation: hash join for field equalities, nested-loop join otherwise.
CREATE SINK join_antlr(start UINT64 NOT NULL, end UINT64 NOT NULL, id UINT64 NOT NULL, val UINT64 NOT NULL, ts UINT64 NOT NULL, id2 UINT64 NOT NULL, val2 UINT64 NOT NULL, ts2 UINT64 NOT NULL) TYPE File SET('join_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK theta_antlr(start UINT64 NOT NULL, end UINT64 NOT NULL, id UINT64 NOT NULL, val UINT64 NOT NULL, id2 UINT64 NOT NULL, val2 UINT64 NOT NULL) TYPE File SET('theta_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK leftjoin_antlr(start UINT64 NOT NULL, end UINT64 NOT NULL, id UINT64 NOT NULL, val UINT64 NOT NULL, id2 UINT64, val2 UINT64) TYPE File SET('leftjoin_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);

SELECT * FROM (SELECT * FROM events) INNER JOIN (SELECT id AS id2, val AS val2, ts AS ts2 FROM events2)
ON id = id2 WINDOW TUMBLING(ts, ts2, SIZE 1 SEC) INTO join_antlr;
CALCITE SELECT l.window_start AS "START", l.window_end AS "END", l.id, l.val, l.ts, r.id AS id2, r.val AS val2, r.ts AS ts2
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND)) l
JOIN TABLE(TUMBLE(TABLE events2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) r
ON l.window_start = r.window_start AND l.window_end = r.window_end AND l.id = r.id INTO File('join_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT start, end, id, val, id2, val2 FROM (SELECT id, val, ts FROM events) INNER JOIN (SELECT id AS id2, val AS val2, ts AS ts2 FROM events2)
ON id < id2 AND val2 > 7 WINDOW TUMBLING(ts, ts2, SIZE 1 SEC) INTO theta_antlr;
CALCITE SELECT l.window_start AS "START", l.window_end AS "END", l.id, l.val, r.id AS id2, r.val AS val2
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND)) l
JOIN TABLE(TUMBLE(TABLE events2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) r
ON l.window_start = r.window_start AND l.window_end = r.window_end AND l.id < r.id AND r.val > 7 INTO File('theta_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT start, end, id, val, id2, val2 FROM (SELECT id, val, ts FROM events) LEFT JOIN (SELECT id AS id2, val AS val2, ts AS ts2 FROM events2)
ON id = id2 WINDOW TUMBLING(ts, ts2, SIZE 1 SEC) INTO leftjoin_antlr;
CALCITE SELECT l.window_start AS "START", l.window_end AS "END", l.id, l.val, r.id AS id2, r.val AS val2
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND)) l
LEFT JOIN TABLE(TUMBLE(TABLE events2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) r
ON l.window_start = r.window_start AND l.window_end = r.window_end AND l.id = r.id INTO File('leftjoin_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

-- Subqueries and LATERAL. Calcite decorrelates them into windowed joins; IN/EXISTS join with a per-window
-- deduplication of the subquery, NOT IN/NOT EXISTS keep the unmatched rows of a left join. The ANTLR twins spell that out.
-- The anti-join twins write (id2 IS NULL OR id = 0) instead of id2 IS NULL: the PredicatePushdownRule of NebulaStream ignores the
-- join type and would push id2 IS NULL below the left join, into its right input, which changes the result.
CREATE SINK in_antlr(start UINT64 NOT NULL, id UINT64 NOT NULL, val UINT64 NOT NULL, ts UINT64 NOT NULL) TYPE File SET('in_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK exists_antlr(start UINT64 NOT NULL, id UINT64 NOT NULL, val UINT64 NOT NULL, ts UINT64 NOT NULL) TYPE File SET('exists_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK notexists_antlr(start UINT64 NOT NULL, id UINT64 NOT NULL, val UINT64 NOT NULL, ts UINT64 NOT NULL) TYPE File SET('notexists_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK notin_antlr(start UINT64 NOT NULL, id UINT64 NOT NULL, val UINT64 NOT NULL, ts UINT64 NOT NULL) TYPE File SET('notin_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK lateral_antlr(start UINT64 NOT NULL, end UINT64 NOT NULL, id UINT64 NOT NULL, val UINT64 NOT NULL, id2 UINT64 NOT NULL, val2 UINT64 NOT NULL) TYPE File SET('lateral_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK joinagg_antlr(start UINT64 NOT NULL, end UINT64 NOT NULL, id UINT64 NOT NULL, s UINT64 NOT NULL, s2 UINT64 NOT NULL) TYPE File SET('joinagg_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);

SELECT start, id, val, ts FROM (SELECT * FROM events) INNER JOIN
    (SELECT start AS s2, id AS id2 FROM (SELECT start, end, id, COUNT(*) AS c FROM events2 GROUP BY id WINDOW TUMBLING(ts, SIZE 1 SEC)))
ON id = id2 WINDOW TUMBLING(ts, s2, SIZE 1 SEC) INTO in_antlr;
CALCITE SELECT l.window_start AS "START", l.id, l.val, l.ts FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND)) l
WHERE l.id IN (SELECT r.id FROM TABLE(TUMBLE(TABLE events2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) r WHERE r.window_start = l.window_start)
INTO File('in_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT start, id, val, ts FROM (SELECT * FROM events) INNER JOIN
    (SELECT start AS s2, id AS id2 FROM (SELECT start, end, id, COUNT(*) AS c FROM events2 GROUP BY id WINDOW TUMBLING(ts, SIZE 1 SEC)))
ON id = id2 WINDOW TUMBLING(ts, s2, SIZE 1 SEC) INTO exists_antlr;
CALCITE SELECT l.window_start AS "START", l.id, l.val, l.ts FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND)) l
WHERE EXISTS (SELECT 1 FROM TABLE(TUMBLE(TABLE events2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) r WHERE r.window_start = l.window_start AND r.id = l.id)
INTO File('exists_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT start, id, val, ts FROM (SELECT * FROM (SELECT * FROM events) LEFT JOIN
    (SELECT start AS s2, id AS id2 FROM (SELECT start, end, id, COUNT(*) AS c FROM events2 GROUP BY id WINDOW TUMBLING(ts, SIZE 1 SEC)))
ON id = id2 WINDOW TUMBLING(ts, s2, SIZE 1 SEC)) WHERE (id2 IS NULL OR id = 0) INTO notexists_antlr;
CALCITE SELECT l.window_start AS "START", l.id, l.val, l.ts FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND)) l
WHERE NOT EXISTS (SELECT 1 FROM TABLE(TUMBLE(TABLE events2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) r WHERE r.window_start = l.window_start AND r.id = l.id)
INTO File('notexists_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT start, id, val, ts FROM (SELECT * FROM (SELECT * FROM events) LEFT JOIN
    (SELECT start AS s2, id AS id2 FROM (SELECT start, end, id, COUNT(*) AS c FROM events2 GROUP BY id WINDOW TUMBLING(ts, SIZE 1 SEC)))
ON id = id2 WINDOW TUMBLING(ts, s2, SIZE 1 SEC)) WHERE (id2 IS NULL OR id = 0) INTO notin_antlr;
CALCITE SELECT l.window_start AS "START", l.id, l.val, l.ts FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND)) l
WHERE l.id NOT IN (SELECT r.id FROM TABLE(TUMBLE(TABLE events2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) r WHERE r.window_start = l.window_start)
INTO File('notin_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT start, end, id, val, id2, val2 FROM (SELECT id, val, ts FROM events) INNER JOIN (SELECT id AS id2, val AS val2, ts AS ts2 FROM events2)
ON id = id2 WINDOW TUMBLING(ts, ts2, SIZE 1 SEC) INTO lateral_antlr;
CALCITE SELECT l.window_start AS "START", l.window_end AS "END", l.id, l.val, x.id2, x.val2
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND)) l,
LATERAL (SELECT r.id AS id2, r.val AS val2 FROM TABLE(TUMBLE(TABLE events2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) r
         WHERE r.window_start = l.window_start AND r.window_end = l.window_end AND r.id = l.id) x
INTO File('lateral_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT start, end, id, s, s2 FROM
    (SELECT start AS ls, id, SUM(val) AS s FROM events GROUP BY id WINDOW TUMBLING(ts, SIZE 1 SEC)) INNER JOIN
    (SELECT start AS rs, id AS id2, SUM(val) AS s2 FROM events2 GROUP BY id WINDOW TUMBLING(ts, SIZE 1 SEC))
ON id = id2 WINDOW TUMBLING(ls, rs, SIZE 1 SEC) INTO joinagg_antlr;
CALCITE SELECT a.window_start AS "START", a.window_end AS "END", a.id, a.s, b.s AS s2
FROM (SELECT window_start, window_end, id, SUM(val) AS s FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND)) GROUP BY window_start, window_end, id) a
JOIN (SELECT window_start, window_end, id, SUM(val) AS s FROM TABLE(TUMBLE(TABLE events2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) GROUP BY window_start, window_end, id) b
ON a.window_start = b.window_start AND a.id = b.id INTO File('joinagg_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

-- A CTE used twice: Calcite inlines it at both uses.
CREATE SINK cte_antlr(id UINT64 NOT NULL, val UINT64 NOT NULL) TYPE File SET('cte_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT * FROM (SELECT id, val FROM events WHERE val > 20 UNION SELECT id, val FROM events WHERE val > 20) INTO cte_antlr;
CALCITE WITH big AS (SELECT id, val FROM events WHERE val > 20) SELECT id, val FROM big UNION ALL SELECT id, val FROM big INTO File('cte_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

-- Hash join with a residual condition: Calcite hashes on id = id2 and passes ts > ts2 as residual. The ANTLR twins
-- run as nested-loop joins over the whole condition.
CREATE SINK residual_antlr(start UINT64 NOT NULL, end UINT64 NOT NULL, id UINT64 NOT NULL, val UINT64 NOT NULL, id2 UINT64 NOT NULL, val2 UINT64 NOT NULL) TYPE File SET('residual_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK leftresidual_antlr(start UINT64 NOT NULL, end UINT64 NOT NULL, id UINT64 NOT NULL, val UINT64 NOT NULL, id2 UINT64, val2 UINT64) TYPE File SET('leftresidual_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);

SELECT start, end, id, val, id2, val2 FROM (SELECT id, val, ts FROM events) INNER JOIN (SELECT id AS id2, val AS val2, ts AS ts2 FROM events2)
ON id = id2 AND ts > ts2 WINDOW TUMBLING(ts, ts2, SIZE 1 SEC) INTO residual_antlr;
CALCITE SELECT l.window_start AS "START", l.window_end AS "END", l.id, l.val, r.id AS id2, r.val AS val2
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND)) l
JOIN TABLE(TUMBLE(TABLE events2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) r
ON l.window_start = r.window_start AND l.window_end = r.window_end AND l.id = r.id AND l.ts > r.ts INTO File('residual_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT start, end, id, val, id2, val2 FROM (SELECT id, val, ts FROM events) LEFT JOIN (SELECT id AS id2, val AS val2, ts AS ts2 FROM events2)
ON id = id2 AND ts > ts2 WINDOW TUMBLING(ts, ts2, SIZE 1 SEC) INTO leftresidual_antlr;
CALCITE SELECT l.window_start AS "START", l.window_end AS "END", l.id, l.val, r.id AS id2, r.val AS val2
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND)) l
LEFT JOIN TABLE(TUMBLE(TABLE events2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) r
ON l.window_start = r.window_start AND l.window_end = r.window_end AND l.id = r.id AND l.ts > r.ts INTO File('leftresidual_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

-- Finite inputs (run.sh sets NES_CALCITE_FINITE_INPUTS=1): joins and aggregations without a window get one window over the
-- whole input that fires at the end of the stream. The ANTLR twins use a huge ingestion-time window for the same effect.
CREATE SINK globalagg_antlr(id UINT64 NOT NULL, c UINT64 NOT NULL, s UINT64 NOT NULL) TYPE File SET('globalagg_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK globalin_antlr(id UINT64 NOT NULL, val UINT64 NOT NULL, ts UINT64 NOT NULL) TYPE File SET('globalin_antlr.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);

SELECT id, c, s FROM (SELECT start, end, id, COUNT(*) AS c, SUM(val) AS s FROM events GROUP BY id WINDOW TUMBLING(SIZE 1000000 HOUR))
INTO globalagg_antlr;
CALCITE SELECT id, COUNT(*) AS c, SUM(val) AS s FROM events GROUP BY id INTO File('globalagg_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);

SELECT id, val, ts FROM (SELECT * FROM events) INNER JOIN
    (SELECT id AS id2 FROM (SELECT start, end, id, COUNT(*) AS c FROM events2 WHERE val > 5 GROUP BY id WINDOW TUMBLING(SIZE 1000000 HOUR)))
ON id = id2 WINDOW TUMBLING(SIZE 1000000 HOUR) INTO globalin_antlr;
CALCITE SELECT id, val, ts FROM events WHERE id IN (SELECT id FROM events2 WHERE val > 5) INTO File('globalin_calcite.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'localhost:8080' AS "SINK".HOST);
