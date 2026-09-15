SELECT
  "ts",
  "p_absent_A1", "empty_A1",
  "p_absent_A2", "empty_A2",
  "p_absent_A3", "empty_A3",
  "p_absent_B1", "empty_B1",
  "p_absent_B2", "empty_B2",
  "p_absent_B3", "empty_B3",
  "p_absent_C1", "empty_C1",
  "p_absent_C2", "empty_C2",
  "p_absent_C3", "empty_C3",
  ("empty_A1" + "empty_A2" + "empty_A3" + "empty_B1" + "empty_B2" + "empty_B3" + "empty_C1" + "empty_C2" + "empty_C3") AS "num_empty"
FROM
(
  SELECT
    "ts",
    "p_absent_A1", Conditional("p_absent_A1" > FLOAT32(0.85), UINT64(1), UINT64(0)) AS "empty_A1",
    "p_absent_A2", Conditional("p_absent_A2" > FLOAT32(0.85), UINT64(1), UINT64(0)) AS "empty_A2",
    "p_absent_A3", Conditional("p_absent_A3" > FLOAT32(0.85), UINT64(1), UINT64(0)) AS "empty_A3",
    "p_absent_B1", Conditional("p_absent_B1" > FLOAT32(0.85), UINT64(1), UINT64(0)) AS "empty_B1",
    "p_absent_B2", Conditional("p_absent_B2" > FLOAT32(0.85), UINT64(1), UINT64(0)) AS "empty_B2",
    "p_absent_B3", Conditional("p_absent_B3" > FLOAT32(0.85), UINT64(1), UINT64(0)) AS "empty_B3",
    "p_absent_C1", Conditional("p_absent_C1" > FLOAT32(0.85), UINT64(1), UINT64(0)) AS "empty_C1",
    "p_absent_C2", Conditional("p_absent_C2" > FLOAT32(0.85), UINT64(1), UINT64(0)) AS "empty_C2",
    "p_absent_C3", Conditional("p_absent_C3" > FLOAT32(0.85), UINT64(1), UINT64(0)) AS "empty_C3"
  FROM
  (
    SELECT
      "ts",
      FT_LOG_SCORE("ts", VARSIZED('A1'), "p_absent_A1") AS "p_absent_A1",
      FT_LOG_SCORE("ts", VARSIZED('A2'), "p_absent_A2") AS "p_absent_A2",
      FT_LOG_SCORE("ts", VARSIZED('A3'), "p_absent_A3") AS "p_absent_A3",
      FT_LOG_SCORE("ts", VARSIZED('B1'), "p_absent_B1") AS "p_absent_B1",
      FT_LOG_SCORE("ts", VARSIZED('B2'), "p_absent_B2") AS "p_absent_B2",
      FT_LOG_SCORE("ts", VARSIZED('B3'), "p_absent_B3") AS "p_absent_B3",
      FT_LOG_SCORE("ts", VARSIZED('C1'), "p_absent_C1") AS "p_absent_C1",
      FT_LOG_SCORE("ts", VARSIZED('C2'), "p_absent_C2") AS "p_absent_C2",
      FT_LOG_SCORE("ts", VARSIZED('C3'), "p_absent_C3") AS "p_absent_C3"
    FROM
      MODEL_INFERENCE(BUCKET_SLOT_CNN_9,
        (SELECT
           "camTs" AS "ts",
           FT_PREPROCESS_ALL_SLOTS("data") AS "pixels9"
         FROM
           (SELECT
              CASTTOUNIXTS("ts") AS "camTs",
              "data",
              1 AS "camKey"
            FROM SSC_CAM)
           INNER JOIN
           (SELECT
              "alignStart",
              "allAligned",
              1 AS "alignKey"
            FROM
              (SELECT
                 start AS "alignStart",
                 MIN(Conditional(
                   ABS("pan" - FLOAT64(0.988990783691406)) > FLOAT64(0.01)
                   OR ABS("tilt" - FLOAT64(-0.252666652202606)) > FLOAT64(0.01),
                   UINT64(0),
                   UINT64(1))) AS "allAligned"
               FROM
                 (SELECT CASTTOUNIXTS("ts") AS "ptuTs", "pan", "tilt" FROM PTU_POS_GATE)
               WINDOW TUMBLING("ptuTs", SIZE 500 MS)))
           ON "camKey" = "alignKey"
           WINDOW TUMBLING("camTs", "alignStart", SIZE 500 MS)
           WHERE "allAligned" = UINT64(1)))
  )
)
INTO
  SSC_SLOT_ANOMALY_SINK
;

SELECT
  "ts",
  "pan",
  "tilt",
  Conditional(
    ABS("pan" - FLOAT64(0.988990783691406)) > FLOAT64(0.01)
    OR ABS("tilt" - FLOAT64(-0.252666652202606)) > FLOAT64(0.01),
    VARSIZED('SSC_NOT_POINTING_AT_HBW'),
    VARSIZED('SSC_ALIGNED'))
  AS "alignment"
FROM
  (SELECT CASTTOUNIXTS("ts") AS "ts", "pan", "tilt" FROM PTU_POS_PRINT)
INTO
  PTU_ALIGNMENT_SINK
;
