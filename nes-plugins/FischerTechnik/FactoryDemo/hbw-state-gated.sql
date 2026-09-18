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
           (SELECT CASTTOUNIXTS("ts") AS "camTs", "data", 1 AS "camKey" FROM SSC_CAM)
           INNER JOIN
           (SELECT CASTTOUNIXTS("ts") AS "ptuTs", "pan", "tilt", 1 AS "ptuKey" FROM PTU_POS_GATE)
           ON "camKey" = "ptuKey"
           WINDOW TUMBLING("camTs", "ptuTs", SIZE 2500 MS)
           WHERE
             ABS("pan" - FLOAT64(0.9842201471328735)) <= FLOAT64(0.003)
             AND ABS("tilt" - FLOAT64(-0.2473333477973938)) <= FLOAT64(0.003)))
  )
)
INTO
  HBW_STATE_SINK
;

