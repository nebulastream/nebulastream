SELECT
  "ts",
  "slot",
  "p_absent"
FROM
(
  SELECT
    "ts",
    VARSIZED('A1') AS "slot",
    "p_absent"
  FROM
    MODEL_INFERENCE(BUCKET_SLOT_CNN,
      (SELECT
         "camTs" AS "ts",
         FT_PREPROCESS_SLOT("data", CASTTOTYPE(86 AS UINT32), CASTTOTYPE(10 AS UINT32), CASTTOTYPE(55 AS UINT32), CASTTOTYPE(51 AS UINT32)) AS "pixels"
       FROM
         (SELECT
            CASTTOUNIXTS("ts") AS "camTs",
            "data",
            1 AS "camKey"
          FROM SSC_CAM_A1)
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
               (SELECT CASTTOUNIXTS("ts") AS "ptuTs", "pan", "tilt" FROM PTU_POS_A1)
             WINDOW TUMBLING("ptuTs", SIZE 2 SEC)))
         ON "camKey" = "alignKey"
         WINDOW TUMBLING("camTs", "alignStart", SIZE 2 SEC)
         WHERE "allAligned" = UINT64(1)))
  UNION
  SELECT
    "ts",
    VARSIZED('A2') AS "slot",
    "p_absent"
  FROM
    MODEL_INFERENCE(BUCKET_SLOT_CNN,
      (SELECT
         "camTs" AS "ts",
         FT_PREPROCESS_SLOT("data", CASTTOTYPE(155 AS UINT32), CASTTOTYPE(20 AS UINT32), CASTTOTYPE(57 AS UINT32), CASTTOTYPE(50 AS UINT32)) AS "pixels"
       FROM
         (SELECT
            CASTTOUNIXTS("ts") AS "camTs",
            "data",
            1 AS "camKey"
          FROM SSC_CAM_A2)
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
               (SELECT CASTTOUNIXTS("ts") AS "ptuTs", "pan", "tilt" FROM PTU_POS_A2)
             WINDOW TUMBLING("ptuTs", SIZE 2 SEC)))
         ON "camKey" = "alignKey"
         WINDOW TUMBLING("camTs", "alignStart", SIZE 2 SEC)
         WHERE "allAligned" = UINT64(1)))
  UNION
  SELECT
    "ts",
    VARSIZED('A3') AS "slot",
    "p_absent"
  FROM
    MODEL_INFERENCE(BUCKET_SLOT_CNN,
      (SELECT
         "camTs" AS "ts",
         FT_PREPROCESS_SLOT("data", CASTTOTYPE(240 AS UINT32), CASTTOTYPE(30 AS UINT32), CASTTOTYPE(76 AS UINT32), CASTTOTYPE(55 AS UINT32)) AS "pixels"
       FROM
         (SELECT
            CASTTOUNIXTS("ts") AS "camTs",
            "data",
            1 AS "camKey"
          FROM SSC_CAM_A3)
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
               (SELECT CASTTOUNIXTS("ts") AS "ptuTs", "pan", "tilt" FROM PTU_POS_A3)
             WINDOW TUMBLING("ptuTs", SIZE 2 SEC)))
         ON "camKey" = "alignKey"
         WINDOW TUMBLING("camTs", "alignStart", SIZE 2 SEC)
         WHERE "allAligned" = UINT64(1)))
  UNION
  SELECT
    "ts",
    VARSIZED('B1') AS "slot",
    "p_absent"
  FROM
    MODEL_INFERENCE(BUCKET_SLOT_CNN,
      (SELECT
         "camTs" AS "ts",
         FT_PREPROCESS_SLOT("data", CASTTOTYPE(88 AS UINT32), CASTTOTYPE(63 AS UINT32), CASTTOTYPE(56 AS UINT32), CASTTOTYPE(40 AS UINT32)) AS "pixels"
       FROM
         (SELECT
            CASTTOUNIXTS("ts") AS "camTs",
            "data",
            1 AS "camKey"
          FROM SSC_CAM_B1)
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
               (SELECT CASTTOUNIXTS("ts") AS "ptuTs", "pan", "tilt" FROM PTU_POS_B1)
             WINDOW TUMBLING("ptuTs", SIZE 2 SEC)))
         ON "camKey" = "alignKey"
         WINDOW TUMBLING("camTs", "alignStart", SIZE 2 SEC)
         WHERE "allAligned" = UINT64(1)))
  UNION
  SELECT
    "ts",
    VARSIZED('B2') AS "slot",
    "p_absent"
  FROM
    MODEL_INFERENCE(BUCKET_SLOT_CNN,
      (SELECT
         "camTs" AS "ts",
         FT_PREPROCESS_SLOT("data", CASTTOTYPE(149 AS UINT32), CASTTOTYPE(73 AS UINT32), CASTTOTYPE(69 AS UINT32), CASTTOTYPE(52 AS UINT32)) AS "pixels"
       FROM
         (SELECT
            CASTTOUNIXTS("ts") AS "camTs",
            "data",
            1 AS "camKey"
          FROM SSC_CAM_B2)
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
               (SELECT CASTTOUNIXTS("ts") AS "ptuTs", "pan", "tilt" FROM PTU_POS_B2)
             WINDOW TUMBLING("ptuTs", SIZE 2 SEC)))
         ON "camKey" = "alignKey"
         WINDOW TUMBLING("camTs", "alignStart", SIZE 2 SEC)
         WHERE "allAligned" = UINT64(1)))
  UNION
  SELECT
    "ts",
    VARSIZED('B3') AS "slot",
    "p_absent"
  FROM
    MODEL_INFERENCE(BUCKET_SLOT_CNN,
      (SELECT
         "camTs" AS "ts",
         FT_PREPROCESS_SLOT("data", CASTTOTYPE(237 AS UINT32), CASTTOTYPE(88 AS UINT32), CASTTOTYPE(78 AS UINT32), CASTTOTYPE(62 AS UINT32)) AS "pixels"
       FROM
         (SELECT
            CASTTOUNIXTS("ts") AS "camTs",
            "data",
            1 AS "camKey"
          FROM SSC_CAM_B3)
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
               (SELECT CASTTOUNIXTS("ts") AS "ptuTs", "pan", "tilt" FROM PTU_POS_B3)
             WINDOW TUMBLING("ptuTs", SIZE 2 SEC)))
         ON "camKey" = "alignKey"
         WINDOW TUMBLING("camTs", "alignStart", SIZE 2 SEC)
         WHERE "allAligned" = UINT64(1)))
  UNION
  SELECT
    "ts",
    VARSIZED('C1') AS "slot",
    "p_absent"
  FROM
    MODEL_INFERENCE(BUCKET_SLOT_CNN,
      (SELECT
         "camTs" AS "ts",
         FT_PREPROCESS_SLOT("data", CASTTOTYPE(92 AS UINT32), CASTTOTYPE(107 AS UINT32), CASTTOTYPE(51 AS UINT32), CASTTOTYPE(51 AS UINT32)) AS "pixels"
       FROM
         (SELECT
            CASTTOUNIXTS("ts") AS "camTs",
            "data",
            1 AS "camKey"
          FROM SSC_CAM_C1)
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
               (SELECT CASTTOUNIXTS("ts") AS "ptuTs", "pan", "tilt" FROM PTU_POS_C1)
             WINDOW TUMBLING("ptuTs", SIZE 2 SEC)))
         ON "camKey" = "alignKey"
         WINDOW TUMBLING("camTs", "alignStart", SIZE 2 SEC)
         WHERE "allAligned" = UINT64(1)))
  UNION
  SELECT
    "ts",
    VARSIZED('C2') AS "slot",
    "p_absent"
  FROM
    MODEL_INFERENCE(BUCKET_SLOT_CNN,
      (SELECT
         "camTs" AS "ts",
         FT_PREPROCESS_SLOT("data", CASTTOTYPE(150 AS UINT32), CASTTOTYPE(128 AS UINT32), CASTTOTYPE(72 AS UINT32), CASTTOTYPE(50 AS UINT32)) AS "pixels"
       FROM
         (SELECT
            CASTTOUNIXTS("ts") AS "camTs",
            "data",
            1 AS "camKey"
          FROM SSC_CAM_C2)
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
               (SELECT CASTTOUNIXTS("ts") AS "ptuTs", "pan", "tilt" FROM PTU_POS_C2)
             WINDOW TUMBLING("ptuTs", SIZE 2 SEC)))
         ON "camKey" = "alignKey"
         WINDOW TUMBLING("camTs", "alignStart", SIZE 2 SEC)
         WHERE "allAligned" = UINT64(1)))
  UNION
  SELECT
    "ts",
    VARSIZED('C3') AS "slot",
    "p_absent"
  FROM
    MODEL_INFERENCE(BUCKET_SLOT_CNN,
      (SELECT
         "camTs" AS "ts",
         FT_PREPROCESS_SLOT("data", CASTTOTYPE(226 AS UINT32), CASTTOTYPE(153 AS UINT32), CASTTOTYPE(88 AS UINT32), CASTTOTYPE(53 AS UINT32)) AS "pixels"
       FROM
         (SELECT
            CASTTOUNIXTS("ts") AS "camTs",
            "data",
            1 AS "camKey"
          FROM SSC_CAM_C3)
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
               (SELECT CASTTOUNIXTS("ts") AS "ptuTs", "pan", "tilt" FROM PTU_POS_C3)
             WINDOW TUMBLING("ptuTs", SIZE 2 SEC)))
         ON "camKey" = "alignKey"
         WINDOW TUMBLING("camTs", "alignStart", SIZE 2 SEC)
         WHERE "allAligned" = UINT64(1)))
)
WHERE
  FT_LOG_SCORE("ts", "slot", "p_absent") > FLOAT32(0.85)
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
  (SELECT CASTTOUNIXTS("ts") AS "ts", "pan", "tilt" FROM PTU_POS)
INTO
  PTU_ALIGNMENT_SINK
;
