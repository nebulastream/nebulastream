SELECT
  "camTs" AS "ts",
  VARSIZED('RED') AS "alert"
FROM
  (SELECT
     CASTTOUNIXTS("ts") AS "camTs",
     "fault_score" AS "faultScore",
     1 AS "camKey"
   FROM
     MODEL_INFERENCE(FACTORY_FAULT_MODEL, (SELECT "ts", FROM_BASE64("data") AS "pixels" FROM CAM)))
INNER JOIN
  (SELECT
     "stateStart",
     "anyActive",
     1 AS "stateKey"
   FROM
     (SELECT
        start AS "stateStart",
        MAX(CASTTOTYPE("active" AS UINT64)) AS "anyActive"
      FROM
        (SELECT CASTTOUNIXTS("ts") AS "stateTs", "active" FROM STATION_STATE)
      WINDOW TUMBLING("stateTs", SIZE 2 SEC)))
ON
  "camKey" = "stateKey"
WINDOW TUMBLING("camTs", "stateStart", SIZE 2 SEC)
WHERE
  "faultScore" > 0.5 AND "anyActive" = 1
INTO
  FactoryFaultSink
;

SELECT
  "camTs" AS "ts",
  VARSIZED('YELLOW') AS "alert"
FROM
  (SELECT
     CASTTOUNIXTS("ts") AS "camTs",
     "fault_score" AS "faultScore",
     1 AS "camKey"
   FROM
     MODEL_INFERENCE(FACTORY_FAULT_MODEL, (SELECT "ts", FROM_BASE64("data") AS "pixels" FROM CAM)))
INNER JOIN
  (SELECT
     "stateStart",
     "anyActive",
     1 AS "stateKey"
   FROM
     (SELECT
        start AS "stateStart",
        MAX(CASTTOTYPE("active" AS UINT64)) AS "anyActive"
      FROM
        (SELECT CASTTOUNIXTS("ts") AS "stateTs", "active" FROM STATION_STATE)
      WINDOW TUMBLING("stateTs", SIZE 2 SEC)))
ON
  "camKey" = "stateKey"
WINDOW TUMBLING("camTs", "stateStart", SIZE 2 SEC)
WHERE
  "faultScore" > 0.5 AND "anyActive" = 0
INTO
  FactoryFaultSink
;
