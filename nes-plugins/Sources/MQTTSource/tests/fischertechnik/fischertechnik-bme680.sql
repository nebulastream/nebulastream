SELECT
  CASTTOUNIXTS("ts") AS "ts",
  "t",
  "rt",
  "h",
  "rh",
  "p",
  "iaq",
  "aq",
  "gr"
FROM
  BME680
INTO
  BME680_SINK
;
