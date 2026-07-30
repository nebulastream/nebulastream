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
SELECT
  CASTTOUNIXTS("ts") AS "ts",
  "hardwareId",
  "hardwareModel",
  "softwareName",
  "softwareVersion",
  "message"
FROM
  BROADCAST
INTO
  BROADCAST_SINK
;

SELECT
  CASTTOUNIXTS("ts") AS "ts",
  "br",
  "ldr"
FROM
  LDR
INTO
  LDR_SINK
;
SELECT
  CASTTOUNIXTS("ts") AS "ts",
  "pan",
  "tilt"
FROM
  PTU_POS
INTO
  PTU_POS_SINK
;
SELECT
  CASTTOUNIXTS("ts") AS "ts",
  "state",
  "type"
FROM
  ORDER_STATE
INTO
  ORDER_STATE_SINK
;
SELECT
  CASTTOUNIXTS("ts") AS "ts",
  "station",
  "code",
  "description",
  "active",
  "target"
FROM
  STATION_STATE
INTO
  STATION_STATE_SINK
;
SELECT
  CASTTOUNIXTS("ts") AS "ts"
FROM
  STOCK
INTO
  STOCK_SINK
