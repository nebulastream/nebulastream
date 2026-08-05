SELECT
  "ts",
  "workpiece/type" AS "type"
FROM
  NFC_DS_LOCAL
WHERE
  "workpiece/type" != VARSIZED('NONE') AND "workpiece/state" == VARSIZED('RAW')
INTO
  FischerTechnikMQTT
;
