-- Step 2: Read the Arrow IPC file back through ArrowFileSource and write to CSV.
-- ARROW_FILE_PATH and CSV_FILE_PATH are substituted by the test harness.
-- Schema must match arrow_roundtrip_produce.sql exactly (including nullable annotations).
CREATE LOGICAL SOURCE arrow_data(
    id UINT64 NOT NULL,
    value INT32 NOT NULL,
    ratio FLOAT64 NOT NULL,
    small_val INT8,
    medium_val UINT16);
CREATE PHYSICAL SOURCE FOR arrow_data TYPE ArrowFile SET(
       'ARROW_FILE_PATH' AS `SOURCE`.FILE_PATH,
       'ARROW' AS PARSER.`TYPE`);
CREATE SINK csv_out(
    ARROW_DATA.ID UINT64 NOT NULL,
    ARROW_DATA.VALUE INT32 NOT NULL,
    ARROW_DATA.RATIO FLOAT64 NOT NULL,
    ARROW_DATA.SMALL_VAL INT8,
    ARROW_DATA.MEDIUM_VAL UINT16) TYPE File SET(
       'CSV_FILE_PATH' AS `SINK`.FILE_PATH,
       'CSV' AS `SINK`.OUTPUT_FORMAT);
SELECT ID, VALUE, RATIO, SMALL_VAL, MEDIUM_VAL FROM ARROW_DATA INTO CSV_OUT SET ('arrow-roundtrip-consume' AS `QUERY`.`ID`);
