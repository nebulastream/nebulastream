CREATE LOGICAL SOURCE heartRates(creationTS UINT64, patientId UINT32, heartRate FLOAT32);

CREATE PHYSICAL SOURCE FOR heartRates TYPE FILE SET(
       '/home/leo/Dokumente/Work/nebulastream-public/nes-systests/testdata/small/matrix/heartrates.csv' AS `SOURCE`.`FILE_PATH`,
       'CSV' as `PARSER`.`TYPE`
);

CREATE SINK highrates(HEARTRATES.CREATIONTS UINT64, HEARTRATES.PATIENTID UINT32, HEARTRATES.HEARTRATE FLOAT32) TYPE Matrix SET(
       '3000' as `SINK`.PORT,
       'CSV' as `SINK`.INPUT_FORMAT,
       'localhost' as `SINK`.`IP_ADDRESS`,
       'nesNotification' as `SINK`.ENDPOINT,
       'Detected high heart rate of |HEARTRATES$HEARTRATE| for patient |HEARTRATES$PATIENTID|' as `SINK`.MESSAGE
);

SELECT * FROM HEARTRATES WHERE HEARTRATE > FLOAT32(100) INTO HIGHRATES;