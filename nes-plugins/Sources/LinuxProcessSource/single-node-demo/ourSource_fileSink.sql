CREATE LOGICAL SOURCE ourSource_fileSink(id UINT64, status VARSIZED, value UINT64, type VARSIZED);

CREATE PHYSICAL SOURCE FOR ourSource_fileSink TYPE LinuxProcess SET(
    'python3 ./producer1.py' AS `SOURCE`.command,
    'CSV' AS PARSER.`TYPE`,
    '\n' AS PARSER.TUPLE_DELIMITER,
    ','  AS PARSER.FIELD_DELIMITER
);

CREATE SINK result(
    ourSource_fileSink.id UINT64,
    ourSource_fileSink.status VARSIZED,
    ourSource_fileSink.value UINT64,
    ourSource_fileSink.type VARSIZED
) TYPE File SET(
    './ourSource_fileSink-output.csv' AS `SINK`.FILE_PATH,
    'CSV' AS `SINK`.INPUT_FORMAT
);
SELECT * FROM ourSource_fileSink INTO result;
