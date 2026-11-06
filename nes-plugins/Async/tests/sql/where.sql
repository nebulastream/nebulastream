-- HTTP_SERVER is replaced with the url of tests/http_path_server.py, which answers with "<path>:<body>".
CREATE LOGICAL SOURCE texts(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR texts TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK direct(id UINT64 NOT NULL, data VARSIZED NOT NULL) TYPE File SET('direct.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK onResult(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('on_result.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
-- The predicate itself calls the service.
SELECT id, data FROM texts WHERE HTTP_CALL(data, 'HTTP_SERVER/direct') = '/direct:Hello' INTO direct;
-- The filter reads the result of a call made in a projection below it.
SELECT id, response FROM (SELECT id, HTTP_CALL(data, 'HTTP_SERVER/result') AS response FROM texts) WHERE response = '/result:NebulaStream' INTO onResult;
