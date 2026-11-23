-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Desugaring shares the left expression as a DAG rather than duplicating its async invocation.
CREATE LOGICAL SOURCE texts(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR texts TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK direct(id UINT64 NOT NULL) TYPE File SET('in_direct.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK nested(id UINT64 NOT NULL) TYPE File SET('in_nested.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK ranged(id UINT64 NOT NULL) TYPE File SET('between.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id FROM texts WHERE HTTP_CALL(data, 'HTTP_SERVER/in-direct') IN ('/in-direct:Hello', '/in-direct:NebulaStream') INTO direct;
SELECT id FROM texts WHERE CONCAT(HTTP_CALL(data, 'HTTP_SERVER/in-nested'), '!') IN ('/in-nested:Hello!', '/in-nested:NebulaStream!') INTO nested;
SELECT id FROM texts WHERE CHAR_LENGTH(HTTP_CALL(data, 'HTTP_SERVER/between')) BETWEEN UINT64(1) AND UINT64(100) INTO ranged;

-- Integer IS NAN is always false, but its async operand must still execute exactly once per row.
CREATE SINK isnan_integer(id UINT64 NOT NULL) TYPE File SET('isnan_integer.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id FROM texts WHERE CHAR_LENGTH(HTTP_CALL(data, 'HTTP_SERVER/isnan-integer')) IS NAN INTO isnan_integer;
