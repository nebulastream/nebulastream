-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--    https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Schema for the ODBC external_systest. PostgreSQL runs this on first boot
-- (see compose.snippet.yaml), creating an empty `measurements` table before it
-- accepts connections. The row data is NOT seeded here: each `.test` file
-- supplies its own rows in an `ATTACH INLINE` / `ATTACH FILE` block, which the
-- ODBC InlineData / FileData registrar (ODBCTestLoader.cpp) INSERTs at bind
-- time. The column types exercise the ODBC -> NebulaStream type mapping in
-- ODBCConnection.cpp: INTEGER, VARCHAR, BIGINT, DOUBLE PRECISION.
CREATE TABLE measurements (
    id      INTEGER PRIMARY KEY,
    label   VARCHAR(64) NOT NULL,
    reading BIGINT NOT NULL,
    score   DOUBLE PRECISION NOT NULL
);
