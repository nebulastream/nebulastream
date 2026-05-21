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

-- Schema for the ODBC sink external_systest. PostgreSQL runs this on first
-- boot (see compose.snippet.yaml), creating an empty `sink_output` table
-- before it accepts connections. The ODBC sink under test appends one row per
-- output tuple with positional `INSERT ... VALUES`, so the column order here
-- must match the query's output schema. The sink capture (ODBCSinkCapture.cpp)
-- polls the table back into the systest result file. UINT64 query columns map
-- to BIGINT — PostgreSQL has no unsigned integer type and the test values are
-- small — and the database coerces the quoted string literals the sink emits.
CREATE TABLE sink_output (
    id          BIGINT NOT NULL,
    value       BIGINT NOT NULL,
    "timestamp" BIGINT NOT NULL
);
