# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Conn-test scenarios for the ODBC (Postgres) source (pure data)."""

from conntest_runner.datamodel import File
from conntest_runner.discovery import Scenario

SCENARIOS = [
    # Native-tuple round-trip: RowModel parses the CSV with the source's schema,
    # seeds the rows, and multiset-compares the decoded observed bytes.
    Scenario("round_trip", config="valid/basic.nesql", data=File("input/rows.csv")),
    # Lifecycle-only (an ODBC poller has no deterministic EoS): open → close.
    Scenario("empty", config="valid/basic.nesql"),
    Scenario(
        "config",
        configs=[
            ("valid/*.nesql", "OK"),
            ("invalid/missing-query.nesql", "ERROR 1000"),
        ],
    ),
    # ODBCSource::open maps a failed SQLDriverConnect to CannotOpenSource (4002).
    Scenario("bad_endpoint", config="valid/basic.nesql", outcome="ERROR 4002"),
    # Connection-loss: route through toxiproxy, cut+restore the link. ODBCSource
    # has no reconnect logic — its CHECK() macro turns the post-restore
    # SQLExecute on the dead connection into CannotOpenSource (4002). Declared
    # as the expected ERROR (the design's reconnect OK/ERROR split); flips to OK
    # for free if the connector ever gains reconnect support. The rows stay in
    # the durable table, so this surfaces the *connector* gap, not data loss.
    Scenario(
        "reconnect",
        config="valid/basic.nesql",
        data=File("input/rows.csv"),
        outcome="ERROR 4002",
        needs="link",
    ),
]
