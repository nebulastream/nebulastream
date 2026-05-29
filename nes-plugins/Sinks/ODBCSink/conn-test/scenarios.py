# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Conn-test scenarios for the ODBC (Postgres) sink (pure data)."""

from conntest_runner.datamodel import File
from conntest_runner.discovery import Scenario

SCENARIOS = [
    Scenario("round_trip", config="valid/basic.nesql", data=File("input/records.csv")),
    Scenario("concurrent", config="valid/basic.nesql", data=File("input/records.csv")),
    # Lifecycle-only: start → stop with an empty queue.
    Scenario("empty", config="valid/basic.nesql"),
    Scenario(
        "config",
        configs=[
            ("valid/*.nesql", "OK"),
            ("invalid/missing-db-host.nesql", "ERROR 1000"),
        ],
    ),
    # ODBCSink::start maps an unreachable endpoint to CannotOpenSink (4004).
    Scenario("bad_endpoint", config="valid/basic.nesql", outcome="ERROR 4004"),
    # Connection-loss: route through toxiproxy, cut+restore the link, then write
    # again. ODBCSink has no reconnect logic — the post-restore INSERT on the
    # dead connection throws RunningRoutineFailure (4001). Declared as the
    # expected ERROR (the reconnect OK/ERROR split); flips to OK for free if the
    # connector ever gains reconnect support.
    Scenario(
        "reconnect",
        config="valid/basic.nesql",
        data=File("input/records.csv"),
        outcome="ERROR 4001",
        needs="link",
    ),
    # Data written while the link is down is lost: ODBCSink does not buffer, so
    # the INSERT issued during the cut fails with RunningRoutineFailure (4001) —
    # the declared outcome. read_back (a direct SELECT) also confirms only the
    # batch written before the cut reached Postgres. Deterministic — the write
    # completes (and fails) while the proxy is down.
    Scenario(
        "outage_loss",
        config="valid/basic.nesql",
        data=File("input/records.csv"),
        outcome="ERROR 4001",
        needs="link",
    ),
]
