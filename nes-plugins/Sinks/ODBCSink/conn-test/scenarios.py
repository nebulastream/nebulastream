# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Conn-test scenarios for the ODBC (Postgres) sink (pure data).

The sink is NATIVE: the framework's ``NativeModel`` generates typed rows from
the CREATE SINK schema, encodes them into a ``.nes`` the harness drains through
ODBCSink, and keeps them as the read-back oracle. No CSV, no committed fixture —
data is ``Generate(count=...)``d per case, so the row count (and thus the number
of native buffers WRITE steps over) is fixed here. ``_ROWS`` is sized so the
generated ``.nes`` spans several buffers, which the buffer-stepping
outage/reconnect scenarios require (write one buffer, cut the link, write more).
"""

from conntest_runner.datamodel import Generate
from conntest_runner.discovery import Scenario

# Enough rows that the generated `.nes` packs into multiple native buffers (the
# writer bounds each buffer's fixed region to 4096 bytes), so the buffer-granular
# scenarios have >=2 buffers to step.
_ROWS = 512

SCENARIOS = [
    Scenario("round_trip", config="valid/basic.nesql", data=Generate(count=_ROWS)),
    Scenario("concurrent", config="valid/basic.nesql", data=Generate(count=_ROWS)),
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
        data=Generate(count=_ROWS),
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
        data=Generate(count=_ROWS),
        outcome="ERROR 4001",
        needs="link",
    ),
]
