# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Conn-test scenarios for the ODBC (Postgres) source (pure data).

Each scenario is bound through its own constructor in ``conntest_runner.scenario_bindings``,
which exposes only the knobs that scenario accepts.
"""

from conntest_runner.scenario_bindings import (
    bad_endpoint,
    config_invalid,
    config_valid,
    crash_recovery,
    empty,
    reconnect,
    round_trip_1_buffer,
    round_trip_10_buffers,
    round_trip_100_buffers,
    silent_link,
    slow_link,
)

SCENARIOS = [
    # Native-tuple round-trip tiers. The light tiers (1/10) run against EVERY valid
    # config — basic.nesql and the differently-shaped sensor.nesql — so each CREATE
    # LOGICAL SOURCE schema is generated, table-created, INSERTed, polled, and
    # compared end-to-end (a new valid config "just works"). The heavy tier (100)
    # pins a single representative config to keep the DB-backed matrix bounded.
    round_trip_1_buffer("valid/*.nesql"),
    round_trip_10_buffers("valid/*.nesql"),
    round_trip_100_buffers("valid/basic.nesql"),
    # Lifecycle-only (an ODBC poller has no deterministic EoS): open → close.
    empty("valid/basic.nesql"),
    config_valid(path="valid/*.nesql"),
    # Both invalid configs raise InvalidConfigParameter (1000); one binding per file.
    config_invalid(path="invalid/missing-query.nesql", error=1000),
    config_invalid(path="invalid/wrong-param-count.nesql", error=1000),
    # ODBCSource::open maps a failed SQLDriverConnect to CannotOpenSource (4002).
    bad_endpoint("valid/basic.nesql", error=4002),
    # Connection-loss: route through toxiproxy, cut+restore the link. ODBCSource
    # has no reconnect logic — its CHECK() macro turns the post-restore SQLExecute
    # on the dead connection into CannotOpenSource (4002). The rows stay in the
    # durable table, so this surfaces the *connector* gap, not data loss.
    reconnect("valid/basic.nesql", error=4002),
    # Cold restart: SIGKILL the harness after the first batch, then a fresh
    # process re-opens. Unlike `reconnect` (a warm cut the dead SQLExecute can't
    # survive → ERROR 4002), a crash makes a brand-new connection whose
    # windowed-poll watermark resets to the restart instant — so it resumes past
    # the pre-crash rows and reads the second batch cleanly. OK.
    crash_recovery("valid/basic.nesql"),
    # Degraded-but-alive link: a bidirectional latency toxic slows the windowed
    # poll. OK iff the connector tolerates the delay and still reads the full
    # dataset (the catalog default 200 ms/direction).
    slow_link("valid/basic.nesql"),
    # A ~15 s link *silence* (the connection stays established — no cut, so the
    # no-reconnect limitation that fails `reconnect` is not exercised). Nothing
    # crosses the wire during the gap and a 15 s idle Postgres connection is
    # routine, so the post-gap windowed poll runs on the same live connection. OK.
    silent_link("valid/basic.nesql"),
    # No `outage` binding: that scenario probes a broker's offline-queue (QoS-0)
    # policy, which a poll-based source against a durable Postgres table has no
    # analogue for — the rows simply stay in the table. The reconnect binding
    # above already captures this connector's connection-loss gap.
]
