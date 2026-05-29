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

The sink is NATIVE: the framework generates typed rows from the CREATE SINK schema
and feeds them to the harness as JSONL, which the harness decodes into native engine
buffers and drains through ODBCSink; the same rows are the read-back oracle. No CSV,
no committed fixture — the framework derives the exact rows that fill the scenario's
buffer count (one decoded buffer each) from the schema, so the buffer-stepping
outage/reconnect scenarios get the N>=2 buffers they need to step.

Each scenario is bound through its own constructor in ``conntest_runner.scenario_bindings``,
which exposes only the knobs that scenario accepts (typed outcomes, the
latency override only on slow_link, …).
"""

from conntest_runner.scenario_bindings import (
    bad_endpoint,
    config_invalid,
    config_valid,
    crash_recovery,
    empty,
    outage,
    reconnect,
    round_trip_1_buffer,
    round_trip_10_buffers,
    round_trip_100_buffers,
    silent_link,
    slow_link,
)

SCENARIOS = [
    # Native round-trip tiers. The light tiers (1/10) run against EVERY valid
    # config — basic.nesql and the nullable-column nullable.nesql — so each schema
    # is generated, written, and read back end-to-end (the schema-driven path, no
    # fixtures). The heavy tier (100) pins a single config to keep the DB-backed
    # matrix bounded.
    round_trip_1_buffer("valid/*.nesql"),
    round_trip_10_buffers("valid/*.nesql"),
    round_trip_100_buffers("valid/basic.nesql"),
    # Lifecycle-only: start → stop with an empty queue.
    empty("valid/basic.nesql"),
    config_valid(path="valid/*.nesql"),
    config_invalid(path="invalid/missing-db-host.nesql", error=1000),
    # ODBCSink::start maps an unreachable endpoint to CannotOpenSink (4004).
    bad_endpoint("valid/basic.nesql", error=4004),
    # Connection-loss: route through toxiproxy, cut+restore the link, then write
    # again. ODBCSink has no reconnect logic — the post-restore INSERT on the
    # dead connection throws RunningRoutineFailure (4001). Flips to OK for free if
    # the connector ever gains reconnect support.
    reconnect("valid/basic.nesql", error=4001),
    # Data written while the link is down is lost: ODBCSink does not buffer, so
    # the INSERT issued during the cut fails with RunningRoutineFailure (4001).
    # read_back (a direct SELECT) confirms only the batch written before the cut
    # reached Postgres — the declared LOSS 4001. Only the LOSS side is bound — a
    # non-buffering sink cannot deliver a during-outage write.
    outage("valid/basic.nesql", loss=4001),
    # Cold restart: SIGKILL the harness mid-write, then the respawned sink
    # re-STARTs at the committed offset and writes the tail. Unlike `reconnect`
    # (a warm cut on a live connection ODBCSink can't heal → ERROR 4001), a crash
    # brings up a *fresh* connection, so the resumed INSERTs succeed against the
    # durable table — the committed prefix plus the resumed tail read back as the
    # whole dataset. OK.
    crash_recovery("valid/basic.nesql"),
    # Degraded-but-alive link: a bidirectional latency toxic slows the INSERTs.
    # OK iff the connector tolerates the delay and the full dataset still lands.
    # ODBC is chatty (login + DDL + per-row INSERTs), so latency compounds over
    # many round-trips — override the 200 ms default down to 50 ms to keep the
    # WRITE step well under the watchdog on slow CI while still degrading the link.
    slow_link("valid/basic.nesql", latency_ms=50),
    # A ~15 s link *silence* (the connection stays established — no cut, so the
    # no-reconnect limitation that fails `reconnect` is not exercised). Nothing
    # crosses the wire during the gap and a 15 s idle Postgres connection is
    # routine, so the post-gap INSERT goes out on the same live connection. OK.
    silent_link("valid/basic.nesql"),
]
