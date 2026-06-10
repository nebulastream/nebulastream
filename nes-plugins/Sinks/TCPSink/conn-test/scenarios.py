# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Conn-test scenarios for the TCP sink (pure data).

Each scenario is bound through its own constructor in
``conntest_runner.scenario_bindings``, which exposes only the knobs that
scenario accepts.

TCP semantics behind the declared outcomes: the sink is fail-fast and
client-only — no reconnect, no offline buffering (there is no broker to queue
for us). A write onto a severed connection fails the query with CannotOpenSink
(4004); writes during a merely *silent* link land in the kernel send buffer
and flow once the link recovers.
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
    # Generate rows from the CREATE SINK schema, write them through the sink,
    # capture them off the relay, compare the multiset (rows-equal). The light
    # tiers (1/10) cover every valid schema; the heavy tier (100) pins one
    # config for churn.
    round_trip_1_buffer("valid/*.nesql", setup_file="tcp_relay.py"),
    round_trip_10_buffers("valid/*.nesql", setup_file="tcp_relay.py"),
    round_trip_100_buffers("valid/basic.nesql", setup_file="tcp_relay.py"),
    # Lifecycle-only: start → stop, nothing written.
    empty("valid/basic.nesql", setup_file="tcp_relay.py"),
    config_valid(path="valid/*.nesql"),
    config_invalid(path="invalid/*.nesql", error=1000),
    # TCPSink::start maps an unreachable endpoint to CannotOpenSink (4004).
    bad_endpoint("valid/basic.nesql", error=4004, setup_file="tcp_relay.py"),
    # Warm link cut+restore between two batches. Fail-fast TCP has no reconnect
    # logic — the post-restore write hits the severed connection and raises
    # CannotOpenSink (4004). Flips to OK for free if the connector ever gains
    # reconnect support.
    reconnect("valid/basic.nesql", error=4004, setup_file="tcp_relay.py"),
    # A batch is written WHILE the link is cut: the write hits the severed
    # connection and fails with CannotOpenSink (4004); the capture confirms
    # only the pre-cut batch survived.
    outage("valid/basic.nesql", loss=4004, setup_file="tcp_relay.py"),
    # Cold restart: SIGKILL the harness mid-write, then the respawned sink
    # opens a fresh connection (the relay keeps the same capture stream) and
    # re-writes from the committed offset. OK.
    crash_recovery("valid/basic.nesql", setup_file="tcp_relay.py"),
    # Degraded-but-alive link (200 ms/direction latency): writes just take
    # longer; well under write_timeout_ms. OK.
    slow_link("valid/basic.nesql", setup_file="tcp_relay.py"),
    # ~15 s of true link silence (the connection stays established; no bytes
    # flow). Nothing is written during the gap, the connection never drops, and
    # the sink has no keepalive to expire — the post-gap write goes out on the
    # same live connection. OK.
    silent_link("valid/basic.nesql", setup_file="tcp_relay.py"),
]
