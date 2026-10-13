# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Conn-test scenarios for the TCP source (pure data).

Read host-side without importing the loader. Each scenario is bound through
its own constructor in ``conntest_runner.scenario_bindings``, which exposes
only the knobs that scenario accepts.

TCP semantics behind the declared outcomes: the source is fail-fast and
client-only. End-of-stream is the peer closing the connection; silence is
bridged indefinitely (no idle budget — TCP has a real EoS signal). There is no
reconnect: a severed connection ends the stream (EOF → clean EoS) or fails the
query (reset → CannotOpenSource), so anything seeded after a link cut is lost.
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
    # round_trip tiers: the framework generates deterministic rows, the loader
    # writes them as JSONL through the relay, the source reads the byte stream.
    # The 10 tier also runs the multi-column schema (multicol.nesql); 1/100 pin basic.
    round_trip_1_buffer("valid/basic.nesql", setup_file="tcp_relay.py"),
    round_trip_10_buffers("valid/basic.nesql", "valid/multicol.nesql", setup_file="tcp_relay.py"),
    round_trip_100_buffers("valid/basic.nesql", setup_file="tcp_relay.py"),
    # Lifecycle-only: open → close, nothing seeded. The source waits on a
    # silent connection (no idle EoS) until the harness CLOSEs it.
    empty("valid/basic.nesql", setup_file="tcp_relay.py"),
    # Config validation: every valid config binds clean; each invalid config is
    # rejected with InvalidConfigParameter (1000). VALIDATE-only, so no
    # setup_file (it spins up no relay).
    config_valid(path="valid/*.nesql"),
    config_invalid(path="invalid/*.nesql", error=1000),
    # TCPSource::open maps an unreachable endpoint to CannotOpenSource (4002).
    bad_endpoint("valid/basic.nesql", error=4002, setup_file="tcp_relay.py"),
    # Warm link cut+restore between two batches. Fail-fast TCP cannot recover:
    # the cut severs the connection, the source sees EoS, and the second batch
    # (seeded after restore) is lost — a connector-OK source loss.
    reconnect("valid/basic.nesql", loss=True, setup_file="tcp_relay.py"),
    # No outage binding: the scenario seeds a batch DURING the cut and a second
    # one after restore, and its LOSS oracle expects the post-restore batch to
    # still arrive — i.e. it requires a connector that *recovers* and drops only
    # the in-flight batch (MQTT's broker-queue semantics). A fail-fast TCP
    # source is gone after the cut (EoS), so neither batch arrives and neither
    # OK nor LOSS is expressible. Bind "outage" here if TCP ever grows a
    # reconnect mode.
    # Degraded-but-alive link (200 ms/direction latency): slow is not broken —
    # the source has no idle budget to trip, it just reads later. OK.
    slow_link("valid/basic.nesql", setup_file="tcp_relay.py"),
    # ~15 s of true link silence (the connection stays established; no bytes
    # flow). TCPSource bridges it by design: there is no idle budget and no
    # read deadline — only a peer close ends the stream — so the blocking read
    # simply waits out the gap and the post-gap batch arrives. OK. (The old TCP
    # source tripped exactly here.)
    silent_link("valid/basic.nesql", setup_file="tcp_relay.py"),
    # Cold restart: SIGKILL the harness after the first batch; the respawned
    # source opens a fresh connection, which the relay pairs with the same seed
    # stream, and the second batch (seeded after the respawn) flows. OK.
    crash_recovery("valid/basic.nesql", setup_file="tcp_relay.py"),
]
