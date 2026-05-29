# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Conn-test scenarios for the MQTT sink (pure data).

Each scenario is bound through its own constructor in ``conntest_runner.scenario_bindings``,
which exposes only the knobs that scenario accepts.
"""

from conntest_runner.scenario_bindings import (
    bad_endpoint,
    config_invalid,
    config_valid,
    crash_recovery,
    empty,
    outage,
    round_trip_1_buffer,
    round_trip_10_buffers,
    round_trip_100_buffers,
    silent_link,
    slow_link,
)

SCENARIOS = [
    # Generate rows from the CREATE SINK schema, write them through the sink, read
    # them back off the broker, compare the multiset (rows-equal). Every sink
    # scenario runs under the framework's concurrent worker count (the TSan
    # signal). The light tiers (1/10) cover every valid schema; the heavy tier
    # (100) pins one config for churn.
    round_trip_1_buffer("valid/*.nesql"),
    round_trip_10_buffers("valid/*.nesql"),
    round_trip_100_buffers("valid/basic.nesql"),
    # Lifecycle-only: start → stop with an empty queue (deterministic drain).
    empty("valid/basic.nesql"),
    config_valid(path="valid/*.nesql"),
    config_invalid(path="invalid/missing-topic.nesql", error=1000),
    # An out-of-range qos (3) is rejected by the QOS validator — same 1000, a
    # different rejection reason (a present-but-bad value vs a missing key).
    config_invalid(path="invalid/bad-qos.nesql", error=1000),
    # MQTTSink::start maps an unreachable broker to CannotOpenSink (4004).
    bad_endpoint("valid/basic.nesql", error=4004),
    # A batch is written WHILE the link to the broker is cut. basic.nesql keeps
    # MQTTSink's client buffering on (send_while_disconnected + automatic_reconnect),
    # so it queues the during-cut write and flushes it on reconnect — every record
    # is read back (OK), the sink-side "near link disappears" capability.
    outage("valid/basic.nesql"),
    # The mirror: no-buffer.nesql sets send_while_disconnected=false (and
    # automatic_reconnect=false), so the during-cut publish hits a disconnected
    # client and MQTTSink::execute maps it to CannotOpenSink (4004). read_back
    # confirms only the pre-cut batch survived — the declared LOSS 4004.
    outage("valid/no-buffer.nesql", loss=4004),
    # Cold restart: SIGKILL the harness mid-write, then the respawned sink
    # re-STARTs at the committed offset and writes the tail. A crash (vs the warm
    # reconnect MQTTSink can't express) brings up a fresh paho client; basic.nesql's
    # buffering flushes the resumed tail, so the whole dataset reads back. OK.
    crash_recovery("valid/basic.nesql"),
    # Degraded-but-alive link: a bidirectional latency toxic slows the publishes.
    # OK iff the connector tolerates the delay and every record still reads back
    # (the catalog default 200 ms/direction).
    slow_link("valid/basic.nesql"),
    # A ~15 s link *silence* (the connection stays established — no cut, so no
    # disconnect for buffering/auto-reconnect to compensate). Nothing is
    # written during the gap and paho's keepalive (60 s) outlasts the hold, so
    # the post-gap publish goes out on the same live connection — even the
    # buffering-free config bridges it. Both OK. (Under the earlier cut-based
    # semantics no-buffer was ERROR 4004.)
    silent_link("valid/basic.nesql"),
    silent_link("valid/no-buffer.nesql"),
    # No reconnect binding: a buffering-free reconnect test (re-establishment
    # alone, no offline buffering) is NOT expressible. For a writer, the
    # post-restore write races paho's async reconnect, so the harness would have
    # to wait on the client's connection state first — a connector-readiness hook
    # the abstract Sink interface doesn't expose. Bind "reconnect" here once Sink
    # can be asked "writable yet?".
]
