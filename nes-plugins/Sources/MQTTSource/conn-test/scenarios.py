# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Conn-test scenarios for the MQTT source (pure data).

Read host-side without importing the loader's paho dependency. Each scenario is
bound through its own constructor in ``conntest_runner.scenario_bindings``, which exposes
only the knobs that scenario accepts.
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
    # round_trip tiers: the framework generates deterministic rows, the loader
    # serializes them to JSONL and publishes them, and the round-trip is verified. The
    # 10 tier also runs the multi-column schema (multicol.nesql) to exercise the MQTT
    # JSON path with VARSIZED + multiple columns; the 1/100 tiers pin basic.
    round_trip_1_buffer("valid/basic.nesql", setup_file="mosquitto.conf"),
    round_trip_10_buffers("valid/basic.nesql", "valid/multicol.nesql", setup_file="mosquitto.conf"),
    round_trip_100_buffers("valid/basic.nesql", setup_file="mosquitto.conf"),
    # Lifecycle-only (MQTT has no deterministic EoS): open → close, nothing seeded.
    empty("valid/basic.nesql", setup_file="mosquitto.conf"),
    # Config validation: every valid binds clean; the missing-topic config fails
    # with InvalidConfigParameter (1000). VALIDATE-only, so no setup_file (it spins
    # up no broker) — the runner folds it into the primary mosquitto.conf group.
    config_valid(path="valid/*.nesql"),
    config_invalid(path="invalid/missing-topic.nesql", error=1000),
    # An out-of-range qos (3) is rejected by the QOS validator — same 1000, a
    # different rejection reason (a present-but-bad value vs a missing key).
    config_invalid(path="invalid/bad-qos.nesql", error=1000),
    # MQTTSource::open maps an unreachable broker to CannotOpenSource (4002).
    bad_endpoint("valid/basic.nesql", error=4002, setup_file="mosquitto.conf"),
    # Connection-loss recovery: route through toxiproxy, transfer a first batch,
    # cut+restore the link, transfer a second batch. Uses the persistent-session
    # config (clean_session=false) so the subscription survives the disconnect —
    # OK iff the connector recovers.
    # persistent-patient.nesql's long idle budget holds the post-gap read open until
    # paho reconnects and the second batch arrives — the connector recovers (OK).
    reconnect("valid/persistent-patient.nesql", setup_file="mosquitto.conf"),
    # persistent.nesql's short (~2 s) idle budget expires during the cut, so the
    # source self-terminates and the second batch (published after restore) is lost —
    # a connection loss in-between the buffers, modelled as a connector-OK source LOSS.
    reconnect("valid/persistent.nesql", loss=True, setup_file="mosquitto.conf"),
    # A batch is in flight DURING the outage (QoS 0). Whether it survives is the
    # broker's queue_qos0_messages policy — the only difference is the mosquitto.conf
    # mounted via setup_file, which also picks the declared outcome.
    #   queue_qos0_messages=true  → buffered + delivered in order (OK, no loss).
    outage("valid/persistent.nesql", setup_file="mosquitto-queue-qos0.conf"),
    #   queue_qos0_messages=false → dropped; the post-reconnect batch confirms the
    #   loss deterministically. A source loss is connector-OK → bare LOSS (no code).
    outage("valid/persistent.nesql", loss=True, setup_file="mosquitto.conf"),
    # Cold restart: SIGKILL the harness after the first batch, then a fresh client
    # re-subscribes. The persistent session (clean_session=false) lets the broker
    # resume delivery from the session position, so the restarted source reads the
    # second batch without re-reading the first. OK.
    crash_recovery("valid/persistent.nesql", setup_file="mosquitto.conf"),
    # Degraded-but-alive link: a bidirectional latency toxic slows the read. OK
    # iff the connector tolerates the delay without tripping its idle-EoS budget
    # (basic.nesql's ~2 s) and still reads the full byte stream (catalog default
    # 200 ms/direction — well under that budget).
    slow_link("valid/basic.nesql", setup_file="mosquitto.conf"),
    # A ~15 s link *silence* (the connection stays established — no cut), bound
    # twice: this is exactly the fault the scenario exists to flush out, a
    # connector that mistakes silence for end-of-stream.
    #   persistent.nesql's ~2 s idle budget (200 ms x 10) fires DURING the gap —
    #   the harness drives the source from a real SourceThread, so the budget
    #   runs against the silent link, the source declares EoS and is closed
    #   (its UNSUBSCRIBE itself sits in the silenced link until the gap lifts),
    #   and the post-gap batch lands on a dead subscription. The connector is
    #   OK by its own idle-EoS modeling — a bare source LOSS. (Confirmed by
    #   running; under the earlier cut-based semantics this was LOSS 4002, the
    #   close-on-severed-connection error that silence does not produce.)
    silent_link("valid/persistent.nesql", loss=True, setup_file="mosquitto.conf"),
    #   persistent-patient.nesql's 30 s idle budget outlasts the hold, the
    #   subscription stays live, and the post-gap batch arrives — OK.
    silent_link("valid/persistent-patient.nesql", setup_file="mosquitto.conf"),
]
