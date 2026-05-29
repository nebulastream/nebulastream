# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Conn-test scenarios for the MQTT sink (pure data)."""

from conntest_runner.datamodel import File
from conntest_runner.discovery import Scenario

SCENARIOS = [
    # Write the records through the sink, read them back off the broker, compare
    # the multiset. concurrent is the same flow under 8 threads x tiny buffers
    # (the TSan signal); the runner applies that drain tuning by scenario name.
    Scenario("round_trip", config="valid/basic.nesql", data=File("input/records.csv")),
    Scenario("concurrent", config="valid/basic.nesql", data=File("input/records.csv")),
    # Lifecycle-only: start → stop with an empty queue (deterministic drain).
    Scenario("empty", config="valid/basic.nesql"),
    Scenario(
        "config",
        configs=[
            ("valid/*.nesql", "OK"),
            ("invalid/missing-topic.nesql", "ERROR 1000"),
        ],
    ),
    # MQTTSink::start maps an unreachable broker to CannotOpenSink (4004).
    Scenario("bad_endpoint", config="valid/basic.nesql", outcome="ERROR 4004"),
    # Write a batch WHILE the link to the broker is cut: MQTTSink's client
    # buffers it (send_while_disconnected) and flushes it on reconnect
    # (automatic_reconnect), so every record is read back — the sink-side
    # "near link disappears" capability.
    Scenario(
        "outage_delivery",
        config="valid/basic.nesql",
        data=File("input/records.csv"),
        needs="link",
    ),
    # The mirror of outage_delivery: with buffering turned OFF, a write made
    # while the link is cut is rejected instead of queued, so the data is lost.
    # no-buffer.nesql sets send_while_disconnected=false (and automatic_reconnect
    # =false), so the during-cut publish hits a disconnected client and
    # MQTTSink::execute maps it to CannotOpenSink (4004) — the declared outcome.
    # read_back then confirms only the pre-cut batch survived. (basic.nesql keeps
    # both knobs at their reconnect-and-deliver defaults, which is outage_delivery
    # above; the two configs are what split delivery from loss.)
    Scenario(
        "outage_loss",
        config="valid/no-buffer.nesql",
        data=File("input/records.csv"),
        outcome="ERROR 4004",
        needs="link",
    ),
    # A buffering-free reconnect test (re-establishment alone, no offline
    # buffering) is NOT expressible: for a writer, the post-restore write races
    # paho's async reconnect, so the harness would have to wait on the client's
    # connection state first — a connector-readiness hook the abstract Sink
    # interface doesn't expose, and adding one for a test is an anti-pattern.
    # Enable once Sink can be asked "are you writable yet?".
    # Scenario("reconnect", config="valid/basic.nesql",
    #          data=File("input/records.csv"), needs="link"),
]
