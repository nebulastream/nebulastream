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

Read host-side without importing the loader's paho dependency.
"""

from conntest_runner.datamodel import File
from conntest_runner.discovery import Scenario

SCENARIOS = [
    # round_trip subsumes the old binary scenario: a .bin File fed to ByteModel,
    # verified by the reply SHA over the observed bytes.
    Scenario(
        "round_trip",
        config="valid/basic.nesql",
        data=File("input/sample.bin"),
        setup_file="mosquitto.conf",
    ),
    # Lifecycle-only (MQTT has no deterministic EoS): open → close, nothing seeded.
    Scenario("empty", config="valid/basic.nesql", setup_file="mosquitto.conf"),
    # Config validation: every valid binds clean; the missing-topic config fails
    # with InvalidConfigParameter (1000). Expectation is data, not a filename.
    Scenario(
        "config",
        configs=[
            ("valid/*.nesql", "OK"),
            ("invalid/missing-topic.nesql", "ERROR 1000"),
        ],
        setup_file="mosquitto.conf",
    ),
    # MQTTSource::open maps an unreachable broker to CannotOpenSource (4002).
    Scenario(
        "bad_endpoint",
        config="valid/basic.nesql",
        outcome="ERROR 4002",
        setup_file="mosquitto.conf",
    ),
    # Connection-loss recovery: route the connector through toxiproxy, transfer
    # a first batch, cut+restore the link, transfer a second batch. Uses the
    # persistent-session config (clean_session=false) so the subscription
    # survives the disconnect — OK iff the connector recovers.
    Scenario(
        "reconnect",
        config="valid/persistent.nesql",
        data=File("input/lines.csv"),
        needs="link",
        setup_file="mosquitto.conf",
    ),
    # Buffer published DURING the outage. Both use the durable-session config +
    # toxiproxy; the during-outage buffer is QoS 0, so whether it survives is
    # decided by the broker's queue_qos0_messages setting — the only difference
    # between the two is the mosquitto.conf mounted via setup_file.
    #   queue_qos0_messages=true  → buffered + delivered in order (no loss).
    Scenario(
        "outage_delivery",
        config="valid/persistent.nesql",
        data=File("input/lines.csv"),
        needs="link",
        setup_file="mosquitto-queue-qos0.conf",
    ),
    #   queue_qos0_messages=false → dropped; the post-reconnect buffer confirms
    #   the loss deterministically (no timeout).
    Scenario(
        "outage_loss",
        config="valid/persistent.nesql",
        data=File("input/lines.csv"),
        needs="link",
        setup_file="mosquitto.conf",
    ),
]
