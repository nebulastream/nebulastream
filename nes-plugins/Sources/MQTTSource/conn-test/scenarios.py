# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Connection-test scenarios for the MQTT source.

The host reads this module without the loader, so it holds data only.
The broker delivers only to existing subscriptions, so every scenario seeds after OPEN has subscribed.
"""

from conntest_runner.scenario_bindings.source import (
    bad_endpoint,
    close_while_reading,
    connection_lost,
    crash_recovery,
    empty,
    full_buffer_not_held,
    partial_buffer_held,
    round_trip_1_buffer,
    round_trip_n_buffers,
    silent_link,
    slow_link,
)

SCENARIOS = [
    round_trip_1_buffer("basic.nesql", "qos0.nesql", "qos2.nesql", "log_messages.nesql"),
    # Only this round trip also covers the multi-column schema.
    # QoS 0 must not lose data either: the broker queue is unlimited and the client buffers what the held-back source does not read.
    round_trip_n_buffers("basic.nesql", "multicol.nesql", "qos0.nesql", "qos2.nesql"),
    # Nothing is published, so the source waits on the silent subscription until the harness closes it.
    empty("basic.nesql"),
    # The client library's connect fails, and open() rethrows it as CannotOpenSource (4002).
    bad_endpoint("basic.nesql", error=4002),
    # The source waits on the silent subscription, so the stop request must interrupt its read.
    close_while_reading("basic.nesql", "no_flush.nesql"),
    # One full buffer is emitted, while the one-day flush interval holds back the partly filled remainder.
    full_buffer_not_held("no_flush.nesql"),
    # A partly filled buffer is held back for the whole 2 s interval that short_flush.nesql sets.
    partial_buffer_held("short_flush.nesql", flush_interval_ms=2000),
    slow_link("basic.nesql"),
    silent_link("basic.nesql"),
    # The source does not reconnect: a lost broker connection fails the read with CannotReadFromSource (4006).
    connection_lost("basic.nesql", error=4006),
    # The respawned source subscribes again under a new client id, and the loader publishes the rest there.
    crash_recovery("basic.nesql"),
]
