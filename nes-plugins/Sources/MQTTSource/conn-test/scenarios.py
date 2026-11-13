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
    round_trip_1_buffer("basic.nesql", "default_flush.nesql", "qos0.nesql", "qos2.nesql", "log_messages.nesql"),
    # The source appends no delimiter, so the loader publishes each row's newline itself.
    # It splits every row across two messages, so a source that writes anything between two messages breaks every row.
    round_trip_1_buffer("empty_delimiter.nesql", loader_args={"split_rows": True}),
    # Only this round trip also covers the multi-column schema.
    # QoS 0 must not lose data either: the source's client queues every message that it receives, whether or not the source reads.
    round_trip_n_buffers("basic.nesql", "multicol.nesql", "qos0.nesql", "qos2.nesql"),
    # No FILL follows the OPEN, so the source subscribes and unsubscribes without a read.
    empty("basic.nesql"),
    # The client library's connect fails, and open() rethrows it as CannotOpenSource (4002).
    bad_endpoint("basic.nesql", error=4002),
    # The read waits for the one-day flush interval or, without an interval, for a message, so only the stop request ends it.
    close_while_reading("no_flush.nesql", "default_flush.nesql"),
    # One full buffer is emitted, while the one-day flush interval holds back the partly filled remainder.
    full_buffer_not_held("no_flush.nesql"),
    # A partly filled buffer is held back for the whole 2 s interval that short_flush.nesql sets.
    partial_buffer_held("short_flush.nesql", flush_interval_ms=2000),
    slow_link("basic.nesql"),
    silent_link("basic.nesql"),
    # The source does not reconnect: a lost broker connection fails the read with CannotReadFromSource (4006).
    # With a flush interval, the read notices the loss only when its timed wait ends, here after at most 100 ms,
    # because the client library's timed wait does not end on a lost connection.
    connection_lost("basic.nesql", error=4006),
    # Without a flush interval, the lost connection ends the read's wait.
    connection_lost("default_flush.nesql", error=4006),
    # The respawned source subscribes again under a new client id, and the loader publishes the rest there.
    crash_recovery("basic.nesql"),
]
