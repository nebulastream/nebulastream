# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Connection-test scenarios for the TCP source.

The host reads this module without the loader, so it holds data only.
End-of-stream is the peer closing the connection, so the source waits through silence indefinitely.
"""

from conntest_runner.scenario_bindings import (
    bad_endpoint,
    close_while_reading,
    crash_recovery,
    empty,
    round_trip_1_buffer,
    round_trip_n_buffers,
)

SCENARIOS = [
    round_trip_1_buffer("basic.nesql"),
    # Only this round trip also covers the multi-column schema and the immediate flush.
    round_trip_n_buffers("basic.nesql", "multicol.nesql", "intervals.nesql"),
    # Nothing is seeded, so the source waits on the silent connection until the harness closes it.
    empty("basic.nesql"),
    # An unreachable endpoint fails the open with CannotOpenSource (4002).
    bad_endpoint("basic.nesql", error=4002),
    # The source waits on the silent connection, so the stop request must interrupt its read.
    close_while_reading("basic.nesql"),
    # The respawned source opens a new connection, and the loader sends the rest of the data there.
    crash_recovery("basic.nesql"),
]
