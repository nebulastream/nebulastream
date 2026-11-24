# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Connection-test scenarios for the MQTT sink.

The host reads this module without the loader, so the module holds only data.
"""

from conntest_runner.scenario_bindings.sink import (
    bad_endpoint,
    buffer_without_tuples,
    empty,
    silent_link,
    slow_link,
    transfer_1_buffer,
    transfer_n_buffers,
)

# With QoS 0 the sink waits for no acknowledgement, so the loader's end marker could overtake the sink's last messages.
# No config may use QoS 0.
SCENARIOS = [
    transfer_1_buffer("basic.nesql"),
    transfer_n_buffers("basic.nesql", "multicol.nesql", "small_inflight.nesql"),
    empty("basic.nesql"),
    buffer_without_tuples("basic.nesql"),
    # The sink connects to the broker when it starts, so an unreachable broker fails it with CannotOpenSink (4004) at START.
    bad_endpoint("basic.nesql", error="4004 start"),
    slow_link("basic.nesql"),
    silent_link("basic.nesql"),
]
