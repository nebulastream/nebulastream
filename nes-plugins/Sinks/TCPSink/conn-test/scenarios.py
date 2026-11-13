# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Connection-test scenarios for the TCP sink.

The host reads this module without the loader, so it holds data only.
"""

from conntest_runner.scenario_bindings.sink import (
    bad_endpoint,
    empty,
    round_trip_1_buffer,
    round_trip_n_buffers,
    silent_link,
    slow_link,
)

SCENARIOS = [
    round_trip_1_buffer("basic.nesql"),
    # More buffers than the sink queues by default, so it must also retry the buffers that it could not queue.
    round_trip_n_buffers("basic.nesql", "multicol.nesql"),
    empty("basic.nesql"),
    # The sink connects in the background, so the unreachable endpoint fails it with CannotOpenSink (4004) when it stops with data left.
    bad_endpoint("basic.nesql", error=4004),
    # The sink has no idle timeout: a slow link only delays the data, and a silent one is waited out.
    slow_link("basic.nesql"),
    silent_link("basic.nesql"),
]
