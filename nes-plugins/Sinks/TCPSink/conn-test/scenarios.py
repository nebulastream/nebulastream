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

The host reads this module without the loader, so the module holds only data.
"""

from conntest_runner.scenario_bindings.sink import (
    bad_endpoint,
    empty,
    silent_link,
    slow_link,
    transfer_1_buffer,
    transfer_n_buffers,
)

SCENARIOS = [
    transfer_1_buffer("basic.nesql"),
    # More buffers than the sink queues by default, so it must also retry the buffers that it could not queue.
    transfer_n_buffers("basic.nesql", "multicol.nesql"),
    empty("basic.nesql"),
    # The sink connects in the background, so an unreachable endpoint fails it with CannotOpenSink (4004) at STOP, not at START.
    bad_endpoint("short_connect_timeout.nesql", error=4004),
    # The sink has no idle timeout, so it must deliver every row over a slow link and after a silent one.
    slow_link("basic.nesql"),
    silent_link("basic.nesql"),
]
