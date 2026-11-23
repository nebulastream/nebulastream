# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Connection-test scenarios for the Kafka sink.

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
    # With room for 4 messages in its producer queue, the sink also retries the buffers that it could not queue.
    transfer_n_buffers("basic.nesql", "multicol.nesql", "small_queue.nesql"),
    empty("basic.nesql"),
    # The sink requests the broker's metadata when it starts, so an unreachable broker fails it with CannotOpenSink (4004) at START.
    bad_endpoint("basic.nesql", error=4004),
    slow_link("basic.nesql"),
    # WRITE returns once the sink has queued its messages, and the producer sends them a few milliseconds later,
    # so a message of the first half can still wait for the broker's acknowledgement when the silence begins.
    # It would fail after the default delivery timeout of 5 s, which is shorter than the silence.
    silent_link("long_delivery_timeout.nesql"),
]
