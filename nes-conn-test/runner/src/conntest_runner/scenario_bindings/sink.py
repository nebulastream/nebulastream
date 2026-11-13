# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The scenarios that a sink can bind, so far only the shared ones."""

from conntest_runner.scenario_bindings._shared import (
    bad_endpoint,
    empty,
    round_trip_1_buffer,
    round_trip_n_buffers,
    silent_link,
    slow_link,
)

__all__ = [
    "bad_endpoint",
    "empty",
    "round_trip_1_buffer",
    "round_trip_n_buffers",
    "silent_link",
    "slow_link",
]
