# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The scenarios that a source can bind: the shared ones, and those about a source's read."""

from __future__ import annotations

from conntest_runner.discovery import Scenario
from conntest_runner.scenario_bindings._shared import (
    bad_endpoint,
    empty,
    outcome,
    round_trip_1_buffer,
    round_trip_n_buffers,
    silent_link,
    slow_link,
)

__all__ = [
    "bad_endpoint",
    "close_while_reading",
    "connection_lost",
    "crash_recovery",
    "empty",
    "full_buffer_not_held",
    "partial_buffer_held",
    "round_trip_1_buffer",
    "round_trip_n_buffers",
    "silent_link",
    "slow_link",
]


def close_while_reading(*globs: str) -> Scenario:
    """Bind a config whose read blocks longer than the step timeout, such as a long flush interval, so that only an interrupted read passes."""
    return Scenario("close_while_reading", configs=list(globs))


def full_buffer_not_held(*globs: str) -> Scenario:
    """Bind a config whose flush interval never expires during a test, such as one day."""
    return Scenario("full_buffer_not_held", configs=list(globs))


def partial_buffer_held(config: str, *, flush_interval_ms: int) -> Scenario:
    """``flush_interval_ms`` is the interval that ``config`` sets; the scenario cannot read it, because its key is the connector's.

    Keep it short, such as 2 s: the scenario waits for the flush.
    """
    return Scenario("partial_buffer_held", config=config, args={"flush_interval_ms": flush_interval_ms})


def connection_lost(config: str, *, error: int | str | None = None) -> Scenario:
    """Pass ``error`` for a connector that reports the lost connection as that error instead of ending its stream."""
    return Scenario("connection_lost", config=config, outcome=outcome(error))


def crash_recovery(config: str, *, error: int | str | None = None) -> Scenario:
    """Pass ``error`` for a connector that cannot resume after the process dies."""
    return Scenario("crash_recovery", config=config, outcome=outcome(error))
