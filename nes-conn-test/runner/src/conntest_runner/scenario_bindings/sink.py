# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The scenarios that a sink can bind: the shared ones, and those about a backend that stops reading or goes away."""

from __future__ import annotations

from conntest_runner.discovery import Kind, Scenario
from conntest_runner.scenario_bindings import _shared
from conntest_runner.scenario_bindings._shared import *  # noqa: F403
from conntest_runner.scenario_bindings._shared import outcome

__all__ = [
    "backpressure",
    "reconnect",
    "reconnect_timeout",
    "shutdown_while_blocked",
    "stop_while_blocked",
]
__all__ += _shared.__all__


def backpressure(*globs: str) -> Scenario:
    """Bind a config whose sink applies backpressure after a few buffers, such as a queue of 4 and thresholds of 4 and 2."""
    return Scenario("backpressure", configs=list(globs), kind=Kind.SINK)


def stop_while_blocked(*globs: str) -> Scenario:
    """Bind a config whose sink queues every buffer of the scenario, such as 512, and gives up on them long before 30 s.

    Otherwise the WRITE waits for the backend, or STOP outlasts the step timeout.
    """
    return Scenario("stop_while_blocked", configs=list(globs), kind=Kind.SINK)


def shutdown_while_blocked(*globs: str) -> Scenario:
    """Bind a config whose sink queues every buffer of the scenario, such as 512, so that the WRITE returns while nothing is read."""
    return Scenario("shutdown_while_blocked", configs=list(globs), kind=Kind.SINK)


def reconnect(config: str, *, loss: bool = False) -> Scenario:
    """Pass ``loss`` for a connector that may lose or repeat the data in flight when the connection breaks.

    A LOSS check needs rows that are unique, so bind a config with a VARSIZED column.
    """
    return Scenario("reconnect", config=config, outcome="LOSS" if loss else "OK", kind=Kind.SINK)


def reconnect_timeout(config: str, *, error: int | str) -> Scenario:
    """``error`` is the error that the connector fails with once it gives up; keep its timeout short, such as 1 s."""
    return Scenario("reconnect_timeout", config=config, outcome=outcome(error), kind=Kind.SINK)
