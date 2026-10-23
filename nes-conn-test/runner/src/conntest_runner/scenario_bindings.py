# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Per-scenario constructors for a plugin's ``conn-test/scenarios.py``.

Each takes only the arguments that its scenario accepts, so an invalid binding is a ``TypeError`` when it is written,
not a defect found at run time.
"""

from __future__ import annotations

from conntest_runner.discovery import Scenario


def _error(code: int | str | None) -> str:
    return "OK" if code is None else f"ERROR {code}"


def round_trip_1_buffer(*globs: str) -> Scenario:
    return Scenario("round_trip_1_buffer", configs=list(globs))


def round_trip_n_buffers(*globs: str, n_buffers: int | None = None) -> Scenario:
    """``n_buffers`` replaces the default; keep it above the harness's pool size, or the source never has to wait."""
    return Scenario("round_trip_n_buffers", configs=list(globs), n_buffers=n_buffers)


def empty(*globs: str) -> Scenario:
    return Scenario("empty", configs=list(globs))


def bad_endpoint(config: str, *, error: int | str) -> Scenario:
    return Scenario("bad_endpoint", config=config, outcome=_error(error))


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


def slow_link(*globs: str) -> Scenario:
    return Scenario("slow_link", configs=list(globs))


def silent_link(*globs: str) -> Scenario:
    return Scenario("silent_link", configs=list(globs))


def connection_lost(config: str, *, error: int | str | None = None) -> Scenario:
    """Pass ``error`` for a connector that reports the lost connection as that error instead of ending its stream."""
    return Scenario("connection_lost", config=config, outcome=_error(error))


def crash_recovery(config: str, *, error: int | str | None = None) -> Scenario:
    """Pass ``error`` for a connector that cannot resume after the process dies."""
    return Scenario("crash_recovery", config=config, outcome=_error(error))
