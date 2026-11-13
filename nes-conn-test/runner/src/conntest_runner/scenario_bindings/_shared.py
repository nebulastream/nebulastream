# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The scenarios that sources and sinks share, which ``source`` and ``sink`` both offer."""

from __future__ import annotations

from conntest_runner.discovery import Scenario


def outcome(code: int | str | None) -> str:
    return "OK" if code is None else f"ERROR {code}"


def transfer_1_buffer(*globs: str, loader_args: dict[str, object] | None = None) -> Scenario:
    """``loader_args`` are for a config whose data the loader must seed or read back differently from the connector's other configs."""
    return Scenario("transfer_1_buffer", configs=list(globs), loader_args=loader_args)


def transfer_n_buffers(*globs: str, n_buffers: int | None = None) -> Scenario:
    """``n_buffers`` replaces the default; keep it above the harness's pool size, or the source never has to wait."""
    return Scenario("transfer_n_buffers", configs=list(globs), n_buffers=n_buffers)


def empty(*globs: str) -> Scenario:
    return Scenario("empty", configs=list(globs))


def bad_endpoint(config: str, *, error: int | str) -> Scenario:
    return Scenario("bad_endpoint", config=config, outcome=outcome(error))


def slow_link(*globs: str) -> Scenario:
    return Scenario("slow_link", configs=list(globs))


def silent_link(*globs: str) -> Scenario:
    return Scenario("silent_link", configs=list(globs))
