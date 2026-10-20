# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The shared scenario catalog, and the check of a case's outcome against its declared expectation.

Catalog functions write only the success path and never branch on the connector.
Whether a raised error passes is decided by the binding's expectation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from conntest_runner.protocol import ConnectorError, HarnessError, describe

if TYPE_CHECKING:
    from collections.abc import Callable

    from conntest_runner.discovery import Expectation
    from conntest_runner.harness import Source


@dataclass(frozen=True)
class ScenarioSpec:
    """``default_buffers`` is how much data the scenario seeds, and None for a scenario that seeds nothing.

    A binding can change it only where its constructor in ``scenario_bindings.py`` takes ``n_buffers``.
    """

    name: str
    fn: Callable[[Ctx], None]
    default_buffers: int | None = None


_CATALOG: dict[str, ScenarioSpec] = {}


def scenario(
    name: str,
    *,
    default_buffers: int | None = None,
) -> Callable[[Callable[[Ctx], None]], Callable[[Ctx], None]]:
    def register(fn: Callable[[Ctx], None]) -> Callable[[Ctx], None]:
        _CATALOG[name] = ScenarioSpec(
            name=name,
            fn=fn,
            default_buffers=default_buffers,
        )
        return fn

    return register


def get_scenario(name: str) -> Callable[[Ctx], None]:
    return get_spec(name).fn


def get_spec(name: str) -> ScenarioSpec:
    if name not in _CATALOG:
        raise KeyError(f"unknown scenario {name!r}; catalog has {sorted(_CATALOG)}")
    return _CATALOG[name]


def scenario_names() -> list[str]:
    return sorted(_CATALOG)


@dataclass
class Ctx:
    """The steps that a scenario takes.

    Each step is one harness command or one loader call, so a scenario reads as its sequence of events.
    """

    loader: Any
    data: Any
    target: str
    source: Source
    rows: list[Any] = field(default_factory=list)  # empty for a scenario that seeds nothing
    schema: Any = None

    def setup(self) -> None:
        self.loader.setup(target=self.target, schema=self.schema)

    def connect(self) -> None:
        self.source.open()

    def disconnect(self) -> None:
        self.source.close()

    def kill(self) -> None:
        """The next connect spawns a fresh harness process and connector."""
        self.source.kill()

    def split_rows(self) -> tuple[list[Any], list[Any]]:
        """Two halves, to seed one before and one after a fault."""
        middle = (len(self.rows) + 1) // 2
        return self.rows[:middle], self.rows[middle:]

    def seed_and_fill(self, rows: Any) -> None:
        self.loader.seed_batch(rows, target=self.target)
        result = self.source.fill(*self.data.fill_counts(rows))
        self.data.compare_fill(rows, observed=result.observed)


def _round_trip(ctx: Ctx) -> None:
    ctx.setup()
    ctx.connect()
    ctx.seed_and_fill(ctx.rows)
    ctx.disconnect()


@scenario("round_trip_1_buffer", default_buffers=1)
def round_trip_1_buffer(ctx: Ctx) -> None:
    """Less data than one full buffer, so the source must emit a partly filled buffer.

    ``empty`` covers zero buffers.
    """
    _round_trip(ctx)


@scenario("round_trip_n_buffers", default_buffers=100)
def round_trip_n_buffers(ctx: Ctx) -> None:
    """More buffers than the harness's pool holds by default, so the source must wait for FILL to free them."""
    _round_trip(ctx)


@scenario("empty")
def empty(ctx: Ctx) -> None:
    ctx.setup()
    ctx.connect()
    ctx.disconnect()


@scenario("bad_endpoint")
def bad_endpoint(ctx: Ctx) -> None:
    """The config points at a dead endpoint, so OPEN must fail with the declared error."""
    ctx.connect()


@scenario("crash_recovery", default_buffers=2)
def crash_recovery(ctx: Ctx) -> None:
    """Models a worker that is killed mid-stream without a CLOSE.

    A new connector must take over whatever the dead one left on the backend.
    The backend keeps the read position, so the new connector resumes after the first batch.
    """
    ctx.setup()
    ctx.connect()
    head, tail = ctx.split_rows()
    ctx.seed_and_fill(head)
    ctx.kill()
    ctx.connect()
    ctx.seed_and_fill(tail)
    ctx.disconnect()


def run_case(fn: Callable[[Ctx], None], ctx: Ctx, expect: Expectation) -> None:
    try:
        fn(ctx)
    except ConnectorError as e:
        if expect.ok:
            raise AssertionError(
                f"expected OK but raised {describe(e.code)}@{e.phase}: {e.message}"
            ) from e
        if e.code != expect.code:
            # An ERROR expectation always has a code.
            # The guard is for the type checker.
            want = describe(expect.code) if expect.code is not None else "?"
            raise AssertionError(f"expected {want}, got {describe(e.code)}@{e.phase}") from e
        if expect.phase is not None and e.phase != expect.phase:
            raise AssertionError(f"expected phase {expect.phase!r}, got {e.phase!r}") from e
    except HarnessError:
        # An infra failure or step timeout is never an expected outcome.
        raise
    else:
        if not expect.ok:
            want = describe(expect.code) if expect.code is not None else "?"
            raise AssertionError(f"expected ERROR {want} but the scenario completed cleanly")
