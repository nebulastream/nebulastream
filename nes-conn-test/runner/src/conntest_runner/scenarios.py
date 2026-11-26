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

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from conntest_runner.protocol import ConnectorError, HarnessError, describe

if TYPE_CHECKING:
    from collections.abc import Callable

    from conntest_runner.datamodel import SinkModel, SourceModel
    from conntest_runner.discovery import Expectation
    from conntest_runner.harness import Sink, Source
    from conntest_runner.link import Link

    # A scenario takes the context of a kind that can run it.
    # The binding modules offer each scenario only to such kinds, so the catalog accepts any context.
    ScenarioFn = Callable[[Any], None]


@dataclass(frozen=True)
class ScenarioSpec:
    """``default_buffers`` is how much data the scenario seeds, and None for a scenario that seeds nothing.

    A binding can change it only where its constructor in ``scenario_bindings`` takes ``n_buffers``.
    ``needs_link`` routes the connector through the fault proxy, which the scenario controls as ``ctx.link``.
    """

    name: str
    fn: ScenarioFn
    default_buffers: int | None = None
    needs_link: bool = False


_CATALOG: dict[str, ScenarioSpec] = {}


def scenario(
    name: str,
    *,
    default_buffers: int | None = None,
    needs_link: bool = False,
) -> Callable[[ScenarioFn], ScenarioFn]:
    def register(fn: ScenarioFn) -> ScenarioFn:
        _CATALOG[name] = ScenarioSpec(
            name=name,
            fn=fn,
            default_buffers=default_buffers,
            needs_link=needs_link,
        )
        return fn

    return register


def get_scenario(name: str) -> ScenarioFn:
    return get_spec(name).fn


def get_spec(name: str) -> ScenarioSpec:
    if name not in _CATALOG:
        raise KeyError(f"unknown scenario {name!r}; catalog has {sorted(_CATALOG)}")
    return _CATALOG[name]


def scenario_needs_link(name: str) -> bool:
    """False for a name that is not in the catalog, which binding_problems reports separately."""
    spec = _CATALOG.get(name)
    return spec is not None and spec.needs_link


def scenario_names() -> list[str]:
    return sorted(_CATALOG)


@dataclass(kw_only=True)
class Ctx(ABC):
    """The steps that a scenario takes.

    Each step is one harness command or one loader call, so a scenario reads as its sequence of events.
    A case runs one connector, and the subclass for its kind implements the steps that every kind takes.
    A kind that leaves one out fails when its case is constructed, not halfway through a scenario.
    A step that only one kind takes is on that kind's subclass, and a scenario that uses it takes that subclass.
    """

    loader: Any
    data: Any
    target: str
    rows: list[Any] = field(default_factory=list)  # empty for a scenario that moves no data
    schema: Any = None
    args: dict[str, Any] = field(default_factory=dict)  # the binding's arguments for this scenario, see discovery.Scenario
    link: Link | None = None  # set for a scenario that needs the link

    @property
    def fault(self) -> Link:
        if self.link is None:
            raise RuntimeError("the scenario uses the fault link but is not registered with needs_link=True")
        return self.link

    def setup(self) -> None:
        self.loader.setup(target=self.target, schema=self.schema)

    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def disconnect(self) -> None: ...

    @abstractmethod
    def kill(self) -> None:
        """The next connect spawns a fresh harness process and connector."""

    @abstractmethod
    def transfer_half(self) -> None:
        """Moves the first half of the data, so that a scenario can inject a fault before it moves the rest."""

    @abstractmethod
    def transfer(self) -> None:
        """Moves the data that has not moved yet."""

    @abstractmethod
    def check_delivered(self) -> None:
        """Compares what reached the backend with the rows, after the connector has stopped."""


@dataclass(kw_only=True)
class SourceCtx(Ctx):
    """A source's data moves when its backend is seeded and FILL reads it, and is compared as it is read."""

    source: Source
    data: SourceModel
    # The rows seeded into the backend so far.
    # A connect after a kill keeps the count, because the backend keeps the read position.
    _seeded: int = field(default=0, init=False)

    def connect(self) -> None:
        self.source.open()

    def disconnect(self) -> None:
        self.source.close()

    def kill(self) -> None:
        self.source.kill()

    def transfer_half(self) -> None:
        self._seed_and_fill((len(self.rows) + 1) // 2 - self._seeded)

    def transfer(self) -> None:
        self._seed_and_fill(len(self.rows) - self._seeded)

    def check_delivered(self) -> None:
        """A no-op, because the rows were compared as FILL read them."""

    def start_reading(self) -> None:
        """Returns once the connector is inside a read, waiting for data that is never seeded."""
        self.source.fill(0, 0)

    def seed(self, rows: Any) -> None:
        self.loader.seed_batch(rows, target=self.target)

    def _seed_and_fill(self, count: int) -> None:
        rows = self.rows[self._seeded : self._seeded + count]
        self._seeded += count
        self.seed(rows)
        result = self.source.fill(*self.data.fill_counts(rows))
        self.data.compare_fill(rows, observed=result.observed)


@dataclass(kw_only=True)
class SinkCtx(Ctx):
    """A sink's data moves with each WRITE, and is compared once the sink has stopped and the loader has read it back."""

    sink: Sink
    data: SinkModel
    # The buffers that the harness's LOAD made from the rows.
    buffers: int
    # The buffers written since the last START.
    _written: int = field(default=0, init=False)

    def connect(self) -> None:
        self.sink.start()
        self._written = 0

    def disconnect(self) -> None:
        self.sink.stop()

    def kill(self) -> None:
        self.sink.kill()

    def transfer_half(self) -> None:
        self._write((self.buffers + 1) // 2 - self._written)

    def transfer(self) -> None:
        self._write(self.buffers - self._written)

    def check_delivered(self) -> None:
        received = self.loader.read_back(
            target=self.target, schema=self.schema, n_rows=len(self.rows), timeout_s=self.sink.step_timeout_s
        )
        self.data.compare_read_back(self.rows, received=received)

    def _write(self, n_buffers: int) -> None:
        self._written += n_buffers
        self.sink.write(n_buffers)


def _transfer_all(ctx: Ctx) -> None:
    ctx.setup()
    ctx.connect()
    ctx.transfer()
    ctx.disconnect()
    ctx.check_delivered()


@scenario("transfer_1_buffer", default_buffers=1)
def transfer_1_buffer(ctx: Ctx) -> None:
    """Less data than one full buffer, so a source must emit a partly filled buffer, and a sink receives a single partly filled buffer.

    ``empty`` covers zero buffers.
    """
    _transfer_all(ctx)


@scenario("transfer_n_buffers", default_buffers=100)
def transfer_n_buffers(ctx: Ctx) -> None:
    """More buffers than the harness's pool holds by default, so a source must wait for FILL to free them.

    The engine runs a sink with them on several worker threads at once.
    """
    _transfer_all(ctx)


@scenario("empty")
def empty(ctx: Ctx) -> None:
    """A sink that is started and stopped without data must deliver nothing, not even an empty record."""
    ctx.setup()
    ctx.connect()
    ctx.disconnect()
    ctx.check_delivered()


@scenario("bad_endpoint", default_buffers=1)
def bad_endpoint(ctx: Ctx) -> None:
    """The config points at an unreachable endpoint, so the connector must fail with the declared error.

    The scenario moves one buffer, because a sink that holds no data can stop before its connect attempt fails.
    """
    ctx.setup()
    ctx.connect()
    ctx.transfer()
    ctx.disconnect()


@scenario("close_while_reading")
def close_while_reading(ctx: SourceCtx) -> None:
    """Models a worker that stops a query while its source waits for data, for example during a long flush interval.
    The stop request must interrupt the connector's read: CLOSE has the step timeout, so a connector that ignores it fails.
    """
    ctx.setup()
    ctx.connect()
    ctx.start_reading()
    ctx.disconnect()


# The latency that slow_link adds in each direction.
SLOW_LINK_LATENCY_MS = 200
# How long silent_link keeps the connection silent; below the link's silence latency, see link.py.
SILENCE_HOLD_S = 15


@scenario("slow_link", default_buffers=2, needs_link=True)
def slow_link(ctx: Ctx) -> None:
    """Bytes flow, but late: a slow link must not be taken for a broken one.

    The link is slow from before the connect, so the handshake is slow too.
    """
    ctx.setup()
    ctx.fault.throttle(latency_ms=SLOW_LINK_LATENCY_MS)
    ctx.connect()
    ctx.transfer()
    ctx.disconnect()
    ctx.check_delivered()


@scenario("silent_link", default_buffers=2, needs_link=True)
def silent_link(ctx: Ctx) -> None:
    """A live connection on which nothing flows for a while, as between bursts of a real stream.

    A connector that takes the silence for a dead peer or for the end of the stream loses the second half.
    The silence is the fault itself, not a wait for some state, so it is a fixed duration.
    """
    ctx.setup()
    ctx.connect()
    ctx.transfer_half()
    ctx.fault.silence()
    time.sleep(SILENCE_HOLD_S)
    ctx.fault.unsilence()
    ctx.transfer()
    ctx.disconnect()
    ctx.check_delivered()


@scenario("connection_lost", default_buffers=1, needs_link=True)
def connection_lost(ctx: SourceCtx) -> None:
    """The connection to the backend goes away for good: the source must end its stream instead of waiting forever.

    The data delivered before the loss must be complete.
    A connector that reports the loss as an error instead binds the scenario with that error.
    A connector that hangs fails the step with a StepTimeout.
    """
    ctx.setup()
    ctx.connect()
    ctx.transfer()
    ctx.fault.cut()
    # Asks for one more byte, which can only end with the end of the stream.
    result = ctx.source.fill(0, 1)
    if not result.got.eos or result.observed:
        raise AssertionError(
            f"expected the end of the stream with nothing more after the connection was lost; "
            f"got eos={result.got.eos} and {len(result.observed)} bytes"
        )
    ctx.disconnect()


@scenario("full_buffer_not_held", default_buffers=2)
def full_buffer_not_held(ctx: SourceCtx) -> None:
    """A full buffer goes downstream at once, however long the flush interval is.

    It catches a source that emits a buffer only when its flush interval expires,
    which the transfer scenarios miss because their intervals are short.
    Bind a config whose flush interval never expires during a test, so a buffer can only arrive because it is full.
    The rows fill one buffer and part of a second.
    The first buffer must arrive as a strict prefix of what was seeded, while the interval holds back the partly filled second.
    """
    ctx.setup()
    ctx.connect()
    ctx.seed(ctx.rows)
    expected = ctx.data.encode(ctx.rows)
    observed = ctx.source.fill(0, 1).observed
    if not observed or len(observed) >= len(expected) or not expected.startswith(observed):
        raise AssertionError(
            f"expected the first full buffer, a strict prefix of the {len(expected)} seeded bytes; got {len(observed)} bytes"
        )
    ctx.disconnect()


@scenario("partial_buffer_held", default_buffers=1)
def partial_buffer_held(ctx: SourceCtx) -> None:
    """A partly filled buffer is held back for the flush interval, not emitted as soon as data arrives.

    The data is guaranteed to be ready when the read starts.
    The harness measures each read that returns a partly filled buffer.
    A correct source starts its interval inside that read, so the measurement cannot come out shorter on a slow machine.
    The check is one-sided on purpose: a source that flushes too late only fails when it misses the step timeout,
    because a tighter upper bound would depend on how fast the machine schedules the source.
    """
    interval_ms = ctx.args["flush_interval_ms"]
    ctx.setup()
    ctx.connect()
    ctx.seed(ctx.rows)
    ctx.loader.wait_seeded()
    result = ctx.source.fill(*ctx.data.fill_counts(ctx.rows))
    ctx.data.compare_fill(ctx.rows, observed=result.observed)
    reads = result.got.partial_read_ms
    if not reads or min(reads) < interval_ms:
        raise AssertionError(
            f"expected every partly filled buffer to be held for the {interval_ms} ms flush interval; "
            f"the reads that returned one took {list(reads)} ms"
        )
    ctx.disconnect()


@scenario("crash_recovery", default_buffers=2)
def crash_recovery(ctx: SourceCtx) -> None:
    """Models a worker that is killed mid-stream without a CLOSE.

    A new connector must take over whatever the dead one left on the backend.
    The backend keeps the read position, so the new connector resumes after the first batch.
    """
    ctx.setup()
    ctx.connect()
    ctx.transfer_half()
    ctx.kill()
    ctx.connect()
    ctx.transfer()
    ctx.disconnect()


def run_case(fn: ScenarioFn, ctx: Ctx, expect: Expectation) -> None:
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
