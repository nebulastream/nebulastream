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
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from conntest_runner.protocol import ConnectorError, HarnessError, describe

if TYPE_CHECKING:
    from collections.abc import Callable

    from conntest_runner.discovery import Expectation
    from conntest_runner.harness import Sink, Source
    from conntest_runner.link import Link


@dataclass(frozen=True)
class ScenarioSpec:
    """``default_buffers`` is how much data the scenario seeds, and None for a scenario that seeds nothing.

    A binding can change it only where its constructor in ``scenario_bindings`` takes ``n_buffers``.
    ``needs_link`` routes the connector through the fault proxy, which the scenario controls as ``ctx.link``.
    """

    name: str
    fn: Callable[[Ctx], None]
    default_buffers: int | None = None
    needs_link: bool = False


_CATALOG: dict[str, ScenarioSpec] = {}


def scenario(
    name: str,
    *,
    default_buffers: int | None = None,
    needs_link: bool = False,
) -> Callable[[Callable[[Ctx], None]], Callable[[Ctx], None]]:
    def register(fn: Callable[[Ctx], None]) -> Callable[[Ctx], None]:
        _CATALOG[name] = ScenarioSpec(
            name=name,
            fn=fn,
            default_buffers=default_buffers,
            needs_link=needs_link,
        )
        return fn

    return register


def get_scenario(name: str) -> Callable[[Ctx], None]:
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


@dataclass
class Ctx:
    """The steps that a scenario takes.

    Each step is one harness command or one loader call, so a scenario reads as its sequence of events.
    A case runs either a source or a sink.
    A step that both kinds take, such as ``connect``, sends the command of the connector that the case runs.
    """

    loader: Any
    data: Any
    target: str
    source: Source | None = None
    sink: Sink | None = None
    rows: list[Any] = field(default_factory=list)  # empty for a scenario that moves no data
    schema: Any = None
    args: dict[str, Any] = field(default_factory=dict)  # the binding's arguments for this scenario, see discovery.Scenario
    link: Link | None = None  # set for a scenario that needs the link
    # How much data has moved: rows seeded into a source's backend, or buffers written to a sink.
    _moved: int = field(default=0, init=False)
    # The buffers that the sink's START made from the rows.
    _sink_buffers: int = field(default=0, init=False)

    @property
    def fault(self) -> Link:
        if self.link is None:
            raise RuntimeError("the scenario uses the fault link but is not registered with needs_link=True")
        return self.link

    @property
    def src(self) -> Source:
        if self.source is None:
            raise RuntimeError("a source step in a case that runs a sink; only scenario_bindings.source may bind this scenario")
        return self.source

    def setup(self) -> None:
        self.loader.setup(target=self.target, schema=self.schema)

    def connect(self) -> None:
        """Opens the source, or starts the sink, which makes its buffers from the rows."""
        if self.sink is not None:
            self._sink_buffers = self.sink.start().n_buffers
            self._moved = 0
        else:
            self.src.open()

    def disconnect(self) -> None:
        if self.sink is not None:
            self.sink.stop()
        else:
            self.src.close()

    def kill(self) -> None:
        """The next connect spawns a fresh harness process and connector."""
        if self.sink is not None:
            self.sink.kill()
        else:
            self.src.kill()

    def transfer_half(self) -> None:
        """Moves the first half of the data, so that a fault can come between the two halves."""
        if self.sink is not None:
            self._write((self._sink_buffers + 1) // 2 - self._moved)
        else:
            self._seed_and_fill((len(self.rows) + 1) // 2 - self._moved)

    def transfer(self) -> None:
        """Moves the data that has not moved yet: a source reads it from its backend, and a sink writes it to its backend.

        A source's data is compared as it is read, and a sink's in ``check_delivered``, after the sink has stopped.
        """
        if self.sink is not None:
            self._write(self._sink_buffers - self._moved)
        else:
            self._seed_and_fill(len(self.rows) - self._moved)

    def check_delivered(self) -> None:
        """Compares what the sink wrote with the rows that it was given; a no-op for a source."""
        if self.sink is None:
            return
        received = self.loader.read_back(target=self.target, schema=self.schema, expect_records=len(self.rows))
        self.data.compare_read_back(self.rows, received=received)

    def start_reading(self) -> None:
        """Returns once the connector is inside a read, waiting for data that is never seeded."""
        self.src.fill(0, 0)

    def seed(self, rows: Any) -> None:
        self.loader.seed_batch(rows, target=self.target)

    def _seed_and_fill(self, count: int) -> None:
        rows = self.rows[self._moved : self._moved + count]
        self._moved += count
        self.seed(rows)
        result = self.src.fill(*self.data.fill_counts(rows))
        self.data.compare_fill(rows, observed=result.observed)

    def _write(self, n_buffers: int) -> None:
        assert self.sink is not None  # noqa: S101  # only called for a sink; narrows the type
        self._moved += n_buffers
        self.sink.write(n_buffers)


def _round_trip(ctx: Ctx) -> None:
    ctx.setup()
    ctx.connect()
    ctx.transfer()
    ctx.disconnect()
    ctx.check_delivered()


@scenario("round_trip_1_buffer", default_buffers=1)
def round_trip_1_buffer(ctx: Ctx) -> None:
    """Less data than one full buffer, so a source must emit a partly filled buffer, and a sink gets one.

    ``empty`` covers zero buffers.
    """
    _round_trip(ctx)


@scenario("round_trip_n_buffers", default_buffers=100)
def round_trip_n_buffers(ctx: Ctx) -> None:
    """More buffers than the harness's pool holds by default, so a source must wait for FILL to free them.

    A sink gets them from several threads at once, as from the engine's worker threads.
    """
    _round_trip(ctx)


@scenario("empty")
def empty(ctx: Ctx) -> None:
    """A sink that is started and stopped without data must deliver nothing, not even an empty record."""
    ctx.setup()
    ctx.connect()
    ctx.disconnect()
    ctx.check_delivered()


@scenario("bad_endpoint", default_buffers=1)
def bad_endpoint(ctx: Ctx) -> None:
    """The config points at a dead endpoint, so the connector must fail with the declared error.

    A connector that connects only once it has data to move fails while moving it, as a sink that writes in the background does.
    """
    ctx.setup()
    ctx.connect()
    ctx.transfer()
    ctx.disconnect()


@scenario("close_while_reading")
def close_while_reading(ctx: Ctx) -> None:
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
def connection_lost(ctx: Ctx) -> None:
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
    result = ctx.src.fill(0, 1)
    if not result.got.eos or result.observed:
        raise AssertionError(
            f"expected the end of the stream with nothing more after the connection was lost; "
            f"got eos={result.got.eos} and {len(result.observed)} bytes"
        )
    ctx.disconnect()


@scenario("full_buffer_not_held", default_buffers=2)
def full_buffer_not_held(ctx: Ctx) -> None:
    """A full buffer goes downstream at once, however long the flush interval is.

    It catches a source that emits a buffer only when its flush interval expires,
    which the round trips miss because their intervals are short.
    Bind a config whose flush interval never expires during a test, so a buffer can only arrive because it is full.
    The rows fill one buffer and part of a second.
    The first buffer must arrive as a strict prefix of what was seeded, while the interval holds back the partly filled second.
    """
    ctx.setup()
    ctx.connect()
    ctx.seed(ctx.rows)
    expected = ctx.data.encode(ctx.rows)
    observed = ctx.src.fill(0, 1).observed
    if not observed or len(observed) >= len(expected) or not expected.startswith(observed):
        raise AssertionError(
            f"expected the first full buffer, a strict prefix of the {len(expected)} seeded bytes; got {len(observed)} bytes"
        )
    ctx.disconnect()


@scenario("partial_buffer_held", default_buffers=1)
def partial_buffer_held(ctx: Ctx) -> None:
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
    result = ctx.src.fill(*ctx.data.fill_counts(ctx.rows))
    ctx.data.compare_fill(ctx.rows, observed=result.observed)
    reads = result.got.partial_read_ms
    if not reads or min(reads) < interval_ms:
        raise AssertionError(
            f"expected every partly filled buffer to be held for the {interval_ms} ms flush interval; "
            f"the reads that returned one took {list(reads)} ms"
        )
    ctx.disconnect()


@scenario("crash_recovery", default_buffers=2)
def crash_recovery(ctx: Ctx) -> None:
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
