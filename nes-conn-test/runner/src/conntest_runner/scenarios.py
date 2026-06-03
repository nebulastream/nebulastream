# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""scenarios.py — the framework scenario catalog + generic outcome runner.

Scenarios are imperative, connector- and shape-blind functions registered
by name in one shared catalog (design §4). A connector does not write
them — it opts in via its ``conn-test/scenarios.py`` and supplies the
binding (config, data, setup_file, expected outcome). The function is run
against a `Ctx` the generic conformance test builds per parametrized case.

The catalog functions write only the *ideal* path; `run_case` turns a
declared `Expectation` into the pass/fail decision:

  * OK  → the function must complete cleanly.
  * ERROR <code> → it must raise a `ConnectorError` with that code (and
    optional phase). A `HarnessError` (infra failure / step timeout) is
    never an expected outcome and always fails.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from conntest_runner.protocol import ConnectorError, HarnessError, describe

if TYPE_CHECKING:
    from collections.abc import Callable

    from conntest_runner.discovery import Expectation
    from conntest_runner.harness import Sink, Source

# ── @scenario registry ─────────────────────────────────────────────────

_CATALOG: dict[str, Callable[[Ctx], None]] = {}


def scenario(name: str) -> Callable[[Callable[[Ctx], None]], Callable[[Ctx], None]]:
    """Register the decorated function in the catalog under ``name``."""

    def register(fn: Callable[[Ctx], None]) -> Callable[[Ctx], None]:
        _CATALOG[name] = fn
        return fn

    return register


def get_scenario(name: str) -> Callable[[Ctx], None]:
    """Look up a catalog function by name, raising ``KeyError`` if unknown."""
    if name not in _CATALOG:
        raise KeyError(f"unknown scenario {name!r}; catalog has {sorted(_CATALOG)}")
    return _CATALOG[name]


def scenario_names() -> list[str]:
    """Return the sorted names of every registered scenario."""
    return sorted(_CATALOG)


# ── per-case context ───────────────────────────────────────────────────


@dataclass
class Ctx:
    """Everything one parametrized (plugin, scenario, config) case needs.

    The generic conformance test builds it; the catalog functions drive it. The
    kind-specific verbs (open/close vs start/stop, seed vs read_back) are
    abstracted behind the methods so the catalog stays shape-blind.
    """

    kind: str  # "Sources" | "Sinks"
    loader: Any  # bound loader instance — runtime-provided, duck-typed
    data: Any  # the data model (ByteModel | RowModel | NativeModel), duck-typed at runtime
    target: str
    source: Source | None = None
    sink: Sink | None = None
    input: Any = None  # bound DataSource (File/Generate/_Records), runtime-provided
    link: Any = None  # Link (needs="link"), else None — runtime-provided, duck-typed
    # The native sink schema (parsed from the CREATE SINK), or None for a
    # formatted/byte sink and for sources. A native sink's setup/read_back are
    # schema-driven; the framework threads it through here.
    schema: Any = None
    _sink_written: list[Any] = field(default_factory=list)  # loaded datasets (runtime-shaped)
    _n_buffers: int = 0  # sink: buffers the harness materialized (from START)
    _buf_cursor: int = 0  # sink: buffers written so far

    @property
    def is_source(self) -> bool:
        """Whether this case drives a Source (vs a Sink)."""
        return self.kind == "Sources"

    @property
    def src(self) -> Source:
        """The Source this case drives (raises if not a source case)."""
        if self.source is None:
            raise RuntimeError("Ctx.src used on a non-source case")
        return self.source

    @property
    def snk(self) -> Sink:
        """The Sink this case drives (raises if not a sink case)."""
        if self.sink is None:
            raise RuntimeError("Ctx.snk used on a non-sink case")
        return self.sink

    @property
    def is_native(self) -> bool:
        """Whether this case's model is a native-tuple (schema-driven) sink."""
        return bool(getattr(self.data, "is_native", False))

    def read_back(self, expect_records: int) -> Any:
        """Read what the sink wrote, passing the schema for a native sink.

        A native loader's read_back is schema-driven (it SELECTs the schema's
        columns and returns typed rows); a formatted loader's is not.
        """
        if self.is_native:
            return self.loader.read_back(
                target=self.target, schema=self.schema, expect_records=expect_records
            )
        return self.loader.read_back(target=self.target, expect_records=expect_records)

    # -- lifecycle ------------------------------------------------------
    def setup(self) -> None:
        """Run the pre-connect backend hook (create table / register subscription).

        Skipped when the loader declares none. A native sink's setup is
        schema-driven (it maps the schema to its backend's DDL).
        """
        fn = getattr(self.loader, "setup", None)
        if not callable(fn):
            return
        if self.is_native:
            fn(target=self.target, schema=self.schema)
        else:
            fn(target=self.target)

    def connect(self) -> None:
        """Open the source / start the sink.

        For a sink, START reports how many buffers it materialized from the
        input; remember it so the scenario can step whole buffers.
        """
        if self.is_source:
            self.src.open()
        else:
            self._n_buffers = self.snk.start().n_buffers
            self._buf_cursor = 0

    def disconnect(self) -> None:
        """Close the source / stop the sink."""
        if self.is_source:
            self.src.close()
        else:
            self.snk.stop()

    def validate(self) -> None:
        """Validate the bound config on whichever side this case drives."""
        (self.src if self.is_source else self.snk).validate()

    # -- data transfer --------------------------------------------------
    def transfer(self, src: Any) -> None:
        """Seed→fill→compare (source) / write-all→stash (sink).

        For a sink this drains every remaining buffer (round_trip's whole-dataset
        write); the multi-batch scenarios step buffers explicitly instead. The
        sink read-back compare is deferred to finish() — it must run after STOP
        drains the sink's delivery tokens.
        """
        data: Any = self.data.load(src)  # ByteData | RowData, correlated with the model at runtime
        if self.is_source:
            self.loader.seed(data, target=self.target)
            result = self.src.fill(self.data.fill_quota(data))
            self.data.compare_fill(data, got=result.got, observed=result.observed)
        else:
            self.write_rest()
            self._sink_written.append(data)

    def write_buffers(self, n: int) -> int:
        """Sink: drain the next ``n`` buffers; return the records actually written."""
        got = self.snk.write(n)
        self._buf_cursor += n
        return got.units_written

    def write_rest(self) -> int:
        """Sink: drain all buffers not yet written; return records written."""
        return self.write_buffers(max(self._n_buffers - self._buf_cursor, 0))

    def expect(self, src: Any) -> None:
        """Sink: record that everything in ``src`` should be read back.

        finish() multiset-compares the read-back against the union of these.
        transfer() does this for round_trip; the buffer-stepping scenarios call
        it explicitly since they write without going through transfer().
        """
        self._sink_written.append(self.data.load(src))

    def finish(self) -> None:
        """Read back what a sink wrote and multiset-compare.

        No-op for sources (their compare happened in transfer). Shape-blind: the
        model combines the written datasets, counts records, and verifies the
        read-back — bytes for a formatted sink, typed rows for a native one.
        """
        if self.is_source:
            return
        expected = self.data.combine(self._sink_written)
        received = self.read_back(self.data.record_count(expected))
        self.data.verify_readback(expected, received)


# ── catalog ────────────────────────────────────────────────────────────


@scenario("round_trip")
def round_trip(ctx: Ctx) -> None:
    """Subsume the old binary/large scenarios: open → transfer(File) → close.

    Covers a .bin File / a big File + sha-only compare.
    """
    ctx.setup()
    ctx.connect()
    ctx.transfer(ctx.input)
    ctx.disconnect()
    ctx.finish()


@scenario("empty")
def empty(ctx: Ctx) -> None:
    """Lifecycle-only by default (design C3): open → close with nothing seeded.

    Asserts both succeed. A connector with a deterministic EoS can opt into a
    true DRAIN-to-EoS variant
    """
    ctx.setup()
    ctx.connect()
    ctx.disconnect()


@scenario("config")
def config(ctx: Ctx) -> None:
    """VALIDATE the bound config.

    The declared outcome (OK / ERROR <code>) decides pass/fail via run_case.
    """
    ctx.validate()


@scenario("bad_endpoint")
def bad_endpoint(ctx: Ctx) -> None:
    """OPEN/START against a dead endpoint — must raise the declared error.

    The error is CannotOpenSource/Sink, or a connector-specific code.
    """
    ctx.connect()


# `concurrent` is round_trip with concurrency tuning (8 threads x tiny buffers
# — a TSan signal, applied by the runner from the scenario name). Same logic.
_CATALOG["concurrent"] = round_trip


@scenario("reconnect")
def reconnect(ctx: Ctx) -> None:
    """needs="link": establish + first batch, sever + restore, second batch.

    Then close. OK iff the connector recovers.
    """
    ctx.setup()
    ctx.connect()
    if ctx.is_source:
        ctx.transfer(ctx.input.head())
        ctx.link.cut()
        ctx.link.restore()
        ctx.transfer(ctx.input.tail())
    else:
        # Sink: one buffer with the link up, the rest after sever+restore. A sink
        # that recovers writes them all (finish compares everything); one that
        # doesn't (ODBCSink) raises on the post-restore write — the declared ERROR.
        ctx.write_buffers(1)
        ctx.link.cut()
        ctx.link.restore()
        ctx.write_rest()
        ctx.expect(ctx.input)
    ctx.disconnect()
    ctx.finish()


@scenario("reconnect_after_death")
def reconnect_after_death(ctx: Ctx) -> None:
    """Process-death variant: kill the harness mid-scenario.

    A fresh process reconnects via open() and resumes (sources only — needs the
    persistent session).
    """
    ctx.setup()
    ctx.connect()
    ctx.transfer(ctx.input.head())
    ctx.src.kill()
    ctx.connect()  # respawns a fresh process
    ctx.transfer(ctx.input.tail())
    ctx.disconnect()
    ctx.finish()


def _seed_during_outage(ctx: Ctx) -> tuple[Any, Any]:
    """Shared setup for the outage scenarios.

    Subscribe, sever the link, publish buffer A *while the connector is
    offline*, heal the link, then publish buffer B after the connector has
    reconnected.
    """
    ctx.setup()
    ctx.connect()
    a = ctx.data.load(ctx.input.head())
    b = ctx.data.load(ctx.input.tail())
    ctx.link.cut()
    ctx.loader.seed(a, target=ctx.target, qos=0)  # volatile, during the outage
    ctx.link.restore()
    ctx.loader.seed(b, target=ctx.target, qos=1)  # durable, after reconnect
    return a, b


@scenario("outage_delivery")
def outage_delivery(ctx: Ctx) -> None:
    """A buffer sent *during* the outage survives and is delivered.

    Realized on each side of the broker by a different mechanism (design:
    differentiate the broker/near-link disappearing from the far subscriber
    disappearing):

      * SOURCE — the *far* subscriber (this source) is offline while a QoS-0
        buffer is published; the *broker* must queue it for the offline durable
        subscription (queue_qos0_messages=true). We fill A+B and assert the
        observed stream is exactly A-then-B — NES assumes in-order/
        sequence-tagged delivery, so a lost OR reordered A both fail.
      * SINK — the *near* link to the broker drops while the sink writes; the
        sink's own client must buffer the write (send_while_disconnected) and
        flush it on reconnect. No broker setting is involved (the message never
        reaches the broker during the cut).
    """
    if ctx.is_source:
        _seed_during_outage(ctx)
        expected: Any = ctx.data.load(ctx.input)  # A++B, in publish order
        result = ctx.src.fill(ctx.data.fill_quota(expected))
        ctx.data.compare_fill(expected, got=result.got, observed=result.observed)
        ctx.disconnect()
        return
    # Sink: write the first buffer with the link up, then the rest WHILE the link
    # is cut (the sink client buffers them), restore, and read everything back.
    # The during-the-cut write is a stronger, deterministic buffering test than
    # writing after restore.
    ctx.setup()
    ctx.connect()
    ctx.write_buffers(1)  # first buffer, link up
    ctx.link.cut()
    ctx.write_rest()  # buffered by the sink while offline
    ctx.link.restore()
    ctx.disconnect()  # stop → flush the buffer on reconnect
    ctx.expect(ctx.input)  # expect every record back
    ctx.finish()


@scenario("outage_loss")
def outage_loss(ctx: Ctx) -> None:
    """A buffer injected *during* the outage is dropped (uniform steps).

    The drop mechanism differs by side of the broker (the mirror of
    outage_delivery):

      * SOURCE — a QoS-0 buffer A is published while the source is the offline
        subscriber; the broker does not queue QoS-0 for it
        (queue_qos0_messages=false). B is the sentinel: filling B's quota yields
        exactly B, deterministically proving A was lost (had it survived it
        would arrive before B, or out of order after it).
      * SINK — toxiproxy sits between the sink and its recipient. We write the
        first batch with the link up, then write a second batch WHILE the link
        is cut, so it physically cannot get through. A sink that does not buffer
        rejects that write with a connector error. That error is the scenario's declared
        outcome. The loss is also confirmed positively: read_back — which reads
        the recipient directly, not through the proxy — must see only the first
        batch. A sink that instead drops the write silently (no error) leaves
        the outcome OK; the declared expectation is what tells the two apart.
    """
    if ctx.is_source:
        _, b = _seed_during_outage(ctx)
        result = ctx.src.fill(ctx.data.fill_quota(b))
        ctx.data.compare_fill(b, got=result.got, observed=result.observed)
        ctx.disconnect()
        return
    # Sink: one buffer with the link up, then one buffer WHILE the link is cut so
    # it physically cannot get through. A sink that does not buffer rejects that
    # write with a connector error (the declared outcome). The loss is also
    # confirmed positively: read_back must see exactly the first buffer's records
    # — identified by the count the harness reports it actually wrote (the buffers
    # preserve input order, so it is the first `kept_n` records).
    ctx.setup()
    ctx.connect()
    model = ctx.data  # ByteModel or NativeModel — driven through shape-blind hooks
    kept_n = ctx.write_buffers(1)  # buffer 0, link up; records actually written
    ctx.link.cut()
    err = None
    try:
        ctx.write_buffers(1)  # buffer 1 into the cut link → rejected/lost
    except ConnectorError as e:
        err = e  # captured, surfaced below as the declared outcome
    ctx.link.restore()
    ctx.disconnect()
    received = ctx.read_back(kept_n)
    # The buffers preserve input order, so the kept rows are the first kept_n of
    # the whole input dataset (buffer 0's records).
    expected = model.prefix(model.load(ctx.input), kept_n)
    model.verify_readback(expected, received)  # only the kept buffer
    if err is not None:
        raise err  # a non-buffering sink's rejection IS the declared outcome


# ── generic outcome enforcement ─────────────────────────────────────────


def run_case(fn: Callable[[Ctx], None], ctx: Ctx, expect: Expectation) -> None:
    """Run a catalog function and enforce the declared expectation."""
    try:
        fn(ctx)
    except ConnectorError as e:
        if expect.ok:
            raise AssertionError(
                f"expected OK but raised {describe(e.code)}@{e.phase}: {e.message}"
            ) from e
        if e.code != expect.code:
            # An ERROR expectation always carries a code; the guard satisfies the
            # type-checker for the OK-default code=None (unreachable here).
            want = describe(expect.code) if expect.code is not None else "?"
            raise AssertionError(f"expected {want}, got {describe(e.code)}@{e.phase}") from e
        if expect.phase is not None and e.phase != expect.phase:
            raise AssertionError(f"expected phase {expect.phase!r}, got {e.phase!r}") from e
    except HarnessError:
        # Infra failure / step timeout — never an expected outcome.
        raise
    else:
        if not expect.ok:
            # An ERROR expectation always carries a code (see above); guard for None.
            want = describe(expect.code) if expect.code is not None else "?"
            raise AssertionError(f"expected ERROR {want} but the scenario completed cleanly")
