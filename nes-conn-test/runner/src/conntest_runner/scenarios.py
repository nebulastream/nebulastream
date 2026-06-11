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

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from conntest_runner.protocol import ConnectorError, HarnessError, describe

# The `silent_link` scenario stalls the link this long — long enough to trip an
# idle budget / keepalive that the continuous flow of `slow_link` never lets
# accumulate.
_SILENCE_HOLD_S = 15.0

if TYPE_CHECKING:
    from collections.abc import Callable

    from conntest_runner.discovery import Expectation
    from conntest_runner.harness import Sink, Source

# ── @scenario registry ─────────────────────────────────────────────────


@dataclass(frozen=True)
class ScenarioSpec:
    """A catalog scenario: its function plus the properties intrinsic to it.

    These three were once declared per *plugin* on every binding; they are really
    properties of the *scenario* and live here so the catalog is their single
    source of truth (a plugin just names the scenario):

      * ``default_buffers`` — how many buffers of data the scenario drives, or
        ``None`` for a lifecycle-only scenario (empty/config/bad_endpoint) that
        seeds nothing.
      * ``buffers_locked`` — whether the count is structurally fixed. The
        buffer-stepping fault scenarios require exactly N (≥2 to step across a
        cut), and the fixed-tier round trips pin a specific count, so a plugin
        may not override it; ``binding_problems`` rejects an ``n_buffers=`` on a
        locked scenario.
      * ``buffers_required`` — whether the scenario has no canonical count, so a
        binding *must* pass ``n_buffers=`` to say how many; ``binding_problems``
        rejects a binding that omits it. Mutually exclusive in spirit with
        ``buffers_locked`` (locked forbids the override, required demands it).
      * ``needs_link`` — whether the scenario routes the connector through the
        toxiproxy ``Link`` (its body calls ``ctx.link.cut()/restore()``).
      * ``default_latency_ms`` — for a latency scenario (``slow_link``), the delay
        the fault link adds per direction; ``None`` for every other scenario. A
        binding may override it via ``Scenario(latency_ms=N)`` (``binding_problems``
        rejects a ``latency_ms`` on a scenario that takes none). The catalog is the
        source of truth for the default, mirroring ``default_buffers``.
      * ``default_step_timeout_ms`` — the per-step harness watchdog (``--step-timeout-ms``)
        this scenario wants, or ``None`` to use the harness default (30 s). A
        scenario whose connector work legitimately takes longer (a deliberate stall,
        a high-latency link) raises it so the watchdog doesn't cut the step short;
        a binding may override per connector. The runner also lifts the global
        deadline above it so the step bound is the one that governs.
      * ``bind_only`` — whether the scenario only BINDs the pristine config
        (``config_valid``/``config_invalid``) and so spins up no live service of its
        own. It binds the config as-authored (no endpoint/target rewrite), needs no
        schema/dataset, and is exempt from the ``setup_file`` contract — the runner
        folds it into the plugin's primary stack group rather than letting it pin a
        file or form a service-less group of its own.
    """

    name: str
    fn: Callable[[Ctx], None]
    default_buffers: int | None = None
    buffers_locked: bool = False
    buffers_required: bool = False
    needs_link: bool = False
    default_latency_ms: int | None = None
    default_step_timeout_ms: int | None = None
    bind_only: bool = False


_CATALOG: dict[str, ScenarioSpec] = {}


def scenario(
    name: str,
    *,
    default_buffers: int | None = None,
    buffers_locked: bool = False,
    buffers_required: bool = False,
    needs_link: bool = False,
    default_latency_ms: int | None = None,
    default_step_timeout_ms: int | None = None,
    bind_only: bool = False,
) -> Callable[[Callable[[Ctx], None]], Callable[[Ctx], None]]:
    """Register the decorated function in the catalog under ``name``."""

    def register(fn: Callable[[Ctx], None]) -> Callable[[Ctx], None]:
        _CATALOG[name] = ScenarioSpec(
            name=name,
            fn=fn,
            default_buffers=default_buffers,
            buffers_locked=buffers_locked,
            buffers_required=buffers_required,
            needs_link=needs_link,
            default_step_timeout_ms=default_step_timeout_ms,
            default_latency_ms=default_latency_ms,
            bind_only=bind_only,
        )
        return fn

    return register


def get_scenario(name: str) -> Callable[[Ctx], None]:
    """Look up a catalog function by name, raising ``KeyError`` if unknown."""
    return get_spec(name).fn


def get_spec(name: str) -> ScenarioSpec:
    """Look up a catalog scenario's spec by name, raising ``KeyError`` if unknown."""
    if name not in _CATALOG:
        raise KeyError(f"unknown scenario {name!r}; catalog has {sorted(_CATALOG)}")
    return _CATALOG[name]


def scenario_needs_link(name: str) -> bool:
    """Whether the named scenario routes the connector through the fault link.

    Host-safe and unknown-name-tolerant (returns ``False``) so the CLI can decide
    whether to merge the toxiproxy sidecar without importing the loader or
    tripping over a misnamed binding (``binding_problems`` flags that separately).
    """
    spec = _CATALOG.get(name)
    return spec is not None and spec.needs_link


def scenario_is_bind_only(name: str) -> bool:
    """Whether the named scenario only BINDs the pristine config (no service).

    Host-safe and unknown-name-tolerant (returns ``False``) so the CLI's
    setup_file grouping and the conformance test can special-case the
    ``config_valid``/``config_invalid`` scenarios — which bind the config as-authored
    and spin up no live service — without importing the loader.
    """
    spec = _CATALOG.get(name)
    return spec is not None and spec.bind_only


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
    data: Any  # the data model (SourceModel | SinkModel), duck-typed at runtime
    target: str
    source: Source | None = None
    sink: Sink | None = None
    dataset: Any = None  # the prebuilt Dataset (generator-driven), or None for lifecycle-only cases
    link: Any = None  # Link (needs_link scenarios), else None — runtime-provided, duck-typed
    # The schema parsed from the config (CREATE SINK / CREATE LOGICAL SOURCE),
    # or None for the schema-less `config_valid`/`config_invalid` scenarios. A native sink's
    # setup/read_back and the ODBC source's setup are schema-driven; the
    # framework threads it through here. Loaders that don't key off it (a
    # formatted/byte connector) accept schema=None.
    schema: Any = None
    # The declared outcome of this case. The fault scenarios read ``expect.lossy``
    # to pick the read-back oracle (full dataset vs the subset that survived an
    # outage); run_case enforces the error contract separately.
    expect: Expectation | None = None
    # The fault-link latency (ms/direction) a latency scenario applies, resolved
    # from the catalog default + any per-binding override; None for every other
    # scenario. ``slow_link`` reads it to throttle ``ctx.link``.
    latency_ms: int | None = None
    _n_buffers: int = 0  # sink: buffers the harness materialized (from START)
    _buf_cursor: int = 0  # sink: buffers written so far

    @property
    def is_source(self) -> bool:
        """Whether this case drives a Source (vs a Sink)."""
        return self.kind == "Sources"

    @property
    def expect_lossy(self) -> bool:
        """Whether the declared outcome is a LOSS (the during-outage batch is gone)."""
        return self.expect is not None and self.expect.lossy

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

    def read_back(self, expect_records: int) -> Any:
        """Read what the sink wrote, as typed rows.

        The schema is passed unconditionally: a NATIVE loader SELECTs its columns;
        an opaque loader parses the records it collected (via ``jsonl_to_rows``) back
        into rows keyed by it. Both return typed rows the model multiset-compares.
        """
        return self.loader.read_back(
            target=self.target, schema=self.schema, expect_records=expect_records
        )

    # -- lifecycle ------------------------------------------------------
    def setup(self) -> None:
        """Run the pre-connect backend hook (create table / register subscription).

        Always called: ``setup`` is part of the loader contract (see
        ``conntest_runner.contracts.Loader``), so a loader with nothing to prepare
        implements it empty rather than omitting it. The schema is passed
        unconditionally; a loader that does not key off it accepts ``schema=None``.
        """
        self.loader.setup(target=self.target, schema=self.schema)

    def connect(self, *, resume: bool = False) -> None:
        """Open the source / start the sink.

        For a sink, START reports how many buffers it materialized from the
        input; remember it so the scenario can step whole buffers. ``resume``
        (sink only) re-STARTs at the already-committed buffer offset
        (``_buf_cursor``) after a crash, so the respawned sink writes only the
        tail instead of re-writing the whole input — leaving ``_buf_cursor``
        untouched. A source resumes naturally (its read position is owned by the
        backend/broker, not the harness), so ``resume`` is a no-op there.
        """
        if self.is_source:
            self.src.open()
        elif resume:
            self._n_buffers = self.snk.start(start_offset=self._buf_cursor).n_buffers
        else:
            self._n_buffers = self.snk.start().n_buffers
            self._buf_cursor = 0

    def disconnect(self) -> None:
        """Close the source / stop the sink."""
        if self.is_source:
            self.src.close()
        else:
            self.snk.stop()

    def kill(self) -> None:
        """SIGKILL the underlying harness process (ungraceful death).

        The next ``connect()`` respawns a fresh process + connector. Used by
        ``crash_recovery`` to model a worker dying mid-stream.
        """
        (self.src if self.is_source else self.snk).kill()

    def bind(self) -> None:
        """Bind/validate the config on whichever side this case drives."""
        (self.src if self.is_source else self.snk).bind()

    # -- data transfer --------------------------------------------------
    def source_batches(self, k: int) -> list[Any]:
        """Source: split the prebuilt dataset's rows into ``k`` seed batches."""
        return list(self.dataset.seed_batches(k))

    def seed_and_fill(self, rows: Any) -> None:
        """Source: seed one batch of rows, fill its quota, compare the read-back.

        The loader writes the typed rows to its backend however it must (INSERT, or
        serialize-and-publish — backend IO is the loader's job); the model then derives
        the FILL quota in the source's native unit (from the BIND-reported kind) and
        multiset-compares the observed JSONL parsed back to typed rows against ``rows``.
        """
        self.loader.seed_batch(rows, target=self.target)
        result = self.src.fill(*self.data.fill_counts(rows))
        self.data.compare_fill(rows, observed=result.observed)

    def transfer(self) -> None:
        """Seed→fill→compare (source) / write-all (sink).

        Source: seed and fill the whole dataset (round_trip's single batch); the
        multi-batch scenarios call seed_and_fill with explicit batches instead.
        Sink: drain every remaining buffer; the read-back compare is deferred to
        finish() — it must run after STOP drains the sink's delivery tokens, and
        compares against the dataset rows (the oracle).
        """
        if self.is_source:
            self.seed_and_fill(self.dataset.rows())
        else:
            self.write_rest()

    def write_buffers(self, n: int) -> int:
        """Sink: drain the next ``n`` buffers; return the records actually written."""
        got = self.snk.write(n)
        self._buf_cursor += n
        return got.units_written

    def write_rest(self) -> int:
        """Sink: drain all buffers not yet written; return records written."""
        return self.write_buffers(max(self._n_buffers - self._buf_cursor, 0))

    def verify_rows(self, expected_rows: Any) -> None:
        """Sink: read back what was written and multiset-compare to ``expected_rows``."""
        received = self.read_back(len(expected_rows))
        self.data.verify_readback(expected_rows, received)

    def finish(self) -> None:
        """Read back what a sink wrote and multiset-compare against the oracle.

        No-op for sources (their compare happened in transfer). The dataset rows
        are the oracle (the sink was fed exactly them); the loader returns typed rows
        (a NATIVE loader SELECTs them, an opaque loader parses its records) and the
        model verifies rows-equal.
        """
        if self.is_source:
            return
        self.verify_rows(self.dataset.rows())


# ── catalog ────────────────────────────────────────────────────────────


def _round_trip(ctx: Ctx) -> None:
    """The base round-trip body, shared verbatim by every ``round_trip_*`` tier.

    Source: seed and fill the generated rows, compare the observed stream. Sink:
    write every buffer, then read back and compare against the dataset rows. The
    buffer count is a property of the registered ``ScenarioSpec`` (resolved by
    ``_effective_buffers`` before this runs), so the data path is identical and
    never branches on N — the tiers differ only in metadata.
    """
    ctx.setup()
    ctx.connect()
    ctx.transfer()
    ctx.disconnect()
    ctx.finish()


@scenario("round_trip_1_buffer", default_buffers=1, buffers_locked=True)
def round_trip_1_buffer(ctx: Ctx) -> None:
    """Single-buffer round trip — the 0→1 boundary (``empty`` covers 0)."""
    _round_trip(ctx)


@scenario("round_trip_10_buffers", default_buffers=10, buffers_locked=True)
def round_trip_10_buffers(ctx: Ctx) -> None:
    """Ten-buffer round trip — the default functional tier."""
    _round_trip(ctx)


@scenario("round_trip_100_buffers", default_buffers=100, buffers_locked=True)
def round_trip_100_buffers(ctx: Ctx) -> None:
    """Hundred-buffer round trip — sustained-churn tier."""
    _round_trip(ctx)


@scenario("round_trip_n_buffers", default_buffers=8, buffers_required=True)
def round_trip_n_buffers(ctx: Ctx) -> None:
    """Configurable round trip — the only tier that reads ``Scenario(n_buffers=N)``.

    A binding must set ``n_buffers`` (``buffers_required``); the
    ``default_buffers=8`` only makes it a data + overridable scenario and is never
    used in practice.
    """
    _round_trip(ctx)


@scenario("empty")
def empty(ctx: Ctx) -> None:
    """Lifecycle-only (design C3): open → close with nothing seeded.

    Asserts both succeed.
    """
    ctx.setup()
    ctx.connect()
    ctx.disconnect()


@scenario("config_valid", bind_only=True)
def config_valid(ctx: Ctx) -> None:
    """BIND a config that must bind clean (OK).

    The valid-config half of the old ``config`` scenario: every matched file must
    BIND without error (run_case enforces the OK outcome). BIND-only — it
    binds the pristine file and touches no live service.
    """
    ctx.bind()


@scenario("config_invalid", bind_only=True)
def config_invalid(ctx: Ctx) -> None:
    """BIND a config that must be rejected with a declared error code.

    The invalid-config half of the old ``config`` scenario: each binding pairs a
    config glob with the error code its rejection must carry (run_case enforces the
    ERROR <code> outcome). BIND-only — same shape as ``config_valid``, only the
    declared outcome differs.
    """
    ctx.bind()


@scenario("bad_endpoint")
def bad_endpoint(ctx: Ctx) -> None:
    """OPEN/START against a dead endpoint — must raise the declared error.

    The error is CannotOpenSource/Sink, or a connector-specific code.
    """
    ctx.connect()


@scenario("reconnect", default_buffers=2, buffers_locked=True, needs_link=True)
def reconnect(ctx: Ctx) -> None:
    """needs_link: establish + first batch, sever + restore, second batch.

    Then close. OK iff the connector recovers.
    """
    ctx.setup()
    ctx.connect()
    if ctx.is_source:
        head, tail = ctx.source_batches(2)
        ctx.seed_and_fill(head)
        ctx.link.cut()
        ctx.link.restore()
        if ctx.expect_lossy:
            # A short idle budget lets the source self-terminate during the gap, so
            # the second batch — published after the link heals — is never delivered.
            # Seed it and prove it is lost (the fill observes nothing).
            ctx.loader.seed_batch(tail, target=ctx.target)
            result = ctx.src.fill(*ctx.data.fill_counts(tail))
            ctx.data.compare_fill([], observed=result.observed)
        else:
            ctx.seed_and_fill(tail)
    else:
        # Sink: one buffer with the link up, the rest after sever+restore. A sink
        # that recovers writes them all (finish compares the whole dataset); one
        # that doesn't (ODBCSink) raises on the post-restore write — declared ERROR.
        ctx.write_buffers(1)
        ctx.link.cut()
        ctx.link.restore()
        ctx.write_rest()
    ctx.disconnect()
    ctx.finish()


@scenario("crash_recovery", default_buffers=2, buffers_locked=True)
def crash_recovery(ctx: Ctx) -> None:
    """SIGKILL the harness mid-stream; a fresh process re-opens and resumes.

    Distinct from ``reconnect`` (a *warm* link cut while the connector lives):
    here the connector *process* dies with no CLOSE/STOP — modelling a worker
    killed by, say, an unrelated query — and a brand-new connector must take
    over the backend residue a dead open left behind. OK iff the round-trip
    completes across the death. No fault link (``needs_link``) — the process
    death is the fault.

      * SOURCE — read the first batch, die, re-open, read the second. The read
        position is backend-owned, so the fresh connector resumes past the
        first batch on its own: a broker session redelivers from its position;
        a time-windowed poll's watermark resets to the restart instant, which
        already excludes the pre-crash rows.
      * SINK — commit the first buffer, die, then re-START at that committed
        offset (``resume=True``) so the respawned sink writes only the tail. The
        runner owns the sink's position, so the resume is deterministic and the
        read-back stays duplicate-free.
    """
    ctx.setup()
    ctx.connect()
    if ctx.is_source:
        head, tail = ctx.source_batches(2)
        ctx.seed_and_fill(head)
        ctx.kill()
        ctx.connect()
        ctx.seed_and_fill(tail)
    else:
        ctx.write_buffers(1)
        ctx.kill()
        ctx.connect(resume=True)
        ctx.write_rest()
    ctx.disconnect()
    ctx.finish()


def _seed_during_outage(ctx: Ctx) -> tuple[Any, Any]:
    """Shared setup for the outage scenarios.

    Subscribe, sever the link, publish batch A *while the connector is
    offline*, heal the link, then publish batch B after the connector has
    reconnected. Returns the two row batches (A, B).
    """
    ctx.setup()
    ctx.connect()
    a, b = ctx.source_batches(2)
    ctx.link.cut()
    # batch A is volatile — published while the connector is offline
    ctx.loader.seed_batch(a, target=ctx.target, qos=0)
    ctx.link.restore()
    # batch B is durable — published after the connector has reconnected
    ctx.loader.seed_batch(b, target=ctx.target, qos=1)
    return a, b


@scenario("outage", default_buffers=2, buffers_locked=True, needs_link=True)
def outage(ctx: Ctx) -> None:
    """A batch is in flight while the link to the broker is cut.

    The steps are uniform; whether the in-flight batch survives is a property of
    the connector's configuration, and the declared outcome picks the read-back
    oracle: ``OK`` expects the whole dataset back, ``LOSS`` expects the during-cut
    batch gone (``ctx.expect_lossy``). The two sides realize the cut differently
    (design: the broker/near-link disappearing vs the far subscriber
    disappearing):

      * SOURCE — the *far* subscriber (this source) is offline while a QoS-0
        batch A is published; whether the *broker* queues it for the offline
        durable subscription is the queue_qos0_messages setting (selected by the
        mounted setup_file). Delivered → fill A+B and assert the observed stream
        is exactly A-then-B (NES assumes in-order/sequence-tagged delivery, so a
        lost OR reordered A both fail). Lost → B is the sentinel: filling B's
        quota yields exactly B, deterministically proving A was dropped. Either
        way the connector returns OK, so a source LOSS carries no error code.
      * SINK — the *near* link to the broker drops while the sink writes the
        during-cut batch. A buffering sink (send_while_disconnected) queues it
        and flushes on reconnect, so every row is read back (OK). A non-buffering
        sink rejects the write with a connector error — the declared LOSS <code>
        — and read_back (straight to the recipient, not through the proxy)
        confirms only the pre-cut batch survived.
    """
    if ctx.is_source:
        _, b = _seed_during_outage(ctx)  # A injected during the cut, B after
        expected = b if ctx.expect_lossy else ctx.dataset.rows()  # B-only vs A++B
        result = ctx.src.fill(*ctx.data.fill_counts(expected))
        ctx.data.compare_fill(expected, observed=result.observed)
        ctx.disconnect()
        return
    # Sink: write the first buffer with the link up, then the during-cut batch
    # while the link is severed (a stronger, deterministic test than writing
    # after restore). read_back is the positive confirmation in either outcome.
    ctx.setup()
    ctx.connect()
    kept_n = ctx.write_buffers(1)  # buffer 0, link up; records actually written
    ctx.link.cut()
    if not ctx.expect_lossy:
        ctx.write_rest()  # buffered by the sink while offline
        ctx.link.restore()
        ctx.disconnect()  # stop → flush the buffer on reconnect
        ctx.finish()  # expect every row back (the whole dataset)
        return
    # LOSS: the during-cut write physically cannot get through. A non-buffering
    # sink rejects it with a connector error (the declared LOSS <code>); capture
    # it and surface it after confirming only the pre-cut batch reached the
    # recipient (the buffers preserve input order, so it is the first kept_n).
    err = None
    try:
        ctx.write_buffers(1)  # buffer 1 into the cut link → rejected/lost
    except ConnectorError as e:
        err = e  # captured, surfaced below as the declared outcome
    ctx.link.restore()
    ctx.disconnect()
    ctx.verify_rows(ctx.dataset.rows()[:kept_n])
    if err is not None:
        raise err  # a non-buffering sink's rejection IS the declared LOSS <code>


@scenario(
    "slow_link", default_buffers=2, buffers_locked=True, needs_link=True, default_latency_ms=200
)
def slow_link(ctx: Ctx) -> None:
    """A degraded-but-alive link: bytes still flow, just slowly.

    needs_link: apply a bidirectional latency toxic *before* connecting (the link
    is slow from the start — handshake included), then run a normal full transfer.
    OK iff the connector tolerates the latency without dropping data or mistaking
    a slow link for a dead one / end-of-stream — the "slow ≠ broken" property,
    distinct from ``outage``/``crash_recovery`` (where bytes actually stop).

    One of the two degraded-but-alive scenarios: this one probes the *rate*
    axis (continuous flow, per-operation delay), ``silent_link`` the
    *continuity* axis (a single long byte-free interval). They discriminate
    independently — see ``silent_link``'s docstring.

    The delay is ``ctx.latency_ms`` per direction — the catalog default
    (``default_latency_ms``), overridable per binding via ``Scenario(latency_ms=N)``
    for a connector whose timing wants a different point.
    """
    assert ctx.latency_ms is not None, "slow_link needs a latency (catalog default or override)"  # noqa: S101
    ctx.setup()
    ctx.link.throttle(latency_ms=ctx.latency_ms)
    ctx.connect()
    ctx.transfer()
    ctx.disconnect()
    ctx.finish()


@scenario("silent_link", default_buffers=2, buffers_locked=True, needs_link=True)
def silent_link(ctx: Ctx) -> None:
    """A long *silence* on a live connection the connector must bridge.

    Silence the link (``Link.silence`` stalls all bytes in both directions —
    the connection stays fully established: no FIN, no RST), hold the silence
    for ``_SILENCE_HOLD_S``, lift it, then send a second batch. No data moves
    during the gap; the gap's *length* is what is under test.

    What this flushes out is a connector that mistakes silence for failure: an
    idle/read deadline that declares end-of-stream, a keepalive that expires, a
    "no traffic = dead peer" heuristic. A connector that simply keeps the
    connection open bridges it (OK); one whose own budget fires during the gap
    declares how it fails — error on the post-gap work, or loss of the second
    batch. Distinct from ``reconnect``/``outage``, whose *cut* severs the
    connection: a connector may legitimately treat a severed connection as
    fatal, but silence on a live connection is a normal stretch of any real
    deployment's life.

    ``slow_link`` is the sibling on the other axis of the degraded-but-alive
    family: there bytes flow continuously (just late), so no idle timer ever
    accumulates; here one long byte-free interval is exactly what lets it.
    The two discriminate independently — a connector can pass one and fail
    the other (MQTTSource's short-idle-budget config passes slow_link and
    loses the post-gap batch here).

    The hold is a deliberate, bounded fault duration injected by the scenario (not
    a wait-and-hope sleep — there is no state to poll); the global deadline bounds
    the case.
    """
    ctx.setup()
    ctx.connect()
    if ctx.is_source:
        head, tail = ctx.source_batches(2)
        ctx.seed_and_fill(head)
        ctx.link.silence()
        time.sleep(_SILENCE_HOLD_S)
        ctx.link.unsilence()
        if ctx.expect_lossy:
            # The connector did not bridge the gap (its own idle budget fired
            # during the silence): the second batch, seeded after the gap,
            # never reaches the source that already gave up — the post-gap
            # read observes nothing (a connector-OK source LOSS).
            ctx.loader.seed_batch(tail, target=ctx.target)
            result = ctx.src.fill(*ctx.data.fill_counts(tail))
            ctx.data.compare_fill([], observed=result.observed)
        else:
            ctx.seed_and_fill(tail)
    else:
        # Sink: a connector that bridges the gap writes the rest on the same
        # still-live connection (finish compares the whole dataset); one whose
        # own timeout killed the connection during the silence raises on the
        # post-gap write — the declared ERROR.
        ctx.write_buffers(1)
        ctx.link.silence()
        time.sleep(_SILENCE_HOLD_S)
        ctx.link.unsilence()
        ctx.write_rest()
    ctx.disconnect()
    ctx.finish()


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
