# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The single, generic conformance battery.

One parametrized test over (plugin, scenario, config-case) for every
discovered Source and Sink plugin. It builds a `Ctx` (stepper + loader +
data model + rendered config) and runs the named catalog function through
`run_case`, which enforces the declared OK/ERROR expectation. The six
bespoke per-scenario functions and the source/sink split are gone — a new
connector or scenario is pure data in the plugin's ``scenarios.py``.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

# Fixtures resolved by name in this module's namespace.
from _conformance_shared import (  # noqa: F401
    compose_endpoint,
    harness_binary,  # pytest fixture re-export (used by tests in this file)
    render_config,
    require_connector_registered,
)

from conntest_runner.datamodel import SinkModel, SourceModel, parse_sink_schema, parse_source_schema
from conntest_runner.discovery import binding_problems, discover_plugins, resolve_configs
from conntest_runner.harness import Sink, Source
from conntest_runner.link import Link
from conntest_runner.naming import unique_token
from conntest_runner.scenarios import (
    Ctx,
    get_scenario,
    get_spec,
    run_case,
    scenario_is_bind_only,
    scenario_names,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PLUGINS = list(discover_plugins(_REPO_ROOT, kinds=("Sources", "Sinks")))

# Static validation of every scenario binding (unknown scenario name,
# unsupported needs, missing data file, invalid outcome, dangling config glob,
# orphan config). Computed once: `_BINDING_PROBLEMS` drives a loud per-defect
# guard test, and `_BROKEN_SCENARIOS` lets `_cases` skip a defective scenario
# so its defect surfaces once (in the guard) instead of also crashing/dropping
# a half-built conformance case. A clean tree leaves both empty.
_KNOWN_SCENARIOS = set(scenario_names())
# Per-scenario knob misuse (n_buffers on a locked scenario, latency_ms outside
# slow_link, a missing required n_buffers) is unrepresentable now that bindings
# go through the conntest_runner.scenario_bindings constructors, so binding_problems only
# validates the scenario name, outcome strings, and config globs.
_BINDING_PROBLEMS = [
    bp for plugin in _PLUGINS for bp in binding_problems(plugin, known_scenarios=_KNOWN_SCENARIOS)
]
_BROKEN_SCENARIOS = {
    (bp.plugin, bp.scenario) for bp in _BINDING_PROBLEMS if bp.scenario is not None
}
_DEAD_ENDPOINT = "127.0.0.1:1"
_GLOBAL_DEADLINE_S = float(os.environ.get("CONNTEST_GLOBAL_DEADLINE_S", "120"))

# Sink worker threads. Every sink scenario drains through the sink on this many
# concurrent workers — concurrency is the reality for a sink and its biggest
# surface for races, so it is the TSan signal on the whole battery, not a
# separate scenario.
_SINK_THREADS = 8


# Buffer size for sizing a generator dataset: "N buffers" derives the row count as
# N * (buffer_size // row_width) (design §2), where row_width is the engine's own
# (from the harness BIND reply). For a native sink this is *also* the harness
# --buffer-size, so the engine's JSON decode packs the same rows_per_buffer and
# emits exactly N native buffers. For a source it only controls the data quantity
# (a source is not buffer-exact); for a formatted sink it sizes the row count, and
# the harness slices the resulting JSONL into record batches of the same size.
_SOURCE_BUFFER_SIZE = 512
_SINK_BUFFER_SIZE = 512
# Sink session pool buffers (the sink's own working set). The native decode pool is
# sized by the harness from the JSONL; the formatted units are unpooled — so the
# session pool no longer scales with the row count and a modest constant suffices.
_SINK_POOL_BUFFERS = 256
# The generator seed. Every dataset is generated from one fixed seed — the runs
# stay deterministic/replayable and nothing varies it (the per-binding knob was
# never used), so it is a single constant rather than a scenario field.
_DEFAULT_SEED = 1


def _effective_buffers(scenario) -> int | None:
    """How many buffers this case drives, or ``None`` for a lifecycle-only one.

    The catalog scenario sets the default (and whether it is locked); a binding
    may override it via ``n_buffers=`` only on a non-locked scenario (a locked
    override is a binding defect, caught by ``binding_problems`` and skipped).
    """
    spec = get_spec(scenario.name)
    if spec.default_buffers is None:
        return None
    if scenario.n_buffers is not None and not spec.buffers_locked:
        return scenario.n_buffers
    return spec.default_buffers


def _effective_latency(scenario) -> int | None:
    """The fault-link latency (ms/direction) this case applies, or None.

    None for a non-latency scenario; otherwise the catalog default
    (``default_latency_ms``) unless the binding overrides it via
    ``Scenario(latency_ms=N)``.
    """
    spec = get_spec(scenario.name)
    if spec.default_latency_ms is None:
        return None
    return scenario.latency_ms if scenario.latency_ms is not None else spec.default_latency_ms


def _effective_step_timeout(scenario) -> int | None:
    """The per-step harness watchdog (ms) this case wants, or None for the default.

    None ⇒ the harness's own default (``--step-timeout-ms`` unset). Otherwise the
    catalog default (``default_step_timeout_ms``) unless the binding overrides it
    via the scenario constructor's ``step_timeout_ms``.
    """
    spec = get_spec(scenario.name)
    binding = getattr(scenario, "step_timeout_ms", None)
    if binding is not None:
        return binding
    return spec.default_step_timeout_ms


def _build_model(plugin, scenario, config_path):
    """Build the schema-bound data model for this case from the config's schema.

    Returns ``(model, schema)``, both ``None`` when no schema is in play. Both kinds
    are schema-driven from the config under test (a new valid config "just works"): a
    sink parses its CREATE SINK, a source its CREATE LOGICAL SOURCE. The model is
    connector-agnostic — the framework builds it from the kind, not from a loader
    declaration. The dataset itself is built later (in ``test_scenario``), once the
    harness BIND has reported the engine's native row width and (for a source) its FILL
    unit. The BIND-only scenarios (``config_valid``/``config_invalid``) only BIND the
    pristine config; non-data scenarios (empty/bad_endpoint) seed nothing — neither
    needs a model.
    """
    if plugin.kind == "Sinks":
        schema = parse_sink_schema(config_path.read_text(encoding="utf-8"))
        return SinkModel(schema), schema
    if plugin.kind == "Sources":
        schema = parse_source_schema(config_path.read_text(encoding="utf-8"))
        return SourceModel(schema), schema
    return None, None


def _resolve_connector_endpoint(plugin, scenario, worker_id: str) -> tuple[str, Link | None]:
    """Resolve the endpoint the *connector* config points at, plus any link.

    A needs_link scenario routes the connector through toxiproxy (which the
    runner brought up) and returns the created `Link` so the caller can clean it
    up; the loader still seeds/reads the real backend directly. A bad_endpoint
    scenario points at a dead endpoint; everything else points at the real one.
    """
    if scenario.name == "bad_endpoint":
        return _DEAD_ENDPOINT, None
    if get_spec(scenario.name).needs_link:
        port = plugin.compose_port
        link = Link(
            name=f"conntest-{plugin.name}-{worker_id}".lower(),
            listen_port=port,
            upstream=compose_endpoint(plugin),
        ).create()
        return f"toxiproxy:{port}", link
    return compose_endpoint(plugin), None


def _primary_setup_file(plugin) -> str | None:
    """The setup_file of a plugin's first service-backed scenario, or None.

    BIND-only scenarios (config_valid/config_invalid) declare no setup_file —
    they spin up no service — so they fold into this primary group, mirroring the
    CLI's ``_setup_file_groups``. A plugin whose scenarios pin no file has None as
    its primary (the single default group).
    """
    for scen in plugin.scenarios:
        if not scenario_is_bind_only(scen.name) and scen.setup_file:
            return scen.setup_file
    return None


def _cases() -> list:
    """Build one param per (plugin, scenario, matched-config).

    Restricted to the active setup_file group so a config reused across setup
    files runs once per group. A BIND-only scenario pins no file, so it folds
    into the plugin's primary group (``_primary_setup_file``) and runs once there.
    """
    active = os.environ.get("CONNTEST_SETUP_FILE") or None
    params = []
    for plugin in _PLUGINS:
        primary = _primary_setup_file(plugin)
        for scen in plugin.scenarios:
            effective = scen.setup_file if scen.setup_file is not None else primary
            if effective != active:
                continue
            if (plugin.name, scen.name) in _BROKEN_SCENARIOS:
                continue  # a binding defect — surfaced by test_scenario_binding_is_valid
            # A generator-driven case is replayable from its (fixed) seed + N: put
            # N in the test id so a failure names exactly what to reproduce (§7).
            n_buffers = _effective_buffers(scen)
            gen_suffix = f"-N{n_buffers}" if n_buffers is not None else ""
            for spec in scen.configs:
                for cfg in resolve_configs(plugin.conn_test_dir, [spec.glob]):
                    suffix = f"-{active}" if active else ""
                    params.append(
                        pytest.param(
                            plugin,
                            scen,
                            cfg,
                            spec.expect,
                            id=f"{scen.name}-{plugin.name}-{cfg.stem}{gen_suffix}{suffix}",
                        )
                    )
    return params


@pytest.mark.parametrize(
    "problem",
    [pytest.param(bp, id=f"{bp.plugin}-{bp.scenario or 'plugin'}") for bp in _BINDING_PROBLEMS],
)
def test_scenario_binding_is_valid(problem) -> None:
    """Every scenario binding must be well-formed.

    A known catalog scenario, an ``n_buffers`` override only where allowed, a
    valid ``outcome``, a config glob that matches ≥1 file, and no orphan config
    (see ``discovery.binding_problems``). A defect would otherwise drop
    the scenario silently from ``_cases`` or crash it mid-run; here it is one
    loud, isolated failure that names the plugin, scenario, and fix — the rest
    of the battery still runs. When the tree is clean this parametrization is
    empty (no-op).
    """
    pytest.fail(problem.message)


def _source_observed_args(tmp_path: Path) -> tuple[Path, list[str]]:
    """Harness args for a source FILL: where to dump the observed JSONL.

    Every source emits JSONL — a NATIVE source's tuples re-encoded through the engine's
    JSON output formatter, an opaque source's JSONL carried through — so the runner only
    needs the dump path; it parses the JSONL back itself. No layout numbers cross the wire.
    """
    observed_path = tmp_path / "observed.jsonl"
    return observed_path, [f"--observed-path={observed_path}"]


@pytest.mark.parametrize(("plugin", "scenario", "config_path", "expect"), _cases())
def test_scenario(
    plugin,
    scenario,
    config_path,
    expect,
    harness_binary: Path,  # noqa: F811  # pytest injects the re-exported fixture
    tmp_path: Path,
    worker_id: str,
    native_debug,
) -> None:
    loader = plugin.loader_cls(compose_endpoint(plugin))
    # Per-test resource name (topic/table) — framework policy, not the loader's:
    # unique across tests and parallel xdist workers sharing one service.
    target = "conntest_" + unique_token(
        test_id=config_path.stem, scenario=scenario.name, worker_id=worker_id
    )

    data_model, schema = _build_model(plugin, scenario, config_path)
    # A data scenario (round_trip/outage/reconnect/...) has a buffer count; the
    # dataset is built once the harness BIND reports the engine's row width.
    n_buffers = _effective_buffers(scenario)

    connector_ep, link = _resolve_connector_endpoint(plugin, scenario, worker_id)

    # config validation binds the pristine file; every other scenario renders.
    if scenario_is_bind_only(scenario.name):
        cfg_file = config_path
    else:
        cfg_file = render_config(
            plugin, config_path, endpoint=connector_ep, target=target, out_dir=tmp_path
        )

    extra: list[str] = []
    # A scenario may raise the per-step harness watchdog (--step-timeout-ms) for
    # connector work that legitimately runs long; lift the global deadline above it
    # too, so the per-step bound is the one that governs (not the 120 s backstop).
    step_timeout_ms = _effective_step_timeout(scenario)
    deadline_s = _GLOBAL_DEADLINE_S
    if step_timeout_ms is not None:
        extra.append(f"--step-timeout-ms={step_timeout_ms}")
        deadline_s = max(deadline_s, step_timeout_ms / 1000 + 60)
    stepper = None
    try:
        if plugin.kind == "Sources":
            observed_path, src_extra = _source_observed_args(tmp_path)
            extra += src_extra
            stepper = Source(
                harness=harness_binary,
                config=cfg_file,
                observed_path=observed_path,
                extra_args=extra,
                global_deadline_s=deadline_s,
                native_debug=native_debug,
            )
            dataset = None
            if n_buffers is not None:
                # BIND first to learn the engine's native row width, then build the exact
                # dataset that fills N buffers. The later OPEN reuses the process. The
                # connector kind never reaches the runner — FILL sends both a row count and
                # a byte length, and the harness picks the one matching how it counts.
                row_width = stepper.bind().row_width
                dataset = data_model.make_dataset(
                    n_buffers, _DEFAULT_SEED, _SOURCE_BUFFER_SIZE, row_width
                )
            ctx = Ctx(
                kind=plugin.kind,
                loader=loader,
                data=data_model,
                target=target,
                source=stepper,
                dataset=dataset,
                link=link,
                schema=schema,
                expect=expect,
                latency_ms=_effective_latency(scenario),
            )
        else:
            # All sink args are constants now (no dataset dependency): a native sink's
            # --buffer-size sets the engine's decode rows-per-buffer (=> exactly N
            # buffers), a formatted sink slices its JSONL into same-size record batches.
            input_path = tmp_path / "input.jsonl"
            if n_buffers is not None:
                extra += [
                    f"--input-path={input_path}",
                    f"--threads={_SINK_THREADS}",
                    f"--buffer-size={_SINK_BUFFER_SIZE}",
                    f"--buffer-count={_SINK_POOL_BUFFERS}",
                ]
            stepper = Sink(
                harness=harness_binary,
                config=cfg_file,
                extra_args=extra,
                global_deadline_s=deadline_s,
                native_debug=native_debug,
            )
            dataset = None
            if n_buffers is not None:
                # BIND first for the row width, build the dataset, then write the
                # JSONL the (not-yet-issued) START will read.
                row_width = stepper.bind().row_width
                dataset = data_model.make_dataset(
                    n_buffers, _DEFAULT_SEED, _SINK_BUFFER_SIZE, row_width
                )
                input_path.write_bytes(data_model.harness_input(dataset))
            ctx = Ctx(
                kind=plugin.kind,
                loader=loader,
                data=data_model,
                target=target,
                sink=stepper,
                dataset=dataset,
                link=link,
                schema=schema,
                expect=expect,
                latency_ms=_effective_latency(scenario),
            )
        run_case(get_scenario(scenario.name), ctx, expect)
    finally:
        if stepper is not None:
            stepper.cleanup()
        if link is not None:
            link.cleanup()
