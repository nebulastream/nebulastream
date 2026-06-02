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

from conntest_runner.discovery import binding_problems, discover_plugins, resolve_configs
from conntest_runner.harness import Sink, Source
from conntest_runner.link import Link
from conntest_runner.scenarios import Ctx, get_scenario, run_case, scenario_names

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PLUGINS = list(discover_plugins(_REPO_ROOT, kinds=("Sources", "Sinks")))

# Static validation of every scenario binding (unknown scenario name,
# unsupported needs, missing data file, invalid outcome, dangling config glob,
# orphan config). Computed once: `_BINDING_PROBLEMS` drives a loud per-defect
# guard test, and `_BROKEN_SCENARIOS` lets `_cases` skip a defective scenario
# so its defect surfaces once (in the guard) instead of also crashing/dropping
# a half-built conformance case. A clean tree leaves both empty.
_KNOWN_SCENARIOS = set(scenario_names())
_BINDING_PROBLEMS = [
    bp for plugin in _PLUGINS for bp in binding_problems(plugin, known_scenarios=_KNOWN_SCENARIOS)
]
_BROKEN_SCENARIOS = {
    (bp.plugin, bp.scenario) for bp in _BINDING_PROBLEMS if bp.scenario is not None
}
_DEAD_ENDPOINT = "127.0.0.1:1"
_GLOBAL_DEADLINE_S = float(os.environ.get("CONNTEST_GLOBAL_DEADLINE_S", "120"))

# Sink drain tuning (threads, buffer_size). The concurrent scenario's 8 threads x
# tiny buffers are the TSan signal. The outage scenarios step whole buffers (write
# one, cut, write the next), so the dataset must span >=2 buffers; for a formatted
# sink a buffer is a buffer_size-bounded batch, so shrink it enough that the small
# fixture splits (a native sink's .nes already has many buffers, so this is a
# harmless no-op there). Default is generous buffers, few threads.
_SINK_TUNING: dict[str, tuple[int, int]] = {
    "concurrent": (8, 128),
    "outage_delivery": (4, 2048),
    "outage_loss": (4, 2048),
}


def _sink_tuning(name: str) -> tuple[int, int]:
    return _SINK_TUNING.get(name, (4, 65536))


def _resolve_connector_endpoint(plugin, scenario, worker_id: str) -> tuple[str, Link | None]:
    """Resolve the endpoint the *connector* config points at, plus any link.

    A needs="link" scenario routes the connector through toxiproxy (which the
    runner brought up) and returns the created `Link` so the caller can clean it
    up; the loader still seeds/reads the real backend directly. A bad_endpoint
    scenario points at a dead endpoint; everything else points at the real one.
    """
    if scenario.name == "bad_endpoint":
        return _DEAD_ENDPOINT, None
    if scenario.needs == "link":
        port = plugin.loader_cls.compose_port
        link = Link(
            name=f"conntest-{plugin.name}-{worker_id}".lower(),
            listen_port=port,
            upstream=compose_endpoint(plugin),
        ).create()
        return f"toxiproxy:{port}", link
    return compose_endpoint(plugin), None


def _cases() -> list:
    """Build one param per (plugin, scenario, matched-config).

    Restricted to the active setup_file group so a config reused across setup
    files runs once per group.
    """
    active = os.environ.get("CONNTEST_SETUP_FILE") or None
    params = []
    for plugin in _PLUGINS:
        for scen in plugin.scenarios:
            if scen.setup_file != active:
                continue
            if (plugin.name, scen.name) in _BROKEN_SCENARIOS:
                continue  # a binding defect — surfaced by test_scenario_binding_is_valid
            for spec in scen.configs:
                for cfg in resolve_configs(plugin.conn_test_dir, [spec.glob]):
                    suffix = f"-{active}" if active else ""
                    params.append(
                        pytest.param(
                            plugin,
                            scen,
                            cfg,
                            spec.expect,
                            id=f"{scen.name}-{plugin.name}-{cfg.stem}{suffix}",
                        )
                    )
    return params


@pytest.mark.parametrize(
    "problem",
    [pytest.param(bp, id=f"{bp.plugin}-{bp.scenario or 'plugin'}") for bp in _BINDING_PROBLEMS],
)
def test_scenario_binding_is_valid(problem) -> None:
    """Every scenario binding must be well-formed.

    A known catalog scenario, a supported ``needs``, an existing ``data`` file,
    a valid ``outcome``, a config glob that matches ≥1 file, and no orphan
    config (see ``discovery.binding_problems``). A defect would otherwise drop
    the scenario silently from ``_cases`` or crash it mid-run; here it is one
    loud, isolated failure that names the plugin, scenario, and fix — the rest
    of the battery still runs. When the tree is clean this parametrization is
    empty (no-op).
    """
    pytest.fail(problem.message)


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
    data_model = getattr(loader, "data_model", None)
    target = loader.make_target(
        test_id=config_path.stem, scenario=scenario.name, worker_id=worker_id
    )

    connector_ep, link = _resolve_connector_endpoint(plugin, scenario, worker_id)

    # config validation binds the pristine file; every other scenario renders.
    if scenario.name == "config":
        cfg_file = config_path
    else:
        cfg_file = render_config(
            plugin, config_path, endpoint=connector_ep, target=target, out_dir=tmp_path
        )

    # The scenario's bound input (or None) — same value for either kind, so
    # compute it once. `bind()` is pure, so hoisting it out of the kind branches
    # does not change behaviour.
    scenario_input = scenario.data.bind(plugin.conn_test_dir) if scenario.data is not None else None

    extra: list[str] = []
    stepper = None
    try:
        if plugin.kind == "Sources":
            observed_path = None
            if data_model is not None and getattr(data_model, "needs_observed", False):
                observed_path = tmp_path / "observed.bin"
                extra.append(f"--observed-path={observed_path}")
                if data_model.native_row_width:
                    extra.append(f"--native-row-width={data_model.native_row_width}")
            stepper = Source(
                harness=harness_binary,
                config=cfg_file,
                observed_path=observed_path,
                extra_args=extra,
                global_deadline_s=_GLOBAL_DEADLINE_S,
                native_debug=native_debug,
            )
            ctx = Ctx(
                kind=plugin.kind,
                loader=loader,
                data=data_model,
                target=target,
                source=stepper,
                input=scenario_input,
                link=link,
            )
        else:
            if scenario.data is not None:
                threads, buffer_size = _sink_tuning(scenario.name)
                bound = scenario.data.bind(plugin.conn_test_dir)
                input_path = tmp_path / "input.bytes"
                # The model decides what the harness reads: formatted sinks get
                # the source bytes; a native sink hands over a native `.nes`.
                harness_bytes = (
                    data_model.harness_input(bound) if data_model is not None else bound.bytes()
                )
                input_path.write_bytes(harness_bytes)
                n_records = len(bound.records())
                extra += [
                    f"--input-path={input_path}",
                    f"--threads={threads}",
                    f"--buffer-size={buffer_size}",
                    f"--buffer-count={n_records + threads + 16}",
                ]
            stepper = Sink(
                harness=harness_binary,
                config=cfg_file,
                extra_args=extra,
                global_deadline_s=_GLOBAL_DEADLINE_S,
                native_debug=native_debug,
            )
            ctx = Ctx(
                kind=plugin.kind,
                loader=loader,
                data=data_model,
                target=target,
                sink=stepper,
                input=scenario_input,
                link=link,
            )
        run_case(get_scenario(scenario.name), ctx, expect)
    finally:
        if stepper is not None:
            stepper.cleanup()
        if link is not None:
            link.cleanup()
