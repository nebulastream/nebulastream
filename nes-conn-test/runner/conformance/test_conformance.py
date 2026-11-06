# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The conformance battery of one connector: one test per scenario and matched config."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from conntest_runner.config_rewrite import apply_overrides
from conntest_runner.datamodel import SourceModel, parse_source_schema
from conntest_runner.discovery import PluginEntry, binding_problems, discover_plugin, resolve_configs
from conntest_runner.harness import Source
from conntest_runner.naming import unique_token
from conntest_runner.scenarios import (
    Ctx,
    get_scenario,
    get_spec,
    run_case,
    scenario_names,
)

if "CONNTEST_PLUGIN" not in os.environ or "CONNTEST_DIR" not in os.environ:
    raise RuntimeError("run the battery through `conntest run`, which sets CONNTEST_PLUGIN and CONNTEST_DIR")
_PLUGINS = [discover_plugin(os.environ["CONNTEST_PLUGIN"], Path(os.environ["CONNTEST_DIR"]))]

# Each binding defect fails once in its own test, and the defective scenario is left out of the cases.
_BINDING_PROBLEMS = [
    bp
    for plugin in _PLUGINS
    for bp in binding_problems(plugin, known_scenarios=set(scenario_names()))
]
_BROKEN_SCENARIOS = {
    (bp.plugin, bp.scenario) for bp in _BINDING_PROBLEMS if bp.scenario is not None
}
_GLOBAL_DEADLINE_S = float(os.environ.get("CONNTEST_GLOBAL_DEADLINE_S", "120"))

# The harness runs with this buffer size, and N buffers of data are N times it.
_BUFFER_SIZE = 4096
# One fixed seed keeps every run replayable.
_DEFAULT_SEED = 1


def _buffers(scenario) -> int | None:
    """The binding's buffer count, or else the scenario's default."""
    if scenario.n_buffers is not None:
        return scenario.n_buffers
    return get_spec(scenario.name).default_buffers


def _render_config(loader, config_path: Path, *, endpoint: str, target: str, out_dir: Path) -> Path:
    text = config_path.read_text(encoding="utf-8")
    rendered = apply_overrides(text, loader.config_overrides(target=target, endpoint=endpoint))
    out = out_dir / "config.nesql"
    out.write_text(rendered, encoding="utf-8")
    return out


def _cases() -> list:
    params = []
    for plugin in _PLUGINS:
        for scenario in plugin.scenarios:
            if (plugin.name, scenario.name) in _BROKEN_SCENARIOS:
                continue
            # The seed is fixed, so N in the test id is enough to reproduce a failure.
            n_buffers = _buffers(scenario)
            gen_suffix = f"-N{n_buffers}" if n_buffers is not None else ""
            for spec in scenario.configs:
                for config_path in resolve_configs(plugin.conn_test_dir, [spec.glob]):
                    params.append(
                        pytest.param(
                            plugin,
                            scenario,
                            config_path,
                            spec.expect,
                            id=f"{scenario.name}-{plugin.name}-{config_path.stem}{gen_suffix}",
                        )
                    )
    return params


@pytest.mark.parametrize(
    "problem",
    [pytest.param(bp, id=f"{bp.plugin}-{bp.scenario or 'plugin'}") for bp in _BINDING_PROBLEMS],
)
def test_scenario_binding_is_valid(problem) -> None:
    pytest.fail(problem.message)


@pytest.mark.parametrize(("plugin", "scenario", "config_path", "expect"), _cases())
def test_scenario(
    plugin: PluginEntry,
    scenario,
    config_path: Path,
    expect,
    harness_binary: Path,
    tmp_path: Path,
    worker_id: str,
) -> None:
    loader = plugin.loader_cls(plugin.endpoint)
    target = "conntest_" + unique_token(
        test_id=config_path.stem, scenario=scenario.name, worker_id=worker_id
    )

    connector_endpoint = loader.dead_endpoint if scenario.name == "bad_endpoint" else loader.endpoint
    if connector_endpoint is None:
        pytest.fail(f"{plugin.name} has no compose service, so its loader must set its endpoint")
    cfg_file = _render_config(loader, config_path, endpoint=connector_endpoint, target=target, out_dir=tmp_path)
    schema = parse_source_schema(config_path.read_text(encoding="utf-8"))
    data_model = SourceModel(schema)

    observed_path = tmp_path / "observed.jsonl"
    with Source(
        harness=harness_binary,
        config=cfg_file,
        observed_path=observed_path,
        extra_args=[f"--observed-path={observed_path}", f"--buffer-size={_BUFFER_SIZE}"],
        global_deadline_s=_GLOBAL_DEADLINE_S,
        # Kept next to the config and the observed bytes, so a failed case has all of its artifacts in one place.
        log_path=tmp_path / "harness.log",
    ) as source:
        n_buffers = _buffers(scenario)
        ctx = Ctx(
            loader=loader,
            data=data_model,
            target=target,
            source=source,
            rows=data_model.make_rows(n_buffers, _DEFAULT_SEED, _BUFFER_SIZE) if n_buffers is not None else [],
            schema=schema,
        )
        run_case(get_scenario(scenario.name), ctx, expect)
