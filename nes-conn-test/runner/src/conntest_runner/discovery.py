# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Loads a connector's conn-test loader and its scenario bindings."""

from __future__ import annotations

import importlib.util
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from conntest_runner.contracts import SourceLoader
from conntest_runner.protocol import describe, resolve_code

if TYPE_CHECKING:
    from pathlib import Path
    from types import ModuleType


class IsolationContractError(RuntimeError):
    """A plugin's ``compose.yaml`` does not declare exactly one service with an ``expose:`` port."""


@dataclass(frozen=True)
class BindingProblem:
    """``scenario`` is ``None`` for a defect of the plugin as a whole, such as an orphan config."""

    plugin: str
    scenario: str | None
    message: str


@dataclass(frozen=True)
class Expectation:
    """Unless ``ok``, the case must fail with a connector-origin error of ``code``, and in ``phase`` if one is given."""

    ok: bool
    code: int | None = None
    phase: str | None = None

    def __str__(self) -> str:
        if self.ok:
            return "OK"
        # An ERROR expectation always has a code.
        # The guard is for the type checker.
        code = describe(self.code) if self.code is not None else "?"
        return f"ERROR {code}" + (f" @{self.phase}" if self.phase else "")


# `ERROR <code> [phase]`
_ERROR_MIN_PARTS = 2
_PHASE_INDEX = 2


def parse_expectation(spec: str) -> Expectation:
    text = spec.strip()
    if text == "OK":
        return Expectation(ok=True)
    parts = text.split()
    if parts[0] != "ERROR" or len(parts) < _ERROR_MIN_PARTS:
        raise ValueError(f"bad expectation {spec!r}: expected 'OK' or 'ERROR <code|Name> [phase]'")
    code = resolve_code(parts[1])
    phase = parts[_PHASE_INDEX] if len(parts) > _PHASE_INDEX else None
    return Expectation(ok=False, code=code, phase=phase)


@dataclass(frozen=True)
class ConfigSpec:
    """One config glob and its expected outcome.

    An invalid outcome is stored as ``error``, not raised,
    so it fails only its own scenario and discovery continues.
    """

    glob: str
    outcome: str
    expect: Expectation | None
    error: str | None = None


def _make_config_spec(glob: str, outcome: str) -> ConfigSpec:
    try:
        return ConfigSpec(glob=glob, outcome=outcome, expect=parse_expectation(outcome))
    except (ValueError, IndexError) as e:
        # An empty outcome raises IndexError.
        msg = str(e) or f"empty or malformed outcome {outcome!r}"
        return ConfigSpec(glob=glob, outcome=outcome, expect=None, error=msg)


class Scenario:
    """One scenario binding.

    Plugins create it through the constructors in ``scenario_bindings.py``,
    which accept only the arguments that each scenario allows.
    A bare glob in ``configs`` expects OK.
    """

    def __init__(
        self,
        name: str,
        *,
        config: str | None = None,
        configs: list[tuple[str, str] | str] | None = None,
        outcome: str = "OK",
        n_buffers: int | None = None,
    ) -> None:
        specs: list[ConfigSpec] = []
        for entry in configs or []:
            glob, oc = entry if isinstance(entry, (tuple, list)) else (entry, "OK")
            specs.append(_make_config_spec(glob, oc))
        if config is not None:
            specs.append(_make_config_spec(config, outcome))
        self.name = name
        self.configs = specs
        self.n_buffers = n_buffers

    def __repr__(self) -> str:
        return (
            f"Scenario(name={self.name!r}, configs={self.configs!r}, "
            f"n_buffers={self.n_buffers!r})"
        )


@dataclass(frozen=True)
class PluginEntry:
    name: str
    conn_test_dir: Path
    loader_cls: type
    # `<service>:<port>` from conn-test/compose.yaml, not declared by the loader, because the framework owns the topology.
    # None for a connector without a compose.yaml, whose loader starts the server itself.
    endpoint: str | None
    scenarios: list[Scenario] = field(default_factory=list)

    def scenarios_named(self, name: str) -> list[Scenario]:
        return [s for s in self.scenarios if s.name == name]

    def has_scenario(self, name: str) -> bool:
        return any(s.name == name for s in self.scenarios)


def resolve_configs(conn_test_dir: Path, globs: list[str]) -> list[Path]:
    base = conn_test_dir / "configs"
    # De-duplicates a file that several globs match and keeps the first-seen order.
    return list(dict.fromkeys(p for g in globs for p in sorted(base.glob(g))))


def _is_loader_class(obj: object) -> bool:
    # The loader module imports the abstract base too.
    # Only a concrete subclass can be instantiated.
    return isinstance(obj, type) and issubclass(obj, SourceLoader) and not getattr(obj, "__abstractmethods__", None)


def _import(path: Path, module_name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def read_plugin_scenarios(conn_test_dir: Path) -> list[Scenario]:
    """Imports only the pure-data ``scenarios.py``, never the loader,
    so it works on a host without the loader's dependencies.
    """
    scenarios_py = conn_test_dir / "scenarios.py"
    if not scenarios_py.exists():
        raise FileNotFoundError(f"{conn_test_dir.parent.name}: no conn-test/scenarios.py")
    mod = _import(scenarios_py, f"conntest_scn_{conn_test_dir.parent.name.lower()}")
    scenarios: list[Scenario] = mod.SCENARIOS
    return scenarios


def _compose_service_names(compose_yaml: Path) -> set[str]:
    """A line-based parse, so that the runner needs no YAML library."""
    names: set[str] = set()
    in_services = False
    service_indent: int | None = None
    for raw in compose_yaml.read_text(encoding="utf-8").splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip())
        if not in_services:
            if indent == 0 and stripped.split("#", 1)[0].strip() == "services:":
                in_services = True
            continue
        if indent == 0:
            break
        if service_indent is None:
            service_indent = indent
        if indent == service_indent and ":" in stripped:
            names.add(stripped.split(":", 1)[0].strip())
    return names


def _compose_endpoint(conn_test_dir: Path) -> str | None:
    compose_yaml = conn_test_dir / "compose.yaml"
    if not compose_yaml.exists():
        return None
    services = sorted(_compose_service_names(compose_yaml))
    if len(services) != 1:
        raise IsolationContractError(
            f"{conn_test_dir.parent.name}: conn-test/compose.yaml must declare "
            f"exactly one service (its data tier); found {services}."
        )
    port = _expose_port(compose_yaml)
    if port is None:
        raise IsolationContractError(
            f"{conn_test_dir.parent.name}: service {services[0]!r} declares no "
            f"`expose:` port in conn-test/compose.yaml — the framework needs it to "
            f"build the connector endpoint."
        )
    return f"{services[0]}:{port}"


def _expose_port(compose_yaml: Path) -> int | None:
    """Only the inline form ``expose: ["1883"]`` is recognized.

    Comments are stripped, so an ``expose:`` in a comment is never taken as the port.
    """
    for raw in compose_yaml.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if line.startswith("expose:"):
            match = re.search(r"\d+", line)
            if match is not None:
                return int(match.group())
    return None


def _orphan_configs(conn_test_dir: Path, scenarios: list[Scenario]) -> list[Path]:
    """A config that no scenario glob matches would never run, so it is reported as a defect."""
    configs_dir = conn_test_dir / "configs"
    if not configs_dir.is_dir():
        return []
    all_configs = {p.resolve() for p in configs_dir.rglob("*.nesql")}
    if not all_configs:
        return []
    globs = [spec.glob for s in scenarios for spec in s.configs]
    referenced = {p.resolve() for p in resolve_configs(conn_test_dir, globs)}
    return sorted(p.relative_to(configs_dir) for p in (all_configs - referenced))


def binding_problems(
    entry: PluginEntry,
    *,
    known_scenarios: set[str],
) -> list[BindingProblem]:
    """Defects are returned, not raised, so each fails one isolated case while the rest of the battery runs.

    The catalog's scenario names are passed in because the catalog module imports this one.
    """
    problems: list[BindingProblem] = []

    def add(scenario_name: str | None, message: str) -> None:
        problems.append(BindingProblem(plugin=entry.name, scenario=scenario_name, message=message))

    ct = entry.conn_test_dir
    for scenario in entry.scenarios:
        if scenario.name not in known_scenarios:
            add(
                scenario.name,
                f"scenario {scenario.name!r} is not a catalog scenario "
                f"(known: {sorted(known_scenarios)})",
            )
        for spec in scenario.configs:
            if spec.error is not None:
                add(
                    scenario.name,
                    f"scenario {scenario.name!r} has an invalid outcome {spec.outcome!r}: {spec.error}",
                )
            if not resolve_configs(ct, [spec.glob]):
                add(
                    scenario.name,
                    f"scenario {scenario.name!r} references config glob {spec.glob!r}, "
                    f"which matches no file under {ct.name}/configs/",
                )
    for orphan in _orphan_configs(ct, entry.scenarios):
        add(None, f"config file {orphan} under configs/ is referenced by no scenario glob")
    return problems


def registered_connectors(build_dir: Path, repo_root: Path) -> dict[str, Path]:
    """The connectors that called add_conn_test, from the list that CMake writes into the build dir.

    One line per connector, with the directory relative to the repo root: `TCPSource nes-plugins/Sources/TCPSource/conn-test`.
    """
    listing = build_dir / "nes-conn-test" / "connectors.txt"
    if not listing.is_file():
        raise FileNotFoundError(f"{listing} does not exist; configure the build with tests enabled")
    return {
        name: repo_root / rel
        for name, rel in (line.split(maxsplit=1) for line in listing.read_text(encoding="utf-8").splitlines() if line.strip())
    }


def discover_plugin(name: str, conn_test_dir: Path) -> PluginEntry:
    loader_py = conn_test_dir / "loader.py"
    if not loader_py.is_file():
        raise FileNotFoundError(f"{name}: no {loader_py}")
    loader_mod = _import(loader_py, f"conntest_plugin_{name.lower()}_loader")
    loader_classes = [v for v in vars(loader_mod).values() if _is_loader_class(v)]
    if len(loader_classes) != 1:
        raise RuntimeError(
            f"{name}: loader.py must define exactly one concrete subclass of "
            f"conntest_runner.contracts.SourceLoader, found {len(loader_classes)}"
        )
    return PluginEntry(
        name=name,
        conn_test_dir=conn_test_dir,
        loader_cls=loader_classes[0],
        endpoint=_compose_endpoint(conn_test_dir),
        scenarios=read_plugin_scenarios(conn_test_dir),
    )
