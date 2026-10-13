# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""discovery.py — discover per-plugin loaders + scenarios.

For each ``nes-plugins/{Sources,Sinks}/<Name>/conn-test/`` directory with
a ``loader.py``, this module imports the loader, reads its declarative
``SCENARIOS`` (from a sibling ``scenarios.py``, host-safe, or a class
attribute), validates the §5.5 isolation contract, enforces the
orphan-config guard, and yields a ``PluginEntry``.

A scenario binds a catalog function (by name) to the connector-specific
parts it needs: the config file(s) it runs against and the expected
outcome of each, the infra ``setup_file``, and an optional ``n_buffers``
override. The buffer count and whether it needs a fault ``link`` are
properties of the catalog scenario itself, not the binding. Expectations
are declared as data (``"OK"`` / ``"ERROR <code|Name> [phase]"``),
subsuming the old filename-encoded codes / ``BAD_ENDPOINT_ERROR_CODE`` /
inline asserts.
"""

from __future__ import annotations

import importlib.util
import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from conntest_runner.contracts import SinkLoader, SourceLoader
from conntest_runner.protocol import describe, resolve_code

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path
    from types import ModuleType

_LOG = logging.getLogger(__name__)


class IsolationContractError(RuntimeError):
    """A broker-tier plugin's ``compose_service`` names no sibling service.

    The named service is absent from its sibling ``compose.yaml`` (§5.5) —
    fail loudly at discovery rather than silently at harness-connect time.
    """


@dataclass(frozen=True)
class BindingProblem:
    """A static defect in one scenario binding.

    The defect is one of: an unknown scenario name, an invalid ``outcome``, a
    config glob that matches no file (dangling), or a config file no glob
    references (orphan). (Per-scenario knob misuse is caught at authoring time by
    the ``conntest_runner.scenario_bindings`` constructors, not here.)
    ``binding_problems`` *returns* these rather than raising, so the conformance
    battery turns each into one loud, isolated failing case while every
    well-formed scenario still runs — a config defect fails that one case, never
    the whole session. ``scenario`` is the offending scenario name, or ``None``
    for a plugin-level defect (an orphan config).
    """

    plugin: str
    scenario: str | None
    message: str


@dataclass(frozen=True)
class Expectation:
    """The declared outcome of a (scenario, config) case.

    ``ok`` means the case must complete cleanly; otherwise a connector-origin
    error with the given ``code`` is required (an optional explicit ``phase``
    may pin where it must surface).

    ``lossy`` marks an outage case where the batch injected *during* the outage
    is expected to be gone, so the read-back compares against the surviving
    subset rather than the full dataset. A source loss is connector-OK
    (``LOSS``, no code — the broker dropped it); a sink loss pairs the data loss
    with the connector error a non-buffering sink raises on the during-cut write
    (``LOSS <code>`` → ``ok=False`` so the error contract still applies).
    """

    ok: bool
    code: int | None = None
    phase: str | None = None
    lossy: bool = False

    def __str__(self) -> str:
        if self.lossy:
            if self.code is None:
                return "LOSS"
            return f"LOSS {describe(self.code)}"
        if self.ok:
            return "OK"
        # An ERROR expectation always carries a code (parse_expectation sets it);
        # the guard satisfies the type-checker for the OK-default `code=None`.
        code = describe(self.code) if self.code is not None else "?"
        return f"ERROR {code}" + (f" @{self.phase}" if self.phase else "")


# An ERROR expectation is ``ERROR <code> [phase]``: a verb plus at least the
# code (`_ERROR_MIN_PARTS`), with the optional phase at `_PHASE_INDEX`.
_ERROR_MIN_PARTS = 2
_PHASE_INDEX = 2


def parse_expectation(spec: str) -> Expectation:
    """Parse ``"OK"`` / ``"ERROR <code|Name> [phase]"`` / ``"LOSS [code|Name]"``.

    The code is resolved against the .inc table (a typo/retired code raises
    here, at scenario parse time). ``LOSS`` declares that an outage's during-cut
    batch is expected gone: a bare ``LOSS`` is a connector-OK loss (a source
    whose broker dropped it), ``LOSS <code>`` pairs the loss with the connector
    error a non-buffering sink raises on the during-cut write.
    """
    text = spec.strip()
    if text == "OK":
        return Expectation(ok=True)
    parts = text.split()
    if parts[0] == "LOSS":
        if len(parts) == 1:
            return Expectation(ok=True, lossy=True)
        return Expectation(ok=False, code=resolve_code(parts[1]), lossy=True)
    if parts[0] != "ERROR" or len(parts) < _ERROR_MIN_PARTS:
        raise ValueError(
            f"bad expectation {spec!r}: expected 'OK', "
            "'ERROR <code|Name> [phase]', or 'LOSS [code|Name]'"
        )
    code = resolve_code(parts[1])
    phase = parts[_PHASE_INDEX] if len(parts) > _PHASE_INDEX else None
    return Expectation(ok=False, code=code, phase=phase)


@dataclass(frozen=True)
class ConfigSpec:
    """One config glob (under ``conn-test/configs/``) and its expected outcome.

    A scenario runs once per matched file. ``outcome`` is the raw authored
    string; ``expect`` is its parsed form, or ``None`` when the string was
    invalid — in which case ``error`` carries the parse message. Parsing is
    deferred (caught, not raised, at construction) so a bad outcome fails the
    one scenario loudly via ``binding_problems`` rather than aborting discovery
    of the whole battery.
    """

    glob: str
    outcome: str
    expect: Expectation | None
    error: str | None = None


def _make_config_spec(glob: str, outcome: str) -> ConfigSpec:
    """Build a ConfigSpec, deferring an invalid-outcome error onto the spec.

    The error is carried as ``expect=None`` + ``error`` instead of raising —
    this keeps discovery resilient so one bad outcome doesn't abort the whole
    session (``binding_problems`` surfaces it per scenario).
    """
    try:
        return ConfigSpec(glob=glob, outcome=outcome, expect=parse_expectation(outcome))
    except (ValueError, IndexError) as e:
        # ValueError: bad format / UnknownErrorCode (a ValueError subclass).
        # IndexError: an empty/whitespace outcome (parse_expectation indexes
        # parts[0]). Either way the outcome is invalid, not a reason to abort.
        msg = str(e) or f"empty or malformed outcome {outcome!r}"
        return ConfigSpec(glob=glob, outcome=outcome, expect=None, error=msg)


class Scenario:
    """One scenario binding — the framework's internal carrier.

    Plugins do **not** construct this directly: they author bindings through the
    per-scenario constructors in ``conntest_runner.scenario_bindings``
    (``round_trip_10_buffers(...)``, ``slow_link(..., latency_ms=...)``, …), which
    expose only the knobs each scenario accepts and return a ``Scenario``. This
    class just holds the resulting data for discovery and the conformance battery.

    The fields: ``config`` is a single glob taking ``outcome`` (default ``"OK"``);
    ``configs`` is a list of ``(glob, outcome)`` pairs (or bare globs, defaulting
    to OK). Both normalize to ``self.configs`` (a list of ``ConfigSpec``).
    ``setup_file`` is the infra file to mount. ``n_buffers`` / ``latency_ms`` are
    per-scenario overrides set only by the constructors that accept them; the
    buffer count, fault ``link``, and default latency are properties of the
    *catalog scenario* (``ScenarioSpec``), not the binding.
    """

    def __init__(
        self,
        name: str,
        *,
        config: str | None = None,
        configs: list[tuple[str, str] | str] | None = None,
        outcome: str = "OK",
        n_buffers: int | None = None,
        latency_ms: int | None = None,
        step_timeout_ms: int | None = None,
        setup_file: str | None = None,
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
        self.latency_ms = latency_ms
        self.step_timeout_ms = step_timeout_ms
        self.setup_file = setup_file

    def __repr__(self) -> str:
        return (
            f"Scenario(name={self.name!r}, configs={self.configs!r}, "
            f"n_buffers={self.n_buffers!r}, latency_ms={self.latency_ms!r}, "
            f"step_timeout_ms={self.step_timeout_ms!r}, setup_file={self.setup_file!r})"
        )


def _coerce_scenario(
    entry: Scenario | tuple[str, list[str]] | tuple[str, list[str], str] | str,
) -> Scenario:
    if isinstance(entry, Scenario):
        return entry
    if isinstance(entry, str):
        # A bare name = a scenario with no config sweep (contributes no cases).
        return Scenario(entry)
    # Legacy tuple form: (name, [globs]) / (name, [globs], setup_file), all OK.
    name, globs, *rest = entry
    return Scenario(
        name,
        configs=[(g, "OK") for g in globs],
        setup_file=rest[0] if rest else None,
    )


@dataclass(frozen=True)
class PluginEntry:
    """One discovered plugin: its identity, conn-test dir, loader class, and scenarios."""

    name: str
    kind: str
    plugin_dir: Path
    conn_test_dir: Path
    loader_cls: type
    # The external system's compose service + port, derived from the plugin's
    # conn-test/compose.yaml (not declared on the loader — topology is framework-
    # owned). Used to build the endpoint handed to the connector and the fault link.
    compose_service: str
    compose_port: int
    scenarios: list[Scenario] = field(default_factory=list)

    def scenarios_named(self, name: str) -> list[Scenario]:
        """Return every bound scenario with the given name."""
        return [s for s in self.scenarios if s.name == name]

    def has_scenario(self, name: str) -> bool:
        """Return whether any bound scenario has the given name."""
        return any(s.name == name for s in self.scenarios)


def resolve_configs(conn_test_dir: Path, globs: list[str]) -> list[Path]:
    """Resolve config globs to matching ``.nesql`` files.

    Globs are relative to ``conn-test/configs/``; the result is de-duplicated
    and sorted.
    """
    base = conn_test_dir / "configs"
    # Ordered set: dict.fromkeys de-dups a file matched by several globs while
    # keeping first-seen order (each glob's own matches are sorted).
    return list(dict.fromkeys(p for g in globs for p in sorted(base.glob(g))))


def _is_loader_class(obj: object) -> bool:
    # A loader IS a concrete SourceLoader/SinkLoader subclass — the single contract
    # (conntest_runner.contracts), so discovery resolves it nominally. The abstract
    # bases themselves (and the SourceLoader/SinkLoader imports a plugin module
    # carries) are excluded: they have unimplemented methods, and discovery
    # instantiates the class, which only a concrete subclass allows.
    return (
        isinstance(obj, type)
        and issubclass(obj, (SourceLoader, SinkLoader))
        and not getattr(obj, "__abstractmethods__", None)
    )


def _import(path: Path, module_name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _read_scenarios(
    plugin_dir: Path, loader_cls: type, scenarios_py: Path, module_prefix: str
) -> list[Scenario]:
    """Resolve a plugin's SCENARIOS.

    A sibling ``scenarios.py`` (preferred, host-safe — never imports the
    loader) wins over a class-level ``SCENARIOS`` attribute. Each entry is a
    ``Scenario`` or a legacy tuple/bare name; all are coerced.
    """
    if scenarios_py.exists():
        scenarios_mod = _import(scenarios_py, module_prefix + "_scenarios")
        return [_coerce_scenario(e) for e in scenarios_mod.SCENARIOS]
    class_scenarios = getattr(loader_cls, "SCENARIOS", None)
    if class_scenarios is not None:
        scenarios = [_coerce_scenario(e) for e in class_scenarios]
        # setup_file must live in scenarios.py: the host reads it there to set
        # up the service *without* importing the loader. On a class attribute
        # the host can't see it, so fail loudly instead of ignoring it.
        if any(s.setup_file for s in scenarios):
            raise ValueError(
                f"{plugin_dir.name}: a scenario declares setup_file on the "
                f"loader's SCENARIOS class attribute; setup_file must be "
                f"declared in conn-test/scenarios.py (the host reads it there "
                f"to mount the service config)."
            )
        return scenarios
    raise FileNotFoundError(
        f"{plugin_dir.name}: no conn-test/scenarios.py and the loader class "
        f"{loader_cls.__name__} has no SCENARIOS class attribute"
    )


def read_plugin_scenarios(conn_test_dir: Path) -> list[Scenario] | None:
    """Host-safe read of a plugin's ``conn-test/scenarios.py``.

    Imports only that pure-data module, never the loader. Returns ``None``
    when the plugin declares its scenarios via the loader class attribute
    instead.
    """
    scenarios_py = conn_test_dir / "scenarios.py"
    if not scenarios_py.exists():
        return None
    mod = _import(scenarios_py, f"conntest_scn_{conn_test_dir.parent.name.lower()}")
    return [_coerce_scenario(e) for e in mod.SCENARIOS]


def _compose_service_names(compose_yaml: Path) -> set[str]:
    """Top-level keys under ``services:`` in a compose file.

    A deliberately minimal parser (no PyYAML dependency): finds the top-level
    ``services:`` mapping and collects the keys indented one level under it.
    """
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
            break  # next top-level key — the services mapping ended
        if service_indent is None:
            service_indent = indent
        if indent == service_indent and ":" in stripped:
            names.add(stripped.split(":", 1)[0].strip())
    return names


def _compose_service_and_port(conn_test_dir: Path) -> tuple[str, int]:
    """Derive the external system's (service, port) from the plugin's compose.yaml.

    Topology is framework-owned: the loader no longer declares it. A conn-test
    plugin's ``compose.yaml`` must declare exactly one service (its data tier)
    with an explicit ``expose:`` port; the framework reads both here and builds
    the connector's endpoint (``<service>:<port>``) from them.
    """
    compose_yaml = conn_test_dir / "compose.yaml"
    if not compose_yaml.exists():
        raise IsolationContractError(
            f"{conn_test_dir.parent.name}: no conn-test/compose.yaml — a loader "
            f"that seeds/reads an external system needs one declaring its service."
        )
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
    return services[0], port


def _expose_port(compose_yaml: Path) -> int | None:
    """The port from the sole service's ``expose:`` key, or None if absent.

    Line-based and comment-stripping (matching ``_compose_service_names``) so a
    stray ``expose:`` in a comment, or a digit in an image tag (``postgres:16``),
    is never mistaken for the port.
    """
    for raw in compose_yaml.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()  # drop comments (whole-line and trailing)
        if line.startswith("expose:"):
            match = re.search(r"\d+", line)  # inline form: expose: ["1883"]
            if match is not None:
                return int(match.group())
    return None


def _orphan_configs(conn_test_dir: Path, scenarios: list[Scenario]) -> list[Path]:
    """Config files under ``configs/`` referenced by no scenario glob.

    Returned as paths relative to ``configs/``, sorted; empty when all are
    referenced. The dual of a dangling glob — a glob that matches no file.
    Returned, not raised, so the caller can surface it as one failing case
    (design §8).
    """
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
    """Every static defect in a plugin's scenario bindings, as data.

    The defects: a scenario name absent from the catalog, an invalid ``outcome``,
    a config glob that matches no file (dangling), and config files matched by no
    glob (orphan). Returned rather than raised so the conformance battery can turn
    each into one loud, isolated failing case while every well-formed scenario
    still runs.

    Per-scenario knob *misuse* — an ``n_buffers`` on a locked scenario, a missing
    ``n_buffers`` on round_trip_n_buffers, a ``latency_ms`` outside slow_link — is
    no longer checked here: the ``conntest_runner.scenario_bindings`` constructors make it
    unrepresentable at authoring time (each scenario's constructor exposes only its
    own knobs), so there is nothing left to validate at runtime.

    ``known_scenarios`` (the catalog's scenario names) is passed in rather than
    imported so discovery keeps no dependency on the catalog module (which imports
    *from* discovery — importing it back would be a cycle).
    """
    problems: list[BindingProblem] = []

    def add(scenario: str | None, message: str) -> None:
        problems.append(BindingProblem(plugin=entry.name, scenario=scenario, message=message))

    ct = entry.conn_test_dir
    for scen in entry.scenarios:
        if scen.name not in known_scenarios:
            add(
                scen.name,
                f"scenario {scen.name!r} is not a catalog scenario "
                f"(known: {sorted(known_scenarios)})",
            )
        for spec in scen.configs:
            if spec.error is not None:
                add(
                    scen.name,
                    f"scenario {scen.name!r} has an invalid outcome {spec.outcome!r}: {spec.error}",
                )
            if not resolve_configs(ct, [spec.glob]):
                add(
                    scen.name,
                    f"scenario {scen.name!r} references config glob {spec.glob!r}, "
                    f"which matches no file under {ct.name}/configs/",
                )
    for orphan in _orphan_configs(ct, entry.scenarios):
        add(None, f"config file {orphan} under configs/ is referenced by no scenario glob")
    return problems


def discover_plugins(
    repo_root: Path, *, kinds: tuple[str, ...] = ("Sources",)
) -> Iterator[PluginEntry]:
    """Yield one PluginEntry per discovered ``conn-test/loader.py``."""
    for kind in kinds:
        kind_dir = repo_root / "nes-plugins" / kind
        if not kind_dir.is_dir():
            continue
        for plugin_dir in sorted(kind_dir.iterdir()):
            if not plugin_dir.is_dir():
                continue
            ct = plugin_dir / "conn-test"
            loader_py = ct / "loader.py"
            scenarios_py = ct / "scenarios.py"
            if not loader_py.exists():
                _LOG.info(
                    "skipping %s: no %s",
                    plugin_dir.name,
                    loader_py.relative_to(repo_root),
                )
                continue
            module_prefix = f"conntest_plugin_{kind.lower()}_{plugin_dir.name}".lower()
            loader_mod = _import(loader_py, module_prefix + "_loader")
            loader_classes = [v for v in vars(loader_mod).values() if _is_loader_class(v)]
            if len(loader_classes) != 1:
                raise RuntimeError(
                    f"{plugin_dir.name}: expected exactly 1 loader class in loader.py "
                    f"— a concrete subclass of conntest_runner.contracts.SourceLoader "
                    f"or SinkLoader — found {len(loader_classes)}"
                )
            loader_cls = loader_classes[0]
            scenarios = _read_scenarios(plugin_dir, loader_cls, scenarios_py, module_prefix)
            compose_service, compose_port = _compose_service_and_port(ct)
            yield PluginEntry(
                name=plugin_dir.name,
                kind=kind,
                plugin_dir=plugin_dir,
                conn_test_dir=ct,
                loader_cls=loader_cls,
                compose_service=compose_service,
                compose_port=compose_port,
                scenarios=scenarios,
            )
