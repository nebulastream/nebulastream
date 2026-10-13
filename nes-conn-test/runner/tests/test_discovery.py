# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Framework self-test for `conntest_runner.discovery`."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from conntest_runner.discovery import (
    Expectation,
    IsolationContractError,
    Scenario,
    binding_problems,
    discover_plugins,
    parse_expectation,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]

# Two tests below exercise discovery against the *real* on-disk plugins, which
# imports each loader.py — and a loader pulls in plugin-specific client libraries
# (paho for MQTT, psycopg for ODBC). Those live in the conntest runner image, not
# on a bare host, so skip them when absent: the framework runs its self-tests
# *inside* the runner (`conntest ci`), where they resolve and run. Every other
# test in this module is host-safe — it reads scenarios via the pure-data path
# (read_plugin_scenarios) and never imports a loader.
_runner_only = pytest.mark.skipif(
    any(importlib.util.find_spec(mod) is None for mod in ("paho", "psycopg")),
    reason="needs the plugin loaders' client libs (paho/psycopg) — runs in the conntest runner",
)


# A loader declares no topology — discovery identifies it by its SourceLoader/
# SinkLoader base class (conntest_runner.contracts) and derives the service/port
# from the sibling compose.yaml. The fake is a real concrete subclass: it
# implements the full contract (config_overrides/setup/seed_batch), so it satisfies
# the ABC the way a shipped plugin does.
_LOADER_WITH_SCENARIOS = (
    "from conntest_runner.contracts import SourceLoader\n"
    "class FakeLoader(SourceLoader):\n"
    "    SCENARIOS = ['round_trip']\n"
    "    def __init__(self, endpoint): pass\n"
    "    def config_overrides(self, *, target, endpoint): return {}\n"
    "    def setup(self, *, target, schema=None): pass\n"
    "    def seed_batch(self, rows, *, target): pass\n"
)

# Single data service with an explicit expose port — the shape every plugin
# compose.yaml must have for the framework to derive its endpoint. The image
# carries a digit-bearing tag on purpose: the port must come from `expose:`, not
# from a stray digit elsewhere (a real bug: `postgres:16` was once read as port 16).
_COMPOSE_ONE_SERVICE = 'services:\n  {service}:\n    image: busybox:1.36\n    expose: ["1234"]\n'


# These exercise discovery against the *real* on-disk plugins, but name
# none of them: the framework must stay plugin-agnostic (the same principle
# runner.compose.yaml / discovery.py / the §5.5 check honor — "name no
# plugin, no broker, no port"). A plugin's own constants are the plugin's
# concern; here we assert only the contract every loader must satisfy


@_runner_only
def test_discovers_real_plugins() -> None:
    # Discovery finds the actual plugins. This also exercises the topology
    # derivation (exactly one compose service with an expose port), which raises
    # inside discover_plugins — so a clean return is proof every shipped plugin's
    # compose.yaml has the shape the framework requires.
    assert list(discover_plugins(_REPO_ROOT))


@_runner_only
def test_every_loader_satisfies_contract() -> None:
    for p in discover_plugins(_REPO_ROOT):
        # service/port are framework-derived from compose.yaml, surfaced on the entry
        assert isinstance(p.compose_service, str), p.name
        assert p.compose_service, p.name
        assert isinstance(p.compose_port, int), p.name
        assert p.compose_port > 0, p.name
        assert p.scenarios, p.name
        assert all(isinstance(s, Scenario) and s.name for s in p.scenarios), p.name


_LOADER_NO_SCENARIOS = (
    "from conntest_runner.contracts import SourceLoader\n"
    "class FakeLoader(SourceLoader):\n"
    "    def __init__(self, endpoint): pass\n"
    "    def config_overrides(self, *, target, endpoint): return {}\n"
    "    def setup(self, *, target, schema=None): pass\n"
    "    def seed_batch(self, rows, *, target): pass\n"
)


def test_missing_scenarios_raises(tmp_path: Path) -> None:
    """No scenarios.py AND no SCENARIOS class attribute → discovery errors."""
    fake = tmp_path / "nes-plugins" / "Sources" / "FakeSource"
    (fake / "conn-test").mkdir(parents=True)
    (fake / "conn-test" / "loader.py").write_text(_LOADER_NO_SCENARIOS, encoding="utf-8")
    with pytest.raises(FileNotFoundError, match="SCENARIOS"):
        list(discover_plugins(tmp_path))


def test_scenarios_class_attribute_fallback(tmp_path: Path) -> None:
    """Loader class with a SCENARIOS class attribute and no scenarios.py."""
    fake = tmp_path / "nes-plugins" / "Sources" / "FakeSource"
    (fake / "conn-test").mkdir(parents=True)
    (fake / "conn-test" / "loader.py").write_text(
        _LOADER_NO_SCENARIOS.replace(
            "    def __init__(self, endpoint): pass\n",
            "    SCENARIOS = ['round_trip', 'empty']\n    def __init__(self, endpoint): pass\n",
        ),
        encoding="utf-8",
    )
    (fake / "conn-test" / "compose.yaml").write_text(
        _COMPOSE_ONE_SERVICE.format(service="svc"), encoding="utf-8"
    )
    plugins = list(discover_plugins(tmp_path))
    assert len(plugins) == 1
    assert [s.name for s in plugins[0].scenarios] == ["round_trip", "empty"]


def test_scenarios_py_wins_over_class_attribute(tmp_path: Path) -> None:
    """If both scenarios.py and a class-level SCENARIOS exist, the file wins."""
    fake = tmp_path / "nes-plugins" / "Sources" / "FakeSource"
    (fake / "conn-test").mkdir(parents=True)
    (fake / "conn-test" / "loader.py").write_text(
        _LOADER_NO_SCENARIOS.replace(
            "    def __init__(self, endpoint): pass\n",
            "    SCENARIOS = ['from_class']\n    def __init__(self, endpoint): pass\n",
        ),
        encoding="utf-8",
    )
    (fake / "conn-test" / "scenarios.py").write_text(
        "SCENARIOS = ['from_file']\n", encoding="utf-8"
    )
    (fake / "conn-test" / "compose.yaml").write_text(
        _COMPOSE_ONE_SERVICE.format(service="svc"), encoding="utf-8"
    )
    plugins = list(discover_plugins(tmp_path))
    assert [s.name for s in plugins[0].scenarios] == ["from_file"]


# A class that is loader-shaped (has seed_batch) but does NOT inherit the
# SourceLoader/SinkLoader base. Nominal discovery must not accept it.
_LOADER_NOT_A_SUBCLASS = (
    "class FakeLoader:\n"
    "    SCENARIOS = ['round_trip']\n"
    "    def __init__(self, endpoint): pass\n"
    "    def config_overrides(self, *, target, endpoint): return {}\n"
    "    def setup(self, *, target, schema=None): pass\n"
    "    def seed_batch(self, payload, *, target): pass\n"
)


def test_loader_not_subclassing_base_is_not_discovered(tmp_path: Path) -> None:
    """A class must inherit the contract base to be a loader (guardrail).

    Discovery is nominal: a plain class that merely looks loader-shaped (the full
    method set, but no SourceLoader/SinkLoader base) is not recognized, and the
    developer gets a RuntimeError naming the contract — not a silent skip that
    would later read as "no plugin found".
    """
    fake = tmp_path / "nes-plugins" / "Sources" / "FakeSource"
    (fake / "conn-test").mkdir(parents=True)
    (fake / "conn-test" / "loader.py").write_text(_LOADER_NOT_A_SUBCLASS, encoding="utf-8")
    (fake / "conn-test" / "compose.yaml").write_text(
        _COMPOSE_ONE_SERVICE.format(service="svc"), encoding="utf-8"
    )
    with pytest.raises(RuntimeError, match="SourceLoader"):
        list(discover_plugins(tmp_path))


# ---------------------------------------------------------------------------
# Topology derivation: the connector endpoint comes from compose.yaml (the sole
# service name + its expose port), not from loader constants.
# ---------------------------------------------------------------------------


def _write_fake_broker_plugin(tmp_path: Path, *, compose: str) -> Path:
    fake = tmp_path / "nes-plugins" / "Sources" / "FakeSource"
    (fake / "conn-test").mkdir(parents=True)
    (fake / "conn-test" / "loader.py").write_text(_LOADER_WITH_SCENARIOS, encoding="utf-8")
    (fake / "conn-test" / "compose.yaml").write_text(compose, encoding="utf-8")
    return fake


def test_topology_derived_from_compose(tmp_path: Path) -> None:
    """The sole service + its expose port become the entry's service/port."""
    _write_fake_broker_plugin(tmp_path, compose=_COMPOSE_ONE_SERVICE.format(service="svc"))
    plugins = list(discover_plugins(tmp_path))
    assert len(plugins) == 1
    assert plugins[0].compose_service == "svc"
    assert plugins[0].compose_port == 1234


def test_missing_expose_port_fails_fast(tmp_path: Path) -> None:
    """A service without an `expose:` port → named error, not a connect timeout."""
    _write_fake_broker_plugin(tmp_path, compose="services:\n  svc:\n    image: busybox\n")
    with pytest.raises(IsolationContractError, match="expose"):
        list(discover_plugins(tmp_path))


def test_multiple_services_fails_fast(tmp_path: Path) -> None:
    """More than one service in a plugin compose.yaml → named error."""
    two_services = (
        'services:\n  a:\n    image: busybox\n    expose: ["1"]\n  b:\n    image: busybox\n'
    )
    _write_fake_broker_plugin(tmp_path, compose=two_services)
    with pytest.raises(IsolationContractError, match="exactly one service"):
        list(discover_plugins(tmp_path))


# ---------------------------------------------------------------------------
# Expectation parsing (declared outcomes, §8).
# ---------------------------------------------------------------------------


def test_parse_expectation_ok() -> None:
    e = parse_expectation("OK")
    assert e == Expectation(ok=True)


def test_parse_expectation_error_numeric() -> None:
    e = parse_expectation("ERROR 1000")
    assert e.ok is False
    assert e.code == 1000
    assert e.phase is None


def test_parse_expectation_error_named() -> None:
    # A symbolic name resolves through the .inc table.
    e = parse_expectation("ERROR CannotOpenSource")
    assert e.ok is False
    assert e.code == 4002


def test_parse_expectation_error_with_phase() -> None:
    e = parse_expectation("ERROR 4002 open")
    assert e.ok is False
    assert e.code == 4002
    assert e.phase == "open"


def test_parse_expectation_unknown_code_raises() -> None:
    with pytest.raises(ValueError, match="Bogus"):
        parse_expectation("ERROR Bogus")


def test_parse_expectation_loss_bare() -> None:
    # A bare LOSS is a connector-OK loss (a source whose broker dropped it):
    # no error contract, but lossy flips the read-back oracle to the survivors.
    e = parse_expectation("LOSS")
    assert e == Expectation(ok=True, lossy=True)
    assert e.code is None


def test_parse_expectation_loss_with_code() -> None:
    # LOSS <code> pairs the loss with the error a non-buffering sink raises on
    # the during-cut write; ok is False so run_case still requires that error.
    e = parse_expectation("LOSS 4004")
    assert e.ok is False
    assert e.lossy is True
    assert e.code == 4004


def test_parse_expectation_loss_named_code() -> None:
    # The symbolic form resolves through the .inc table, like ERROR.
    e = parse_expectation("LOSS CannotOpenSink")
    assert e.lossy is True
    assert e.code == 4004


def test_scenario_normalizes_config_and_outcome() -> None:
    s = Scenario("bad_endpoint", config="valid/basic.nesql", outcome="ERROR 4002")
    assert len(s.configs) == 1
    assert s.configs[0].glob == "valid/basic.nesql"
    assert s.configs[0].expect == Expectation(ok=False, code=4002)


def test_scenario_configs_list_mixed_outcomes() -> None:
    s = Scenario(
        "config",
        configs=[("valid/*.nesql", "OK"), ("invalid/no-topic.nesql", "ERROR 1000")],
    )
    assert [c.expect.ok for c in s.configs] == [True, False]
    assert s.configs[1].expect.code == 1000


# ---------------------------------------------------------------------------
# binding_problems (§8): every static scenario-binding defect is *reported as
# data* (not raised), so the battery fails that one case loudly and keeps the
# rest running. A bad outcome / orphan config used to abort discovery wholesale;
# now discovery returns and the defect is one entry in binding_problems().
# ---------------------------------------------------------------------------

_SCN = "from conntest_runner.discovery import Scenario\nSCENARIOS = [{}]\n"


def _discover_with(
    tmp_path: Path,
    *,
    scenarios_py: str,
    configs: tuple[str, ...] = (),
    data_files: tuple[str, ...] = (),
):
    """Write a minimal discoverable plugin and return its single PluginEntry.

    The plugin is a loader + the given scenarios.py + any config/data fixtures.
    Discovery must NOT raise on a *binding* defect — those are reported by
    binding_problems, not raised — so this returns even for a malformed binding
    (as long as scenarios.py imports).
    """
    fake = tmp_path / "nes-plugins" / "Sources" / "FakeSource"
    ct = fake / "conn-test"
    ct.mkdir(parents=True)
    ct.joinpath("loader.py").write_text(_LOADER_WITH_SCENARIOS, encoding="utf-8")
    ct.joinpath("compose.yaml").write_text(
        _COMPOSE_ONE_SERVICE.format(service="svc"), encoding="utf-8"
    )
    ct.joinpath("scenarios.py").write_text(scenarios_py, encoding="utf-8")
    for rel in configs:
        f = ct / "configs" / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text("-- config\n", encoding="utf-8")
    for rel in data_files:
        f = ct / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text("x\n", encoding="utf-8")
    entries = list(discover_plugins(tmp_path))
    assert len(entries) == 1
    return entries[0]


def test_clean_bindings_have_no_problems(tmp_path: Path) -> None:
    entry = _discover_with(
        tmp_path,
        scenarios_py=_SCN.format("Scenario('round_trip', config='valid/basic.nesql')"),
        configs=("valid/basic.nesql",),
    )
    assert binding_problems(entry, known_scenarios={"round_trip"}) == []


def test_unknown_scenario_reported(tmp_path: Path) -> None:
    entry = _discover_with(
        tmp_path,
        scenarios_py=_SCN.format("Scenario('nope', config='valid/basic.nesql')"),
        configs=("valid/basic.nesql",),
    )
    probs = binding_problems(entry, known_scenarios={"round_trip"})
    assert any(p.scenario == "nope" and "not a catalog scenario" in p.message for p in probs)

    # Per-scenario knob misuse (n_buffers on a locked scenario, a missing required
    # n_buffers, latency_ms outside slow_link) is no longer a binding_problems
    # concern: the conntest_runner.scenario_bindings constructors make it unrepresentable at
    # authoring time, so there is nothing to validate at runtime.


def test_dangling_config_glob_reported(tmp_path: Path) -> None:
    # References valid/missing.nesql, which does not exist; no config file is
    # written, so there is no orphan to confuse the assertion.
    entry = _discover_with(
        tmp_path,
        scenarios_py=_SCN.format("Scenario('round_trip', config='valid/missing.nesql')"),
    )
    probs = binding_problems(entry, known_scenarios={"round_trip"})
    assert any("matches no file" in p.message for p in probs)
    assert not any(p.scenario is None for p in probs)  # no orphan reported


def test_invalid_outcome_reported_not_raised(tmp_path: Path) -> None:
    # A bad outcome must NOT raise at construction/discovery (the old behavior
    # that aborted the whole session) — it is deferred onto the spec and
    # surfaced here, per scenario.
    entry = _discover_with(
        tmp_path,
        scenarios_py=_SCN.format(
            "Scenario('round_trip', config='valid/basic.nesql', outcome='ERROR Bogus')"
        ),
        configs=("valid/basic.nesql",),
    )
    probs = binding_problems(entry, known_scenarios={"round_trip"})
    assert any("invalid outcome" in p.message and "Bogus" in p.message for p in probs)


def test_invalid_outcome_deferred_onto_spec() -> None:
    # Construction itself no longer raises: the parse error rides on the spec.
    s = Scenario("round_trip", config="valid/basic.nesql", outcome="ERROR Bogus")
    assert s.configs[0].expect is None
    assert s.configs[0].error
    assert "Bogus" in s.configs[0].error


def test_empty_outcome_deferred_not_raised() -> None:
    # An empty/whitespace outcome makes parse_expectation raise IndexError, not
    # ValueError — it must still be caught and deferred, not abort construction.
    s = Scenario("round_trip", config="valid/basic.nesql", outcome="   ")
    assert s.configs[0].expect is None
    assert s.configs[0].error


def test_orphan_config_reported(tmp_path: Path) -> None:
    # basic.nesql is referenced; stray.nesql is not -> a plugin-level problem
    # (scenario=None). Discovery no longer raises; the guard test surfaces it.
    entry = _discover_with(
        tmp_path,
        scenarios_py=_SCN.format("Scenario('round_trip', config='valid/basic.nesql')"),
        configs=("valid/basic.nesql", "invalid/stray.nesql"),
    )
    probs = binding_problems(entry, known_scenarios={"round_trip"})
    assert any(p.scenario is None and "stray" in p.message for p in probs)


def test_multiple_defects_all_discovered_never_raised(tmp_path: Path) -> None:
    # A grab-bag of defects in one scenario must all be *discovered* (returned),
    # never raised — that is what lets the battery report each and keep running.
    entry = _discover_with(
        tmp_path,
        scenarios_py=_SCN.format(
            "Scenario('nope', config='valid/missing.nesql', outcome='ERROR Bogus')"
        ),
    )
    joined = " | ".join(p.message for p in binding_problems(entry, known_scenarios={"round_trip"}))
    assert "not a catalog scenario" in joined
    assert "invalid outcome" in joined
    assert "matches no file" in joined
