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


_LOADER_WITH_SCENARIOS = (
    "class FakeLoader:\n"
    "    system = 'fake'\n"
    "    compose_service = 'svc'\n"
    "    compose_port = 1234\n"
    "    SCENARIOS = ['round_trip']\n"
    "    def __init__(self, endpoint): pass\n"
    "    def seed(self, fixture, *, target): pass\n"
)


# These exercise discovery against the *real* on-disk plugins, but name
# none of them: the framework must stay plugin-agnostic (the same principle
# runner.compose.yaml / discovery.py / the §5.5 check honor — "name no
# plugin, no broker, no port"). A plugin's own constants are the plugin's
# concern; here we assert only the contract every loader must satisfy


def test_discovers_real_plugins() -> None:
    # Discovery finds the actual plugins. This also exercises the §5.5
    # isolation check, which raises inside discover_plugins — so a clean
    # return is proof every shipped plugin's compose_service matches its
    # compose.yaml.
    assert list(discover_plugins(_REPO_ROOT))


def test_every_loader_satisfies_contract() -> None:
    for p in discover_plugins(_REPO_ROOT):
        assert isinstance(p.loader_cls.system, str), p.name
        assert p.loader_cls.system, p.name
        assert isinstance(p.loader_cls.compose_service, str), p.name
        assert p.loader_cls.compose_service, p.name
        assert isinstance(p.loader_cls.compose_port, int), p.name
        assert p.scenarios, p.name
        assert all(isinstance(s, Scenario) and s.name for s in p.scenarios), p.name


_LOADER_NO_SCENARIOS = (
    "class FakeLoader:\n"
    "    system = 'fake'\n"
    "    compose_service = 'svc'\n"
    "    compose_port = 0\n"
    "    def __init__(self, endpoint): pass\n"
    "    def seed(self, fixture, *, target): pass\n"
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
    plugins = list(discover_plugins(tmp_path))
    assert [s.name for s in plugins[0].scenarios] == ["from_file"]


# ---------------------------------------------------------------------------
# §5.5 isolation contract: compose_service must name a compose service.
# ---------------------------------------------------------------------------

_COMPOSE_WITH = (
    "services:\n  {service}:\n    image: busybox\n  runner:\n    image: ${{CONNTEST_DEV_IMAGE}}\n"
)


def _write_fake_broker_plugin(tmp_path: Path, *, compose_service: str) -> Path:
    fake = tmp_path / "nes-plugins" / "Sources" / "FakeSource"
    (fake / "conn-test").mkdir(parents=True)
    (fake / "conn-test" / "loader.py").write_text(_LOADER_WITH_SCENARIOS, encoding="utf-8")
    (fake / "conn-test" / "compose.yaml").write_text(
        _COMPOSE_WITH.format(service=compose_service), encoding="utf-8"
    )
    return fake


def test_isolation_contract_match(tmp_path: Path) -> None:
    """compose_service ('svc') names a service in compose.yaml → discovers."""
    _write_fake_broker_plugin(tmp_path, compose_service="svc")
    plugins = list(discover_plugins(tmp_path))
    assert len(plugins) == 1
    assert plugins[0].loader_cls.compose_service == "svc"


def test_isolation_contract_mismatch_fails_fast(tmp_path: Path) -> None:
    """compose.yaml declares 'svcX', loader says 'svc' → named error.

    Not a silent timeout at harness-connect time (validation §9 item 4).
    """
    _write_fake_broker_plugin(tmp_path, compose_service="svcX")
    with pytest.raises(IsolationContractError, match="compose_service"):
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


def test_unsupported_needs_reported(tmp_path: Path) -> None:
    entry = _discover_with(
        tmp_path,
        scenarios_py=_SCN.format(
            "Scenario('round_trip', config='valid/basic.nesql', needs='wormhole')"
        ),
        configs=("valid/basic.nesql",),
    )
    probs = binding_problems(entry, known_scenarios={"round_trip"})
    assert any("needs='wormhole'" in p.message and "fault capability" in p.message for p in probs)


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


def test_missing_data_file_reported(tmp_path: Path) -> None:
    entry = _discover_with(
        tmp_path,
        scenarios_py=(
            "from conntest_runner.discovery import Scenario\n"
            "from conntest_runner.datamodel import File\n"
            "SCENARIOS = [Scenario('round_trip', config='valid/basic.nesql', "
            "data=File('input/nope.csv'))]\n"
        ),
        configs=("valid/basic.nesql",),
    )
    probs = binding_problems(entry, known_scenarios={"round_trip"})
    assert any("data file" in p.message and "nope.csv" in p.message for p in probs)


def test_present_data_file_not_reported(tmp_path: Path) -> None:
    entry = _discover_with(
        tmp_path,
        scenarios_py=(
            "from conntest_runner.discovery import Scenario\n"
            "from conntest_runner.datamodel import File\n"
            "SCENARIOS = [Scenario('round_trip', config='valid/basic.nesql', "
            "data=File('input/rows.csv'))]\n"
        ),
        configs=("valid/basic.nesql",),
        data_files=("input/rows.csv",),
    )
    assert binding_problems(entry, known_scenarios={"round_trip"}) == []


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
            "Scenario('nope', config='valid/missing.nesql', outcome='ERROR Bogus', needs='x')"
        ),
    )
    joined = " | ".join(p.message for p in binding_problems(entry, known_scenarios={"round_trip"}))
    assert "not a catalog scenario" in joined
    assert "fault capability" in joined
    assert "invalid outcome" in joined
    assert "matches no file" in joined
