# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for conn-test plugin discovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from conntest_runner.discovery import (
    Expectation,
    IsolationContractError,
    Scenario,
    binding_problems,
    discover_plugin,
    parse_expectation,
)

_LOADER = (
    "from conntest_runner.contracts import SourceLoader\n"
    "class FakeLoader(SourceLoader):\n"
    "    def __init__(self, endpoint): pass\n"
    "    def config_overrides(self, *, target, endpoint): return {}\n"
    "    def setup(self, *, target, schema=None): pass\n"
    "    def seed_batch(self, rows, *, target): pass\n"
)

# The image tag has a digit on purpose: the port must come from `expose:`, not from a digit elsewhere.
# `postgres:16` was once read as port 16.
_COMPOSE_ONE_SERVICE = 'services:\n  {service}:\n    image: busybox:1.36\n    expose: ["1234"]\n'


def test_missing_scenarios_raises(tmp_path: Path) -> None:
    ct = tmp_path / "conn-test"
    ct.mkdir()
    (ct / "loader.py").write_text(_LOADER, encoding="utf-8")
    (ct / "compose.yaml").write_text(_COMPOSE_ONE_SERVICE.format(service="svc"), encoding="utf-8")
    with pytest.raises(FileNotFoundError, match="scenarios.py"):
        discover_plugin("FakeSource", ct)


_LOADER_NOT_A_SUBCLASS = (
    "class FakeLoader:\n"
    "    def __init__(self, endpoint): pass\n"
    "    def config_overrides(self, *, target, endpoint): return {}\n"
    "    def setup(self, *, target, schema=None): pass\n"
    "    def seed_batch(self, payload, *, target): pass\n"
)


def test_loader_not_subclassing_base_is_not_discovered(tmp_path: Path) -> None:
    """The error states the contract, not a skip that would later read as "no plugin found"."""
    ct = tmp_path / "conn-test"
    ct.mkdir()
    (ct / "loader.py").write_text(_LOADER_NOT_A_SUBCLASS, encoding="utf-8")
    (ct / "compose.yaml").write_text(_COMPOSE_ONE_SERVICE.format(service="svc"), encoding="utf-8")
    with pytest.raises(RuntimeError, match="SourceLoader"):
        discover_plugin("FakeSource", ct)


def _write_fake_broker_plugin(tmp_path: Path, *, compose: str) -> Path:
    ct = tmp_path / "conn-test"
    ct.mkdir()
    (ct / "loader.py").write_text(_LOADER, encoding="utf-8")
    (ct / "compose.yaml").write_text(compose, encoding="utf-8")
    (ct / "scenarios.py").write_text("SCENARIOS = []\n", encoding="utf-8")
    return ct


def test_topology_derived_from_compose(tmp_path: Path) -> None:
    ct = _write_fake_broker_plugin(tmp_path, compose=_COMPOSE_ONE_SERVICE.format(service="svc"))
    assert discover_plugin("FakeSource", ct).endpoint == "svc:1234"
    # Without a compose.yaml, the loader starts the server itself.
    (ct / "compose.yaml").unlink()
    assert discover_plugin("FakeSource", ct).endpoint is None


def test_missing_expose_port_fails_fast(tmp_path: Path) -> None:
    """The error is reported at discovery, not as a later connect timeout."""
    ct = _write_fake_broker_plugin(tmp_path, compose="services:\n  svc:\n    image: busybox\n")
    with pytest.raises(IsolationContractError, match="expose"):
        discover_plugin("FakeSource", ct)


def test_multiple_services_fails_fast(tmp_path: Path) -> None:
    two_services = (
        'services:\n  a:\n    image: busybox\n    expose: ["1"]\n  b:\n    image: busybox\n'
    )
    ct = _write_fake_broker_plugin(tmp_path, compose=two_services)
    with pytest.raises(IsolationContractError, match="exactly one service"):
        discover_plugin("FakeSource", ct)


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        ("OK", Expectation(ok=True)),
        ("ERROR 1000", Expectation(ok=False, code=1000)),
        # A symbolic name resolves through the .inc table.
        ("ERROR CannotOpenSource", Expectation(ok=False, code=4002)),
        ("ERROR 4002 open", Expectation(ok=False, code=4002, phase="open")),
    ],
)
def test_parse_expectation(outcome: str, expected: Expectation) -> None:
    assert parse_expectation(outcome) == expected


def test_parse_expectation_unknown_code_raises() -> None:
    with pytest.raises(ValueError, match="Bogus"):
        parse_expectation("ERROR Bogus")


def test_scenario_normalizes_config_and_outcome() -> None:
    s = Scenario("bad_endpoint", config="basic.nesql", outcome="ERROR 4002")
    assert len(s.configs) == 1
    assert s.configs[0].glob == "basic.nesql"
    assert s.configs[0].expect == Expectation(ok=False, code=4002)


def test_scenario_configs_list_mixed_outcomes() -> None:
    s = Scenario(
        "config",
        configs=[("*.nesql", "OK"), ("no-topic.nesql", "ERROR 1000")],
    )
    assert [c.expect.ok for c in s.configs] == [True, False]
    assert s.configs[1].expect.code == 1000


# Binding defects are reported, not raised, so the battery fails only the affected case and keeps running the rest.

_SCN = "from conntest_runner.discovery import Scenario\nSCENARIOS = [{}]\n"


def _discover_with(
    tmp_path: Path,
    *,
    scenarios_py: str,
    configs: tuple[str, ...] = (),
    data_files: tuple[str, ...] = (),
):
    """Loads the plugin even for a malformed binding, as long as scenarios.py imports."""
    ct = tmp_path / "conn-test"
    ct.mkdir()
    ct.joinpath("loader.py").write_text(_LOADER, encoding="utf-8")
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
    return discover_plugin("FakeSource", ct)


def test_clean_bindings_have_no_problems(tmp_path: Path) -> None:
    entry = _discover_with(
        tmp_path,
        scenarios_py=_SCN.format("Scenario('round_trip', config='basic.nesql')"),
        configs=("basic.nesql",),
    )
    assert binding_problems(entry, known_scenarios={"round_trip"}) == []


def test_unknown_scenario_reported(tmp_path: Path) -> None:
    entry = _discover_with(
        tmp_path,
        scenarios_py=_SCN.format("Scenario('nope', config='basic.nesql')"),
        configs=("basic.nesql",),
    )
    probs = binding_problems(entry, known_scenarios={"round_trip"})
    assert any(p.scenario == "nope" and "not a catalog scenario" in p.message for p in probs)


def test_dangling_config_glob_reported(tmp_path: Path) -> None:
    # No config file is written, so no orphan is reported alongside.
    entry = _discover_with(
        tmp_path,
        scenarios_py=_SCN.format("Scenario('round_trip', config='missing.nesql')"),
    )
    probs = binding_problems(entry, known_scenarios={"round_trip"})
    assert any("matches no file" in p.message for p in probs)
    assert not any(p.scenario is None for p in probs)  # no orphan reported


def test_invalid_outcome_reported_not_raised(tmp_path: Path) -> None:
    entry = _discover_with(
        tmp_path,
        scenarios_py=_SCN.format(
            "Scenario('round_trip', config='basic.nesql', outcome='ERROR Bogus')"
        ),
        configs=("basic.nesql",),
    )
    probs = binding_problems(entry, known_scenarios={"round_trip"})
    assert any("invalid outcome" in p.message and "Bogus" in p.message for p in probs)


def test_invalid_outcome_deferred_onto_spec() -> None:
    s = Scenario("round_trip", config="basic.nesql", outcome="ERROR Bogus")
    assert s.configs[0].expect is None
    assert s.configs[0].error
    assert "Bogus" in s.configs[0].error


def test_empty_outcome_deferred_not_raised() -> None:
    # A blank outcome fails parsing with IndexError, not ValueError, and must still be deferred.
    s = Scenario("round_trip", config="basic.nesql", outcome="   ")
    assert s.configs[0].expect is None
    assert s.configs[0].error


def test_orphan_config_reported(tmp_path: Path) -> None:
    # An unreferenced config is a problem of the plugin, not of a scenario.
    entry = _discover_with(
        tmp_path,
        scenarios_py=_SCN.format("Scenario('round_trip', config='basic.nesql')"),
        configs=("basic.nesql", "stray.nesql"),
    )
    probs = binding_problems(entry, known_scenarios={"round_trip"})
    assert any(p.scenario is None and "stray" in p.message for p in probs)


def test_multiple_defects_all_discovered_never_raised(tmp_path: Path) -> None:
    entry = _discover_with(
        tmp_path,
        scenarios_py=_SCN.format(
            "Scenario('nope', config='missing.nesql', outcome='ERROR Bogus')"
        ),
    )
    joined = " | ".join(p.message for p in binding_problems(entry, known_scenarios={"round_trip"}))
    assert "not a catalog scenario" in joined
    assert "invalid outcome" in joined
    assert "matches no file" in joined
