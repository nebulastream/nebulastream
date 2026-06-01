# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""_conformance_shared.py — fixtures + helpers for the generic battery.

Hoisted here so the single generic conformance test stays thin: harness
binary resolution, the compose endpoint, config materialization, the
active setup_file group, and the "is this connector compiled into
conntest-harness?" pre-flight (now probed via a stepper VALIDATE).
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

from conntest_runner.compose import ComposeEndpoint
from conntest_runner.config_rewrite import apply_overrides
from conntest_runner.discovery import PluginEntry, resolve_configs
from conntest_runner.harness import Sink, Source
from conntest_runner.protocol import (
    NAME_TO_CODE,
    ConnectorError,
    HarnessError,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PROBE_DEADLINE_S = 30.0


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def harness_binary(request: pytest.FixtureRequest, repo_root: Path) -> Path:
    """Resolve the harness binary path; skips if not built."""
    from conntest_runner.cli import _resolve_build_dir  # noqa: PLC0415  # defer CLI import

    override = request.config.getoption("--build-dir")
    build_dir = repo_root / _resolve_build_dir(override, repo_root=repo_root)
    binary = build_dir / "nes-conn-test" / "conntest-harness"
    if not binary.exists():
        pytest.skip(f"conntest-harness not built at {binary}")
    return binary


def compose_endpoint(plugin: PluginEntry) -> str:
    """Return ``<service>:<container_port>`` for a plugin.

    Reachable by compose DNS from the runner service the battery runs inside.
    """
    return str(
        ComposeEndpoint(
            host=plugin.loader_cls.compose_service,
            port=plugin.loader_cls.compose_port,
        )
    )


# ---------------------------------------------------------------------------
# Config materialization
# ---------------------------------------------------------------------------
def render_config(
    plugin: PluginEntry,
    config_path: Path,
    *,
    endpoint: str,
    target: str,
    out_dir: Path,
) -> Path:
    """Render a pristine ``.nesql`` into a runnable config.

    Swap the loader's dynamic keys (endpoint + per-test target) by key, leaving
    every other line as authored.
    """
    loader = plugin.loader_cls(endpoint)
    overrides = loader.config_overrides(target=target, endpoint=endpoint)
    rendered = apply_overrides(config_path.read_text(encoding="utf-8"), overrides)
    out = out_dir / "config.nesql"
    out.write_text(rendered, encoding="utf-8")
    return out


def active_setup_file() -> str | None:
    """Return the setup_file of the stack the runner currently has up, or None.

    Read per group via ``CONNTEST_SETUP_FILE``.
    """
    return os.environ.get("CONNTEST_SETUP_FILE") or None


# ---------------------------------------------------------------------------
# Pre-flight: a plugin must be *activated* (compiled into conntest-harness).
# ---------------------------------------------------------------------------
_TYPE_RE = re.compile(r"\bTYPE\s+([A-Za-z0-9_]+)")
_registered_cache: dict[str, bool] = {}
_UNKNOWN_TYPE_CODES = {NAME_TO_CODE["UnknownSourceType"], NAME_TO_CODE["UnknownSinkType"]}


def _plugin_connector_type(plugin: PluginEntry) -> str | None:
    for p in resolve_configs(plugin.conn_test_dir, ["valid/*.nesql", "invalid/*.nesql"]):
        m = _TYPE_RE.search(p.read_text(encoding="utf-8"))
        if m:
            return m.group(1)
    return None


def probe_registered(harness: Path, plugin: PluginEntry) -> bool:
    """True iff conntest-harness has the plugin's connector type registered.

    Drives a stepper VALIDATE on a pristine valid config: an unregistered
    type binds to UnknownSource/SinkType; a registered type validates clean
    or fails differently. Cached per plugin.
    """
    if plugin.name in _registered_cache:
        return _registered_cache[plugin.name]
    valid = resolve_configs(plugin.conn_test_dir, ["valid/*.nesql"])
    if not valid:
        return True  # indeterminate — never false-fail a real plugin
    stepper_cls = Sink if plugin.kind == "Sinks" else Source
    stepper = stepper_cls(harness=harness, config=valid[0], global_deadline_s=_PROBE_DEADLINE_S)
    registered = True
    try:
        stepper.validate()
    except ConnectorError as e:
        registered = e.code not in _UNKNOWN_TYPE_CODES
    except HarnessError:
        registered = True  # indeterminate
    finally:
        stepper.cleanup()
    _registered_cache[plugin.name] = registered
    return registered


@pytest.fixture(autouse=True)
def require_connector_registered(request: pytest.FixtureRequest) -> None:
    """Fail a plugin's battery fast when its connector isn't compiled in.

    Runs once per plugin (when its connector isn't compiled into
    conntest-harness). No-op for tests without a ``plugin``.
    """
    callspec = getattr(request.node, "callspec", None)
    if callspec is None:
        return
    plugin = callspec.params.get("plugin")
    if not isinstance(plugin, PluginEntry):
        return
    harness = request.getfixturevalue("harness_binary")
    if not probe_registered(harness, plugin):
        connector_type = _plugin_connector_type(plugin)
        pytest.fail(
            f"{plugin.kind} type {connector_type!r} (plugin {plugin.name}) ships a "
            f"conn-test surface but is not registered in conntest-harness — the "
            f"connector was not compiled in. Activate it in "
            f"nes-plugins/CMakeLists.txt:\n"
            f'    activate_optional_plugin("{plugin.kind}/{plugin.name}" ON)\n'
            f"then rebuild the conntest-harness target.",
            pytrace=False,
        )
