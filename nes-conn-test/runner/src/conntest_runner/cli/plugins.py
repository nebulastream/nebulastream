# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""plugins.py — host-side plugin discovery for the conntest CLI.

Globs each connector's ``conn-test/`` files (compose.yaml, scenarios) WITHOUT
importing the loaders, so the host python needs none of the runner's third-party
deps. ``find_plugin_composes`` / ``select_plugin_composes`` are the one canonical
pair for "which compose stack(s)"; the ``list`` subcommand also lives here.
"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from conntest_runner.cli._util import _die, _log, _repo_root

if TYPE_CHECKING:
    import argparse
    from pathlib import Path

# Services defined by the framework (runner.compose.yaml + the debug
# override), excluded from the data-service set the orchestrator waits on.
_FRAMEWORK_SERVICES = frozenset({"runner", "lldb-relay"})


def find_plugin_composes(repo_root: Path) -> list[Path]:
    """Every connector's ``conn-test/compose.yaml``, sorted for a stable battery order."""
    return sorted((repo_root / "nes-plugins").rglob("conn-test/compose.yaml"))


def plugin_name(compose_yaml: Path) -> str:
    """The connector name from its compose path (.../<kind>/<Name>/conn-test/compose.yaml)."""
    return compose_yaml.parent.parent.name


def select_plugin_composes(
    repo_root: Path, plugin: str | None, *, required: bool = False
) -> list[Path]:
    """Discovered plugin composes, optionally narrowed to a single plugin by name.

    Narrowing here — *before* the battery loop — means the other plugins' docker
    stacks are never brought up, rather than brought up only to have their tests
    deselected. ``plugin=None`` returns every compose. For a named plugin,
    ``required=True`` (the run path) dies with the available list when the name
    matches nothing; ``required=False`` (the debug path) returns ``[]`` so the
    caller can emit its own error.
    """
    composes = find_plugin_composes(repo_root)
    if plugin is None:
        return composes
    matched = [c for c in composes if plugin_name(c) == plugin]
    if not matched and required:
        available = ", ".join(sorted(plugin_name(c) for c in composes))
        _die(f"--plugin {plugin!r} matches no connector; available: {available}", code=2)
    return matched


def _compose_data_services(
    compose_files: list[Path], project: str, env: dict[str, str]
) -> list[str]:
    """Service names to wait on: every service in the merged stack minus the framework's own.

    Excludes ``runner`` / ``lldb-relay``. Resolved via ``docker compose config
    --services`` so we never hard-code connector-specifics like ``broker``
    """
    cmd = ["docker", "compose"]
    for f in compose_files:
        cmd += ["-f", str(f)]
    cmd += ["-p", project, "config", "--services"]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    services = [s.strip() for s in proc.stdout.splitlines() if s.strip()]
    data = [s for s in services if s not in _FRAMEWORK_SERVICES]
    if not data:
        _log(
            f"warning: no data services found in {[str(f) for f in compose_files]} "
            f"(config --services rc={proc.returncode}); falling back to `up --wait`"
        )
    return data


# ---------------------------------------------------------------------------
# Subcommand "list"
# ---------------------------------------------------------------------------
def _cmd_list(_args: argparse.Namespace) -> int:
    repo = _repo_root()
    composes = find_plugin_composes(repo)
    loaders = sorted((repo / "nes-plugins").rglob("conn-test/loader.py"))
    by_dir = {c.parent: c for c in composes}
    if not loaders:
        _log("no conn-test plugins found under nes-plugins/")
        return 0
    # Deferred: discovery pulls in the runner's third-party deps the host python may lack.
    from conntest_runner.discovery import (  # noqa: PLC0415
        _compose_service_names,
        read_plugin_scenarios,
    )

    for loader in loaders:
        ct = loader.parent
        name = ct.parent.name
        kind = ct.parent.parent.name
        # Same host-safe read the `run` path uses (cli/battery.py): imports the
        # plugin's pure-data scenarios.py, never the loader. De-dup by name
        # (a plugin binds some scenarios more than once, against different
        # configs); dict.fromkeys keeps first-seen order.
        bound = read_plugin_scenarios(ct)
        scenarios: list[str] | str = (
            "<via loader class>"  # no conn-test/scenarios.py
            if bound is None
            else list(dict.fromkeys(s.name for s in bound))
        )
        compose = by_dir.get(ct)
        if compose is not None:
            services = sorted(_compose_service_names(compose))
            svc = f"services={services}"
        else:
            svc = "docker-free"
        print(f"{kind}/{name}: {svc} scenarios={scenarios}")
    return 0
