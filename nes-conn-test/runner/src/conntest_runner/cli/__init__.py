# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""conntest_runner.cli — the unified ``conntest`` CLI (docker-setup-v3).

One launcher that is BOTH the host orchestrator and (after a fallback
self-wrap) the in-container driver — the ClickHouse "one launcher, branch
on is_local" shape — so CI and local share one code path and v2's
CI-vs-local drift cannot recur.

Subcommands
-----------
  run [SELECTOR|-k EXPR] [-- pytest-args]   battery / one scenario
  ci                                        exactly what CI runs
  coverage                                  instrumented battery (ccov)
  debug [SCENARIO]                          C++ lldb + CLion attach
  list                                      plugins x scenarios
  install-clion                             (re)write the CLion run configs

The governing principle (§1): never ask "which container am I". The
orchestrator drives ``docker compose``; compose *creates* the ``runner``
service already on the plugin's project network, so the harness reaches
``<service>:<port>`` (e.g. ``broker:1883``) by DNS — no ``docker network
connect $HOSTNAME`` self-attach (the v2 mistake that broke CI).

Execution model (§5.2): on a host that has ``docker compose`` (Docker
Desktop, Colima+plugin, Linux, the GitHub runner VM) the orchestrator
runs right here and the ONLY dev container is the ``runner``. On a host
without it, ``conntest`` re-execs itself once inside a one-shot dev
container (which ships compose) and proceeds identically.

This package is the split of the former ``cli.py``: ``dispatch`` (parser +
dispatch table), ``env`` / ``context`` / ``plugins`` / ``battery`` / ``debug`` /
``clion`` / ``selfwrap`` / ``bash`` / ``_util``. The launcher shim and the
framework self-tests import their entry points from here.
"""

from __future__ import annotations

from conntest_runner.cli.context import (
    RunContext,
    _apply_plugin_suppressions,
    _find_plugin_suppressions,
    _merge_suppressions,
)
from conntest_runner.cli.dispatch import main
from conntest_runner.cli.env import (
    _docker_sock_gid,
    _docker_sock_mount_source,
    _resolve_build_dir,
)

__all__ = [
    "RunContext",
    "_apply_plugin_suppressions",
    "_docker_sock_gid",
    "_docker_sock_mount_source",
    "_find_plugin_suppressions",
    "_merge_suppressions",
    "_resolve_build_dir",
    "main",
]
