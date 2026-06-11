# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""dispatch.py — argument parsing and subcommand dispatch for the conntest CLI.

Builds the ``argparse`` parser and dispatches via a small table. ``self_wrap``
on each entry encodes the one ordering rule: ``list`` / ``install-clion`` are
pure host-side file ops and run BEFORE ``_maybe_self_wrap``; everything else
self-wraps into the dev container first. ``ci`` is ``run`` with every selector
forced off (the funnel that keeps CI and local on one path).
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from conntest_runner.cli.battery import _cmd_coverage, _cmd_run
from conntest_runner.cli.clion import _cmd_install_clion
from conntest_runner.cli.debug import _cmd_debug
from conntest_runner.cli.plugins import _cmd_list
from conntest_runner.cli.selfwrap import _maybe_self_wrap

if TYPE_CHECKING:
    from collections.abc import Callable

_DEFAULT_DEBUG_PORT = 2345


def _split_pytest_passthrough(argv: list[str]) -> tuple[list[str], list[str]]:
    """Split on the first bare ``--``: everything after it is forwarded to pytest.

    Mirrors the deleted clion-source-smoke.sh's behaviour.
    """
    if "--" in argv:
        i = argv.index("--")
        return argv[:i], argv[i + 1 :]
    return argv, []


def _make_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="conntest",
        description="the unified conntest CLI (run / ci / coverage / debug / list / install-clion)",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="run the battery (or one scenario)")
    run.add_argument(
        "selector",
        nargs="?",
        default="",
        help="scenario short name (round_trip, empty, …) or a raw pytest -k expression",
    )
    run.add_argument("-k", dest="k", default="", help="pytest -k expression")
    run.add_argument(
        "--plugin",
        default=None,
        help="run only this connector's stack + tests"
        "skips bringing up every other plugin's "
        "docker infrastructure",
    )
    run.add_argument(
        "--build", action="store_true", help="build conntest-harness in the runner before pytest"
    )
    run.add_argument("--build-dir", default=None, help="build dir name under the repo root")
    run.add_argument("--image", default=None, help="dev image for the runner")

    ci = sub.add_parser("ci", help="run exactly what CI runs (all plugins + meta)")
    ci.add_argument("--build-dir", default=None)
    ci.add_argument("--image", default=None)

    cov = sub.add_parser("coverage", help="instrumented battery + ccov list wiring")
    cov.add_argument("--build-dir", default=None)
    cov.add_argument("--image", default=None)
    cov.add_argument(
        "--ccov-dir", default=None, help="CMAKE_COVERAGE_OUTPUT_DIRECTORY (profraw/binaries lists)"
    )
    cov.add_argument("--harness", default=None, help="path to the conntest-harness binary")

    dbg = sub.add_parser("debug", help="run one test under lldb-server for CLion")
    dbg.add_argument("scenario", nargs="?", default="round_trip_10_buffers")
    dbg.add_argument(
        "--plugin",
        help="connector to debug (e.g. MQTTSource); required — the launcher "
        "errors with setup instructions when omitted",
    )
    dbg.add_argument(
        "--selector", default="", help="full pytest selector (overrides scenario/plugin)"
    )
    dbg.add_argument("--port", type=int, default=_DEFAULT_DEBUG_PORT)
    dbg.add_argument(
        "--detach",
        action="store_true",
        help="spawn detached, wait for the lldb banner, exit 0 (CLion before-launch)",
    )
    dbg.add_argument("--build-dir", default=None)
    dbg.add_argument("--image", default=None)

    sub.add_parser("list", help="list discovered plugins x scenarios")

    icl = sub.add_parser("install-clion", help="(re)write the CLion run configs")
    icl.add_argument("--quiet", action="store_true")

    return p


@dataclass(frozen=True)
class _Command:
    """A dispatch-table entry: the handler and whether it self-wraps first."""

    handler: Callable[[argparse.Namespace], int]
    self_wrap: bool  # False for pure host-side file ops (list, install-clion)


def _run_ci(args: argparse.Namespace) -> int:
    # `ci` is `run` with every selector/flag forced off — the funnel that keeps
    # CI and local on one code path so v2's CI-vs-local drift cannot recur.
    args.selector = ""
    args.k = ""
    args.build = False
    return _cmd_run(args, ci=True)


_COMMANDS: dict[str, _Command] = {
    "list": _Command(_cmd_list, self_wrap=False),
    "install-clion": _Command(_cmd_install_clion, self_wrap=False),
    "run": _Command(lambda a: _cmd_run(a, ci=False), self_wrap=True),
    "ci": _Command(_run_ci, self_wrap=True),
    "coverage": _Command(_cmd_coverage, self_wrap=True),
    "debug": _Command(_cmd_debug, self_wrap=True),
}


def main(argv: list[str] | None = None) -> int:
    """Parse argv and dispatch to the selected subcommand handler."""
    raw = list(sys.argv[1:] if argv is None else argv)
    pre, post = _split_pytest_passthrough(raw)
    args = _make_parser().parse_args(pre)
    args.pytest_extra = post

    # argparse (required=True subparsers) guarantees args.cmd is a known key.
    command = _COMMANDS[args.cmd]
    # `list` / `install-clion` are pure host-side file ops (self_wrap=False) and
    # must run on the host python; everything else re-execs into the dev
    # container first when the host lacks `docker compose`.
    if command.self_wrap:
        _maybe_self_wrap(raw)
    return command.handler(args)
