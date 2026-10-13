# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""debug.py — the ``debug`` subcommand (C++ lldb-server + CLion attach).

Brings up a single connector's stack plus the long-lived runner + lldb-relay
sidecar (``up``+``exec``, not the run path's one-off ``run``), pins the
scenario's ``setup_file``, and runs one parametrized test under lldb-server.
``--detach`` spawns that foreground flow in a new session and polls its log for
the gdbserver banner — the CLion 'before launch' hook.
"""

from __future__ import annotations

import re
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

from conntest_runner.cli._util import (
    _RUNNER_COMPOSE,
    _RUNNER_DIR,
    _capture,
    _die,
    _log,
    _repo_root,
)
from conntest_runner.cli.bash import _debug_runner_bash
from conntest_runner.cli.battery import _SCENARIO_K, _debug_setup_file
from conntest_runner.cli.context import _build_context
from conntest_runner.cli.env import _resolve_build_dir, _resolve_image
from conntest_runner.cli.plugins import find_plugin_composes, plugin_name, select_plugin_composes

if TYPE_CHECKING:
    import argparse
    from typing import NoReturn

    from conntest_runner.cli.context import RunContext

_DEBUG_OVERRIDE = _RUNNER_DIR / "compose.debug.yaml"
_CONNTEST_EXE = _RUNNER_DIR / "conntest"
_DEBUG_PROJECT_PREFIX = "nebulastream-conntest-debug"


def _cleanup_stale_debug() -> None:
    proc = _capture(["docker", "ps", "-aq", "--filter", f"name=^{_DEBUG_PROJECT_PREFIX}-"])
    if proc is None:
        return
    out = proc.stdout.split()
    if out:
        _log(f"removing stale debug container(s): {' '.join(out)}")
        subprocess.run(["docker", "rm", "-f", *out], capture_output=True, check=False)


def _harness_debug_info_mapped(ctx: RunContext) -> bool:
    """Whether the harness debug info uses HOST paths so CLion breakpoints can bind.

    True when built with -fdebug-prefix-map (host paths) or no remap is needed.
    When ``host_root == container_repo`` (CI / identity mount) no remap is
    needed → True. Otherwise -fdebug-prefix-map rewrites every DW_AT_comp_dir
    /name from the container path to ``host_root``; the host path can only
    appear in the binary if that rewrite happened, so a fixed-string scan for
    it is a reliable proxy. If grep is missing or the harness isn't built we
    don't block (return True) — the pytest fixture skips a missing binary.
    """
    if ctx.host_root == ctx.container_repo:
        return True
    harness = ctx.host_build_dir / "nes-conn-test" / "conntest-harness"
    if not harness.is_file():
        return True
    needle = f"{ctx.host_root}/nes-conn-test"
    try:
        return (
            subprocess.run(
                ["grep", "-a", "-F", "-q", "-m1", "-e", needle, str(harness)],
                check=False,
            ).returncode
            == 0
        )
    except FileNotFoundError:
        return True


def _assert_debug_paths_mapped(ctx: RunContext) -> None:
    """Refuse a debug run when the harness still carries container paths.

    Otherwise CLion breakpoints never bind, the inferior runs free, and CLion
    reports 'Debugger detached' the instant it continues.
    """
    if _harness_debug_info_mapped(ctx):
        return
    _die(
        "conntest debug: conntest-harness was built WITHOUT "
        "-fdebug-prefix-map, so its debug info uses container paths "
        f"({ctx.container_repo}/...). CLion breakpoints set on host paths "
        f"({ctx.host_root}/...) will NOT bind: the inferior runs free and "
        "CLion reports 'Debugger detached' immediately.\n"
        "Fix: set HOST_NEBULASTREAM_ROOT in your CLion CMake profile's "
        "Environment (Settings > Build, Execution, Deployment > CMake) to\n"
        f"    HOST_NEBULASTREAM_ROOT={ctx.host_root}\n"
        "then Reload CMake Project and rebuild conntest-harness "
        "(see nes-conn-test/README.md -> Debugging)."
    )


def _die_no_debug_target() -> NoReturn:
    """Fail with launcher setup instructions when no connector is selected.

    The launcher invoked ``conntest debug`` (--detach or not) without naming a
    connector. We can't pick a compose stack to bring up, so rather than crash
    deep in subprocess with a ``NoneType`` argv (the plugin flag threads
    straight into the foreground argv / pytest -k), fail up front with
    instructions for pointing the ConnTest Debug Launcher at a real target.
    """
    try:
        names = sorted(plugin_name(c) for c in find_plugin_composes(_repo_root()))
    except OSError:
        names = []
    available = ", ".join(names) if names else "(none discovered under nes-plugins/)"
    _die(
        "debug: no connector selected.\n"
        "  The ConnTest Debug Launcher ran `conntest debug --detach` with no\n"
        "  scenario or plugin, so there is no connector stack to bring up.\n"
        "\n"
        "  Point it at a specific scenario for a specific plugin. In CLion:\n"
        '  Edit Configurations -> "ConnTest Debug Launcher" -> Script text,\n'
        "  e.g.:\n"
        "\n"
        "      conntest debug round_trip_10_buffers --detach --plugin <PLUGIN_NAME>\n"
        "\n"
        "    scenario (1st positional): round_trip_10_buffers, empty, config,\n"
        "                               bad_endpoint, reconnect, … (connector-specific)\n"
        f"    --plugin <PLUGIN_NAME>:    one of {available}\n"
        "\n"
        "  Then set breakpoints in that connector's source and click Debug.\n"
        "  `conntest list` shows each plugin's available scenarios."
    )


def _cmd_debug(args: argparse.Namespace) -> int:
    port = args.port
    if not args.plugin and not args.selector:
        _die_no_debug_target()
    if args.detach:
        return _debug_detach(args, port)

    image = _resolve_image(args.image, must_exist_locally=True)
    scenario = args.scenario or "round_trip_10_buffers"
    k = _SCENARIO_K.get(scenario, scenario)

    # --plugin selects the connector whose compose stack we bring up and whose
    # tests we run. A raw --selector may instead pin the plugin via a [Name]
    # suffix (e.g. a hand-typed nodeid) — honour that for compose resolution.
    plugin = args.plugin
    if args.selector:
        m = re.search(r"\[([^\]]+)\]$", args.selector)
        if m:
            plugin = m.group(1)

    matched = select_plugin_composes(_repo_root(), plugin, required=False) if plugin else []
    if not matched:
        _die(f"no conn-test/compose.yaml for plugin {plugin!r}")
    plugin_compose = matched[0]

    # The connector's conformance compose may mount ${CONNTEST_SETUP_FILE};
    # pin the file this scenario's group declares (None when the connector
    # uses none). It is needed at TWO points: the compose interpolation at
    # `up` (via ctx.env) and pytest's _cases() collection filter inside the
    # runner (via the exec env, where it also fixes the parametrized id).
    setup_file = _debug_setup_file(plugin_compose, scenario)

    extra_env = {"CONNTEST_DEBUG_PORT": str(port)}
    if setup_file:
        extra_env["CONNTEST_SETUP_FILE"] = setup_file
    ctx = _build_context(
        image=image,
        build_dir_name=_resolve_build_dir(args.build_dir),
        k_expr="",
        pytest_extra=[],
        extra_env=extra_env,
    )
    _assert_debug_paths_mapped(ctx)

    # Build the pytest argv host-side and forward it shlex-quoted. A -k
    # substring expression (plugin + scenario) is what actually selects the
    # parametrized test_scenario[...] case; --selector, when given, is passed
    # through verbatim as the user's own full selector.
    pytest_args = [
        "-s",
        f"--build-dir={ctx.container_build_dir}",
        f"--native-debug-port={port}",
        "-v",
    ]
    if args.selector:
        pytest_args.append(args.selector)
        sel_label = args.selector
    else:
        clauses = [f"({plugin})"] + ([f"({k})"] if k else [])
        kexpr = " and ".join(clauses)
        pytest_args += ["conformance/test_conformance.py", "-k", kexpr]
        sel_label = f"-k {kexpr}"
    pytest_quoted = " ".join(shlex.quote(a) for a in pytest_args)

    _cleanup_stale_debug()
    project = f"{_DEBUG_PROJECT_PREFIX}-{port}"
    base = [
        "docker",
        "compose",
        "-f",
        str(plugin_compose),
        "-f",
        str(_RUNNER_COMPOSE),
        "-f",
        str(_DEBUG_OVERRIDE),
        "-p",
        project,
    ]
    _log(
        f"debug: plugin={plugin} scenario={scenario} select={sel_label} "
        f"setup_file={setup_file or '(none)'} port={port} project={project}"
    )
    try:
        # Bring up EVERYTHING (data + runner + lldb-relay): the debug flow
        # needs the runner long-lived with its published port + the socat
        # keepalive sidecar, so we `up`+`exec` (not the run-path's one-off
        # `run`). The relay shares the runner's netns and survives the
        # ~12s macOS/Colima idle-drop on a paused session.
        subprocess.run([*base, "up", "-d", "--wait"], env=ctx.env, check=True)
        # Forward the active setup_file into the runner so _cases() collects
        # only this group's scenarios and the id suffix matches the -k select.
        setup_env = ["-e", f"CONNTEST_SETUP_FILE={setup_file}"] if setup_file else []
        exec_cmd = [
            *base,
            "exec",
            "-T",
            "-e",
            f"CONNTEST_PYTEST_ARGS={pytest_quoted}",
            *setup_env,
            "runner",
            "bash",
            "-c",
            _debug_runner_bash(),
        ]
        return subprocess.run(exec_cmd, env=ctx.env, check=False).returncode
    finally:
        _log(f"debug: down -v ({project})")
        subprocess.run([*base, "down", "-v"], env=ctx.env, check=False)


def _debug_detach(args: argparse.Namespace, port: int) -> int:
    """Spawn the foreground debug flow detached, poll its log, exit 0 once attachable.

    Polls the spawned process's log for the lldb-server banner. This is the
    CLion 'before launch' hook — CLion SIGTERMs its own process group, so the
    child must be in a new session (start_new_session) to survive.
    """
    # A deterministic, port-keyed path the CLion run-config and `_banner_ready`
    # both read; not a randomized tempfile by design.
    log_file = Path(f"/tmp/conntest-debug-{port}.log")  # noqa: S108  # see comment above
    fg_argv = [
        sys.executable,
        str(_CONNTEST_EXE),
        "debug",
        args.scenario or "round_trip_10_buffers",
        "--plugin",
        args.plugin,
        "--port",
        str(port),
        "--build-dir",
        _resolve_build_dir(args.build_dir),
    ]
    if args.image:
        fg_argv += ["--image", args.image]
    if args.selector:
        fg_argv += ["--selector", args.selector]

    log = log_file.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        fg_argv,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    _log(f"debug: detached (pid={proc.pid} log={log_file})")
    _log("debug: waiting for lldb-server (cold start can take ~30s)…")
    for i in range(120):
        if proc.poll() is not None:
            _log("debug: background process exited before lldb-server started")
            _tail(log_file, 20)
            return proc.returncode or 1
        if _banner_ready(log_file, port):
            time.sleep(1)
            _log(f"debug: lldb-server ready on {port} after ~{i}s — CLion can attach")
            return 0
        time.sleep(1)
    _log(f"debug: port {port} did not come up within 120s")
    _tail(log_file, 30)
    proc.terminate()
    return 1


def _banner_ready(log_file: Path, port: int) -> bool:
    try:
        for line in log_file.read_text(encoding="utf-8", errors="replace").splitlines():
            if "gdbserver listening on" in line and line.rstrip().endswith(f":{port}"):
                return True
    except FileNotFoundError:
        pass
    return False


def _tail(path: Path, n: int) -> None:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        for line in lines[-n:]:
            print(f"  {line}", file=sys.stderr)
    except FileNotFoundError:
        pass
