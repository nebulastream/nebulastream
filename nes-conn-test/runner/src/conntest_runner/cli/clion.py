# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""clion.py — the ``install-clion`` subcommand (CLion run-config generation).

Generates three ``.idea/runConfigurations/*.xml`` from fixed templates (Debug,
its before-launch Launcher, and the battery Runner), idempotently: a write is
skipped only when every target already byte-matches AND parses. Removes the
pre-rename stale configs. A pure host-side file op — no docker, no self-wrap.
"""

from __future__ import annotations

import contextlib
import os
from typing import TYPE_CHECKING
from xml.dom import minidom

from conntest_runner.cli._util import _die, _repo_root
from conntest_runner.cli.env import _resolve_build_dir

if TYPE_CHECKING:
    import argparse
    from pathlib import Path


def _cmd_install_clion(args: argparse.Namespace) -> int:
    return _install_clion(quiet=args.quiet)


def _install_clion(*, quiet: bool) -> int:
    write_root = _repo_root()
    host_root = os.environ.get("HOST_NEBULASTREAM_ROOT", "").strip() or str(write_root)
    out_dir = write_root / ".idea" / "runConfigurations"
    debug_xml = out_dir / "ConnTest_Debug.xml"
    launcher_xml = out_dir / "ConnTest_Debug_Launcher.xml"
    run_xml = out_dir / "ConnTest.xml"

    conntest = f"{host_root}/nes-conn-test/runner/conntest"
    build_dir_name = _resolve_build_dir(repo_root=write_root)
    symbol_file = f"{host_root}/{build_dir_name}/nes-conn-test/conntest-harness"

    stale = [
        out_dir / "ConnTest_Debug_selfcheck.xml",
        # Pre-rename filenames (ConnTest Source Smoke / Debug ConnTest
        # (source) / before-launch helper); remove so CLion does not show
        # the old configs alongside the renamed ones.
        out_dir / "ConnTest_Debug_source.xml",
        out_dir / "ConnTest_Debug_BeforeLaunch.xml",
        out_dir / "ConnTest_Source_Smoke.xml",
        # Must stay last: _clean_stale rmdir's this entry's parent dir.
        write_root / ".idea" / "tools" / "External Tools.xml",
    ]

    debug_body = _CLION_DEBUG_XML.format(symbol_file=symbol_file)
    launcher_body = _CLION_LAUNCHER_XML.format(conntest=conntest)
    run_body = _CLION_RUN_XML.format(conntest=conntest)

    targets = (
        (debug_xml, debug_body),
        (launcher_xml, launcher_body),
        (run_xml, run_body),
    )
    # Idempotency: skip the write when every file already byte-matches what
    # we would generate AND is well-formed (CMake calls us on every
    # configure). Exact match auto-detects any template change — these
    # configs are generated and marked "do not edit", so we do not try to
    # preserve GUI edits to them. The wellformedness guard stops a
    # previously-broken file from being cached as "up to date" (CLion
    # silently drops malformed run-configs from its dropdown).
    if all(
        p.is_file() and p.read_text(encoding="utf-8") == body and _xml_wellformed(p)
        for p, body in targets
    ):
        _clean_stale(stale, quiet=quiet)
        if not quiet:
            print("install-clion: generated XMLs already up to date")
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    for path, body in targets:
        path.write_text(body, encoding="utf-8")
        if not _xml_wellformed(path):
            _die(f"install-clion: generated XML is not well-formed: {path}")
        if not quiet:
            print(f"install-clion: wrote {path}")

    _clean_stale(stale, quiet=quiet)
    if not quiet:
        print(f"install-clion: host repo path = {host_root}")
    return 0


def _xml_wellformed(path: Path) -> bool:
    try:
        # Input is our own freshly-generated run-config (fixed template), not
        # untrusted external XML, so the XXE/billion-laughs concern S318 guards
        # against does not apply and defusedxml is not a project dependency.
        minidom.parseString(path.read_text(encoding="utf-8"))  # noqa: S318  # see comment above
    except Exception:  # noqa: BLE001
        return False
    else:
        return True


def _clean_stale(stale: list[Path], *, quiet: bool) -> None:
    for p in stale:
        if p.is_file():
            p.unlink()
            if not quiet:
                print(f"install-clion: removed stale {p}")
    tools = stale[-1].parent if stale else None
    if tools is not None and tools.is_dir():
        with contextlib.suppress(OSError):
            tools.rmdir()


# CLion run-config XML templates. Comments must NOT contain consecutive
# hyphens (IntelliJ silently drops run-configs whose XML fails to parse).
_CLION_DEBUG_XML = """<component name="ProjectRunConfigurationManager">
  <!--
    Conn-test remote debug (source or sink). GENERATED by
    `conntest install-clion`. Do not edit directly; edit the template in
    src/conntest_runner/cli/clion.py and reconfigure cmake (which invokes the
    generator automatically).

    Click Debug here: the Before Launch step runs the sibling
    "ConnTest Debug Launcher" config, which invokes
    `conntest debug` in detach mode to spawn the dev container, broker, and
    lldb-server on tcp:127.0.0.1:2345, then returns once the listener is
    ready so CLion can attach. kind="LLDB" pairs the lldb client with
    lldb-server (the GDB client mishandles lldb-server's aarch64 register
    packet). The CMake build applies -fdebug-prefix-map so gutter
    breakpoints resolve with no path mapping.
    NB: XML comments must not contain double hyphens.
  -->
  <configuration default="false"
                 name="ConnTest Debug"
                 type="CLion_Remote"
                 version="1"
                 remoteCommand="connect://127.0.0.1:2345"
                 symbolFile="{symbol_file}"
                 sysroot=""
                 useSudo="false">
    <debugger kind="LLDB" isBundled="true" />
    <method v="2">
      <option name="RunConfigurationTask"
              enabled="true"
              run_configuration_name="ConnTest Debug Launcher"
              run_configuration_type="ShConfigurationType" />
    </method>
  </configuration>
</component>
"""

_CLION_LAUNCHER_XML = """<component name="ProjectRunConfigurationManager">
  <!--
    Conn-test debug Before Launch helper. GENERATED by
    `conntest install-clion`. Wraps `conntest debug` (detach mode) as a
    Shell Script run config so "ConnTest Debug" can chain it via
    RunConfigurationTask. The user typically does not run this directly.

    SCRIPT_TEXT below is intentionally generic (no scenario/plugin pinned):
    clicking Debug as-shipped fails fast with on-screen instructions rather
    than silently debugging one hardcoded connector. To target a real run,
    edit SCRIPT_TEXT the same way you would the "ConnTest" config: it takes
    the regular `conntest debug` args. The first positional is the scenario
    short name (round_trip, empty, config, bad_endpoint, reconnect, …) and
    the plugin flag selects the connector. The chosen connector's setup_file
    (e.g. mosquitto.conf) is mounted automatically. Remember to point
    breakpoints at that connector's source. A reconfigure regenerates this
    file from the template, so to keep a custom selection across configures
    duplicate the config in CLion and edit the copy.
    NB: XML comments must not contain consecutive hyphens; the flags
    themselves live in SCRIPT_TEXT below.
  -->
  <configuration default="false"
                 name="ConnTest Debug Launcher"
                 type="ShConfigurationType">
    <option name="SCRIPT_TEXT"
            value="exec &quot;{conntest}&quot; debug --detach" />
    <option name="INDEPENDENT_SCRIPT_PATH" value="true" />
    <option name="SCRIPT_PATH" value="" />
    <option name="SCRIPT_OPTIONS" value="" />
    <option name="INDEPENDENT_SCRIPT_WORKING_DIRECTORY" value="true" />
    <option name="SCRIPT_WORKING_DIRECTORY" value="$PROJECT_DIR$" />
    <option name="INDEPENDENT_INTERPRETER_PATH" value="true" />
    <option name="INTERPRETER_PATH" value="/bin/bash" />
    <option name="INTERPRETER_OPTIONS" value="" />
    <option name="EXECUTE_IN_TERMINAL" value="false" />
    <option name="EXECUTE_SCRIPT_FILE" value="false" />
    <envs />
    <method v="2" />
  </configuration>
</component>
"""

_CLION_RUN_XML = """<component name="ProjectRunConfigurationManager">
  <!--
    Conn-test battery runner. GENERATED by
    `conntest install-clion`. Click Run to build conntest-harness in the
    runner then execute the conformance battery (the `run` subcommand with
    the build flag, in SCRIPT_TEXT below). Edit that command (or duplicate
    the config in CLion) to append a scenario selector such as round_trip.
    NB: XML comments must not contain consecutive hyphens.
  -->
  <configuration default="false"
                 name="ConnTest"
                 type="ShConfigurationType">
    <option name="SCRIPT_TEXT" value="exec &quot;{conntest}&quot; run --build" />
    <option name="INDEPENDENT_SCRIPT_PATH" value="true" />
    <option name="SCRIPT_PATH" value="" />
    <option name="SCRIPT_OPTIONS" value="" />
    <option name="INDEPENDENT_SCRIPT_WORKING_DIRECTORY" value="true" />
    <option name="SCRIPT_WORKING_DIRECTORY" value="$PROJECT_DIR$" />
    <option name="INDEPENDENT_INTERPRETER_PATH" value="true" />
    <option name="INTERPRETER_PATH" value="/bin/bash" />
    <option name="INTERPRETER_OPTIONS" value="" />
    <option name="EXECUTE_IN_TERMINAL" value="true" />
    <option name="EXECUTE_SCRIPT_FILE" value="false" />
    <envs />
    <method v="2" />
  </configuration>
</component>
"""
