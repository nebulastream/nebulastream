# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""battery.py — the conformance battery engine (run / ci / coverage).

Drives one plugin at a time inside its own compose project: ``up -d --wait`` the
data services, run pytest in a one-off runner on the project network, capture a
postmortem on failure, ``down -v`` in ``finally``. A plugin whose scenarios pin
``setup_file``s runs one stack lifecycle per file. ``_setup_file_groups`` /
``_debug_setup_file`` / ``_SCENARIO_K`` are shared with the debug path.
"""

from __future__ import annotations

import re
import shlex
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from xml.etree import ElementTree as ET

from conntest_runner.cli._util import (
    _RUNNER_COMPOSE,
    _TOXIPROXY_COMPOSE,
    _die,
    _log,
)
from conntest_runner.cli.bash import _runner_bash_body
from conntest_runner.cli.context import _build_context
from conntest_runner.cli.env import _resolve_build_dir, _resolve_image
from conntest_runner.cli.plugins import (
    _compose_data_services,
    find_plugin_composes,
    plugin_name,
    select_plugin_composes,
)

if TYPE_CHECKING:
    import argparse

    from conntest_runner.cli.context import RunContext

_PROJECT_PREFIX = "nebulastream-conntest"
_CI_BUILD_DIR = "build"

# pytest's exit code for "no tests collected" — a -k selector / scenario-group
# restriction legitimately deselected everything, which is not a failure.
_PYTEST_EXIT_NO_TESTS = 5


@dataclass
class _GroupOutcome:
    """The result of one plugin/setup_file-group pytest run, for the end summary.

    ``conformance_junit`` / ``meta_junit`` are *host* paths to the JUnit XMLs the
    in-runner pytest wrote (the build dir is bind-mounted, so a container-written
    file appears here); ``None`` when that pytest did not run (data services never
    came up, or ``set -e`` aborted the meta step before conformance). A non-zero
    ``rc`` with no per-test failures in the JUnit is an infrastructure failure —
    surfaced in the summary so it is never silent. ``deselected`` marks an exit-5
    "selector matched nothing" run: a success with no tests.
    """

    label: str
    rc: int
    conformance_junit: Path | None = None
    meta_junit: Path | None = None
    deselected: bool = False


def _slug(text: str) -> str:
    """Filesystem-safe slug for a setup_file, to name its per-group JUnit XML."""
    return re.sub(r"[^A-Za-z0-9]+", "-", text).strip("-").lower() or "group"


def _junit_paths(
    ctx: RunContext, plugin: str, *, group_tag: str | None, run_meta: bool
) -> tuple[str, Path, Path | None]:
    """JUnit locations for one group: (container ``--junit-xml`` arg, host conformance, host meta).

    The build dir is bind-mounted, so a container-written file appears at the host
    path the summary parses. ``meta`` is set only on the first group of the first
    plugin (it runs the framework self-tests into their own JUnit).
    """
    if group_tag is None:
        base = f"junit-conntest-{plugin}.xml"
    else:
        base = f"junit-conntest-{plugin}-{group_tag}.xml"
    meta = ctx.host_build_dir / "junit-conntest-meta.xml" if run_meta else None
    return f"{ctx.container_build_dir}/{base}", ctx.host_build_dir / base, meta


# Short scenario name -> pytest -k substring. The generic battery is one
# parametrized test whose ids are "<scenario>-<plugin>-<config>", so a
# scenario's keyword is simply its name (matched as a substring of the id).
_SCENARIO_K = {
    "round_trip": "round_trip",  # substring of every tier id → runs all four
    "round-trip": "round_trip",
    "round_trip_1_buffer": "round_trip_1_buffer",
    "round_trip_10_buffers": "round_trip_10_buffers",
    "round_trip_100_buffers": "round_trip_100_buffers",
    "round_trip_n_buffers": "round_trip_n_buffers",
    "empty": "empty",
    "config": "config",  # substring of both ids → runs config_valid and config_invalid
    "config_valid": "config_valid",
    "config_invalid": "config_invalid",
    "bad_endpoint": "bad_endpoint",
    "bad-endpoint": "bad_endpoint",
    "reconnect": "reconnect",
    "outage": "outage",
    "conformance": "",
    "": "",
}


# ---------------------------------------------------------------------------
# Build the harness inside the runner (local convenience: `conntest run --build`)
# ---------------------------------------------------------------------------
def _build_harness(ctx: RunContext) -> None:
    project = f"{_PROJECT_PREFIX}-build-{uuid.uuid4().hex}"
    base = ["docker", "compose", "-f", str(_RUNNER_COMPOSE), "-p", project]
    body = f"set -e; cmake --build {shlex.quote(ctx.container_build_dir)} --target conntest-harness"
    _log(f"building conntest-harness in the runner ({ctx.container_build_dir})")
    try:
        subprocess.run(
            [*base, "run", "--rm", "-T", "--no-deps", "--entrypoint", "bash", "runner", "-c", body],
            env=ctx.env,
            check=True,
        )
    finally:
        subprocess.run([*base, "down", "-v"], env=ctx.env, check=False)


# ---------------------------------------------------------------------------
# Run one plugin's battery inside its own compose project
# ---------------------------------------------------------------------------
def _setup_file_groups(plugin_compose: Path) -> list[tuple[str | None, str | None]]:
    """How to split a plugin's run into stack lifecycles.

    Returns ``(setup_file, scenario_keyword)`` pairs — one stack ``up``/``down``
    per pair. ``setup_file`` (or None) is the service start-up file to mount;
    ``scenario_keyword`` (or None for "no restriction") is a pytest ``-k``
    sub-expression scoping the run to that group's scenarios.

    A plugin whose scenarios declare no ``setup_file`` runs as a single default
    group — identical to the pre-grouping behaviour. Connectors that *do* pin
    setup files get one stack per distinct file, each started with that file
    mounted. The compose↔setup_file contract is enforced here so neither half
    can be configured without the other (no silently-ignored ``setup_file``,
    no dangling ``${CONNTEST_SETUP_FILE}``).
    """
    # Deferred: discovery pulls in the runner's third-party deps the host python may lack.
    from conntest_runner.discovery import read_plugin_scenarios  # noqa: PLC0415
    from conntest_runner.scenarios import scenario_is_bind_only  # noqa: PLC0415

    plugin = plugin_name(plugin_compose)
    scenarios = read_plugin_scenarios(plugin_compose.parent)
    compose_uses_var = "CONNTEST_SETUP_FILE" in plugin_compose.read_text(encoding="utf-8")

    if scenarios is None:
        if compose_uses_var:
            _die(
                f"{plugin}: compose.yaml mounts ${{CONNTEST_SETUP_FILE}} but the "
                f"scenarios live on the loader class attribute, where setup_file "
                f"cannot be declared. Move them to conn-test/scenarios.py."
            )
        return [(None, None)]

    # BIND-only scenarios (config_valid/config_invalid) spin up no service, so
    # they pin no setup_file and are exempt from the contract below; they fold into
    # the primary group at the end.
    service_scenarios = [s for s in scenarios if not scenario_is_bind_only(s.name)]
    with_file = [s for s in service_scenarios if s.setup_file]
    if compose_uses_var and len(with_file) != len(service_scenarios):
        missing = sorted({s.name for s in service_scenarios if not s.setup_file})
        _die(
            f"{plugin}: compose.yaml mounts ${{CONNTEST_SETUP_FILE}}, so every "
            f"service-backed scenario must declare setup_file; missing on: {', '.join(missing)}"
        )
    if not compose_uses_var and with_file:
        named = sorted({s.name for s in with_file})
        _die(
            f"{plugin}: scenario(s) {', '.join(named)} declare setup_file but "
            f"compose.yaml hardcodes its setup mount. Switch the mount to "
            f"${{CONNTEST_SETUP_FILE}} or drop setup_file."
        )
    if not with_file:
        return [(None, None)]  # single default stack; no per-scenario restriction

    # Every declared setup_file must exist on disk. The compose mounts it as
    # ${CONNTEST_SETUP_FILE}; a missing source makes the docker daemon
    # auto-create a *directory* at that path, which then fails the bind with
    # the opaque "cannot mount a directory onto a file" OCI error at `up`
    # time (and litters the conn-test dir with an empty dir named after the
    # phantom file). Catch it here — where we already own the compose↔
    # setup_file contract — with a message that names the offending file.
    missing_files = sorted(
        {
            s.setup_file
            for s in with_file
            if s.setup_file is not None and not (plugin_compose.parent / s.setup_file).is_file()
        }
    )
    if missing_files:
        _die(
            f"{plugin}: scenario(s) declare setup_file(s) that do not exist "
            f"under {plugin_compose.parent}: {', '.join(missing_files)}. "
            f"Create the file(s) or fix the setup_file name in scenarios.py."
        )

    # One stack per distinct setup_file, each scoped to its scenarios' tests. A
    # BIND-only scenario pins no file, so it folds into the primary group (the
    # first service-backed file) — it runs there with no service of its own rather
    # than forming a service-less group whose ${CONNTEST_SETUP_FILE} mount can't be
    # satisfied. (`with_file` is non-empty here — the `not with_file` early return
    # above handled the no-pinned-file case.)
    primary = with_file[0].setup_file
    by_file: dict[str | None, set[str]] = {}
    for s in scenarios:
        key = s.setup_file if s.setup_file is not None else primary
        by_file.setdefault(key, set()).add(s.name)
    groups: list[tuple[str | None, str | None]] = []
    for setup_file, names in by_file.items():
        funcs = sorted({_SCENARIO_K.get(n, n) for n in names})
        groups.append((setup_file, " or ".join(funcs)))
    return groups


def _debug_setup_file(plugin_compose: Path, scenario: str) -> str | None:
    """The setup_file the debug stack must mount for ``scenario``.

    A connector's conformance compose may mount ``${CONNTEST_SETUP_FILE}``
    The run path resolves one per setup_file
    group, but debug brings up a single scenario, so pin the file that
    scenario's group declares — without it the ``${...:?required}``
    interpolation aborts ``up`` before the broker even starts. Returns None
    when the connector pins no setup_file (its compose then has no
    ``${CONNTEST_SETUP_FILE}`` to satisfy). Reuses ``_setup_file_groups`` so
    the same compose<->setup_file contract is enforced and a bad/absent file
    is reported with the same message the run path gives.
    """
    func = _SCENARIO_K.get(scenario, scenario)
    groups = _setup_file_groups(plugin_compose)
    for setup_file, scenario_keyword in groups:
        if scenario_keyword is None or func in scenario_keyword.split(" or "):
            return setup_file
    # Scenario name didn't map to any group (e.g. a --selector-only run on a
    # connector that still pins a setup_file): fall back to the first group's
    # file so ${CONNTEST_SETUP_FILE} is satisfied rather than left unset.
    return groups[0][0] if groups else None


def _run_plugin(plugin_compose: Path, ctx: RunContext, *, run_meta: bool) -> list[_GroupOutcome]:
    """Run a plugin's battery, one stack lifecycle per setup_file group."""
    groups = _setup_file_groups(plugin_compose)
    multi = len(groups) > 1
    outcomes: list[_GroupOutcome] = []
    for i, (setup_file, scenario_keyword) in enumerate(groups):
        # A distinct JUnit name per group so a multi-group plugin's later group
        # cannot overwrite an earlier one's results (the summary parses them all).
        group_tag: str | None = None
        if multi:
            group_tag = _slug(setup_file) if setup_file else f"group{i}"
        outcomes.append(
            _run_plugin_group(
                plugin_compose,
                ctx,
                run_meta=(run_meta and i == 0),
                setup_file=setup_file,
                scenario_keyword=scenario_keyword,
                group_tag=group_tag,
            )
        )
    return outcomes


def _plugin_needs_link(plugin_compose: Path) -> bool:
    """Whether the plugin binds any scenario that needs the fault link.

    Read host-side without importing the loader: the binding only names catalog
    scenarios, and the catalog says which of those route through the link.
    """
    # Deferred: discovery pulls in the runner's third-party deps the host python may lack.
    from conntest_runner.discovery import read_plugin_scenarios  # noqa: PLC0415
    from conntest_runner.scenarios import scenario_needs_link  # noqa: PLC0415

    scenarios = read_plugin_scenarios(plugin_compose.parent)
    return scenarios is not None and any(scenario_needs_link(s.name) for s in scenarios)


def _run_plugin_group(
    plugin_compose: Path,
    ctx: RunContext,
    *,
    run_meta: bool,
    setup_file: str | None,
    scenario_keyword: str | None,
    group_tag: str | None = None,
) -> _GroupOutcome:
    plugin = plugin_name(plugin_compose)
    project = f"{_PROJECT_PREFIX}-{uuid.uuid4().hex}"
    compose_files = [plugin_compose, _RUNNER_COMPOSE]
    # Merge the toxiproxy sidecar only when this plugin binds a fault (needs_link)
    # scenario, so plugins without one bring up no extra service.
    if _plugin_needs_link(plugin_compose):
        compose_files.append(_TOXIPROXY_COMPOSE)
    base = ["docker", "compose"]
    for f in compose_files:
        base += ["-f", str(f)]
    base += ["-p", project]

    # One generic battery covers both kinds (source seed→fill→compare and
    # sink write→read_back→compare are dispatched inside the catalog by kind).
    conformance_test = "conformance/test_conformance.py"
    junit_conformance, host_junit, host_meta_junit = _junit_paths(
        ctx, plugin, group_tag=group_tag, run_meta=run_meta
    )
    pytest_args = [
        conformance_test,
        "-v",
        f"--build-dir={ctx.container_build_dir}",
        f"--junit-xml={junit_conformance}",
    ]
    # Scope by plugin, then (for a setup_file group) by that group's scenarios,
    # then by any user selector / -k.
    clauses = [f"({plugin})"]
    if scenario_keyword:
        clauses.append(f"({scenario_keyword})")
    if ctx.k_expr:
        clauses.append(f"({ctx.k_expr})")
    pytest_args += ["-k", " and ".join(clauses)]
    pytest_args += ctx.pytest_extra
    pytest_quoted = " ".join(shlex.quote(a) for a in pytest_args)

    meta_quoted = ""
    if run_meta:
        meta_args = [
            "tests/test_discovery.py",
            "tests/test_protocol.py",
            "tests/test_datamodel.py",
            # The driver-protocol abuse suite spawns the harness binary, so it
            # needs --build-dir (the binary_paths fixture skips without one) —
            # which the pure-Python meta tests simply ignore.
            "tests/test_driver_protocol.py",
            f"--build-dir={ctx.container_build_dir}",
            "-v",
            f"--junit-xml={ctx.container_build_dir}/junit-conntest-meta.xml",
            *ctx.pytest_extra,
        ]
        meta_quoted = " ".join(shlex.quote(a) for a in meta_args)

    # A setup_file group starts the service with that file mounted (the
    # connector's compose.yaml maps ${CONNTEST_SETUP_FILE} to the right path).
    env = {**ctx.env, "CONNTEST_SETUP_FILE": setup_file} if setup_file else ctx.env
    data_services = _compose_data_services(compose_files, project, env)

    label = plugin if not setup_file else f"{plugin} [setup_file={setup_file}]"
    _log(f"{label}: up compose data service(s) {data_services or '(all)'} (project={project})")
    rc = 1
    try:
        # `up -d --wait` aborts when a data service exits or never turns
        # healthy — the framework's most common CI failure mode (a data
        # container dies during init). Capture a postmortem here, BEFORE the
        # `finally: down -v` destroys the container and its logs, instead of
        # letting `check=True` raise a bare CalledProcessError that strands us
        # with only docker's "exited (N)" line and no container stderr. Return
        # non-zero (don't raise) so the battery still exercises the remaining
        # plugins — a flaky db on one connector no longer masks the rest.
        up_rc = subprocess.run(
            [*base, "up", "-d", "--wait", *data_services],
            env=env,
            check=False,
        ).returncode
        if up_rc != 0:
            _log(
                f"{label}: data service(s) failed to come up (rc={up_rc}); "
                f"capturing postmortem before teardown"
            )
            _capture_postmortem(base, plugin, ctx, env=env, phase="up --wait")
            return _GroupOutcome(label=label, rc=up_rc or 1)
        _log(f"{label}: pytest in a one-off runner on the project network")
        # Forward the active setup_file into the runner so the parametrization
        # collects only this group's scenarios (a scenario reused across setup
        # files otherwise produces a duplicate test in every group).
        setup_env = ["-e", f"CONNTEST_SETUP_FILE={setup_file}"] if setup_file else []
        run_cmd = [
            *base,
            "run",
            "--rm",
            "-T",
            "--no-deps",
            "--entrypoint",
            "bash",
            "-e",
            f"CONNTEST_PYTEST_ARGS={pytest_quoted}",
            "-e",
            f"CONNTEST_META_PYTEST_ARGS={meta_quoted}",
            *setup_env,
            "runner",
            "-c",
            _runner_bash_body(),
        ]
        rc = subprocess.run(run_cmd, env=env, check=False).returncode
        # pytest exit 5 == "no tests collected": a -k selector / scenario-group
        # restriction legitimately deselected every test here. Not a failure.
        if rc == _PYTEST_EXIT_NO_TESTS and (ctx.k_expr or scenario_keyword):
            _log(f"{label}: no tests matched the selector — skipping")
            return _GroupOutcome(label=label, rc=0, deselected=True)
        if rc != 0:
            _capture_postmortem(base, plugin, ctx, env=env, phase="pytest")
        return _GroupOutcome(
            label=label, rc=rc, conformance_junit=host_junit, meta_junit=host_meta_junit
        )
    finally:
        _log(f"{label}: down -v")
        subprocess.run([*base, "down", "-v"], env=env, check=False)


def _capture_postmortem(
    base: list[str],
    plugin: str,
    ctx: RunContext,
    *,
    env: dict[str, str] | None = None,
    phase: str = "pytest",
) -> None:
    """Dump the project's container states + compose logs next to the junit (§5.4, host-side form).

    On a failing run this turns a CI flake into a readable artifact instead of
    "no idea why". Best-effort; never masks the real failure.

    Captured (before ``down -v`` destroys the project):
      * ``df -h`` of the build-dir filesystem — instant proof of a full disk,
        the leading cause of data-container init failures on the shared
        self-hosted runners ("No space left on device" during initdb).
      * ``docker compose ps -a`` — per-container status + exit code, which is
        the only place the *reason* ``up --wait`` aborted is visible (docker's
        "exited (N)" line names the code but not the container's own output).
      * ``docker compose logs`` — each service's stderr (e.g. postgres
        initdb / FATAL lines), which ``up --wait`` never prints.
      * ``docker system df`` — WHICH docker resource ate the disk (images /
        build cache / volumes), so the cleanup is targeted. Slow on a bloated
        store (it sizes every object), so it runs last under a timeout and a
        miss never costs us the cheaper sections above.

    Each probe is captured independently and the file is written in a
    ``finally`` so a slow/hung probe still yields a partial artifact rather
    than nothing. ``env`` must be the same environment the failing step ran
    with (it may carry ``CONNTEST_SETUP_FILE``, which the compose files
    interpolate); defaults to ``ctx.env``. ``phase`` records where we failed
    (``up --wait`` vs ``pytest``) so the artifact is self-describing.

    (The full §5.4 idea — an ``lldb -p <pid> thread backtrace all`` of a
    stuck harness — needs an in-runner hook at crash time; the harness is
    already reaped by the time pytest returns to this host process, so we
    capture what the host can still see.)
    """
    env = ctx.env if env is None else env
    out = ctx.host_build_dir / f"postmortem-conntest-{plugin}.log"
    sections = [f"=== conntest postmortem: {plugin} (failed at: {phase}) ==="]

    def _probe(title: str, cmd: list[str], timeout: int) -> None:
        try:
            p = subprocess.run(
                cmd, env=env, capture_output=True, text=True, timeout=timeout, check=False
            )
            sections.append(f"--- {title} ---\n" + p.stdout + p.stderr)
        except Exception as exc:  # noqa: BLE001 - a probe must never raise
            sections.append(f"--- {title} (capture failed: {exc}) ---")

    try:
        _probe("df -h (build-dir filesystem)", ["df", "-h", str(ctx.host_build_dir)], timeout=10)
        _probe("docker compose ps -a", [*base, "ps", "-a"], timeout=30)
        _probe("docker compose logs", [*base, "logs", "--no-color", "--timestamps"], timeout=30)
        # Slow on a bloated store; run last so a timeout can't strand the rest.
        _probe("docker system df", ["docker", "system", "df"], timeout=30)
    finally:
        try:
            out.write_text("\n\n".join(sections), encoding="utf-8")
            _log(f"{plugin}: wrote postmortem ({phase}) to {out}")
        except Exception as exc:  # noqa: BLE001 - postmortem must never raise
            _log(f"{plugin}: postmortem write failed: {exc}")


def _parse_junit(path: Path) -> tuple[int, int, list[str]] | None:
    """Parse a pytest JUnit XML → ``(total, skipped, failed_test_ids)``.

    Returns ``None`` if the file is missing or unparseable, so the caller treats
    the group as an infrastructure failure rather than a clean pass. A failed test
    is a ``<testcase>`` carrying a ``<failure>`` or ``<error>`` child; its ``name``
    is the parametrized id (e.g. ``test_scenario[round_trip_10_buffers-ODBCSink-basic-N10]``).
    """
    try:
        root = ET.parse(path).getroot()  # noqa: S314  # our own pytest JUnit, in our build dir
    except (OSError, ET.ParseError):
        return None
    total = skipped = 0
    failed: list[str] = []
    # ``iter("testsuite")`` yields the root itself when it is a bare <testsuite>
    # and the children when it is a <testsuites> wrapper (pytest 9 emits the latter).
    for suite in root.iter("testsuite"):
        total += int(suite.get("tests", "0"))
        skipped += int(suite.get("skipped", "0"))
        failed.extend(
            case.get("name", "<unknown test>")
            for case in suite.iter("testcase")
            if case.find("failure") is not None or case.find("error") is not None
        )
    return total, skipped, failed


def _aggregate_results(
    outcomes: list[_GroupOutcome],
) -> tuple[int, int, list[tuple[str, str]], list[str]]:
    """Fold every group's JUnit XMLs into ``(total, skipped, failed, infra)``.

    ``failed`` is ``(group label, test id)`` pairs; ``infra`` is the labels of
    groups that returned non-zero with no per-test failure (data services never
    came up, a crash, or an unwritten/unparseable JUnit) — surfaced so a failure
    that produced no JUnit detail is never silently dropped.
    """
    total = skipped = 0
    failed: list[tuple[str, str]] = []
    infra: list[str] = []
    for outcome in outcomes:
        before = len(failed)
        for junit, src in (
            (outcome.conformance_junit, outcome.label),
            (outcome.meta_junit, "meta"),
        ):
            if junit is None:
                continue
            parsed = _parse_junit(junit)
            if parsed is None:
                continue
            count, n_skipped, ids = parsed
            total += count
            skipped += n_skipped
            failed.extend((src, test_id) for test_id in ids)
        if outcome.rc != 0 and len(failed) == before and not outcome.deselected:
            infra.append(outcome.label)
    return total, skipped, failed, infra


def _print_summary(outcomes: list[_GroupOutcome]) -> None:
    """Print the single aggregate pass/fail summary at the very end of the battery."""
    total, skipped, failed, infra = _aggregate_results(outcomes)
    bar = "=" * 64
    print(f"\n{bar}\n  conntest battery summary\n{bar}")
    skip_note = f", {skipped} skipped" if skipped else ""
    if not failed and not infra:
        print(f"✓ All tests passed ({total - skipped} passed{skip_note}).")
        return
    print(f"✗ {len(failed)} failed / {total} tests{skip_note}")
    if failed:
        print("\nFailed tests:")
        for label, test_id in failed:
            print(f"  [{label}] {test_id}")
    if infra:
        print("\nInfrastructure failures (no per-test detail — see postmortem-conntest-*.log):")
        for label in infra:
            print(f"  [{label}]")


def _run_battery(ctx: RunContext, plugins: list[Path], *, with_meta: bool = True) -> int:
    if not plugins:
        _die("no plugin compose.yaml found under nes-plugins/", code=2)
    # Clear stale JUnit XMLs from prior runs so the end summary parses only the
    # files this run writes — a plugin not run this time, or a group whose pytest
    # never started, must not contribute a stale prior result.
    for stale in ctx.host_build_dir.glob("junit-conntest-*.xml"):
        stale.unlink()
    overall = 0
    outcomes: list[_GroupOutcome] = []
    for i, plugin_compose in enumerate(plugins):
        plugin_outcomes = _run_plugin(plugin_compose, ctx, run_meta=(with_meta and i == 0))
        outcomes.extend(plugin_outcomes)
        rc = next((o.rc for o in plugin_outcomes if o.rc != 0), 0)
        if rc != 0:
            _log(f"{plugin_compose} FAILED (rc={rc})")
            overall = rc
    _print_summary(outcomes)
    return overall


# ---------------------------------------------------------------------------
# Selector handling (scenario short name or raw -k expr)
# ---------------------------------------------------------------------------
def _resolve_k_expr(selector: str, k_flag: str) -> str:
    if k_flag:
        return k_flag
    sel = selector.strip()
    if sel in ("discovery", "protocol"):
        _die(
            "discovery/protocol are meta tests; conntest runs them once per "
            "session before the conformance battery. Run without a selector "
            "(or with a per-test keyword)."
        )
    if sel in _SCENARIO_K:
        return _SCENARIO_K[sel]
    return sel  # treat as a raw pytest -k expression


# ---------------------------------------------------------------------------
# Subcommands "run" and "ci"
# ---------------------------------------------------------------------------
def _cmd_run(args: argparse.Namespace, *, ci: bool) -> int:
    image = _resolve_image(args.image, must_exist_locally=not ci)
    build_dir_name = _resolve_build_dir(args.build_dir, default=_CI_BUILD_DIR if ci else None)
    k_expr = "" if ci else _resolve_k_expr(args.selector, args.k)
    ctx = _build_context(
        image=image,
        build_dir_name=build_dir_name,
        k_expr=k_expr,
        pytest_extra=args.pytest_extra,
    )
    _log(f"image={ctx.image}")
    _log(f"build-dir host={ctx.host_build_dir} container={ctx.container_build_dir}")
    if getattr(args, "build", False):
        _build_harness(ctx)
    plugin = getattr(args, "plugin", None)
    composes = select_plugin_composes(ctx.repo_root, plugin, required=True)
    # A focused single-plugin run executes only that plugin's tests — the
    # framework meta tests (discovery/protocol) are not plugin-specific, so
    # skip them here; a full `conntest run` still runs them once.
    return _run_battery(ctx, composes, with_meta=(plugin is None))


# ---------------------------------------------------------------------------
# Subcommand: coverage (instrumented battery + ccov list wiring)
# ---------------------------------------------------------------------------
def _cmd_coverage(args: argparse.Namespace) -> int:
    image = _resolve_image(args.image, must_exist_locally=False)
    build_dir_name = _resolve_build_dir(args.build_dir, default=_CI_BUILD_DIR)
    ctx = _build_context(image=image, build_dir_name=build_dir_name, k_expr="", pytest_extra=[])

    profraw_dir = ctx.host_build_dir / "nes-conn-test" / "profraw"
    if profraw_dir.exists():
        for p in profraw_dir.glob("*.profraw"):
            p.unlink()
    profraw_dir.mkdir(parents=True, exist_ok=True)
    # The harness (in the runner) writes profraws here via the bind mount.
    # On the coverage lane host_root == container_repo, so the host and
    # container views of this path coincide.
    container_profraw = f"{ctx.container_build_dir}/nes-conn-test/profraw"
    ctx.env["LLVM_PROFILE_FILE"] = f"{container_profraw}/conntest.%p-%m.profraw"

    rc = _run_battery(ctx, find_plugin_composes(ctx.repo_root))

    ccov_dir = Path(args.ccov_dir) if args.ccov_dir else ctx.host_build_dir / "ccov"
    ccov_dir.mkdir(parents=True, exist_ok=True)
    profraws = sorted(profraw_dir.glob("*.profraw"))
    if not profraws:
        _die(
            f"no .profraw files in {profraw_dir} after the battery. Was the "
            "build configured with -DCODE_COVERAGE=ON?",
            code=1,
        )
    with (ccov_dir / "profraw.list").open("a", encoding="utf-8") as fh:
        for p in profraws:
            fh.write(f"{p}\n")
    if args.harness:
        with (ccov_dir / "binaries.list").open("a", encoding="utf-8") as fh:
            fh.write(f"-object={args.harness}\n")
    _log(f"coverage: merged {len(profraws)} profraw(s) into {ccov_dir}")
    return rc
