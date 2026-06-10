# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""context.py — the per-invocation RunContext and sanitizer-suppression merge.

``RunContext`` resolves everything once (image, bind-mount source/target, build
dir, compose env) and is reused for every plugin. ``_build_context`` constructs
it and folds in the per-connector sanitizer suppressions: conntest-harness is one
binary that whole-archives every plugin, so each connector's known-benign
findings (declared in its ``conn-test/*.lsan-supp`` / ``*.tsan-supp``) are merged
into one document and the sanitizers are pointed at it.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

from conntest_runner.cli._util import _die, _repo_root
from conntest_runner.cli.env import _read_container_repo_from_cache

# (file extension, env var, option separator). TSan parses space-separated
# options; LSan colon-separated — matching the symbolizer-injection split in
# `_runner_bash_body`, which appends to whatever this sets.
_SUPP_SANITIZERS = (
    ("lsan-supp", "LSAN_OPTIONS", ":"),
    ("tsan-supp", "TSAN_OPTIONS", " "),
)
# A directive is `<class>:<pattern>` (leak:, race:, mutex:, deadlock:, …).
# Comments (`#`) and blank lines are not directives.
_SUPP_DIRECTIVE = re.compile(r"^[a-z_]+:")


@dataclass
class RunContext:
    """Everything resolved once per invocation and reused for every plugin."""

    repo_root: Path
    image: str
    host_root: str  # bind-mount source (host-daemon-visible repo)
    container_repo: str  # bind-mount target == CMAKE_HOME_DIRECTORY
    host_build_dir: Path  # host path to the build dir
    container_build_dir: str  # build dir as seen inside the runner (pytest)
    env: dict[str, str]  # environment for `docker compose`
    k_expr: str = ""
    pytest_extra: list[str] = field(default_factory=list)


def _build_context(
    *,
    image: str,
    build_dir_name: str,
    k_expr: str,
    pytest_extra: list[str],
    extra_env: dict[str, str] | None = None,
) -> RunContext:
    repo_root = _repo_root()
    host_root = os.environ.get("HOST_NEBULASTREAM_ROOT", "").strip() or str(repo_root)
    host_build_dir = (repo_root / build_dir_name).resolve()
    if not (host_build_dir / "CMakeCache.txt").is_file():
        _die(
            f"build dir {host_build_dir} is not configured (no CMakeCache.txt). "
            "Configure it (CLion CMake profile, or `cmake -B <dir>`), or pass "
            "--build-dir to point at an existing one."
        )
    container_repo = _read_container_repo_from_cache(host_build_dir) or host_root
    # The runner sees the repo at container_repo; translate the build dir
    # into that view. On CI host_root == container_repo so this is identity;
    # locally it maps e.g. <repo>/cmake-build-debug -> /tmp/nebulastream/cmake-build-debug.
    try:
        rel = host_build_dir.relative_to(Path(host_root))
    except ValueError:
        rel = Path(host_build_dir.name)
    container_build_dir = f"{container_repo}/{rel.as_posix()}"

    env = os.environ.copy()
    env["HOST_NEBULASTREAM_ROOT"] = host_root
    env["CONNTEST_CONTAINER_REPO"] = container_repo
    env["CONNTEST_DEV_IMAGE"] = image
    env["CONNTEST_HOST_UID"] = str(os.getuid())
    env["CONNTEST_HOST_GID"] = str(os.getgid())
    env.setdefault("CONNTEST_READY_TIMEOUT_S", os.environ.get("CONNTEST_READY_TIMEOUT_S", "10"))
    if extra_env:
        env.update(extra_env)

    ctx = RunContext(
        repo_root=repo_root,
        image=image,
        host_root=host_root,
        container_repo=container_repo,
        host_build_dir=host_build_dir,
        container_build_dir=container_build_dir,
        env=env,
        k_expr=k_expr,
        pytest_extra=pytest_extra,
    )
    # Merge each connector's sanitizer suppressions and point the harness's
    # LSan/TSan at the result (no-op when no connector ships any).
    _apply_plugin_suppressions(ctx)
    return ctx


def _find_plugin_suppressions(repo_root: Path, ext: str) -> list[Path]:
    """Every connector's ``conn-test/*.<ext>`` file, sorted for a stable merge."""
    return sorted((repo_root / "nes-plugins").rglob(f"conn-test/*.{ext}"))


def _merge_suppressions(texts: list[str]) -> str:
    """Concatenate connector suppression files into one deduped document.

    A library shared by two connectors (paho, by both MQTT connectors) is
    declared once in each — self-contained — so the merge keeps each directive
    only once, first-seen order. Comments are dropped: the rationale lives in
    the per-connector source files; this generated artifact only needs the
    directives the sanitizer matches against.
    """
    seen: set[str] = set()
    lines = [
        "# Generated by conntest — merged connector sanitizer suppressions.",
        "# Source of truth: each connector's nes-plugins/*/*/conn-test/*-supp.",
    ]
    for text in texts:
        for raw in text.splitlines():
            line = raw.strip()
            if not _SUPP_DIRECTIVE.match(line) or line in seen:
                continue
            seen.add(line)
            lines.append(line)
    return "\n".join(lines) + "\n"


def _apply_plugin_suppressions(ctx: RunContext) -> None:
    """Merge per-connector suppression files and point the sanitizers at them.

    Writes one merged file per sanitizer under the (bind-mounted, gitignored)
    build dir and appends ``suppressions=<container path>`` to the matching
    ``*SAN_OPTIONS``. A no-op for a sanitizer no connector supplies. Harmless on
    a non-matching lane / non-sanitizer build — an uninstrumented binary just
    ignores the env var.
    """
    supp_dir = ctx.host_build_dir / "conntest-suppressions"
    for ext, var, sep in _SUPP_SANITIZERS:
        files = _find_plugin_suppressions(ctx.repo_root, ext)
        if not files:
            continue
        merged = _merge_suppressions([f.read_text(encoding="utf-8") for f in files])
        supp_dir.mkdir(parents=True, exist_ok=True)
        name = f"conntest.{ext}"
        (supp_dir / name).write_text(merged, encoding="utf-8")
        opt = f"suppressions={ctx.container_build_dir}/conntest-suppressions/{name}"
        existing = ctx.env.get(var, "")
        ctx.env[var] = f"{existing}{sep}{opt}" if existing else opt
