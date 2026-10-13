# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""_util.py — shared primitives for the conntest CLI package.

The leaf of the ``conntest_runner.cli`` dependency graph: stderr logging, the
process-exit helper, the subprocess-capture wrapper, repo-root resolution, and
the static framework paths every command module derives from. Imports nothing
from ``conntest_runner.cli.*``.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing import NoReturn

# ---------------------------------------------------------------------------
# Static locations (relative to this file: .../conntest_runner/cli/_util.py)
# ---------------------------------------------------------------------------
_RUNNER_DIR = Path(__file__).resolve().parents[3]  # .../nes-conn-test/runner
_REPO_ROOT_FALLBACK = _RUNNER_DIR.parents[1]  # .../<repo>
_RUNNER_COMPOSE = _RUNNER_DIR / "runner.compose.yaml"
_TOXIPROXY_COMPOSE = _RUNNER_DIR / "toxiproxy.compose.yaml"


def _log(msg: str) -> None:
    print(f"[conntest] {msg}", file=sys.stderr, flush=True)


def _die(msg: str, code: int = 2) -> NoReturn:
    print(f"conntest: {msg}", file=sys.stderr, flush=True)
    raise SystemExit(code)


def _capture(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str] | None:
    """Run ``cmd`` capturing text output; ``None`` if the binary is absent.

    The recurring host-probe shape: run a tool, read its ``stdout`` / check its
    ``returncode``, and treat a missing binary (``FileNotFoundError`` — e.g. no
    ``docker``/``git`` on PATH) as "no answer". Always ``check=False``; callers
    inspect ``returncode`` themselves, so a non-zero exit returns the process
    rather than raising.
    """
    try:
        return subprocess.run(cmd, capture_output=True, text=True, check=False, **kwargs)
    except FileNotFoundError:
        return None


def _repo_root() -> Path:
    proc = _capture(["git", "rev-parse", "--show-toplevel"], cwd=str(_RUNNER_DIR))
    if proc is not None and proc.returncode == 0 and (out := proc.stdout.strip()):
        return Path(out)
    return _REPO_ROOT_FALLBACK
