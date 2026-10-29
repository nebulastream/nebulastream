# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The ``--build-dir`` option and the fixtures that the runner's tests and the conformance battery share.

Without ``--build-dir``, tests that need the harness are skipped, so the pure Python tests still run.

With it, a missing harness binary fails, so a ctest run cannot pass by skipping everything.
"""

from __future__ import annotations

from pathlib import Path

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--build-dir",
        action="store",
        default=None,
        metavar="DIR",
        help="CMake build dir holding nes-conn-test/conntest-harness",
    )


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def harness_binary(request: pytest.FixtureRequest) -> Path:
    build_dir = request.config.getoption("--build-dir")
    if build_dir is None:
        pytest.skip("no --build-dir given")
    binary = Path(build_dir) / "nes-conn-test" / "conntest-harness"
    if not binary.is_file():
        pytest.fail(f"conntest-harness not built at {binary}; build the conntest-harness target")
    return binary


@pytest.fixture
def worker_id(request: pytest.FixtureRequest) -> str:
    workerinput = getattr(request.config, "workerinput", None)
    if workerinput is not None:
        return workerinput.get("workerid", "master")
    return "master"
