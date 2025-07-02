# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import os
from pathlib import Path
import subprocess
import tempfile
import json

def is_exe(file: Path) -> bool:
    if not file.exists():
        return False

    if not file.is_file():
        return False

    return os.access(file, os.X_OK)


def cov_from_cmd(cmd: list[str]):
    assert is_exe(Path(cmd[0]))
    with tempfile.NamedTemporaryFile() as profraw, tempfile.NamedTemporaryFile() as profdata:
        env = os.environ | {
            "ASAN_OPTIONS": "detect_leaks=0",
            "LLVM_PROFILE_FILE": profraw.name,
        }
        subprocess.run(cmd, check=True, env=env)
        subprocess.run(["llvm-profdata-19", "merge", "-sparse", profraw.name, "-o", profdata.name], check=True)
        cov_json = subprocess.run(["llvm-cov-19", "export", "-format=text", f"-instr-profile={profdata.name}", cmd[0]], check=True, capture_output=True, text=True)
        cov_dir = Path(f"cov-{"_".join(cmd)}")
        subprocess.run(["llvm-cov-19", "show", "-format=html", f"-output-dir={cov_dir}", f"-instr-profile={profdata.name}", cmd[0]], check=True, capture_output=True, text=True)

        return json.loads(cov_json.stdout), cov_dir


def split_list(l: list, splitter):
    res = []
    cur = []
    for el in l:
        if el == splitter:
            res.append(cur)
            cur = []
            continue
        cur.append(el)
    if cur:
        res.append(cur)
    return res


def main():
    args = sys.argv[1:]

    cmds = split_list(args, "--")

    covs_w_dirs = [cov_from_cmd(cmd) for cmd in cmds]

    for _, d in covs_w_dirs:
        print(d)

    covs = [j for j, _ in covs_w_dirs]

    print(len(covs[0]["data"][0]["files"]))
    print(covs[0]["data"][0]["files"][0].keys())
    print(len(covs[0]["data"][0]["functions"]))
    print(covs[0]["data"][0]["functions"][0].keys())
    print()
    print(covs[0]["data"][0]["totals"])
    # print(covs[1]["data"][0]["totals"])





if __name__ == "__main__":
    main()
