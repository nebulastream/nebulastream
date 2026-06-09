#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate the plugin-coverage matrix for the disabled/enabled plugin workflows.

Usage:
    python generate_plugins_matrix.py --mode disabled              # pretty-print
    python generate_plugins_matrix.py --mode enabled --output-github

The plugin list is auto-discovered by scanning nes-plugins/CMakeLists.txt for
`option(NES_PLUGIN_*)` declarations, so adding a new plugin only requires the
new option line in CMake — the CI matrix updates itself.

--mode disabled emits one entry per plugin disabling that plugin alone, plus a
final "all-plugins-disabled" entry that disables every discovered plugin at
once. --mode enabled emits one entry per plugin that disables every plugin and
then re-enables one. The full -DNES_PLUGIN_X=OFF baseline is baked into the
"arg" string so the workflow only needs a single cmake invocation per entry.
"""

import argparse
import json
import os
import re
import sys

OPTION_PATTERN = re.compile(r"^\s*option\(\s*(NES_PLUGIN_[A-Z0-9_]+)\s+", re.MULTILINE)


def repo_root():
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, "..", ".."))


def discover_plugins():
    """Return the list of NES_PLUGIN_* option names declared in nes-plugins/CMakeLists.txt.

    The order matches their declaration order in the file so the generated
    matrix is stable across runs.
    """
    cmake_path = os.path.join(repo_root(), "nes-plugins", "CMakeLists.txt")
    with open(cmake_path) as f:
        content = f.read()
    plugins = OPTION_PATTERN.findall(content)
    if not plugins:
        print(
            f"ERROR: no NES_PLUGIN_* options found in {cmake_path}",
            file=sys.stderr,
        )
        sys.exit(1)
    return plugins


def short_name(option):
    """NES_PLUGIN_TCP_SOURCE -> tcp-source"""
    return option.removeprefix("NES_PLUGIN_").lower().replace("_", "-")


def disabled_matrix(plugins):
    entries = [
        {"name": f"no-{short_name(p)}", "arg": f"-D{p}=OFF"}
        for p in plugins
    ]
    all_off = " ".join(f"-D{p}=OFF" for p in plugins)
    entries.append({"name": "all-plugins-disabled", "arg": all_off})
    return entries


def enabled_matrix(plugins):
    all_off = " ".join(f"-D{p}=OFF" for p in plugins)
    return [
        {"name": f"only-{short_name(p)}", "arg": f"{all_off} -D{p}=ON"}
        for p in plugins
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        required=True,
        choices=["disabled", "enabled"],
        help="Which matrix to generate",
    )
    parser.add_argument(
        "--output-github",
        dest="output_github",
        action="store_true",
        help="Write matrix JSON to $GITHUB_OUTPUT as 'matrix' variable",
    )
    args = parser.parse_args()

    plugins = discover_plugins()
    matrix = disabled_matrix(plugins) if args.mode == "disabled" else enabled_matrix(plugins)
    matrix_json = json.dumps(matrix, separators=(",", ":"))

    if args.output_github:
        github_output = os.environ.get("GITHUB_OUTPUT", "")
        if not github_output:
            print("ERROR: GITHUB_OUTPUT not set", file=sys.stderr)
            sys.exit(1)
        with open(github_output, "a") as f:
            f.write(f"matrix={matrix_json}\n")
        print(
            f"Wrote {len(matrix)} {args.mode}-mode jobs to GITHUB_OUTPUT",
            file=sys.stderr,
        )
    else:
        print(json.dumps(matrix, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
