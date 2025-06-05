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

import re
import subprocess
import sys

license_text = """/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
"""

license_text_cmake = (
"""# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
)

COLOR_RED_BOLD = "\033[1;31m"
COLOR_RESET = "\033[0m"

INGORED_ENDINGS = {
    "clang-format",
    "clang-tidy",
    "csv",
    "disabled",
    "dockerfile",
    "dockerignore",
    "dummy",
    "env",
    "gitignore",
    "json",
    "kts",
    "md",
    "md5",
    "patch",
    "png",
    "properties",
    "svg",
    "test",
    "xml",
    "yaml",
    "zip",
}

VENDORED_FILES = {
    "cmake/CodeCoverage.cmake",
    "nes-systests/utils/SystestPlugin/NES-Systest-Runner/gradlew",
    "nes-systests/utils/SystestPlugin/NES-Systest-Runner/gradlew.bat",
    "vcpkg/vcpkg-registry/ports/llvm/portfile.cmake",
}

if __name__ == "__main__":

    git_ls_files = subprocess.run(["git", "ls-files"], capture_output=True, text=True, check=True)

    result = True

    files = git_ls_files.stdout.split("\n")
    for filename in files:
        filename = filename.strip()

        suffix = filename.split(".")[-1]

        if filename in VENDORED_FILES:
            pass
        elif suffix in {"cpp", "proto", "java", "g4", "inc"}:
            with open(filename, "r", encoding="utf-8") as fp:
                content = fp.read()
                if not content.startswith(license_text):
                    print(f'{COLOR_RED_BOLD}Error{COLOR_RESET}: file lacks license preamble: {filename}:1')
                    result = False
        elif suffix in {"h", "hpp"} or filename.endswith(".inc.in"):
            with open(filename, "r", encoding="utf-8") as fp:
                content = fp.read()
                # Use regex to match the license text followed by any number of newlines and #pragma once
                pattern = re.escape(license_text) + r'\s*#pragma once\s*'
                if not re.match(pattern, content, re.DOTALL):
                    print(f'{COLOR_RED_BOLD}Error{COLOR_RESET}: file lacks license preamble followed by #pragma once: {filename}:1')
                    result = False
        elif filename.endswith("CMakeLists.txt") or suffix == "cmake":
            with open(filename, "r", encoding="utf-8") as fp:
                content = fp.read()
                if not content.startswith(license_text_cmake):
                    print(f'{COLOR_RED_BOLD}Error{COLOR_RESET}: file lacks license preamble: {filename}:1')
                    result = False
        elif suffix in {"sh", "py"}:
            with open(filename, "r", encoding="utf-8") as fp:
                content = fp.read()
                if license_text_cmake not in content:
                    print(f'{COLOR_RED_BOLD}Error{COLOR_RESET}: file lacks license preamble (potentially after shebang): {filename}:3')
                    result = False
        elif suffix in INGORED_ENDINGS or "." not in filename or filename.startswith(".github"):
            pass
        else:
            print(f'{COLOR_RED_BOLD}Error{COLOR_RESET}: check_preamble.py does not know how to check file for license preamble, pls fix: {filename}:3')
            result = False

    if not result:
        sys.exit(1)

sys.exit(0)
