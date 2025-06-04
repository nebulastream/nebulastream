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

if __name__ == "__main__":

    git_ls_files = subprocess.run(["git", "ls-files"], capture_output=True, text=True, check=True)

    result = True

    files = git_ls_files.stdout.split("\n")
    for filename in files:
        filename = filename.strip()
        if filename.endswith(".cpp") or filename.endswith(".proto"):
            with open(filename, "r", encoding="utf-8") as fp:
                content = fp.read()
                if not content.startswith(license_text):
                    print(f'{COLOR_RED_BOLD}Error{COLOR_RESET}: file lacks license preamble: {filename}:1')
                    result = False
        if filename.endswith(".hpp") or filename.endswith(".h"):
            with open(filename, "r", encoding="utf-8") as fp:
                content = fp.read()
                # Use regex to match the license text followed by any number of newlines and #pragma once
                pattern = re.escape(license_text) + r'\s*#pragma once\s*'
                if not re.match(pattern, content, re.DOTALL):
                    print(f'{COLOR_RED_BOLD}Error{COLOR_RESET}: file lacks license preamble followed by #pragma once: {filename}:1')
                    result = False

        if filename.endswith("CMakeLists.txt"):
            with open(filename, "r", encoding="utf-8") as fp:
                content = fp.read()
                if not content.startswith(license_text_cmake):
                    print(f'{COLOR_RED_BOLD}Error{COLOR_RESET}: file lacks license preamble: {filename}:1')
                    result = False

    if not result:
        sys.exit(1)

sys.exit(0)
