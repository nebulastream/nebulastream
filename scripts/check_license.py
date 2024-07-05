#!/usr/bin/env python

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import re

license_text_cpp = """/*
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

# Define a function to check if a path matches any of the patterns
def is_excluded(path, patterns):
    return any(re.search(pattern, path) for pattern in patterns)


if __name__ == "__main__":
    print(str(sys.argv))
    if len(sys.argv) != 3:
        print("Usage: check_license.py <path> <no-license-check file>")
        sys.exit(1)

    root_dir = sys.argv[1]
    no_license_file = sys.argv[2]

    # Check if the path exists
    if not os.path.exists(no_license_file):
        print("Path {} does not exist", no_license_file)
        sys.exit(1)

    # Read the .no-license-check file and store its contents in a list
    with open(no_license_file, 'r') as f:
        exclude_patterns = [line.strip() for line in f if len(line.strip()) > 0 and not line.startswith("#")]

    result = True
    # Check if C++ have the required license preamble
    for subdir, dirs, files in os.walk(root_dir):
        dirs[:] = [d for d in dirs if not is_excluded(os.path.join(subdir, d), exclude_patterns)]
        for file in files:
            if is_excluded(os.path.join(subdir, file), exclude_patterns):
                continue
            filename = os.path.join(subdir, file)
            if filename.endswith(".cpp") or filename.endswith(".hpp") or filename.endswith(".h") or filename.endswith(".proto"):
                with open(filename, "r", encoding="utf-8") as fp:
                    content = fp.read()
                    if not content.startswith(license_text_cpp):
                        print("File", filename, " lacks the cpp license preamble")
                        result = False

    # Check if all CMakeLists.txt files have the required license preamble
    for subdir, dirs, files in os.walk(root_dir):
        dirs[:] = [d for d in dirs if not is_excluded(os.path.join(subdir, d), exclude_patterns)]
        for file in files:
            if file == "CMakeLists.txt":
                filename = os.path.join(subdir, file)
                with open(filename, "r", encoding="utf-8") as fp:
                    content = fp.read()
                    if not content.startswith(license_text_cmake):
                        print("File", filename, " lacks the CMakeLists license preamble")
                        result = False

    if not result:
        sys.exit(1)

sys.exit(0)
