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

if __name__ == "__main__":
    exclude_dirs = set(
        ['cmake-build-unikernel-release', 'cmake-build-unikernel-debug', 'cmake-build-unikernel-release-original-deps',
         'cmake-build-release', 'cmake-build-debug-docker', 'cmake-build-release-docker', 'nes-unikernel',
         'build', 'yaml', 'jitify', 'magicenum', '.idea', 'gen'])
    exclude_files = ['backward.hpp',
                     'apex_memmove.hpp',
                     'rte_memory.h',
                     'JNI.hpp',
                     'JNI.cpp',
                     "base64.h",
                     "base64.cpp",
                     'apex_memmove.cpp',
                     'HyperLogLog.hpp',
                     'digestible.h']
    exclude = set(['cmake-build-debug', 'cmake-build-release', 'cmake-build-debug-docker', 'cmake-build-release-docker',
                   'build', 'yaml', 'jitify', 'magicenum', 'Backward', 'gen'])
    result = True
    print(str(sys.argv))
    for subdir, dirs, files in os.walk(sys.argv[1]):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        for file in files:
            if file in exclude_files:
                continue
            filename = os.path.join(subdir, file)
            if filename.endswith(".cpp") or filename.endswith(".hpp") or filename.endswith(".h") or filename.endswith(
                    ".proto"):
                with open(filename, "r", encoding="utf-8") as fp:
                    content = fp.read()
                    if not content.startswith(license_text_cpp):
                        print("File", filename, " lacks the license preamble")
                        result = False

    if not result:
        sys.exit(1)

sys.exit(0)
