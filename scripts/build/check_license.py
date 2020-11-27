#!/usr/bin/env python
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
    exclude = set(['cmake-build-debug', 'cmake-build-release', 'build'])
    for subdir, dirs, files in os.walk(sys.argv[1]):
        dirs[:] = [d for d in dirs if d not in exclude]
        for file in files:
            filename = os.path.join(subdir, file)
            if filename.endswith(".cpp") or filename.endswith(".hpp") or filename.endswith(".proto"):
                with open(filename, "r") as fp:
                    content = fp.read()
                    if not content.startswith(license_text_cpp):
                        print("File", filename, " lacks the license preamble")
                        sys.exit(1)

sys.exit(0)