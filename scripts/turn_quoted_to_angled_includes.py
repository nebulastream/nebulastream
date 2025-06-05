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
import sys
from check_todos import get_added_lines_from_diff
from pathlib import Path
from collections import defaultdict


def main():
    if len(sys.argv) > 1:
        print(
            'Usage: pipe git diff into this to turn added #include "..." to #include <...>, e.g:'
        )
        print()
        print("    git diff HEAD^ | python3", sys.argv[0])
        print()
        sys.exit()

    diff = sys.stdin.read()

    added_lines = get_added_lines_from_diff(diff)

    foo = defaultdict(list)

    for file, line_no, line in added_lines:
        if '#include "' in line:
            foo[file].append((line_no, line))

    quoted_include_regex = re.compile('(.*)#include "([^"]*)"(.*)')

    for file, lines_with_quoted_include in foo.items():
        f = Path(file)
        lines = f.read_text().split("\n")
        for line_no, line in lines_with_quoted_include:
            assert lines[line_no - 1] == line
            line = re.sub(quoted_include_regex, r"\1#include <\2>\3", line)
            lines[line_no - 1] = line
        f.write_text("\n".join(lines))


if __name__ == "__main__":
    main()
