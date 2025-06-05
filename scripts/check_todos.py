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

"""
  Checks that all TODOs that were introduced since forking off $BASE_REF or main
  - have a corresponding issue nr
  - point to open issues on github
"""
import json
import os
import re
import subprocess
import sys
import urllib.request
from collections import defaultdict
from typing import Tuple, List

COLOR_RED_BOLD = "\033[1;31m"
COLOR_BG_RED_FONT_WHITE = "\033[1;41m\033[1;37m"
COLOR_RESET = "\033[0m"

def run_cmd(cmd: list) -> str:
    """runs cmd, returns stdout or crashes"""
    try:
        # Handle decode errors by replacing illegal chars with � (see #343)
        p = subprocess.run(cmd, capture_output=True, check=True, text=True, errors='replace')
    except subprocess.CalledProcessError as e:
        print(e)
        print("\nstderr output:")
        print(e.stderr)
        sys.exit(1)
    return p.stdout


def get_added_lines_from_diff(diff: str) -> List[Tuple[str, int, str]]:
    """returns all added lines from diff as (file_no, line_no, line)"""
    file_header = re.compile("diff --git a/.* b/(.*)")
    line_context = re.compile(r"@@ -\d+,\d+ \+(\d+),\d+ @@")
    line_no = 0

    diff_file = ""
    added_lines = []

    skip_until_line_ctx = False

    for line in diff.split("\n"):
        if m := file_header.match(line):
            diff_file = m[1]
            skip_until_line_ctx = True
        if m := line_context.match(line):
            line_no = int(m[1]) - 1
            skip_until_line_ctx = False
        if skip_until_line_ctx:
            continue

        if line.startswith("+"):
            added_lines.append((diff_file, line_no, line[1:]))
        if not line.startswith("-"):
            line_no += 1

    return added_lines


def line_contains_todo(filename: str, line: str) -> bool:
    """
    Heuristic to find TODOs.

    To be sensitive (i.e. catch many TODOs), we ignore case while searching,
    since TODO might not always be written in all caps, causing false negatives.

    To be specific (i.e. have little false positives), we shall not match e.g.
    `toDouble`. For this, we search for TODOs in single line comments,
    when checking code files.

    Additionally, `NO_TODO_CHECK` at the end of the line can be used to suppress the check.
    """
    if line.endswith("NO_TODO_CHECK"):
        return False

    if filename.endswith(".md"):
        return re.match(".*todo[^a-zA-Z].*", line, re.IGNORECASE)
    else:
        return re.match(".*(///|#).*todo.*", line, re.IGNORECASE)  # NO_TODO_CHECK


def main():
    """
    Searches for new TODOs (added since forking of $BASE_REF or main).
    Checks that new TODOs are well-formed (format, have issue number).
    Checks that listed issues exist and are open.
    """
    # This regex describes conforming how a well-formed TODO should look like.  NO_TODO_CHECK
    # Note: corresponding regex also in closing issue gh action
    todo_with_issue = re.compile(".*(///|#).*\\sTODO #(\\d+).*")  # NO_TODO_CHECK

    OWNER = "nebulastream"
    REPO = "nebulastream-public"

    if "DISTANCE_MERGE_BASE" not in os.environ:
        print(f"{COLOR_BG_RED_FONT_WHITE}Fatal{COLOR_RESET}: {os.path.basename(__file__)}: DISTANCE_MERGE_BASE not set in env")
        sys.exit(1)

    distance_main = os.environ["DISTANCE_MERGE_BASE"]

    diff = run_cmd(["git", "diff", f"HEAD~{distance_main}", "--",
                    # Ignore patch files in our vcpkg ports
                    ":!vcpkg/vcpkg-registry/**/*.patch"
                    ])

    added_lines = get_added_lines_from_diff(diff)

    illegal_todos = []
    todo_issues = defaultdict(list)
    fail = 0

    # Checks if line contains TODO. If so, checks if TODO adheres to format.  NO_TODO_CHECK
    for diff_file, line_no, line in added_lines:
        if not line_contains_todo(diff_file, line):
            continue

        if tm := todo_with_issue.match(line):
            todo_no = int(tm[2])
            todo_issues[todo_no].append(f"{diff_file}:{line_no}")
        else:
            illegal_todos.append((diff_file, line_no, line))

    if illegal_todos:
        fail = 1
        # sort by file, line_no
        illegal_todos.sort(key=lambda x: (x[0], x[1]))
        for file, line_no, line in illegal_todos:
            print(f"{COLOR_RED_BOLD}Error{COLOR_RESET}: TODO with incorrect format {file}:{line_no}:{line}")
        print("Hint: A correct TODO is e.g. '/// foo TODO #123 bar' or '/// TODO #123: foo bar'") # NO_TODO_CHECK

    if not todo_issues:
        # No added issue references, thus nothing to check
        sys.exit(fail)
    if todo_issues and "GH_TOKEN" not in os.environ:
        print(f"env var GH_TOKEN not set, not checking if {len(todo_issues.keys())} added TODO(s) are open")
        sys.exit(fail)

    all_issues = set()
    open_issues = set()
    todo_issues_numbers = set(todo_issues.keys())

    token = os.environ["GH_TOKEN"]

    # Iterates the paginated issues API.
    # Fetches up to 1000 pages of 100 issues, but `breaks` as soon as all
    # (relevant) issues are fetched.
    for i in range(1000):
        url = f'https://api.github.com/repos/{OWNER}/{REPO}/issues?state=all&per_page=100&page={i}'
        req = urllib.request.Request(url, headers={
            "Authorization": "Bearer " + token,
            "User-Agent": "Python urllib",
        })
        with urllib.request.urlopen(req) as response:
            data = response.read()
        issues_full = json.loads(data)

        all_issues  |= {i["number"] for i in issues_full}
        open_issues |= {i["number"] for i in issues_full if i["state"] == "open"}

        if len(issues_full) < 100 or todo_issues_numbers.issubset(open_issues):
            # end of pagination or we have all relevant issue IDs
            break

    if not todo_issues_numbers.issubset(open_issues):
        for issue in sorted(todo_issues_numbers.difference(open_issues)):
            if issue in all_issues:
                state = "closed"
            else:
                state = "nonexisting"
            for loc in todo_issues[issue]:
                print(f"{COLOR_RED_BOLD}Error{COLOR_RESET}: TODO referencing {state} Issue at {loc}")
        fail = 1

    sys.exit(fail)

if __name__ == "__main__":
    main()
