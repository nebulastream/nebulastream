#!/usr/bin/env python3
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
from typing import Tuple

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


def get_added_lines_from_diff(diff: str) -> Tuple[str, int, str]:
    """returns all added lines from diff as (file_no, line_no, line)"""
    file_header = re.compile("diff --git a/.* b/(.*)")
    line_context = re.compile(r"@@ -\d+,\d+ \+(\d+),\d+ @@")
    line_no = 0

    diff_file = ""
    added_lines = []

    for line in diff.split("\n"):
        if m := file_header.match(line):
            diff_file = m[1]
        if m := line_context.match(line):
            line_no = int(m[1]) - 1

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

    if "CI" in os.environ and "NUM_COMMITS" not in os.environ:
        print("Error: running in CI, but NUM_COMMITS not set")
        sys.exit(1)
    if "CI" in os.environ and "GH_TOKEN" not in os.environ:
        print("Error: running in CI, but GH_TOKEN not set")
        sys.exit(1)

    if "NUM_COMMITS" in os.environ:
        distance_main = os.environ["NUM_COMMITS"]
    else:
        merge_base = run_cmd(["git", "merge-base", "HEAD", "main"]).strip()
        distance_main = int(run_cmd(["git", "rev-list", "--count", f"{merge_base}..HEAD"]).strip())
        print(f"checking added TODOs for last {distance_main} commits (i.e. since forking off 'main')")
        print("Set env var NUM_COMMITS to override the number of commits to be checked, e.g. call:")
        print(f"\n  NUM_COMMITS=42 python3 {sys.argv[0]}\n\n")

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
            illegal_todos.append((diff_file, line_no, line[1:]))

    if illegal_todos:
        fail = 1
        print()
        print("Error: The following TODOs are not correctly formatted!")
        print("       A correct TODO is e.g. '/// foo TODO #123 bar' or '/// TODO #123: foo bar'") # NO_TODO_CHECK
        print()
        # sort by file, line_no
        illegal_todos.sort(key=lambda x: (x[0], x[1]))
        for file, line_no, line in illegal_todos:
            print(f"{file}:{line_no}:{line}")
        print()

    if not todo_issues:
        # No added issue references, thus nothing to check
        sys.exit(fail)
    if todo_issues and "GH_TOKEN" not in os.environ:
        print("env var GH_TOKEN not set, not checking that these referenced issues are open:")
        for issue in sorted(todo_issues.keys()):
            print(f"  {'#' + str(issue) : >5}: https://github.com/{OWNER}/{REPO}/issues/{issue}")
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
        print("Error: The following TODOs do not have a corresponding open Issue:")
        for issue in sorted(todo_issues_numbers.difference(open_issues)):
            if issue in all_issues:
                state = "closed"
            else:
                state = "nonexisting"
            print(f"\n#{issue} ({state}), referenced at:")
            for loc in todo_issues[issue]:
                print(" ", loc)
        fail = 1

    sys.exit(fail)

if __name__ == "__main__":
    main()
