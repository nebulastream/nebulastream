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

def run_cmd(cmd: list) -> str:
    try:
        #ignoring errors to pass format check. Proper fix in: Todo #343
        p = subprocess.run(cmd, capture_output=True, check=True, text=True, errors='ignore')
    except subprocess.CalledProcessError as e:
        print(e)
        print("\nstderr output:")
        print(e.stderr)
        sys.exit(1)
    return p.stdout


def main():
    todo_regex      = re.compile(r".*(///|#).* TODO.*")
    todo_with_issue = re.compile(r".*(///|#).* TODO #(\d+).*")  # Note: corresponding regex also in closing issue gh action

    OWNER = "nebulastream"
    REPO = "nebulastream-public"

    if "CI" in os.environ and "BASE_REF" not in os.environ:
        print("Error: running in CI, but BASE_REF not set")
        sys.exit(1)
    elif "BASE_REF" in os.environ:
        base = os.environ["BASE_REF"]
    else:
        base = "main"

        merge_base = run_cmd(["git", "merge-base", "HEAD", "main"]).strip()
        distance_main = int(run_cmd(["git", "rev-list", "--count", f"{merge_base}..HEAD"]).strip())
        print(f"checking added TODOs for last {distance_main} commits (i.e. since forking off 'main')")
        print("Set env var BASE_REF to override base, e.g. call:")
        print(f"\n  BASE_REF=origin/main python3 {sys.argv[0]}")

    diff = run_cmd(["git", "diff", "--merge-base", base, "--",
                    # Ignore patch files in our vcpkg ports
                    ":!vcpkg/vcpkg-registry/**/*.patch"
                    ])

    file_header = re.compile(r"diff --git a/.* b/(.*)")
    line_context = re.compile(r"@@ -\d*,\d \+(\d+),\d+ @@")

    todos_without_issue_no = []
    todo_issues = {}
    line_no = 0
    fail = 0

    for line in diff.split("\n"):
        if m := file_header.match(line):
            diff_file = m[1]
        if m := line_context.match(line):
            line_no = int(m[1]) - 1

        if line.startswith("+") and todo_regex.match(line):
            if tm := todo_with_issue.match(line):
                todo_no = int(tm[2])
                if todo_no not in todo_issues:
                    todo_issues[todo_no] = [f"{diff_file}:{line_no}"]
                else:
                    todo_issues[todo_no].append(f"{diff_file}:{line_no}")
            else:
                todos_without_issue_no.append((diff_file, line_no, line[1:]))
        if not line.startswith("-"):
            line_no += 1

    if todos_without_issue_no:
        fail = 1
        print("\nError: The following TODOs do not have an issue number, like e.g. 'TODO #123'\n")
        # sort by file, line_no
        todos_without_issue_no.sort(key=lambda x: (x[0], x[1]))
        for file, line_no, line in todos_without_issue_no:
            print(f"{file}:{line_no}:{line}")
        print()

    if "CI" in os.environ and "GH_TOKEN" not in os.environ:
        print("Error: running in CI, but GH_TOKEN not set")
        sys.exit(1)
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
