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
  Checks the wording of the comments that were added since forking off main.

  An error fails the check.
  A warning does not, because its pattern also matches correct comments: the author or the coding agent has to judge it.
  A line that ends with NO_COMMENT_CHECK is skipped.

  The rules about what a comment contains are described in docs/standards/comment_guidelines.md.
  The rules about wording (literal verbs, no dashes as punctuation) are in AGENTS.md.
"""
import os
import re
import subprocess
import sys
from typing import Iterator, List, Optional, Tuple

COLOR_YELLOW_BOLD = "\033[1;33m"
COLOR_RED_BOLD = "\033[1;31m"
COLOR_RESET = "\033[0m"

# The text of a comment that starts with two or more slashes.
# A colon or slash in front excludes the slashes of a URL.
SLASH_COMMENT = re.compile(r"(?:^|[^:/])//+!?(.*)$")
# The text of a comment that fills the line.
# A hash after code is not matched, because it is often part of a string.
HASH_COMMENT = re.compile(r"^\s*#(?!!)(.*)$")
# The text of an SQL comment that fills the line.
# A line that starts with three or more dashes is not matched, because it is a separator.
DASH_COMMENT = re.compile(r"^\s*--(?!-)(.*)$")

COMMENT_BY_SUFFIX = {
    ".cpp": SLASH_COMMENT,
    ".hpp": SLASH_COMMENT,
    ".rs": SLASH_COMMENT,
    ".py": HASH_COMMENT,
    ".sh": HASH_COMMENT,
    ".bats": HASH_COMMENT,
    ".cmake": HASH_COMMENT,
    "CMakeLists.txt": HASH_COMMENT,
    ".test": HASH_COMMENT,
    ".sql": DASH_COMMENT,
}

# Words that describe code as if it were a person or a moving object, and filler phrases.
FIGURATIVE_WORDS = [
    "drives?", "silently", "loudly", "drains?", "slurps?", "trips", "lands",
    "walks", "lives", "sits", "sitting", "rewires?", "rewired", "carries", "reaps?", "reaping", "answers", "asks",
    # `names` is matched only in front of a determiner, where it is usually the verb.
    # The plural noun is mostly followed by a verb or a preposition, as in "the names of the fields".
    "names (?:the|a|an|its|their|no|every|each|one|two|which|what|this|that|these|those)",
    "rather", "plus", "note that", "seamlessly", "simply", "of course",
]

ERRORS = [
    ("dash used as punctuation", re.compile("[–—]")),
    ("label for a step", re.compile(r"^\s*step \d+\b", re.IGNORECASE)),
]

WARNINGS = [
    # A digit next to the hyphen is not matched, because that is usually a subtraction.
    ("spaced hyphen used as punctuation", re.compile(r"[A-Za-z)`.,]+ - [A-Za-z(`]+")),
    (
        "describes the change instead of the code",
        re.compile(
            r"\b(previously|no longer|as requested|used to|instead of the old"
            r"|now (uses|returns|calls|supports|handles|checks))\b",
            re.IGNORECASE,
        ),
    ),
    (
        "more than one sentence on the line",
        re.compile(r"[\w)`]+(?<!e\.g)(?<!i\.e)(?<!c\.f)(?<!etc)(?<!vs)\. [A-Z]\w*"),
    ),
    ("figurative or filler wording", re.compile(r"\b(" + "|".join(FIGURATIVE_WORDS) + r")\b", re.IGNORECASE)),
    ("marker for a gotcha is written NOTE:", re.compile(r"^\s*Note:")),
]


def run_cmd(cmd: list) -> str:
    """runs cmd, returns stdout or exits"""
    try:
        p = subprocess.run(cmd, capture_output=True, check=True, text=True, errors="replace")
    except subprocess.CalledProcessError as e:
        print(e)
        print("\nstderr output:")
        print(e.stderr)
        sys.exit(1)
    return p.stdout


def added_lines(diff: str) -> Iterator[Tuple[str, int, str]]:
    """yields every added line of a diff as (file, line number, text)"""
    new_file = re.compile(r"^\+\+\+ b/(.*)$")
    hunk = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")
    path: Optional[str] = None
    line_no = 0
    # A header line and an added line can both start with "+++", so the position in the diff decides which one it is.
    in_header = True

    for line in diff.split("\n"):
        if line.startswith("diff --git "):
            in_header = True
            path = None
        elif m := hunk.match(line):
            in_header = False
            line_no = int(m[1])
        elif in_header:
            if m := new_file.match(line):
                path = m[1]
        elif line.startswith("+"):
            if path is not None:
                yield path, line_no, line[1:]
            line_no += 1
        elif not line.startswith("-"):
            line_no += 1


def comment_text(path: str, line: str) -> Optional[str]:
    """returns the text of the comment on the line, or None if the line has no comment that is checked"""
    if line.rstrip().endswith("NO_COMMENT_CHECK"):
        return None
    # A line with a linter directive is skipped as a whole, also when a reason follows the directive.
    if "NOLINT" in line:
        return None
    for suffix, pattern in COMMENT_BY_SUFFIX.items():
        if path.endswith(suffix):
            m = pattern.search(line)
            return m[1] if m else None
    return None


def findings(rules: list, text: str) -> List[str]:
    """returns one message for every rule that the comment text matches"""
    messages = []
    for description, pattern in rules:
        if m := pattern.search(text):
            messages.append(f"{description} ('{m[0].strip()}')")
    return messages


def main():
    if "DISTANCE_MERGE_BASE" in os.environ:
        distance = os.environ["DISTANCE_MERGE_BASE"]
    else:
        distance = run_cmd(["git", "rev-list", "--count", "origin/main..HEAD"]).strip()

    diff = run_cmd(["git", "diff", "-U0", f"HEAD~{distance}", "--",
                    # Ignore patch files in our vcpkg & nix ports, and the vendored corrosion tree
                    ":!vcpkg/vcpkg-registry/**/*.patch",
                    ":!.nix/**/patches/*.patch",
                    ":!cmake/corrosion"
                    ])

    errors = 0
    warnings = 0
    for path, line_no, line in added_lines(diff):
        text = comment_text(path, line)
        if text is None:
            continue
        for message in findings(ERRORS, text):
            errors += 1
            print(f"{COLOR_RED_BOLD}Error{COLOR_RESET}: comment: {message} in {path}:{line_no}:{text}")
        for message in findings(WARNINGS, text):
            warnings += 1
            print(f"{COLOR_YELLOW_BOLD}Warning{COLOR_RESET}: comment: {message} in {path}:{line_no}:{text}")

    if errors or warnings:
        print(f"check_comments.py: {errors} error(s), {warnings} warning(s). See docs/standards/comment_guidelines.md")

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
