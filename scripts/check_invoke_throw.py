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

"""Flag a `throw` inside the argument list of a plain nautilus::invoke.

A compiled nautilus frame has no landing pads: an exception thrown by a proxy
called through plain nautilus::invoke unwinds straight through the compiled
frame and skips the traced destructors, leaking every resource they would have
released (e.g. the Emit operator's result buffer). Throwing proxies must be
called through nautilus::invokeGuarded / invokeGuardedPtr instead, which park
the exception and rethrow it at the compiled-call boundary.

This check only sees a literal `throw` lexically inside the invoke's argument
list (the inline-lambda pattern). A named proxy that throws, or a throw hidden
behind a call or a PRECONDITION/INVARIANT macro, is not detected -- reviewers
still need to check those.

Suppress a finding with /// NOLINT(no-throw-in-plain-invoke) on the invoke line
or /// NOLINTNEXTLINE(no-throw-in-plain-invoke) on the line above it.
"""

import re
import subprocess
import sys

MARKER = "NOLINT(no-throw-in-plain-invoke)"
MARKER_NEXT_LINE = "NOLINTNEXTLINE(no-throw-in-plain-invoke)"
INVOKE = re.compile(r"\binvoke\s*\(")
THROW = re.compile(r"\bthrow\b")


def find_matching_paren(text: str, open_paren: int) -> int:
    """Index of the ')' closing the '(' at open_paren, or len(text) if unbalanced."""
    depth = 0
    for i in range(open_paren, len(text)):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    return len(text)


def check_file(path: str) -> list[str]:
    with open(path, encoding="utf-8", errors="replace") as f:
        original = f.read()
    original_lines = original.splitlines()
    # Strip /// comments so a mention of nautilus::invoke(...) in a doc comment is not scanned.
    # Newlines are kept, so line numbers in the stripped text match the original.
    stripped = re.sub(r"///.*", "", original)

    findings = []
    for match in INVOKE.finditer(stripped):
        # Only plain invokes: skip member calls (.invoke / ->invoke) and std::invoke.
        # nautilus::invoke and unqualified invoke are checked; invokeGuarded* never matches \binvoke\(.
        prefix = stripped[max(0, match.start() - 8) : match.start()]
        if prefix.endswith(".") or prefix.endswith("->") or prefix.endswith("std::"):
            continue
        line_number = stripped.count("\n", 0, match.start()) + 1
        if MARKER in original_lines[line_number - 1]:
            continue
        if line_number >= 2 and MARKER_NEXT_LINE in original_lines[line_number - 2]:
            continue
        open_paren = stripped.index("(", match.start())
        span = stripped[open_paren : find_matching_paren(stripped, open_paren) + 1]
        if THROW.search(span):
            findings.append(f"{path}:{line_number}: throw inside a plain invoke() -- use nautilus::invokeGuarded")
    return findings


def main() -> int:
    files = sys.argv[1:]
    if not files:
        files = subprocess.run(
            ["git", "ls-files", "--", "*.cpp", "*.hpp"], capture_output=True, text=True, check=True
        ).stdout.splitlines()

    findings = [finding for path in files for finding in check_file(path)]
    for finding in findings:
        print(finding)
    if findings:
        print(
            "check_invoke_throw.py: a proxy that throws must be called through nautilus::invokeGuarded /"
            " invokeGuardedPtr; a plain nautilus::invoke lets the exception unwind through the compiled frame,"
            " skipping the traced destructors and leaking their resources."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
