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
Stop hook for Claude Code: requests a review of the comments that a larger change added.
It is not active by default.
docs/standards/comment_guidelines.md describes how to enable it.

When the agent wants to stop,
the hook collects the comment lines that the uncommitted changes add to the files that the agent edited in this session.
If enough of them were not sent to a review before, the hook blocks the stop once.
For a small number (NES_COMMENT_REVIEW_MIN, default 5), its message tells the agent to reread its own comments.
For a large number (NES_COMMENT_REVIEW_SUBAGENT_MIN, default 50),
it tells the agent to start a subagent that reviews the comments against the comment guidelines.
Both messages include the findings of the repository's comment check.

The hook does nothing in a checkout that has no guidelines or no check script.
It never blocks because of an error of its own.
"""
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path

MIN_NEW_COMMENTS = int(os.environ.get("NES_COMMENT_REVIEW_MIN", "5"))
# A subagent costs one to two minutes, so only a change with many new comments gets one.
# An author tends to accept its own comments, which is why the larger change is not left to a reread.
SUBAGENT_MIN_NEW_COMMENTS = int(os.environ.get("NES_COMMENT_REVIEW_SUBAGENT_MIN", "50"))
# The review compares comments with a guide, which does not need the largest model.
REVIEW_MODEL = os.environ.get("NES_COMMENT_REVIEW_MODEL", "sonnet")
SHORT_RULES = "AGENTS.md"
# How long after a block the edits to comments count as the result of the requested review.
# A subagent can run in the background, so the agent applies its findings in a later turn than the one that was blocked.
# 30 minutes is picked arbitrarily, well above the one to two minutes that a review takes.
REVIEW_WINDOW_SECONDS = 30 * 60
GUIDE = "docs/standards/comment_guidelines.md"
CHECKER = "scripts/check_comments.py"
# The repository ignores .claude, so the state files do not show up as untracked changes.
# Every session has a file of its own, because several sessions can run in one checkout,
# and a stop of one session must not close the review that another one is waiting for.
STATE_DIR = ".claude/comment-review"
STATE_MAX_AGE_SECONDS = 14 * 24 * 60 * 60
# The brief lists at most this many comment lines, so that a very large change does not produce a very large brief.
MAX_LISTED_COMMENTS = 120
EDIT_TOOLS = {"Edit", "Write", "MultiEdit", "NotebookEdit"}
# A new file starts with the license preamble, which is not a comment of the author.
PREAMBLE_PARTS = (
    "Apache License",
    "may not use this file",
    "obtain a copy of the License",
    "apache.org/licenses",
    "Unless required by applicable law",
    "distributed under the License",
    "WITHOUT WARRANTIES",
    "See the License for",
    "limitations under the License",
)


def git(root: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True, errors="replace").stdout


def load_checker(path: Path):
    # Loading the check script must not leave a __pycache__ directory in the checkout.
    sys.dont_write_bytecode = True
    spec = importlib.util.spec_from_file_location("check_comments", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def files_edited_in_session(transcript: str, root: Path):
    """
    Returns the files that the agent edited in this session, relative to the repository root.
    Returns None when the transcript cannot be read, so that the caller does not filter by file.
    An edit that a shell command made is not found.
    """
    edited = set()
    try:
        with open(transcript, errors="replace") as lines:
            for line in lines:
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                content = (entry.get("message") or {}).get("content")
                if entry.get("type") != "assistant" or not isinstance(content, list):
                    continue
                for item in content:
                    if not isinstance(item, dict) or item.get("type") != "tool_use" or item.get("name") not in EDIT_TOOLS:
                        continue
                    file_path = (item.get("input") or {}).get("file_path")
                    if not file_path:
                        continue
                    try:
                        edited.add(str(Path(file_path).resolve().relative_to(root.resolve())))
                    except ValueError:
                        pass
    except OSError:
        return None
    return edited


def added_comments(root: Path, checker) -> list:
    """returns (file, line number, comment text) for every comment line that the uncommitted changes add"""
    candidates = []
    for path, line_no, line in checker.added_lines(git(root, "diff", "-U0", "HEAD")):
        candidates.append((path, line_no, line))
    # An untracked file is not part of the diff, so all of its lines count as added.
    for path in git(root, "ls-files", "--others", "--exclude-standard").splitlines():
        if not any(path.endswith(suffix) for suffix in checker.COMMENT_BY_SUFFIX):
            continue
        try:
            lines = (root / path).read_text(errors="replace").splitlines()
        except OSError:
            continue
        candidates.extend((path, line_no, line) for line_no, line in enumerate(lines, start=1))

    comments = []
    for path, line_no, line in candidates:
        text = checker.comment_text(path, line)
        if text is None or not text.strip():
            continue
        if any(part in text for part in PREAMBLE_PARTS):
            continue
        comments.append((path, line_no, text.strip()))
    return comments


def key(path: str, text: str) -> str:
    # The line number is not part of the key, so that a reviewed comment does not count as new when lines above it change.
    return hashlib.sha1(f"{path}\0{text}".encode()).hexdigest()


def state_file(root: Path, session_id: str) -> Path:
    """returns the state file of the session and removes the files of sessions that ended long ago"""
    directory = root / STATE_DIR
    directory.mkdir(parents=True, exist_ok=True)
    for old in directory.glob("*.json"):
        try:
            if time.time() - old.stat().st_mtime > STATE_MAX_AGE_SECONDS:
                old.unlink()
        except OSError:
            pass
    name = "".join(character for character in session_id if character.isalnum() or character == "-") or "unknown"
    return directory / f"{name}.json"


def log(root: Path, session_id: str, decision: str, comments: int, new: int) -> None:
    """appends one line per stop, so that an unexpected block or pass can be traced afterwards"""
    try:
        with open(root / STATE_DIR / "log", "a") as file:
            stamp = time.strftime("%Y-%m-%d %H:%M:%S")
            file.write(f"{stamp} session={session_id[:8]} {decision} comments={comments} new={new}\n")
    except OSError:
        pass


def read_state(state: Path) -> tuple:
    """returns the keys of the reviewed comments and the time until which a requested review is open"""
    try:
        stored = json.loads(state.read_text())
        return set(stored["seen"]), float(stored["review_open_until"])
    except (OSError, ValueError, KeyError, TypeError):
        return set(), 0.0


def write_state(state: Path, keys: set, review_open_until: float) -> None:
    state.parent.mkdir(parents=True, exist_ok=True)
    state.write_text(json.dumps({"seen": sorted(keys), "review_open_until": review_open_until}))


def message(root: Path, new: list, checker) -> str:
    per_file = {}
    for path, _, _ in new:
        per_file[path] = per_file.get(path, 0) + 1
    files = "\n".join(f"  {path}: {count}" for path, count in sorted(per_file.items()))

    found = []
    for path, line_no, text in new:
        for kind, rules in (("error", checker.ERRORS), ("warning", checker.WARNINGS)):
            for finding in checker.findings(rules, text):
                found.append(f"  {kind}: {finding} in {path}:{line_no}: {text}")
    mechanical = "\n".join(found) if found else "  none"

    listed = [f"    {path}:{line_no}: {text}" for path, line_no, text in new[:MAX_LISTED_COMMENTS]]
    if len(new) > MAX_LISTED_COMMENTS:
        listed.append(f"    and {len(new) - MAX_LISTED_COMMENTS} more in the same files")
    listing = "\n".join(listed)
    paths = " ".join(sorted(per_file))

    if len(new) < SUBAGENT_MIN_NEW_COMMENTS:
        return f"""Comment check before you finish.
Your uncommitted changes add {len(new)} comment lines that have not been checked:
{listing}

Reread each of them against the Comments section of {SHORT_RULES}.
Delete a comment that restates the code or describes the change, and rewrite one that breaks another rule.
Then tell the user in one sentence what you changed, or that nothing needed a change.

Findings of {CHECKER} on these lines:
{mechanical}"""

    return f"""Comment review before you finish.
Your uncommitted changes add {len(new)} comment lines that have not been reviewed:
{files}

Start one subagent on the {REVIEW_MODEL} model for the review, so that it reads the comments without your context.
Give it this brief and nothing about your own reasoning:

  Review the comments that the uncommitted changes in {root} add.
  Read {GUIDE} first and apply it.
  These are the added comment lines:
{listing}
  Read the code around them with `git diff HEAD -- {paths}`.
  A file that the diff does not show is new as a whole, so read that file.
  Only read: do not run code, and do not edit any file.
  Look for what a script cannot find: a comment that restates the code, describes the change instead of the code,
  repeats a fact that another comment already states, states a limit that nothing enforces,
  or refers to a private helper by name.
  Also report where a comment is missing: a public type without a doc comment,
  or a decision that a reader cannot recover from the code.
  Return one line per finding: file:line, the problem, the chapter of the guide, and the text that you propose.
  Return "no findings" if the comments are fine.

Then apply the findings that are correct, and tell the user in one or two sentences what you changed.
If you reject a finding, say which one and why.

Findings of {CHECKER} on these lines:
{mechanical}"""


def main() -> None:
    data = json.load(sys.stdin)
    start = os.environ.get("CLAUDE_PROJECT_DIR") or data.get("cwd") or "."
    top = git(Path(start), "rev-parse", "--show-toplevel").strip()
    if not top:
        return
    root = Path(top)
    if not (root / GUIDE).is_file() or not (root / CHECKER).is_file():
        return

    checker = load_checker(root / CHECKER)
    comments = added_comments(root, checker)
    edited = files_edited_in_session(data.get("transcript_path") or "", root)
    if edited is not None:
        comments = [comment for comment in comments if comment[0] in edited]

    session_id = str(data.get("session_id") or "")
    state = state_file(root, session_id)
    seen, review_open_until = read_state(state)
    current = {key(path, text) for path, _, text in comments}
    new = [comment for comment in comments if key(comment[0], comment[2]) not in seen]
    now = time.time()

    # While a requested review is open, the agent rewrites comments because of its findings.
    # Those edits must not start a second review, so they are recorded as reviewed.
    # The same applies when stop_hook_active is set, so that the hook does not block the stop that follows a block.
    # The first stop at which the comments differ closes the review, on the assumption that the difference is the applied findings.
    # NOTE: an unrelated edit to a comment closes the review as well, and the findings can then start a second review.
    if data.get("stop_hook_active") or now < review_open_until:
        write_state(state, current, 0.0 if current != seen else review_open_until)
        log(root, session_id, "pass-review-open", len(comments), len(new))
        return

    if len(new) < MIN_NEW_COMMENTS:
        log(root, session_id, "pass", len(comments), len(new))
        return

    write_state(state, current, now + REVIEW_WINDOW_SECONDS)
    level = "block-subagent" if len(new) >= SUBAGENT_MIN_NEW_COMMENTS else "block-reread"
    log(root, session_id, level, len(comments), len(new))
    print(json.dumps({"decision": "block", "reason": message(root, new, checker)}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # pylint: disable=broad-except
        print(f"stop-comment-review: skipped after an error: {error}", file=sys.stderr)
    sys.exit(0)
