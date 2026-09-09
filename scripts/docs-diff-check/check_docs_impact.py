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
Runs weekly, checking every PR merged into the default branch in the last 7
days for whether it has made docs/ stale. A PR touching docs/ itself is not
skipped - that doesn't guarantee the docs update was correct or complete for
the change made.

For each PR:
  1. An LLM reads the diff and produces a short semantic summary of what
     changed (or decides it's purely internal, in which case processing
     stops here).
  2. That summary is embedded and used to query a RAG index built fresh this
     run from every docs/ section (fresh because jobs land on a random
     self-hosted worker each time, so there's nothing to cache between runs).
  3. The retrieved doc excerpts, plus the diff, go back to the LLM, which
     decides whether the diff makes any specific statement in those excerpts
     incorrect or outdated, and if so, explains exactly which one and why.

Flagged PRs are reported to Zulip with that explanation.
"""
import base64
import json
import math
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

REPO = os.environ["GITHUB_REPOSITORY"]
DOCS_PREFIX = "docs/"
DOCS_ROOT = os.environ.get("DOCS_ROOT", "docs")

# =============================================================================
# What this tool deliberately does not look at.
#
# Everything excluded from consideration - by file type, by doc path, or by
# PR size - lives here, with the reasoning for each. Extend these tuples to
# skip more without having to touch prompt logic or control flow elsewhere.
# =============================================================================

# File suffixes stripped entirely out of a PR's diff before any LLM sees it -
# not evaluated at all, not even to note their existence.
SKIPPED_DIFF_FILE_SUFFIXES = (
    ".test",  # SLT-style test specs: test coverage, not product surface.
)

# Doc files/path prefixes never entered into the RAG index - can never be
# retrieved or flagged against, regardless of what a diff touches. A prompt
# instruction alone wasn't reliable here: the model correctly identified
# docs/git/checklist_pr.md as a checklist and flagged it against anyway.
EXCLUDED_DOC_PREFIXES = (
    "docs/organizational/",  # Meeting notes, bug-triage workflow: process, not product docs.
)
EXCLUDED_DOC_FILES = (
    "docs/git/checklist_pr.md",  # PR process checklist: not technical documentation.
)

# A PR touching more than this many files is assumed to be a mass
# reformat/rename/vendoring sweep rather than a deliberate semantic change -
# not worth an LLM call, and it's also close enough to GitHub's own 300-file
# diff-endpoint hard limit that trying to check it is unreliable anyway.
MAX_PR_FILES = 150

MAX_SECTION_CHARS = 3000
MAX_TOTAL_SECTION_CHARS = 16000
MAX_SELECTED_SECTIONS = 8

LLM_API_BASE = os.environ["LLM_API_BASE"].rstrip("/")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_MODEL = os.environ["LLM_MODEL"]
EMBED_MODEL = os.environ["EMBED_MODEL"]
OLLAMA_NATIVE_BASE = os.environ.get("OLLAMA_NATIVE_BASE", LLM_API_BASE.rsplit("/v1", 1)[0])

ZULIP_SITE = os.environ["ZULIP_SITE"].rstrip("/")
ZULIP_BOT_EMAIL = os.environ["ZULIP_BOT_EMAIL"]
ZULIP_BOT_API_KEY = os.environ["ZULIP_BOT_API_KEY"]
ZULIP_STREAM = os.environ["ZULIP_STREAM"]
ZULIP_TOPIC = os.environ.get("ZULIP_TOPIC", "Docs drift warnings")
ZULIP_NOTIFY_USERS = [u for u in os.environ.get("ZULIP_NOTIFY_USERS", "").split(",") if u]

MAX_DIFF_CHARS = 12000
MAX_BODY_CHARS = 2000
EMBED_BATCH_SIZE = 32
MAX_FILE_CONTENT_CHARS = 4000
MAX_TOTAL_FILE_CONTENT_CHARS = 12000


def describe_exception(exc):
    """Renders an exception with whatever diagnostic detail is available -
    an HTTPError's response body (which usually carries the actual cause:
    an Ollama error message, a Zulip API error, a GitHub API error) rather
    than just its generic status line."""
    if isinstance(exc, urllib.error.HTTPError):
        try:
            body = exc.read().decode(errors="replace")[:2000]
        except Exception:
            body = "<could not read response body>"
        return f"HTTP {exc.code} {exc.reason}: {body}"
    if isinstance(exc, subprocess.CalledProcessError):
        return f"{exc}: {exc.stderr.strip() if exc.stderr else '<no stderr>'}"
    return f"{type(exc).__name__}: {exc}"


def gh(*args):
    result = subprocess.run(
        ["gh", *args, "--repo", REPO],
        capture_output=True, text=True, check=True,
    )
    return result.stdout


def list_prs_merged_last_week():
    since = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    out = gh(
        "pr", "list",
        "--search", f"is:pr is:merged sort:created-asc merged:>={since}",
        "--json", "number,title,body,url,author,mergedAt",
        "--limit", "200",
    )
    return json.loads(out)


def strip_skipped_files_from_diff(diff):
    """Drops every file's hunks from a unified diff whose path ends with one
    of SKIPPED_DIFF_FILE_SUFFIXES - see that tuple for what and why."""
    out = []
    skip = False
    for line in diff.splitlines(keepends=True):
        if line.startswith("diff --git "):
            parts = line.split()
            path = parts[2][2:] if len(parts) > 2 else ""
            skip = path.endswith(SKIPPED_DIFF_FILE_SUFFIXES)
        if not skip:
            out.append(line)
    return "".join(out)


def _is_comment_only_diff_line(line):
    """True for a +/- diff line that is nothing but an HTML comment, e.g.
    '+<!-- quick-start-run-worker:start -->'. Doesn't match the '+++ b/...'
    /'--- a/...' file-header lines since those don't start with '<!--'."""
    if not line or line[0] not in "+-":
        return False
    content = line[1:].strip()
    return content.startswith("<!--") and content.endswith("-->")


def strip_html_comments_from_md_diffs(diff):
    """Drops +/- lines in .md files that are pure HTML comments. These are
    typically anchor markers added so tooling can extract a code block (see
    docs/website/scripts/test-quick-start.sh), not new documented content -
    but a diff showing only such additions around an unchanged code block
    was mistaken for new commands being introduced in testing."""
    out = []
    in_md = False
    for line in diff.splitlines(keepends=True):
        if line.startswith("diff --git "):
            parts = line.split()
            path = parts[2][2:] if len(parts) > 2 else ""
            in_md = path.endswith(".md")
        if in_md and _is_comment_only_diff_line(line.rstrip("\n")):
            continue
        out.append(line)
    return "".join(out)


def get_pr_diff(number):
    diff = gh("pr", "diff", str(number))
    diff = strip_skipped_files_from_diff(diff)
    diff = strip_html_comments_from_md_diffs(diff)
    if len(diff) > MAX_DIFF_CHARS:
        diff = diff[:MAX_DIFF_CHARS] + "\n... (diff truncated)"
    return diff


def get_current_file_contents(files):
    """Reads the CURRENT (post-merge) content of each changed file straight
    off the checkout already on disk - no extra API calls, since the whole
    repo (not just docs/) is checked out and this script runs from the repo
    root. This lets the model reason about what a file actually says now,
    rather than inferring it from diff +/- syntax, which is exactly what
    misread a documentation file's diff as introducing new commands when it
    only added HTML comment anchors around content that was already there.
    Skips files that no longer exist (deleted in this PR) or aren't text."""
    parts = []
    total = 0
    for path in files:
        if path.endswith(SKIPPED_DIFF_FILE_SUFFIXES):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                content = f.read()
        except (FileNotFoundError, IsADirectoryError, UnicodeDecodeError, OSError):
            continue
        if len(content) > MAX_FILE_CONTENT_CHARS:
            content = content[:MAX_FILE_CONTENT_CHARS] + "\n... (file truncated)"
        if total + len(content) > MAX_TOTAL_FILE_CONTENT_CHARS:
            continue
        parts.append(f"### Current content of {path}\n{content}")
        total += len(content)
    return "\n\n".join(parts)


def pr_context(pr):
    """PR title + description, formatted for a prompt. The description often
    states WHY a change was made, which the diff alone doesn't convey (e.g.
    'field was unused, this makes the source actually count tuples' explains
    intent a diff of a few added lines wouldn't on its own)."""
    body = (pr.get("body") or "").strip()
    if len(body) > MAX_BODY_CHARS:
        body = body[:MAX_BODY_CHARS] + "\n... (description truncated)"
    if not body:
        return f"PR title: {pr['title']}"
    return f"PR title: {pr['title']}\n\nPR description:\n{body}"


def get_pr_files(number):
    """List of files this PR changed, via --name-only (the diff/patch
    endpoint) rather than `pr list --json files` (the GraphQL
    files(first:100) field, capped at 100 regardless of --limit), so it
    isn't subject to that truncation for large PRs. Returns None if the PR
    is too large for GitHub's diff endpoint to serve at all (its own hard
    cap is 300 files) - treated the same as exceeding MAX_PR_FILES by the
    caller, since it's well past that threshold anyway."""
    try:
        out = gh("pr", "diff", str(number), "--name-only")
    except subprocess.CalledProcessError as exc:
        if "exceeded the maximum number of files" in (exc.stderr or ""):
            return None
        raise
    return [line for line in out.splitlines() if line]


def touches_only_docs(files):
    """True if every file this PR changed is under docs/, in which case
    there is no code change to check docs against."""
    return bool(files) and all(f.startswith(DOCS_PREFIX) for f in files)


def call_llm(prompt):
    payload = {
        "model": LLM_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
    }
    req = urllib.request.Request(
        f"{LLM_API_BASE}/chat/completions",
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            **({"Authorization": f"Bearer {LLM_API_KEY}"} if LLM_API_KEY else {}),
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=600) as resp:
        body = json.loads(resp.read())
    content = body["choices"][0]["message"]["content"].strip()
    if content.startswith("```"):
        content = content.strip("`").split("\n", 1)[-1].rsplit("```", 1)[0]
    return content.strip()


def build_docs_header_index():
    """Walks DOCS_ROOT and returns a flat outline of every markdown header,
    each entry knowing the line range of the section it owns (down to, but
    not including, the next header of equal-or-higher level)."""
    index = []
    for root, _, files in os.walk(DOCS_ROOT):
        for fname in sorted(files):
            if not fname.endswith(".md"):
                continue
            path = os.path.relpath(os.path.join(root, fname), DOCS_ROOT)
            path = os.path.join(DOCS_PREFIX.rstrip("/"), path).replace(os.sep, "/")
            if path.startswith(EXCLUDED_DOC_PREFIXES) or path in EXCLUDED_DOC_FILES:
                continue
            with open(os.path.join(root, fname), encoding="utf-8", errors="replace") as f:
                lines = f.readlines()

            headers = []
            in_fence = False
            for i, line in enumerate(lines, start=1):
                if line.lstrip().startswith(("```", "~~~")):
                    in_fence = not in_fence
                    continue
                if in_fence:
                    continue
                stripped = line.lstrip("#")
                level = len(line) - len(stripped)
                if 1 <= level <= 6 and stripped.startswith(" "):
                    headers.append({"file": path, "level": level, "title": stripped.strip(), "start_line": i})

            for i, h in enumerate(headers):
                end_line = len(lines)
                for later in headers[i + 1:]:
                    if later["level"] <= h["level"]:
                        end_line = later["start_line"] - 1
                        break
                h["end_line"] = end_line
                h["_lines"] = lines
                index.append(h)
    return index


def get_excerpt_text(entry):
    body = "".join(entry["_lines"][entry["start_line"] - 1:entry["end_line"]])
    if len(body) > MAX_SECTION_CHARS:
        body = body[:MAX_SECTION_CHARS] + "\n... (section truncated)"
    return body


def summarize_diff_for_retrieval(pr, diff):
    """Produces a short natural-language description of what the diff changes,
    phrased so it can be embedded and matched against documentation prose
    (as opposed to a keyword list, which misses cases with no shared
    vocabulary). Deliberately diff-only, no full file content: this is a
    coarse first-pass filter, and re-sending the (much larger) full file
    content here too would double the expensive prompt-processing cost of
    every PR for no benefit - the precise, doc-verified determination
    happens in cross_reference_with_docs, where full content earns its cost."""
    prompt = (
        "Look at this code diff from the NebulaStream stream-processing "
        "engine. You do NOT get to see the actual documentation here - "
        "this is a coarse first pass; a later stage checks against the "
        "real docs. Answer four yes/no sub-questions, then a final verdict "
        "derived from them - don't jump straight to the verdict.\n\n"
        "Q1: does the diff change the observable behavior of something an "
        "EXISTING doc might describe (renamed keyword/flag/option, changed "
        "default, changed output/behavior of an existing component, "
        "removed behavior still described as present)? This repo documents "
        "developer/testing tooling too, not just end-user SQL/CLI/config "
        "(e.g. docs/development/*.md describes test harness and internal "
        "component behavior) - when unsure, answer yes, the next stage can "
        "rule it out against the real docs.\n"
        "Q2: does the diff add something NEW a user can now write in a "
        "query or set via CLI/config themselves - strictly a new SQL "
        "keyword, operator, or built-in function; a new CLI flag; a new "
        "configuration option?\n"
        "Q3: is the diff ONLY project process or contribution-policy prose "
        "- a PR checklist item, a review/assignment requirement, a "
        "contribution rule, meeting notes - even if it's inside README.md "
        "or docs/? (This is about process, not product/code behavior.)\n"
        "Q4: is the diff ONLY internal/refactor/test-only/build-system "
        "changes, or added/changed/removed binary assets (images, "
        "screenshots, diagrams), with no user-observable effect?\n\n"
        "Verdict: user_facing = (Q1 is yes OR Q2 is yes) AND Q3 is no AND "
        "Q4 is no. Q3=yes always forces user_facing=false, no matter what "
        "Q1/Q2 are - process/policy prose is never in scope here, even "
        "though the diff technically 'documents' something.\n\n"
        "Examples:\n"
        "- Diff renames a CLI flag --old-name to --new-name: Q1=yes Q2=no "
        "Q3=no Q4=no -> user_facing=true, "
        'summary="the --old-name CLI flag is renamed to --new-name".\n'
        "- Diff adds a new PR-checklist requirement to README.md: Q1=no "
        "(nothing existing becomes wrong) Q2=no (not SQL/CLI/config) Q3=yes "
        "-> user_facing=false regardless of anything else.\n"
        "- Diff adds an internal counter field and increments it in a "
        "private method; no docs mention this field: Q1=no Q2=no Q3=no "
        "Q4=yes -> user_facing=false.\n"
        "- Diff adds a new built-in SQL function ISNAN(x) usable in "
        "queries: Q2=yes -> user_facing=true, "
        'summary="a new ISNAN built-in function is now usable in query '
        'expressions".\n\n'
        "If user_facing=true, write a 1-3 sentence natural-language "
        "description of WHAT changed, phrased the way it might appear in "
        "documentation prose, so it can be matched against doc text.\n\n"
        f"{pr_context(pr)}\n\nDiff:\n{diff}\n\n"
        "Respond with ONLY a JSON object: "
        '{"q1_existing_behavior_changed": true|false, '
        '"q2_new_reachable_syntax": true|false, '
        '"q3_process_or_policy_only": true|false, '
        '"q4_purely_internal": true|false, '
        '"user_facing": true|false, "summary": "..."}'
    )
    content = call_llm(prompt)
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        print(f"::warning::could not parse diff summary for PR #{pr['number']}: {content}")
        return False, ""
    user_facing = bool(parsed.get("user_facing"))
    summary = parsed.get("summary", "").strip()
    if user_facing and not summary:
        print(f"::warning::PR #{pr['number']}: user_facing=true but summary was empty, treating as not user-facing")
        return False, ""
    return user_facing, summary


def embed_texts(texts):
    """Batch-embeds a list of strings via Ollama's native /api/embed endpoint,
    chunked to EMBED_BATCH_SIZE per request - a single request for the whole
    docs corpus (hundreds of sections) took over 10 minutes on CPU-bound
    inference in testing and blew the request timeout outright."""
    if not texts:
        return []
    embeddings = []
    for i in range(0, len(texts), EMBED_BATCH_SIZE):
        chunk = texts[i:i + EMBED_BATCH_SIZE]
        payload = {"model": EMBED_MODEL, "input": chunk}
        req = urllib.request.Request(
            f"{OLLAMA_NATIVE_BASE}/api/embed",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=600) as resp:
            body = json.loads(resp.read())
        embeddings.extend(body["embeddings"])
    return embeddings


def cosine_similarity(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def select_relevant_sections_by_embedding(summary, index, chunk_vectors):
    query_vector = embed_texts([summary])[0]
    scored = sorted(
        zip(index, chunk_vectors),
        key=lambda pair: cosine_similarity(query_vector, pair[1]),
        reverse=True,
    )
    return [h for h, _ in scored[:MAX_SELECTED_SECTIONS]]


def cross_reference_with_docs(pr, diff, file_contents, sections):
    excerpt_parts = []
    total = 0
    for h in sections:
        text = get_excerpt_text(h)
        if total + len(text) > MAX_TOTAL_SECTION_CHARS:
            continue
        excerpt_parts.append(f"### {h['file']} > {h['title']}\n{text}")
        total += len(text)
    excerpts = "\n\n".join(excerpt_parts)

    prompt = (
        "You review code diffs from the NebulaStream stream-processing engine "
        "against the CURRENT text of specific documentation sections and "
        "related source code excerpts, to catch docs that a merged PR has "
        "made stale.\n"
        "The excerpts below were read from the repository AFTER this PR was "
        "merged, so they already reflect the current, post-merge state of the "
        "codebase (including this PR's own changes, if it touched docs/). The "
        "diff's '-' lines are old code that no longer exists; its '+' lines "
        "are the current code. The CURRENT content of each file the diff "
        "touches is also given below, in full - use it as the ground truth "
        "for what a file actually contains now, rather than trying to "
        "reconstruct that purely from diff +/- lines (e.g. lines added "
        "around an existing, unchanged block can look like new content in "
        "the diff even though the file's actual current content shows nothing "
        "new was introduced there). Reason about whether the excerpts, as "
        "they currently stand, correctly describe that current state.\n"
        "The full file content is there so you read the diff correctly - it "
        "is NOT license to flag anything else you notice in the file. Only "
        "ever base needs_docs_update on what the diff's '+'/'-' lines "
        "actually changed. If something looks off in a part of the file the "
        "diff didn't touch, that's a pre-existing issue unrelated to this "
        "PR and out of scope - do not flag it.\n"
        "Below are excerpts (documentation and/or code) that plausibly relate "
        "to this diff, followed by the diff itself. Flag needs_docs_update=true "
        "in either of these cases:\n"
        "(a) the diff makes an EXISTING, specific statement in the excerpts "
        "incorrect or outdated (a renamed keyword/flag/option, a changed "
        "default, removed behavior the excerpt still describes as present, "
        "an example that no longer matches actual behavior);\n"
        "(b) the diff adds something new that a user can now write in a "
        "query or set via CLI/config - strictly a new SQL keyword, "
        "operator, or built-in function; a new CLI flag; a new "
        "configuration option - AND an excerpt is the kind of place that "
        "would list/enumerate such things (e.g. a table or list of "
        "supported functions, flags, or options) but doesn't yet include "
        "this new one.\n"
        "Do NOT flag purely internal additions with nothing new reachable "
        "from a query/config/CLI (internal classes/helpers/refactors, "
        "counters, instrumentation, test-only or build-system changes); do "
        "NOT flag case (b) against an excerpt that isn't actually an "
        "enumeration of that kind of thing - a coverage gap only matters "
        "where the excerpt already tries to be a complete list; and do NOT "
        "flag anything about project process or contribution-policy "
        "statements (a new PR checklist item, a new contribution rule, a "
        "new review/assignment requirement) even if some excerpt is a "
        "checklist/policy doc that doesn't mention it yet - those are "
        "entirely out of scope here, not a documentation staleness issue. "
        "Ignore anything the excerpts don't actually claim. Every claim in "
        "your reason must be something you can directly point to in the "
        "excerpts or diff above - do not hedge with 'might', 'could', or "
        "'possibly'; if you cannot state definitely that a specific excerpt "
        "sentence is now wrong, answer needs_docs_update=false instead of "
        "guessing.\n\n"
        f"{pr_context(pr)}\n\n"
        f"Excerpts:\n{excerpts}\n\n"
        f"Diff:\n{diff}\n\n"
        f"Current file contents:\n{file_contents}\n\n"
        "Respond with ONLY a JSON object: {\"needs_docs_update\": true|false, "
        '"reason": "a thorough explanation for a maintainer: name the exact '
        "file/section that is now stale, quote or paraphrase the specific "
        "statement it makes, and explain precisely what about the diff "
        "contradicts it. If needs_docs_update is false, briefly explain why "
        'the excerpts are unaffected."}'
    )
    content = call_llm(prompt)
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        print(f"::warning::could not parse cross-reference response for PR #{pr['number']}: {content}")
        return False, "unparseable LLM response"
    needs_update = bool(parsed.get("needs_docs_update"))
    reason = parsed.get("reason", "")

    if needs_update and is_hedged(reason):
        confirmed, quote = verify_flagged_reason(pr, diff, excerpts, reason)
        if not confirmed or not quote_appears_in(quote, excerpts):
            print(f"::warning::PR #{pr['number']}: downgrading hedged flag, could not confirm a specific stale statement verbatim in the excerpts - {reason}")
            return False, f"initially flagged but not confirmed on verification: {reason}"
        reason = f"{reason}\n\nConfirmed excerpt text: \"{quote}\""

    return needs_update, reason


HEDGE_WORDS = ("might", "may ", "could ", "possibly", "perhaps", "seems to", "appears to", "likely", "unclear", "not certain", "not sure")


def is_hedged(text):
    lowered = text.lower()
    return any(w in lowered for w in HEDGE_WORDS)


def quote_appears_in(quote, excerpts):
    """Don't just trust the model's self-reported confirmed=true - a quote
    it can't actually back up verbatim means it didn't really verify
    anything. Whitespace-normalized substring match, not exact, since
    reflowed prose can differ in line breaks without differing in content."""
    normalize = lambda s: " ".join(s.split())
    quote = normalize(quote)
    return bool(quote) and len(quote) > 15 and quote.lower() in normalize(excerpts).lower()


def verify_flagged_reason(pr, diff, excerpts, reason):
    """Second-pass check for a hedged first-pass answer ('docs might still
    mention X'). Rather than trust vague language, ask the model to either
    quote the exact excerpt sentence it claims is now wrong, or admit it
    can't - a hedge usually means it never actually verified the claim
    against the real excerpt text."""
    prompt = (
        "You previously flagged a PR as needing a docs update, but your "
        "reasoning used hedging language ('might', 'could', 'possibly') "
        "instead of a definite claim. Look again at the excerpts and the "
        "diff below. Can you quote the EXACT sentence in the excerpts that "
        "is now incorrect because of this diff? If yes, confirmed=true and "
        "quote must be copied verbatim from the excerpts. If you cannot "
        "point to a specific sentence that is actually contradicted (not "
        "just plausibly related), confirmed=false.\n\n"
        f"{pr_context(pr)}\n\n"
        f"Excerpts:\n{excerpts}\n\n"
        f"Diff:\n{diff}\n\n"
        f"Your earlier reasoning: {reason}\n\n"
        "Respond with ONLY a JSON object: "
        '{"confirmed": true|false, "quote": "exact sentence from the excerpts, or empty string"}'
    )
    content = call_llm(prompt)
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        print(f"::warning::could not parse verification response for PR #{pr['number']}: {content}")
        return False, ""
    return bool(parsed.get("confirmed")), parsed.get("quote", "")


def check_pr_against_docs(pr, diff, file_contents, index, chunk_vectors):
    user_facing, summary = summarize_diff_for_retrieval(pr, diff)
    if not user_facing:
        return False, summary or "internal/refactor change, no user-facing impact"

    if not index:
        return ask_llm_needs_docs_update(pr, diff, file_contents)

    sections = select_relevant_sections_by_embedding(summary, index, chunk_vectors)
    return cross_reference_with_docs(pr, diff, file_contents, sections)


def ask_llm_needs_docs_update(pr, diff, file_contents):
    prompt = (
        "You review code diffs from a stream-processing engine (NebulaStream) "
        "to decide if project documentation (under docs/) is now stale.\n"
        "This PR did NOT modify anything under docs/. Decide whether the code "
        "change either makes some EXISTING documented statement incorrect (a "
        "renamed keyword/flag/option, a changed default, removed behavior "
        "still described as present), or adds something new a user can now "
        "write in a query or set via config/CLI (a new SQL keyword/operator/"
        "function, CLI flag, or configuration option). Purely internal "
        "additions with nothing new reachable from a query/config/CLI are "
        "out of scope, no matter how novel the implementation is, and so "
        "are project process/contribution-policy statements (a new PR "
        "checklist item, a new contribution rule) - never flag those.\n"
        "Ignore refactors, internal-only changes, test-only changes, and pure "
        "bug fixes that don't change documented behavior. The current file "
        "contents below are for reading the diff correctly, not license to "
        "flag anything else noticed in the file - base your answer only on "
        "what the diff's '+'/'-' lines actually changed.\n\n"
        f"{pr_context(pr)}\n\n"
        f"Diff:\n{diff}\n\n"
        f"Current file contents:\n{file_contents}\n\n"
        "Respond with ONLY a JSON object: "
        '{"needs_docs_update": true|false, "reason": "one sentence"}'
    )
    content = call_llm(prompt)
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        print(f"::warning::could not parse LLM response for PR #{pr['number']}: {content}")
        return False, "unparseable LLM response"
    return bool(parsed.get("needs_docs_update")), parsed.get("reason", "")


def post_drift_warning(pr, reason):
    mentions = " ".join(f"@**{u}**" for u in ZULIP_NOTIFY_USERS)
    content = (
        f"**Possible docs drift**: [#{pr['number']} {pr['title']}]({pr['url']}) "
        f"may need a docs update. {mentions}\n"
        f"> {reason}"
    )
    data = urllib.parse.urlencode({
        "type": "stream",
        "to": ZULIP_STREAM,
        "topic": ZULIP_TOPIC,
        "content": content,
    }).encode()
    req = urllib.request.Request(f"{ZULIP_SITE}/api/v1/messages", data=data, method="POST")
    auth = f"{ZULIP_BOT_EMAIL}:{ZULIP_BOT_API_KEY}".encode()
    req.add_header("Authorization", "Basic " + base64.b64encode(auth).decode())
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()


def main():
    prs = list_prs_merged_last_week()
    print(f"Found {len(prs)} PR(s) merged in the last 7 days")

    docs_index = build_docs_header_index()
    print(f"Indexed {len(docs_index)} doc section(s)")
    chunk_vectors = embed_texts([get_excerpt_text(h) for h in docs_index]) if docs_index else []
    print(f"Embedded {len(chunk_vectors)} doc section(s) with {EMBED_MODEL}")

    flagged = 0
    errored = []
    for pr in prs:
        try:
            files = get_pr_files(pr["number"])
            if files is None or len(files) > MAX_PR_FILES:
                count = "300+" if files is None else str(len(files))
                print(f"PR #{pr['number']}: ok - touches {count} files (>{MAX_PR_FILES}), assumed formatting/mass-change, skipped")
                continue
            if touches_only_docs(files):
                print(f"PR #{pr['number']}: ok - docs-only change, no code to check")
                continue
            diff = get_pr_diff(pr["number"])
            if not diff.strip():
                continue
            file_contents = get_current_file_contents(files)
            needs_update, reason = check_pr_against_docs(pr, diff, file_contents, docs_index, chunk_vectors)
            if needs_update:
                flagged += 1
                print(f"PR #{pr['number']}: flagged - {reason}")
                post_drift_warning(pr, reason)
            else:
                print(f"PR #{pr['number']}: ok - {reason}")
        except Exception as exc:
            detail = describe_exception(exc)
            errored.append((pr["number"], detail))
            print(f"::warning::PR #{pr['number']}: skipped after error - {detail}")
            continue

    print(f"Done. Flagged {flagged} of {len(prs)} PR(s), {len(errored)} errored.")

    if errored:
        print("::group::PRs skipped due to errors")
        for number, detail in errored:
            print(f"PR #{number}: {detail}")
        print("::endgroup::")

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("## Docs drift check summary\n\n")
            f.write(f"- PRs checked: {len(prs)}\n")
            f.write(f"- Flagged: {flagged}\n")
            f.write(f"- Errored: {len(errored)}\n\n")
            if errored:
                f.write("### Errors\n\n")
                f.write("| PR | Detail |\n|---|---|\n")
                for number, detail in errored:
                    escaped = detail.replace("|", "\\|").replace("\n", " ")
                    f.write(f"| #{number} | {escaped} |\n")

    return 1 if errored else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"::error::docs drift check failed before completing: {describe_exception(exc)}")
        sys.exit(1)
