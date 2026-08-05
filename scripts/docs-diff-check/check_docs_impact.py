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
Runs weekly, checking PRs merged into the default branch in the last 7 days.
Only cares whether CODE changes have made docs/ stale - PRs that only touch
docs/ are skipped outright, nothing is said about them.

For each qualifying PR:
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
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

REPO = os.environ["GITHUB_REPOSITORY"]
DOCS_PREFIX = "docs/"
DOCS_ROOT = os.environ.get("DOCS_ROOT", "docs")

MAX_SECTION_CHARS = 1500
MAX_TOTAL_SECTION_CHARS = 8000
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

MAX_DIFF_CHARS = 12000


def gh(*args):
    result = subprocess.run(
        ["gh", *args, "--repo", REPO],
        capture_output=True, text=True, check=True,
    )
    return result.stdout


def list_prs_merged_last_week():
    since = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    out = gh(
        "pr", "list",
        "--search", f"is:pr is:merged sort:created-asc merged:>={since}",
        "--json", "number,title,url,author,mergedAt,files",
        "--limit", "200",
    )
    return json.loads(out)


def touches_docs(pr):
    return any(f["path"].startswith(DOCS_PREFIX) for f in pr.get("files", []))


def get_pr_diff(number):
    diff = gh("pr", "diff", str(number))
    if len(diff) > MAX_DIFF_CHARS:
        diff = diff[:MAX_DIFF_CHARS] + "\n... (diff truncated)"
    return diff


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
            with open(os.path.join(root, fname), encoding="utf-8", errors="replace") as f:
                lines = f.readlines()

            headers = []
            for i, line in enumerate(lines, start=1):
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
    vocabulary)."""
    prompt = (
        "Look at this code diff from the NebulaStream stream-processing engine.\n"
        "Decide if it changes any user-facing behavior, public API, "
        "configuration option, CLI flag, SQL/query syntax, or deployment step "
        "(user_facing=true), or if it's purely internal/refactor/test-only "
        "(user_facing=false).\n"
        "If user-facing, write a 1-3 sentence natural-language description of "
        "WHAT changed, phrased the way it might appear in documentation prose "
        "(e.g. 'the SELECT keyword used to project columns in a query is "
        "renamed to CHOOSE'), so it can be matched against doc text.\n\n"
        f"PR title: {pr['title']}\n\nDiff:\n{diff}\n\n"
        "Respond with ONLY a JSON object: "
        '{"user_facing": true|false, "summary": "..."}'
    )
    content = call_llm(prompt)
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        print(f"::warning::could not parse diff summary for PR #{pr['number']}: {content}")
        return False, ""
    return bool(parsed.get("user_facing")), parsed.get("summary", "")


def embed_texts(texts):
    """Batch-embeds a list of strings via Ollama's native /api/embed endpoint."""
    if not texts:
        return []
    payload = {"model": EMBED_MODEL, "input": texts}
    req = urllib.request.Request(
        f"{OLLAMA_NATIVE_BASE}/api/embed",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=600) as resp:
        body = json.loads(resp.read())
    return body["embeddings"]


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


def cross_reference_with_docs(pr, diff, sections):
    excerpt_parts = []
    total = 0
    for h in sections:
        text = get_excerpt_text(h)
        if total + len(text) > MAX_TOTAL_SECTION_CHARS:
            break
        excerpt_parts.append(f"### {h['file']} > {h['title']}\n{text}")
        total += len(text)
    excerpts = "\n\n".join(excerpt_parts)

    prompt = (
        "You review code diffs from the NebulaStream stream-processing engine "
        "against the CURRENT text of specific documentation sections and "
        "related source code excerpts, to catch docs that a merged PR has "
        "made stale.\n"
        "Below are excerpts (documentation and/or code) that plausibly relate "
        "to this diff, followed by the diff itself. Decide whether the diff "
        "makes any specific statement in the documentation excerpts incorrect "
        "or outdated (e.g. a renamed keyword/flag/option, a changed default, "
        "removed behavior that the excerpt still describes as present). "
        "Ignore purely internal refactors and anything the excerpts don't "
        "actually claim.\n\n"
        f"PR title: {pr['title']}\n\n"
        f"Excerpts:\n{excerpts}\n\n"
        f"Diff:\n{diff}\n\n"
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
    return bool(parsed.get("needs_docs_update")), parsed.get("reason", "")


def check_pr_against_docs(pr, diff, index, chunk_vectors):
    user_facing, summary = summarize_diff_for_retrieval(pr, diff)
    if not user_facing:
        return False, summary or "internal/refactor change, no user-facing impact"

    if not index:
        return ask_llm_needs_docs_update(pr, diff)

    sections = select_relevant_sections_by_embedding(summary, index, chunk_vectors)
    return cross_reference_with_docs(pr, diff, sections)


def ask_llm_needs_docs_update(pr, diff):
    prompt = (
        "You review code diffs from a stream-processing engine (NebulaStream) "
        "to decide if project documentation (under docs/) is now stale.\n"
        "This PR did NOT modify anything under docs/. Decide whether the code "
        "change alters user-facing behavior, public APIs, configuration options, "
        "CLI flags, query language semantics, or deployment/build steps in a way "
        "that existing documentation would need to be updated to stay correct.\n"
        "Ignore refactors, internal-only changes, test-only changes, and pure "
        "bug fixes that don't change documented behavior.\n\n"
        f"PR title: {pr['title']}\n\n"
        f"Diff:\n{diff}\n\n"
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
    content = (
        f"**Possible docs drift**: [#{pr['number']} {pr['title']}]({pr['url']}) "
        f"by @**{pr['author']['login']}** was merged without touching `docs/`.\n"
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
    for pr in prs:
        if touches_docs(pr):
            continue

        diff = get_pr_diff(pr["number"])
        if not diff.strip():
            continue
        needs_update, reason = check_pr_against_docs(pr, diff, docs_index, chunk_vectors)
        if needs_update:
            flagged += 1
            print(f"PR #{pr['number']}: flagged - {reason}")
            post_drift_warning(pr, reason)
        else:
            print(f"PR #{pr['number']}: ok - {reason}")

    print(f"Done. Flagged {flagged} of {len(prs)} PR(s).")


if __name__ == "__main__":
    sys.exit(main())
