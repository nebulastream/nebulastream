# AI Tool Usage

This folder records every use of AI tools (Anthropic's Claude family) during
the bachelor's thesis *Out-of-Core Stream Processing in NebulaStream: A
RocksDB-Based State Backend for Memory-Constrained Edge Devices* at TU Berlin
(DIMA chair).

The eight subfolders mirror the phases listed in `misc/declaration-of-aids.tex`
of the thesis LaTeX project. Each conversation gets its own markdown file. Once
committed, entries are not edited; corrections go in a follow-up entry that
links back to the original.

## Folder layout

| Phase | Folder |
|-------|--------|
| 1. Finding and nailing down the topic | [`01-topic-finding/`](01-topic-finding) |
| 2. Understanding the topic | [`02-understanding-topic/`](02-understanding-topic) |
| 3. Literature research | [`03-literature-research/`](03-literature-research) |
| 4. Implementing the prototype | [`04-implementing-prototype/`](04-implementing-prototype) |
| 5. Testing the prototype | [`05-testing-prototype/`](05-testing-prototype) |
| 6. Planning the thesis | [`06-planning-thesis/`](06-planning-thesis) |
| 7. Writing the thesis | [`07-writing-thesis/`](07-writing-thesis) |
| 8. Revising the thesis | [`08-revising-thesis/`](08-revising-thesis) |

## File naming

`YYYY-MM-DD-short-topic-kebab.md`

Examples: `2026-05-11-summary-rewrite.md`, `2026-05-12-uml-class-diagram.md`.

## Per-entry contents

Every entry uses [`_template.md`](_template.md) as a skeleton:

1. YAML frontmatter (date, phase, level of use, model, tool, share link if available).
2. Goal of the conversation.
3. Outcome or artifact produced.
4. The verbatim prompt.
5. The full transcript.

## Levels of use (DIMA scale)

| Level | Characterization | Examples |
|-------|------------------|----------|
| 1 | For inspiration only | Topic suggestions, wording, spell check |
| 2 | Supplementary | Proposed RQs, term explanations, summaries |
| 3 | Supportive | Iterative writing dialogue, code review, test writing |
| 4 | Content-shaping | Background generation, model fills outline, multi-line code |

Source: TU Berlin DIMA thesis template, adapted from [Leuphana](https://pubdata.leuphana.de/server/api/core/bitstreams/f4c7f7f2-3974-410c-a07b-4e545390561e/content) (pp. 20, 24).

## Adding a new entry

1. End the conversation. If on claude.ai, click Share, copy the public URL.
2. Copy the conversation as markdown ("Copy as markdown" on claude.ai, or paste from Claude Desktop).
3. Save as `<phase>/YYYY-MM-DD-short-topic.md`, filled out from `_template.md`.
4. Commit and push: `git commit -m "ai-usage: <phase>: <topic>"` then `git push`.

`misc/declaration-of-aids.tex` already cites each phase folder, so no `.tex`
edit is needed when adding new entries inside an existing phase.

---

Author: Benjamin Sainsanaa Steinbüchel · b.steinbuechel@campus.tu-berlin.de
