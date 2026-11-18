# Agent instructions

Rules for coding agents that work in this repository.

## Comments

Write a comment when you know something that the reader cannot recover from the code:
the assumption that makes it correct, what debugging found, the obvious alternative and what rules it out, behavior that is absent on purpose, or where a constant comes from.
Write only what you know from the code, the tests, or this conversation, and do not invent a reason or a measurement.

- Do not restate the code. If a reader loses nothing when the comment is deleted, delete it.
- Describe the code as it is, not your change: no "now", "previously", "as requested". The reader has not seen the diff or this conversation.
- Do not label steps or sections ("Step 1", "Helpers").
- Every public type gets a doc comment. A function gets one only when its name and signature do not show what a caller must know.
- A doc comment states the purpose and what a caller must know. It does not list members or parameters, and it does not retell the body: how and why the code works is written inside the function.
- State each fact in one place. Do not repeat a type's comment on its members, a function's contract where it is called, or an assertion's message.
- Do not state that something is impossible or required unless a test, an assertion, or a type enforces it. Otherwise, write what you observed.
- A test whose name states the property gets no comment.
- Mark missing work with `TODO #<issue>:`, using an issue that exists, and a counterintuitive gotcha with `NOTE:`.
- Do not refer to internal helpers or private members by name, because those names change. Public types and functions, protocol fields, and issue or RFC numbers are fine.
- Show an example of input and output for a format, a naming scheme, or a mapping. Draw a small ASCII diagram for a structure or a sequence of states.
- Start every sentence on a new line. Break a long one at a comma, a colon, or a semicolon, not at the column limit.
- Use the literal verb for what the code does. Short sentences, no dashes as punctuation.
- Keep existing comments when you edit around them, and fix the ones that described behavior that you changed.

```cpp
/// BAD (restates the code): Sort the plans by host.
/// BAD (narrates the change): Now sorts by host instead of by id.
/// GOOD (a reason that the code cannot show):
/// The placement result is stored in an unordered map.
/// Sort by host so the rendered output is deterministic.
std::ranges::sort(plans, {}, &Entry::host);
```

Before you finish a change, reread every comment that you added against this list.
`docs/standards/comment_guidelines.md` is the full guide with examples.
