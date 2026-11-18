# Comment Guidelines

## Purpose and Reader

A comment states what the code cannot show: a reason, a constraint, a contract with the caller.
A comment has a cost: it must stay true when the code changes, and readers who encounter too many bad comments stop reading comments.

Write every comment for a reader who opens the file months later to change it.
This reader knows the language and the component, but has not seen the diff, the pull request, or the discussion that led to the code.
The test for any comment is whether this reader would miss it if it were deleted.

`AGENTS.md` at the repository root contains the short form of these rules for coding agents.
This guide is the reference.

## When to Write a Comment

Write a comment when you know something that the reader cannot recover from the code.
The kinds below follow Jon Gjengset's talk, [Don't make me read between the lines](https://www.youtube.com/watch?v=Ke3PYexY6XE).
The examples are from this repository, some of them shortened.

### TODOs

Say what is missing and link the issue that tracks it.
The form is described under [TODO and NOTE](#todo-and-note).

### Sources

Cite the specification, paper, or upstream code that the implementation follows.
Pin a link to a version or commit so that it keeps pointing at the same text.

```cpp
/// Implements MurmurHash3 as in robin-hood-hashing:
/// https://github.com/martinus/robin-hood-hashing/blob/fb1483621fda28d4afb31c0097c1a4a457fdd35b/src/include/robin_hood.h#L748
VarVal hashVarVal(const VarVal& input)
```

### Correctness

State the assumption that the code depends on, and why it holds.

```cpp
/// Bool must be checked before arithmetic since bool is arithmetic.
else if constexpr (std::is_same_v<Raw, bool>)
```

### Hard-earned insights

Record what long debugging or research found, even when the resulting code is one line.

```cpp
/// Both functions are kept out of line on purpose.
/// Calling the join directly from both branches with swapped arguments lets the host compiler merge the two calls into one call site.
/// Nautilus then traces both branches as the same code and binds the same paged vector to outer and inner (https://github.com/nebulastream/nautilus/issues/487).
[[gnu::noinline]] void performNLJLeftOuter(
```

### Choices

Explain constants, units, bounds, and types: where the value comes from, or that it was picked arbitrarily.

```cpp
/// Maximum hostname length per RFC 1035
constexpr size_t MaxHostnameLength = 253;
```

### Failed simplifications

State the obvious alternative and what rules it out, so that nobody tries it again.

```cpp
/// std::move_only_function is the C++23 equivalent but is not yet available in libc++19.
using BackendProvider = absl::AnyInvocable<UniquePtr<QuerySubmissionBackend>(const WorkerConfig&)>;
```

Say what goes wrong with the alternative, not only that it was not chosen.
A contrast without a failure cannot be wrong, so nobody notices when it stops being true.
If you cannot say what goes wrong, the comment records a preference and is not needed.

```cpp
/// BAD: a contrast, with no way to tell whether it still holds.
/// The callback is returned to the caller instead of being called here.

/// GOOD: the failure that the code avoids.
/// The callback must be called outside the state transition.
/// Otherwise destroying the sources can try to take the state lock again, which deadlocks.
```

### Algorithm narratives

Describe the steps of logic that are hard to read from the code, for example template machinery or bit operations.
Do not describe straightforward statements.

```cpp
/// Rounds up to the next multiple of the alignment, which must be a power of two.
return (size + alignment - 1) & ~(alignment - 1);
```

### Intentional omissions

Say when behavior is absent on purpose, where a maintainer might otherwise add it.

```cpp
/// A stopped query does not need the final watermark or the EoS.
if (auto watermarkBuffer = getBufferUnlessStopped(*bufferProvider, stopToken))
```

### Trade-offs

Record the alternative considered and the constraint that ruled it out.

```cpp
/// These are free functions instead of members of Schema,
/// because Schema must not require its field type to have a notion of size, only a getFullyQualifiedName() method.
[[nodiscard]] inline size_t getSizeInBytes(const Schema<QualifiedUnboundField, Ordered>& schema)
```

## When Not to Write a Comment

Three kinds of comments should not be written at all.

### Restating the code

The comment repeats what the next line says. It will be out of date as soon as the code it describes changes.

```cpp
/// BAD
/// Sort the plans by host.
std::ranges::sort(plans, {}, &Entry::host);

/// GOOD
/// The placement result is stored in an unordered map.
/// Sort by host so the rendered output is deterministic.
std::ranges::sort(plans, {}, &Entry::host);
```

### Narrating the change

The comment describes how the code is different from an earlier version, or why the author was asked to change it.
The reader has not seen the earlier version.
Describe the code as it is and put the history into the commit message.

```cpp
/// BAD
/// Now retries three times instead of failing on the first error.

/// GOOD
/// The broker rejects connections while it starts, so a single failure does not mean that it is down.
```

### Labeling steps

The comment marks a section of a function with a name.
If a function needs such labels to be readable, split it into functions with those names.

```cpp
/// BAD
/// Step 1: parse the input
auto plan = parse(input);
/// Step 2: optimize the plan
plan = optimize(plan);
```

## TODO and NOTE

Two markers make a comment easy to find, both for a reader and for a search.
Use them.

### TODO

`TODO #<issue>:` marks work that is still missing.
The issue number is required: the format check rejects a TODO without an open issue.

```cpp
/// TODO #<issue>: duplicate sinks are not registered
lockedSinks->emplace(std::move(sinkName), sinkDescriptor);
```

### NOTE

`NOTE:` marks a gotcha.
A gotcha is behavior that a reader does not expect and that leads to a mistake when it is overlooked.
Keep the marker for those cases, so that it still stands out.

```rust
/// NOTE: The return value only indicates whether the local queue was closed,
/// not whether the close signal reached the receiver.
pub fn close(self) -> bool {
```

## Examples and Diagrams

A concrete example often explains a rule faster than a paragraph describing it.
When a comment explains a format, a naming scheme, or a mapping, show some inputs and outputs.
Pick values that make the rule visible, including the edge cases.

```cpp
/// Derives the key of one part of a test file from its path under the root.
/// E.g., with root `/root/nes-systests`:
///     /root/nes-systests/benchmark/Nexmark.test           part 0 of 1 -> BENCHMARK_D_NEXMARK
///     /root/nes-systests/benchmark/Nexmark.test           part 1 of 3 -> BENCHMARK_D_NEXMARK_C1
///     /root/nes-systests/regression/2025-09-10_Join.test  part 0 of 1 -> REGRESSION_D_2025_2D_09_2D_10__JOIN
///     /elsewhere/Nexmark.test                             part 0 of 1 -> _D_ELSEWHERE_D_NEXMARK
[[nodiscard]] TestFileKey deriveKeyOf(const std::filesystem::path& testFile, size_t part, size_t parts) const;
```

When a comment describes a structure, such as a memory layout, a hierarchy of buffers, or a sequence of states, draw it.
A small ASCII diagram shows at a glance what the reader would otherwise have to assemble from a paragraph.

```cpp
/// Buffer structure:
/// main buffer (header, inline records)
/// ├── child 0 .. k-1: variable-sized data of the inline records
/// ├── child k (firstPageChildIndex): page 0 (header, records)
/// │     └── children: variable-sized data of its records
/// └── child k+n-1: page n-1
class PagedVector
```

An example or a diagram does not replace the reason for the code.
It replaces only the paragraph that would describe the mechanism.

## Doc Comments

A doc comment is the comment above a declaration.
It is written for a caller who does not read the implementation.

Every public type gets a doc comment.
In C++, a type is public when it is declared in a header under a component's `include` directory.
In Rust, a type is public when it is declared `pub`.

A function gets a doc comment only when its name and signature do not show what a caller must know.

```cpp
/// BAD: the name and the return type already say this.
/// Returns the fully qualified name.
[[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;

/// GOOD: the signature shows neither that the copy can stop early nor what the return value means.
/// Copies the content of the buffer and all of its children, in iteration order, into destination.
/// Stops once destination is full.
/// @return number of bytes written; equals getTotalContentLength(buffer) iff destination was large enough.
[[nodiscard]] size_t copyInto(const TupleBuffer& buffer, std::span<std::byte> destination);
```

A doc comment states the purpose and what a caller must know: preconditions, ownership, failure.
It does not list the members or the parameters.

```cpp
/// A single (possibly qualified) config assignment as a frontend produced it, e.g.
/// `'ALL' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES`
class LiteralConfigValue
```

A doc comment does not retell the body.
How the code works and why is written inside the function, at the line where the decision is made.
How several types or functions work together is written once, in a comment at the top of the file.

```cpp
/// BAD: retells the body.
/// First collects the plans of all workers, then sorts them by host, then renders each one.
std::string render(const DistributedPlan& plan);

/// GOOD: what a caller can depend on.
/// Renders one section per worker, ordered by host.
std::string render(const DistributedPlan& plan);
```

## One Owner per Fact

Write every fact in one place.
The doc comment of a declaration owns its contract, and the comment at a decision owns the reason for it.
Every other place refers to the owner or says nothing.
A fact that is written in three places is updated in one of them when the code changes.

A sign of a duplicate is the same clause, almost word for word, in a file comment, a type comment, and an inline comment.

### A type and its members

The comment on a type and the comments on its members are read together.
The type's comment says what the type is for and what all members share.
A member's comment says only what is specific to that member.

```rust
// BAD: the summary previews every variant, and each variant repeats it.
/// The wait applied to a request: none, until a state, or until terminated.
pub enum Wait {
    /// No wait.
    None,
    /// Wait until the query reaches the state.
    UntilState { state: QueryState },
    /// Wait until the query has terminated.
    UntilTerminated,
}

// GOOD
/// When the coordinator releases a request's reply.
/// A `None` timeout waits indefinitely.
pub enum Wait {
    /// As soon as the statement has been applied to the catalog.
    None,
    /// Once the query reaches the state or terminates; a timeout is an error.
    UntilState { state: QueryState, timeout: Option<Duration> },
    /// Once every affected query has terminated; a timeout is an error.
    UntilTerminated { timeout: Option<Duration> },
}
```

### A call and the function that it calls

Do not repeat the contract of a function where it is called.
Say only what this call adds.

```cpp
/// BAD: repeats the doc comment of the function.
/// Stops once the destination is full and returns the number of bytes written.
const auto written = copyInto(buffer, destination);

/// GOOD: what is specific to this call.
/// A short write is expected here, because the rest is sent with the next frame.
const auto written = copyInto(buffer, destination);
```

### An assertion and its message

Do not repeat the message of an assertion in a comment above it.

```cpp
/// BAD: the message already says it.
/// An identifier cannot contain a dot.
PRECONDITION(not value.contains('.'), "Invalid identifier, cannot contain dot: {}", value);
```

## Referring to Other Code

The compiler does not check a name in a comment.
When the function, field, or type is renamed or removed, the comment keeps the old name.

Refer by name only to what is stable:

- public types and functions
- protocol fields and wire constants
- external identifiers, such as an RFC number or an issue number

Do not refer to internal helpers or private members by name.
Describe the behavior instead.

```cpp
/// BAD: refers to two private helpers by name.
/// Called by flushPending() after resetCursor() has run.

/// GOOD: says the same without a name that can change.
/// Runs once per flushed batch, after the cursor was reset.

/// GOOD: refers to a public function from the same header.
/// @return number of bytes written; equals getTotalContentLength(buffer) iff destination was large enough.
```

## Comments in Tests

A test whose name and assertion messages state the property needs no comment.
Write one for a setup that the test body does not explain, for a fixture whose shape is the scenario, or for a test with several phases.

```cpp
/// BAD: the test name already says this.
/// Tests that an identifier with a dot is rejected.
TEST(IdentifierTest, RejectsIdentifierWithDot)

/// GOOD: a setup that the test body does not explain.
/// The renderer logs when it skips an edge, so a logger must be set up regardless of which tests ran before in this binary.
static void SetUpTestSuite() { Logger::setupLogging("PlanRendererTest.log", LogLevel::LOG_DEBUG); }
```

## One Sentence per Line

Start every sentence on a new line.
Do not fill lines up to the column limit.
A reflowed paragraph turns a one-word edit into a diff of several lines and hides which sentence changed.

Break a sentence that is too long for one line at semantic boundaries: after a comma, a colon, a semicolon, or a closing parenthesis.
This helps the reading flow tremendously, reducing the need to backtrack between lines.

```rust
// BAD: wrapped at the column limit, so sentences start in the middle of a line.
// SQLite has no CASCADE clause (the builder omits it) and enforces foreign
// keys, so the caller must list children before parents and drop triggers
// first. The trigger file is executed before this macro runs.

// GOOD: one sentence per line, with the long one broken at its comma.
// SQLite has no CASCADE clause (the builder omits it) and enforces foreign keys,
// so the caller must list children before parents and drop triggers first.
// The trigger file is executed before this macro runs.
```

## Keeping Comments True

A comment that contradicts the code is worse than no comment, because the reader believes it.

A comment that states a limit, such as "this cannot be moved" or "this is never null", is read as a fact, and nobody tests it again.
Check the limit when you write the comment, and say what enforces it: a test, an assertion, a type, or the version of a library.
If you cannot check it, write what you observed, not what is impossible.

```cpp
/// BAD: no way to check whether this is still the case.
/// std::move_only_function cannot be used here.

/// GOOD: the condition can be checked again when the toolchain changes.
/// std::move_only_function is the C++23 equivalent but is not yet available in libc++19.
```

When you change behavior, check the comments that describe it and fix the ones that described the old behavior.
This includes the doc comment of the function or type, not only the lines next to the change.

```cpp
/// BAD: was true until the call became asynchronous.
/// Blocks until the query has stopped.
void stop(QueryId queryId);
```

Keep an existing comment when you edit the surrounding code, and its statement still holds.
Do not delete it as cleanup.

When code and comment disagree, and you do not know why, find out which one is wrong before you change either.

Keep a comment next to the code that it explains.
A reason written forty lines away isn't read, and it isn't updated when the code changes.

## Optional: Automatic Review in Claude Code

`scripts/comment_review_hook.py` is a Stop hook for Claude Code.
It is not active unless you enable it, and other tools such as Codex or Cursor do not run it.

When an agent finishes a change, the hook counts the comment lines that the change adds:

- **5 to 49 lines:** the hook asks the agent to reread its own comments against `AGENTS.md` and to fix them.
- **50 lines or more:** the hook asks the agent to start a subagent.
  The subagent reads this guide and the diff without the context of the agent that wrote the code, and returns its findings.
  The agent applies them and reports what it changed.
  Expect one to two extra minutes for such a change.

The hook looks only at uncommitted changes to files that the agent edited in the session.
It does not see a file that was changed by a shell command, or work that was committed before the agent stopped.

To enable it, add this to `.claude/settings.local.json` in your checkout:

```json
{
  "hooks": {
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "f=\"$CLAUDE_PROJECT_DIR/scripts/comment_review_hook.py\"; [ -f \"$f\" ] && python3 \"$f\" || true"
          }
        ]
      }
    ]
  }
}
```

The command does nothing on a branch that does not have the script yet.
Three environment variables change its behavior:

- `NES_COMMENT_REVIEW_MIN`: the number of lines from which the agent rereads its comments (default 5).
- `NES_COMMENT_REVIEW_SUBAGENT_MIN`: the number of lines from which a subagent reviews them (default 50).
- `NES_COMMENT_REVIEW_MODEL`: the model of the subagent (default `sonnet`).
