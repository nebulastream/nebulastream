---
name: clang-tidy-loop
description: Run clang-tidy-diff against origin/main inside a fresh NES Docker container (full cmake build + clang-tidy), auto-apply safe fixes, collect remaining warnings, present them as a numbered list, ask the user which to fix, apply the chosen fixes, run format.sh -i, and repeat until clean. Use this when the user wants to run clang-tidy, clean up lint warnings, or iterate through clang-tidy fixes.
---

The user wants to run the clang-tidy fix-and-format loop on the NES codebase.

## Overview

The loop is:
1. Run a fresh Docker container: cmake configure + build + `clang-tidy-diff` with `-fix` (fixes written in-place via bind mount)
2. Collect warnings that remain in the output as `[<check-name>]` lines
3. Revert known bad auto-fixes (see below)
4. Present remaining warnings as a numbered list, ask user which to fix manually
5. Apply the user's choices
6. Run `scripts/format.sh -i`
7. Repeat from step 1 until clang-tidy produces no `[check-name]` output and format.sh reports "no problems found"

---

## Step 1 — Run clang-tidy in a fresh container

Determine thread count (quarter of the host's logical CPUs):
```bash
THREADS=$(( $(nproc) / 4 ))
```
Also set WORKSPACE_ROOT to the absolute path of the workspace root folder.
Check if there is a ccache volume already mounted, (usually under /ccache) and set CCACHE_DIR accordingly.

Then launch the container:

```bash
    bash -c "
        export LLVM_SYMBOLIZER_PATH=llvm-symbolizer-19 && \
        git config --global --add safe.directory $WORKSPACE_ROOT && \
        cd $WORKSPACE_ROOT && \
        rm -rf build/ && mkdir build && \
        cmake -GNinja -B build -DCMAKE_EXPORT_COMPILE_COMMANDS=ON && \
        cmake --build build -j $THREADS -- -k 0 && \
        git diff -U0 origin/main -- ':!*.inc' \
            | clang-tidy-diff-19.py \
                -clang-tidy-binary clang-tidy-19 \
                -p1 -path build -fix \
                -config-file .clang-tidy \
                -use-color \
                -j $THREADS
    " 2>&1
```

ccache persists across runs via the named volume and is picked up automatically by cmake's `UseCompilerCache.cmake`.

**Why `LLVM_SYMBOLIZER_PATH`**: tells LLVM tools where to find `llvm-symbolizer` so raw memory addresses in diagnostics are resolved to human-readable function names and source locations.

---

## Step 2 — Check for bad auto-fixes

After every clang-tidy run, inspect the diff for these categories of bad auto-fixes and correct them before proceeding.

### A. `readability-convert-member-functions-to-static` on interface/virtual methods

clang-tidy incorrectly adds `static` to methods like `getOutputSchema()`, `dependsOn()`, `requiredBy()`, `apply()` when they implement interface methods.

**Correct fix**:
- In the `.hpp` declaration: leave as non-static, add nothing
- In the `.cpp` definition: add `/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)` on the line immediately before the definition
- Revert any `static` keyword clang-tidy added to these methods

### B. New `"quoted"` includes added by `misc-include-cleaner`

clang-tidy adds missing includes using `"double-quote"` style. This project uses `<angle-bracket>` style exclusively. Run `scripts/format.sh -i` — it will print `Warning: New include with double quotes in ...` for every offending file and line.

**Correct fix**: Convert every newly-added `#include "foo.hpp"` → `#include <foo.hpp>`.

### C. `} // namespace` comment on anonymous namespace closings

If clang-tidy wraps a function in an anonymous namespace, it may emit `} // namespace` as the closing brace. `format.sh` rejects this as an "Illegal comment".

**Correct fix**: Change `} // namespace` → `}` (plain closing brace, no comment).

---

## Step 3 — Collect and present remaining warnings

Parse the clang-tidy output for lines matching `^\[`. Group by file+check and present as a numbered list:

```
Remaining clang-tidy warnings:
1. [bugprone-unchecked-optional-access] file.cpp:42 — optional accessed without check
2. [misc-use-anonymous-namespace] file.cpp:10 — static function should be in anonymous namespace
3. [readability-convert-member-functions-to-static] file.cpp:55 — method can be made static
...
```

Ask: **Which numbers should I fix? (list numbers, "all", or "none")**

---

## Step 4 — Known fix patterns

| Check | Fix |
|-------|-----|
| `misc-use-anonymous-namespace` | Wrap the `static` free function in `namespace { ... }`. Use plain `}` as closing brace (no `// namespace` comment). |
| `bugprone-unchecked-optional-access` | Add `PRECONDITION(opt.has_value(), "...")` before the dereference. Use `/// NOLINT(bugprone-unchecked-optional-access)` only when a check is genuinely impossible. |
| `readability-convert-member-functions-to-static` | Add `/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)` in the `.cpp` before the definition. Never add `static` to interface methods. |
| `misc-include-cleaner` (missing include) | Add the missing `#include <header.hpp>` in angle-bracket style. |
| `misc-include-cleaner` (unused include) | Remove the unused `#include` line. |
| `cppcoreguidelines-pro-type-const-cast` | Refactor to avoid `const_cast`, or add `/// NOLINT(cppcoreguidelines-pro-type-const-cast)`. |

---

## Step 5 — Run format.sh

```bash
scripts/format.sh -i
```

Report output to user:
- `Error:` lines → must fix before proceeding
- `Warning: New include with double quotes in ...` → convert those includes to angle-bracket style
- `format.sh: no problems found` → proceed to next clang-tidy round or declare done

---

## Notes

- The build inside the container is always fresh (`rm -rf build/`); it does not touch `cmake-build-debug/` on the host
- Run the container without `-it` (non-interactive) since this is scripted
- The container runs rootless — no file ownership issues
- `format.sh` warnings about "file has no corresponding header file" for test files and orphaned `.cpp` files are pre-existing and harmless
- Always pass `-i` to `format.sh` for in-place clang-format
