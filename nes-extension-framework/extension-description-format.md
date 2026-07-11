# Extension Description File Format

This document describes the format for `.extension.md` files used with the `/generate-extension` slash command.
Write one of these files to describe what you want to build, then run:

```
/generate-extension path/to/your-description.extension.md
```

The agent reads your description and `EXTENSION_REQUIREMENTS.md`, then generates all boilerplate placeholder
files in the correct locations. You only need to fill in the `// TODO:` sections.

See `examples/` in this directory for complete examples.

---

## File format

Files must use the `.extension.md` suffix. The frontmatter is YAML; the rest is Markdown.

```markdown
---
type: source | sink | function | operator     # required
name: PascalCaseName                          # required — becomes the C++ class name
plugin: true | false                          # sources/sinks only; true → nes-plugins/, false → built-in
stateful: true | false                        # operators only; true → OperatorHandler stub generated
category: ArithmeticalFunctions               # functions only — subdirectory under Functions/
---

## Description
Free text. What this extension does. Used as file-level doc comments.

## Config Parameters
# Sources and sinks only. Omit section entirely if the extension has no configuration.
- PARAM_NAME (type, required): description
- OTHER_PARAM (type, optional, default=value): description

## Arguments
# Functions only.
- children: N
- input types: numeric | any | float | bool | string
- output type: same as input | bool | float | string

## Behavior
# Optional but improves TODO comment quality.
# Describe what each lifecycle method should do.

## Examples
# Optional. Used as inline doc comments on execute()/fillTupleBuffer().
- input → expected output
```

---

## Field reference

### `type` (required)

| Value | What it generates |
|-------|------------------|
| `function` | 4 files: logical + physical, header + implementation |
| `operator` | 6 files: logical + physical + lowering rule, header + implementation each |
| `source` | 2–3 files: header + implementation (+ CMakeLists.txt for plugins) |
| `sink` | 2–3 files: header + implementation (+ CMakeLists.txt for plugins) |

### `name` (required)

PascalCase identifier used as the C++ class name and registry key. Examples: `KafkaSource`, `Sigmoid`, `Deduplication`.

For sources: the registry key (string used in SQL `TYPE` clause) is the `name` as-is.
For functions: the SQL function name is `name` (case-insensitive match).

### `plugin` (sources and sinks only, default: `true`)

- `true`: files go in `nes-plugins/Sources/<Name>/` or `nes-plugins/Sinks/<Name>/`. Requires adding an `activate_optional_plugin()` call (printed in Next Steps).
- `false`: files go into the built-in `nes-sources/` or `nes-sinks/` directories.

### `stateful` (operators only, default: `false`)

When `true`, an `OperatorHandler` subclass stub is generated and wired into the lowering rule. Use this for operators that maintain state across records (e.g. joins, deduplication, windowed aggregation).

### `category` (functions only, default: `ArithmeticalFunctions`)

Determines the subdirectory under `Functions/` in both `nes-logical-operators` and `nes-physical-operators`.
Existing categories: `ArithmeticalFunctions`, `BooleanFunctions`, `ComparisonFunctions`.
You may create a new category by specifying a new name — the directories will be created.

---

## Config Parameters syntax

```
- PARAM_NAME (type, required): description
- PARAM_NAME (type, optional, default=value): description
```

Supported types:

| Type string | C++ type | Notes |
|-------------|----------|-------|
| `string` | `std::string` | Most common |
| `int` | `int` | Integer values |
| `bool` | `bool` | `true`/`false` |
| `size` | `size_t` | Byte/count sizes |
| `float` | `float` | Floating-point |

Required params use `std::nullopt` as default (agent enforces presence at validation time).
Optional params use `std::optional<T>{defaultValue}`.

---

## What the agent generates

For every method that needs implementation, the agent inserts a `// TODO:` comment explaining
what should go there, derived from your `## Behavior` and `## Examples` sections.

For functions and operators, `Reflector<T>` / `Unreflector<T>` serialization structs are always
generated — these are required by the NebulaStream serialization system and must not be removed.

The agent also generates a placeholder system test (`.test` file) in `nes-systests/`, using the
templates in §5 of `EXTENSION_REQUIREMENTS.md`. If `## Examples` is provided for a function, the
expected output values are filled in automatically.

The agent prints (but does not write) the CMake lines you need to add to existing files.

---

## What the agent does NOT generate

- Modifications to existing files: CMake additions are printed for you to apply manually
- Third-party library integration: `#include` directives for external libraries must be added by hand
