# NebulaStream Extension Framework

This directory contains the code-generation framework for NebulaStream extensions.
It is a first-class module in the `nes-*` family, toggled by the CMake option
`NES_ENABLE_EXTENSION_FRAMEWORK` (default: ON).

To disable:
```bash
cmake -DNES_ENABLE_EXTENSION_FRAMEWORK=OFF ...
```

---

## What it does

A developer who wants to add a new source, sink, function, or operator writes a short
`.extension.md` description file and runs a Claude Code slash command:

```
/generate-extension path/to/my-thing.extension.md
```

The agent reads the description and `EXTENSION_REQUIREMENTS.md`, then generates:
- All required C++ placeholder files (headers + implementations) in the correct locations
- A placeholder system test (`.test` file) in `nes-systests/`
- A printout of the CMake lines to add to existing `CMakeLists.txt` files

The developer then fills in the `// TODO:` sections.

---

## Contents

| File / Directory | Purpose |
|------------------|---------|
| `EXTENSION_REQUIREMENTS.md` | Agent-facing reference: interfaces, skeletons, systest templates |
| `extension-description-format.md` | User-facing: how to write a description file |
| `examples/` | Four complete example description files (one per extension type) |
| `CMakeLists.txt` | CMake toggle (no C++ targets) |

---

## Quick start

1. Copy an example from `examples/` as a starting point:
   ```bash
   cp nes-extension-framework/examples/kafka-source.extension.md my-source.extension.md
   ```

2. Edit the description file — fill in `name`, `type`, config params, behavior notes.

3. Run the generator:
   ```
   /generate-extension my-source.extension.md
   ```

4. Fill in the `// TODO:` sections in the generated files.

5. Add the CMake lines printed by the generator to the relevant `CMakeLists.txt` files.

---

## Extension types

| Type | Description files | Generated files |
|------|------------------|-----------------|
| `source` | `examples/kafka-source.extension.md` | 2–3 files in `nes-plugins/Sources/<Name>/` or `nes-sources/` |
| `sink` | `examples/s3-sink.extension.md` | 2–3 files in `nes-plugins/Sinks/<Name>/` or `nes-sinks/` |
| `function` | `examples/sigmoid.extension.md` | 4 files in `nes-logical-operators/` and `nes-physical-operators/` |
| `operator` | `examples/deduplication.extension.md` | 6 files across `nes-logical-operators/`, `nes-physical-operators/`, `nes-query-compiler/` |
