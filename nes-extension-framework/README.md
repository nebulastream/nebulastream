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
`.extension.md` description file, then generates all C++ boilerplate in one step —
either with the standalone Python CLI or with the Claude Code slash command.

### CLI generator (no Claude required)

```bash
# From the project root:
python3 nes-extension-framework/generate_extension.py my-thing.extension.md

# Preview what would be written without touching the filesystem:
python3 nes-extension-framework/generate_extension.py my-thing.extension.md --dry-run

# Overwrite files that already exist:
python3 nes-extension-framework/generate_extension.py my-thing.extension.md --force
```

The script deterministically generates all placeholder files from the description,
no network access or Claude session required.

### Claude Code slash command

For a more interactive experience where Claude fills in more context-aware TODOs:

```
/generate-extension path/to/my-thing.extension.md
```

Both approaches generate:
- All required C++ placeholder files (headers + implementations) in `nes-plugins/<Type>/<Name>/`
- A placeholder system test (`.test` file) in `nes-systests/`
- A printout of the single `activate_optional_plugin(...)` line to add to `nes-plugins/CMakeLists.txt`

The developer then fills in the `// TODO:` sections.

---

## Contents

| File / Directory | Purpose |
|------------------|---------|
| `generate_extension.py` | Standalone CLI: deterministic C++ boilerplate generator |
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

3. Run the generator (CLI or slash command):
   ```bash
   python3 nes-extension-framework/generate_extension.py my-source.extension.md
   # or: /generate-extension my-source.extension.md
   ```

4. Fill in the `// TODO:` sections in the generated files.

5. Add the CMake lines printed by the generator to the relevant `CMakeLists.txt` files.

---

## Extension types

| Type | Example description | Generated files |
|------|---------------------|-----------------|
| `source` | `examples/kafka-source.extension.md` | 3 files in `nes-plugins/Sources/<Name>/` + systest |
| `sink` | `examples/s3-sink.extension.md` | 3 files in `nes-plugins/Sinks/<Name>/` + systest |
| `function` | `examples/sigmoid.extension.md` | 5 files in `nes-plugins/Functions/<Name>/` + systest |
| `operator` | `examples/deduplication.extension.md` | 7 files in `nes-plugins/Operators/<Name>/` + systest |
