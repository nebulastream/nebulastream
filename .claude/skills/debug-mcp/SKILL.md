---
name: debug-mcp
description: Record a systest crash with Undo live-record and expose the recording via UDB's MCP time-travel debugger server. Use this when the user wants to debug a failing/crashing systest, set up the UDB MCP server, or start a debugging session for a test case.
---

The user wants to start a UDB MCP debugging session for a systest.

Run the following bash command from the project root, substituting any parameters the user specified:

```bash
scripts/debug-mcp.sh -t <TEST> [OPTIONS]
```

**Parameters to extract from the user's message:**

| User says | Flag |
|-----------|------|
| test file / test case | `-t` (required, relative to `nes-systests/`, e.g. `operator/join/JoinWithVarSized.test:01`) |
| execution mode / compiler / interpreter | `--mode COMPILER` or `--mode INTERPRETER` (default: INTERPRETER) |
| port | `--port N` (default: 8000) |
| worker threads | `--workers N` (default: 4) |
| re-use existing recording / skip recording | `--skip-record` |
| specific recording file | `--recording path/to/file.undo` |
| build directory | `--build-dir dir` (default: cmake-build-debug) |

**Execution steps:**

1. Run the `scripts/debug-mcp.sh` command with the Bash tool. This will:
   - Record the test crash using `live-record` (unless `--skip-record` is set)
   - Start a Docker container named `nes-udb-mcp` with UDB loaded at the crash point
   - Wait until the MCP server at `http://localhost:<port>/sse` is reachable
   - Print the `claude mcp add` command to connect

2. After the script succeeds, tell the user:
   - The MCP server URL
   - How to add it: `! claude mcp add --transport sse nes-udb-mcp http://localhost:<port>/sse`
   - That they can now ask questions about the crash

**Notes:**
- The script must be run from the project root (the directory containing `cmake-build-debug/`, `nes-systests/`, etc.)
- Recording files are named `<test_path>_<MODE>_<N>w.undo` and stored in the project root
- The `undo-addons` Docker volume caches the UDB `explain` addon between runs
- If the test passes without crashing, no recording is saved and the script exits with a message
- To stop the server later: `docker stop nes-udb-mcp`
