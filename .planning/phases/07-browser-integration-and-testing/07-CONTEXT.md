# Phase 7: Browser Integration and Testing - Context

**Gathered:** 2026-03-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Wire the WASM `validateTopology()` module into the topology editor with Web Worker execution, debounced validation on topology changes, and error display. The UI validates topologies using the WASM module in real time, with results shown in a status bar and below the YAML export panel. CI is deferred.

</domain>

<decisions>
## Implementation Decisions

### Validation replacement strategy
- WASM replaces the JS topology validation (orphan nodes, missing config checks stay as lightweight JS per-node badges)
- Keep the JS ANTLR SQL parser for inline squiggly lines in the SQL editor — WASM handles full topology-level semantic validation separately
- Two validation layers: fast JS structural checks (per-node badges) + WASM semantic validation (global status indicator)

### Error display
- Global status indicator in a bottom status bar on the canvas
- Checkmark icon when valid, warning icon when errors exist
- Hover over warning icon to see the error message
- Full error text also displayed below the YAML export panel
- Spinner icon in status bar while validation is running (Web Worker processing)
- Per-node error badges (AlertTriangle) kept for structural issues only (orphan nodes, missing required fields)

### Validation trigger
- Debounce all topology changes (node moves, property edits, query changes, connections, YAML imports)
- Any change starts a debounce timer; validate when changes settle
- First validation runs automatically once WASM module finishes loading

### WASM loading
- Load silently in background on app startup — UI fully usable immediately
- Status bar shows subtle loading indicator until WASM is ready
- Retry 3 times on load failure, then show error in status bar
- If WASM unavailable, structural JS validation still works (per-node badges) — user can build topologies without semantic validation

### Web Worker
- Validation runs in a dedicated Web Worker to avoid blocking UI (dragging, typing, panel switching)
- Worker loads the WASM module and exposes validateTopology() via message passing

### CI pipeline
- Deferred — not included in this phase
- Focus is on browser integration (Web Worker, error display, debouncing)

### Claude's Discretion
- Debounce duration (tune based on validation speed)
- Web Worker message protocol design
- Vite configuration for WASM asset handling
- Status bar component styling and positioning
- Error display formatting below YAML panel
- How to bundle/serve the WASM binary (import vs public asset)

</decisions>

<specifics>
## Specific Ideas

- "Just have some small checkmark in the bottom or a warning symbol that if you hover over will tell you whats wrong"
- "Display the full error message under the YAML export"
- Keep JS ANTLR parser because "if we want the squiggly lines we need it in JS"
- Retry 3 times on WASM load failure, then show error

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `components/Canvas/{WorkerNode,SourceNode,SinkNode}.tsx`: Per-node error badges with AlertTriangle + tooltip (keep for structural validation)
- `lib/validation.ts`: Structural topology validation (orphan nodes, missing configs, cycle detection) — keep this
- `lib/sql/validateSql.ts` + ANTLR grammar: SQL syntax validation for squiggly lines — keep this
- `store/topologySlice.ts`: Zustand state slice that triggers YAML regeneration on changes — hook validation here
- `components/YamlOverlay/YamlOverlay.tsx`: YAML preview panel — error display goes below this

### Established Patterns
- Zustand for state management with slices (topology, query, UI)
- React Flow (@xyflow/react v12.10) for canvas
- Tailwind CSS for styling
- Vitest for unit tests, Playwright for E2E

### Integration Points
- WASM binary: `nes-topology-editor/wasm/build/nes-validator/nes-validator-wasm.{wasm,cjs}` (408KB gzipped)
- WASM interface: `validateTopology(yamlString: string): string` via Embind (empty = valid, non-empty = error)
- WASM built with SINGLE_FILE=1 (base64-embedded binary in .cjs file)
- UI source on `topology-editor-ui` branch
- New: Web Worker file, status bar component, validation hook/store integration

</code_context>

<deferred>
## Deferred Ideas

- Structured error objects with categories, severity, and location info — requires NES-side error handling changes (v1.2)
- Error location mapping to specific UI nodes/fields — deferred to v1.2 per REQUIREMENTS.md
- CI pipeline for WASM build + tests — deferred from this phase, add when integration stabilizes
- Playwright E2E tests for browser-level validation testing — add with CI

</deferred>

---

*Phase: 07-browser-integration-and-testing*
*Context gathered: 2026-03-15*
