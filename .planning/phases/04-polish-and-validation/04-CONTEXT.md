# Phase 4: Polish and Validation - Context

**Gathered:** 2026-03-14
**Status:** Ready for planning

<domain>
## Phase Boundary

Make the editor production-ready with undo/redo, keyboard shortcuts, real-time topology validation, ANTLR-based SQL validation, resizable query panel, dark mode, and sidebar toggle behavior. Covers PROP-06, QURY-04, UIPL-01, UIPL-02, UIPL-03, UIPL-04.

</domain>

<decisions>
## Implementation Decisions

### Undo/redo (UIPL-01)
- Full store snapshots on each mutation — simple, reliable, covers all operations
- Unlimited history depth (topology data is small)
- All changes are undoable regardless of source (canvas, sidebar, YAML editor)
- Keyboard only — no toolbar undo/redo buttons, just Ctrl+Z / Ctrl+Shift+Z
- When Monaco editor has focus (YAML or SQL), Monaco handles its own Ctrl+Z internally — store-level undo only fires when focus is outside Monaco editors
- YAML edits that debounce back to the store create undo snapshots like any other store mutation

### Topology validation (PROP-06)
- Errors detected: orphan nodes (unattached sources/sinks), missing required config fields, empty schema on logical sources, physical sources without a linked logical source
- Display: warning/error icon overlaid on affected nodes, hover tooltip shows error message
- Validation runs in real-time on every store change (lightweight for topology-sized data)
- Errors are warnings only — do not block YAML export/copy. NES CLI catches real problems at deploy time

### ANTLR SQL validation (QURY-04)
- Full port of `nes-sql-parser/AntlrSQL.g4` to JavaScript target — strip C++ action blocks, compile via antlr4-tool, use antlr4 JS runtime in browser
- Research must investigate: what do the C++ `@lexer::members` and `@parser::members` blocks actually do? Port the logic or determine if it can be omitted
- Errors shown as Monaco error markers (red squiggly underlines) — same pattern as YAML overlay
- Validation runs on debounced keystroke (~500ms after typing stops), same as YAML overlay
- **Nice-to-have: context-aware autocomplete** — SQL keywords + logical source names and sink names from the store at appropriate grammar positions. Researcher should investigate feasibility of using ANTLR parser state for next-token suggestions. If too complex, ship without it — validation is the must-have

### Dark mode (UIPL-04)
- Full app scope — canvas, toolbar, sidebar, query panel, YAML overlay all switch
- Toolbar sun/moon icon toggle, persisted in localStorage
- Tailwind `dark:` variant for implementation

### Panel behavior (UIPL-03)
- Right sidebar: toggleable (open/close), NOT resizable. Opens automatically on node click, closes on Escape
- Query panel (bottom): resizable by dragging its top edge
- YAML overlay: fixed-size modal (already implemented as blocking overlay)

### Keyboard shortcuts (UIPL-02)
- Minimal set: Ctrl+Z undo, Ctrl+Shift+Z redo, Delete removes selected, Ctrl+C/V copy-paste (already works), Escape closes overlay/sidebar

### Claude's Discretion
- Undo snapshot granularity (batch rapid changes or snapshot each?)
- Exact validation error messages and icon design
- ANTLR grammar port approach for the C++ action blocks
- Dark mode color palette choices
- Query panel resize constraints (min/max height)
- Autocomplete implementation approach (if feasible)

</decisions>

<specifics>
## Specific Ideas

- Store-level undo must not fight with Monaco's built-in undo — focus detection is key
- Validation icons on nodes similar to how IDEs show error/warning icons in the gutter
- ANTLR autocomplete with actual logical source and sink names at appropriate SQL positions (e.g., after FROM suggest logical source names, after INTO suggest sink names) — aspirational but valuable
- The sidebar currently has no close mechanism besides clicking another node — Escape close is a new behavior

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `lib/validation.ts`: Already has `isValidConnectionType` and `wouldCreateCycle` — topology validation can extend this module
- `lib/sourceConfigs.ts`: Has `FormFieldDef` with `required` flags — can drive missing-config validation
- Monaco editor pattern: YAML overlay already does `editor.setModelMarkers()` for error display — SQL editor can reuse same approach
- `store/uiSlice.ts`: Has `selectedNodeId` and `yamlOverlayVisible` — sidebar toggle and Escape handling can extend this slice
- Tailwind v3 with `dark:` variant support — dark mode implementation is straightforward

### Established Patterns
- Zustand slice pattern — undo/redo middleware wraps the store creation
- Echo prevention ref pattern (YamlOverlay) — may be needed for undo interactions
- Debounce pattern (YamlOverlay 500ms) — reuse for SQL validation
- `applyAutoLayout()` imperative call pattern — can be called after undo/redo restores a snapshot

### Integration Points
- `store/index.ts`: Undo middleware wraps the `create()` call
- `App.tsx`: Global keyboard shortcut handler at app level
- Custom node components (WorkerNode, SourceNode, SinkNode): Add validation error icon overlay
- `QueryPanel.tsx`: Add ANTLR validation + error markers to Monaco onChange
- `App.css` / Tailwind config: Dark mode class strategy

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 04-polish-and-validation*
*Context gathered: 2026-03-14*
