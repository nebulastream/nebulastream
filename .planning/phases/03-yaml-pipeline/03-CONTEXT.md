# Phase 3: YAML Pipeline - Context

**Gathered:** 2026-03-14
**Status:** Ready for planning

<domain>
## Phase Boundary

Users can see their topology as valid YAML in real time and edit it bidirectionally. The YAML editor is a toggleable overlay on the canvas -- no separate import/export dialogs. Covers OUTP-01 through OUTP-04. OUTP-05 (templates) has been dropped. Also includes a cross-cutting change: always auto-layout (remove free node positioning).

</domain>

<decisions>
## Implementation Decisions

### YAML preview placement
- Toggleable floating overlay panel on the canvas, activated via a toolbar button
- Uses Monaco editor with YAML syntax highlighting
- Panel floats over the canvas area (does not take permanent layout space)
- Toggle button in the toolbar shows/hides the overlay

### Two-way sync (editable YAML)
- YAML editor is fully editable -- not read-only
- Canvas-to-YAML: any topology change regenerates YAML in the editor
- YAML-to-canvas: user edits in the YAML editor sync back to the canvas with live debounce (~500ms after typing stops)
- If YAML is invalid during editing, canvas stays at the last valid state
- Parse errors shown as inline Monaco error markers (red squiggly underlines at error locations)

### Import/export model
- No explicit import or export dialogs
- The YAML editor IS the import/export mechanism
- To "export": user copies YAML from the editor (select-all, copy)
- To "import": user pastes YAML into the editor, which syncs to canvas via the live debounce
- No download button -- copy-paste only
- No unsaved-changes warnings -- the YAML always reflects the current topology state

### Always auto-layout (cross-cutting)
- Remove free node positioning entirely from the canvas
- The only allowed drag operations are: drag-to-connect and drag-to-reassign (source/sink between workers)
- Auto-layout (dagre) runs automatically on every structural graph change (add/remove/connect/disconnect nodes or edges)
- This also applies to import: pasting YAML auto-layouts the resulting topology
- This is a change that affects Phase 1's canvas behavior retroactively

### Query serialization format
- Use the future NES query object format: `queries: [{query: '...', name: '...'}]`
- Always use array format, even for a single query
- Each query object includes `query` (SQL string) and `name` (string) -- no other fields for now
- Forward-compatible with the upcoming NES branch that supports named queries with optimizer options
- Current nes-cli won't understand this format, but that's acceptable

### Dropped requirements
- OUTP-05 (template topologies) dropped entirely -- users can paste YAML examples from NES docs if needed

### Claude's Discretion
- YAML overlay dimensions, position, and resize behavior
- Debounce timing for YAML-to-canvas sync
- Monaco editor configuration (theme, minimap, word wrap)
- How to handle the overlay toggle animation (if any)
- YAML key ordering in generated output

</decisions>

<specifics>
## Specific Ideas

- The YAML editor should feel like a "live view" of the topology, not a separate import/export workflow
- Auto-layout on every change keeps the canvas clean without manual positioning
- The future query format (`queries: [{query, name}]`) prepares for the upcoming NES branch where queries become richer objects with optimizer options

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `QueryPanel.tsx`: Collapsible bottom panel with Monaco editor -- pattern reference for overlay component
- `@monaco-editor/react`: Already installed, supports YAML syntax highlighting
- `lib/layout.ts`: `getLayoutedElements()` using dagre -- will be called on every structural change
- `lib/types.ts`: All domain types (Worker, LogicalSource, PhysicalSource, Sink, Query) map directly to YAML format
- `lib/clipboard.ts`: Serialization pattern (store data -> transportable format) -- reference for YAML export logic

### Established Patterns
- Zustand slice pattern (topologySlice + uiSlice + querySlice) -- overlay visibility state goes in uiSlice
- Store actions dispatch structural changes -- auto-layout can hook into these
- Monaco editor usage in QueryPanel -- same integration pattern for YAML editor
- Inline styles for panel components (consistent with QueryPanel, Toolbar patterns)

### Integration Points
- `App.tsx`: Overlay component renders inside the canvas area (positioned absolutely)
- `Toolbar.tsx`: Add YAML toggle button
- `topologySlice.ts` + `querySlice.ts`: All state needed for YAML generation already exists
- `uiSlice.ts`: Add `yamlOverlayVisible` state
- Auto-layout hook needs to wrap or subscribe to all structural store actions

</code_context>

<deferred>
## Deferred Ideas

None -- discussion stayed within phase scope

</deferred>

---

*Phase: 03-yaml-pipeline*
*Context gathered: 2026-03-14*
