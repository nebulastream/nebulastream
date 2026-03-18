# Phase 2: Configuration Panels - Context

**Gathered:** 2026-03-14
**Status:** Ready for planning

<domain>
## Phase Boundary

Users can configure every aspect of their topology nodes -- worker properties, source/sink settings, logical source schemas, and SQL queries. Covers PROP-01 through PROP-05, SRCE-01 through SRCE-03, QURY-01 through QURY-03. Topology validation (PROP-06), ANTLR SQL validation (QURY-04), and undo/redo belong to later phases.

</domain>

<decisions>
## Implementation Decisions

### Panel layout and interaction
- Right sidebar for property editing and logical source management
- Click a node to open its panel; click empty canvas or another node to close/switch
- Type-specific panel content: worker panel, source panel, and sink panel each have tailored layouts
- Panel is resizable by dragging the left edge (pulling UIPL-03 forward from Phase 4 for this panel)

### Sidebar organization
- Two tabs in the right sidebar: **Properties** (node config) and **Sources** (logical source management)
- Logical source management is NOT part of the property panel -- it is a separate, dedicated pane
- Physical source properties only allow selecting from existing logical sources (not creating them inline)

### Logical source management
- Dedicated "Sources" tab lists all logical sources with create/edit/delete
- Schema builder is an inline table: rows of (field name + type dropdown), add/remove with +/- buttons
- Type dropdown offers NES native types: INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64, FLOAT32, FLOAT64, BOOLEAN, CHAR, TEXT
- Delete policy: delete-with-orphan -- deleting a logical source leaves physical sources with a broken reference that shows a warning badge

### SQL query editor
- Query list and editor live in a **bottom panel** below the canvas (not in the sidebar)
- Monaco editor (VS Code editor component) for SQL editing with syntax highlighting
- Queries support user-defined names, displayed in query tabs
- Query names are included in YAML export (NES will be made to ignore them), enabling round-trip: export with names, import back and preserve names
- Unnamed queries are also supported -- displayed with *(unnamed)* italic placeholder in the tab list
- Clicking the tab allows optionally adding a name

### Source/sink config forms
- Fixed forms per source/sink type (not dynamic key-value pairs)
- Form fields are derived from the NES C++ Config classes for each physical source/sink type -- names and descriptions must match
- This derivation is a manual process but must be documented so it can be repeated when new source/sink types are added to NES
- All source types discovered from the NES codebase (researcher will inventory the Config classes)
- All sink types discovered from the NES codebase (same approach)
- Physical source links to logical source via an autocomplete text field (type-ahead filtering)

### Claude's Discretion
- Exact panel width and resize constraints
- Form validation rules and error message styling
- Tab styling and active state indicators
- Bottom panel height and resize behavior
- Monaco editor configuration (theme, minimap, line numbers)
- Empty state messages for each panel

</decisions>

<specifics>
## Specific Ideas

- Logical source management must be clearly separated from physical source properties -- logical sources are a first-class concept, not a sub-property of physical sources
- The mockup (mockup.png) shows the intended app layout with canvas occupying the main area
- Source/sink form fields should feel familiar to NES users -- match the Config class naming exactly
- Query naming enables better UX now and forward-compatibility if NES adds named query support later

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `uiSlice.ts`: `selectedNodeId` already tracked -- ready to trigger panel open/close
- `topologySlice.ts`: Full CRUD actions for workers, physicalSources, sinks, logicalSources
- `lib/types.ts`: All domain types defined (Worker, LogicalSource, PhysicalSource, Sink, SchemaField)
- `PhysicalSource.sourceConfig` and `parserConfig` are `Record<string, string>` -- fixed forms will map known keys to typed inputs
- `Sink.config` is `Record<string, string>` -- same approach

### Established Patterns
- Zustand slice pattern for state management (topologySlice + uiSlice)
- React Flow with custom node components (WorkerNode, SourceNode, SinkNode)
- Toolbar component for top-level actions
- Tailwind CSS v3 for styling

### Integration Points
- App.tsx currently renders `<Toolbar />` + `<Canvas />` -- sidebar and bottom panel need to be added to this layout
- Node click events on Canvas need to dispatch `selectNode()` to open the property panel
- Logical source CRUD actions already exist in topologySlice but have no UI yet
- Query model needs to be added to types and store (not yet defined)

</code_context>

<deferred>
## Deferred Ideas

None -- discussion stayed within phase scope

</deferred>

---

*Phase: 02-configuration-panels*
*Context gathered: 2026-03-14*
