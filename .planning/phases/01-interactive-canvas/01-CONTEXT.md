# Phase 1: Interactive Canvas - Context

**Gathered:** 2026-03-13
**Status:** Ready for planning

<domain>
## Phase Boundary

Users can visually build a NES topology by placing workers, sources, and sinks on a canvas and connecting them. Covers CANV-01 through CANV-12. Property editing, SQL queries, YAML output, and polish features belong to later phases.

</domain>

<decisions>
## Implementation Decisions

### Source/sink attachment model
- Sources and sinks are **separate small nodes** connected to their host worker via edges (not nested inside or docked on workers)
- This lets them spread out naturally on the canvas and avoids React Flow parent-child bugs
- Dragging a source/sink from one worker's edge to another worker reassigns the host
- Source/sink **creation** happens via the property panel "+" button with a form (Phase 2 concern) — Phase 1 just needs the canvas representation and drag-to-reassign

### Node visual design
- **Workers:** Large nodes displaying only the host address (e.g., "worker-1:9090"). All other details in property panel (Phase 2)
- **Sources:** Small color-coded nodes — color represents their logical source. Icon indicates physical type (e.g., Generator, CSV)
- **Sinks:** Small nodes in a single uniform color. Icon indicates sink type (e.g., File, Void, Print)
- All three node types are visually distinct (size + color + icon scheme)

### Connection creation
- **Worker-to-worker:** Standard React Flow handle-to-handle dragging (drag from output handle to input handle)
- **Source/sink-to-worker:** Connected via edges; drag-to-reassign between workers
- Topology must be a **DAG** — edge validation prevents cycles
- Invalid connection attempts show a brief toast/tooltip ("Cannot connect: would create a cycle")

### Worker creation
- "+" button on canvas creates a new worker with auto-generated defaults ("worker-1:9090", "worker-2:9090", etc.)
- User can rename later via property panel (Phase 2)

### Claude's Discretion
- "+" button placement (top toolbar vs floating)
- Auto-layout direction (top-to-bottom vs left-to-right)
- Source/sink dock side relative to workers
- Worker node overflow handling when many sources/sinks are attached
- Exact icon set for physical source/sink types
- Grid snapping granularity
- Minimap styling and position
- Copy/paste behavior details

</decisions>

<specifics>
## Specific Ideas

- Sources should be color-coded by logical source to visually group them — this is an important UX signal
- Sinks are all one color — they don't have a logical source equivalent to group by
- Physical types (Generator, CSV, File, Void, Print) should be distinguished by icons on the small nodes
- Workers should be noticeably larger than source/sink nodes
- The mockup (mockup.png) shows the full app layout — the canvas occupies the top-right area with a minimap showing node connections

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- No existing frontend code — this is a greenfield React + TypeScript project in `nes-topology-editor/`
- YAML topology format documented in `docs/nebulastream-frontend.md` — defines workers, logical sources, physical sources, sinks, queries

### Established Patterns
- NES YAML format: workers have `host`, `grpc`, `capacity`, `downstream[]`; physical sources have `logical`, `host`, `type`, `source_config`, `parser_config`; sinks have `name`, `host`, `schema`, `type`, `config`
- Workers can have multiple downstream targets (DAG structure)
- Sources reference a logical source by name; physical sources are typed (Generator, CSV, etc.)

### Integration Points
- New module at `nes-topology-editor/` inside the NES monorepo
- React Flow library for the node graph canvas
- Data model must map to the YAML topology format for Phase 3 export

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 01-interactive-canvas*
*Context gathered: 2026-03-13*
