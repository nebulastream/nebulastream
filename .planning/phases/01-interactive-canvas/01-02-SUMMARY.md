---
phase: 01-interactive-canvas
plan: 02
subsystem: ui
tags: [react, reactflow, custom-nodes, drag-drop, canvas, minimap, lucide-react]

# Dependency graph
requires:
  - phase: 01-interactive-canvas/01
    provides: Zustand store with domain types, CRUD actions, and React Flow selectors
provides:
  - Three custom React Flow node components (WorkerNode, SourceNode, SinkNode)
  - Canvas with ReactFlow, grid snap, minimap, and controls
  - Palette sidebar with HTML drag-and-drop to canvas
  - Toolbar with Add Worker button
  - App shell layout (palette + toolbar + canvas)
  - 21 behavioral tests for CANV-01, CANV-02, CANV-07, CANV-10
affects: [01-03, 01-04]

# Tech tracking
tech-stack:
  added: []
  patterns: [custom-node-react-memo, html-drag-drop-api-dataTransfer, vi-hoisted-mock-pattern]

key-files:
  created:
    - nes-topology-editor/src/components/Canvas/WorkerNode.tsx
    - nes-topology-editor/src/components/Canvas/SourceNode.tsx
    - nes-topology-editor/src/components/Canvas/SinkNode.tsx
    - nes-topology-editor/src/components/Canvas/nodeTypes.ts
    - nes-topology-editor/src/components/Canvas/Canvas.tsx
    - nes-topology-editor/src/components/Palette/Palette.tsx
    - nes-topology-editor/src/components/Toolbar/Toolbar.tsx
    - nes-topology-editor/src/styles/nodes.css
    - nes-topology-editor/src/__tests__/palette-dnd.test.tsx
    - nes-topology-editor/src/__tests__/nodes.test.tsx
    - nes-topology-editor/src/__tests__/canvas.test.tsx
    - nes-topology-editor/src/lib/validation.ts
  modified:
    - nes-topology-editor/src/App.tsx
    - nes-topology-editor/src/App.css

key-decisions:
  - "vi.hoisted() pattern for test mocks to avoid zustand useSyncExternalStore infinite loop with React 19"
  - "Custom nodes wrapped in React.memo() at module scope per ReactFlow anti-pattern guidance"
  - "onNodeDragStop for position sync instead of per-frame onNodesChange to avoid store thrashing"

patterns-established:
  - "vi.hoisted mock pattern: use vi.hoisted() for stable references in vi.mock factories when testing zustand stores with React 19"
  - "Custom node module-scope pattern: all custom node components defined at module scope, not inside other components"
  - "HTML DnD API pattern: palette items set dataTransfer with application/reactflow MIME type, canvas reads on drop"

requirements-completed: [CANV-01, CANV-02, CANV-03, CANV-07, CANV-09, CANV-10]

# Metrics
duration: 12min
completed: 2026-03-13
---

# Phase 01 Plan 02: Visual Canvas with Custom Nodes, Palette, and Toolbar Summary

**React Flow canvas with three visually distinct custom nodes (worker/source/sink), drag-and-drop palette, grid snap, minimap, and 21 behavioral tests**

## Performance

- **Duration:** 12 min
- **Started:** 2026-03-13T22:58:53Z
- **Completed:** 2026-03-13T23:11:19Z
- **Tasks:** 3
- **Files modified:** 14

## Accomplishments
- Built three custom React Flow node types: WorkerNode (large, slate-blue, 4 handles), SourceNode (small, color-coded, type icon), SinkNode (small, amber/orange, type icon)
- Created Canvas component with ReactFlow wrapper featuring grid snap (20px), minimap, controls, and HTML drag-and-drop support
- Implemented Palette sidebar with draggable Worker/Source/Sink items using HTML Drag and Drop API dataTransfer
- Added Toolbar with "Add Worker" button and disabled "Auto Layout" placeholder
- Wrote 21 behavioral tests covering CANV-01/02 (palette DnD), CANV-07 (visual distinctness), CANV-10 (minimap/grid)
- All 65 tests pass (21 new + 44 existing)

## Task Commits

Each task was committed atomically:

1. **Task 1: Custom node components and node type registry** - `d735ac8dd9` (feat)
2. **Task 2: Canvas, Palette, Toolbar, and App shell** - `17f4673590` (feat)
3. **Task 3: Behavioral tests for palette, nodes, and canvas** - `ac58c3bed3` (test)

## Files Created/Modified
- `nes-topology-editor/src/components/Canvas/WorkerNode.tsx` - Large custom node with host address and 4 connection handles
- `nes-topology-editor/src/components/Canvas/SourceNode.tsx` - Small color-coded node with type-based lucide icon
- `nes-topology-editor/src/components/Canvas/SinkNode.tsx` - Small amber/orange node with type-based lucide icon
- `nes-topology-editor/src/components/Canvas/nodeTypes.ts` - Registry mapping worker/source/sink to components
- `nes-topology-editor/src/components/Canvas/Canvas.tsx` - ReactFlow wrapper with grid, minimap, DnD, position sync
- `nes-topology-editor/src/components/Palette/Palette.tsx` - Sidebar with draggable node templates
- `nes-topology-editor/src/components/Toolbar/Toolbar.tsx` - Add Worker button and Auto Layout placeholder
- `nes-topology-editor/src/styles/nodes.css` - Distinct styles for worker/source/sink nodes
- `nes-topology-editor/src/App.tsx` - Updated layout with palette sidebar + toolbar + canvas
- `nes-topology-editor/src/App.css` - Full layout styles for palette, toolbar, canvas
- `nes-topology-editor/src/lib/validation.ts` - Stub for pre-existing test (isValidConnectionType, wouldCreateCycle)
- `nes-topology-editor/src/__tests__/palette-dnd.test.tsx` - 6 tests for drag-drop palette behavior
- `nes-topology-editor/src/__tests__/nodes.test.tsx` - 11 tests for node rendering and visual distinctness
- `nes-topology-editor/src/__tests__/canvas.test.tsx` - 5 tests for canvas features (minimap, grid, snap)

## Decisions Made
- Used `vi.hoisted()` pattern for test mocks to avoid React 19 + Zustand `useSyncExternalStore` infinite loop when mocking stores
- All custom nodes wrapped in `React.memo()` at module scope per ReactFlow anti-pattern guidance
- Position sync uses `onNodeDragStop` (not per-frame `onNodesChange`) to avoid store write thrashing
- Created `validation.ts` implementation (not just stub) since pre-existing tests required real functions

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed OnNodeDrag type import**
- **Found during:** Task 2 (Canvas component)
- **Issue:** ReactFlow v12 exports `OnNodeDrag` not `NodeDragHandler`
- **Fix:** Changed import from `NodeDragHandler` to `OnNodeDrag`
- **Files modified:** Canvas.tsx
- **Verification:** Build succeeds

**2. [Rule 3 - Blocking] Created validation.ts for pre-existing test**
- **Found during:** Task 1 (build verification)
- **Issue:** Pre-existing `validation.test.ts` imports from `../lib/validation` which didn't exist, blocking TypeScript build
- **Fix:** Implemented `isValidConnectionType` and `wouldCreateCycle` functions (needed for Plan 04 anyway)
- **Files modified:** src/lib/validation.ts
- **Verification:** Build succeeds, all 15 validation tests pass

**3. [Rule 1 - Bug] Fixed test mock paths for zustand store**
- **Found during:** Task 3 (behavioral tests)
- **Issue:** `vi.mock('../../store')` resolved incorrectly from `src/__tests__/` directory; needed `../store`
- **Fix:** Corrected mock paths and used `vi.hoisted()` for stable references
- **Files modified:** palette-dnd.test.tsx, canvas.test.tsx
- **Verification:** All 21 behavioral tests pass

---

**Total deviations:** 3 auto-fixed (2 bugs, 1 blocking)
**Impact on plan:** All fixes necessary for build/test correctness. Validation.ts implementation benefits Plan 04.

## Issues Encountered
- React 19's strict `useSyncExternalStore` behavior causes infinite re-render loops when zustand store mocks return unstable references. Resolved by using `vi.hoisted()` pattern with plain function mocks that bypass zustand entirely in tests.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Canvas with palette and toolbar ready for Plan 03 (edge connections, configuration panels)
- All node types render with correct handles for Plan 04 (connection validation, auto-layout)
- validation.ts already implemented for Plan 04 connection logic

---
*Phase: 01-interactive-canvas*
*Completed: 2026-03-13*

## Self-Check: PASSED
- All 11 key files verified present
- All 3 task commits verified in git history (d735ac8dd9, 17f4673590, ac58c3bed3)
