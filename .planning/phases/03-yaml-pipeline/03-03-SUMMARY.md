---
phase: 03-yaml-pipeline
plan: 03
subsystem: ui
tags: [react-flow, dagre, auto-layout, canvas]

# Dependency graph
requires:
  - phase: 01-interactive-canvas
    provides: Canvas component, layout.ts dagre helper, store topology slice
provides:
  - applyAutoLayout helper for automatic dagre layout on structural changes
  - Canvas with no free node positioning (always auto-layouted)
affects: [03-yaml-pipeline, 04-validation]

# Tech tracking
tech-stack:
  added: []
  patterns: [store.getState() for imperative layout triggers, structural fingerprint for change detection]

key-files:
  created: [nes-topology-editor/src/lib/autoLayout.ts]
  modified: [nes-topology-editor/src/components/Canvas/Canvas.tsx, nes-topology-editor/src/components/Toolbar/Toolbar.tsx]

key-decisions:
  - "Structural fingerprint (node IDs + connections) detects topology changes without position sensitivity"
  - "applyAutoLayout is imperative (useStore.getState) for use in event handlers and effects"

patterns-established:
  - "Auto-layout: all node positioning is dagre-computed, no manual drag positioning"
  - "Structural fingerprint pattern for detecting graph topology changes vs position-only changes"

requirements-completed: [OUTP-04]

# Metrics
duration: 3min
completed: 2026-03-14
---

# Phase 03 Plan 03: Auto-Layout Summary

**Automatic dagre LR layout on every structural change with free positioning disabled**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-14T22:22:24Z
- **Completed:** 2026-03-14T22:25:47Z
- **Tasks:** 1
- **Files modified:** 7

## Accomplishments
- Created applyAutoLayout helper that reads store state, computes dagre LR layout, writes positions back
- Modified Canvas to call applyAutoLayout after every structural change (connect, delete, paste)
- Disabled free node positioning -- dragging to empty space snaps back to layout position
- Removed manual Auto Layout button from Toolbar (layout is now always automatic)
- Added structural fingerprint useEffect for detecting topology changes (covers YAML import)

## Task Commits

Each task was committed atomically:

1. **Task 1: Create applyAutoLayout helper and integrate into Canvas** - `851f286244` (feat)

## Files Created/Modified
- `nes-topology-editor/src/lib/autoLayout.ts` - Standalone helper: reads store, computes dagre layout, writes positions
- `nes-topology-editor/src/components/Canvas/Canvas.tsx` - Auto-layout on structural changes, no free positioning
- `nes-topology-editor/src/components/Toolbar/Toolbar.tsx` - Removed Auto Layout button, added applyAutoLayout after addWorker
- `nes-topology-editor/src/__tests__/canvas.test.tsx` - Added autoLayout mock
- `nes-topology-editor/src/__tests__/connections.test.tsx` - Added autoLayout mock
- `nes-topology-editor/src/__tests__/delete.test.tsx` - Added autoLayout mock
- `nes-topology-editor/src/__tests__/palette-dnd.test.tsx` - Updated to reflect removed Auto Layout button

## Decisions Made
- Structural fingerprint uses node IDs + connection topology (not positions) to detect when re-layout is needed
- applyAutoLayout uses useStore.getState() (imperative, no hooks) so it can be called from event handlers
- Direction is LR (left-to-right) matching existing Toolbar pattern

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added autoLayout mock to 4 test files**
- **Found during:** Task 1 (verification)
- **Issue:** Canvas now imports applyAutoLayout which calls useStore.getState() and dagre -- tests that import Canvas needed the mock
- **Fix:** Added `vi.mock('../lib/autoLayout', ...)` to canvas.test.tsx, connections.test.tsx, delete.test.tsx, palette-dnd.test.tsx
- **Files modified:** 4 test files
- **Verification:** All 143 tests pass
- **Committed in:** 851f286244 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Test mock additions necessary for test suite to pass with new import. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Auto-layout foundation ready for YAML import (Plan 02's replaceTopology will trigger layout via structural fingerprint)
- All existing tests pass (143/143)

---
*Phase: 03-yaml-pipeline*
*Completed: 2026-03-14*
