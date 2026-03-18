---
phase: 04-polish-and-validation
plan: 02
subsystem: ui
tags: [react, validation, topology, lucide-react]

requires:
  - phase: 01-interactive-canvas
    provides: Canvas node components (WorkerNode, SourceNode, SinkNode)
  - phase: 02-configuration-panels
    provides: Source/sink config definitions (SOURCE_CONFIGS, SINK_CONFIGS)
provides:
  - validateTopology pure function detecting orphan nodes, missing config, empty schemas
  - ValidationError type and selectValidationErrorMap selector
  - Visual error/warning overlays on all canvas node types
affects: [04-polish-and-validation]

tech-stack:
  added: []
  patterns: [validation-error-overlay, pure-validation-function]

key-files:
  created: [nes-topology-editor/src/__tests__/topology-validation.test.ts]
  modified: [nes-topology-editor/src/lib/validation.ts, nes-topology-editor/src/store/selectors.ts, nes-topology-editor/src/components/Canvas/Canvas.tsx, nes-topology-editor/src/components/Canvas/WorkerNode.tsx, nes-topology-editor/src/components/Canvas/SourceNode.tsx, nes-topology-editor/src/components/Canvas/SinkNode.tsx]

key-decisions:
  - "Validation errors injected into node data via Canvas.tsx useMemo, not via store selectors directly"
  - "AlertTriangle icon from lucide-react with title attribute tooltip (no custom tooltip library)"
  - "Orange for warnings, red for errors; highest severity shown if both present"

patterns-established:
  - "Validation overlay pattern: pure validateTopology -> selectValidationErrorMap -> inject into node data -> render icon in node component"

requirements-completed: [PROP-06]

duration: 4min
completed: 2026-03-15
---

# Phase 04 Plan 02: Topology Validation Summary

**Pure validateTopology function with real-time warning/error icon overlays on canvas nodes using AlertTriangle icons**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-15T00:15:00Z
- **Completed:** 2026-03-15T00:19:00Z
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments
- Pure `validateTopology` function detecting 5 topology issue types (orphan nodes, missing logical source, empty schema, missing required config)
- 8 unit tests covering all validation scenarios including valid topology edge case
- AlertTriangle icon overlays on WorkerNode, SourceNode, and SinkNode with hover tooltips
- Validation is non-blocking -- YAML export continues to work regardless of errors

## Task Commits

Each task was committed atomically:

1. **Task 1: Topology validation pure functions (TDD RED)** - `97bf3014ce` (test)
2. **Task 1: Topology validation pure functions (TDD GREEN)** - `2a3cfd274b` (feat)
3. **Task 2: Validation error overlays on canvas nodes** - `0905613d17` (feat)

## Files Created/Modified
- `src/lib/validation.ts` - Added ValidationError interface and validateTopology pure function
- `src/__tests__/topology-validation.test.ts` - 8 tests for topology validation
- `src/store/selectors.ts` - Added selectValidationErrorMap selector
- `src/components/Canvas/Canvas.tsx` - Injects validationErrors into node data
- `src/components/Canvas/WorkerNode.tsx` - Renders AlertTriangle for validation errors
- `src/components/Canvas/SourceNode.tsx` - Renders AlertTriangle for validation errors
- `src/components/Canvas/SinkNode.tsx` - Renders AlertTriangle for validation errors
- `src/__tests__/canvas.test.tsx` - Updated mock with selectValidationErrorMap
- `src/__tests__/connections.test.tsx` - Updated mock with selectValidationErrorMap
- `src/__tests__/delete.test.tsx` - Updated mock with selectValidationErrorMap

## Decisions Made
- Validation errors injected into node data via Canvas.tsx useMemo, keeping node components simple consumers
- Used title attribute for tooltip (simple, no library), consistent with lightweight approach
- Orange (#f59e0b) for warnings, red (#ef4444) for errors, matching existing color usage

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Updated test mocks for selectValidationErrorMap**
- **Found during:** Task 2 (validation overlays)
- **Issue:** Existing Canvas test files mock `store/selectors` but didn't include new `selectValidationErrorMap` export, causing 19 test failures
- **Fix:** Added `selectValidationErrorMap: () => new Map()` to mocks in canvas.test.tsx, connections.test.tsx, delete.test.tsx, palette-dnd.test.tsx
- **Files modified:** 4 test files
- **Verification:** All 158 tests pass
- **Committed in:** 0905613d17 (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Auto-fix necessary for test compatibility. No scope creep.

## Issues Encountered
- Pre-existing TypeScript errors in ANTLR-generated files (AntlrSQLLexer.ts, AntlrSQLParser.ts) and QueryPanel.tsx -- these are out of scope and not caused by this plan's changes

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Validation foundation complete for any future validation rules
- ValidateTopology can be extended with additional checks without UI changes

---
*Phase: 04-polish-and-validation*
*Completed: 2026-03-15*
