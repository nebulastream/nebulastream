---
phase: 04-polish-and-validation
plan: 03
subsystem: ui
tags: [antlr, sql, parser, monaco, validation, antlr4ng]

requires:
  - phase: 02-configuration-panels
    provides: QueryPanel component with Monaco SQL editor
provides:
  - ANTLR TypeScript lexer/parser generated from NES SQL grammar
  - validateSql function returning errors with line/column positions
  - Real-time SQL validation with Monaco error markers (500ms debounce)
affects: []

tech-stack:
  added: [antlr4ng, antlr-ng]
  patterns: [ANTLR combined grammar with TypeScript target, debounced Monaco setModelMarkers]

key-files:
  created:
    - nes-topology-editor/src/lib/sql/AntlrSQL.g4
    - nes-topology-editor/src/lib/sql/AntlrSQLLexer.ts
    - nes-topology-editor/src/lib/sql/AntlrSQLParser.ts
    - nes-topology-editor/src/lib/sql/AntlrSQLListener.ts
    - nes-topology-editor/src/lib/sql/NesLexerBase.ts
    - nes-topology-editor/src/lib/sql/NesParserBase.ts
    - nes-topology-editor/src/lib/sql/validateSql.ts
    - nes-topology-editor/src/__tests__/sql-validation.test.ts
  modified:
    - nes-topology-editor/src/components/QueryEditor/QueryPanel.tsx
    - nes-topology-editor/package.json

key-decisions:
  - "Combined ANTLR grammar (not split lexer/parser) for simpler generation with antlr-ng"
  - "antlr-ng with -D language=TypeScript flag for native TypeScript generation"
  - "Lexer extends NesLexerBase via post-generation edit (superClass only applies to parser in combined grammar)"
  - "@ts-nocheck on generated files to suppress unused variable warnings from strict tsconfig"

patterns-established:
  - "ANTLR grammar regeneration: npx antlr-ng -D language=TypeScript -o . AntlrSQL.g4"
  - "SQL validation via validateSql() returning SqlError[] with line/column/message"

requirements-completed: [QURY-04]

duration: 9min
completed: 2026-03-15
---

# Phase 04 Plan 03: SQL Validation Summary

**NES ANTLR SQL grammar ported to TypeScript with real-time Monaco editor validation via 500ms debounced markers**

## Performance

- **Duration:** 9 min
- **Started:** 2026-03-15T00:14:58Z
- **Completed:** 2026-03-15T00:24:00Z
- **Tasks:** 2
- **Files modified:** 11

## Accomplishments
- Ported NES ANTLR SQL grammar from C++ to TypeScript using antlr-ng code generator
- Created validateSql wrapper that parses SQL and returns errors with line/column positions
- Wired debounced validation into Monaco editor showing red squiggly underlines for invalid SQL
- All 126 project tests pass including 6 new SQL validation tests

## Task Commits

Each task was committed atomically:

1. **Task 1: Port ANTLR grammar and generate TypeScript parser** - `fa8c796297` (feat)
2. **Task 2: Wire SQL validation into Monaco editor with debounced markers** - `cc8d85b77b` (feat)

## Files Created/Modified
- `src/lib/sql/AntlrSQL.g4` - Modified NES grammar with TypeScript-compatible semantic predicates
- `src/lib/sql/AntlrSQLLexer.ts` - Generated ANTLR lexer extending NesLexerBase
- `src/lib/sql/AntlrSQLParser.ts` - Generated ANTLR parser extending NesParserBase
- `src/lib/sql/AntlrSQLListener.ts` - Generated parse tree listener
- `src/lib/sql/NesLexerBase.ts` - Base lexer with isValidDecimal and isHint predicates
- `src/lib/sql/NesParserBase.ts` - Base parser with SQL behavior flags
- `src/lib/sql/validateSql.ts` - SQL validation wrapper function
- `src/__tests__/sql-validation.test.ts` - Tests for valid/invalid SQL validation
- `src/components/QueryEditor/QueryPanel.tsx` - Added debounced SQL validation with Monaco markers

## Decisions Made
- Used combined ANTLR grammar instead of split lexer/parser to avoid implicit token errors
- Used antlr-ng `-D language=TypeScript` flag (not the default Java target)
- Manually patched generated lexer to extend NesLexerBase (combined grammar superClass only applies to parser)
- Added `@ts-nocheck` to generated files rather than relaxing project-wide tsconfig strictness
- Fixed IS token typo in original grammar (`'IS'  'is'` changed to `'IS' | 'is'`)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] antlr-ng generates Java by default, not TypeScript**
- **Found during:** Task 1 (grammar generation)
- **Issue:** Running antlr-ng without language flag produced .java files
- **Fix:** Added `-D language=TypeScript` flag to generation command
- **Files modified:** Generated output files
- **Verification:** TypeScript .ts files generated correctly

**2. [Rule 3 - Blocking] Split grammar approach fails with implicit token errors**
- **Found during:** Task 1 (grammar generation)
- **Issue:** Splitting into separate lexer/parser .g4 files caused 80+ "cannot create implicit token" errors for literal characters
- **Fix:** Reverted to combined grammar with `options { superClass=NesParserBase; }`
- **Files modified:** AntlrSQL.g4
- **Verification:** Combined grammar generates without errors

**3. [Rule 1 - Bug] Generated lexer extends antlr.Lexer instead of NesLexerBase**
- **Found during:** Task 1 (grammar generation)
- **Issue:** Combined grammar superClass only applies to parser; lexer still extends base Lexer
- **Fix:** Post-generation edit to change `extends antlr.Lexer` to `extends NesLexerBase`
- **Files modified:** AntlrSQLLexer.ts
- **Verification:** isValidDecimal/isHint semantic predicates work at runtime; tests pass

**4. [Rule 3 - Blocking] Generated files have unused variable warnings under strict tsconfig**
- **Found during:** Task 2 (type checking)
- **Issue:** noUnusedLocals/noUnusedParameters flags cause TS6133 errors in generated code
- **Fix:** Added `// @ts-nocheck` comment to all 3 generated files
- **Files modified:** AntlrSQLLexer.ts, AntlrSQLParser.ts, AntlrSQLListener.ts
- **Verification:** `npx tsc -b --noEmit` passes cleanly

---

**Total deviations:** 4 auto-fixed (1 bug, 3 blocking)
**Impact on plan:** All auto-fixes necessary for ANTLR TypeScript generation to work. No scope creep.

## Issues Encountered
None beyond the deviations documented above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- SQL validation fully operational for the query editor
- Grammar can be regenerated if NES SQL grammar changes upstream

---
*Phase: 04-polish-and-validation*
*Completed: 2026-03-15*
