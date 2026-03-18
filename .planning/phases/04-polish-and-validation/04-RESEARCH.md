# Phase 4: Polish and Validation - Research

**Researched:** 2026-03-15
**Domain:** Undo/redo, ANTLR SQL validation, dark mode, keyboard shortcuts, topology validation
**Confidence:** HIGH (undo, dark mode, shortcuts, topology validation), MEDIUM (ANTLR port)

## Summary

Phase 4 adds production-polish features to the NES topology editor: undo/redo via store snapshots, keyboard shortcuts, real-time topology validation with canvas error overlays, ANTLR-based SQL validation in the query panel Monaco editor, resizable query panel (already implemented), sidebar toggle behavior, and dark mode.

The undo/redo system uses **zundo** -- the standard zustand undo middleware (<700B). It wraps the store `create()` call and provides `undo()`, `redo()`, `canUndo`, `canRedo` via a parallel temporal store. The `partialize` option excludes UI-only state (selectedNodeId, clipboard, etc.) from history tracking. A critical interaction: Monaco editors have their own internal undo -- the app must detect focus and only fire store-level undo when focus is outside Monaco instances.

The ANTLR SQL validation port is the highest-risk item. The NES grammar (`nes-sql-parser/AntlrSQL.g4`) has C++ action blocks (`@lexer::members`, `@parser::members`) containing an `isValidDecimal()` semantic predicate and two boolean flags. These must be rewritten in TypeScript and placed in lexer/parser base classes. The grammar is then compiled with **antlr-ng** CLI to produce TypeScript lexer/parser that runs in-browser via the **antlr4ng** runtime. Errors feed into Monaco's `setModelMarkers()` API -- the same pattern already used in YamlOverlay.

**Primary recommendation:** Use zundo for undo/redo, antlr4ng + antlr-ng for SQL validation, Tailwind `darkMode: 'class'` for dark mode. Topology validation is pure functions extending `lib/validation.ts`.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Undo/redo via full store snapshots on each mutation, unlimited history depth
- Keyboard only (Ctrl+Z/Ctrl+Shift+Z) -- no toolbar undo/redo buttons
- Monaco handles its own Ctrl+Z when focused; store undo only fires outside Monaco editors
- YAML edits create undo snapshots like any other mutation
- Topology validation errors: orphan nodes, missing required config, empty schema on logical sources, physical sources without linked logical source
- Validation is warnings only -- does not block YAML export
- Warning/error icon overlaid on affected nodes with hover tooltip
- Validation runs real-time on every store change
- Full port of AntlrSQL.g4 to JavaScript target using antlr4ng runtime
- SQL errors shown as Monaco error markers (red squiggly underlines)
- SQL validation on debounced keystroke (~500ms)
- Dark mode: full app scope, toolbar sun/moon toggle, localStorage persistence, Tailwind dark: variant
- Right sidebar: toggleable (open/close), NOT resizable. Opens on node click, closes on Escape
- Query panel (bottom): resizable by dragging top edge (already implemented)
- YAML overlay: fixed-size modal (already implemented)
- Keyboard shortcuts: Ctrl+Z undo, Ctrl+Shift+Z redo, Delete removes selected, Ctrl+C/V copy-paste (already works), Escape closes overlay/sidebar

### Claude's Discretion
- Undo snapshot granularity (batch rapid changes or snapshot each?)
- Exact validation error messages and icon design
- ANTLR grammar port approach for C++ action blocks
- Dark mode color palette choices
- Query panel resize constraints (min/max height)
- Autocomplete implementation approach (if feasible)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| PROP-06 | Topology validation highlights errors on the canvas (orphan nodes, missing config) | Topology validation functions extend `lib/validation.ts`; error icons overlaid on custom node components via data props |
| QURY-04 | ANTLR-based SQL validation using the NES grammar | antlr-ng CLI + antlr4ng runtime to port AntlrSQL.g4; Monaco `setModelMarkers()` for inline errors |
| UIPL-01 | Undo/redo for all editing operations | zundo `temporal` middleware wrapping zustand store; `partialize` to exclude UI state |
| UIPL-02 | Keyboard shortcuts (Ctrl+Z undo, Delete, Ctrl+S export) | Global keydown handler in App.tsx; focus detection to avoid conflict with Monaco |
| UIPL-03 | Resizable panels (canvas, properties, YAML preview) | Query panel resize already implemented; sidebar toggle (open/close) via uiSlice; no new resize needed |
| UIPL-04 | Dark mode | Tailwind `darkMode: 'class'` in config; `dark` class on `<html>`; localStorage persistence |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| zundo | ^2.3 | Undo/redo middleware for zustand | Only maintained zustand undo middleware; <700B; used by pmndrs ecosystem |
| antlr4ng | ^3.0 | ANTLR4 TypeScript runtime for browser | Active maintainer; ES2022+; no node/browser differentiation; 9-35% faster than alternatives |
| antlr-ng | latest | CLI tool to generate TS parser/lexer from .g4 | Companion to antlr4ng runtime; generates TypeScript directly |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| lucide-react | (existing) | Icons for validation warnings, dark mode toggle | Sun/Moon icons, AlertTriangle/AlertCircle for validation |

### Already Installed (No New Dependencies)
| Library | Purpose |
|---------|---------|
| zustand ^5 | Store -- zundo v2 is compatible |
| @monaco-editor/react ^4.7 | SQL editor -- setModelMarkers for validation |
| tailwindcss ^3.4 | Dark mode via `dark:` variant |

**Installation:**
```bash
cd nes-topology-editor
npm install zundo antlr4ng
npm install -D antlr-ng
```

## Architecture Patterns

### Recommended Project Structure
```
src/
├── lib/
│   ├── validation.ts          # EXTEND: add topology validation functions
│   ├── sql/                   # NEW: ANTLR-generated parser + validation wrapper
│   │   ├── AntlrSQLLexer.ts   # Generated by antlr-ng
│   │   ├── AntlrSQLParser.ts  # Generated by antlr-ng
│   │   ├── AntlrSQLListener.ts # Generated
│   │   ├── NesLexerBase.ts    # Custom base class with isValidDecimal()
│   │   ├── NesParserBase.ts   # Custom base class with parser member flags
│   │   └── validateSql.ts     # Wrapper: parse SQL string -> error[] with line/col
│   └── ...
├── store/
│   ├── index.ts               # MODIFY: wrap create() with temporal middleware
│   ├── topologySlice.ts       # No change
│   ├── uiSlice.ts             # EXTEND: sidebarOpen state, dark mode
│   └── querySlice.ts          # No change
├── components/
│   ├── Canvas/
│   │   ├── WorkerNode.tsx     # MODIFY: add validation error icon overlay
│   │   ├── SourceNode.tsx     # MODIFY: add validation error icon overlay
│   │   └── SinkNode.tsx       # MODIFY: add validation error icon overlay
│   ├── QueryEditor/
│   │   └── QueryPanel.tsx     # MODIFY: add ANTLR validation on onChange
│   └── App.tsx                # MODIFY: global keyboard handler, dark mode class
```

### Pattern 1: Zundo Temporal Middleware
**What:** Wrap zustand store creation with `temporal()` to get automatic undo/redo
**When to use:** Always -- this is the undo/redo implementation

```typescript
// store/index.ts
import { create } from 'zustand';
import { temporal } from 'zundo';

export type StoreState = TopologySlice & UiSlice & QuerySlice;

export const useStore = create<StoreState>()(
  temporal(
    (...args) => ({
      ...createTopologySlice(...args),
      ...createUiSlice(...args),
      ...createQuerySlice(...args),
    }),
    {
      // Only track topology + query data, not UI state
      partialize: (state) => ({
        workers: state.workers,
        logicalSources: state.logicalSources,
        physicalSources: state.physicalSources,
        sinks: state.sinks,
        queries: state.queries,
      }),
      // Optional: batch rapid changes (e.g. debounce within 500ms)
      // equality: (a, b) => JSON.stringify(a) === JSON.stringify(b),
    },
  ),
);

// Access undo/redo:
// useStore.temporal.getState().undo()
// useStore.temporal.getState().redo()
```

### Pattern 2: Focus-Aware Keyboard Shortcuts
**What:** Global keydown handler that checks `document.activeElement` to avoid conflicting with Monaco
**When to use:** For Ctrl+Z/Ctrl+Shift+Z to not fight Monaco's internal undo

```typescript
// In App.tsx useEffect
const handleKeyDown = (e: KeyboardEvent) => {
  // Check if focus is inside a Monaco editor
  const active = document.activeElement;
  const inMonaco = active?.closest('.monaco-editor') !== null;

  if (e.key === 'z' && (e.ctrlKey || e.metaKey)) {
    if (inMonaco) return; // Let Monaco handle its own undo
    e.preventDefault();
    if (e.shiftKey) {
      useStore.temporal.getState().redo();
    } else {
      useStore.temporal.getState().undo();
    }
  }

  if (e.key === 'Escape') {
    // Close YAML overlay first, then sidebar
    const state = useStore.getState();
    if (state.yamlOverlayVisible) {
      state.toggleYamlOverlay();
    } else if (state.selectedNodeId) {
      state.selectNode(null);
    }
  }
};
```

### Pattern 3: Topology Validation as Pure Functions
**What:** Validation functions that take store state and return error objects keyed by node ID
**When to use:** Called via `useMemo` or `useStore` selector on every state change

```typescript
// lib/validation.ts (extend existing file)
export interface ValidationError {
  nodeId: string;
  severity: 'warning' | 'error';
  message: string;
}

export function validateTopology(
  workers: Worker[],
  logicalSources: LogicalSource[],
  physicalSources: PhysicalSource[],
  sinks: Sink[],
): ValidationError[] {
  const errors: ValidationError[] = [];

  // Orphan sources (no hostWorkerId)
  for (const ps of physicalSources) {
    if (!ps.hostWorkerId) {
      errors.push({ nodeId: ps.id, severity: 'warning', message: 'Source not attached to any worker' });
    }
  }

  // Orphan sinks
  for (const sink of sinks) {
    if (!sink.hostWorkerId) {
      errors.push({ nodeId: sink.id, severity: 'warning', message: 'Sink not attached to any worker' });
    }
  }

  // Physical source without linked logical source
  for (const ps of physicalSources) {
    if (!ps.logicalSourceId || !logicalSources.find(ls => ls.id === ps.logicalSourceId)) {
      errors.push({ nodeId: ps.id, severity: 'error', message: 'Physical source has no linked logical source' });
    }
  }

  // Empty schema on logical sources
  for (const ls of logicalSources) {
    if (!ls.schema || ls.schema.length === 0) {
      // Find physical sources that reference this logical source
      for (const ps of physicalSources.filter(p => p.logicalSourceId === ls.id)) {
        errors.push({ nodeId: ps.id, severity: 'warning', message: `Logical source "${ls.name}" has no schema fields` });
      }
    }
  }

  // Missing required config fields (use sourceConfigs.ts FormFieldDef.required)
  // ... check SOURCE_CONFIGS and SINK_CONFIGS against actual config values

  return errors;
}
```

### Pattern 4: ANTLR Grammar Port Strategy
**What:** Port C++ action blocks to TypeScript base classes, strip from grammar, generate with antlr-ng
**When to use:** For the AntlrSQL.g4 JavaScript port

The grammar has these C++ blocks that need porting:

1. **`@lexer::postinclude` / `@parser::postinclude`**: C++ warning suppression pragma -- **remove entirely** (not applicable in TS)

2. **`@parser::members`**: Two boolean flags
   ```typescript
   // NesParserBase.ts
   export class NesParserBase extends Parser {
     SQL_standard_keyword_behavior = false;
     legacy_exponent_literal_as_decimal_enabled = false;
   }
   ```

3. **`@lexer::members`**: `isValidDecimal()` and `isHint()` semantic predicates
   ```typescript
   // NesLexerBase.ts
   export class NesLexerBase extends Lexer {
     isValidDecimal(): boolean {
       const nextChar = this.inputStream.LA(1);
       // Return false if next char is A-Z, 0-9, or underscore
       if ((nextChar >= 65 && nextChar <= 90) ||
           (nextChar >= 48 && nextChar <= 57) ||
           nextChar === 95) {
         return false;
       }
       return true;
     }

     isHint(): boolean {
       return this.inputStream.LA(1) === 43; // '+'
     }
   }
   ```

4. **Grammar modifications**: Replace `@lexer::members`/`@parser::members` with `@lexer::header` and `@parser::header` import statements, and change `{isValidDecimal()}?` to `{this.isValidDecimal()}?` (JS target syntax).

5. **Grammar fix needed**: Line 435 has `IS: 'IS'  'is';` which should be `IS: 'IS' | 'is';` (missing pipe operator -- likely a typo in the original grammar).

### Pattern 5: Dark Mode with Tailwind class Strategy
**What:** Toggle `dark` class on `<html>` element, persist in localStorage
**When to use:** For UIPL-04

```typescript
// tailwind.config.js
export default {
  darkMode: 'class',
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  // ...
};

// In uiSlice or separate hook:
const savedTheme = localStorage.getItem('theme');
const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
const isDark = savedTheme === 'dark' || (!savedTheme && prefersDark);
document.documentElement.classList.toggle('dark', isDark);
```

### Anti-Patterns to Avoid
- **Fighting Monaco undo**: Never intercept Ctrl+Z globally without checking `document.activeElement` -- Monaco editors MUST handle their own undo internally
- **Tracking UI state in undo history**: Undo should only restore data (workers, sources, sinks, queries), NOT UI state (selectedNodeId, sidebarTab, yamlOverlayVisible)
- **Blocking validation**: Topology validation errors must be non-blocking warnings -- NES CLI validates at deploy time
- **Synchronous ANTLR parsing**: Run validation on debounced keystroke (500ms), not on every character
- **Inline dark mode colors**: Use Tailwind `dark:` variants, not runtime style switching -- keeps all theming in CSS

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Undo/redo state management | Custom history stack with past/future arrays | zundo temporal middleware | Handles edge cases (batching, partial tracking, memory limits), <700B |
| SQL parsing | Regex-based SQL validation | antlr4ng with AntlrSQL.g4 | Grammar-accurate validation matching NES server-side parser |
| Dark mode theming | CSS variables toggled by JS | Tailwind `dark:` variant with `darkMode: 'class'` | Zero-runtime CSS, full design-system integration |
| Keyboard shortcut management | Multiple scattered event listeners | Single global handler in App.tsx | Centralized, easy to reason about priority (Monaco focus check) |

**Key insight:** The undo system and ANTLR parser are the two items where custom solutions would be most tempting but most error-prone. Zundo handles zustand integration cleanly; antlr4ng guarantees parser correctness matching the C++ server.

## Common Pitfalls

### Pitfall 1: Undo Restoring Stale Layout Positions
**What goes wrong:** After undo, node positions from old snapshot are restored but auto-layout has since rearranged nodes
**Why it happens:** Position data is included in undo snapshots
**How to avoid:** Call `applyAutoLayout()` after every undo/redo operation. Since auto-layout runs on structural changes anyway, this is consistent.
**Warning signs:** Nodes jump to weird positions after undo

### Pitfall 2: Monaco Undo vs Store Undo Conflict
**What goes wrong:** User presses Ctrl+Z in SQL editor and both Monaco AND the store undo fire
**Why it happens:** Global keydown handler captures the event before Monaco
**How to avoid:** Check `document.activeElement?.closest('.monaco-editor')` -- if true, return early and let Monaco handle it
**Warning signs:** Double-undo behavior, Monaco content out of sync

### Pitfall 3: ANTLR Semantic Predicate Syntax Difference
**What goes wrong:** Grammar compiles but `isValidDecimal()` predicate fails at runtime
**Why it happens:** C++ uses `_input->LA(1)` while TypeScript uses `this.inputStream.LA(1)`. The predicate call syntax also differs: C++ uses `{isValidDecimal()}?` while JS/TS target needs `{this.isValidDecimal()}?`
**How to avoid:** Use base class pattern with correct TypeScript API; test predicate with known decimal/non-decimal inputs
**Warning signs:** Parser accepts invalid tokens or rejects valid ones

### Pitfall 4: ANTLR Grammar Typo on Line 435
**What goes wrong:** `IS: 'IS'  'is';` (missing `|` operator) causes lexer generation error
**Why it happens:** Original grammar has a bug that C++ ANTLR target may handle differently
**How to avoid:** Fix to `IS: 'IS' | 'is';` before generating
**Warning signs:** antlr-ng reports lexer rule error

### Pitfall 5: Dark Mode Flash on Page Load
**What goes wrong:** Page loads in light mode then flashes to dark
**Why it happens:** React renders before useEffect reads localStorage
**How to avoid:** Add inline `<script>` in index.html `<head>` that sets `dark` class before first paint
**Warning signs:** Visible white flash when dark mode is saved

### Pitfall 6: Zustand Temporal with Slice Pattern
**What goes wrong:** `temporal()` middleware doesn't compose with existing slice pattern
**Why it happens:** Slices are merged in the `create()` callback; temporal wraps the entire callback
**How to avoid:** Temporal wraps the outer function that combines slices (shown in Pattern 1 above). The type needs `StoreState` as the generic.
**Warning signs:** TypeScript errors about incompatible slice types

## Code Examples

### ANTLR SQL Validation Wrapper
```typescript
// lib/sql/validateSql.ts
import { CharStream, CommonTokenStream } from 'antlr4ng';
import { AntlrSQLLexer } from './AntlrSQLLexer';
import { AntlrSQLParser } from './AntlrSQLParser';

export interface SqlError {
  line: number;
  column: number;
  message: string;
}

class ErrorCollector {
  errors: SqlError[] = [];

  syntaxError(
    _recognizer: unknown,
    _offendingSymbol: unknown,
    line: number,
    charPositionInLine: number,
    msg: string,
  ): void {
    this.errors.push({ line, column: charPositionInLine, message: msg });
  }
}

export function validateSql(sql: string): SqlError[] {
  const inputStream = CharStream.fromString(sql);
  const lexer = new AntlrSQLLexer(inputStream);
  const tokenStream = new CommonTokenStream(lexer);
  const parser = new AntlrSQLParser(tokenStream);

  const collector = new ErrorCollector();
  lexer.removeErrorListeners();
  parser.removeErrorListeners();
  lexer.addErrorListener(collector);
  parser.addErrorListener(collector);

  parser.singleStatement(); // Entry rule

  return collector.errors;
}
```

### Monaco Marker Integration for SQL
```typescript
// In QueryPanel.tsx onChange handler (debounced)
const handleSqlChange = useCallback((value: string | undefined) => {
  if (!value || !monacoRef.current || !editorRef.current) return;

  const model = editorRef.current.getModel();
  if (!model) return;

  const errors = validateSql(value);
  const markers = errors.map((err) => ({
    severity: monacoRef.current!.MarkerSeverity.Error,
    message: err.message,
    startLineNumber: err.line,
    startColumn: err.column + 1,
    endLineNumber: err.line,
    endColumn: err.column + 2,
  }));

  monacoRef.current.editor.setModelMarkers(model, 'sql-validation', markers);
}, []);
```

### Validation Error Overlay on Nodes
```typescript
// In WorkerNode.tsx -- receive validation errors via node data
const WorkerNode: React.FC<NodeProps> = ({ data }) => {
  const { worker, validationErrors } = data as WorkerNodeData;
  const hasErrors = validationErrors && validationErrors.length > 0;

  return (
    <div className="worker-node">
      <div className="worker-node__label">{worker.host}</div>
      {hasErrors && (
        <div
          className="validation-badge"
          title={validationErrors.map((e: ValidationError) => e.message).join('\n')}
        >
          <AlertTriangle size={14} />
        </div>
      )}
      {/* handles... */}
    </div>
  );
};
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Custom undo arrays | zundo temporal middleware | 2023+ | No hand-rolled history management |
| antlr4ts (abandoned) | antlr4ng v3 | 2024 | Active maintenance, ES2022, faster runtime |
| antlr4 (JS target) | antlr-ng CLI + antlr4ng | 2024-2025 | Native TypeScript generation, no declaration hacks |
| Tailwind darkMode: 'media' | darkMode: 'class' | Tailwind 3.0+ | Manual toggle with localStorage persistence |

**Deprecated/outdated:**
- **antlr4ts**: Abandoned, not compatible with modern ANTLR4. Use antlr4ng instead.
- **antlr4 (official JS package)**: Still maintained but antlr4ng is faster and has native TS types.

## Open Questions

1. **ANTLR Autocomplete Feasibility**
   - What we know: The grammar has enough structure (FROM clause -> relation -> source name) to suggest logical source names after FROM and sink names after INTO
   - What's unclear: Whether antlr4ng exposes parser state (expected tokens) in a way that Monaco's completion provider can use
   - Recommendation: Ship validation first (must-have). Attempt autocomplete as a stretch goal -- if parser state inspection is too complex, provide keyword-only completions via a static list

2. **Undo Snapshot Granularity**
   - What we know: Zundo snapshots on every `set()` call by default. Rapid typing in sidebar fields could create many snapshots.
   - What's unclear: Whether users expect character-level undo in form fields or "form submission" level undo
   - Recommendation: Use zundo's `equality` option with a shallow comparison -- identical state transitions are deduplicated. For finer control, use `wrapTemporal` option to skip certain setters, or batch via `limit` (100 snapshots is plenty).

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Vitest 3.x with jsdom environment |
| Config file | `nes-topology-editor/vitest.config.ts` |
| Quick run command | `cd nes-topology-editor && npx vitest run --reporter=verbose` |
| Full suite command | `cd nes-topology-editor && npx vitest run` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| UIPL-01 | Undo/redo restores previous store state | unit | `npx vitest run src/__tests__/undo.test.ts -x` | Wave 0 |
| UIPL-01 | Undo partializes (excludes UI state) | unit | `npx vitest run src/__tests__/undo.test.ts -x` | Wave 0 |
| UIPL-02 | Ctrl+Z triggers undo, Ctrl+Shift+Z triggers redo | unit | `npx vitest run src/__tests__/shortcuts.test.ts -x` | Wave 0 |
| UIPL-02 | Escape closes overlay then sidebar | unit | `npx vitest run src/__tests__/shortcuts.test.ts -x` | Wave 0 |
| PROP-06 | Orphan nodes detected by validation | unit | `npx vitest run src/__tests__/topology-validation.test.ts -x` | Wave 0 |
| PROP-06 | Missing config detected by validation | unit | `npx vitest run src/__tests__/topology-validation.test.ts -x` | Wave 0 |
| QURY-04 | Valid SQL passes ANTLR validation | unit | `npx vitest run src/__tests__/sql-validation.test.ts -x` | Wave 0 |
| QURY-04 | Invalid SQL returns errors with line/col | unit | `npx vitest run src/__tests__/sql-validation.test.ts -x` | Wave 0 |
| UIPL-03 | Sidebar toggles open/close | unit | `npx vitest run src/__tests__/sidebar-toggle.test.ts -x` | Wave 0 |
| UIPL-04 | Dark mode toggle sets class and localStorage | unit | `npx vitest run src/__tests__/dark-mode.test.ts -x` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cd nes-topology-editor && npx vitest run --reporter=verbose`
- **Per wave merge:** `cd nes-topology-editor && npx vitest run`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `src/__tests__/undo.test.ts` -- covers UIPL-01
- [ ] `src/__tests__/shortcuts.test.ts` -- covers UIPL-02
- [ ] `src/__tests__/topology-validation.test.ts` -- covers PROP-06
- [ ] `src/__tests__/sql-validation.test.ts` -- covers QURY-04
- [ ] `src/__tests__/sidebar-toggle.test.ts` -- covers UIPL-03
- [ ] `src/__tests__/dark-mode.test.ts` -- covers UIPL-04
- [ ] `src/lib/sql/` directory with generated ANTLR parser files
- [ ] npm install: `zundo antlr4ng` (runtime), `antlr-ng` (dev)

## Sources

### Primary (HIGH confidence)
- [zundo GitHub](https://github.com/charkour/zundo) - API, partialize, equality, temporal usage pattern
- [antlr4ng GitHub](https://github.com/mike-lischke/antlr4ng) - v3.0.16, TypeScript runtime, browser support
- [antlr-ng getting started](https://www.antlr-ng.org/getting-started.html) - CLI installation and usage
- [Tailwind v3 dark mode docs](https://v3.tailwindcss.com/docs/dark-mode) - class strategy, toggle pattern
- Existing codebase: `nes-sql-parser/AntlrSQL.g4` - grammar analysis for C++ action block porting

### Secondary (MEDIUM confidence)
- [ANTLR4 semantic predicates docs](https://github.com/antlr/antlr4/blob/master/doc/predicates.md) - predicate syntax across targets
- [Monaco + ANTLR integration](https://medium.com/better-programming/create-a-custom-web-editor-using-typescript-react-antlr-and-monaco-editor-bcfc7554e446) - setModelMarkers pattern with ANTLR errors

### Tertiary (LOW confidence)
- ANTLR autocomplete via parser state -- no concrete implementation examples found; needs spike

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - zundo and antlr4ng are well-documented with clear APIs
- Architecture: HIGH - patterns follow existing codebase conventions (zustand slices, Monaco markers, pure validation functions)
- Pitfalls: HIGH - undo/Monaco conflict and ANTLR port risks are well-understood from grammar analysis
- ANTLR autocomplete: LOW - feasibility unclear without implementation spike

**Research date:** 2026-03-15
**Valid until:** 2026-04-15 (stable ecosystem, grammar is fixed)
