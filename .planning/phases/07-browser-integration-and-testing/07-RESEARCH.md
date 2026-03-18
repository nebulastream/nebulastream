# Phase 7: Browser Integration and Testing - Research

**Researched:** 2026-03-16
**Domain:** WASM/Web Worker integration in Vite+React app, Emscripten browser builds
**Confidence:** HIGH

## Summary

Phase 7 wires the existing WASM `validateTopology()` module into the React topology editor. The WASM module (1.3MB .wasm + 73KB .cjs loader) currently targets Node.js (`-sENVIRONMENT=node`). The first critical step is rebuilding it for browser/worker environments. The module then loads in a Web Worker, receives YAML strings via `postMessage`, and returns validation results. The UI subscribes to Zustand store changes, debounces, generates YAML via existing `storeToYaml()`, sends it to the worker, and displays results in a status bar and below the YAML overlay.

The existing codebase provides strong integration points: Zustand store with slices, `storeToYaml()` for YAML generation, per-node structural validation in `lib/validation.ts`, and the YAML overlay component. The new code needs a validation Web Worker, a validation Zustand slice, a status bar component, and Vite configuration for WASM asset handling.

**Primary recommendation:** Rebuild the WASM with `-sENVIRONMENT=worker -sEXPORT_ES6=1 -sSINGLE_FILE=1 -sMODULARIZE=1`, create a thin Web Worker that loads the module and exposes `validateTopology` via message passing, and integrate via a new `validationSlice` in the Zustand store.

<user_constraints>

## User Constraints (from CONTEXT.md)

### Locked Decisions
- WASM replaces the JS topology validation (orphan nodes, missing config checks stay as lightweight JS per-node badges)
- Keep the JS ANTLR SQL parser for inline squiggly lines in the SQL editor -- WASM handles full topology-level semantic validation separately
- Two validation layers: fast JS structural checks (per-node badges) + WASM semantic validation (global status indicator)
- Global status indicator in a bottom status bar on the canvas
- Checkmark icon when valid, warning icon when errors exist
- Hover over warning icon to see the error message
- Full error text also displayed below the YAML export panel
- Spinner icon in status bar while validation is running (Web Worker processing)
- Per-node error badges (AlertTriangle) kept for structural issues only (orphan nodes, missing required fields)
- Debounce all topology changes (node moves, property edits, query changes, connections, YAML imports)
- Any change starts a debounce timer; validate when changes settle
- First validation runs automatically once WASM module finishes loading
- Load silently in background on app startup -- UI fully usable immediately
- Status bar shows subtle loading indicator until WASM is ready
- Retry 3 times on load failure, then show error in status bar
- If WASM unavailable, structural JS validation still works (per-node badges) -- user can build topologies without semantic validation
- Validation runs in a dedicated Web Worker to avoid blocking UI (dragging, typing, panel switching)
- Worker loads the WASM module and exposes validateTopology() via message passing
- CI pipeline deferred -- not included in this phase

### Claude's Discretion
- Debounce duration (tune based on validation speed)
- Web Worker message protocol design
- Vite configuration for WASM asset handling
- Status bar component styling and positioning
- Error display formatting below YAML panel
- How to bundle/serve the WASM binary (import vs public asset)

### Deferred Ideas (OUT OF SCOPE)
- Structured error objects with categories, severity, and location info -- requires NES-side error handling changes (v1.2)
- Error location mapping to specific UI nodes/fields -- deferred to v1.2 per REQUIREMENTS.md
- CI pipeline for WASM build + tests -- deferred from this phase, add when integration stabilizes
- Playwright E2E tests for browser-level validation testing -- add with CI

</user_constraints>

<phase_requirements>

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| BINT-01 | WASM module loads in the topology editor | WASM rebuild for browser env, Vite config, Web Worker loader with retry logic |
| BINT-02 | Topology YAML validated via WASM module with errors displayed to user | Worker message protocol, validationSlice in Zustand, status bar + YAML panel error display |
| BINT-03 | Validation runs in Web Worker to avoid blocking UI | Dedicated worker file, postMessage protocol, non-blocking async pattern |
| BINT-04 | Validation debounced on topology changes | Zustand subscription to store changes, debounce timer, storeToYaml integration |
| TEST-02 | WASM integration tests (validate known-good and known-bad YAML) | Vitest tests with worker mock or direct WASM loading, reuse test YAML from Node.js tests |
| TEST-03 | CI job builds WASM module and runs tests | Deferred per user decision -- document test commands for future CI wiring |

</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Vite | ^6.0.0 | Build tool + dev server | Already in project, handles WASM + workers natively |
| Zustand | ^5.0.0 | State management | Already in project, add validationSlice |
| React | ^19.0.0 | UI framework | Already in project |
| Emscripten | 4.0 (emsdk) | WASM compiler | Already installed in `wasm/emsdk/` |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| lucide-react | ^0.474.0 | Icons (CheckCircle, AlertTriangle, Loader2) | Status bar icons |
| Vitest | ^3.2.4 | Unit/integration tests | WASM integration tests |
| happy-dom | ^17.6.1 | Test environment | DOM simulation for component tests |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Raw postMessage | Comlink | Adds dependency for minimal API surface (single function) -- not worth it |
| SINGLE_FILE=1 | Separate .wasm asset | SINGLE_FILE embeds wasm as base64, simpler for Worker but ~33% larger. At 1.3MB wasm, base64 is ~1.7MB. Acceptable for this use case and avoids complex asset path resolution in Worker. |
| New npm scripts | Direct emcmake calls | Existing wasm build infrastructure works, just change linker flags |

**No new npm packages needed.**

## Architecture Patterns

### Recommended Project Structure
```
src/
  lib/
    wasmValidation.ts        # Worker message protocol types + helper
  workers/
    validationWorker.ts       # Web Worker: loads WASM, handles messages
  store/
    validationSlice.ts        # Zustand slice: WASM status, validation results, debounce
  components/
    StatusBar/
      StatusBar.tsx           # Bottom status bar with validation state
    YamlOverlay/
      YamlOverlay.tsx         # Existing -- add error display below editor
```

### Pattern 1: WASM Build for Browser Worker

**What:** Rebuild the existing WASM module with browser/worker-compatible Emscripten flags
**When to use:** Required -- current build targets Node.js only

Current linker flags (Node-only):
```
-sMODULARIZE=1 -sEXPORT_ES6=0 -sENVIRONMENT=node -fwasm-exceptions -sALLOW_MEMORY_GROWTH=1 -O2 -sASSERTIONS=0
```

Required linker flags (Worker-compatible):
```
-sMODULARIZE=1 -sEXPORT_ES6=1 -sENVIRONMENT=worker -sSINGLE_FILE=1 -fwasm-exceptions -sALLOW_MEMORY_GROWTH=1 -O2 -sASSERTIONS=0
```

Changes needed in `nes-topology-editor/wasm/nes-validator/CMakeLists.txt`:
```cmake
set_target_properties(nes-validator-wasm PROPERTIES
    SUFFIX ".mjs"  # Was .cjs -- ES6 module needs .mjs
    LINK_FLAGS "-sMODULARIZE=1 -sEXPORT_ES6=1 -sENVIRONMENT=worker -sSINGLE_FILE=1 -fwasm-exceptions -sALLOW_MEMORY_GROWTH=1 -O2 -sASSERTIONS=0"
)
```

Key changes:
1. `ENVIRONMENT=worker` (was `node`) -- removes Node.js fs/path dependencies
2. `EXPORT_ES6=1` (was `0`) -- enables `import` syntax in worker
3. `SINGLE_FILE=1` (new) -- embeds .wasm binary as base64 in the .mjs file, eliminating separate .wasm fetch
4. Suffix `.mjs` (was `.cjs`) -- ES module extension

**SINGLE_FILE rationale:** Without SINGLE_FILE, the worker must resolve the .wasm file path relative to itself. In a Vite build, worker assets get hashed filenames and moved to different directories, making relative path resolution fragile. SINGLE_FILE avoids this entirely by embedding the binary.

### Pattern 2: Web Worker with Message Protocol

**What:** Dedicated worker file that loads WASM and validates on demand
**When to use:** All WASM validation calls

```typescript
// workers/validationWorker.ts
import createModule from '../../wasm/build/nes-validator/nes-validator-wasm.mjs';

interface ValidateRequest {
  type: 'validate';
  id: number;
  yaml: string;
}

interface ValidateResponse {
  type: 'result';
  id: number;
  error: string; // empty = valid
}

interface ReadyMessage {
  type: 'ready';
}

interface ErrorMessage {
  type: 'error';
  message: string;
}

type WorkerMessage = ValidateResponse | ReadyMessage | ErrorMessage;

let wasmModule: { validateTopology: (yaml: string) => string } | null = null;

async function init() {
  try {
    wasmModule = await createModule();
    self.postMessage({ type: 'ready' } satisfies ReadyMessage);
  } catch (err) {
    self.postMessage({ type: 'error', message: String(err) } satisfies ErrorMessage);
  }
}

self.onmessage = (event: MessageEvent<ValidateRequest>) => {
  if (event.data.type === 'validate' && wasmModule) {
    const error = wasmModule.validateTopology(event.data.yaml);
    self.postMessage({
      type: 'result',
      id: event.data.id,
      error,
    } satisfies ValidateResponse);
  }
};

init();
```

### Pattern 3: Zustand Validation Slice

**What:** Store slice managing WASM loading state, validation results, debounce
**When to use:** Central state for all validation UI

```typescript
// store/validationSlice.ts
export type WasmStatus = 'loading' | 'ready' | 'error' | 'validating';

export interface ValidationSlice {
  wasmStatus: WasmStatus;
  wasmError: string | null;
  semanticError: string | null;  // empty string = valid, null = not yet validated
  wasmRetryCount: number;

  setWasmStatus: (status: WasmStatus) => void;
  setWasmError: (error: string | null) => void;
  setSemanticError: (error: string | null) => void;
  incrementRetryCount: () => void;
}
```

### Pattern 4: Vite Worker Import

**What:** Use Vite's built-in worker support via `new Worker(new URL(...))`
**When to use:** Creating the validation worker instance

```typescript
// Vite handles bundling and URL resolution automatically
const worker = new Worker(
  new URL('../workers/validationWorker.ts', import.meta.url),
  { type: 'module' }
);
```

This is the standard Vite pattern. No `?worker` import suffix needed -- `new Worker(new URL(...))` gives more control over lifecycle.

### Pattern 5: Debounced Validation Hook

**What:** React hook that subscribes to store changes and debounces validation
**When to use:** In the App component or a provider near the root

```typescript
// lib/wasmValidation.ts
export function useWasmValidation() {
  const workerRef = useRef<Worker | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>();
  const requestIdRef = useRef(0);

  // Subscribe to topology + query changes
  const workers = useStore(s => s.workers);
  const logicalSources = useStore(s => s.logicalSources);
  const physicalSources = useStore(s => s.physicalSources);
  const sinks = useStore(s => s.sinks);
  const queries = useStore(s => s.queries);
  const wasmStatus = useStore(s => s.wasmStatus);

  useEffect(() => {
    if (wasmStatus !== 'ready') return;

    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      const yaml = storeToYaml(workers, logicalSources, physicalSources, sinks, queries);
      const id = ++requestIdRef.current;
      workerRef.current?.postMessage({ type: 'validate', id, yaml });
    }, 300); // 300ms debounce -- fast enough for interactive, slow enough to batch
  }, [workers, logicalSources, physicalSources, sinks, queries, wasmStatus]);
}
```

### Anti-Patterns to Avoid
- **Loading WASM on main thread:** Always use the Web Worker. Even WASM initialization (module compilation) should happen in the worker.
- **Synchronous validation:** Never `await` validation in event handlers. Fire-and-forget via postMessage, display results when they arrive.
- **Re-creating workers:** Create one worker on app startup, reuse it. Terminate only on app unmount.
- **Validating on every keystroke:** Debounce is critical. Without it, rapid typing creates a queue of WASM calls.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| YAML serialization | Custom YAML builder | `storeToYaml()` from `lib/yaml.ts` | Already handles all NES YAML format quirks |
| Structural validation | New structural checks | Existing `validateTopology()` in `lib/validation.ts` | Already covers orphan nodes, missing config, cycles |
| Worker bundling | Manual worker build pipeline | Vite's built-in `new Worker(new URL(...))` | Handles dev/prod, HMR, code splitting automatically |
| Debounce utility | Custom debounce | `setTimeout`/`clearTimeout` pattern | Simple enough to inline, no lodash needed |

**Key insight:** The existing codebase already has YAML generation and structural validation. This phase adds a parallel WASM semantic validation layer, not a replacement.

## Common Pitfalls

### Pitfall 1: WASM Built for Wrong Environment
**What goes wrong:** The current WASM module uses `-sENVIRONMENT=node`. Loading it in a Web Worker fails with errors about missing `require`, `fs`, or `process`.
**Why it happens:** Emscripten generates environment-specific glue code. Node.js glue uses `require('fs')` for file loading.
**How to avoid:** Rebuild with `-sENVIRONMENT=worker` and `-sSINGLE_FILE=1`.
**Warning signs:** "require is not defined" or "fs is not defined" errors in browser console.

### Pitfall 2: WASM File Path Resolution in Workers
**What goes wrong:** Without `SINGLE_FILE=1`, the .mjs loader tries to fetch the .wasm file relative to the worker's URL. Vite hashes and moves assets, breaking the relative path.
**Why it happens:** Emscripten's default `locateFile` uses the script directory, which differs between dev and production builds.
**How to avoid:** Use `SINGLE_FILE=1` to embed the binary. Alternatively, override `locateFile` in the module factory, but SINGLE_FILE is simpler.
**Warning signs:** 404 errors fetching `.wasm` file in production build.

### Pitfall 3: Stale Validation Results
**What goes wrong:** User makes rapid changes, validation returns results for an old topology state.
**Why it happens:** Multiple validate requests in flight; responses arrive out of order.
**How to avoid:** Include a request ID in each message. Only apply results if the response ID matches the latest request ID.
**Warning signs:** Validation status flickers between valid/invalid on rapid edits.

### Pitfall 4: Worker Not Ready When First Validation Fires
**What goes wrong:** App sends validate message before WASM module finishes loading in the worker.
**Why it happens:** WASM compilation takes time (could be seconds for 1.3MB module).
**How to avoid:** Worker sends a `ready` message after initialization. Only enable validation dispatch after receiving `ready`.
**Warning signs:** First validation silently fails or hangs.

### Pitfall 5: Memory Leaks from Unreturned Worker
**What goes wrong:** Creating new Worker instances without terminating old ones.
**Why it happens:** Re-renders or hot module replacement create duplicate workers.
**How to avoid:** Store worker ref, terminate in cleanup function of useEffect.
**Warning signs:** Multiple "ready" messages in console, increasing memory usage.

### Pitfall 6: Vitest Cannot Run Web Workers
**What goes wrong:** Tests using `new Worker()` fail in Vitest because happy-dom/jsdom don't support real Web Workers.
**Why it happens:** Test environments simulate DOM but not multi-threaded Worker APIs.
**How to avoid:** For unit tests, mock the worker or test the WASM module directly (Node.js compatible build). For integration tests, use `@vitest/web-worker` plugin or test the validation logic without the worker wrapper.
**Warning signs:** "Worker is not defined" in test output.

## Code Examples

### WASM Module Interface (verified from main.cpp)
```cpp
// Source: nes-topology-editor/wasm/nes-validator/main.cpp
EMSCRIPTEN_BINDINGS(nes_validator) {
    emscripten::function("validateTopology", &NES::Validator::validateTopology);
}
// JS interface: module.validateTopology(yamlString: string) => string
// Empty string = valid, non-empty = error message
```

### Existing Store Composition (verified from store/index.ts)
```typescript
// Source: nes-topology-editor/src/store/index.ts (topology-editor-ui branch)
export type StoreState = TopologySlice & UiSlice & QuerySlice;
// Add: & ValidationSlice

export const useStore = create<StoreState>()(
  temporal(
    (...args) => ({
      ...createTopologySlice(...args),
      ...createUiSlice(...args),
      ...createQuerySlice(...args),
      // Add: ...createValidationSlice(...args),
    }),
    { partialize: (state) => ({ /* topology + query fields for undo */ }) },
  ),
);
```

### Status Bar Component Pattern
```typescript
// Source: project conventions (lucide-react icons, Tailwind CSS)
import { CheckCircle, AlertTriangle, Loader2, XCircle } from 'lucide-react';

// Status mapping:
// loading -> Loader2 (spinning) + "Loading validator..."
// ready (no errors) -> CheckCircle (green)
// ready (has errors) -> AlertTriangle (amber) + hover tooltip
// validating -> Loader2 (spinning) + "Validating..."
// error -> XCircle (red) + "Validator unavailable"
```

### Vite Config for WASM Worker (no plugins needed)
```typescript
// vite.config.ts -- minimal changes needed
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  worker: {
    format: 'es',  // Use ES module workers
  },
});
```

Vite 6 handles `new Worker(new URL(...), { type: 'module' })` natively. No `vite-plugin-wasm` needed since the WASM is loaded inside the worker (not imported in main thread).

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `?worker` import suffix | `new Worker(new URL(...))` | Vite 4+ | Standards-based, better IDE support |
| vite-plugin-wasm for all WASM | Native `?init` or worker-loaded | Vite 4+ | No plugin needed when WASM loads in worker |
| CJS Emscripten output | ESM with EXPORT_ES6=1 | Emscripten 3.0+ | Required for `import` in module workers |
| Separate .wasm + .js | SINGLE_FILE=1 | Emscripten 2.0+ | Simpler deployment, no asset path issues |

## Open Questions

1. **Debounce duration tuning**
   - What we know: Validation speed unknown in browser (Node.js tests pass quickly but browser WASM may differ)
   - What's unclear: Optimal debounce for responsive UX without excessive WASM calls
   - Recommendation: Start with 300ms, adjust based on observed validation latency. Could expose as a constant.

2. **WASM module size with SINGLE_FILE**
   - What we know: .wasm is 1.3MB, base64 encoding adds ~33% = ~1.7MB total .mjs file
   - What's unclear: Whether gzip/brotli compression of the .mjs sufficiently reduces transfer size
   - Recommendation: Accept SINGLE_FILE overhead. The 408KB gzipped figure from Phase 6 should still hold since gzip compresses base64 well. Verify after rebuild.

3. **Dual WASM builds (Node.js tests + browser worker)**
   - What we know: Current build targets Node.js. Changing to worker breaks existing Node.js tests.
   - What's unclear: Whether to maintain two build targets or adapt tests.
   - Recommendation: Add a second CMake target `nes-validator-wasm-browser` with worker settings, keeping original for Node.js tests. Or adapt Node.js tests to use the browser build with `ENVIRONMENT=web,worker,node`.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Vitest 3.2.4 |
| Config file | `nes-topology-editor/vitest.config.ts` (exists on `topology-editor-ui` branch) |
| Quick run command | `cd nes-topology-editor && npx vitest run --reporter=verbose` |
| Full suite command | `cd nes-topology-editor && npx vitest run` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| BINT-01 | WASM module loads in worker | integration | `npx vitest run src/__tests__/wasm-loading.test.ts` | No -- Wave 0 |
| BINT-02 | Validation errors displayed | unit | `npx vitest run src/__tests__/validation-display.test.ts` | No -- Wave 0 |
| BINT-03 | Validation runs in Web Worker (non-blocking) | unit | `npx vitest run src/__tests__/validation-worker.test.ts` | No -- Wave 0 |
| BINT-04 | Validation debounced on changes | unit | `npx vitest run src/__tests__/validation-debounce.test.ts` | No -- Wave 0 |
| TEST-02 | WASM validates known-good and known-bad YAML | integration | `node --test wasm/test/test-validator.mjs` (Node.js) | Yes -- existing |
| TEST-03 | CI job builds WASM + runs tests | manual-only | Deferred per user decision | N/A |

### Sampling Rate
- **Per task commit:** `cd nes-topology-editor && npx vitest run --reporter=verbose`
- **Per wave merge:** `cd nes-topology-editor && npx vitest run`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `src/__tests__/wasm-loading.test.ts` -- covers BINT-01 (mock Worker, verify ready message handling, retry logic)
- [ ] `src/__tests__/validation-worker.test.ts` -- covers BINT-03 (mock Worker postMessage protocol)
- [ ] `src/__tests__/validation-debounce.test.ts` -- covers BINT-04 (verify debounce timing, stale result rejection)
- [ ] `src/__tests__/validation-display.test.ts` -- covers BINT-02 (StatusBar rendering, error display states)
- [ ] Worker mock utility for Vitest (since happy-dom lacks Web Worker support)

## Sources

### Primary (HIGH confidence)
- `nes-topology-editor/wasm/nes-validator/CMakeLists.txt` -- current WASM build flags (verified: ENVIRONMENT=node, EXPORT_ES6=0)
- `nes-topology-editor/wasm/nes-validator/main.cpp` -- Embind interface (verified: validateTopology function)
- `nes-topology-editor/wasm/test/test-validator.mjs` -- existing Node.js integration tests (5 tests)
- `topology-editor-ui` branch source files -- store, components, validation, YAML generation (all verified via git show)
- [Emscripten Settings Reference](https://emscripten.org/docs/tools_reference/settings_reference.html) -- ENVIRONMENT, MODULARIZE, EXPORT_ES6, SINGLE_FILE docs

### Secondary (MEDIUM confidence)
- [Vite+React+Emscripten+Web Worker tutorial](https://dev.to/joyhughes/a-simple-web-app-using-vitereact-c-emscripten-webassembly-and-a-web-worker-48ia) -- verified pattern: ENVIRONMENT=worker + SINGLE_FILE + EXPORT_ES6 + Worker loading
- [Vite Features: Web Workers](https://vite.dev/guide/features) -- `new Worker(new URL(...))` syntax, `?worker` import

### Tertiary (LOW confidence)
- Debounce duration (300ms) -- based on general UX best practices, needs validation with actual WASM performance

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries already in project, no new dependencies
- Architecture: HIGH -- patterns verified from official Emscripten docs and Vite docs, consistent with existing codebase
- WASM rebuild flags: HIGH -- verified current flags from CMakeLists.txt, Emscripten docs confirm required changes
- Pitfalls: HIGH -- derived from verified technical constraints (ENVIRONMENT=node incompatibility, asset path issues)
- Debounce/UX tuning: MEDIUM -- reasonable defaults but may need adjustment

**Research date:** 2026-03-16
**Valid until:** 2026-04-16 (stable domain, no fast-moving dependencies)
