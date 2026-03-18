# Phase 3: YAML Pipeline - Research

**Researched:** 2026-03-14
**Domain:** YAML serialization, Monaco editor integration, bidirectional state sync
**Confidence:** HIGH

## Summary

Phase 3 adds a toggleable YAML overlay panel on the canvas with bidirectional editing: topology changes update the YAML in real time, and user edits to the YAML sync back to the canvas. The NES YAML topology format is well-documented in the repository (`docs/nebulastream-frontend.md`) with multiple real test fixtures available. The format has five top-level keys (`query`, `workers`, `logical`, `physical`, `sinks`) that map directly to existing Zustand store state. A cross-cutting change removes free node positioning, making auto-layout run on every structural graph change.

The technical challenge is modest. The serialization/deserialization is a straightforward mapping between the store's typed data model and the NES YAML format. Monaco editor is already installed and proven in `QueryPanel.tsx`. The main complexity is the bidirectional sync loop (preventing echo cycles) and the YAML-to-store parser that must validate and reconcile user-typed YAML with the existing store model.

**Primary recommendation:** Use `js-yaml` for parse/dump. Build a pure-function serializer (`storeToYaml`) and deserializer (`yamlToStore`) in `lib/yaml.ts`, then wire them into a `YamlOverlay` component that subscribes to the store and debounces YAML-to-store updates.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- YAML preview is a toggleable floating overlay panel on the canvas, activated via a toolbar button
- Uses Monaco editor with YAML syntax highlighting
- Panel floats over the canvas area (does not take permanent layout space)
- YAML editor is fully editable -- not read-only
- Canvas-to-YAML: any topology change regenerates YAML in the editor
- YAML-to-canvas: user edits in the YAML editor sync back to the canvas with live debounce (~500ms after typing stops)
- If YAML is invalid during editing, canvas stays at the last valid state
- Parse errors shown as inline Monaco error markers (red squiggly underlines at error locations)
- No explicit import or export dialogs -- the YAML editor IS the import/export mechanism
- To "export": user copies YAML from the editor (select-all, copy)
- To "import": user pastes YAML into the editor, which syncs to canvas via the live debounce
- No download button -- copy-paste only
- No unsaved-changes warnings
- Remove free node positioning entirely from the canvas
- The only allowed drag operations are: drag-to-connect and drag-to-reassign (source/sink between workers)
- Auto-layout (dagre) runs automatically on every structural graph change
- Use the future NES query object format: `queries: [{query: '...', name: '...'}]`
- Always use array format, even for a single query
- OUTP-05 (template topologies) dropped entirely

### Claude's Discretion
- YAML overlay dimensions, position, and resize behavior
- Debounce timing for YAML-to-canvas sync
- Monaco editor configuration (theme, minimap, word wrap)
- How to handle the overlay toggle animation (if any)
- YAML key ordering in generated output

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| OUTP-01 | Live YAML preview updates in real-time as topology changes | Store subscription + `storeToYaml()` pure function; Monaco editor with `defaultLanguage="yaml"` |
| OUTP-02 | Generated YAML is valid and compatible with nes-cli format | NES YAML format fully documented in `docs/nebulastream-frontend.md` with test fixtures; use `js-yaml` dump with correct key ordering |
| OUTP-03 | User can download/export the YAML file | Satisfied via copy-paste from editable Monaco editor (per user decision: no download button) |
| OUTP-04 | User can import existing YAML topology files | Satisfied via paste into editor + `yamlToStore()` parser with debounced sync (per user decision: no import dialog) |
| OUTP-05 | Template topologies available as starting points | DROPPED per user decision -- users can paste YAML examples from NES docs |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| js-yaml | ^4.1 | YAML parse/stringify | Lightweight, fast parsing, simple API (`load`/`dump`), well-maintained, 60M+ weekly downloads |
| @monaco-editor/react | ^4.7.0 | YAML editor component | Already installed, used in QueryPanel, supports `defaultLanguage="yaml"` with built-in syntax highlighting |
| @dagrejs/dagre | ^1.1.4 | Auto-layout | Already installed, `getLayoutedElements()` in `lib/layout.ts` |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| monaco-editor (bundled) | via @monaco-editor/react | Error markers API | `editor.setModelMarkers()` for inline YAML parse error display |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| js-yaml | yaml (eemeli) | More features (streaming, CST), but heavier; overkill for simple parse/dump |
| js-yaml | hand-rolled YAML | Error-prone for edge cases (multiline strings, special chars) |

**Installation:**
```bash
cd nes-topology-editor && npm install js-yaml && npm install -D @types/js-yaml
```

## Architecture Patterns

### Recommended Project Structure
```
src/
├── lib/
│   └── yaml.ts              # Pure functions: storeToYaml(), yamlToStore(), validateYaml()
├── components/
│   └── YamlOverlay/
│       └── YamlOverlay.tsx   # Floating overlay with Monaco YAML editor
├── store/
│   ├── uiSlice.ts            # Add yamlOverlayVisible state
│   └── topologySlice.ts      # Add replaceTopology() action for YAML import
```

### Pattern 1: Pure Serialization Functions
**What:** `storeToYaml()` and `yamlToStore()` as pure functions in `lib/yaml.ts` with no React or store dependencies.
**When to use:** Always -- these are the core of the phase and must be independently testable.
**Example:**
```typescript
// Source: NES YAML format from docs/nebulastream-frontend.md
import yaml from 'js-yaml';
import type { Worker, LogicalSource, PhysicalSource, Sink, Query } from './types';

interface NesTopology {
  queries: { query: string; name: string }[];
  workers: NesWorkerYaml[];
  logical: NesLogicalYaml[];
  physical: NesPhysicalYaml[];
  sinks: NesSinkYaml[];
}

// NES YAML uses host addresses as identifiers, not UUIDs
// The store uses UUID ids -- serialization must map between them
interface NesWorkerYaml {
  host: string;       // e.g. "worker-1:9090" (this is the data port)
  grpc: string;       // e.g. "worker-1:8080"
  capacity: number;
  downstream?: string[]; // array of host addresses
}

export function storeToYaml(
  workers: Worker[],
  logicalSources: LogicalSource[],
  physicalSources: PhysicalSource[],
  sinks: Sink[],
  queries: Query[],
): string {
  const topology: Record<string, unknown> = {};
  // Build NES-compatible YAML object, then dump
  // Key ordering: queries, sinks, logical, physical, workers
  return yaml.dump(topology, { lineWidth: -1, noRefs: true });
}
```

### Pattern 2: Bidirectional Sync with Echo Prevention
**What:** The YAML overlay subscribes to the store and also writes back to it. An "origin" flag prevents infinite loops.
**When to use:** Any bidirectional data binding between store and editor.
**Example:**
```typescript
// Inside YamlOverlay component:
const isFromYamlEdit = useRef(false);

// Store -> YAML (regenerate when topology changes)
useEffect(() => {
  if (isFromYamlEdit.current) {
    isFromYamlEdit.current = false;
    return; // Skip: this change came from YAML editing
  }
  const newYaml = storeToYaml(workers, logicalSources, physicalSources, sinks, queries);
  setEditorValue(newYaml);
}, [workers, logicalSources, physicalSources, sinks, queries]);

// YAML -> Store (debounced, on editor change)
const handleEditorChange = useCallback((value: string | undefined) => {
  // Debounce, then parse, then update store
  isFromYamlEdit.current = true;
  replaceTopology(parsed);
}, []);
```

### Pattern 3: Store `replaceTopology` Action
**What:** A single atomic action that replaces the entire topology state from parsed YAML data.
**When to use:** When YAML is parsed and needs to overwrite the canvas state.
**Example:**
```typescript
// In topologySlice:
replaceTopology: (data: {
  workers: Worker[];
  logicalSources: LogicalSource[];
  physicalSources: PhysicalSource[];
  sinks: Sink[];
}) => set(() => data),
```

### Pattern 4: Auto-Layout on Structural Change
**What:** After every structural mutation (add/remove/connect nodes), run dagre layout and update positions.
**When to use:** This is a cross-cutting concern that modifies Phase 1 behavior.
**Example:**
```typescript
// Approach: wrap structural actions or use zustand subscribe
// Best approach: call auto-layout at the end of replaceTopology and
// wrap individual store actions in Canvas to trigger layout after dispatch.
// The existing Toolbar.handleAutoLayout pattern shows how:
const { nodes: layoutedNodes } = getLayoutedElements(nodes, edges, 'LR');
setAllNodePositions(layoutedNodes.map(n => ({ id: n.id, position: n.position })));
```

### Anti-Patterns to Avoid
- **Echo loop:** Store change -> regenerate YAML -> editor onChange fires -> parse YAML -> update store -> repeat. Must use origin tracking ref to break the cycle.
- **Partial store updates on YAML import:** Don't update workers, then sources, then sinks separately -- use a single `replaceTopology` action to avoid intermediate invalid states and multiple re-renders.
- **Stringifying on every keystroke:** The YAML-to-store direction must be debounced. The store-to-YAML direction is fine to run on every store change since `yaml.dump` is fast.

## NES YAML Format Reference

Based on `docs/nebulastream-frontend.md` and test fixture files in the repository:

### Top-Level Keys (in canonical NES order)
```yaml
query: |                          # string (single) or array (multiple)
  SELECT * FROM src INTO sink

sinks:
  - name: VOID_SINK              # sink name (referenced in SQL)
    host: worker-1:9090           # data port of host worker
    schema:                       # output schema
      - name: src$field
        type: FLOAT64
    type: Void                    # Void | File | Print | Checksum
    config: { }                   # type-specific config

logical:
  - name: GENERATOR_SOURCE       # logical source name
    schema:
      - name: DOUBLE
        type: FLOAT64

physical:
  - logical: GENERATOR_SOURCE    # references logical source by name
    host: worker-1:9090           # data port of host worker
    parser_config:
      type: CSV
      fieldDelimiter: ","
    type: Generator               # Generator | CSV | TCP | etc.
    source_config:                # type-specific config
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10

workers:
  - host: worker-1:9090           # data port (main identifier)
    grpc: worker-1:8080           # gRPC port
    capacity: 10000
    downstream: [ worker-2:9090 ] # optional: array of downstream worker hosts
```

### Mapping: Store Types -> NES YAML

| Store Field | YAML Field | Notes |
|-------------|------------|-------|
| `Worker.host` (e.g. "worker-1:9090") | `workers[].host` | Worker host IS the data port in store |
| `Worker.grpc` | `workers[].grpc` | gRPC port |
| `Worker.capacity` | `workers[].capacity` | |
| `Worker.downstream[]` (UUIDs) | `workers[].downstream[]` (host strings) | Must map UUID -> host string |
| `LogicalSource.name` | `logical[].name` | |
| `LogicalSource.schema` | `logical[].schema` | Same structure: `{name, type}` |
| `PhysicalSource.logicalSourceId` (UUID) | `physical[].logical` (name string) | Must resolve UUID -> logical source name |
| `PhysicalSource.hostWorkerId` (UUID) | `physical[].host` (host string) | Must resolve UUID -> worker host |
| `PhysicalSource.type` | `physical[].type` | e.g. "Generator", "CSV" |
| `PhysicalSource.sourceConfig` | `physical[].source_config` | key mapping: camelCase -> snake_case? No -- store uses same keys |
| `PhysicalSource.parserConfig` | `physical[].parser_config` | |
| `Sink.name` | `sinks[].name` | |
| `Sink.hostWorkerId` (UUID) | `sinks[].host` (host string) | Must resolve UUID -> worker host |
| `Sink.type` | `sinks[].type` | Void, File, Print, Checksum |
| `Sink.schema` | `sinks[].schema` | Same structure: `{name, type}` |
| `Sink.config` | `sinks[].config` | |
| `Query.sql` | `queries[].query` | Future format per user decision |
| `Query.name` | `queries[].name` | Future format per user decision |

### Key Insight: ID Resolution
The store uses UUIDs for internal references (`Worker.id`, `PhysicalSource.hostWorkerId`, etc.) but NES YAML uses host addresses and logical source names as identifiers. Serialization must resolve UUIDs to display names/hosts, and deserialization must create new UUIDs and build the reference map.

### Query Format Difference
Current NES YAML uses `query: |` (single string) or `query: [...]` (array of strings).
User decided on future format: `queries: [{query: '...', name: '...'}]`.
This means the **generated** YAML uses the new `queries` key, but the **parser** should accept BOTH formats for import compatibility (old `query` key and new `queries` key).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| YAML parsing/stringifying | Custom parser | js-yaml `load`/`dump` | Edge cases: multiline strings, special chars, quoting rules, anchors |
| YAML syntax highlighting | Custom tokenizer | Monaco built-in YAML language | Already bundled with Monaco, works via `defaultLanguage="yaml"` |
| Debounce | Custom setTimeout wrapper | Simple `setTimeout`/`clearTimeout` in useRef | Only one usage; lodash.debounce is overkill |
| Auto-layout | Manual position calculation | dagre via existing `getLayoutedElements()` | Already proven in Phase 1 |

**Key insight:** The YAML format is simple enough that the serializer/deserializer can be straightforward object mapping, but the actual YAML string handling (parsing, dumping, quoting) must use a library.

## Common Pitfalls

### Pitfall 1: Echo Loop in Bidirectional Sync
**What goes wrong:** Store update triggers YAML regeneration, Monaco onChange fires, parse triggers store update, infinite loop.
**Why it happens:** Monaco's `onChange` fires for both user edits AND programmatic value changes.
**How to avoid:** Use a ref flag (`isFromYamlEdit`) to track the origin of changes. When the store updates from YAML editing, skip the YAML regeneration for that cycle.
**Warning signs:** Browser freeze, infinite re-renders, "Maximum update depth exceeded" error.

### Pitfall 2: Monaco Controlled vs Uncontrolled Value
**What goes wrong:** Setting Monaco's `value` prop on every store change resets cursor position and undo history.
**Why it happens:** `@monaco-editor/react` treats `value` as a controlled prop -- setting it moves cursor to start.
**How to avoid:** Use `defaultValue` for initial content. For subsequent updates from store, use the Monaco editor instance API: `editor.getModel().setValue(newYaml)` only when the change didn't originate from user typing. Or compare the current editor value with the new YAML and skip if identical.
**Warning signs:** Cursor jumps to position 0,0 after every keystroke.

### Pitfall 3: UUID <-> Host/Name Resolution on Import
**What goes wrong:** Importing YAML creates workers with host addresses but no UUIDs. Physical sources reference workers by host, but the store needs UUID references.
**Why it happens:** NES YAML uses host addresses as identifiers; the store uses generated UUIDs.
**How to avoid:** In `yamlToStore()`, generate UUIDs for all entities first, then build a `host -> UUID` map and `logicalName -> UUID` map to resolve cross-references.
**Warning signs:** Sources/sinks not connected to workers after import; blank canvas.

### Pitfall 4: Removing onNodeDragStop Position Updates
**What goes wrong:** After removing free positioning, existing `onNodeDragStop` handler that calls `updateNodePosition()` must be modified to NOT update positions for simple drags, but still handle drag-to-connect and drag-to-reassign.
**Why it happens:** The cross-cutting change removes free positioning but keeps drag-to-connect.
**How to avoid:** In `onNodeDragStop`, only run auto-layout after a structural change (new connection), not after a plain drag. For plain drags, snap the node back to its auto-layouted position.
**Warning signs:** Nodes stay where user dropped them instead of snapping back to layout.

### Pitfall 5: Monaco Error Markers API
**What goes wrong:** Using wrong API to show parse errors.
**Why it happens:** Monaco markers require access to the `monaco` namespace and the editor model.
**How to avoid:** Use `onMount` callback to capture the monaco instance, then call `monaco.editor.setModelMarkers(model, 'yaml', markers)` with error line/column from js-yaml parse exceptions.
**Warning signs:** No red underlines despite invalid YAML.

## Code Examples

### storeToYaml -- Serialization
```typescript
// Source: NES YAML format from docs/nebulastream-frontend.md
import yaml from 'js-yaml';

function storeToYaml(
  workers: Worker[],
  logicalSources: LogicalSource[],
  physicalSources: PhysicalSource[],
  sinks: Sink[],
  queries: Query[],
): string {
  // Build worker ID -> host map for reference resolution
  const workerHostMap = new Map(workers.map(w => [w.id, w.host]));
  const logicalNameMap = new Map(logicalSources.map(ls => [ls.id, ls.name]));

  const doc: Record<string, unknown> = {};

  // Queries (future format)
  if (queries.length > 0) {
    doc.queries = queries.map(q => ({
      query: q.sql,
      name: q.name,
    }));
  }

  // Sinks
  if (sinks.length > 0) {
    doc.sinks = sinks.map(s => ({
      name: s.name,
      host: workerHostMap.get(s.hostWorkerId) ?? '',
      schema: s.schema.map(f => ({ name: f.name, type: f.type })),
      type: s.type,
      config: Object.keys(s.config).length > 0 ? s.config : {},
    }));
  }

  // Logical sources
  if (logicalSources.length > 0) {
    doc.logical = logicalSources.map(ls => ({
      name: ls.name,
      schema: ls.schema.map(f => ({ name: f.name, type: f.type })),
    }));
  }

  // Physical sources
  if (physicalSources.length > 0) {
    doc.physical = physicalSources.map(ps => ({
      logical: logicalNameMap.get(ps.logicalSourceId) ?? '',
      host: workerHostMap.get(ps.hostWorkerId) ?? '',
      parser_config: Object.keys(ps.parserConfig).length > 0 ? ps.parserConfig : undefined,
      type: ps.type,
      source_config: Object.keys(ps.sourceConfig).length > 0 ? ps.sourceConfig : undefined,
    }));
  }

  // Workers
  if (workers.length > 0) {
    doc.workers = workers.map(w => {
      const entry: Record<string, unknown> = {
        host: w.host,
        grpc: w.grpc,
        capacity: w.capacity,
      };
      if (w.downstream.length > 0) {
        entry.downstream = w.downstream.map(id => workerHostMap.get(id) ?? '');
      }
      return entry;
    });
  }

  return yaml.dump(doc, { lineWidth: -1, noRefs: true, quotingType: '"' });
}
```

### yamlToStore -- Deserialization
```typescript
// Source: NES YAML format from docs/nebulastream-frontend.md
import yaml from 'js-yaml';
import { generateId } from './ids';

interface ParseResult {
  workers: Worker[];
  logicalSources: LogicalSource[];
  physicalSources: PhysicalSource[];
  sinks: Sink[];
  queries: Query[];
}

function yamlToStore(yamlStr: string): ParseResult {
  const doc = yaml.load(yamlStr) as Record<string, unknown>;

  // 1. Parse workers, assign UUIDs, build host->id map
  const hostToId = new Map<string, string>();
  const workers: Worker[] = (doc.workers as any[] ?? []).map(w => {
    const id = generateId();
    hostToId.set(w.host, id);
    return {
      id,
      host: w.host ?? '',
      grpc: w.grpc ?? '',
      capacity: w.capacity ?? 10000,
      downstream: [], // resolve after all workers parsed
      position: { x: 0, y: 0 },
    };
  });

  // 2. Resolve downstream references (host -> UUID)
  (doc.workers as any[] ?? []).forEach((w, i) => {
    if (w.downstream) {
      workers[i]!.downstream = w.downstream
        .map((h: string) => hostToId.get(h))
        .filter(Boolean) as string[];
    }
  });

  // 3. Parse logical sources
  const logicalNameToId = new Map<string, string>();
  const logicalSources: LogicalSource[] = (doc.logical as any[] ?? []).map(ls => {
    const id = generateId();
    logicalNameToId.set(ls.name, id);
    return { id, name: ls.name, schema: ls.schema ?? [] };
  });

  // 4. Parse physical sources (resolve logical name -> UUID, host -> UUID)
  const physicalSources: PhysicalSource[] = (doc.physical as any[] ?? []).map(ps => ({
    id: generateId(),
    logicalSourceId: logicalNameToId.get(ps.logical) ?? '',
    hostWorkerId: hostToId.get(ps.host) ?? '',
    type: ps.type ?? '',
    sourceConfig: ps.source_config ?? {},
    parserConfig: ps.parser_config ?? {},
    position: { x: 0, y: 0 },
  }));

  // 5. Parse sinks
  const sinks: Sink[] = (doc.sinks as any[] ?? []).map(s => ({
    id: generateId(),
    name: s.name ?? '',
    hostWorkerId: hostToId.get(s.host) ?? '',
    type: s.type ?? '',
    schema: s.schema ?? [],
    config: s.config ?? {},
    position: { x: 0, y: 0 },
  }));

  // 6. Parse queries (support both old and new format)
  let queries: Query[] = [];
  if (doc.queries && Array.isArray(doc.queries)) {
    // New format: queries: [{query, name}]
    queries = doc.queries.map((q: any) => ({
      id: generateId(),
      name: q.name ?? '',
      sql: q.query ?? '',
    }));
  } else if (doc.query) {
    // Old format: query: string | string[]
    const rawQueries = Array.isArray(doc.query) ? doc.query : [doc.query];
    queries = rawQueries.map((sql: string) => ({
      id: generateId(),
      name: '',
      sql: sql.trim(),
    }));
  }

  return { workers, logicalSources, physicalSources, sinks, queries };
}
```

### Monaco Error Markers
```typescript
// Source: Monaco editor API docs
import type { editor as MonacoEditor, IDisposable } from 'monaco-editor';

function setYamlErrors(
  monaco: typeof import('monaco-editor'),
  editor: MonacoEditor.IStandaloneCodeEditor,
  error: yaml.YAMLException | null,
) {
  const model = editor.getModel();
  if (!model) return;

  if (!error) {
    monaco.editor.setModelMarkers(model, 'yaml', []);
    return;
  }

  // js-yaml YAMLException has mark.line and mark.column
  const line = (error.mark?.line ?? 0) + 1; // 0-indexed -> 1-indexed
  const column = (error.mark?.column ?? 0) + 1;

  monaco.editor.setModelMarkers(model, 'yaml', [{
    severity: monaco.MarkerSeverity.Error,
    message: error.reason || error.message,
    startLineNumber: line,
    startColumn: column,
    endLineNumber: line,
    endColumn: column + 1,
  }]);
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| NES `query: \|` (single string) | Future: `queries: [{query, name}]` | Upcoming NES branch | Editor generates new format; parser accepts both |
| `data:` field in worker YAML (test files) | `host:` + `grpc:` (documented format) | Current docs | Some test fixtures use `data:` for data port; official docs show `host:` |

**Note on `host` vs `data` field:** The NES test YAML fixtures use `data: worker-1:9090` for the data port and `host: worker-1:8080` for what appears to be the gRPC port. However, the official documented format in `docs/nebulastream-frontend.md` uses `host: worker-1:9090` (data port) and `grpc: worker-1:8080`. The editor should follow the documented format since that's what nes-cli expects.

## Open Questions

1. **Worker host/grpc vs data field inconsistency**
   - What we know: Test fixtures use `host`/`data`, docs use `host`/`grpc` with inverted semantics
   - What's unclear: Which port mapping nes-cli actually expects in current builds
   - Recommendation: Follow `docs/nebulastream-frontend.md` format (`host` = data port, `grpc` = gRPC port) since that's the documented contract. The store already has `host` and `grpc` fields matching this.

2. **Auto-layout triggering mechanism**
   - What we know: Layout must run on every structural change
   - What's unclear: Best hook point -- zustand middleware vs. explicit calls after each action
   - Recommendation: Explicit calls. After `replaceTopology`, after any connect/disconnect/add/remove in Canvas. A helper function `applyAutoLayout()` that reads store, computes layout, writes positions back. Middleware adds complexity for marginal benefit.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | vitest 3.x + jsdom |
| Config file | `nes-topology-editor/vitest.config.ts` |
| Quick run command | `cd nes-topology-editor && npx vitest run` |
| Full suite command | `cd nes-topology-editor && npx vitest run` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| OUTP-01 | storeToYaml produces valid YAML from store state | unit | `cd nes-topology-editor && npx vitest run src/lib/yaml.test.ts -x` | No -- Wave 0 |
| OUTP-02 | Generated YAML matches NES format (key names, structure) | unit | `cd nes-topology-editor && npx vitest run src/lib/yaml.test.ts -x` | No -- Wave 0 |
| OUTP-03 | YAML editor content is copyable (editable Monaco) | manual-only | N/A -- requires browser clipboard | N/A |
| OUTP-04 | yamlToStore parses NES YAML and creates correct store state | unit | `cd nes-topology-editor && npx vitest run src/lib/yaml.test.ts -x` | No -- Wave 0 |
| OUTP-04 | yamlToStore accepts old `query:` format | unit | `cd nes-topology-editor && npx vitest run src/lib/yaml.test.ts -x` | No -- Wave 0 |
| CROSS | Auto-layout runs on structural changes | unit | `cd nes-topology-editor && npx vitest run src/lib/yaml.test.ts -x` | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `cd nes-topology-editor && npx vitest run`
- **Per wave merge:** `cd nes-topology-editor && npx vitest run`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `src/lib/yaml.test.ts` -- covers OUTP-01, OUTP-02, OUTP-04 (storeToYaml, yamlToStore, roundtrip)
- [ ] `npm install js-yaml && npm install -D @types/js-yaml` -- dependency installation

## Sources

### Primary (HIGH confidence)
- `docs/nebulastream-frontend.md` -- NES YAML topology file format specification with examples
- `cmake-build-debug-tsan/test-tmp/*/tests/good/*.yaml` -- Real NES topology test fixtures (verified format)
- `nes-topology-editor/src/lib/types.ts` -- Store data model types
- `nes-topology-editor/src/components/QueryEditor/QueryPanel.tsx` -- Existing Monaco editor integration pattern
- `nes-topology-editor/src/store/topologySlice.ts` -- Store actions API
- `nes-topology-editor/src/lib/layout.ts` -- dagre auto-layout implementation

### Secondary (MEDIUM confidence)
- [js-yaml npm](https://www.npmjs.com/package/js-yaml) -- API documentation
- [Monaco editor built-in YAML](https://github.com/microsoft/monaco-editor) -- YAML language support confirmed in node_modules

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- js-yaml is the standard choice, Monaco already in project
- Architecture: HIGH -- bidirectional sync is well-understood pattern, store model maps cleanly to YAML format
- Pitfalls: HIGH -- echo loop and cursor reset are well-documented React+Monaco issues
- NES YAML format: HIGH -- documented in-repo with test fixtures to validate against

**Research date:** 2026-03-14
**Valid until:** 2026-04-14 (stable domain, no fast-moving dependencies)
