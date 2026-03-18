# Phase 2: Configuration Panels - Research

**Researched:** 2026-03-14
**Domain:** React property panels, form editors, Monaco SQL editor, NES source/sink config inventory
**Confidence:** HIGH

## Summary

Phase 2 adds three major UI regions to the topology editor: (1) a right sidebar with a Properties tab for node configuration and a Sources tab for logical source management, (2) a bottom panel with a SQL query editor using Monaco. The existing codebase has a clean Zustand store with all domain types already defined and CRUD actions for workers, physical sources, sinks, and logical sources. The main work is building form components, wiring them to store actions, and integrating Monaco editor.

The NES codebase has been inventoried for source and sink types. There are 3 source types (Generator, File, TCP) and 4 sink types (File, Print, Void, Checksum), each with well-defined ConfigParameter structs that specify field names, types, and defaults. Parser config applies to all sources (type, fieldDelimiter, tupleDelimiter, allowCommasInStrings). The topology editor needs fixed forms derived from these C++ Config classes.

**Primary recommendation:** Build form components as pure controlled inputs driven by Zustand store state, add update actions to topologySlice for property editing, and use `@monaco-editor/react` for the SQL query editor.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Right sidebar for property editing and logical source management
- Click a node to open its panel; click empty canvas or another node to close/switch
- Type-specific panel content: worker panel, source panel, and sink panel each have tailored layouts
- Panel is resizable by dragging the left edge (pulling UIPL-03 forward from Phase 4 for this panel)
- Two tabs in the right sidebar: **Properties** (node config) and **Sources** (logical source management)
- Logical source management is NOT part of the property panel -- it is a separate, dedicated pane
- Physical source properties only allow selecting from existing logical sources (not creating them inline)
- Dedicated "Sources" tab lists all logical sources with create/edit/delete
- Schema builder is an inline table: rows of (field name + type dropdown), add/remove with +/- buttons
- Type dropdown offers NES native types: INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64, FLOAT32, FLOAT64, BOOLEAN, CHAR, TEXT
- Delete policy: delete-with-orphan -- deleting a logical source leaves physical sources with a broken reference that shows a warning badge
- Query list and editor live in a **bottom panel** below the canvas (not in the sidebar)
- Monaco editor (VS Code editor component) for SQL editing with syntax highlighting
- Queries support user-defined names, displayed in query tabs
- Query names are included in YAML export (NES will be made to ignore them), enabling round-trip
- Unnamed queries are also supported -- displayed with *(unnamed)* italic placeholder in the tab list
- Clicking the tab allows optionally adding a name
- Fixed forms per source/sink type (not dynamic key-value pairs)
- Form fields are derived from the NES C++ Config classes for each physical source/sink type
- All source types discovered from the NES codebase
- All sink types discovered from the NES codebase
- Physical source links to logical source via an autocomplete text field (type-ahead filtering)

### Claude's Discretion
- Exact panel width and resize constraints
- Form validation rules and error message styling
- Tab styling and active state indicators
- Bottom panel height and resize behavior
- Monaco editor configuration (theme, minimap, line numbers)
- Empty state messages for each panel

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| PROP-01 | Clicking a worker shows its sources/sinks and host config in a side panel | Right sidebar Properties tab with worker-specific form; selectNode already in uiSlice |
| PROP-02 | Clicking a source shows its logical source and physical source config | Source panel form with logical source autocomplete and type-specific source config fields |
| PROP-03 | Clicking a sink shows its sink type and configuration | Sink panel form with type dropdown and type-specific config fields |
| PROP-04 | Property fields use typed inputs (text, number, dropdown) with inline validation | NES Config inventory provides exact field types and defaults for all source/sink types |
| PROP-05 | User can build schemas visually for logical sources | Schema builder in Sources tab: inline table with field name + type dropdown |
| SRCE-01 | User can manage logical sources (create, edit, delete) | Sources tab with CRUD, topologySlice already has logicalSources array |
| SRCE-02 | User can define schema for logical sources (field name + type) | Schema builder with NES native types dropdown |
| SRCE-03 | User can create physical sources and link them to a logical source | Source panel autocomplete field for logicalSourceId |
| QURY-01 | User can add queries to a query list | Bottom panel query tabs with add button; new Query model in store |
| QURY-02 | User can remove queries from the list | Delete button on query tabs |
| QURY-03 | User can edit queries in a SQL editor with syntax highlighting | Monaco editor in bottom panel with SQL language mode |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| @monaco-editor/react | ^4.7.0 | SQL editor with syntax highlighting | De facto Monaco wrapper for React; CDN-loaded, no webpack config needed |
| zustand | ^5.0.0 | State management (already installed) | Project standard from Phase 1 |
| react | ^19.0.0 | UI framework (already installed) | Project standard |
| tailwindcss | ^3.4.0 | Styling (already installed) | Project standard from Phase 1 |
| lucide-react | ^0.474.0 | Icons (already installed) | Project standard from Phase 1 |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| @xyflow/react | ^12.10.0 | Canvas (already installed) | Node click events for panel open/close |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| @monaco-editor/react | CodeMirror 6 | CodeMirror is lighter but Monaco provides richer SQL support and matches the "VS Code editor" decision |
| Custom resize | react-resizable-panels | Adds a dependency; CSS resize with mouse events is sufficient for two resize edges |

**Installation:**
```bash
cd nes-topology-editor && npm install @monaco-editor/react
```

## NES Source/Sink Type Inventory

This section documents every source and sink type discovered from the NES C++ codebase. The topology editor must create fixed forms matching these config parameters exactly.

### Source Types

#### 1. Generator (`type: "Generator"`)
From `nes-plugins/Sources/GeneratorSource/GeneratorSource.hpp` -- `ConfigParametersGenerator`:

| Config Key | C++ Type | Default | Required | Description |
|------------|----------|---------|----------|-------------|
| `seed` | uint32_t | current time | No | Random seed |
| `generator_schema` | string | (none) | **Yes** | Generator field definitions (multiline) |
| `max_runtime_ms` | int32_t | -1 | No | Max runtime in ms (-1 = unlimited) |
| `stop_generator_when_sequence_finishes` | enum | (none) | **Yes** | ALL, ONE, or NONE |
| `generator_rate_type` | enum | FIXED | No | FIXED or SINUS |
| `generator_rate_config` | string | "emit_rate 1000" | No | Rate configuration string |
| `flush_interval_ms` | uint64_t | 10 | No | Flush interval in ms |
| `max_inflight_buffers` | size_t | 0 (invalid=use default) | No | Inherited from SourceDescriptor |

#### 2. File (`type: "File"`)
From `nes-sources/private/FileSource.hpp` -- `ConfigParametersCSV`:

| Config Key | C++ Type | Default | Required | Description |
|------------|----------|---------|----------|-------------|
| `file_path` | string | (none) | **Yes** | Path to input file |
| `max_inflight_buffers` | size_t | 0 | No | Inherited from SourceDescriptor |

#### 3. TCP (`type: "TCP"`)
From `nes-plugins/Sources/TCPSource/TCPSource.hpp` -- `ConfigParametersTCP`:

| Config Key | C++ Type | Default | Required | Description |
|------------|----------|---------|----------|-------------|
| `socket_host` | string | (none) | **Yes** | TCP host address |
| `socket_port` | uint32_t | (none) | **Yes** | TCP port (0-65535) |
| `socket_domain` | enum-like | AF_INET | No | AF_INET or AF_INET6 |
| `socket_type` | enum-like | SOCK_STREAM | No | SOCK_STREAM, SOCK_DGRAM, etc. |
| `tuple_delimiter` | char | '\n' | No | Tuple separator character |
| `flush_interval_ms` | float | 0 | No | Flush interval in ms |
| `socket_buffer_size` | uint32_t | 1024 | No | Socket buffer size |
| `bytes_sed_for_socket_buffer_size_transfer` | uint32_t | 0 | No | Bytes for buffer size transfer |
| `connect_timeout_seconds` | uint32_t | 10 | No | Connection timeout |
| `max_inflight_buffers` | size_t | 0 | No | Inherited from SourceDescriptor |

### Parser Config (all source types)
From `nes-sources/include/Sources/SourceDescriptor.hpp` -- `ParserConfig`:

| Config Key | Type | Default | Description |
|------------|------|---------|-------------|
| `type` | string | - | Parser type (e.g., "CSV") |
| `fieldDelimiter` | string | - | Field delimiter character |
| `tupleDelimiter` | string | - | Tuple delimiter character |
| `allowCommasInStrings` | bool | false | Allow commas in string fields |

### Sink Types

#### 1. File (`type: "File"`)
From `nes-sinks/include/Sinks/FileSink.hpp` -- `ConfigParametersFile` + `SinkDescriptor`:

| Config Key | C++ Type | Default | Required | Description |
|------------|----------|---------|----------|-------------|
| `file_path` | string | (none) | **Yes** | Output file path |
| `append` | bool | false | No | Append to existing file |
| `input_format` | enum | (none) | No | CSV or JSON |
| `add_timestamp` | bool | false | No | Add timestamp column |

#### 2. Print (`type: "Print"`)
From `nes-sinks/include/Sinks/PrintSink.hpp` -- `ConfigParametersPrint`:

| Config Key | C++ Type | Default | Required | Description |
|------------|----------|---------|----------|-------------|
| `ingestion` | uint32_t | 0 | No | Ingestion count |
| `input_format` | enum | (none) | No | CSV or JSON |

#### 3. Void (`type: "Void"`)
From `nes-plugins/Sinks/VoidSink/VoidSink.hpp` -- `ConfigParametersVoid`:

No config parameters (empty parameterMap).

#### 4. Checksum (`type: "Checksum"`)
From `nes-plugins/Sinks/ChecksumSink/ChecksumSink.hpp` -- `ConfigParametersChecksum`:

| Config Key | C++ Type | Default | Required | Description |
|------------|----------|---------|----------|-------------|
| `file_path` | string | (none) | **Yes** | Output file path for checksum |

### NES Schema Types
From CONTEXT.md decision -- the NES native types for schema field type dropdowns:
`INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64, FLOAT32, FLOAT64, BOOLEAN, CHAR, TEXT`

### YAML Format Reference
From `nes-frontend/apps/cli/tests/good/select-gen-into-void.yaml`:
```yaml
query: |
  SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK
sinks:
  - name: VOID_SINK
    schema:
      - name: GENERATOR_SOURCE$DOUBLE
        type: FLOAT64
    type: Void
    config: { }
logical:
  - name: GENERATOR_SOURCE
    schema:
      - name: DOUBLE
        type: FLOAT64
physical:
  - logical: GENERATOR_SOURCE
    parser_config:
      type: CSV
      fieldDelimiter: ","
    type: Generator
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        NORMAL_DISTRIBUTION FLOAT64 0 1
```

## Architecture Patterns

### Recommended Project Structure
```
src/
├── components/
│   ├── Canvas/           # Existing
│   ├── Toolbar/          # Existing
│   ├── Sidebar/          # NEW: right sidebar container
│   │   ├── Sidebar.tsx           # Tab container (Properties | Sources)
│   │   ├── PropertiesPanel.tsx   # Routes to type-specific panels
│   │   ├── WorkerPanel.tsx       # Worker config form
│   │   ├── SourcePanel.tsx       # Physical source config form
│   │   ├── SinkPanel.tsx         # Sink config form
│   │   ├── LogicalSourcesPanel.tsx  # Logical source CRUD list
│   │   └── SchemaBuilder.tsx     # Inline schema table component
│   └── QueryEditor/      # NEW: bottom panel
│       ├── QueryPanel.tsx        # Tab list + editor container
│       └── QueryTab.tsx          # Individual query tab
├── lib/
│   ├── types.ts          # Add Query type
│   └── sourceConfigs.ts  # NEW: NES source/sink form definitions
├── store/
│   ├── topologySlice.ts  # Add update actions for properties
│   ├── uiSlice.ts        # Add sidebar tab, bottom panel state
│   └── querySlice.ts     # NEW: query CRUD state
```

### Pattern 1: Type-Discriminated Panel Routing
**What:** Based on selected node type, render the appropriate panel component.
**When to use:** Any time a clicked node opens a context-sensitive panel.
**Example:**
```typescript
// PropertiesPanel.tsx
function PropertiesPanel() {
  const selectedNodeId = useStore(s => s.selectedNodeId);
  const worker = useStore(s => s.workers.find(w => w.id === selectedNodeId));
  const source = useStore(s => s.physicalSources.find(s => s.id === selectedNodeId));
  const sink = useStore(s => s.sinks.find(s => s.id === selectedNodeId));

  if (worker) return <WorkerPanel worker={worker} />;
  if (source) return <SourcePanel source={source} />;
  if (sink) return <SinkPanel sink={sink} />;
  return <EmptyState message="Click a node to edit its properties" />;
}
```

### Pattern 2: Fixed Form Definitions as Data
**What:** Define source/sink config forms as typed objects, not dynamic key-value pairs.
**When to use:** All source and sink configuration forms.
**Example:**
```typescript
// lib/sourceConfigs.ts
export interface FormFieldDef {
  key: string;
  label: string;
  type: 'text' | 'number' | 'select' | 'textarea' | 'boolean';
  required?: boolean;
  defaultValue?: string;
  options?: { value: string; label: string }[];
  placeholder?: string;
}

export const GENERATOR_SOURCE_CONFIG: FormFieldDef[] = [
  { key: 'seed', label: 'Seed', type: 'number', defaultValue: '1' },
  { key: 'generator_schema', label: 'Generator Schema', type: 'textarea', required: true,
    placeholder: 'SEQUENCE UINT64 0 100 1' },
  { key: 'stop_generator_when_sequence_finishes', label: 'Stop When Sequence Finishes', type: 'select',
    required: true, options: [
      { value: 'ALL', label: 'ALL' },
      { value: 'ONE', label: 'ONE' },
      { value: 'NONE', label: 'NONE' },
    ]},
  { key: 'generator_rate_type', label: 'Rate Type', type: 'select', defaultValue: 'FIXED',
    options: [
      { value: 'FIXED', label: 'FIXED' },
      { value: 'SINUS', label: 'SINUS' },
    ]},
  { key: 'generator_rate_config', label: 'Rate Config', type: 'text', defaultValue: 'emit_rate 1000' },
  { key: 'flush_interval_ms', label: 'Flush Interval (ms)', type: 'number', defaultValue: '10' },
  { key: 'max_runtime_ms', label: 'Max Runtime (ms)', type: 'number', defaultValue: '-1',
    placeholder: '-1 for unlimited' },
];

export const SOURCE_CONFIGS: Record<string, FormFieldDef[]> = {
  Generator: GENERATOR_SOURCE_CONFIG,
  File: FILE_SOURCE_CONFIG,
  TCP: TCP_SOURCE_CONFIG,
};
```

### Pattern 3: Resizable Panel with CSS + Mouse Events
**What:** Drag-to-resize sidebar and bottom panel without adding a library dependency.
**When to use:** The sidebar left edge and bottom panel top edge.
**Example:**
```typescript
function ResizablePanel({ side, minSize, maxSize, defaultSize, children }) {
  const [size, setSize] = useState(defaultSize);
  const isResizing = useRef(false);

  const onMouseDown = useCallback((e: React.MouseEvent) => {
    isResizing.current = true;
    e.preventDefault();
    const startPos = side === 'left' ? e.clientX : e.clientY;
    const startSize = size;
    const onMouseMove = (ev: MouseEvent) => {
      const delta = side === 'left'
        ? startPos - ev.clientX   // sidebar grows leftward
        : startPos - ev.clientY;  // bottom panel grows upward
      setSize(Math.min(maxSize, Math.max(minSize, startSize + delta)));
    };
    const onMouseUp = () => {
      isResizing.current = false;
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }, [size, side, minSize, maxSize]);

  return (
    <div style={{ [side === 'left' ? 'width' : 'height']: size }}>
      <div className="resize-handle" onMouseDown={onMouseDown} />
      {children}
    </div>
  );
}
```

### Pattern 4: Store Update Actions for Property Editing
**What:** Add granular update actions to topologySlice for editing individual fields.
**When to use:** Every form field change.
**Example:**
```typescript
// Add to TopologySlice interface
updateWorker: (id: string, updates: Partial<Omit<Worker, 'id'>>) => void;
updatePhysicalSource: (id: string, updates: Partial<Omit<PhysicalSource, 'id'>>) => void;
updateSink: (id: string, updates: Partial<Omit<Sink, 'id'>>) => void;
addLogicalSource: (name: string) => void;
updateLogicalSource: (id: string, updates: Partial<Omit<LogicalSource, 'id'>>) => void;
removeLogicalSource: (id: string) => void;
```

### Anti-Patterns to Avoid
- **Inline editing on canvas nodes:** Node labels should remain read-only on canvas; editing happens in the sidebar panel
- **Dynamic key-value forms for source/sink config:** Use fixed forms derived from NES Config classes, not generic `Record<string,string>` editors
- **Storing derived UI state (active tab, panel sizes) in topology state:** Keep UI state in uiSlice, domain state in topologySlice
- **Creating logical sources inline from physical source panel:** Logical sources are managed in the dedicated Sources tab only

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| SQL editor with syntax highlighting | Custom textarea + regex highlighting | Monaco Editor (`@monaco-editor/react`) | Monaco handles tokenization, selection, undo, keyboard shortcuts, accessibility |
| Code editor undo/redo | Custom undo stack for editor content | Monaco built-in undo/redo | Monaco already manages edit history |
| Autocomplete/typeahead | Custom filtered dropdown | HTML datalist or simple filtered list component | Simple enough for logical source selection; no need for a library |

**Key insight:** Monaco is the only significant new dependency. Everything else (resize, forms, tabs) is straightforward React + Tailwind.

## Common Pitfalls

### Pitfall 1: Monaco Editor Loading Delay
**What goes wrong:** Monaco loads from CDN asynchronously; the editor shows a blank area for 1-2 seconds on first render.
**Why it happens:** `@monaco-editor/react` lazy-loads Monaco from jsDelivr CDN by default.
**How to avoid:** Use the `loading` prop to show a placeholder. Optionally configure `loader.config({ paths: { vs: '/monaco-editor/min/vs' } })` to self-host for offline use.
**Warning signs:** White flash when opening query panel for the first time.

### Pitfall 2: Monaco Editor Height
**What goes wrong:** Monaco requires an explicit container height; it won't auto-size to parent.
**Why it happens:** Monaco uses absolute positioning internally.
**How to avoid:** Set explicit height on the editor container div, or use `height="100%"` with a parent that has a defined height.

### Pitfall 3: Stale Closures in Form Handlers
**What goes wrong:** Form onChange handlers capture stale store values if using inline functions with dependencies.
**Why it happens:** React closures + Zustand external store.
**How to avoid:** Use `useStore.getState()` in callbacks or ensure proper dependency arrays. For form fields, pass the current value as a controlled input and use the store action directly.

### Pitfall 4: Logical Source Deletion Cascade
**What goes wrong:** Deleting a logical source without updating physical sources that reference it leaves invisible broken state.
**Why it happens:** The decision is delete-with-orphan (intentional), but the warning badge must be visible.
**How to avoid:** After deleting a logical source, physical sources with that `logicalSourceId` should show a warning badge. Add a selector that checks whether `logicalSourceId` exists in the `logicalSources` array.

### Pitfall 5: Query Model Not Yet in Store
**What goes wrong:** Queries have no model in the current `types.ts` or store.
**Why it happens:** Phase 1 didn't need queries.
**How to avoid:** Add a `Query` type and `querySlice` to the store before building the query panel UI. Query model needs: `id`, `name` (optional string), `sql` (string).

### Pitfall 6: App Layout Restructuring
**What goes wrong:** Current App.tsx is a simple vertical flex (Toolbar + Canvas). Adding sidebar and bottom panel requires a more complex layout.
**Why it happens:** Phase 1 only needed toolbar + canvas.
**How to avoid:** Restructure App.tsx layout early. Use CSS grid or nested flexbox:
```
+-----------------------------------+
| Toolbar                           |
+-------------------+---------------+
|                   |               |
|   Canvas          |  Sidebar      |
|                   |               |
+-------------------+---------------+
| Query Panel (bottom)              |
+-----------------------------------+
```

## Code Examples

### Query Type Definition
```typescript
// Add to lib/types.ts
export interface Query {
  id: string;
  name: string;  // empty string = unnamed
  sql: string;
}
```

### Monaco Editor SQL Setup
```typescript
// Source: @monaco-editor/react docs
import Editor from '@monaco-editor/react';

function SqlEditor({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  return (
    <Editor
      height="100%"
      defaultLanguage="sql"
      value={value}
      onChange={(v) => onChange(v ?? '')}
      options={{
        minimap: { enabled: false },
        lineNumbers: 'on',
        fontSize: 13,
        scrollBeyondLastLine: false,
        wordWrap: 'on',
        automaticLayout: true,
      }}
      loading={<div className="p-4 text-gray-400">Loading editor...</div>}
    />
  );
}
```

### Schema Builder Component
```typescript
// SchemaBuilder.tsx
const NES_TYPES = [
  'INT8', 'INT16', 'INT32', 'INT64',
  'UINT8', 'UINT16', 'UINT32', 'UINT64',
  'FLOAT32', 'FLOAT64',
  'BOOLEAN', 'CHAR', 'TEXT',
] as const;

function SchemaBuilder({ fields, onChange }: {
  fields: SchemaField[];
  onChange: (fields: SchemaField[]) => void;
}) {
  const addField = () => onChange([...fields, { name: '', type: 'INT32' }]);
  const removeField = (i: number) => onChange(fields.filter((_, idx) => idx !== i));
  const updateField = (i: number, updates: Partial<SchemaField>) =>
    onChange(fields.map((f, idx) => idx === i ? { ...f, ...updates } : f));

  return (
    <div>
      {fields.map((field, i) => (
        <div key={i} className="flex gap-2 items-center mb-1">
          <input value={field.name} onChange={e => updateField(i, { name: e.target.value })}
                 className="flex-1 px-2 py-1 border rounded text-sm" placeholder="Field name" />
          <select value={field.type} onChange={e => updateField(i, { type: e.target.value })}
                  className="px-2 py-1 border rounded text-sm">
            {NES_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
          <button onClick={() => removeField(i)} className="text-red-500 text-sm px-1">-</button>
        </div>
      ))}
      <button onClick={addField} className="text-blue-500 text-sm">+ Add field</button>
    </div>
  );
}
```

### Logical Source Autocomplete
```typescript
function LogicalSourceSelect({ value, onChange }: { value: string; onChange: (id: string) => void }) {
  const logicalSources = useStore(s => s.logicalSources);
  const [filter, setFilter] = useState('');
  const selectedName = logicalSources.find(ls => ls.id === value)?.name ?? '';

  const filtered = logicalSources.filter(ls =>
    ls.name.toLowerCase().includes(filter.toLowerCase())
  );

  return (
    <div className="relative">
      <input
        value={filter || selectedName}
        onChange={e => setFilter(e.target.value)}
        onFocus={() => setFilter('')}
        placeholder="Search logical sources..."
        className="w-full px-2 py-1 border rounded text-sm"
      />
      {filter !== '' && (
        <ul className="absolute z-10 w-full bg-white border rounded shadow-md max-h-40 overflow-y-auto">
          {filtered.map(ls => (
            <li key={ls.id} onClick={() => { onChange(ls.id); setFilter(''); }}
                className="px-2 py-1 hover:bg-blue-50 cursor-pointer text-sm">
              {ls.name}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| react-monaco-editor (webpack plugin) | @monaco-editor/react (CDN-loaded) | 2020+ | No webpack config needed; works with Vite out of the box |
| Controlled inputs with Redux | Zustand slices with selectors | Project standard | Simpler, less boilerplate |

**Deprecated/outdated:**
- `react-monaco-editor`: requires webpack plugin for Monaco worker bundling; `@monaco-editor/react` handles this automatically

## Open Questions

1. **Parser config form scope**
   - What we know: ParserConfig has type, fieldDelimiter, tupleDelimiter, allowCommasInStrings. In YAML examples, parser_config appears on every physical source.
   - What's unclear: Should parser config be a separate form section or inline with source config?
   - Recommendation: Show as a collapsible "Parser Config" section within the source panel, since it applies to all source types.

2. **Sink schema editing**
   - What we know: Sinks have a `schema` field in the YAML. The Sink type has `schema: SchemaField[]`.
   - What's unclear: Should users edit sink schemas in Phase 2, or is that derived from queries in later phases?
   - Recommendation: Include a read-only schema display or a simple schema builder similar to logical sources. The schema is part of the Sink model already.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Vitest 3.x with jsdom |
| Config file | `nes-topology-editor/vitest.config.ts` |
| Quick run command | `cd nes-topology-editor && npx vitest run --reporter=verbose` |
| Full suite command | `cd nes-topology-editor && npx vitest run` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PROP-01 | Worker panel opens on click with host/grpc/capacity fields | unit | `npx vitest run src/components/Sidebar/WorkerPanel.test.tsx -x` | Wave 0 |
| PROP-02 | Source panel shows logical source + source config | unit | `npx vitest run src/components/Sidebar/SourcePanel.test.tsx -x` | Wave 0 |
| PROP-03 | Sink panel shows sink type + config | unit | `npx vitest run src/components/Sidebar/SinkPanel.test.tsx -x` | Wave 0 |
| PROP-04 | Typed inputs with validation | unit | `npx vitest run src/components/Sidebar/FormFields.test.tsx -x` | Wave 0 |
| PROP-05 | Schema builder add/remove/edit fields | unit | `npx vitest run src/components/Sidebar/SchemaBuilder.test.tsx -x` | Wave 0 |
| SRCE-01 | Logical source CRUD | unit | `npx vitest run src/store/topologySlice.test.ts -x` | Partial (store exists, needs CRUD tests) |
| SRCE-02 | Schema field name + type | unit | `npx vitest run src/components/Sidebar/SchemaBuilder.test.tsx -x` | Wave 0 |
| SRCE-03 | Physical source links to logical source | unit | `npx vitest run src/components/Sidebar/SourcePanel.test.tsx -x` | Wave 0 |
| QURY-01 | Add query to list | unit | `npx vitest run src/store/querySlice.test.ts -x` | Wave 0 |
| QURY-02 | Remove query from list | unit | `npx vitest run src/store/querySlice.test.ts -x` | Wave 0 |
| QURY-03 | SQL editor with syntax highlighting | unit | `npx vitest run src/components/QueryEditor/QueryPanel.test.tsx -x` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cd nes-topology-editor && npx vitest run --reporter=verbose`
- **Per wave merge:** `cd nes-topology-editor && npx vitest run`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `src/store/querySlice.test.ts` -- covers QURY-01, QURY-02
- [ ] `src/components/Sidebar/WorkerPanel.test.tsx` -- covers PROP-01
- [ ] `src/components/Sidebar/SourcePanel.test.tsx` -- covers PROP-02, SRCE-03
- [ ] `src/components/Sidebar/SinkPanel.test.tsx` -- covers PROP-03
- [ ] `src/components/Sidebar/SchemaBuilder.test.tsx` -- covers PROP-05, SRCE-02
- [ ] `src/components/Sidebar/LogicalSourcesPanel.test.tsx` -- covers SRCE-01
- [ ] `src/components/QueryEditor/QueryPanel.test.tsx` -- covers QURY-03
- [ ] Monaco editor may need mocking in jsdom (no real DOM rendering)

## Sources

### Primary (HIGH confidence)
- NES C++ codebase -- `nes-plugins/Sources/GeneratorSource/GeneratorSource.hpp`, `nes-plugins/Sources/TCPSource/TCPSource.hpp`, `nes-sources/private/FileSource.hpp` for source configs
- NES C++ codebase -- `nes-sinks/include/Sinks/FileSink.hpp`, `nes-sinks/include/Sinks/PrintSink.hpp`, `nes-plugins/Sinks/VoidSink/VoidSink.hpp`, `nes-plugins/Sinks/ChecksumSink/ChecksumSink.hpp` for sink configs
- NES C++ codebase -- `nes-sources/include/Sources/SourceDescriptor.hpp` for ParserConfig struct
- NES YAML test files -- `nes-frontend/apps/cli/tests/good/select-gen-into-void.yaml` for YAML format
- Existing project code -- `src/lib/types.ts`, `src/store/topologySlice.ts`, `src/store/uiSlice.ts`, `src/App.tsx`

### Secondary (MEDIUM confidence)
- [@monaco-editor/react npm page](https://www.npmjs.com/package/@monaco-editor/react) -- version 4.7.0, CDN-loaded Monaco wrapper

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - only new dependency is well-established @monaco-editor/react
- Architecture: HIGH - patterns follow existing project conventions (Zustand slices, Tailwind, component structure)
- NES config inventory: HIGH - directly read from C++ source headers in this repository
- Pitfalls: MEDIUM - Monaco integration pitfalls based on general knowledge, verified against docs

**Research date:** 2026-03-14
**Valid until:** 2026-04-14 (stable -- NES config classes change infrequently)
