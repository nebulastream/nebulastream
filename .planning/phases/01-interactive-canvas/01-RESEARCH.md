# Phase 1: Interactive Canvas - Research

**Researched:** 2026-03-13
**Domain:** React Flow node graph editor -- drag-and-drop canvas with custom nodes, edge validation, layout, and interaction
**Confidence:** HIGH

## Summary

Phase 1 builds the visual backbone of the topology editor: a React Flow canvas where users place worker, source, and sink nodes, connect them, and manipulate them. This is a greenfield React + TypeScript project in `nes-topology-editor/` using `@xyflow/react` v12 as the graph editor library.

The CONTEXT.md decisions define sources/sinks as **separate small nodes** connected to workers via edges (not nested/docked), which avoids the known React Flow parent-child ordering bugs (#4438, #4455). Workers are created via a "+" button, and connections use standard React Flow handle-to-handle dragging with DAG cycle prevention. The data model must map to the NES YAML topology format (workers with `host`, `grpc`, `capacity`, `downstream[]`; physical sources with `logical`, `host`, `type`; sinks with `name`, `host`, `type`).

All 12 CANV requirements map directly to React Flow built-in features or well-documented patterns: custom nodes (CANV-07), `isValidConnection` callback with DFS cycle detection (CANV-08), `snapGrid` prop (CANV-09), `<MiniMap>` component (CANV-10), dagre layout (CANV-11), and HTML Drag and Drop API for palette-to-canvas (CANV-01, CANV-02). Copy/paste (CANV-12) requires a custom implementation since the official example is Pro-only, but the pattern is straightforward (serialize selected nodes, paste with offset).

**Primary recommendation:** Use the decisions from CONTEXT.md (separate source/sink nodes with edges, not parent-child) and build the Zustand store + domain model first, then the canvas, then interactions. This order ensures the state architecture is solid before layering UI.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Sources and sinks are **separate small nodes** connected to their host worker via edges (not nested inside or docked on workers)
- Dragging a source/sink from one worker's edge to another worker reassigns the host
- Source/sink **creation** happens via the property panel "+" button with a form (Phase 2 concern) -- Phase 1 just needs the canvas representation and drag-to-reassign
- **Workers:** Large nodes displaying only the host address (e.g., "worker-1:9090"). All other details in property panel (Phase 2)
- **Sources:** Small color-coded nodes -- color represents their logical source. Icon indicates physical type (e.g., Generator, CSV)
- **Sinks:** Small nodes in a single uniform color. Icon indicates sink type (e.g., File, Void, Print)
- **Worker-to-worker:** Standard React Flow handle-to-handle dragging (drag from output handle to input handle)
- **Source/sink-to-worker:** Connected via edges; drag-to-reassign between workers
- Topology must be a **DAG** -- edge validation prevents cycles
- Invalid connection attempts show a brief toast/tooltip ("Cannot connect: would create a cycle")
- "+" button on canvas creates a new worker with auto-generated defaults ("worker-1:9090", "worker-2:9090", etc.)

### Claude's Discretion
- "+" button placement (top toolbar vs floating)
- Auto-layout direction (top-to-bottom vs left-to-right)
- Source/sink dock side relative to workers
- Worker node overflow handling when many sources/sinks are attached
- Exact icon set for physical source/sink types
- Grid snapping granularity
- Minimap styling and position
- Copy/paste behavior details

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| CANV-01 | User can drag worker nodes onto the canvas from a palette | HTML Drag and Drop API + React Flow `onDrop`/`screenToFlowPosition` -- official example pattern |
| CANV-02 | User can drag source/sink nodes onto the canvas from a palette | Same drag-and-drop pattern as CANV-01 with different node types |
| CANV-03 | User can reposition nodes by dragging | React Flow built-in -- nodes are draggable by default |
| CANV-04 | User can connect workers by dragging (creates downstream link) | React Flow handle-to-handle connection with `onConnect` callback |
| CANV-05 | User can attach sources/sinks to workers by dragging them onto a worker | Custom implementation: `onNodeDragStop` + hit-testing, or drag source/sink edge to worker handle |
| CANV-06 | User can select and delete nodes/edges | React Flow built-in selection + `deleteKeyCode` prop (Backspace by default) + `onDelete` handler |
| CANV-07 | Workers, sources, and sinks are visually distinct | Custom node components with different sizes, colors, icons via `nodeTypes` registry |
| CANV-08 | Invalid connections are prevented (edge validation) | `isValidConnection` prop with DFS cycle detection using `getOutgoers` utility |
| CANV-09 | Nodes snap to grid for clean alignment | `snapToGrid` + `snapGrid={[20, 20]}` props on `<ReactFlow>` |
| CANV-10 | Minimap shows thumbnail overview | `<MiniMap>` built-in component from `@xyflow/react` |
| CANV-11 | Auto-layout arranges nodes hierarchically | `@dagrejs/dagre` with `getLayoutedElements()` pattern from official example |
| CANV-12 | User can copy/paste nodes with their properties | Custom implementation: serialize selected nodes to clipboard, paste with position offset and new IDs |
</phase_requirements>

## Standard Stack

### Core (Phase 1 specific)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| @xyflow/react | ^12.10 | Node graph canvas | The standard for React node-based UIs. v12 supports React 19, dark mode, computed flows |
| react | 19.x | UI framework | Project constraint |
| typescript | 5.7+ | Type safety | Non-negotiable for structured YAML generation |
| zustand | ^5.x | State management | React Flow uses Zustand internally; same paradigm for app state |
| @dagrejs/dagre | latest | Auto-layout algorithm | Official React Flow example library for hierarchical DAG layout. ~426K weekly downloads |
| vite | ^6.x | Build tooling | Standard for greenfield React+TS. Sub-second HMR |

### Supporting (Phase 1 specific)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| lucide-react | latest | Icons for source/sink type indicators | Custom node rendering (Generator, CSV, File, Void, Print icons) |
| tailwindcss | 4.x | Utility CSS | Styling custom nodes, toolbar, palette |
| @tailwindcss/vite | ^4.x | Vite plugin for Tailwind | Build integration |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| @dagrejs/dagre | elkjs | ELK is more powerful but vastly more complex config. Dagre is sufficient for tree/DAG layout |
| Separate source/sink nodes | React Flow parentId nesting | parentId has known bugs (#4438, #4455). User decided separate nodes |
| Custom drag-and-drop lib | react-dnd | HTML Drag and Drop API is simpler and matches the official React Flow example |

**Installation (Phase 1 only):**
```bash
# Initialize project
npm create vite@latest nes-topology-editor -- --template react-ts
cd nes-topology-editor

# Core
npm install @xyflow/react zustand @dagrejs/dagre

# Styling
npm install tailwindcss @tailwindcss/vite lucide-react

# Dev
npm install -D @vitejs/plugin-react
```

## Architecture Patterns

### Project Structure (Phase 1)
```
nes-topology-editor/
  src/
    App.tsx                       # Layout shell, ReactFlowProvider wrapper
    main.tsx                      # Entry point
    store/
      index.ts                    # Combined Zustand store
      topologySlice.ts            # Workers, sources, sinks, edges
      uiSlice.ts                  # Selection state, panel visibility
      selectors.ts                # Derived: reactFlowNodes, reactFlowEdges
    components/
      Canvas/
        Canvas.tsx                # ReactFlow wrapper with event handlers
        WorkerNode.tsx            # Large custom node -- host address label
        SourceNode.tsx            # Small custom node -- color-coded, icon
        SinkNode.tsx              # Small custom node -- uniform color, icon
        nodeTypes.ts              # Registry: { worker, source, sink }
        edgeTypes.ts              # Edge type configs (downstream vs placement)
      Palette/
        Palette.tsx               # Sidebar with draggable node templates
      Toolbar/
        Toolbar.tsx               # "+" worker button, auto-layout trigger
    lib/
      types.ts                    # Domain model types (Worker, PhysicalSource, Sink)
      layout.ts                   # dagre layout utility
      validation.ts               # Cycle detection, connection rules
      ids.ts                      # ID generation utilities
    styles/
      nodes.css                   # Custom node styling
```

### Pattern 1: Domain Model Separate from React Flow Nodes
**What:** Maintain a domain model (Worker, PhysicalSource, Sink) in Zustand. Derive React Flow nodes/edges via selectors.
**When to use:** Always. This is the architectural foundation.
**Example:**
```typescript
// Source: .planning/research/ARCHITECTURE.md (project research)
// store/topologySlice.ts
interface TopologySlice {
  workers: Worker[];
  physicalSources: PhysicalSource[];
  sinks: Sink[];
  addWorker: () => void;
  removeWorker: (id: string) => void;
  connectDownstream: (sourceId: string, targetId: string) => void;
  attachSourceToWorker: (sourceId: string, workerId: string) => void;
  attachSinkToWorker: (sinkId: string, workerId: string) => void;
}

// store/selectors.ts
export const selectReactFlowNodes = (state: AppState): Node[] => {
  const nodes: Node[] = [];
  // Workers -> large nodes
  for (const w of state.workers) {
    nodes.push({
      id: w.id,
      type: 'worker',
      position: w.position,
      data: { worker: w },
    });
  }
  // Sources -> small nodes
  for (const s of state.physicalSources) {
    nodes.push({
      id: s.id,
      type: 'source',
      position: s.position,
      data: { source: s },
    });
  }
  // Sinks -> small nodes
  for (const sk of state.sinks) {
    nodes.push({
      id: sk.id,
      type: 'sink',
      position: sk.position,
      data: { sink: sk },
    });
  }
  return nodes;
};
```

### Pattern 2: Drag-and-Drop from Palette to Canvas
**What:** Use HTML Drag and Drop API with React Flow's `screenToFlowPosition` to add nodes.
**When to use:** CANV-01 and CANV-02 (palette to canvas).
**Example:**
```typescript
// Source: https://reactflow.dev/examples/interaction/drag-and-drop
// components/Canvas/Canvas.tsx
const onDragOver = useCallback((event: React.DragEvent) => {
  event.preventDefault();
  event.dataTransfer.dropEffect = 'move';
}, []);

const onDrop = useCallback((event: React.DragEvent) => {
  event.preventDefault();
  const type = event.dataTransfer.getData('application/reactflow');
  if (!type) return;

  const position = screenToFlowPosition({
    x: event.clientX,
    y: event.clientY,
  });

  if (type === 'worker') {
    store.getState().addWorker(position);
  }
  // Sources/sinks creation is Phase 2 (property panel "+")
  // but we can support drag-from-palette for initial prototyping
}, [screenToFlowPosition]);
```

### Pattern 3: DAG Cycle Prevention via isValidConnection
**What:** Use DFS traversal with `getOutgoers` to detect if a new edge would create a cycle.
**When to use:** CANV-08 (edge validation). Applied to worker-to-worker connections.
**Example:**
```typescript
// Source: https://reactflow.dev/examples/interaction/prevent-cycles
const isValidConnection = useCallback(
  (connection: Connection) => {
    const nodes = getNodes();
    const edges = getEdges();
    const target = nodes.find((node) => node.id === connection.target);
    if (!target) return false;

    const hasCycle = (node: Node, visited = new Set<string>()): boolean => {
      if (visited.has(node.id)) return false;
      visited.add(node.id);
      for (const outgoer of getOutgoers(node, nodes, edges)) {
        if (outgoer.id === connection.source) return true;
        if (hasCycle(outgoer, visited)) return true;
      }
      return false;
    };

    if (target.id === connection.source) return false;
    return !hasCycle(target);
  },
  [getNodes, getEdges],
);
```

### Pattern 4: Auto-Layout with Dagre
**What:** Use `@dagrejs/dagre` to compute hierarchical positions for all nodes.
**When to use:** CANV-11 (auto-layout button click).
**Example:**
```typescript
// Source: https://reactflow.dev/examples/layout/dagre
import dagre from '@dagrejs/dagre';

const getLayoutedElements = (
  nodes: Node[],
  edges: Edge[],
  direction = 'TB'
) => {
  const g = new dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: direction, nodesep: 80, ranksep: 100 });

  nodes.forEach((node) => {
    const width = node.type === 'worker' ? 200 : 120;
    const height = node.type === 'worker' ? 100 : 60;
    g.setNode(node.id, { width, height });
  });

  edges.forEach((edge) => {
    g.setEdge(edge.source, edge.target);
  });

  dagre.layout(g);

  const isHorizontal = direction === 'LR';
  const layoutedNodes = nodes.map((node) => {
    const pos = g.node(node.id);
    const width = node.type === 'worker' ? 200 : 120;
    const height = node.type === 'worker' ? 100 : 60;
    return {
      ...node,
      targetPosition: isHorizontal ? 'left' : 'top',
      sourcePosition: isHorizontal ? 'right' : 'bottom',
      position: { x: pos.x - width / 2, y: pos.y - height / 2 },
    };
  });

  return { nodes: layoutedNodes, edges };
};
```

### Pattern 5: Connection Type Validation
**What:** Enforce that only valid connections are created: worker-to-worker (downstream), source-to-worker (placement), sink-to-worker (placement). Prevent source-to-source, sink-to-sink, etc.
**When to use:** CANV-08 (in addition to cycle detection).
**Example:**
```typescript
// lib/validation.ts
type NodeType = 'worker' | 'source' | 'sink';

const VALID_CONNECTIONS: Record<string, Set<string>> = {
  'worker->worker': new Set(['downstream']),   // worker output -> worker input
  'source->worker': new Set(['placement']),     // source attaches to worker
  'sink->worker': new Set(['placement']),       // sink attaches to worker
};

export function isValidConnectionType(
  sourceType: NodeType,
  targetType: NodeType,
): boolean {
  return VALID_CONNECTIONS.has(`${sourceType}->${targetType}`);
}
```

### Anti-Patterns to Avoid
- **Using React Flow nodes as domain model:** Do NOT store domain data (host, capacity) only in React Flow node.data. Keep a separate domain model and derive RF nodes from it.
- **Using `useNodes()` inside custom node components:** Causes re-render of ALL nodes on every drag frame. Use narrow Zustand selectors or `useStore(s => s.nodes.find(...))` instead.
- **Inline custom node declarations:** Custom node components declared inside another component's render function lose React.memo benefits and cause re-mounts on every render. Declare them at module scope.
- **Using React Flow parentId for source/sink containment:** Known bugs (#4438, #4455) cause node disappearance. Use separate nodes with edges (per user decision).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| DAG layout algorithm | Custom node positioning | `@dagrejs/dagre` | Graph layout is a solved hard problem. Dagre handles edge routing, rank assignment, and overlap avoidance |
| Cycle detection | Custom graph traversal from scratch | React Flow `getOutgoers` utility + DFS | React Flow provides the traversal utility; just add the DFS wrapper |
| Node drag-and-drop | Custom mouse event tracking | HTML Drag and Drop API + `screenToFlowPosition` | Official pattern; handles coordinate transform from screen to flow space |
| Minimap | Custom SVG thumbnail | `<MiniMap>` from `@xyflow/react` | Built-in component, zero config needed |
| Grid snapping | Custom position rounding | `snapToGrid` + `snapGrid` props | Built-in feature, one prop |
| ID generation | Custom counter | `crypto.randomUUID()` or nanoid | Collision-free, standard |

## Common Pitfalls

### Pitfall 1: Re-render Avalanche in Custom Nodes
**What goes wrong:** Accessing `useNodes()` or full store state inside WorkerNode/SourceNode/SinkNode causes every node to re-render on every drag frame.
**Why it happens:** React Flow nodes are React components. Subscribing to the full nodes array means position changes (continuous during drag) trigger re-renders across all nodes.
**How to avoid:** Use narrow selectors. Custom nodes receive their own data via props (`data` from `node.data`). For store access, use `useStore(s => s.specificField)`. Wrap all custom node components in `React.memo()`.
**Warning signs:** Open React DevTools Profiler during drag -- if more than the dragged node highlights, this is happening.

### Pitfall 2: Connection Validation Must Check Both Type and Cycle
**What goes wrong:** Implementing only cycle detection without type checking, or vice versa. Users can connect source-to-source (invalid) or create worker cycles (invalid).
**Why it happens:** `isValidConnection` is a single callback. Developers implement one check and forget the other.
**How to avoid:** The `isValidConnection` function must: (1) look up source and target node types, (2) check if the connection type pairing is valid, (3) if worker-to-worker, run cycle detection.
**Warning signs:** Being able to draw an edge between two source nodes on the canvas.

### Pitfall 3: Node Position Sync Between Store and React Flow
**What goes wrong:** Node positions in the Zustand domain model get out of sync with React Flow's internal positions after drag.
**Why it happens:** React Flow manages position internally. If you also store position in your domain model but don't update it on `onNodesChange`, positions diverge.
**How to avoid:** Either (a) let React Flow own positions and only store domain data in your slice, reading positions from RF when needed (e.g., for auto-layout), or (b) sync positions on `onNodeDragStop` (not on every drag frame). Option (a) is simpler for Phase 1.
**Warning signs:** After auto-layout, nodes snap to old positions on next interaction.

### Pitfall 4: Drag-to-Reassign Source/Sink UX Ambiguity
**What goes wrong:** Users expect to drag a source node onto a different worker to reassign it, but the drag just repositions the node without changing the edge.
**Why it happens:** React Flow's default drag behavior moves the node. Reassignment requires custom logic in `onNodeDragStop` to detect overlap with a worker and update the domain model.
**How to avoid:** Implement hit-testing in `onNodeDragStop`: check if the dropped source/sink node overlaps a worker node. If yes, call `store.attachSourceToWorker(sourceId, workerId)`. Show visual feedback (worker highlight) during drag.
**Warning signs:** Source nodes can be repositioned but never change their worker attachment.

### Pitfall 5: Copy-Paste Must Generate New IDs
**What goes wrong:** Pasted nodes have the same IDs as originals, causing React Flow to corrupt the graph state.
**Why it happens:** Naive clipboard serialization copies node objects including their IDs.
**How to avoid:** On paste, generate new UUIDs for every node and edge. Update all edge source/target references to use the new IDs. Offset positions by a fixed amount (e.g., +50, +50) so pasted nodes don't overlap originals.
**Warning signs:** Pasting nodes causes the original nodes to disappear or edges to point to wrong nodes.

## Code Examples

### Custom Worker Node Component
```typescript
// Source: React Flow custom nodes docs + project CONTEXT.md decisions
// components/Canvas/WorkerNode.tsx
import { memo } from 'react';
import { Handle, Position, type NodeProps, type Node } from '@xyflow/react';
import type { Worker } from '../../lib/types';

type WorkerNodeData = { worker: Worker };
type WorkerNodeType = Node<WorkerNodeData, 'worker'>;

function WorkerNode({ data }: NodeProps<WorkerNodeType>) {
  return (
    <div className="worker-node">
      <Handle type="target" position={Position.Top} id="in" />
      <div className="worker-label">{data.worker.host}</div>
      <Handle type="source" position={Position.Bottom} id="out" />
      {/* Side handles for source/sink attachment */}
      <Handle type="target" position={Position.Left} id="sources" />
      <Handle type="target" position={Position.Right} id="sinks" />
    </div>
  );
}

export default memo(WorkerNode);
```

### Custom Source Node Component
```typescript
// components/Canvas/SourceNode.tsx
import { memo } from 'react';
import { Handle, Position, type NodeProps, type Node } from '@xyflow/react';
import { Database, FileText } from 'lucide-react';
import type { PhysicalSource } from '../../lib/types';

type SourceNodeData = { source: PhysicalSource; color: string };
type SourceNodeType = Node<SourceNodeData, 'source'>;

const SOURCE_ICONS: Record<string, React.ComponentType<any>> = {
  Generator: Database,
  CSV: FileText,
};

function SourceNode({ data }: NodeProps<SourceNodeType>) {
  const Icon = SOURCE_ICONS[data.source.type] ?? Database;
  return (
    <div
      className="source-node"
      style={{ borderColor: data.color, backgroundColor: `${data.color}20` }}
    >
      <Icon size={16} />
      <span>{data.source.type}</span>
      <Handle type="source" position={Position.Right} id="attach" />
    </div>
  );
}

export default memo(SourceNode);
```

### Node Types Registry
```typescript
// components/Canvas/nodeTypes.ts
import type { NodeTypes } from '@xyflow/react';
import WorkerNode from './WorkerNode';
import SourceNode from './SourceNode';
import SinkNode from './SinkNode';

export const nodeTypes: NodeTypes = {
  worker: WorkerNode,
  source: SourceNode,
  sink: SinkNode,
};
```

### ReactFlow Canvas Setup
```typescript
// Source: React Flow docs -- ReactFlow component props
// components/Canvas/Canvas.tsx (simplified)
import {
  ReactFlow,
  MiniMap,
  Background,
  BackgroundVariant,
  Controls,
  useReactFlow,
  type OnConnect,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { nodeTypes } from './nodeTypes';
import { useStore } from '../../store';
import { selectReactFlowNodes, selectReactFlowEdges } from '../../store/selectors';

export function Canvas() {
  const nodes = useStore(selectReactFlowNodes);
  const edges = useStore(selectReactFlowEdges);
  const { screenToFlowPosition } = useReactFlow();

  // ... onConnect, onDrop, onDragOver, isValidConnection handlers

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onConnect={onConnect}
      onDrop={onDrop}
      onDragOver={onDragOver}
      isValidConnection={isValidConnection}
      nodeTypes={nodeTypes}
      snapToGrid
      snapGrid={[20, 20]}
      deleteKeyCode="Backspace"
      fitView
    >
      <Background variant={BackgroundVariant.Dots} gap={20} size={1} />
      <MiniMap zoomable pannable />
      <Controls />
    </ReactFlow>
  );
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `reactflow` package | `@xyflow/react` | July 2024 (v12) | Must use new package name. Old `reactflow` is deprecated |
| `useNodesState` / `useEdgesState` | External Zustand store + `onNodesChange`/`onEdgesChange` | v12 recommended | Better state control, supports undo/redo, avoids internal state conflicts |
| `dagre` npm package | `@dagrejs/dagre` | 2024 | Old `dagre` is unmaintained. Use the `@dagrejs` scoped package |
| Parent-child nodes for grouping | Separate nodes with edges | User decision for this project | Avoids parentId ordering bugs |

**Deprecated/outdated:**
- `reactflow` package: Use `@xyflow/react` instead
- `dagre` package: Use `@dagrejs/dagre` instead
- React Flow's `useNodesState`/`useEdgesState` for complex apps: Use external Zustand store

## Open Questions

1. **Source/sink edge reassignment UX**
   - What we know: User wants drag-to-reassign (drop source on different worker)
   - What's unclear: Should the old edge delete automatically, or should user manually delete? Should there be a visual "snap" animation?
   - Recommendation: Auto-delete old edge on reassignment. Show worker highlight on hover during drag as visual affordance.

2. **Worker "+" button behavior when canvas is crowded**
   - What we know: "+" button creates a new worker with auto-generated defaults
   - What's unclear: Where should the new worker appear? Center of viewport? Near existing workers?
   - Recommendation: Place at center of current viewport using `screenToFlowPosition`. Auto-layout button provides cleanup.

3. **Copy/paste scope for edges**
   - What we know: CANV-12 says "copy/paste nodes with their properties"
   - What's unclear: Should edges between copied nodes also be pasted? Should edges to non-copied nodes be included?
   - Recommendation: Copy selected nodes + edges between them. Do NOT copy edges to unselected nodes.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Vitest 3.x + @testing-library/react 16.x |
| Config file | None -- Wave 0 gap |
| Quick run command | `npx vitest run --reporter=verbose` |
| Full suite command | `npx vitest run` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CANV-01 | Drag worker from palette creates worker node | integration | `npx vitest run src/__tests__/palette-dnd.test.tsx -t "worker"` | Wave 0 |
| CANV-02 | Drag source/sink from palette creates node | integration | `npx vitest run src/__tests__/palette-dnd.test.tsx -t "source"` | Wave 0 |
| CANV-03 | Node repositioning updates position | unit | `npx vitest run src/__tests__/store.test.ts -t "position"` | Wave 0 |
| CANV-04 | Worker-to-worker connection creates downstream edge | integration | `npx vitest run src/__tests__/connections.test.tsx -t "downstream"` | Wave 0 |
| CANV-05 | Source/sink attachment to worker | integration | `npx vitest run src/__tests__/connections.test.tsx -t "attach"` | Wave 0 |
| CANV-06 | Select and delete removes nodes/edges | integration | `npx vitest run src/__tests__/delete.test.tsx` | Wave 0 |
| CANV-07 | Node types are visually distinct | unit | `npx vitest run src/__tests__/nodes.test.tsx -t "distinct"` | Wave 0 |
| CANV-08 | Invalid connections prevented (cycle + type) | unit | `npx vitest run src/__tests__/validation.test.ts` | Wave 0 |
| CANV-09 | Grid snapping enabled | manual-only | Verify `snapToGrid` prop is set -- visual check | N/A |
| CANV-10 | Minimap renders | unit | `npx vitest run src/__tests__/canvas.test.tsx -t "minimap"` | Wave 0 |
| CANV-11 | Auto-layout positions nodes hierarchically | unit | `npx vitest run src/__tests__/layout.test.ts` | Wave 0 |
| CANV-12 | Copy/paste duplicates nodes with new IDs | unit | `npx vitest run src/__tests__/copy-paste.test.ts` | Wave 0 |

### Sampling Rate
- **Per task commit:** `npx vitest run --reporter=verbose`
- **Per wave merge:** `npx vitest run`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `nes-topology-editor/` project scaffold (Vite + React + TS)
- [ ] `vitest.config.ts` -- Vitest configuration with jsdom environment
- [ ] `src/__tests__/store.test.ts` -- Zustand store unit tests
- [ ] `src/__tests__/validation.test.ts` -- Cycle detection + connection type validation
- [ ] `src/__tests__/layout.test.ts` -- Dagre layout utility tests
- [ ] `src/__tests__/copy-paste.test.ts` -- Copy/paste with ID regeneration
- [ ] Framework install: `npm install -D vitest @testing-library/react jsdom`

## Sources

### Primary (HIGH confidence)
- [React Flow Drag and Drop Example](https://reactflow.dev/examples/interaction/drag-and-drop) -- palette-to-canvas pattern
- [React Flow Preventing Cycles Example](https://reactflow.dev/examples/interaction/prevent-cycles) -- DFS cycle detection with getOutgoers
- [React Flow Connection Validation Example](https://reactflow.dev/examples/interaction/validation) -- isValidConnection usage
- [React Flow Dagre Layout Example](https://reactflow.dev/examples/layout/dagre) -- dagre integration pattern
- [React Flow Custom Nodes Docs](https://reactflow.dev/learn/customization/custom-nodes) -- custom node component pattern
- [React Flow MiniMap API](https://reactflow.dev/api-reference/components/minimap) -- MiniMap component props
- [React Flow ReactFlow API](https://reactflow.dev/api-reference/react-flow) -- snapToGrid, deleteKeyCode, isValidConnection props
- [React Flow Copy and Paste Example](https://reactflow.dev/examples/interaction/copy-paste) -- Pro example reference (pattern documented, code is Pro-only)
- [React Flow useKeyPress Hook](https://reactflow.dev/api-reference/hooks/use-key-press) -- keyboard shortcut detection
- [@dagrejs/dagre on npm](https://www.npmjs.com/package/@dagrejs/dagre) -- layout library package

### Secondary (MEDIUM confidence)
- Project research at `.planning/research/` -- STACK.md, ARCHITECTURE.md, PITFALLS.md (verified against official docs)
- [React Flow Performance Guide](https://reactflow.dev/learn/advanced-use/performance) -- re-render optimization
- [React Flow Sub Flows](https://reactflow.dev/learn/layouting/sub-flows) -- parent-child node documentation

### Tertiary (LOW confidence)
- Copy/paste implementation details -- Pro example is behind paywall. Pattern is standard (serialize/deserialize selected nodes with new IDs) but exact React Flow integration details need validation during implementation.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries are mature, documented, widely used
- Architecture: HIGH -- patterns from official React Flow docs and project research
- Pitfalls: HIGH -- documented in React Flow issues and confirmed in prior project research
- Copy/paste: MEDIUM -- Pro example not publicly available; implementation pattern is clear but untested

**Research date:** 2026-03-13
**Valid until:** 2026-04-13 (stable domain, 30-day validity)
