import React, { useCallback, useState, useEffect, useMemo, useRef } from 'react';
import {
  ReactFlow,
  Background,
  BackgroundVariant,
  MiniMap,
  Controls,
  type Node,
  type Edge,
  type OnNodeDrag,
  type OnNodesChange,
  type OnEdgesChange,
  type OnConnect,
  type OnDelete,
  type IsValidConnection,
  applyNodeChanges,
  applyEdgeChanges,
} from '@xyflow/react';
import { useStore } from '../../store';
import { selectReactFlowNodes, selectReactFlowEdges, selectValidationErrorMap } from '../../store/selectors';
import { nodeTypes } from './nodeTypes';
import { isValidConnectionType, wouldCreateCycle } from '../../lib/validation';
import { copySelectedNodes, pasteNodes } from '../../lib/clipboard';
import { applyAutoLayout } from '../../lib/autoLayout';
import { Trash2 } from 'lucide-react';

// Node dimensions used for overlap detection
const NODE_DIMS: Record<string, { w: number; h: number }> = {
  worker: { w: 200, h: 80 },
  source: { w: 120, h: 50 },
  sink: { w: 120, h: 50 },
};

const DEFAULT_DIMS = NODE_DIMS.worker!;

/** Find the node under the dragged node's center (excluding itself) */
function findDropTarget(draggedNode: Node, allNodes: Node[]): Node | null {
  const dims = NODE_DIMS[draggedNode.type ?? 'worker'] ?? DEFAULT_DIMS;
  const cx = draggedNode.position.x + dims.w / 2;
  const cy = draggedNode.position.y + dims.h / 2;

  for (const other of allNodes) {
    if (other.id === draggedNode.id) continue;
    const od = NODE_DIMS[other.type ?? 'worker'] ?? DEFAULT_DIMS;
    if (
      cx >= other.position.x &&
      cx <= other.position.x + od.w &&
      cy >= other.position.y &&
      cy <= other.position.y + od.h
    ) {
      return other;
    }
  }
  return null;
}

/** Build a structural fingerprint string that changes only on topology structure changes */
function buildStructuralFingerprint(
  workers: { id: string; downstream: string[] }[],
  physicalSources: { id: string; hostWorkerId: string }[],
  sinks: { id: string; hostWorkerId: string }[],
): string {
  const wPart = workers
    .map((w) => `${w.id}:${w.downstream.join(',')}`)
    .join(';');
  const sPart = physicalSources.map((s) => `${s.id}:${s.hostWorkerId}`).join(';');
  const kPart = sinks.map((s) => `${s.id}:${s.hostWorkerId}`).join(';');
  return `${workers.length}|${physicalSources.length}|${sinks.length}|${wPart}|${sPart}|${kPart}`;
}

const Canvas: React.FC = () => {
  // Select raw store arrays (stable references) to avoid Zustand infinite re-render.
  const workers = useStore((s) => s.workers);
  const physicalSources = useStore((s) => s.physicalSources);
  const sinks = useStore((s) => s.sinks);
  const logicalSources = useStore((s) => s.logicalSources);

  // Derive ReactFlow nodes/edges from store data (memoized).
  const storeState = useMemo(
    () => ({ workers, physicalSources, sinks, logicalSources }) as Parameters<typeof selectReactFlowNodes>[0],
    [workers, physicalSources, sinks, logicalSources],
  );
  const validationErrorMap = useMemo(
    () => selectValidationErrorMap(storeState),
    [storeState],
  );
  const storeNodes = useMemo(() => {
    const nodes = selectReactFlowNodes(storeState);
    // Inject validation errors into each node's data
    return nodes.map((n) => {
      const nodeErrors = validationErrorMap.get(n.id);
      if (nodeErrors && nodeErrors.length > 0) {
        return { ...n, data: { ...n.data, validationErrors: nodeErrors } };
      }
      return n;
    });
  }, [storeState, validationErrorMap]);
  const storeEdges = useMemo(
    () => selectReactFlowEdges(storeState),
    [storeState],
  );

  // Local state for ReactFlow (controlled component).
  const [rfNodes, setRfNodes] = useState<Node[]>(storeNodes);
  const [rfEdges, setRfEdges] = useState<Edge[]>(storeEdges);

  // Track which node is being hovered during drag for visual feedback
  const [dropTargetId, setDropTargetId] = useState<string | null>(null);

  // Sync from store -> local state when store data changes.
  useEffect(() => { setRfNodes(storeNodes); }, [storeNodes]);
  useEffect(() => { setRfEdges(storeEdges); }, [storeEdges]);

  const connectDownstream = useStore((s) => s.connectDownstream);
  const disconnectDownstream = useStore((s) => s.disconnectDownstream);
  const attachSourceToWorker = useStore((s) => s.attachSourceToWorker);
  const attachSinkToWorker = useStore((s) => s.attachSinkToWorker);
  const removeWorker = useStore((s) => s.removeWorker);
  const removePhysicalSource = useStore((s) => s.removePhysicalSource);
  const removeSink = useStore((s) => s.removeSink);
  const addNodes = useStore((s) => s.addNodes);
  const setClipboard = useStore((s) => s.setClipboard);
  const clipboard = useStore((s) => s.clipboard);
  const selectNode = useStore((s) => s.selectNode);
  const setSidebarTab = useStore((s) => s.setSidebarTab);
  const selectedNodeId = useStore((s) => s.selectedNodeId);
  const [toast, setToast] = useState<string | null>(null);

  // Structural fingerprint for auto-layout on topology changes (including YAML import)
  const structuralFingerprint = useMemo(
    () => buildStructuralFingerprint(workers, physicalSources, sinks),
    [workers, physicalSources, sinks],
  );

  // Track previous fingerprint to detect structural changes
  const prevFingerprintRef = useRef<string | null>(null);

  // Auto-layout when structural identity changes (mount, add/remove nodes, YAML import)
  useEffect(() => {
    if (prevFingerprintRef.current === null) {
      // Initial mount -- apply layout if there are nodes
      if (workers.length > 0 || physicalSources.length > 0 || sinks.length > 0) {
        applyAutoLayout();
      }
    } else if (prevFingerprintRef.current !== structuralFingerprint) {
      applyAutoLayout();
    }
    prevFingerprintRef.current = structuralFingerprint;
  }, [structuralFingerprint, workers.length, physicalSources.length, sinks.length]);

  // Sync store selectedNodeId -> React Flow node.selected property
  useEffect(() => {
    setRfNodes((nds) =>
      nds.map((n) => (n.selected === (n.id === selectedNodeId) ? n : { ...n, selected: n.id === selectedNodeId })),
    );
  }, [selectedNodeId]);

  // Auto-dismiss toast
  useEffect(() => {
    if (toast) {
      const timer = setTimeout(() => setToast(null), 2500);
      return () => clearTimeout(timer);
    }
  }, [toast]);

  // Apply ReactFlow internal changes (selection, dimensions, position) to local state.
  const onNodesChange: OnNodesChange = useCallback(
    (changes) => {
      setRfNodes((nds) => applyNodeChanges(changes, nds));
    },
    [],
  );

  const onEdgesChange: OnEdgesChange = useCallback(
    (changes) => {
      setRfEdges((eds) => applyEdgeChanges(changes, eds));
    },
    [],
  );

  // Select node in store and switch sidebar to properties tab on click
  const onNodeClick = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      selectNode(node.id);
      setSidebarTab('properties');
    },
    [selectNode, setSidebarTab],
  );

  // Deselect node when clicking empty canvas
  const onPaneClick = useCallback(() => {
    selectNode(null);
  }, [selectNode]);

  // Show drop highlight while dragging a node over another
  const onNodeDrag: OnNodeDrag = useCallback(
    (_event, node) => {
      const target = findDropTarget(node, rfNodes);
      const targetId = target ? target.id : null;
      setDropTargetId((prev) => (prev === targetId ? prev : targetId));
    },
    [rfNodes],
  );

  const onNodeDragStop: OnNodeDrag = useCallback(
    (_event, node) => {
      setDropTargetId(null);

      // Drag-to-connect: drop any node onto another to create a connection
      const target = findDropTarget(node, rfNodes);
      if (!target) {
        // No drop target -- do NOT update position. Node snaps back to auto-layouted position.
        return;
      }

      const dragType = node.type ?? '';
      const targetType = target.type ?? '';

      // Validate the connection type (check both directions for drag)
      if (!isValidConnectionType(dragType, targetType) && !isValidConnectionType(targetType, dragType)) {
        setToast(`Cannot connect ${dragType} to ${targetType}`);
        return;
      }

      // Worker->worker: create downstream connection
      if (dragType === 'worker' && targetType === 'worker') {
        if (wouldCreateCycle(rfNodes, rfEdges, { source: node.id, target: target.id })) {
          setToast('Cannot connect: would create a cycle');
          return;
        }
        connectDownstream(node.id, target.id);
        applyAutoLayout();
      } else if (dragType === 'source' && targetType === 'worker') {
        // Re-attach source to a different worker
        attachSourceToWorker(node.id, target.id);
        applyAutoLayout();
      } else if (dragType === 'sink' && targetType === 'worker') {
        attachSinkToWorker(node.id, target.id);
        applyAutoLayout();
      } else if (dragType === 'worker' && targetType === 'sink') {
        attachSinkToWorker(target.id, node.id);
        applyAutoLayout();
      }
    },
    [rfNodes, rfEdges, connectDownstream, attachSourceToWorker, attachSinkToWorker],
  );

  const isValidConnection: IsValidConnection = useCallback(
    (connection) => {
      const sourceNode = rfNodes.find((n) => n.id === connection.source);
      const targetNode = rfNodes.find((n) => n.id === connection.target);
      if (!sourceNode || !targetNode) return false;

      const sourceType = sourceNode.type ?? '';
      const targetType = targetNode.type ?? '';

      if (!isValidConnectionType(sourceType, targetType)) {
        setToast('Invalid connection type');
        return false;
      }

      if (sourceType === 'worker' && targetType === 'worker') {
        if (wouldCreateCycle(rfNodes, rfEdges, { source: connection.source!, target: connection.target! })) {
          setToast('Cannot connect: would create a cycle');
          return false;
        }
      }

      return true;
    },
    [rfNodes, rfEdges],
  );

  const onConnect: OnConnect = useCallback(
    (connection) => {
      const sourceNode = rfNodes.find((n) => n.id === connection.source);
      const targetNode = rfNodes.find((n) => n.id === connection.target);
      if (!sourceNode || !targetNode) return;

      const sourceType = sourceNode.type;
      const targetType = targetNode.type;

      if (sourceType === 'worker' && targetType === 'worker') {
        connectDownstream(connection.source!, connection.target!);
        applyAutoLayout();
      } else if (sourceType === 'source' && targetType === 'worker') {
        attachSourceToWorker(connection.source!, connection.target!);
        applyAutoLayout();
      } else if (sourceType === 'worker' && targetType === 'sink') {
        attachSinkToWorker(connection.target!, connection.source!);
        applyAutoLayout();
      }
    },
    [rfNodes, connectDownstream, attachSourceToWorker, attachSinkToWorker],
  );

  const onDelete: OnDelete = useCallback(
    ({ nodes: deletedNodes, edges: deletedEdges }) => {
      for (const node of deletedNodes) {
        if (node.type === 'worker') {
          removeWorker(node.id);
        } else if (node.type === 'source') {
          removePhysicalSource(node.id);
        } else if (node.type === 'sink') {
          removeSink(node.id);
        }
      }

      for (const edge of deletedEdges) {
        // Only allow deleting worker->worker downstream edges.
        // Source->worker and sink->worker edges are permanent.
        if (edge.id.startsWith('downstream-')) {
          disconnectDownstream(edge.source, edge.target);
        }
      }

      // Auto-layout after structural deletion
      applyAutoLayout();
    },
    [removeWorker, removePhysicalSource, removeSink, disconnectDownstream],
  );

  // Derive selection state for delete button visibility
  const hasSelection = rfNodes.some((n) => n.selected) || rfEdges.some((e) => e.selected);

  const handleDeleteSelected = useCallback(() => {
    const selectedNodes = rfNodes.filter((n) => n.selected);
    const selectedEdges = rfEdges.filter((e) => e.selected);
    onDelete({ nodes: selectedNodes, edges: selectedEdges });
  }, [rfNodes, rfEdges, onDelete]);

  // Copy/paste keyboard handlers
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      const isModifier = e.ctrlKey || e.metaKey;
      if (!isModifier) return;

      if (e.key === 'c') {
        const selectedIds = rfNodes.filter((n) => n.selected).map((n) => n.id);
        if (selectedIds.length > 0) {
          const clipData = copySelectedNodes(selectedIds, rfNodes, rfEdges);
          setClipboard(clipData);
        }
      } else if (e.key === 'v') {
        if (clipboard) {
          const { nodes: pastedNodes, edges: pastedEdges } = pasteNodes(clipboard);
          const nodeEntries = pastedNodes.map((n) => ({
            id: n.id,
            type: n.type ?? 'worker',
            position: n.position,
            data: n.data as Record<string, unknown>,
          }));
          addNodes(nodeEntries);
          for (const edge of pastedEdges) {
            const sNode = pastedNodes.find((n) => n.id === edge.source);
            const tNode = pastedNodes.find((n) => n.id === edge.target);
            if (sNode && tNode && sNode.type === 'worker' && tNode.type === 'worker') {
              connectDownstream(edge.source, edge.target);
            }
          }
          // Auto-layout after paste
          applyAutoLayout();
        }
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [rfNodes, rfEdges, clipboard, setClipboard, addNodes, connectDownstream]);

  return (
    <div className="canvas-container" style={{ width: '100%', height: '100%', position: 'relative' }}>
      {toast && (
        <div
          className="canvas-toast"
          style={{
            position: 'absolute',
            top: 12,
            left: '50%',
            transform: 'translateX(-50%)',
            zIndex: 1000,
            background: '#ef4444',
            color: '#fff',
            padding: '6px 16px',
            borderRadius: 6,
            fontSize: 13,
            fontWeight: 500,
            pointerEvents: 'none',
          }}
          data-testid="canvas-toast"
        >
          {toast}
        </div>
      )}
      {hasSelection && (
        <button
          data-testid="delete-selected-btn"
          onClick={handleDeleteSelected}
          style={{
            position: 'absolute',
            top: 12,
            right: 12,
            zIndex: 1000,
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            background: '#fff',
            border: '1px solid #d1d5db',
            borderRadius: 6,
            padding: '6px 12px',
            fontSize: 13,
            fontWeight: 500,
            color: '#dc2626',
            cursor: 'pointer',
            boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
          }}
        >
          <Trash2 size={14} />
          Delete
        </button>
      )}
      <ReactFlow
        nodes={rfNodes.map((n) => (
          n.id === dropTargetId
            ? { ...n, className: 'drop-target' }
            : n
        ))}
        edges={rfEdges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        onNodeDrag={onNodeDrag}
        onNodeDragStop={onNodeDragStop}
        onConnect={onConnect}
        onDelete={onDelete}
        isValidConnection={isValidConnection}
        snapToGrid
        snapGrid={[20, 20]}
        deleteKeyCode={['Backspace', 'Delete']}
        fitView
      >
        <Background variant={BackgroundVariant.Dots} gap={20} />
        <MiniMap zoomable pannable />
        <Controls />
      </ReactFlow>
    </div>
  );
};

export default Canvas;
