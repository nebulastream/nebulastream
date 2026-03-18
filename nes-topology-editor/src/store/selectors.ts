import { MarkerType, type Node, type Edge } from '@xyflow/react';
import type { StoreState } from './index';
import { validateTopology, type ValidationError } from '../lib/validation';

/**
 * Stable color palette for logical source coloring.
 * Colors are indexed by logical source ID hash.
 */
const SOURCE_COLORS = [
  '#3b82f6', // blue
  '#10b981', // emerald
  '#f59e0b', // amber
  '#ef4444', // red
  '#8b5cf6', // violet
  '#ec4899', // pink
  '#06b6d4', // cyan
  '#84cc16', // lime
] as const;

function getSourceColor(logicalSourceId: string): string {
  let hash = 0;
  for (let i = 0; i < logicalSourceId.length; i++) {
    hash = (hash * 31 + logicalSourceId.charCodeAt(i)) | 0;
  }
  return SOURCE_COLORS[Math.abs(hash) % SOURCE_COLORS.length]!;
}

export function selectReactFlowNodes(state: StoreState): Node[] {
  const workerNodes: Node[] = state.workers.map((w) => ({
    id: w.id,
    type: 'worker',
    position: w.position,
    data: { worker: w },
  }));

  const sourceNodes: Node[] = state.physicalSources.map((s) => ({
    id: s.id,
    type: 'source',
    position: s.position,
    data: { source: s, color: getSourceColor(s.logicalSourceId) },
  }));

  const sinkNodes: Node[] = state.sinks.map((s) => ({
    id: s.id,
    type: 'sink',
    position: s.position,
    data: { sink: s },
  }));

  return [...workerNodes, ...sourceNodes, ...sinkNodes];
}

/**
 * Compute a validation error map keyed by nodeId from the current store state.
 */
export function selectValidationErrorMap(state: StoreState): Map<string, ValidationError[]> {
  const errors = validateTopology(
    state.workers,
    state.logicalSources,
    state.physicalSources,
    state.sinks,
  );
  const map = new Map<string, ValidationError[]>();
  for (const error of errors) {
    const existing = map.get(error.nodeId);
    if (existing) {
      existing.push(error);
    } else {
      map.set(error.nodeId, [error]);
    }
  }
  return map;
}

export function selectReactFlowEdges(state: StoreState): Edge[] {
  const edges: Edge[] = [];

  // Worker downstream edges: worker bottom → worker top
  for (const worker of state.workers) {
    for (const targetId of worker.downstream) {
      edges.push({
        id: `downstream-${worker.id}-${targetId}`,
        source: worker.id,
        sourceHandle: 'out',
        target: targetId,
        targetHandle: 'in',
        type: 'default',
        markerEnd: { type: MarkerType.ArrowClosed },
      });
    }
  }

  // Source-to-worker edges: source right → worker left (data flows source → worker)
  for (const source of state.physicalSources) {
    if (source.hostWorkerId) {
      edges.push({
        id: `source-worker-${source.id}-${source.hostWorkerId}`,
        source: source.id,
        sourceHandle: 'attach',
        target: source.hostWorkerId,
        targetHandle: 'sources',
        type: 'default',
        deletable: false,
        markerEnd: { type: MarkerType.ArrowClosed },
      });
    }
  }

  // Worker-to-sink edges: worker right → sink left (data flows worker → sink)
  for (const sink of state.sinks) {
    if (sink.hostWorkerId) {
      edges.push({
        id: `sink-worker-${sink.id}-${sink.hostWorkerId}`,
        source: sink.hostWorkerId,
        sourceHandle: 'sinks',
        target: sink.id,
        targetHandle: 'attach',
        type: 'default',
        deletable: false,
        markerEnd: { type: MarkerType.ArrowClosed },
      });
    }
  }

  return edges;
}
