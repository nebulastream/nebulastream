import type { Node, Edge } from '@xyflow/react';
import { generateId } from './ids';

export interface ClipboardData {
  nodes: Node[];
  edges: Edge[];
}

/**
 * Copy selected nodes and the edges between them.
 * Edges to unselected nodes are excluded.
 */
export function copySelectedNodes(
  selectedNodeIds: string[],
  allNodes: Node[],
  allEdges: Edge[],
): ClipboardData {
  const selectedSet = new Set(selectedNodeIds);

  const nodes = allNodes.filter((n) => selectedSet.has(n.id));
  const edges = allEdges.filter(
    (e) => selectedSet.has(e.source) && selectedSet.has(e.target),
  );

  return { nodes, edges };
}

/**
 * Paste nodes from clipboard data, generating new IDs and offsetting positions.
 * Edge source/target references are remapped to the new node IDs.
 */
export function pasteNodes(
  clipboard: ClipboardData,
  offset: { x: number; y: number } = { x: 50, y: 50 },
): { nodes: Node[]; edges: Edge[] } {
  // Build old -> new ID mapping
  const idMap = new Map<string, string>();
  for (const node of clipboard.nodes) {
    idMap.set(node.id, generateId());
  }

  const nodes = clipboard.nodes.map((node) => ({
    ...node,
    id: idMap.get(node.id)!,
    position: {
      x: node.position.x + offset.x,
      y: node.position.y + offset.y,
    },
  }));

  const edges = clipboard.edges.map((edge) => ({
    ...edge,
    id: generateId(),
    source: idMap.get(edge.source)!,
    target: idMap.get(edge.target)!,
  }));

  return { nodes, edges };
}
