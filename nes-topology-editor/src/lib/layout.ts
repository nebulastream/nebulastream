import dagre from '@dagrejs/dagre';
import type { Node, Edge } from '@xyflow/react';

const WORKER_WIDTH = 200;
const WORKER_HEIGHT = 100;
const SMALL_WIDTH = 120;
const SMALL_HEIGHT = 60;

function getNodeDimensions(type: string | undefined): { width: number; height: number } {
  if (type === 'source' || type === 'sink') {
    return { width: SMALL_WIDTH, height: SMALL_HEIGHT };
  }
  return { width: WORKER_WIDTH, height: WORKER_HEIGHT };
}

/**
 * Auto-layout nodes using dagre hierarchical layout.
 * Returns new node array with updated positions and the original edges.
 */
export function getLayoutedElements(
  nodes: Node[],
  edges: Edge[],
  direction: 'TB' | 'LR' = 'TB',
): { nodes: Node[]; edges: Edge[] } {
  if (nodes.length === 0) {
    return { nodes: [], edges: [] };
  }

  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: direction, nodesep: 80, ranksep: 100 });

  for (const node of nodes) {
    const { width, height } = getNodeDimensions(node.type);
    g.setNode(node.id, { width, height });
  }

  for (const edge of edges) {
    g.setEdge(edge.source, edge.target);
  }

  dagre.layout(g);

  const layoutedNodes = nodes.map((node) => {
    const dagreNode = g.node(node.id);
    const { width, height } = getNodeDimensions(node.type);

    return {
      ...node,
      position: {
        // dagre returns center positions; offset to top-left for React Flow
        x: dagreNode.x - width / 2,
        y: dagreNode.y - height / 2,
      },
    };
  });

  return { nodes: layoutedNodes, edges };
}
