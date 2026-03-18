import { describe, it, expect } from 'vitest';
import { copySelectedNodes, pasteNodes } from '../lib/clipboard';
import type { Node, Edge } from '@xyflow/react';

function makeNode(id: string, x = 0, y = 0): Node {
  return { id, type: 'worker', position: { x, y }, data: {} };
}

function makeEdge(source: string, target: string): Edge {
  return { id: `${source}-${target}`, source, target };
}

describe('copySelectedNodes', () => {
  it('copies selected nodes and edges between them', () => {
    const nodes = [makeNode('A'), makeNode('B'), makeNode('C')];
    const edges = [makeEdge('A', 'B'), makeEdge('B', 'C')];

    const clipboard = copySelectedNodes(['A', 'B'], nodes, edges);

    expect(clipboard.nodes).toHaveLength(2);
    expect(clipboard.edges).toHaveLength(1);
    expect(clipboard.edges[0]!.source).toBe('A');
    expect(clipboard.edges[0]!.target).toBe('B');
  });

  it('excludes edges to unselected nodes', () => {
    const nodes = [makeNode('A'), makeNode('B'), makeNode('C')];
    const edges = [makeEdge('A', 'B'), makeEdge('B', 'C')];

    const clipboard = copySelectedNodes(['A', 'B'], nodes, edges);

    // B->C should not be included because C is not selected
    const edgeIds = clipboard.edges.map((e) => e.id);
    expect(edgeIds).not.toContain('B-C');
  });
});

describe('pasteNodes', () => {
  it('generates new IDs for all nodes and edges', () => {
    const clipboard = {
      nodes: [makeNode('A', 100, 200), makeNode('B', 300, 400)],
      edges: [makeEdge('A', 'B')],
    };

    const result = pasteNodes(clipboard);

    // All IDs should be different from originals
    expect(result.nodes[0]!.id).not.toBe('A');
    expect(result.nodes[1]!.id).not.toBe('B');
    expect(result.edges[0]!.id).not.toBe('A-B');
  });

  it('offsets positions by +50, +50', () => {
    const clipboard = {
      nodes: [makeNode('A', 100, 200)],
      edges: [],
    };

    const result = pasteNodes(clipboard);

    expect(result.nodes[0]!.position.x).toBe(150);
    expect(result.nodes[0]!.position.y).toBe(250);
  });

  it('updates edge source/target to new IDs', () => {
    const clipboard = {
      nodes: [makeNode('A'), makeNode('B')],
      edges: [makeEdge('A', 'B')],
    };

    const result = pasteNodes(clipboard);

    const newNodeIds = result.nodes.map((n) => n.id);
    expect(newNodeIds).toContain(result.edges[0]!.source);
    expect(newNodeIds).toContain(result.edges[0]!.target);
    // And they should not reference old IDs
    expect(result.edges[0]!.source).not.toBe('A');
    expect(result.edges[0]!.target).not.toBe('B');
  });

  it('does not include edges to nodes outside the selection', () => {
    // Clipboard only contains nodes A and B, edge A->B
    // There should be no dangling edges
    const clipboard = {
      nodes: [makeNode('A'), makeNode('B')],
      edges: [makeEdge('A', 'B')],
    };

    const result = pasteNodes(clipboard);

    // All edge endpoints should reference nodes in the result
    const nodeIds = new Set(result.nodes.map((n) => n.id));
    for (const edge of result.edges) {
      expect(nodeIds.has(edge.source)).toBe(true);
      expect(nodeIds.has(edge.target)).toBe(true);
    }
  });
});
