import { describe, it, expect } from 'vitest';
import { getLayoutedElements } from '../lib/layout';
import type { Node, Edge } from '@xyflow/react';

function makeNode(id: string, type = 'worker'): Node {
  return { id, type, position: { x: 0, y: 0 }, data: {} };
}

function makeEdge(source: string, target: string): Edge {
  return { id: `${source}-${target}`, source, target };
}

describe('getLayoutedElements', () => {
  it('positions upstream nodes above downstream (TB direction)', () => {
    const nodes = [makeNode('A'), makeNode('B'), makeNode('C')];
    const edges = [makeEdge('A', 'B'), makeEdge('B', 'C')];

    const result = getLayoutedElements(nodes, edges);

    const posA = result.nodes.find((n) => n.id === 'A')!.position;
    const posB = result.nodes.find((n) => n.id === 'B')!.position;
    const posC = result.nodes.find((n) => n.id === 'C')!.position;

    expect(posA.y).toBeLessThan(posB.y);
    expect(posB.y).toBeLessThan(posC.y);
  });

  it('uses larger dimensions for worker nodes than source/sink nodes', () => {
    // Workers should be 200x100, sources/sinks 120x60
    // We verify indirectly: with same graph shape, different types produce different spacing
    const workerNodes = [makeNode('A', 'worker'), makeNode('B', 'worker')];
    const sourceNodes = [makeNode('A', 'source'), makeNode('B', 'source')];
    const edges = [makeEdge('A', 'B')];

    const workerResult = getLayoutedElements(workerNodes, edges);
    const sourceResult = getLayoutedElements(sourceNodes, edges);

    const workerGap = Math.abs(
      workerResult.nodes[1]!.position.y - workerResult.nodes[0]!.position.y,
    );
    const sourceGap = Math.abs(
      sourceResult.nodes[1]!.position.y - sourceResult.nodes[0]!.position.y,
    );

    // Worker nodes are taller (100 vs 60), so vertical gap should be larger
    expect(workerGap).toBeGreaterThan(sourceGap);
  });

  it('preserves all edges', () => {
    const nodes = [makeNode('A'), makeNode('B'), makeNode('C')];
    const edges = [makeEdge('A', 'B'), makeEdge('B', 'C')];

    const result = getLayoutedElements(nodes, edges);
    expect(result.edges).toHaveLength(2);
    expect(result.edges.map((e) => e.id)).toEqual(['A-B', 'B-C']);
  });

  it('returns empty for empty graph', () => {
    const result = getLayoutedElements([], []);
    expect(result.nodes).toHaveLength(0);
    expect(result.edges).toHaveLength(0);
  });
});
