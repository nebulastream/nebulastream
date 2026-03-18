import { describe, it, expect } from 'vitest';
import { isValidConnectionType, wouldCreateCycle } from '../lib/validation';
import type { Node, Edge } from '@xyflow/react';

describe('isValidConnectionType', () => {
  // Valid connections: worker->worker, source->worker, sink->worker
  it('allows worker -> worker', () => {
    expect(isValidConnectionType('worker', 'worker')).toBe(true);
  });

  it('allows source -> worker', () => {
    expect(isValidConnectionType('source', 'worker')).toBe(true);
  });

  it('allows worker -> sink', () => {
    expect(isValidConnectionType('worker', 'sink')).toBe(true);
  });

  // Invalid connections
  it('rejects source -> source', () => {
    expect(isValidConnectionType('source', 'source')).toBe(false);
  });

  it('rejects sink -> sink', () => {
    expect(isValidConnectionType('sink', 'sink')).toBe(false);
  });

  it('rejects worker -> source', () => {
    expect(isValidConnectionType('worker', 'source')).toBe(false);
  });

  it('rejects sink -> worker', () => {
    expect(isValidConnectionType('sink', 'worker')).toBe(false);
  });

  it('rejects source -> sink', () => {
    expect(isValidConnectionType('source', 'sink')).toBe(false);
  });

  it('rejects sink -> source', () => {
    expect(isValidConnectionType('sink', 'source')).toBe(false);
  });
});

describe('wouldCreateCycle', () => {
  function makeNode(id: string): Node {
    return { id, position: { x: 0, y: 0 }, data: {} };
  }

  function makeEdge(source: string, target: string): Edge {
    return { id: `${source}-${target}`, source, target };
  }

  it('detects cycle: A->B->C, adding C->A', () => {
    const nodes = [makeNode('A'), makeNode('B'), makeNode('C')];
    const edges = [makeEdge('A', 'B'), makeEdge('B', 'C')];
    expect(wouldCreateCycle(nodes, edges, { source: 'C', target: 'A' })).toBe(true);
  });

  it('allows shortcut: A->B->C, adding A->C (no cycle)', () => {
    const nodes = [makeNode('A'), makeNode('B'), makeNode('C')];
    const edges = [makeEdge('A', 'B'), makeEdge('B', 'C')];
    expect(wouldCreateCycle(nodes, edges, { source: 'A', target: 'C' })).toBe(false);
  });

  it('allows unrelated edge: A->B, adding C->A (no cycle)', () => {
    const nodes = [makeNode('A'), makeNode('B'), makeNode('C')];
    const edges = [makeEdge('A', 'B')];
    expect(wouldCreateCycle(nodes, edges, { source: 'C', target: 'A' })).toBe(false);
  });

  it('detects self-loop: A->A', () => {
    const nodes = [makeNode('A')];
    const edges: Edge[] = [];
    expect(wouldCreateCycle(nodes, edges, { source: 'A', target: 'A' })).toBe(true);
  });

  it('allows diamond DAG: A->B, A->C, B->D, C->D, adding A->D', () => {
    const nodes = [makeNode('A'), makeNode('B'), makeNode('C'), makeNode('D')];
    const edges = [
      makeEdge('A', 'B'),
      makeEdge('A', 'C'),
      makeEdge('B', 'D'),
      makeEdge('C', 'D'),
    ];
    expect(wouldCreateCycle(nodes, edges, { source: 'A', target: 'D' })).toBe(false);
  });

  it('detects cycle in complex graph: A->B->C->D, adding D->B', () => {
    const nodes = [makeNode('A'), makeNode('B'), makeNode('C'), makeNode('D')];
    const edges = [makeEdge('A', 'B'), makeEdge('B', 'C'), makeEdge('C', 'D')];
    expect(wouldCreateCycle(nodes, edges, { source: 'D', target: 'B' })).toBe(true);
  });
});
