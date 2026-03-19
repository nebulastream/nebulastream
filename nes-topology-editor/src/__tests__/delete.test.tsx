import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render } from '@testing-library/react';
import React from 'react';

const { mockFns, rfProps } = vi.hoisted(() => ({
  mockFns: {
    addWorker: vi.fn(),
    addPhysicalSource: vi.fn(),
    addSink: vi.fn(),
    updateNodePosition: vi.fn(),
    connectDownstream: vi.fn(),
    disconnectDownstream: vi.fn(),
    attachSourceToWorker: vi.fn(),
    attachSinkToWorker: vi.fn(),
    detachSourceFromWorker: vi.fn(),
    detachSinkFromWorker: vi.fn(),
    removeWorker: vi.fn(),
    removePhysicalSource: vi.fn(),
    removeSink: vi.fn(),
    addNodes: vi.fn(),
    setClipboard: vi.fn(),
    setAllNodePositions: vi.fn(),
    selectNode: vi.fn(),
    setSidebarTab: vi.fn(),
    selectedNodeId: null as string | null,
    clipboard: null as unknown,
  },
  rfProps: { captured: {} as Record<string, unknown> },
}));

const WORKER_A = {
  id: 'w-a', type: 'worker', position: { x: 0, y: 0 },
  data: { worker: { id: 'w-a', host: 'a:9090', data: 'a:8080', capacity: 10000, downstream: ['w-b'], position: { x: 0, y: 0 } } },
};
const SOURCE_1 = {
  id: 's-1', type: 'source', position: { x: 0, y: 200 },
  data: { source: { id: 's-1', logicalSourceId: '', hostWorkerId: 'w-a', type: 'Generator', sourceConfig: {}, parserConfig: {}, position: { x: 0, y: 200 } }, color: '#3b82f6' },
};
const SINK_1 = {
  id: 'k-1', type: 'sink', position: { x: 200, y: 200 },
  data: { sink: { id: 'k-1', name: 'Sink', hostWorkerId: 'w-a', type: 'Print', schema: [], config: {}, parserConfig: {}, position: { x: 200, y: 200 } } },
};
const WORKER_B = {
  id: 'w-b', type: 'worker', position: { x: 200, y: 0 },
  data: { worker: { id: 'w-b', host: 'b:9090', data: 'b:8080', capacity: 10000, downstream: [], position: { x: 200, y: 0 } } },
};

const testNodes = [WORKER_A, WORKER_B, SOURCE_1, SINK_1];
const testEdges = [
  { id: 'downstream-w-a-w-b', source: 'w-a', target: 'w-b', type: 'default' },
  { id: 'source-worker-s-1-w-a', source: 's-1', target: 'w-a', type: 'default' },
  { id: 'sink-worker-k-1-w-a', source: 'w-a', target: 'k-1', type: 'default' },
];

vi.mock('@xyflow/react', () => ({
  ReactFlow: ({ children, ...props }: Record<string, unknown>) => {
    rfProps.captured = props;
    return <div data-testid="reactflow">{children as React.ReactNode}</div>;
  },
  ReactFlowProvider: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  Background: () => <div data-testid="background" />,
  BackgroundVariant: { Dots: 'dots' },
  MiniMap: () => <div data-testid="minimap" />,
  Controls: () => <div data-testid="controls" />,
  Handle: () => <div />,
  Position: { Top: 'top', Bottom: 'bottom', Left: 'left', Right: 'right' },
  useReactFlow: () => ({
    screenToFlowPosition: ({ x, y }: { x: number; y: number }) => ({ x, y }),
    fitView: vi.fn(),
  }),
  applyNodeChanges: () => [],
  applyEdgeChanges: () => [],
}));

const stableState: Record<string, unknown> = {
  workers: [],
  physicalSources: [],
  sinks: [],
  logicalSources: [],
  clipboard: null,
  selectedNodeId: null,
  ...mockFns,
};

vi.mock('../store', () => ({
  useStore: (selector: (s: Record<string, unknown>) => unknown) => selector(stableState),
}));

vi.mock('../store/selectors', () => ({
  selectReactFlowNodes: () => testNodes,
  selectReactFlowEdges: () => testEdges,
  selectValidationErrorMap: () => new Map(),
}));

// Mock autoLayout to avoid real dagre calls
vi.mock('../lib/autoLayout', () => ({
  applyAutoLayout: () => {},
}));

vi.mock('lucide-react', () => ({
  Trash2: (props: Record<string, unknown>) => <svg data-testid="trash-icon" {...props} />,
  Database: (props: Record<string, unknown>) => <svg {...props} />,
  FileText: (props: Record<string, unknown>) => <svg {...props} />,
  FileOutput: (props: Record<string, unknown>) => <svg {...props} />,
  Circle: (props: Record<string, unknown>) => <svg {...props} />,
  Printer: (props: Record<string, unknown>) => <svg {...props} />,
  AlertTriangle: (props: Record<string, unknown>) => <svg {...props} />,
}));

import Canvas from '../components/Canvas/Canvas';

describe('Delete handling (CANV-06)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('deleting a worker node calls removeWorker with the node ID', () => {
    render(<Canvas />);
    const onDelete = rfProps.captured.onDelete as (params: { nodes: typeof testNodes; edges: typeof testEdges }) => void;
    expect(onDelete).toBeDefined();

    onDelete({ nodes: [WORKER_A], edges: [] });
    expect(mockFns.removeWorker).toHaveBeenCalledWith('w-a');
  });

  it('deleting a source node calls removePhysicalSource', () => {
    render(<Canvas />);
    const onDelete = rfProps.captured.onDelete as (params: { nodes: typeof testNodes; edges: typeof testEdges }) => void;

    onDelete({ nodes: [SOURCE_1], edges: [] });
    expect(mockFns.removePhysicalSource).toHaveBeenCalledWith('s-1');
  });

  it('deleting a sink node calls removeSink', () => {
    render(<Canvas />);
    const onDelete = rfProps.captured.onDelete as (params: { nodes: typeof testNodes; edges: typeof testEdges }) => void;

    onDelete({ nodes: [SINK_1], edges: [] });
    expect(mockFns.removeSink).toHaveBeenCalledWith('k-1');
  });

  it('deleting a downstream edge calls disconnectDownstream', () => {
    render(<Canvas />);
    const onDelete = rfProps.captured.onDelete as (params: { nodes: typeof testNodes; edges: typeof testEdges }) => void;

    onDelete({ nodes: [], edges: [testEdges[0]!] });
    expect(mockFns.disconnectDownstream).toHaveBeenCalledWith('w-a', 'w-b');
  });

  it('deleting a source-worker edge is a no-op (permanent connection)', () => {
    render(<Canvas />);
    const onDelete = rfProps.captured.onDelete as (params: { nodes: typeof testNodes; edges: typeof testEdges }) => void;

    onDelete({ nodes: [], edges: [testEdges[1]!] });
    expect(mockFns.detachSourceFromWorker).not.toHaveBeenCalled();
  });

  it('deleting a sink-worker edge is a no-op (permanent connection)', () => {
    render(<Canvas />);
    const onDelete = rfProps.captured.onDelete as (params: { nodes: typeof testNodes; edges: typeof testEdges }) => void;

    onDelete({ nodes: [], edges: [testEdges[2]!] });
    expect(mockFns.detachSinkFromWorker).not.toHaveBeenCalled();
  });

  it('multiple selected nodes can be deleted in one operation', () => {
    render(<Canvas />);
    const onDelete = rfProps.captured.onDelete as (params: { nodes: typeof testNodes; edges: typeof testEdges }) => void;

    onDelete({ nodes: [WORKER_A, SOURCE_1, SINK_1], edges: [] });
    expect(mockFns.removeWorker).toHaveBeenCalledWith('w-a');
    expect(mockFns.removePhysicalSource).toHaveBeenCalledWith('s-1');
    expect(mockFns.removeSink).toHaveBeenCalledWith('k-1');
  });
});
