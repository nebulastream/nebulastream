import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render } from '@testing-library/react';
import React from 'react';

// Use vi.hoisted so these are available in vi.mock factories (which get hoisted)
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
    clipboard: null as unknown,
  },
  rfProps: { captured: {} as Record<string, unknown> },
}));

const WORKER_A = {
  id: 'w-a', type: 'worker', position: { x: 0, y: 0 },
  data: { worker: { id: 'w-a', host: 'a:9090', grpc: 'a:8080', capacity: 10000, downstream: [], position: { x: 0, y: 0 } } },
};
const WORKER_B = {
  id: 'w-b', type: 'worker', position: { x: 200, y: 0 },
  data: { worker: { id: 'w-b', host: 'b:9090', grpc: 'b:8080', capacity: 10000, downstream: [], position: { x: 200, y: 0 } } },
};
const WORKER_C = {
  id: 'w-c', type: 'worker', position: { x: 400, y: 0 },
  data: { worker: { id: 'w-c', host: 'c:9090', grpc: 'c:8080', capacity: 10000, downstream: [], position: { x: 400, y: 0 } } },
};
const SOURCE_1 = {
  id: 's-1', type: 'source', position: { x: 0, y: 200 },
  data: { source: { id: 's-1', logicalSourceId: '', hostWorkerId: '', type: 'Generator', sourceConfig: {}, parserConfig: {}, position: { x: 0, y: 200 } }, color: '#3b82f6' },
};
const SINK_1 = {
  id: 'k-1', type: 'sink', position: { x: 200, y: 200 },
  data: { sink: { id: 'k-1', name: 'Sink', hostWorkerId: '', type: 'Print', schema: [], config: {}, position: { x: 200, y: 200 } } },
};

let testNodes = [WORKER_A, WORKER_B, WORKER_C, SOURCE_1, SINK_1];
let testEdges: { id: string; source: string; target: string; type: string; label?: string }[] = [];

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
  selectNode: vi.fn(),
  setSidebarTab: vi.fn(),
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

import Canvas from '../components/Canvas/Canvas';

describe('Connection handling (CANV-04, CANV-05)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    testNodes = [WORKER_A, WORKER_B, WORKER_C, SOURCE_1, SINK_1];
    testEdges = [];
  });

  it('onConnect with worker-to-worker calls connectDownstream', () => {
    render(<Canvas />);
    const onConnect = rfProps.captured.onConnect as (connection: { source: string; target: string }) => void;
    expect(onConnect).toBeDefined();

    onConnect({ source: 'w-a', target: 'w-b' });
    expect(mockFns.connectDownstream).toHaveBeenCalledWith('w-a', 'w-b');
  });

  it('onConnect with source-to-worker calls attachSourceToWorker', () => {
    render(<Canvas />);
    const onConnect = rfProps.captured.onConnect as (connection: { source: string; target: string }) => void;

    onConnect({ source: 's-1', target: 'w-a' });
    expect(mockFns.attachSourceToWorker).toHaveBeenCalledWith('s-1', 'w-a');
  });

  it('onConnect with worker-to-sink calls attachSinkToWorker', () => {
    render(<Canvas />);
    const onConnect = rfProps.captured.onConnect as (connection: { source: string; target: string }) => void;

    onConnect({ source: 'w-a', target: 'k-1' });
    expect(mockFns.attachSinkToWorker).toHaveBeenCalledWith('k-1', 'w-a');
  });

  it('isValidConnection rejects source-to-source connections', () => {
    const source2 = {
      id: 's-2', type: 'source', position: { x: 100, y: 200 },
      data: { source: { id: 's-2', logicalSourceId: '', hostWorkerId: '', type: 'CSV', sourceConfig: {}, parserConfig: {}, position: { x: 100, y: 200 } }, color: '#10b981' },
    };
    testNodes = [...testNodes, source2];

    render(<Canvas />);
    const isValid = rfProps.captured.isValidConnection as (conn: { source: string; target: string }) => boolean;

    expect(isValid({ source: 's-1', target: 's-2' })).toBe(false);
  });

  it('isValidConnection rejects cycle-creating worker connections', () => {
    // Set up chain: A -> B -> C, then try C -> A (cycle)
    testEdges = [
      { id: 'downstream-w-a-w-b', source: 'w-a', target: 'w-b', type: 'default', label: 'downstream' },
      { id: 'downstream-w-b-w-c', source: 'w-b', target: 'w-c', type: 'default', label: 'downstream' },
    ];

    render(<Canvas />);
    const isValid = rfProps.captured.isValidConnection as (conn: { source: string; target: string }) => boolean;

    expect(isValid({ source: 'w-c', target: 'w-a' })).toBe(false);
  });

  it('isValidConnection accepts valid worker-to-worker connections', () => {
    render(<Canvas />);
    const isValid = rfProps.captured.isValidConnection as (conn: { source: string; target: string }) => boolean;

    expect(isValid({ source: 'w-a', target: 'w-b' })).toBe(true);
  });

  it('onConnect dispatches correct action based on node types', () => {
    render(<Canvas />);
    const onConnect = rfProps.captured.onConnect as (connection: { source: string; target: string }) => void;

    // Worker -> Worker
    onConnect({ source: 'w-a', target: 'w-b' });
    expect(mockFns.connectDownstream).toHaveBeenCalledTimes(1);
    expect(mockFns.attachSourceToWorker).not.toHaveBeenCalled();
    expect(mockFns.attachSinkToWorker).not.toHaveBeenCalled();

    vi.clearAllMocks();

    // Source -> Worker
    onConnect({ source: 's-1', target: 'w-a' });
    expect(mockFns.attachSourceToWorker).toHaveBeenCalledTimes(1);
    expect(mockFns.connectDownstream).not.toHaveBeenCalled();

    vi.clearAllMocks();

    // Worker -> Sink
    onConnect({ source: 'w-a', target: 'k-1' });
    expect(mockFns.attachSinkToWorker).toHaveBeenCalledTimes(1);
    expect(mockFns.connectDownstream).not.toHaveBeenCalled();
  });
});
