import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';

// Mock @xyflow/react to avoid needing full ReactFlow context
vi.mock('@xyflow/react', () => ({
  Handle: ({ type, position, id }: { type: string; position: string; id: string }) => (
    <div data-testid={`handle-${id}`} data-type={type} data-position={position} />
  ),
  Position: {
    Top: 'top',
    Bottom: 'bottom',
    Left: 'left',
    Right: 'right',
  },
}));

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyFC = React.FC<any>;

import WorkerNode from '../components/Canvas/WorkerNode';
import SourceNode from '../components/Canvas/SourceNode';
import SinkNode from '../components/Canvas/SinkNode';

// Cast to bypass strict NodeProps typing (we're testing with mocked @xyflow/react)
const Worker = WorkerNode as AnyFC;
const Source = SourceNode as AnyFC;
const Sink = SinkNode as AnyFC;

describe('WorkerNode', () => {
  const workerData = {
    worker: {
      id: 'w1',
      host: 'worker-1:9090',
      dataAddress: 'worker-1:8080',
      capacity: 10000,
      downstream: [],
      position: { x: 0, y: 0 },
    },
  };

  it('renders the host address label', () => {
    render(<Worker id="w1" data={workerData} type="worker" />);
    expect(screen.getByText('worker-1:9090')).toBeDefined();
  });

  it('renders with handles for connections (top, bottom, left, right)', () => {
    const { container } = render(<Worker id="w1" data={workerData} type="worker" />);
    expect(container.querySelector('[data-testid="handle-in"]')).toBeDefined();
    expect(container.querySelector('[data-testid="handle-out"]')).toBeDefined();
    expect(container.querySelector('[data-testid="handle-sources"]')).toBeDefined();
    expect(container.querySelector('[data-testid="handle-sinks"]')).toBeDefined();
  });

  it('has worker-node CSS class', () => {
    const { container } = render(<Worker id="w1" data={workerData} type="worker" />);
    expect(container.querySelector('.worker-node')).toBeDefined();
  });
});

describe('SourceNode', () => {
  const sourceData = {
    source: {
      id: 's1',
      logicalSourceId: 'ls1',
      hostWorkerId: '',
      type: 'Generator',
      sourceConfig: {},
      parserConfig: {},
      position: { x: 0, y: 0 },
    },
    color: '#3b82f6',
  };

  it('renders with a type-based icon and label', () => {
    render(<Source id="s1" data={sourceData} type="source" />);
    expect(screen.getByText('Generator')).toBeDefined();
  });

  it('renders with color-coded styling from data.color', () => {
    const { container } = render(<Source id="s1" data={sourceData} type="source" />);
    const node = container.querySelector('.source-node') as HTMLElement;
    expect(node).toBeDefined();
    // happy-dom keeps hex, jsdom converts to rgb
    expect(node.style.borderColor).toMatch(/rgb\(59, 130, 246\)|#3b82f6/);
  });

  it('has source-node CSS class (distinct from worker/sink)', () => {
    const { container } = render(<Source id="s1" data={sourceData} type="source" />);
    expect(container.querySelector('.source-node')).toBeDefined();
    expect(container.querySelector('.worker-node')).toBeNull();
    expect(container.querySelector('.sink-node')).toBeNull();
  });
});

describe('SinkNode', () => {
  const sinkData = {
    sink: {
      id: 'sk1',
      name: 'Test Sink',
      hostWorkerId: '',
      type: 'Print',
      schema: [],
      config: {},
      parserConfig: {},
      position: { x: 0, y: 0 },
    },
  };

  it('renders with a type-based icon and label', () => {
    render(<Sink id="sk1" data={sinkData} type="sink" />);
    expect(screen.getByText('Print')).toBeDefined();
  });

  it('renders with sink-node CSS class (amber/orange styling)', () => {
    const { container } = render(<Sink id="sk1" data={sinkData} type="sink" />);
    expect(container.querySelector('.sink-node')).toBeDefined();
  });

  it('is visually distinct from worker and source nodes', () => {
    const { container } = render(<Sink id="sk1" data={sinkData} type="sink" />);
    expect(container.querySelector('.sink-node')).toBeDefined();
    expect(container.querySelector('.worker-node')).toBeNull();
    expect(container.querySelector('.source-node')).toBeNull();
  });
});

describe('Visual distinctness (CANV-07)', () => {
  it('all three node types produce different CSS classes', () => {
    const workerData = {
      worker: {
        id: 'w1', host: 'w:9090', dataAddress: 'w:8080', capacity: 10,
        downstream: [], position: { x: 0, y: 0 },
      },
    };
    const sourceData = {
      source: {
        id: 's1', logicalSourceId: 'ls1', hostWorkerId: '', type: 'Generator',
        sourceConfig: {}, parserConfig: {}, position: { x: 0, y: 0 },
      },
      color: '#3b82f6',
    };
    const sinkData = {
      sink: {
        id: 'sk1', name: 'Sink', hostWorkerId: '', type: 'Print',
        schema: [], config: {}, parserConfig: {}, position: { x: 0, y: 0 },
      },
    };

    const { container: wc } = render(<Worker id="w1" data={workerData} type="worker" />);
    const { container: sc } = render(<Source id="s1" data={sourceData} type="source" />);
    const { container: skc } = render(<Sink id="sk1" data={sinkData} type="sink" />);

    // Each has its own unique class
    expect(wc.querySelector('.worker-node')).not.toBeNull();
    expect(sc.querySelector('.source-node')).not.toBeNull();
    expect(skc.querySelector('.sink-node')).not.toBeNull();

    // None share each other's class
    expect(wc.querySelector('.source-node')).toBeNull();
    expect(wc.querySelector('.sink-node')).toBeNull();
    expect(sc.querySelector('.worker-node')).toBeNull();
    expect(skc.querySelector('.worker-node')).toBeNull();
  });
});
