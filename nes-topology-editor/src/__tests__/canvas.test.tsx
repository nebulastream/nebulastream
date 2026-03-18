import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import React from 'react';

// Use vi.hoisted so these are available in vi.mock factories (which get hoisted)
const { EMPTY_NODES, EMPTY_EDGES, mockFns, rfProps } = vi.hoisted(() => ({
  EMPTY_NODES: [] as never[],
  EMPTY_EDGES: [] as never[],
  mockFns: {
    addWorker: () => {},
    addPhysicalSource: () => {},
    addSink: () => {},
    updateNodePosition: () => {},
  },
  rfProps: { captured: {} as Record<string, unknown> },
}));

vi.mock('@xyflow/react', () => ({
  ReactFlow: ({ children, ...props }: Record<string, unknown>) => {
    rfProps.captured = props;
    return <div data-testid="reactflow">{children as React.ReactNode}</div>;
  },
  ReactFlowProvider: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  Background: ({ variant, gap }: { variant?: string; gap?: number }) => (
    <div data-testid="background" data-variant={variant} data-gap={gap} />
  ),
  BackgroundVariant: { Dots: 'dots' },
  MiniMap: (props: Record<string, unknown>) => (
    <div data-testid="minimap" data-zoomable={String(props.zoomable)} data-pannable={String(props.pannable)} />
  ),
  Controls: () => <div data-testid="controls" />,
  Handle: () => <div />,
  Position: { Top: 'top', Bottom: 'bottom', Left: 'left', Right: 'right' },
  useReactFlow: () => ({
    screenToFlowPosition: ({ x, y }: { x: number; y: number }) => ({ x, y }),
  }),
  applyNodeChanges: () => [],
  applyEdgeChanges: () => [],
}));

// Mock useStore to bypass zustand's useSyncExternalStore entirely
vi.mock('../store', () => {
  const stableState: Record<string, unknown> = {
    workers: [],
    physicalSources: [],
    sinks: [],
    logicalSources: [],
    addWorker: mockFns.addWorker,
    addPhysicalSource: mockFns.addPhysicalSource,
    addSink: mockFns.addSink,
    updateNodePosition: mockFns.updateNodePosition,
  };
  return {
    useStore: (selector: (s: Record<string, unknown>) => unknown) => selector(stableState),
  };
});

vi.mock('../store/selectors', () => ({
  selectReactFlowNodes: () => EMPTY_NODES,
  selectReactFlowEdges: () => EMPTY_EDGES,
  selectValidationErrorMap: () => new Map(),
}));

// Mock autoLayout to avoid real dagre calls
vi.mock('../lib/autoLayout', () => ({
  applyAutoLayout: () => {},
}));

import Canvas from '../components/Canvas/Canvas';

describe('Canvas features (CANV-10)', () => {
  it('renders a MiniMap child component', () => {
    render(<Canvas />);
    expect(screen.getByTestId('minimap')).toBeDefined();
  });

  it('renders a Background component with grid', () => {
    render(<Canvas />);
    const bg = screen.getByTestId('background');
    expect(bg).toBeDefined();
    expect(bg.getAttribute('data-variant')).toBe('dots');
    expect(bg.getAttribute('data-gap')).toBe('20');
  });

  it('passes snapToGrid and snapGrid props to ReactFlow', () => {
    render(<Canvas />);
    expect(rfProps.captured.snapToGrid).toBe(true);
    expect(rfProps.captured.snapGrid).toEqual([20, 20]);
  });

  it('renders Controls component', () => {
    render(<Canvas />);
    expect(screen.getByTestId('controls')).toBeDefined();
  });

  it('MiniMap is zoomable and pannable', () => {
    render(<Canvas />);
    const minimap = screen.getByTestId('minimap');
    expect(minimap.getAttribute('data-zoomable')).toBe('true');
    expect(minimap.getAttribute('data-pannable')).toBe('true');
  });
});
