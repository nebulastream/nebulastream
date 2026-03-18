import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import React from 'react';

// Use vi.hoisted so mock functions are available in vi.mock factories
const { callTracker } = vi.hoisted(() => {
  const tracker = {
    addWorkerCalls: [] as unknown[][],
  };
  return { callTracker: tracker };
});

// Mock @xyflow/react
vi.mock('@xyflow/react', () => ({
  ReactFlowProvider: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  useReactFlow: () => ({
    screenToFlowPosition: ({ x, y }: { x: number; y: number }) => ({ x, y }),
    fitView: () => {},
  }),
}));

// Mock useStore as a plain function (bypass zustand's useSyncExternalStore)
vi.mock('../store', () => {
  const stableState: Record<string, unknown> = {
    workers: [],
    physicalSources: [],
    sinks: [],
    logicalSources: [],
    addWorker: (...args: unknown[]) => { callTracker.addWorkerCalls.push(args); },
    setAllNodePositions: () => {},
    updateNodePosition: () => {},
  };
  return {
    useStore: Object.assign(
      (selector: (s: Record<string, unknown>) => unknown) => selector(stableState),
      { getState: () => stableState },
    ),
  };
});

vi.mock('../store/selectors', () => ({
  selectReactFlowNodes: () => [],
  selectReactFlowEdges: () => [],
  selectValidationErrorMap: () => new Map(),
}));

// Mock autoLayout to avoid real dagre calls
vi.mock('../lib/autoLayout', () => ({
  applyAutoLayout: () => {},
}));

import Toolbar from '../components/Toolbar/Toolbar';

describe('Toolbar node creation buttons (CANV-01, CANV-02)', () => {
  beforeEach(() => {
    callTracker.addWorkerCalls.length = 0;
  });

  it('renders Add Worker button only (auto-layout is automatic)', () => {
    render(<Toolbar />);
    expect(screen.getByTestId('add-worker-btn')).toBeDefined();
    // Auto Layout button removed -- layout is now automatic
    expect(screen.queryByTestId('auto-layout-btn')).toBeNull();
    // Source and Sink buttons should not be in toolbar
    expect(screen.queryByText('Add Source')).toBeNull();
    expect(screen.queryByText('Add Sink')).toBeNull();
  });

  it('clicking Add Worker calls store.addWorker with a position', () => {
    render(<Toolbar />);
    fireEvent.click(screen.getByTestId('add-worker-btn'));
    expect(callTracker.addWorkerCalls.length).toBe(1);
    const pos = callTracker.addWorkerCalls[0]![0] as { x: unknown; y: unknown };
    expect(pos).toHaveProperty('x');
    expect(pos).toHaveProperty('y');
  });
});
