import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import type { Worker } from '../../lib/types';

const { mockState } = vi.hoisted(() => ({
  mockState: {
    workers: [] as Worker[],
    physicalSources: [] as unknown[],
    sinks: [] as unknown[],
    logicalSources: [] as unknown[],
    selectedNodeId: null as string | null,
    updateWorker: vi.fn(),
    selectNode: vi.fn(),
    setSidebarTab: vi.fn(),
  },
}));

vi.mock('../../store', () => ({
  useStore: Object.assign(
    (selector: (s: typeof mockState) => unknown) => selector(mockState),
    { getState: () => mockState },
  ),
}));

import WorkerPanel from './WorkerPanel';

const mockWorker: Worker = {
  id: 'w1',
  host: '127.0.0.1',
  data: '4000',
  capacity: 8,
  downstream: [],
  position: { x: 0, y: 0 },
};

describe('WorkerPanel (PROP-01)', () => {
  it('renders without crashing', () => {
    render(<WorkerPanel worker={mockWorker} />);
    expect(screen.getByRole('heading', { level: 3 })).toBeDefined();
  });

  it('renders host input with label "Host Address" and initial value', () => {
    render(<WorkerPanel worker={mockWorker} />);
    const label = screen.getByText('Host Address');
    expect(label).toBeDefined();
    const input = screen.getByDisplayValue('127.0.0.1');
    expect(input).toBeDefined();
  });

  it('renders data address input with label "Data Address" and initial value', () => {
    render(<WorkerPanel worker={mockWorker} />);
    const label = screen.getByText('Data Address');
    expect(label).toBeDefined();
    const input = screen.getByDisplayValue('4000');
    expect(input).toBeDefined();
  });

  it('renders capacity input with label "Capacity" and initial value', () => {
    render(<WorkerPanel worker={mockWorker} />);
    const label = screen.getByText('Capacity');
    expect(label).toBeDefined();
    const input = screen.getByDisplayValue('8');
    expect(input).toBeDefined();
  });

  it('shows "Connected Sources" and "Connected Sinks" headings', () => {
    render(<WorkerPanel worker={mockWorker} />);
    expect(screen.getByText('Connected Sources')).toBeDefined();
    expect(screen.getByText('Connected Sinks')).toBeDefined();
  });
});
