import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import type { Sink } from '../../lib/types';
import { SINK_TYPES } from '../../lib/sourceConfigs';

const { mockState } = vi.hoisted(() => ({
  mockState: {
    workers: [] as unknown[],
    physicalSources: [] as unknown[],
    sinks: [] as unknown[],
    logicalSources: [] as unknown[],
    selectedNodeId: null as string | null,
    updateSink: vi.fn(),
  },
}));

vi.mock('../../store', () => ({
  useStore: Object.assign(
    (selector: (s: typeof mockState) => unknown) => selector(mockState),
    { getState: () => mockState },
  ),
}));

import SinkPanel from './SinkPanel';

const mockSink: Sink = {
  id: 'k1',
  name: 'test-sink',
  hostWorkerId: 'w1',
  type: 'File',
  schema: [],
  config: {},
  parserConfig: {},
  position: { x: 0, y: 0 },
};

const voidSink: Sink = {
  ...mockSink,
  id: 'k2',
  type: 'Void',
};

describe('SinkPanel (PROP-03, PROP-04)', () => {
  it('renders without crashing', () => {
    const { container } = render(<SinkPanel sink={mockSink} />);
    expect(container.querySelector('h3')).toBeDefined();
  });

  it('renders sink name input with initial value', () => {
    render(<SinkPanel sink={mockSink} />);
    const input = screen.getByDisplayValue('test-sink');
    expect(input).toBeDefined();
  });

  it('renders sink type dropdown containing SINK_TYPES', () => {
    render(<SinkPanel sink={mockSink} />);
    const select = screen.getByDisplayValue('File');
    expect(select).toBeDefined();
    for (const t of SINK_TYPES) {
      expect(screen.getByRole('option', { name: t })).toBeDefined();
    }
  });

  it('shows "No configuration required" for Void sink type', () => {
    render(<SinkPanel sink={voidSink} />);
    expect(screen.getByText(/no configuration required/i)).toBeDefined();
  });
});
