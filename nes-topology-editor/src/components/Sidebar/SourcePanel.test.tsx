import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import type { PhysicalSource } from '../../lib/types';
import { SOURCE_TYPES, SOURCE_CONFIGS } from '../../lib/sourceConfigs';

const { mockState } = vi.hoisted(() => ({
  mockState: {
    workers: [] as unknown[],
    physicalSources: [] as unknown[],
    sinks: [] as unknown[],
    logicalSources: [] as unknown[],
    selectedNodeId: null as string | null,
    updatePhysicalSource: vi.fn(),
  },
}));

vi.mock('../../store', () => ({
  useStore: Object.assign(
    (selector: (s: typeof mockState) => unknown) => selector(mockState),
    { getState: () => mockState },
  ),
}));

import SourcePanel from './SourcePanel';

const mockSource: PhysicalSource = {
  id: 's1',
  logicalSourceId: '',
  hostWorkerId: 'w1',
  type: 'Generator',
  sourceConfig: {},
  parserConfig: {},
  position: { x: 0, y: 0 },
};

describe('SourcePanel (PROP-02, PROP-04, SRCE-03)', () => {
  it('renders without crashing', () => {
    const { container } = render(<SourcePanel source={mockSource} />);
    expect(container.querySelector('h3')).toBeDefined();
  });

  it('renders source type dropdown containing SOURCE_TYPES', () => {
    render(<SourcePanel source={mockSource} />);
    const select = screen.getByDisplayValue('Generator');
    expect(select).toBeDefined();
    // All source types should be options
    for (const t of SOURCE_TYPES) {
      expect(screen.getByRole('option', { name: t })).toBeDefined();
    }
  });

  it('renders logical source select area', () => {
    render(<SourcePanel source={mockSource} />);
    expect(screen.getByText(/logical source/i)).toBeDefined();
  });

  it('renders source configuration section with ConfigForm fields for Generator type', () => {
    render(<SourcePanel source={mockSource} />);
    // Generator config has a "Seed" field and "Generator Schema" field
    const generatorFields = SOURCE_CONFIGS['Generator']!;
    for (const field of generatorFields) {
      expect(screen.getByText(field.label)).toBeDefined();
    }
  });
});
