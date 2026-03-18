import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import type { LogicalSource } from '../../lib/types';

const { mockState } = vi.hoisted(() => ({
  mockState: {
    logicalSources: [] as LogicalSource[],
    addLogicalSource: vi.fn(),
    updateLogicalSource: vi.fn(),
    removeLogicalSource: vi.fn(),
  },
}));

vi.mock('../../store', () => ({
  useStore: Object.assign(
    (selector: (s: typeof mockState) => unknown) => selector(mockState),
    { getState: () => mockState },
  ),
}));

import LogicalSourcesPanel from './LogicalSourcesPanel';

describe('LogicalSourcesPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockState.logicalSources = [];
  });

  it('renders empty state when no logical sources', () => {
    render(<LogicalSourcesPanel />);
    expect(screen.getByText('No logical sources defined')).toBeDefined();
  });

  it('renders logical source names when sources exist', () => {
    mockState.logicalSources = [
      { id: 'ls1', name: 'Sensor', schema: [] },
      { id: 'ls2', name: 'GPS', schema: [{ name: 'lat', type: 'FLOAT64' }] },
    ];
    render(<LogicalSourcesPanel />);
    expect(screen.getByDisplayValue('Sensor')).toBeDefined();
    expect(screen.getByDisplayValue('GPS')).toBeDefined();
  });

  it('clicking "New Source" calls addLogicalSource', () => {
    render(<LogicalSourcesPanel />);
    const btn = screen.getByRole('button', { name: /new source/i });
    fireEvent.click(btn);
    expect(mockState.addLogicalSource).toHaveBeenCalledWith('Source_1');
  });

  it('clicking delete calls removeLogicalSource', () => {
    mockState.logicalSources = [
      { id: 'ls1', name: 'Sensor', schema: [] },
    ];
    render(<LogicalSourcesPanel />);
    const deleteBtn = screen.getByRole('button', { name: /delete/i });
    fireEvent.click(deleteBtn);
    expect(mockState.removeLogicalSource).toHaveBeenCalledWith('ls1');
  });

  it('editing name calls updateLogicalSource', () => {
    mockState.logicalSources = [
      { id: 'ls1', name: 'Old', schema: [] },
    ];
    render(<LogicalSourcesPanel />);
    const input = screen.getByDisplayValue('Old');
    fireEvent.change(input, { target: { value: 'NewName' } });
    expect(mockState.updateLogicalSource).toHaveBeenCalledWith('ls1', { name: 'NewName' });
  });
});
