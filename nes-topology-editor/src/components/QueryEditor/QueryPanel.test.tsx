import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import type { Query } from '../../lib/types';

const { mockState } = vi.hoisted(() => ({
  mockState: {
    queries: [] as Query[],
    selectedQueryId: null as string | null,
    addQuery: vi.fn(),
    removeQuery: vi.fn(),
    updateQuery: vi.fn(),
    selectQuery: vi.fn(),
  },
}));

vi.mock('../../store', () => ({
  useStore: Object.assign(
    (selector: (s: typeof mockState) => unknown) => selector(mockState),
    { getState: () => mockState },
  ),
}));

vi.mock('@monaco-editor/react', () => ({
  default: (props: { value?: string; onChange?: (v: string) => void }) => (
    <textarea
      data-testid="monaco"
      value={props.value ?? ''}
      onChange={(e) => props.onChange?.(e.target.value)}
    />
  ),
}));

import QueryPanel from './QueryPanel';

describe('QueryPanel', () => {
  beforeEach(() => {
    mockState.queries = [];
    mockState.selectedQueryId = null;
    mockState.addQuery.mockReset();
    mockState.removeQuery.mockReset();
    mockState.updateQuery.mockReset();
    mockState.selectQuery.mockReset();
  });

  it('renders empty state when no queries', () => {
    render(<QueryPanel />);
    expect(screen.getByText('Add a query to get started')).toBeDefined();
  });

  it('renders query tabs with names', () => {
    mockState.queries = [
      { id: 'q1', name: 'My Query', sql: 'SELECT 1' },
      { id: 'q2', name: 'Another', sql: '' },
    ];
    mockState.selectedQueryId = 'q1';
    render(<QueryPanel />);
    expect(screen.getByText('My Query')).toBeDefined();
    expect(screen.getByText('Another')).toBeDefined();
  });

  it('unnamed query shows "(unnamed)" text', () => {
    mockState.queries = [{ id: 'q1', name: '', sql: '' }];
    mockState.selectedQueryId = 'q1';
    render(<QueryPanel />);
    expect(screen.getByText('(unnamed)')).toBeDefined();
  });

  it('clicking "+" calls addQuery', () => {
    render(<QueryPanel />);
    const addBtn = screen.getByTestId('add-query');
    fireEvent.click(addBtn);
    expect(mockState.addQuery).toHaveBeenCalled();
  });

  it('clicking "X" calls removeQuery', () => {
    mockState.queries = [{ id: 'q1', name: 'Test', sql: '' }];
    mockState.selectedQueryId = 'q1';
    render(<QueryPanel />);
    const removeBtn = screen.getByTestId('remove-query-q1');
    fireEvent.click(removeBtn);
    expect(mockState.removeQuery).toHaveBeenCalledWith('q1');
  });

  it('renders Monaco editor for selected query', () => {
    mockState.queries = [{ id: 'q1', name: 'Test', sql: 'SELECT * FROM t' }];
    mockState.selectedQueryId = 'q1';
    render(<QueryPanel />);
    const editor = screen.getByTestId('monaco');
    expect(editor).toBeDefined();
    expect((editor as HTMLTextAreaElement).value).toBe('SELECT * FROM t');
  });

  it('clicking a tab calls selectQuery', () => {
    mockState.queries = [
      { id: 'q1', name: 'First', sql: '' },
      { id: 'q2', name: 'Second', sql: '' },
    ];
    mockState.selectedQueryId = 'q1';
    render(<QueryPanel />);
    const tab = screen.getByTestId('query-tab-q2');
    fireEvent.click(tab);
    expect(mockState.selectQuery).toHaveBeenCalledWith('q2');
  });
});
