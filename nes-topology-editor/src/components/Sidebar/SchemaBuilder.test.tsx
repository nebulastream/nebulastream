import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import SchemaBuilder from './SchemaBuilder';
import type { SchemaField } from '../../lib/types';

const NES_TYPES = [
  'INT8', 'INT16', 'INT32', 'INT64',
  'UINT8', 'UINT16', 'UINT32', 'UINT64',
  'FLOAT32', 'FLOAT64', 'BOOLEAN', 'CHAR', 'TEXT',
];

describe('SchemaBuilder', () => {
  it('renders existing fields with correct name and type values', () => {
    const fields: SchemaField[] = [
      { name: 'sensor_id', type: 'INT32' },
      { name: 'temperature', type: 'FLOAT64' },
    ];
    const onChange = vi.fn();
    render(<SchemaBuilder fields={fields} onChange={onChange} />);

    expect(screen.getByDisplayValue('sensor_id')).toBeDefined();
    expect(screen.getByDisplayValue('temperature')).toBeDefined();
    // Each field has a select with its type
    const selects = screen.getAllByRole('combobox');
    expect(selects[0]).toHaveProperty('value', 'INT32');
    expect(selects[1]).toHaveProperty('value', 'FLOAT64');
  });

  it('clicking add button calls onChange with new field appended', () => {
    const fields: SchemaField[] = [{ name: 'id', type: 'INT64' }];
    const onChange = vi.fn();
    render(<SchemaBuilder fields={fields} onChange={onChange} />);

    const addBtn = screen.getByRole('button', { name: /add/i });
    fireEvent.click(addBtn);

    expect(onChange).toHaveBeenCalledWith([
      { name: 'id', type: 'INT64' },
      { name: '', type: 'INT32' },
    ]);
  });

  it('clicking remove button calls onChange with field removed', () => {
    const fields: SchemaField[] = [
      { name: 'a', type: 'INT32' },
      { name: 'b', type: 'TEXT' },
    ];
    const onChange = vi.fn();
    render(<SchemaBuilder fields={fields} onChange={onChange} />);

    const removeBtns = screen.getAllByRole('button', { name: /remove/i });
    fireEvent.click(removeBtns[0]!);

    expect(onChange).toHaveBeenCalledWith([{ name: 'b', type: 'TEXT' }]);
  });

  it('typing in name input calls onChange with updated field', () => {
    const fields: SchemaField[] = [{ name: 'old', type: 'INT32' }];
    const onChange = vi.fn();
    render(<SchemaBuilder fields={fields} onChange={onChange} />);

    const input = screen.getByDisplayValue('old');
    fireEvent.change(input, { target: { value: 'new_name' } });

    expect(onChange).toHaveBeenCalledWith([{ name: 'new_name', type: 'INT32' }]);
  });

  it('changing type dropdown calls onChange with updated type', () => {
    const fields: SchemaField[] = [{ name: 'x', type: 'INT32' }];
    const onChange = vi.fn();
    render(<SchemaBuilder fields={fields} onChange={onChange} />);

    const select = screen.getByRole('combobox');
    fireEvent.change(select, { target: { value: 'FLOAT64' } });

    expect(onChange).toHaveBeenCalledWith([{ name: 'x', type: 'FLOAT64' }]);
  });

  it('all 13 NES types appear in type dropdown', () => {
    const fields: SchemaField[] = [{ name: 'f', type: 'INT32' }];
    const onChange = vi.fn();
    render(<SchemaBuilder fields={fields} onChange={onChange} />);

    const options = screen.getAllByRole('option');
    const optionValues = options.map((o) => (o as HTMLOptionElement).value);

    for (const t of NES_TYPES) {
      expect(optionValues).toContain(t);
    }
    expect(options.length).toBe(13);
  });

  it('empty state shows "No fields defined" with add button', () => {
    const onChange = vi.fn();
    render(<SchemaBuilder fields={[]} onChange={onChange} />);

    expect(screen.getByText('No fields defined')).toBeDefined();
    expect(screen.getByRole('button', { name: /add/i })).toBeDefined();
  });
});
