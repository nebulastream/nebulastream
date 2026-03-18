import { describe, it, expect, vi, beforeEach } from 'vitest';
import React from 'react';
import { render } from '@testing-library/react';

// Mock the Zustand store before importing StatusBar
const mockStoreValues: Record<string, unknown> = {
  wasmStatus: 'loading',
  semanticError: null,
  wasmError: null,
  darkMode: false,
};

vi.mock('../store', () => ({
  useStore: (selector: (state: Record<string, unknown>) => unknown) => selector(mockStoreValues),
}));

// Dynamic import after mock is set up
const { default: StatusBar } = await import('../components/StatusBar/StatusBar');

function setMockState(overrides: Partial<typeof mockStoreValues>) {
  Object.assign(mockStoreValues, {
    wasmStatus: 'loading',
    semanticError: null,
    wasmError: null,
    darkMode: false,
    ...overrides,
  });
}

describe('StatusBar', () => {
  beforeEach(() => {
    setMockState({});
  });

  it('renders loading state with spinner text', () => {
    setMockState({ wasmStatus: 'loading' });
    const { container } = render(React.createElement(StatusBar));
    expect(container.textContent).toContain('Loading validator...');
  });

  it('renders validating state with spinner text', () => {
    setMockState({ wasmStatus: 'validating' });
    const { container } = render(React.createElement(StatusBar));
    expect(container.textContent).toContain('Validating...');
  });

  it('renders valid state (empty semantic error)', () => {
    setMockState({ wasmStatus: 'ready', semanticError: '' });
    const { container } = render(React.createElement(StatusBar));
    expect(container.textContent).toContain('Valid');
  });

  it('renders warning state (non-empty semantic error)', () => {
    setMockState({ wasmStatus: 'ready', semanticError: 'Invalid source name' });
    const { container } = render(React.createElement(StatusBar));
    expect(container.textContent).toContain('Validation error');
    // Check tooltip contains the error
    const root = container.firstElementChild as HTMLElement;
    expect(root?.title).toBe('Invalid source name');
  });

  it('renders error state (WASM unavailable)', () => {
    setMockState({ wasmStatus: 'error', wasmError: 'WASM failed to load' });
    const { container } = render(React.createElement(StatusBar));
    expect(container.textContent).toContain('Validator unavailable');
    const root = container.firstElementChild as HTMLElement;
    expect(root?.title).toBe('WASM failed to load');
  });

  it('renders ready state with null semantic error (not yet validated)', () => {
    setMockState({ wasmStatus: 'ready', semanticError: null });
    const { container } = render(React.createElement(StatusBar));
    // Should not show "Valid" text, just the icon
    expect(container.textContent).not.toContain('Valid');
    expect(container.textContent).not.toContain('Validation error');
  });
});
