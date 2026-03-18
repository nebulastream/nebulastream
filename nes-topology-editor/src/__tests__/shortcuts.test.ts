import { describe, it, expect, beforeEach, vi } from 'vitest';
import { useStore } from '../store';

// Mock autoLayout to avoid dagre dependency
vi.mock('../lib/autoLayout', () => ({
  applyAutoLayout: vi.fn(),
}));

// Import handler after mocks
import { handleGlobalKeyDown } from '../lib/keyboardHandler';

describe('Global keyboard shortcuts', () => {
  beforeEach(() => {
    useStore.setState({
      workers: [],
      logicalSources: [],
      physicalSources: [],
      sinks: [],
      queries: [],
      selectedNodeId: null,
      clipboard: null,
      sidebarTab: 'properties' as const,
      selectedQueryId: null,
      yamlOverlayVisible: false,
    });
    useStore.temporal.getState().clear();
  });

  it('Escape when yamlOverlayVisible=true toggles it to false and does not deselect node', () => {
    useStore.setState({ yamlOverlayVisible: true, selectedNodeId: 'node-1' });

    const event = new KeyboardEvent('keydown', { key: 'Escape' });
    handleGlobalKeyDown(event);

    expect(useStore.getState().yamlOverlayVisible).toBe(false);
    expect(useStore.getState().selectedNodeId).toBe('node-1');
  });

  it('Escape when yamlOverlayVisible=false and selectedNodeId is set calls selectNode(null)', () => {
    useStore.setState({ yamlOverlayVisible: false, selectedNodeId: 'node-1' });

    const event = new KeyboardEvent('keydown', { key: 'Escape' });
    handleGlobalKeyDown(event);

    expect(useStore.getState().selectedNodeId).toBeNull();
  });

  it('keyboard handler skips ctrl+z when activeElement is inside .monaco-editor', () => {
    // Add a worker first
    useStore.getState().addWorker();
    expect(useStore.getState().workers).toHaveLength(1);

    // Create a mock element inside monaco-editor
    const monacoDiv = document.createElement('div');
    monacoDiv.className = 'monaco-editor';
    const textarea = document.createElement('textarea');
    monacoDiv.appendChild(textarea);
    document.body.appendChild(monacoDiv);

    // Focus the textarea inside monaco-editor
    textarea.focus();
    expect(document.activeElement).toBe(textarea);

    // Ctrl+Z should NOT trigger store revert when in Monaco
    const event = new KeyboardEvent('keydown', { key: 'z', ctrlKey: true });
    handleGlobalKeyDown(event);

    // Workers should still be 1 (not reverted)
    expect(useStore.getState().workers).toHaveLength(1);

    // Cleanup
    document.body.removeChild(monacoDiv);
  });
});
