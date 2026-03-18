import { describe, it, expect, beforeEach } from 'vitest';
import { useStore } from '../store';

describe('History (temporal middleware)', () => {
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
    // Clear history
    useStore.temporal.getState().clear();
  });

  it('after addWorker then revert, workers array returns to previous state', () => {
    expect(useStore.getState().workers).toHaveLength(0);
    useStore.getState().addWorker();
    expect(useStore.getState().workers).toHaveLength(1);

    useStore.temporal.getState().undo();
    expect(useStore.getState().workers).toHaveLength(0);
  });

  it('after revert then redo, workers array is restored', () => {
    useStore.getState().addWorker();
    const workersBefore = useStore.getState().workers;
    expect(workersBefore).toHaveLength(1);

    useStore.temporal.getState().undo();
    expect(useStore.getState().workers).toHaveLength(0);

    useStore.temporal.getState().redo();
    expect(useStore.getState().workers).toHaveLength(1);
    expect(useStore.getState().workers[0]!.id).toBe(workersBefore[0]!.id);
  });

  it('UI state is NOT restored by history (partialize excludes them)', () => {
    // Change UI state
    useStore.getState().selectNode('some-node');
    useStore.getState().toggleYamlOverlay();

    // Change topology state
    useStore.getState().addWorker();

    // Revert should revert topology but NOT UI state
    useStore.temporal.getState().undo();

    expect(useStore.getState().workers).toHaveLength(0);
    // UI state should remain unchanged after revert
    expect(useStore.getState().selectedNodeId).toBe('some-node');
    expect(useStore.getState().yamlOverlayVisible).toBe(true);
  });

  it('query changes are revertable', () => {
    useStore.getState().addQuery();
    expect(useStore.getState().queries).toHaveLength(1);
    const queryId = useStore.getState().queries[0]!.id;

    useStore.getState().updateQuery(queryId, { name: 'test-query', sql: 'SELECT * FROM sensor' });
    expect(useStore.getState().queries[0]!.name).toBe('test-query');

    useStore.temporal.getState().undo();
    expect(useStore.getState().queries[0]!.name).toBe('');

    useStore.temporal.getState().undo();
    expect(useStore.getState().queries).toHaveLength(0);
  });
});
