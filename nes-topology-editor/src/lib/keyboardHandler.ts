import { useStore } from '../store';
import { applyAutoLayout } from './autoLayout';

/**
 * Global keyboard shortcut handler.
 * Extracted from useEffect for testability.
 */
export function handleGlobalKeyDown(e: KeyboardEvent): void {
  const activeEl = document.activeElement;
  const insideMonaco = activeEl?.closest('.monaco-editor') != null;
  const insideInput =
    activeEl instanceof HTMLInputElement ||
    activeEl instanceof HTMLTextAreaElement;

  const state = useStore.getState();

  // Escape: close YAML overlay first, then deselect node
  if (e.key === 'Escape') {
    if (state.yamlOverlayVisible) {
      state.toggleYamlOverlay();
    } else if (state.selectedNodeId) {
      state.selectNode(null);
    }
    return;
  }

  // Skip shortcuts that conflict with Monaco or input fields
  if (insideMonaco || insideInput) return;

  const ctrl = e.ctrlKey || e.metaKey;

  // Ctrl+Z / Ctrl+Shift+Z: history navigation
  if (ctrl && e.key === 'z' && !e.shiftKey) {
    e.preventDefault();
    useStore.temporal.getState().undo();
    applyAutoLayout();
    return;
  }

  if (ctrl && (e.key === 'Z' || (e.key === 'z' && e.shiftKey))) {
    e.preventDefault();
    useStore.temporal.getState().redo();
    applyAutoLayout();
    return;
  }

  // Ctrl+S: prevent browser save
  if (ctrl && e.key === 's') {
    e.preventDefault();
    // Toggle YAML overlay as a save action
    if (!state.yamlOverlayVisible) {
      state.toggleYamlOverlay();
    }
    return;
  }

  // Delete / Backspace: remove selected node
  if (e.key === 'Delete' || e.key === 'Backspace') {
    const nodeId = state.selectedNodeId;
    if (!nodeId) return;

    // Determine node type and remove
    const worker = state.workers.find((w) => w.id === nodeId);
    if (worker) {
      state.removeWorker(nodeId);
      state.selectNode(null);
      applyAutoLayout();
      return;
    }

    const source = state.physicalSources.find((s) => s.id === nodeId);
    if (source) {
      state.removePhysicalSource(nodeId);
      state.selectNode(null);
      applyAutoLayout();
      return;
    }

    const sink = state.sinks.find((s) => s.id === nodeId);
    if (sink) {
      state.removeSink(nodeId);
      state.selectNode(null);
      applyAutoLayout();
      return;
    }
  }
}
