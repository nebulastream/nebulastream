import React, { useCallback } from 'react';
import { useReactFlow } from '@xyflow/react';
import { Plus, Code, Sun, Moon } from 'lucide-react';
import { useStore } from '../../store';
import { applyAutoLayout } from '../../lib/autoLayout';

const Toolbar: React.FC = () => {
  const addWorker = useStore((s) => s.addWorker);
  const toggleYamlOverlay = useStore((s) => s.toggleYamlOverlay);
  const yamlOverlayVisible = useStore((s) => s.yamlOverlayVisible);
  const darkMode = useStore((s) => s.darkMode);
  const toggleDarkMode = useStore((s) => s.toggleDarkMode);

  const { screenToFlowPosition } = useReactFlow();

  const handleAddWorker = useCallback(() => {
    // Place new worker at center of current viewport
    const position = screenToFlowPosition({
      x: window.innerWidth / 2,
      y: window.innerHeight / 2,
    });
    addWorker(position);
    applyAutoLayout();
  }, [addWorker, screenToFlowPosition]);

  return (
    <div className="toolbar">
      <button
        className="toolbar__btn"
        onClick={handleAddWorker}
        title="Add Worker"
        data-testid="add-worker-btn"
      >
        <Plus size={16} />
        <span>Add Worker</span>
      </button>
      <button
        className="toolbar__btn"
        onClick={toggleYamlOverlay}
        title="Toggle YAML overlay"
        data-testid="yaml-toggle-btn"
        style={{
          background: yamlOverlayVisible ? (darkMode ? '#312e81' : '#e0e7ff') : undefined,
        }}
      >
        <Code size={16} />
        <span>YAML</span>
      </button>
      <button
        className="toolbar__btn"
        onClick={toggleDarkMode}
        title="Toggle dark mode"
        data-testid="dark-mode-toggle"
      >
        {darkMode ? <Sun size={16} /> : <Moon size={16} />}
        <span>{darkMode ? 'Light' : 'Dark'}</span>
      </button>
    </div>
  );
};

export default Toolbar;
