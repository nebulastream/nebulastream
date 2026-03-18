import { useCallback, useRef, useState } from 'react';
import { useStore } from '../../store';
import type { SidebarTab } from '../../store/uiSlice';
import PropertiesPanel from './PropertiesPanel';
import LogicalSourcesPanel from './LogicalSourcesPanel';

const MIN_WIDTH = 280;
const MAX_WIDTH = 600;
const DEFAULT_WIDTH = 360;

export default function Sidebar() {
  const sidebarTab = useStore((s) => s.sidebarTab);
  const setSidebarTab = useStore((s) => s.setSidebarTab);
  const [width, setWidth] = useState(DEFAULT_WIDTH);
  const dragging = useRef(false);
  const startX = useRef(0);
  const startWidth = useRef(DEFAULT_WIDTH);

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = true;
    startX.current = e.clientX;
    startWidth.current = width;

    const handleMouseMove = (ev: MouseEvent) => {
      if (!dragging.current) return;
      // Dragging left edge: moving left increases width
      const delta = startX.current - ev.clientX;
      const newWidth = Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, startWidth.current + delta));
      setWidth(newWidth);
    };

    const handleMouseUp = () => {
      dragging.current = false;
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
  }, [width]);

  const tabs: { key: SidebarTab; label: string }[] = [
    { key: 'properties', label: 'Properties' },
    { key: 'sources', label: 'Sources' },
  ];

  return (
    <div
      className="relative flex flex-col border-l border-gray-200 bg-white dark:bg-gray-800 dark:border-gray-700"
      style={{ width, minWidth: MIN_WIDTH }}
    >
      {/* Resize handle */}
      <div
        className="absolute left-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-blue-400 active:bg-blue-500 z-20"
        onMouseDown={handleMouseDown}
      />

      {/* Tab bar */}
      <div className="flex border-b border-gray-200 dark:border-gray-700">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            type="button"
            className={`flex-1 px-3 py-2 text-sm font-medium ${
              sidebarTab === tab.key
                ? 'text-blue-600 border-b-2 border-blue-600'
                : 'text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200'
            }`}
            onClick={() => setSidebarTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div className="flex-1 overflow-y-auto p-3 dark:text-gray-100">
        {sidebarTab === 'properties' ? (
          <PropertiesPanel />
        ) : (
          <LogicalSourcesPanel />
        )}
      </div>
    </div>
  );
}
