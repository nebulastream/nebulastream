import React, { type DragEvent } from 'react';
import { Database, Printer, Monitor } from 'lucide-react';

interface PaletteItem {
  type: string;
  label: string;
  icon: React.ReactNode;
  className: string;
}

const paletteItems: PaletteItem[] = [
  {
    type: 'worker',
    label: 'Worker',
    icon: <Monitor size={20} />,
    className: 'palette-item--worker',
  },
  {
    type: 'source',
    label: 'Source',
    icon: <Database size={20} />,
    className: 'palette-item--source',
  },
  {
    type: 'sink',
    label: 'Sink',
    icon: <Printer size={20} />,
    className: 'palette-item--sink',
  },
];

const Palette: React.FC = () => {
  const onDragStart = (event: DragEvent<HTMLDivElement>, nodeType: string) => {
    event.dataTransfer.setData('application/reactflow', nodeType);
    event.dataTransfer.effectAllowed = 'move';
  };

  return (
    <div className="palette">
      <h3 className="palette__title">Components</h3>
      {paletteItems.map((item) => (
        <div
          key={item.type}
          className={`palette-item ${item.className}`}
          draggable
          data-testid={`palette-${item.type}`}
          onDragStart={(e) => onDragStart(e, item.type)}
        >
          {item.icon}
          <span>{item.label}</span>
        </div>
      ))}
    </div>
  );
};

export default Palette;
