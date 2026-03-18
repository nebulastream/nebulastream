import { useState, useRef, useEffect } from 'react';
import { useStore } from '../../store';

interface LogicalSourceSelectProps {
  value: string;
  onChange: (logicalSourceId: string) => void;
}

export default function LogicalSourceSelect({ value, onChange }: LogicalSourceSelectProps) {
  const logicalSources = useStore((s) => s.logicalSources);
  const [filter, setFilter] = useState('');
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  // Find current logical source name
  const current = logicalSources.find((ls) => ls.id === value);
  const displayName = current ? current.name : '';

  // Check for orphan (value set but no matching logical source)
  const isOrphan = value !== '' && !current;

  const filtered = logicalSources.filter((ls) =>
    ls.name.toLowerCase().includes(filter.toLowerCase()),
  );

  // Close dropdown on outside click
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  return (
    <div ref={containerRef} className="relative">
      <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Logical Source</label>
      {isOrphan && (
        <div className="mb-1 px-2 py-0.5 bg-yellow-100 text-yellow-800 text-xs rounded">
          Missing logical source
        </div>
      )}
      <input
        type="text"
        className="w-full px-2 py-1.5 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
        value={open ? filter : displayName}
        placeholder="Search logical sources..."
        onFocus={() => {
          setOpen(true);
          setFilter('');
        }}
        onChange={(e) => setFilter(e.target.value)}
      />
      {open && filtered.length > 0 && (
        <div className="absolute left-0 right-0 mt-1 bg-white dark:bg-gray-700 border border-gray-300 dark:border-gray-600 rounded shadow-lg z-10 max-h-40 overflow-y-auto">
          {filtered.map((ls) => (
            <button
              key={ls.id}
              type="button"
              className="w-full text-left px-2 py-1.5 text-sm hover:bg-blue-50 dark:hover:bg-gray-600 dark:text-gray-100"
              onClick={() => {
                onChange(ls.id);
                setFilter('');
                setOpen(false);
              }}
            >
              {ls.name}
            </button>
          ))}
        </div>
      )}
      {value && (
        <button
          type="button"
          className="mt-1 text-xs text-gray-400 hover:text-gray-600"
          onClick={() => onChange('')}
        >
          Clear
        </button>
      )}
    </div>
  );
}
