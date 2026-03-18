import { Plus, Trash2 } from 'lucide-react';
import { useStore } from '../../store';
import SchemaBuilder from './SchemaBuilder';

export default function LogicalSourcesPanel() {
  const logicalSources = useStore((s) => s.logicalSources);
  const addLogicalSource = useStore((s) => s.addLogicalSource);
  const updateLogicalSource = useStore((s) => s.updateLogicalSource);
  const removeLogicalSource = useStore((s) => s.removeLogicalSource);

  const handleCreate = () => {
    addLogicalSource(`Source_${logicalSources.length + 1}`);
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-200">Logical Sources</h3>
        <button
          type="button"
          aria-label="New Source"
          onClick={handleCreate}
          className="flex items-center gap-1 text-sm text-blue-600 hover:text-blue-800"
        >
          <Plus size={14} />
          New Source
        </button>
      </div>

      {logicalSources.length === 0 ? (
        <p className="text-gray-400 text-sm text-center py-8">
          No logical sources defined
        </p>
      ) : (
        <div className="space-y-3">
          {logicalSources.map((ls) => (
            <div
              key={ls.id}
              className="border border-gray-200 dark:border-gray-600 rounded-lg p-3 bg-white dark:bg-gray-700"
            >
              <div className="flex items-center gap-2 mb-2">
                <input
                  type="text"
                  value={ls.name}
                  onChange={(e) =>
                    updateLogicalSource(ls.id, { name: e.target.value })
                  }
                  className="flex-1 text-sm font-medium border border-gray-300 dark:border-gray-600 dark:bg-gray-600 dark:text-gray-100 rounded px-2 py-1"
                />
                <button
                  type="button"
                  aria-label="Delete"
                  onClick={() => removeLogicalSource(ls.id)}
                  className="text-red-400 hover:text-red-600"
                >
                  <Trash2 size={14} />
                </button>
              </div>

              <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">Schema</div>
              <SchemaBuilder
                fields={ls.schema}
                onChange={(schema) =>
                  updateLogicalSource(ls.id, { schema })
                }
              />
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
