import { useState } from 'react';
import type { PhysicalSource } from '../../lib/types';
import { useStore } from '../../store';
import { SOURCE_TYPES, SOURCE_CONFIGS, PARSER_CONFIG, buildDefaults } from '../../lib/sourceConfigs';
import ConfigForm from './ConfigForm';
import LogicalSourceSelect from './LogicalSourceSelect';

interface SourcePanelProps {
  source: PhysicalSource;
}

export default function SourcePanel({ source }: SourcePanelProps) {
  const [parserOpen, setParserOpen] = useState(true);
  const workers = useStore((s) => s.workers);
  const selectNode = useStore((s) => s.selectNode);
  const hostWorker = workers.find((w) => w.id === source.hostWorkerId);

  const handleUpdate = (updates: Partial<Omit<PhysicalSource, 'id'>>) => {
    useStore.getState().updatePhysicalSource(source.id, updates);
  };

  const handleTypeChange = (newType: string) => {
    const configFields = SOURCE_CONFIGS[newType] ?? [];
    handleUpdate({ type: newType, sourceConfig: buildDefaults(configFields) });
  };

  const sourceFields = SOURCE_CONFIGS[source.type] ?? [];

  return (
    <div>
      <h3 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-3">
        Source{' '}
        <span className="inline-block px-1.5 py-0.5 bg-green-100 dark:bg-green-900 text-green-700 dark:text-green-300 text-xs rounded">
          {source.type}
        </span>
      </h3>

      {/* Host Worker */}
      <div className="mb-4">
        <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase mb-1">Host Worker</h4>
        {hostWorker ? (
          <button
            type="button"
            className="text-xs text-blue-600 hover:text-blue-800 hover:underline cursor-pointer"
            onClick={() => selectNode(hostWorker.id)}
          >
            {hostWorker.host}
          </button>
        ) : (
          <p className="text-xs text-gray-400">Not attached to a worker</p>
        )}
      </div>

      {/* Logical Source */}
      <div className="mb-4">
        <LogicalSourceSelect
          value={source.logicalSourceId}
          onChange={(id) => handleUpdate({ logicalSourceId: id })}
        />
      </div>

      {/* Source Type */}
      <div className="mb-4">
        <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Source Type</label>
        <select
          className="w-full px-2 py-1.5 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
          value={source.type}
          onChange={(e) => handleTypeChange(e.target.value)}
        >
          {SOURCE_TYPES.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </div>

      {/* Source Configuration */}
      <div className="mb-4">
        <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase mb-2">Source Configuration</h4>
        <ConfigForm
          fields={sourceFields}
          values={source.sourceConfig}
          onChange={(key, value) =>
            handleUpdate({ sourceConfig: { ...source.sourceConfig, [key]: value } })
          }
        />
      </div>

      {/* Parser Configuration */}
      <div className="mb-4">
        <button
          type="button"
          className="flex items-center text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase mb-2"
          onClick={() => setParserOpen(!parserOpen)}
        >
          <span className="mr-1">{parserOpen ? '\u25BC' : '\u25B6'}</span>
          Parser Configuration
        </button>
        {parserOpen && (
          <ConfigForm
            fields={PARSER_CONFIG}
            values={source.parserConfig}
            onChange={(key, value) =>
              handleUpdate({ parserConfig: { ...source.parserConfig, [key]: value } })
            }
          />
        )}
      </div>
    </div>
  );
}
