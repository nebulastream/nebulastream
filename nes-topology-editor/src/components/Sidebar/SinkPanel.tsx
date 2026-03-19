import type { Sink } from '../../lib/types';
import { useStore } from '../../store';
import { getSinkTypes, getSinkFields, buildDefaults } from '../../lib/sourceConfigs';
import type { FormFieldDef } from '../../lib/sourceConfigs';
import ConfigForm from './ConfigForm';
import SchemaBuilder from './SchemaBuilder';

interface SinkPanelProps {
  sink: Sink;
}

export default function SinkPanel({ sink }: SinkPanelProps) {
  const workers = useStore((s) => s.workers);
  const selectNode = useStore((s) => s.selectNode);
  const configMetadata = useStore((s) => s.configMetadata);
  const hostWorker = workers.find((w) => w.id === sink.hostWorkerId);

  const handleUpdate = (updates: Partial<Omit<Sink, 'id'>>) => {
    useStore.getState().updateSink(sink.id, updates);
  };

  const handleTypeChange = (newType: string) => {
    const configFields = getSinkFields(newType, configMetadata);
    handleUpdate({ type: newType, config: buildDefaults(configFields), parserConfig: sink.parserConfig });
  };

  const sinkTypes = getSinkTypes(configMetadata);
  const sinkFields = getSinkFields(sink.type, configMetadata);

  return (
    <div>
      <h3 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-3">
        Sink{' '}
        <span className="inline-block px-1.5 py-0.5 bg-orange-100 dark:bg-orange-900 text-orange-700 dark:text-orange-300 text-xs rounded">
          {sink.name || sink.type}
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

      {/* Sink Name */}
      <div className="mb-4">
        <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Sink Name</label>
        <input
          type="text"
          className="w-full px-2 py-1.5 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
          value={sink.name}
          onChange={(e) => handleUpdate({ name: e.target.value })}
        />
      </div>

      {/* Sink Type */}
      <div className="mb-4">
        <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Sink Type</label>
        <select
          className="w-full px-2 py-1.5 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
          value={sink.type}
          onChange={(e) => handleTypeChange(e.target.value)}
        >
          {sinkTypes.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </div>

      {/* Schema */}
      <div className="mb-4">
        <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase mb-2">Schema</h4>
        <SchemaBuilder
          fields={sink.schema}
          onChange={(schema) => handleUpdate({ schema })}
        />
      </div>

      {/* Configuration */}
      <div className="mb-4">
        <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase mb-2">Configuration</h4>
        {sinkFields.length === 0 ? (
          <p className="text-xs text-gray-400 italic">No configuration required</p>
        ) : (
          <ConfigForm
            fields={sinkFields}
            values={sink.config}
            onChange={(key, value) =>
              handleUpdate({ config: { ...sink.config, [key]: value } })
            }
          />
        )}
      </div>
    </div>
  );
}
