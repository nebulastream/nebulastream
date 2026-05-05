import { Plus } from 'lucide-react';
import type { Worker } from '../../lib/types';
import { useStore } from '../../store';
import { SOURCE_CONFIGS, PARSER_CONFIG, SINK_CONFIGS, buildDefaults } from '../../lib/sourceConfigs';

interface WorkerPanelProps {
  worker: Worker;
}

export default function WorkerPanel({ worker }: WorkerPanelProps) {
  const workers = useStore((s) => s.workers);
  const physicalSources = useStore((s) => s.physicalSources);
  const sinks = useStore((s) => s.sinks);
  const selectNode = useStore((s) => s.selectNode);

  const connectedSources = physicalSources.filter((s) => s.hostWorkerId === worker.id);
  const connectedSinks = sinks.filter((s) => s.hostWorkerId === worker.id);
  const downstreamWorkers = workers.filter((w) => worker.downstream.includes(w.id));
  const upstreamWorkers = workers.filter((w) => w.downstream.includes(worker.id));

  const handleChange = (field: string, value: string | number) => {
    useStore.getState().updateWorker(worker.id, { [field]: value });
  };

  const handleAddSource = () => {
    const store = useStore.getState();
    const sourceIndex = connectedSources.length;
    const defaultType = 'Generator';
    store.addPhysicalSource({
      logicalSourceId: '',
      hostWorkerId: worker.id,
      type: defaultType,
      sourceConfig: buildDefaults(SOURCE_CONFIGS[defaultType] ?? []),
      parserConfig: buildDefaults(PARSER_CONFIG),
      position: { x: worker.position.x - 200, y: worker.position.y + sourceIndex * 80 },
    });
    // Select the newly created source
    const newSources = useStore.getState().physicalSources;
    const newSource = newSources[newSources.length - 1];
    if (newSource) {
      store.selectNode(newSource.id);
    }
  };

  const handleAddSink = () => {
    const store = useStore.getState();
    const sinkIndex = connectedSinks.length;
    const defaultSinkType = 'Print';
    store.addSink({
      name: '',
      hostWorkerId: worker.id,
      type: defaultSinkType,
      schema: [],
      config: buildDefaults(SINK_CONFIGS[defaultSinkType] ?? []),
      parserConfig: {},
      position: { x: worker.position.x + 300, y: worker.position.y + sinkIndex * 80 },
    });
    // Select the newly created sink
    const newSinks = useStore.getState().sinks;
    const newSink = newSinks[newSinks.length - 1];
    if (newSink) {
      store.selectNode(newSink.id);
    }
  };

  const linkClass = 'text-blue-600 hover:text-blue-800 hover:underline cursor-pointer';
  const addBtnClass = 'flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 cursor-pointer mt-1';

  return (
    <div>
      <h3 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-3">Worker</h3>

      <div className="mb-3">
        <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Host Address</label>
        <input
          type="text"
          className="w-full px-2 py-1.5 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
          value={worker.host}
          onChange={(e) => handleChange('host', e.target.value)}
        />
      </div>

      <div className="mb-3">
        <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Data Address</label>
        <input
          type="text"
          className="w-full px-2 py-1.5 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
          value={worker.dataAddress}
          onChange={(e) => handleChange('dataAddress', e.target.value)}
        />
      </div>

      <div className="mb-3">
        <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Capacity</label>
        <input
          type="number"
          className="w-full px-2 py-1.5 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
          value={worker.capacity}
          onChange={(e) => handleChange('capacity', Number(e.target.value))}
        />
      </div>

      <div className="mt-4">
        <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase mb-2">Upstream Workers</h4>
        {upstreamWorkers.length === 0 ? (
          <p className="text-xs text-gray-400">None</p>
        ) : (
          <ul className="text-xs space-y-1">
            {upstreamWorkers.map((w) => (
              <li key={w.id}>
                <button type="button" className={linkClass} onClick={() => selectNode(w.id)}>
                  {w.host}
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>

      <div className="mt-3">
        <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase mb-2">Downstream Workers</h4>
        {downstreamWorkers.length === 0 ? (
          <p className="text-xs text-gray-400">None</p>
        ) : (
          <ul className="text-xs space-y-1">
            {downstreamWorkers.map((w) => (
              <li key={w.id}>
                <button type="button" className={linkClass} onClick={() => selectNode(w.id)}>
                  {w.host}
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>

      <div className="mt-3">
        <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase mb-2">Connected Sources</h4>
        {connectedSources.length === 0 ? (
          <p className="text-xs text-gray-400">No sources attached</p>
        ) : (
          <ul className="text-xs space-y-1">
            {connectedSources.map((s) => (
              <li key={s.id}>
                <button type="button" className={linkClass} onClick={() => selectNode(s.id)}>
                  {s.type}
                </button>
              </li>
            ))}
          </ul>
        )}
        <button type="button" className={addBtnClass} onClick={handleAddSource} data-testid="add-source-btn">
          <Plus size={12} />
          Add Source
        </button>
      </div>

      <div className="mt-3">
        <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase mb-2">Connected Sinks</h4>
        {connectedSinks.length === 0 ? (
          <p className="text-xs text-gray-400">No sinks attached</p>
        ) : (
          <ul className="text-xs space-y-1">
            {connectedSinks.map((s) => (
              <li key={s.id}>
                <button type="button" className={linkClass} onClick={() => selectNode(s.id)}>
                  {s.name || s.type}
                </button>
              </li>
            ))}
          </ul>
        )}
        <button type="button" className={addBtnClass} onClick={handleAddSink} data-testid="add-sink-btn">
          <Plus size={12} />
          Add Sink
        </button>
      </div>
    </div>
  );
}
