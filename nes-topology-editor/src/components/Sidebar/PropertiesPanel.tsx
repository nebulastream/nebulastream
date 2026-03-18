import { MousePointer } from 'lucide-react';
import { useStore } from '../../store';
import WorkerPanel from './WorkerPanel';
import SourcePanel from './SourcePanel';
import SinkPanel from './SinkPanel';

export default function PropertiesPanel() {
  const selectedNodeId = useStore((s) => s.selectedNodeId);
  const workers = useStore((s) => s.workers);
  const physicalSources = useStore((s) => s.physicalSources);
  const sinks = useStore((s) => s.sinks);

  if (!selectedNodeId) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-gray-400 dark:text-gray-500">
        <MousePointer className="w-8 h-8 mb-2" />
        <p className="text-sm">Select a node to edit its properties</p>
      </div>
    );
  }

  const worker = workers.find((w) => w.id === selectedNodeId);
  if (worker) return <WorkerPanel worker={worker} />;

  const source = physicalSources.find((s) => s.id === selectedNodeId);
  if (source) return <SourcePanel source={source} />;

  const sink = sinks.find((s) => s.id === selectedNodeId);
  if (sink) return <SinkPanel sink={sink} />;

  return (
    <div className="flex flex-col items-center justify-center h-full text-gray-400 dark:text-gray-500">
      <p className="text-sm">Node not found</p>
    </div>
  );
}
