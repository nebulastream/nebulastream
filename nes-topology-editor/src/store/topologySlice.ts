import type { StateCreator } from 'zustand';
import type {
  Worker,
  LogicalSource,
  PhysicalSource,
  Sink,
  Position,
} from '../lib/types';
import { generateId, generateWorkerHost } from '../lib/ids';

export interface TopologySlice {
  workers: Worker[];
  logicalSources: LogicalSource[];
  physicalSources: PhysicalSource[];
  sinks: Sink[];

  addWorker: (position?: Position) => void;
  removeWorker: (id: string) => void;
  addPhysicalSource: (source: Omit<PhysicalSource, 'id'>) => void;
  removePhysicalSource: (id: string) => void;
  addSink: (sink: Omit<Sink, 'id'>) => void;
  removeSink: (id: string) => void;
  connectDownstream: (fromId: string, toId: string) => void;
  disconnectDownstream: (fromId: string, toId: string) => void;
  attachSourceToWorker: (sourceId: string, workerId: string) => void;
  attachSinkToWorker: (sinkId: string, workerId: string) => void;
  detachSourceFromWorker: (sourceId: string) => void;
  detachSinkFromWorker: (sinkId: string) => void;
  updateNodePosition: (id: string, position: Position) => void;
  setAllNodePositions: (positions: { id: string; position: Position }[]) => void;
  addNodes: (nodes: { id: string; type: string; position: Position; data: Record<string, unknown> }[]) => void;

  updateWorker: (id: string, updates: Partial<Omit<Worker, 'id'>>) => void;
  updatePhysicalSource: (id: string, updates: Partial<Omit<PhysicalSource, 'id'>>) => void;
  updateSink: (id: string, updates: Partial<Omit<Sink, 'id'>>) => void;
  addLogicalSource: (name: string) => void;
  updateLogicalSource: (id: string, updates: Partial<Omit<LogicalSource, 'id'>>) => void;
  removeLogicalSource: (id: string) => void;
  replaceTopology: (data: {
    workers: Worker[];
    logicalSources: LogicalSource[];
    physicalSources: PhysicalSource[];
    sinks: Sink[];
  }) => void;
}

export const createTopologySlice: StateCreator<
  TopologySlice,
  [],
  [],
  TopologySlice
> = (set) => ({
  workers: [],
  logicalSources: [],
  physicalSources: [],
  sinks: [],

  addWorker: (position?: Position) =>
    set((state) => {
      const host = generateWorkerHost(state.workers);
      const hostBase = host.replace(':9090', '');
      const worker: Worker = {
        id: generateId(),
        host,
        grpc: `${hostBase}:8080`,
        capacity: 10000,
        downstream: [],
        position: position ?? { x: 0, y: 0 },
      };
      return { workers: [...state.workers, worker] };
    }),

  removeWorker: (id: string) =>
    set((state) => ({
      workers: state.workers
        .filter((w) => w.id !== id)
        .map((w) => ({
          ...w,
          downstream: w.downstream.filter((d) => d !== id),
        })),
      // Cascade-delete: sources and sinks belong to their host worker
      physicalSources: state.physicalSources.filter((s) => s.hostWorkerId !== id),
      sinks: state.sinks.filter((s) => s.hostWorkerId !== id),
    })),

  addPhysicalSource: (source: Omit<PhysicalSource, 'id'>) =>
    set((state) => ({
      physicalSources: [
        ...state.physicalSources,
        { ...source, id: generateId() },
      ],
    })),

  removePhysicalSource: (id: string) =>
    set((state) => ({
      physicalSources: state.physicalSources.filter((s) => s.id !== id),
    })),

  addSink: (sink: Omit<Sink, 'id'>) =>
    set((state) => ({
      sinks: [...state.sinks, { ...sink, id: generateId() }],
    })),

  removeSink: (id: string) =>
    set((state) => ({
      sinks: state.sinks.filter((s) => s.id !== id),
    })),

  connectDownstream: (fromId: string, toId: string) =>
    set((state) => ({
      workers: state.workers.map((w) =>
        w.id === fromId && !w.downstream.includes(toId)
          ? { ...w, downstream: [...w.downstream, toId] }
          : w,
      ),
    })),

  disconnectDownstream: (fromId: string, toId: string) =>
    set((state) => ({
      workers: state.workers.map((w) =>
        w.id === fromId
          ? { ...w, downstream: w.downstream.filter((d) => d !== toId) }
          : w,
      ),
    })),

  attachSourceToWorker: (sourceId: string, workerId: string) =>
    set((state) => ({
      physicalSources: state.physicalSources.map((s) =>
        s.id === sourceId ? { ...s, hostWorkerId: workerId } : s,
      ),
    })),

  attachSinkToWorker: (sinkId: string, workerId: string) =>
    set((state) => ({
      sinks: state.sinks.map((s) =>
        s.id === sinkId ? { ...s, hostWorkerId: workerId } : s,
      ),
    })),

  detachSourceFromWorker: (sourceId: string) =>
    set((state) => ({
      physicalSources: state.physicalSources.map((s) =>
        s.id === sourceId ? { ...s, hostWorkerId: '' } : s,
      ),
    })),

  detachSinkFromWorker: (sinkId: string) =>
    set((state) => ({
      sinks: state.sinks.map((s) =>
        s.id === sinkId ? { ...s, hostWorkerId: '' } : s,
      ),
    })),

  updateNodePosition: (id: string, position: Position) =>
    set((state) => ({
      workers: state.workers.map((w) =>
        w.id === id ? { ...w, position } : w,
      ),
      physicalSources: state.physicalSources.map((s) =>
        s.id === id ? { ...s, position } : s,
      ),
      sinks: state.sinks.map((s) =>
        s.id === id ? { ...s, position } : s,
      ),
    })),

  setAllNodePositions: (positions: { id: string; position: Position }[]) =>
    set((state) => {
      const posMap = new Map(positions.map((p) => [p.id, p.position]));
      return {
        workers: state.workers.map((w) =>
          posMap.has(w.id) ? { ...w, position: posMap.get(w.id)! } : w,
        ),
        physicalSources: state.physicalSources.map((s) =>
          posMap.has(s.id) ? { ...s, position: posMap.get(s.id)! } : s,
        ),
        sinks: state.sinks.map((s) =>
          posMap.has(s.id) ? { ...s, position: posMap.get(s.id)! } : s,
        ),
      };
    }),

  addNodes: (nodes: { id: string; type: string; position: Position; data: Record<string, unknown> }[]) =>
    set((state) => {
      const newWorkers: Worker[] = [];
      const newSources: PhysicalSource[] = [];
      const newSinks: Sink[] = [];

      for (const node of nodes) {
        if (node.type === 'worker' && node.data.worker) {
          newWorkers.push({ ...(node.data.worker as Worker), id: node.id, position: node.position });
        } else if (node.type === 'source' && node.data.source) {
          newSources.push({ ...(node.data.source as PhysicalSource), id: node.id, position: node.position });
        } else if (node.type === 'sink' && node.data.sink) {
          newSinks.push({ ...(node.data.sink as Sink), id: node.id, position: node.position });
        }
      }

      return {
        workers: [...state.workers, ...newWorkers],
        physicalSources: [...state.physicalSources, ...newSources],
        sinks: [...state.sinks, ...newSinks],
      };
    }),

  updateWorker: (id: string, updates: Partial<Omit<Worker, 'id'>>) =>
    set((state) => ({
      workers: state.workers.map((w) =>
        w.id === id ? { ...w, ...updates } : w,
      ),
    })),

  updatePhysicalSource: (id: string, updates: Partial<Omit<PhysicalSource, 'id'>>) =>
    set((state) => ({
      physicalSources: state.physicalSources.map((s) =>
        s.id === id ? { ...s, ...updates } : s,
      ),
    })),

  updateSink: (id: string, updates: Partial<Omit<Sink, 'id'>>) =>
    set((state) => ({
      sinks: state.sinks.map((s) =>
        s.id === id ? { ...s, ...updates } : s,
      ),
    })),

  addLogicalSource: (name: string) =>
    set((state) => ({
      logicalSources: [
        ...state.logicalSources,
        { id: generateId(), name, schema: [] },
      ],
    })),

  updateLogicalSource: (id: string, updates: Partial<Omit<LogicalSource, 'id'>>) =>
    set((state) => ({
      logicalSources: state.logicalSources.map((ls) =>
        ls.id === id ? { ...ls, ...updates } : ls,
      ),
    })),

  removeLogicalSource: (id: string) =>
    set((state) => ({
      logicalSources: state.logicalSources.filter((ls) => ls.id !== id),
    })),

  replaceTopology: (data) =>
    set(() => ({
      workers: data.workers,
      logicalSources: data.logicalSources,
      physicalSources: data.physicalSources,
      sinks: data.sinks,
    })),
});
