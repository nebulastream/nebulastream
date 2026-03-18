import { describe, it, expect, beforeEach } from 'vitest';
import { useStore } from '../store';
import { selectReactFlowNodes, selectReactFlowEdges } from '../store/selectors';

describe('Topology Store', () => {
  beforeEach(() => {
    // Reset store to initial state before each test
    useStore.setState({
      workers: [],
      logicalSources: [],
      physicalSources: [],
      sinks: [],
      selectedNodeId: null,
      clipboard: null,
    });
  });

  describe('Worker CRUD and auto-naming', () => {
    it('addWorker creates a worker with auto-generated host', () => {
      const state = useStore.getState();
      state.addWorker();
      const updated = useStore.getState();
      expect(updated.workers).toHaveLength(1);
      expect(updated.workers[0]!.host).toBe('worker-1:9090');
      expect(updated.workers[0]!.grpc).toBe('worker-1:8080');
      expect(updated.workers[0]!.capacity).toBe(10000);
      expect(updated.workers[0]!.downstream).toEqual([]);
    });

    it('addWorker auto-increments host numbers', () => {
      const state = useStore.getState();
      state.addWorker();
      state.addWorker();
      state.addWorker();
      const updated = useStore.getState();
      expect(updated.workers).toHaveLength(3);
      expect(updated.workers[0]!.host).toBe('worker-1:9090');
      expect(updated.workers[1]!.host).toBe('worker-2:9090');
      expect(updated.workers[2]!.host).toBe('worker-3:9090');
    });

    it('addWorker accepts optional position', () => {
      const state = useStore.getState();
      state.addWorker({ x: 100, y: 200 });
      const updated = useStore.getState();
      expect(updated.workers[0]!.position).toEqual({ x: 100, y: 200 });
    });

    it('removeWorker removes the worker by ID', () => {
      const state = useStore.getState();
      state.addWorker();
      state.addWorker();
      const afterAdd = useStore.getState();
      const workerId = afterAdd.workers[0]!.id;
      afterAdd.removeWorker(workerId);
      const updated = useStore.getState();
      expect(updated.workers).toHaveLength(1);
      expect(updated.workers[0]!.host).toBe('worker-2:9090');
    });

    it('removeWorker clears downstream references to removed worker', () => {
      const state = useStore.getState();
      state.addWorker();
      state.addWorker();
      const afterAdd = useStore.getState();
      const w1Id = afterAdd.workers[0]!.id;
      const w2Id = afterAdd.workers[1]!.id;
      afterAdd.connectDownstream(w1Id, w2Id);
      // Verify connection exists
      const connected = useStore.getState();
      expect(connected.workers[0]!.downstream).toContain(w2Id);
      // Remove target worker
      connected.removeWorker(w2Id);
      const updated = useStore.getState();
      expect(updated.workers[0]!.downstream).not.toContain(w2Id);
    });
  });

  describe('Downstream connection management', () => {
    it('connectDownstream adds target to source worker downstream', () => {
      const state = useStore.getState();
      state.addWorker();
      state.addWorker();
      const afterAdd = useStore.getState();
      const w1Id = afterAdd.workers[0]!.id;
      const w2Id = afterAdd.workers[1]!.id;
      afterAdd.connectDownstream(w1Id, w2Id);
      const updated = useStore.getState();
      expect(updated.workers[0]!.downstream).toContain(w2Id);
    });

    it('connectDownstream does not duplicate connections', () => {
      const state = useStore.getState();
      state.addWorker();
      state.addWorker();
      const afterAdd = useStore.getState();
      const w1Id = afterAdd.workers[0]!.id;
      const w2Id = afterAdd.workers[1]!.id;
      afterAdd.connectDownstream(w1Id, w2Id);
      useStore.getState().connectDownstream(w1Id, w2Id);
      const updated = useStore.getState();
      expect(
        updated.workers[0]!.downstream.filter((d) => d === w2Id),
      ).toHaveLength(1);
    });

    it('disconnectDownstream removes the link', () => {
      const state = useStore.getState();
      state.addWorker();
      state.addWorker();
      const afterAdd = useStore.getState();
      const w1Id = afterAdd.workers[0]!.id;
      const w2Id = afterAdd.workers[1]!.id;
      afterAdd.connectDownstream(w1Id, w2Id);
      useStore.getState().disconnectDownstream(w1Id, w2Id);
      const updated = useStore.getState();
      expect(updated.workers[0]!.downstream).not.toContain(w2Id);
    });
  });

  describe('Source/Sink attachment and reassignment', () => {
    it('addPhysicalSource adds a source to the store', () => {
      const state = useStore.getState();
      state.addPhysicalSource({
        logicalSourceId: 'ls-1',
        hostWorkerId: '',
        type: 'Generator',
        sourceConfig: {},
        parserConfig: {},
        position: { x: 0, y: 0 },
      });
      const updated = useStore.getState();
      expect(updated.physicalSources).toHaveLength(1);
      expect(updated.physicalSources[0]!.type).toBe('Generator');
    });

    it('attachSourceToWorker updates source hostWorkerId', () => {
      const state = useStore.getState();
      state.addWorker();
      state.addPhysicalSource({
        logicalSourceId: 'ls-1',
        hostWorkerId: '',
        type: 'Generator',
        sourceConfig: {},
        parserConfig: {},
        position: { x: 0, y: 0 },
      });
      const afterAdd = useStore.getState();
      const workerId = afterAdd.workers[0]!.id;
      const sourceId = afterAdd.physicalSources[0]!.id;
      afterAdd.attachSourceToWorker(sourceId, workerId);
      const updated = useStore.getState();
      expect(updated.physicalSources[0]!.hostWorkerId).toBe(workerId);
    });

    it('attachSinkToWorker updates sink hostWorkerId', () => {
      const state = useStore.getState();
      state.addWorker();
      state.addSink({
        name: 'test-sink',
        hostWorkerId: '',
        type: 'Print',
        schema: [],
        config: {},
        position: { x: 0, y: 0 },
      });
      const afterAdd = useStore.getState();
      const workerId = afterAdd.workers[0]!.id;
      const sinkId = afterAdd.sinks[0]!.id;
      afterAdd.attachSinkToWorker(sinkId, workerId);
      const updated = useStore.getState();
      expect(updated.sinks[0]!.hostWorkerId).toBe(workerId);
    });

    it('removePhysicalSource removes by ID', () => {
      const state = useStore.getState();
      state.addPhysicalSource({
        logicalSourceId: 'ls-1',
        hostWorkerId: '',
        type: 'CSV',
        sourceConfig: {},
        parserConfig: {},
        position: { x: 0, y: 0 },
      });
      const afterAdd = useStore.getState();
      afterAdd.removePhysicalSource(afterAdd.physicalSources[0]!.id);
      const updated = useStore.getState();
      expect(updated.physicalSources).toHaveLength(0);
    });

    it('removeSink removes by ID', () => {
      const state = useStore.getState();
      state.addSink({
        name: 'test-sink',
        hostWorkerId: '',
        type: 'Void',
        schema: [],
        config: {},
        position: { x: 0, y: 0 },
      });
      const afterAdd = useStore.getState();
      afterAdd.removeSink(afterAdd.sinks[0]!.id);
      const updated = useStore.getState();
      expect(updated.sinks).toHaveLength(0);
    });
  });

  describe('Position updates', () => {
    it('updateNodePosition updates worker position', () => {
      const state = useStore.getState();
      state.addWorker({ x: 0, y: 0 });
      const afterAdd = useStore.getState();
      const workerId = afterAdd.workers[0]!.id;
      afterAdd.updateNodePosition(workerId, { x: 500, y: 300 });
      const updated = useStore.getState();
      expect(updated.workers[0]!.position).toEqual({ x: 500, y: 300 });
    });

    it('updateNodePosition updates source position', () => {
      const state = useStore.getState();
      state.addPhysicalSource({
        logicalSourceId: 'ls-1',
        hostWorkerId: '',
        type: 'Generator',
        sourceConfig: {},
        parserConfig: {},
        position: { x: 0, y: 0 },
      });
      const afterAdd = useStore.getState();
      const sourceId = afterAdd.physicalSources[0]!.id;
      afterAdd.updateNodePosition(sourceId, { x: 200, y: 100 });
      const updated = useStore.getState();
      expect(updated.physicalSources[0]!.position).toEqual({
        x: 200,
        y: 100,
      });
    });

    it('updateNodePosition updates sink position', () => {
      const state = useStore.getState();
      state.addSink({
        name: 'test-sink',
        hostWorkerId: '',
        type: 'File',
        schema: [],
        config: {},
        position: { x: 0, y: 0 },
      });
      const afterAdd = useStore.getState();
      const sinkId = afterAdd.sinks[0]!.id;
      afterAdd.updateNodePosition(sinkId, { x: 300, y: 400 });
      const updated = useStore.getState();
      expect(updated.sinks[0]!.position).toEqual({ x: 300, y: 400 });
    });
  });

  describe('Selector output shape', () => {
    it('selectReactFlowNodes returns nodes with correct types', () => {
      const state = useStore.getState();
      state.addWorker({ x: 100, y: 100 });
      state.addPhysicalSource({
        logicalSourceId: 'ls-1',
        hostWorkerId: '',
        type: 'Generator',
        sourceConfig: {},
        parserConfig: {},
        position: { x: 200, y: 200 },
      });
      state.addSink({
        name: 'test-sink',
        hostWorkerId: '',
        type: 'Print',
        schema: [],
        config: {},
        position: { x: 300, y: 300 },
      });

      const updated = useStore.getState();
      const nodes = selectReactFlowNodes(updated);

      expect(nodes).toHaveLength(3);
      const workerNode = nodes.find((n) => n.type === 'worker');
      const sourceNode = nodes.find((n) => n.type === 'source');
      const sinkNode = nodes.find((n) => n.type === 'sink');

      expect(workerNode).toBeDefined();
      expect(workerNode!.position).toEqual({ x: 100, y: 100 });
      expect(sourceNode).toBeDefined();
      expect(sourceNode!.position).toEqual({ x: 200, y: 200 });
      expect(sinkNode).toBeDefined();
      expect(sinkNode!.position).toEqual({ x: 300, y: 300 });
    });

    it('selectReactFlowEdges returns downstream and placement edges', () => {
      const state = useStore.getState();
      state.addWorker({ x: 0, y: 0 });
      state.addWorker({ x: 200, y: 0 });
      const afterWorkers = useStore.getState();
      const w1Id = afterWorkers.workers[0]!.id;
      const w2Id = afterWorkers.workers[1]!.id;

      afterWorkers.connectDownstream(w1Id, w2Id);

      afterWorkers.addPhysicalSource({
        logicalSourceId: 'ls-1',
        hostWorkerId: w1Id,
        type: 'Generator',
        sourceConfig: {},
        parserConfig: {},
        position: { x: 50, y: 50 },
      });

      afterWorkers.addSink({
        name: 'test-sink',
        hostWorkerId: w2Id,
        type: 'Print',
        schema: [],
        config: {},
        position: { x: 250, y: 50 },
      });

      const updated = useStore.getState();
      const edges = selectReactFlowEdges(updated);

      // Should have: 1 downstream edge + 1 source-to-worker + 1 sink-to-worker = 3 edges
      expect(edges).toHaveLength(3);

      const downstreamEdge = edges.find(
        (e) => e.source === w1Id && e.target === w2Id,
      );
      expect(downstreamEdge).toBeDefined();

      const sourceId = updated.physicalSources[0]!.id;
      const sinkId = updated.sinks[0]!.id;

      const sourcePlacement = edges.find((e) => e.source === sourceId);
      expect(sourcePlacement).toBeDefined();
      expect(sourcePlacement!.target).toBe(w1Id);

      const sinkPlacement = edges.find((e) => e.target === sinkId);
      expect(sinkPlacement).toBeDefined();
      expect(sinkPlacement!.source).toBe(w2Id);
    });

    it('selectReactFlowEdges does not include edges for unattached sources/sinks', () => {
      const state = useStore.getState();
      state.addPhysicalSource({
        logicalSourceId: 'ls-1',
        hostWorkerId: '',
        type: 'Generator',
        sourceConfig: {},
        parserConfig: {},
        position: { x: 0, y: 0 },
      });
      state.addSink({
        name: 'test-sink',
        hostWorkerId: '',
        type: 'Print',
        schema: [],
        config: {},
        position: { x: 0, y: 0 },
      });

      const updated = useStore.getState();
      const edges = selectReactFlowEdges(updated);
      expect(edges).toHaveLength(0);
    });
  });
});
