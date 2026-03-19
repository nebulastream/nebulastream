import { describe, it, expect, beforeEach } from 'vitest';
import { useStore } from '../store';

describe('Query Slice', () => {
  beforeEach(() => {
    useStore.setState({
      queries: [],
      selectedQueryId: null,
    });
  });

  describe('addQuery', () => {
    it('creates a query with generated id and empty sql', () => {
      useStore.getState().addQuery();
      const { queries } = useStore.getState();
      expect(queries).toHaveLength(1);
      expect(queries[0]!.id).toBeTruthy();
      expect(queries[0]!.name).toBe('');
      expect(queries[0]!.sql).toBe('');
    });

    it('creates multiple queries with unique ids', () => {
      useStore.getState().addQuery();
      useStore.getState().addQuery();
      const { queries } = useStore.getState();
      expect(queries).toHaveLength(2);
      expect(queries[0]!.id).not.toBe(queries[1]!.id);
    });
  });

  describe('removeQuery', () => {
    it('removes a query by id', () => {
      useStore.getState().addQuery();
      useStore.getState().addQuery();
      const { queries } = useStore.getState();
      const idToRemove = queries[0]!.id;
      useStore.getState().removeQuery(idToRemove);
      const updated = useStore.getState();
      expect(updated.queries).toHaveLength(1);
      expect(updated.queries[0]!.id).not.toBe(idToRemove);
    });

    it('does nothing if id not found', () => {
      useStore.getState().addQuery();
      useStore.getState().removeQuery('nonexistent');
      expect(useStore.getState().queries).toHaveLength(1);
    });
  });

  describe('updateQuery', () => {
    it('merges partial updates by id', () => {
      useStore.getState().addQuery();
      const { queries } = useStore.getState();
      const id = queries[0]!.id;
      useStore.getState().updateQuery(id, { name: 'My Query', sql: 'SELECT * FROM t' });
      const updated = useStore.getState();
      expect(updated.queries[0]!.name).toBe('My Query');
      expect(updated.queries[0]!.sql).toBe('SELECT * FROM t');
      expect(updated.queries[0]!.id).toBe(id);
    });

    it('updates only specified fields', () => {
      useStore.getState().addQuery();
      const { queries } = useStore.getState();
      const id = queries[0]!.id;
      useStore.getState().updateQuery(id, { name: 'Named' });
      const updated = useStore.getState();
      expect(updated.queries[0]!.name).toBe('Named');
      expect(updated.queries[0]!.sql).toBe('');
    });
  });
});

describe('Topology Slice - Update Actions', () => {
  beforeEach(() => {
    useStore.setState({
      workers: [],
      logicalSources: [],
      physicalSources: [],
      sinks: [],
    });
  });

  describe('updateWorker', () => {
    it('merges partial Worker updates by id', () => {
      useStore.getState().addWorker();
      const { workers } = useStore.getState();
      const id = workers[0]!.id;
      useStore.getState().updateWorker(id, { capacity: 5000, host: 'custom:9090' });
      const updated = useStore.getState();
      expect(updated.workers[0]!.capacity).toBe(5000);
      expect(updated.workers[0]!.host).toBe('custom:9090');
      expect(updated.workers[0]!.id).toBe(id);
    });
  });

  describe('updatePhysicalSource', () => {
    it('merges partial PhysicalSource updates by id', () => {
      useStore.getState().addPhysicalSource({
        logicalSourceId: 'ls-1',
        hostWorkerId: '',
        type: 'Generator',
        sourceConfig: {},
        parserConfig: {},
        position: { x: 0, y: 0 },
      });
      const { physicalSources } = useStore.getState();
      const id = physicalSources[0]!.id;
      useStore.getState().updatePhysicalSource(id, {
        type: 'CSV',
        sourceConfig: { file_path: '/data.csv' },
      });
      const updated = useStore.getState();
      expect(updated.physicalSources[0]!.type).toBe('CSV');
      expect(updated.physicalSources[0]!.sourceConfig).toEqual({ file_path: '/data.csv' });
      expect(updated.physicalSources[0]!.logicalSourceId).toBe('ls-1');
    });
  });

  describe('updateSink', () => {
    it('merges partial Sink updates by id', () => {
      useStore.getState().addSink({
        name: 'sink-1',
        hostWorkerId: '',
        type: 'File',
        schema: [],
        config: {},
        parserConfig: {},
        position: { x: 0, y: 0 },
      });
      const { sinks } = useStore.getState();
      const id = sinks[0]!.id;
      useStore.getState().updateSink(id, { name: 'renamed', config: { file_path: '/out.csv' } });
      const updated = useStore.getState();
      expect(updated.sinks[0]!.name).toBe('renamed');
      expect(updated.sinks[0]!.config).toEqual({ file_path: '/out.csv' });
      expect(updated.sinks[0]!.type).toBe('File');
    });
  });

  describe('Logical Source CRUD', () => {
    it('addLogicalSource creates with generated id and empty schema', () => {
      useStore.getState().addLogicalSource('MySource');
      const { logicalSources } = useStore.getState();
      expect(logicalSources).toHaveLength(1);
      expect(logicalSources[0]!.name).toBe('MySource');
      expect(logicalSources[0]!.schema).toEqual([]);
      expect(logicalSources[0]!.id).toBeTruthy();
    });

    it('updateLogicalSource merges partial updates by id', () => {
      useStore.getState().addLogicalSource('Source1');
      const { logicalSources } = useStore.getState();
      const id = logicalSources[0]!.id;
      useStore.getState().updateLogicalSource(id, {
        name: 'RenamedSource',
        schema: [{ name: 'field1', type: 'INT32' }],
      });
      const updated = useStore.getState();
      expect(updated.logicalSources[0]!.name).toBe('RenamedSource');
      expect(updated.logicalSources[0]!.schema).toEqual([{ name: 'field1', type: 'INT32' }]);
    });

    it('removeLogicalSource removes by id without cascading to physical sources', () => {
      useStore.getState().addLogicalSource('Source1');
      const { logicalSources } = useStore.getState();
      const lsId = logicalSources[0]!.id;

      // Add a physical source referencing this logical source
      useStore.getState().addPhysicalSource({
        logicalSourceId: lsId,
        hostWorkerId: '',
        type: 'Generator',
        sourceConfig: {},
        parserConfig: {},
        position: { x: 0, y: 0 },
      });

      useStore.getState().removeLogicalSource(lsId);
      const updated = useStore.getState();
      expect(updated.logicalSources).toHaveLength(0);
      // Physical source should remain (orphan policy)
      expect(updated.physicalSources).toHaveLength(1);
      expect(updated.physicalSources[0]!.logicalSourceId).toBe(lsId);
    });
  });
});

describe('UI Slice - Sidebar and Query Selection', () => {
  beforeEach(() => {
    useStore.setState({
      sidebarTab: 'properties',
      selectedQueryId: null,
    });
  });

  it('sidebarTab defaults to properties', () => {
    expect(useStore.getState().sidebarTab).toBe('properties');
  });

  it('setSidebarTab changes the active tab', () => {
    useStore.getState().setSidebarTab('sources');
    expect(useStore.getState().sidebarTab).toBe('sources');
    useStore.getState().setSidebarTab('properties');
    expect(useStore.getState().sidebarTab).toBe('properties');
  });

  it('selectedQueryId defaults to null', () => {
    expect(useStore.getState().selectedQueryId).toBeNull();
  });

  it('selectQuery sets the selected query id', () => {
    useStore.getState().selectQuery('q-1');
    expect(useStore.getState().selectedQueryId).toBe('q-1');
    useStore.getState().selectQuery(null);
    expect(useStore.getState().selectedQueryId).toBeNull();
  });
});
