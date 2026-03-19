import { describe, it, expect } from 'vitest';
import { validateTopology } from '../lib/validation';
import type { Worker, LogicalSource, PhysicalSource, Sink } from '../lib/types';

/** Helper to create a minimal valid worker */
function makeWorker(overrides: Partial<Worker> = {}): Worker {
  return {
    id: 'w1',
    host: 'localhost',
    data: '4000',
    capacity: 4,
    downstream: [],
    position: { x: 0, y: 0 },
    ...overrides,
  };
}

/** Helper to create a minimal valid logical source */
function makeLogicalSource(overrides: Partial<LogicalSource> = {}): LogicalSource {
  return {
    id: 'ls1',
    name: 'sensor',
    schema: [{ name: 'id', type: 'UINT64' }],
    ...overrides,
  };
}

/** Helper to create a minimal valid physical source */
function makePhysicalSource(overrides: Partial<PhysicalSource> = {}): PhysicalSource {
  return {
    id: 'ps1',
    logicalSourceId: 'ls1',
    hostWorkerId: 'w1',
    type: 'Generator',
    sourceConfig: {
      generator_schema: 'SEQUENCE UINT64 0 100 1',
      stop_generator_when_sequence_finishes: 'ALL',
    },
    parserConfig: {},
    position: { x: 0, y: 0 },
    ...overrides,
  };
}

/** Helper to create a minimal valid sink */
function makeSink(overrides: Partial<Sink> = {}): Sink {
  return {
    id: 'sk1',
    name: 'output',
    hostWorkerId: 'w1',
    type: 'File',
    schema: [],
    config: { file_path: '/tmp/out.csv' },
    parserConfig: {},
    position: { x: 0, y: 0 },
    ...overrides,
  };
}

describe('validateTopology', () => {
  it('Test 1: orphan physical source (hostWorkerId = "") returns warning', () => {
    const workers = [makeWorker()];
    const logicalSources = [makeLogicalSource()];
    const physicalSources = [makePhysicalSource({ hostWorkerId: '' })];
    const sinks: Sink[] = [];

    const errors = validateTopology(workers, logicalSources, physicalSources, sinks);
    const orphanWarning = errors.find(
      (e) => e.nodeId === 'ps1' && e.severity === 'warning' && /orphan|not assigned|no worker/i.test(e.message),
    );
    expect(orphanWarning).toBeDefined();
  });

  it('Test 2: orphan sink (hostWorkerId = "") returns warning', () => {
    const workers = [makeWorker()];
    const logicalSources: LogicalSource[] = [];
    const physicalSources: PhysicalSource[] = [];
    const sinks = [makeSink({ hostWorkerId: '' })];

    const errors = validateTopology(workers, logicalSources, physicalSources, sinks);
    const orphanWarning = errors.find(
      (e) => e.nodeId === 'sk1' && e.severity === 'warning' && /orphan|not assigned|no worker/i.test(e.message),
    );
    expect(orphanWarning).toBeDefined();
  });

  it('Test 3: physical source with empty logicalSourceId returns error', () => {
    const workers = [makeWorker()];
    const logicalSources = [makeLogicalSource()];
    const physicalSources = [makePhysicalSource({ logicalSourceId: '' })];
    const sinks: Sink[] = [];

    const errors = validateTopology(workers, logicalSources, physicalSources, sinks);
    const noLogical = errors.find(
      (e) => e.nodeId === 'ps1' && e.severity === 'error' && /logical source/i.test(e.message),
    );
    expect(noLogical).toBeDefined();
  });

  it('Test 4: physical source referencing non-existent logical source returns error', () => {
    const workers = [makeWorker()];
    const logicalSources = [makeLogicalSource({ id: 'ls-other' })];
    const physicalSources = [makePhysicalSource({ logicalSourceId: 'ls-missing' })];
    const sinks: Sink[] = [];

    const errors = validateTopology(workers, logicalSources, physicalSources, sinks);
    const missing = errors.find(
      (e) => e.nodeId === 'ps1' && e.severity === 'error' && /logical source/i.test(e.message),
    );
    expect(missing).toBeDefined();
  });

  it('Test 5: logical source with empty schema causes warning on referencing physical sources', () => {
    const workers = [makeWorker()];
    const logicalSources = [makeLogicalSource({ schema: [] })];
    const physicalSources = [makePhysicalSource()];
    const sinks: Sink[] = [];

    const errors = validateTopology(workers, logicalSources, physicalSources, sinks);
    const emptySchema = errors.find(
      (e) => e.nodeId === 'ps1' && e.severity === 'warning' && /schema/i.test(e.message),
    );
    expect(emptySchema).toBeDefined();
  });

  it('Test 6: physical source missing required config field returns warning', () => {
    const workers = [makeWorker()];
    const logicalSources = [makeLogicalSource()];
    // Generator requires 'generator_schema' and 'stop_generator_when_sequence_finishes'
    const physicalSources = [makePhysicalSource({ sourceConfig: {} })];
    const sinks: Sink[] = [];

    const errors = validateTopology(workers, logicalSources, physicalSources, sinks);
    const missingConfig = errors.find(
      (e) => e.nodeId === 'ps1' && e.severity === 'warning' && /required.*config|config.*missing/i.test(e.message),
    );
    expect(missingConfig).toBeDefined();
  });

  it('Test 7: sink missing required config field returns warning', () => {
    const workers = [makeWorker()];
    const logicalSources: LogicalSource[] = [];
    const physicalSources: PhysicalSource[] = [];
    // File sink requires 'file_path'
    const sinks = [makeSink({ config: {} })];

    const errors = validateTopology(workers, logicalSources, physicalSources, sinks);
    const missingConfig = errors.find(
      (e) => e.nodeId === 'sk1' && e.severity === 'warning' && /required.*config|config.*missing/i.test(e.message),
    );
    expect(missingConfig).toBeDefined();
  });

  it('Test 8: fully valid topology returns empty array', () => {
    const workers = [makeWorker()];
    const logicalSources = [makeLogicalSource()];
    const physicalSources = [makePhysicalSource()];
    const sinks = [makeSink()];

    const errors = validateTopology(workers, logicalSources, physicalSources, sinks);
    expect(errors).toEqual([]);
  });
});
