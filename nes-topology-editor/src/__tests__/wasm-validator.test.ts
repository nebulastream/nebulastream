/**
 * Integration tests using the actual WASM validator module.
 * Tests that storeToYaml() output is accepted by the NES validator.
 */
import { describe, it, expect, beforeAll, vi, beforeEach } from 'vitest';

// Mock generateId for deterministic UUIDs
let idCounter = 0;
vi.mock('../lib/ids', () => ({
  generateId: () => `uuid-${++idCounter}`,
}));

import { storeToYaml } from '../lib/yaml';
import { SOURCE_CONFIGS, SINK_CONFIGS, PARSER_CONFIG, buildDefaults } from '../lib/sourceConfigs';
import type {
  Worker,
  LogicalSource,
  PhysicalSource,
  Sink,
  Query,
} from '../lib/types';

// Load the WASM module once for all tests
let validateTopology: (yaml: string) => string;

beforeAll(async () => {
  const mod = await import('../../wasm/build/nes-validator/nes-validator-wasm.mjs');
  const module = await mod.default();
  validateTopology = module.validateTopology;
}, 30_000);

beforeEach(() => {
  idCounter = 0;
});

describe('WASM validator with storeToYaml output', () => {
  it('accepts empty topology (no workers)', () => {
    const yaml = storeToYaml([], [], [], [], []);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('accepts topology with only a worker', () => {
    const workers: Worker[] = [
      { id: 'w1', host: 'localhost:9090', data: 'localhost:8080', capacity: 10000, downstream: [], position: { x: 0, y: 0 } },
    ];
    const yaml = storeToYaml(workers, [], [], [], []);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('accepts topology without workers even with logical sources', () => {
    const logical: LogicalSource[] = [
      { id: 'ls1', name: 'sensor', schema: [{ name: 'id', type: 'INT64' }, { name: 'value', type: 'FLOAT64' }] },
    ];
    const yaml = storeToYaml([], logical, [], [], []);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('accepts full valid topology with query', () => {
    const workers: Worker[] = [
      { id: 'w1', host: 'localhost:9090', data: 'localhost:8080', capacity: 10000, downstream: [], position: { x: 0, y: 0 } },
    ];
    const logical: LogicalSource[] = [
      { id: 'ls1', name: 'sensor', schema: [{ name: 'id', type: 'INT64' }, { name: 'value', type: 'FLOAT64' }] },
    ];
    const physical: PhysicalSource[] = [
      {
        id: 'ps1', logicalSourceId: 'ls1', hostWorkerId: 'w1', type: 'Generator',
        sourceConfig: { generator_rate_type: 'FIXED', generator_rate_config: 'emit_rate 10', seed: '1', stop_generator_when_sequence_finishes: 'NONE', generator_schema: 'SEQUENCE INT64 0 100 1\nNORMAL_DISTRIBUTION FLOAT64 0 1' },
        parserConfig: { type: 'CSV' },
        position: { x: 0, y: 0 },
      },
    ];
    const sinks: Sink[] = [
      {
        id: 's1', name: 'VOID_SINK', hostWorkerId: 'w1', type: 'Void',
        schema: [{ name: 'SENSOR$ID', type: 'INT64' }, { name: 'SENSOR$VALUE', type: 'FLOAT64' }],
        config: {}, parserConfig: {}, position: { x: 0, y: 0 },
      },
    ];
    const queries: Query[] = [
      { id: 'q1', name: 'myQuery', sql: 'SELECT * FROM SENSOR INTO VOID_SINK' },
    ];
    const yaml = storeToYaml(workers, logical, physical, sinks, queries);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('rejects invalid SQL query', () => {
    const logical: LogicalSource[] = [
      { id: 'ls1', name: 'sensor', schema: [{ name: 'id', type: 'INT64' }] },
    ];
    const queries: Query[] = [
      { id: 'q1', name: 'bad', sql: 'SELCT * FORM sensor' },
    ];
    const yaml = storeToYaml([], logical, [], [], queries);
    const result = validateTopology(yaml);
    expect(result).not.toBe('');
  });

  it('rejects query referencing nonexistent source', () => {
    const workers: Worker[] = [
      { id: 'w1', host: 'localhost:9090', data: 'localhost:8080', capacity: 10000, downstream: [], position: { x: 0, y: 0 } },
    ];
    const sinks: Sink[] = [
      {
        id: 's1', name: 'MY_SINK', hostWorkerId: 'w1', type: 'Void',
        schema: [{ name: 'id', type: 'INT64' }],
        config: {}, parserConfig: {}, position: { x: 0, y: 0 },
      },
    ];
    const queries: Query[] = [
      { id: 'q1', name: 'bad', sql: 'SELECT * FROM NONEXISTENT INTO MY_SINK' },
    ];
    const yaml = storeToYaml(workers, [], [], sinks, queries);
    const result = validateTopology(yaml);
    expect(result).not.toBe('');
    expect(result).toContain('NONEXISTENT');
  });

  it('accepts sink with empty name (validator does not reject it)', () => {
    const workers: Worker[] = [
      { id: 'w1', host: 'localhost:9090', data: 'localhost:8080', capacity: 10000, downstream: [], position: { x: 0, y: 0 } },
    ];
    const sinks: Sink[] = [
      {
        id: 's1', name: '', hostWorkerId: 'w1', type: 'Print',
        schema: [], config: {}, parserConfig: {}, position: { x: 0, y: 0 },
      },
    ];
    const yaml = storeToYaml(workers, [], [], sinks, []);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('accepts topology with sink and source but no query', () => {
    const workers: Worker[] = [
      { id: 'w1', host: 'localhost:9090', data: 'localhost:8080', capacity: 10000, downstream: [], position: { x: 0, y: 0 } },
    ];
    const logical: LogicalSource[] = [
      { id: 'ls1', name: 'sensor', schema: [{ name: 'id', type: 'INT64' }] },
    ];
    const sinks: Sink[] = [
      {
        id: 's1', name: 'VOID_SINK', hostWorkerId: 'w1', type: 'Void',
        schema: [{ name: 'SENSOR$ID', type: 'INT64' }],
        config: {}, parserConfig: {}, position: { x: 0, y: 0 },
      },
    ];
    const yaml = storeToYaml(workers, logical, [], sinks, []);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });
});

describe('default source/sink configs produce valid YAML', () => {
  // Helper: simulates what WorkerPanel does when adding a source
  function makeDefaultSource(type: string, logicalSourceId: string, hostWorkerId: string): PhysicalSource {
    return {
      id: 'ps1',
      logicalSourceId,
      hostWorkerId,
      type,
      sourceConfig: buildDefaults(SOURCE_CONFIGS[type] ?? []),
      parserConfig: buildDefaults(PARSER_CONFIG),
      position: { x: 0, y: 0 },
    };
  }

  function makeDefaultSink(type: string, hostWorkerId: string): Sink {
    return {
      id: 's1',
      name: 'MY_SINK',
      hostWorkerId,
      type,
      schema: [{ name: 'id', type: 'INT64' }],
      config: buildDefaults(SINK_CONFIGS[type] ?? []),
      parserConfig: {},
      position: { x: 0, y: 0 },
    };
  }

  const worker: Worker = { id: 'w1', host: 'localhost:9090', data: 'localhost:8080', capacity: 10000, downstream: [], position: { x: 0, y: 0 } };
  const logical: LogicalSource = { id: 'ls1', name: 'sensor', schema: [{ name: 'id', type: 'INT64' }] };

  it('Generator source with defaults is valid when logical source is set', () => {
    const source = makeDefaultSource('Generator', logical.id, worker.id);
    source.sourceConfig.generator_schema = 'SEQUENCE INT64 0 100 1';
    const yaml = storeToYaml([worker], [logical], [source], [], []);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('File source with defaults is valid when logical source is set and file_path filled', () => {
    const source = makeDefaultSource('File', logical.id, worker.id);
    source.sourceConfig.file_path = '/data/input.csv';
    const yaml = storeToYaml([worker], [logical], [source], [], []);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('TCP source with defaults is valid when logical source is set and required fields filled', () => {
    const source = makeDefaultSource('TCP', logical.id, worker.id);
    source.sourceConfig.socket_host = '127.0.0.1';
    source.sourceConfig.socket_port = '5000';
    const yaml = storeToYaml([worker], [logical], [source], [], []);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('Void sink with defaults is valid', () => {
    const sink = makeDefaultSink('Void', worker.id);
    const yaml = storeToYaml([worker], [], [], [sink], []);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('Print sink with defaults is valid', () => {
    const sink = makeDefaultSink('Print', worker.id);
    const yaml = storeToYaml([worker], [], [], [sink], []);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('File sink with defaults is valid when file_path filled', () => {
    const sink = makeDefaultSink('File', worker.id);
    sink.config.file_path = '/data/output.csv';
    const yaml = storeToYaml([worker], [], [], [sink], []);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('source without logical source set produces invalid YAML', () => {
    const source = makeDefaultSource('Generator', '', worker.id);
    const yaml = storeToYaml([worker], [logical], [source], [], []);
    const result = validateTopology(yaml);
    expect(result).not.toBe('');
  });
});
