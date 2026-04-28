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

  it('rejects sink with empty name', () => {
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
    expect(result).not.toBe('');
  });

  it('accepts sink with valid name and output_format', () => {
    const workers: Worker[] = [
      { id: 'w1', host: 'localhost:9090', data: 'localhost:8080', capacity: 10000, downstream: [], position: { x: 0, y: 0 } },
    ];
    const sinks: Sink[] = [
      {
        id: 's1', name: 'MY_PRINT', hostWorkerId: 'w1', type: 'Print',
        schema: [], config: { output_format: 'CSV' }, parserConfig: {}, position: { x: 0, y: 0 },
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

  it('Print sink with input_format instead of output_format is rejected', () => {
    const sink = makeDefaultSink('Print', worker.id);
    delete sink.config.output_format;
    sink.config.input_format = 'CSV';
    const yaml = storeToYaml([worker], [], [], [sink], []);
    const result = validateTopology(yaml);
    expect(result).toContain('input_format');
  });

  it('File sink with defaults is valid when file_path filled', () => {
    const sink = makeDefaultSink('File', worker.id);
    sink.config.file_path = '/data/output.csv';
    const yaml = storeToYaml([worker], [], [], [sink], []);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('File sink with input_format instead of output_format is rejected', () => {
    const sink = makeDefaultSink('File', worker.id);
    sink.config.file_path = '/data/output.csv';
    delete sink.config.output_format;
    sink.config.input_format = 'CSV';
    const yaml = storeToYaml([worker], [], [], [sink], []);
    const result = validateTopology(yaml);
    expect(result).toContain('input_format');
  });

  it('source without logical source set produces invalid YAML', () => {
    const source = makeDefaultSource('Generator', '', worker.id);
    const yaml = storeToYaml([worker], [logical], [source], [], []);
    const result = validateTopology(yaml);
    expect(result).not.toBe('');
  });
});

describe('WASM validator rejects invalid YAML (CLI parity)', () => {
  it('rejects unknown top-level key "queries"', () => {
    const yaml = `queries:\n  - query: SELECT 1\n    name: ""\nworkers: []\nsinks: []\nlogical: []\nphysical: []`;
    const result = validateTopology(yaml);
    expect(result).toContain("Unknown key 'queries'");
  });

  it('rejects unknown key on a sink node', () => {
    const yaml = [
      'query: []',
      'sinks:',
      '  - name: MY_SINK',
      '    type: Void',
      '    schema: []',
      '    host: localhost:8080',
      '    config: {}',
      '    parser_config: {}',
      '    bogus_key: oops',
      'logical: []',
      'physical: []',
      'workers:',
      '  - host: localhost:8080',
      '    data: localhost:9090',
      '    max_operators: 100',
    ].join('\n');
    const result = validateTopology(yaml);
    expect(result).toContain("Unknown key 'bogus_key'");
  });

  it('rejects unknown key on a physical source node', () => {
    const yaml = [
      'query: []',
      'sinks: []',
      'logical:',
      '  - name: SRC',
      '    schema: []',
      'physical:',
      '  - logical: SRC',
      '    type: File',
      '    host: localhost:8080',
      '    parser_config: {}',
      '    source_config: {}',
      '    extra_field: bad',
      'workers:',
      '  - host: localhost:8080',
      '    data: localhost:9090',
      '    max_operators: 100',
    ].join('\n');
    const result = validateTopology(yaml);
    expect(result).toContain("Unknown key 'extra_field'");
  });

  it('rejects sink missing required parser_config', () => {
    const yaml = [
      'query: []',
      'sinks:',
      '  - name: MY_SINK',
      '    type: Print',
      '    schema: []',
      '    host: localhost:8080',
      '    config:',
      '      input_format: CSV',
      'logical: []',
      'physical: []',
      'workers:',
      '  - host: localhost:8080',
      '    data: localhost:9090',
      '    max_operators: 100',
    ].join('\n');
    const result = validateTopology(yaml);
    expect(result).not.toBe('');
  });

  it('accepts topology with multiple queries', () => {
    const worker: Worker = { id: 'w1', host: 'localhost:9090', data: 'localhost:8080', capacity: 10000, downstream: [], position: { x: 0, y: 0 } };
    const logical: LogicalSource = { id: 'ls1', name: 'sensor', schema: [{ name: 'id', type: 'INT64' }] };
    const source: PhysicalSource = {
      id: 'ps1', logicalSourceId: logical.id, hostWorkerId: worker.id, type: 'Generator',
      sourceConfig: { ...buildDefaults(SOURCE_CONFIGS['Generator'] ?? []), generator_schema: 'SEQUENCE INT64 0 100 1' },
      parserConfig: buildDefaults(PARSER_CONFIG),
      position: { x: 0, y: 0 },
    };
    const sink1: Sink = {
      id: 's1', name: 'SINK_A', hostWorkerId: worker.id, type: 'Void',
      schema: [{ name: 'SENSOR$ID', type: 'INT64' }], config: {}, parserConfig: {}, position: { x: 0, y: 0 },
    };
    const sink2: Sink = {
      id: 's2', name: 'SINK_B', hostWorkerId: worker.id, type: 'Void',
      schema: [{ name: 'SENSOR$ID', type: 'INT64' }], config: {}, parserConfig: {}, position: { x: 0, y: 0 },
    };
    const queries: Query[] = [
      { id: 'q1', name: '', sql: 'SELECT * FROM SENSOR INTO SINK_A' },
      { id: 'q2', name: '', sql: 'SELECT * FROM SENSOR INTO SINK_B' },
    ];
    const yaml = storeToYaml([worker], [logical], [source], [sink1, sink2], queries);
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });
});

describe('WASM validator with raw YAML topologies', () => {
  it('rejects multi-worker topology with Print sinks missing output_format', () => {
    const yaml = [
      'query:',
      '  - SELECT * FROM PULSE INTO PULSE_PRINT',
      '  - SELECT * FROM SPO2_SRC INTO SPO2_PRINT',
      '  - SELECT * FROM BLOOD_PRESSURE INTO BP_PRINT',
      'sinks:',
      '  - name: PULSE_PRINT',
      '    type: Print',
      '    schema:',
      '      - name: PULSE$bpm',
      '        type: INT32',
      '    host: "w1:8080"',
      '    config: {}',
      '    parser_config: {}',
      '  - name: SPO2_PRINT',
      '    type: Print',
      '    schema:',
      '      - name: SPO2_SRC$spo2',
      '        type: INT32',
      '    host: "w2:8080"',
      '    config: {}',
      '    parser_config: {}',
      '  - name: BP_PRINT',
      '    type: Print',
      '    schema:',
      '      - name: BLOOD_PRESSURE$systolic',
      '        type: INT32',
      '    host: "w3:8080"',
      '    config: {}',
      '    parser_config: {}',
      'logical:',
      '  - name: PULSE',
      '    schema:',
      '      - name: bpm',
      '        type: INT32',
      '  - name: SPO2_SRC',
      '    schema:',
      '      - name: spo2',
      '        type: INT32',
      '  - name: BLOOD_PRESSURE',
      '    schema:',
      '      - name: systolic',
      '        type: INT32',
      'physical:',
      '  - logical: PULSE',
      '    type: Generator',
      '    host: "w1:8080"',
      '    parser_config:',
      '      type: CSV',
      '    source_config:',
      '      generator_rate_type: FIXED',
      '      generator_rate_config: emit_rate 10',
      '      seed: 1',
      '      stop_generator_when_sequence_finishes: NONE',
      '      generator_schema: SEQUENCE INT32 60 120 1',
      '  - logical: SPO2_SRC',
      '    type: Generator',
      '    host: "w2:8080"',
      '    parser_config:',
      '      type: CSV',
      '    source_config:',
      '      generator_rate_type: FIXED',
      '      generator_rate_config: emit_rate 10',
      '      seed: 1',
      '      stop_generator_when_sequence_finishes: NONE',
      '      generator_schema: SEQUENCE INT32 90 100 1',
      '  - logical: BLOOD_PRESSURE',
      '    type: Generator',
      '    host: "w3:8080"',
      '    parser_config:',
      '      type: CSV',
      '    source_config:',
      '      generator_rate_type: FIXED',
      '      generator_rate_config: emit_rate 10',
      '      seed: 1',
      '      stop_generator_when_sequence_finishes: NONE',
      '      generator_schema: SEQUENCE INT32 80 140 1',
      'workers:',
      '  - host: "w1:8080"',
      '    data: "w1:9090"',
      '    downstream: []',
      '  - host: "w2:8080"',
      '    data: "w2:9090"',
      '    downstream: []',
      '  - host: "w3:8080"',
      '    data: "w3:9090"',
      '    downstream: []',
    ].join('\n');
    const result = validateTopology(yaml);
    expect(result).toContain('output_format');
  });

  it('accepts multi-worker topology with Print sinks and output_format', () => {
    const yaml = [
      'query:',
      '  - SELECT * FROM PULSE INTO PULSE_PRINT',
      '  - SELECT * FROM SPO2_SRC INTO SPO2_PRINT',
      '  - SELECT * FROM BLOOD_PRESSURE INTO BP_PRINT',
      'sinks:',
      '  - name: PULSE_PRINT',
      '    type: Print',
      '    schema:',
      '      - name: PULSE$bpm',
      '        type: INT32',
      '    host: "w1:8080"',
      '    config:',
      '      output_format: CSV',
      '    parser_config: {}',
      '  - name: SPO2_PRINT',
      '    type: Print',
      '    schema:',
      '      - name: SPO2_SRC$spo2',
      '        type: INT32',
      '    host: "w2:8080"',
      '    config:',
      '      output_format: CSV',
      '    parser_config: {}',
      '  - name: BP_PRINT',
      '    type: Print',
      '    schema:',
      '      - name: BLOOD_PRESSURE$systolic',
      '        type: INT32',
      '    host: "w3:8080"',
      '    config:',
      '      output_format: CSV',
      '    parser_config: {}',
      'logical:',
      '  - name: PULSE',
      '    schema:',
      '      - name: bpm',
      '        type: INT32',
      '  - name: SPO2_SRC',
      '    schema:',
      '      - name: spo2',
      '        type: INT32',
      '  - name: BLOOD_PRESSURE',
      '    schema:',
      '      - name: systolic',
      '        type: INT32',
      'physical:',
      '  - logical: PULSE',
      '    type: Generator',
      '    host: "w1:8080"',
      '    parser_config:',
      '      type: CSV',
      '    source_config:',
      '      generator_rate_type: FIXED',
      '      generator_rate_config: emit_rate 10',
      '      seed: 1',
      '      stop_generator_when_sequence_finishes: NONE',
      '      generator_schema: SEQUENCE INT32 60 120 1',
      '  - logical: SPO2_SRC',
      '    type: Generator',
      '    host: "w2:8080"',
      '    parser_config:',
      '      type: CSV',
      '    source_config:',
      '      generator_rate_type: FIXED',
      '      generator_rate_config: emit_rate 10',
      '      seed: 1',
      '      stop_generator_when_sequence_finishes: NONE',
      '      generator_schema: SEQUENCE INT32 90 100 1',
      '  - logical: BLOOD_PRESSURE',
      '    type: Generator',
      '    host: "w3:8080"',
      '    parser_config:',
      '      type: CSV',
      '    source_config:',
      '      generator_rate_type: FIXED',
      '      generator_rate_config: emit_rate 10',
      '      seed: 1',
      '      stop_generator_when_sequence_finishes: NONE',
      '      generator_schema: SEQUENCE INT32 80 140 1',
      'workers:',
      '  - host: "w1:8080"',
      '    data: "w1:9090"',
      '    downstream: []',
      '  - host: "w2:8080"',
      '    data: "w2:9090"',
      '    downstream: []',
      '  - host: "w3:8080"',
      '    data: "w3:9090"',
      '    downstream: []',
    ].join('\n');
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('rejects topology with invalid parser_config type (ffff)', () => {
    const yaml = [
      'query: []',
      'sinks: []',
      'logical:',
      '  - name: SRC',
      '    schema:',
      '      - name: val',
      '        type: INT32',
      'physical:',
      '  - logical: SRC',
      '    type: TCP',
      '    host: "w1:8080"',
      '    parser_config:',
      '      type: ffff',
      '    source_config:',
      '      socket_host: 127.0.0.1',
      '      socket_port: 9000',
      'workers:',
      '  - host: "w1:8080"',
      '    data: "w1:9090"',
      '    downstream: []',
    ].join('\n');
    const result = validateTopology(yaml);
    expect(result).toContain('Invalid parser type ffff');
  });

  it('rejects Print sink missing required output_format', () => {
    const yaml = [
      'query: []',
      'sinks:',
      '  - name: MY_SINK',
      '    type: Print',
      '    schema: []',
      '    host: "w1:8080"',
      '    config:',
      '      ingestion: "0"',
      '    parser_config: {}',
      'logical: []',
      'physical: []',
      'workers:',
      '  - host: "w1:8080"',
      '    data: "w1:9090"',
      '    downstream: []',
    ].join('\n');
    const result = validateTopology(yaml);
    expect(result).toContain('output_format');
  });

  it('rejects File sink missing required file_path', () => {
    const yaml = [
      'query: []',
      'sinks:',
      '  - name: MY_SINK',
      '    type: File',
      '    schema: []',
      '    host: "w1:8080"',
      '    config:',
      '      output_format: CSV',
      '    parser_config: {}',
      'logical: []',
      'physical: []',
      'workers:',
      '  - host: "w1:8080"',
      '    data: "w1:9090"',
      '    downstream: []',
    ].join('\n');
    const result = validateTopology(yaml);
    expect(result).toContain('file_path');
  });

  it('rejects query when source and sink are on disconnected workers', () => {
    const yaml = [
      'query:',
      '  - SELECT * FROM SRC INTO MY_SINK',
      'sinks:',
      '  - name: MY_SINK',
      '    type: Print',
      '    schema:',
      '      - name: SRC$val',
      '        type: INT32',
      '    host: "w2:8080"',
      '    config:',
      '      output_format: CSV',
      '    parser_config: {}',
      'logical:',
      '  - name: SRC',
      '    schema:',
      '      - name: val',
      '        type: INT32',
      'physical:',
      '  - logical: SRC',
      '    type: TCP',
      '    host: "w1:8080"',
      '    parser_config:',
      '      type: CSV',
      '    source_config:',
      '      socket_host: 127.0.0.1',
      '      socket_port: 9000',
      'workers:',
      '  - host: "w1:8080"',
      '    data: "w1:9090"',
      '    downstream: []',
      '  - host: "w2:8080"',
      '    data: "w2:9090"',
      '    downstream: []',
    ].join('\n');
    const result = validateTopology(yaml);
    expect(result).toContain('placement failure');
  });

  it('accepts query when source and sink are on connected workers', () => {
    const yaml = [
      'query:',
      '  - SELECT * FROM SRC INTO MY_SINK',
      'sinks:',
      '  - name: MY_SINK',
      '    type: Print',
      '    schema:',
      '      - name: SRC$val',
      '        type: INT32',
      '    host: "w2:8080"',
      '    config:',
      '      output_format: CSV',
      '    parser_config: {}',
      'logical:',
      '  - name: SRC',
      '    schema:',
      '      - name: val',
      '        type: INT32',
      'physical:',
      '  - logical: SRC',
      '    type: TCP',
      '    host: "w1:8080"',
      '    parser_config:',
      '      type: CSV',
      '    source_config:',
      '      socket_host: 127.0.0.1',
      '      socket_port: 9000',
      'workers:',
      '  - host: "w1:8080"',
      '    data: "w1:9090"',
      '    downstream:',
      '      - "w2:8080"',
      '  - host: "w2:8080"',
      '    data: "w2:9090"',
      '    downstream: []',
    ].join('\n');
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('accepts query when source and sink are on the same worker', () => {
    const yaml = [
      'query:',
      '  - SELECT * FROM SRC INTO MY_SINK',
      'sinks:',
      '  - name: MY_SINK',
      '    type: Print',
      '    schema:',
      '      - name: SRC$val',
      '        type: INT32',
      '    host: "w1:8080"',
      '    config:',
      '      output_format: CSV',
      '    parser_config: {}',
      'logical:',
      '  - name: SRC',
      '    schema:',
      '      - name: val',
      '        type: INT32',
      'physical:',
      '  - logical: SRC',
      '    type: TCP',
      '    host: "w1:8080"',
      '    parser_config:',
      '      type: CSV',
      '    source_config:',
      '      socket_host: 127.0.0.1',
      '      socket_port: 9000',
      'workers:',
      '  - host: "w1:8080"',
      '    data: "w1:9090"',
      '    downstream: []',
    ].join('\n');
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });

  it('accepts File sink with any output_format (validated at runtime, not config time)', () => {
    // output_format is a free-form string in the C++ ConfigParameter — validation
    // happens at runtime when the output formatter is instantiated, not at config time.
    // This matches nes-cli behavior.
    const yaml = [
      'query: []',
      'sinks:',
      '  - name: MY_SINK',
      '    type: File',
      '    schema: []',
      '    host: "w1:8080"',
      '    config:',
      '      file_path: /tmp/out.csv',
      '      append: "false"',
      '      add_timestamp: "false"',
      '      output_format: f',
      '    parser_config: {}',
      'logical: []',
      'physical: []',
      'workers:',
      '  - host: "w1:8080"',
      '    data: "w1:9090"',
      '    downstream: []',
    ].join('\n');
    const result = validateTopology(yaml);
    expect(result).toBe('');
  });
});
