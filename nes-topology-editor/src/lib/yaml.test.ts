import { describe, it, expect, vi, beforeEach } from 'vitest';

// Mock generateId to produce deterministic UUIDs for testing
let idCounter = 0;
vi.mock('./ids', () => ({
  generateId: () => `uuid-${++idCounter}`,
}));

import { storeToYaml, yamlToStore } from './yaml';
import type {
  Worker,
  LogicalSource,
  PhysicalSource,
  Sink,
  Query,
} from './types';

beforeEach(() => {
  idCounter = 0;
});

describe('storeToYaml', () => {
  it('emits all sections even when empty', () => {
    const result = storeToYaml([], [], [], [], []);
    expect(result).toContain('query: []');
    expect(result).toContain('sinks: []');
    expect(result).toContain('logical: []');
    expect(result).toContain('physical: []');
    expect(result).toContain('workers: []');
  });

  it('serializes a single worker', () => {
    const workers: Worker[] = [
      {
        id: 'w1',
        host: 'worker-1:9090',
        data: 'worker-1:8080',
        capacity: 10000,
        downstream: [],
        position: { x: 0, y: 0 },
      },
    ];
    const result = storeToYaml(workers, [], [], [], []);
    expect(result).toContain('workers:');
    expect(result).toContain('host: worker-1:9090');
    expect(result).toContain('data: worker-1:8080');
    expect(result).toContain('max_operators: 10000');
    expect(result).not.toContain('downstream');
  });

  it('serializes worker with downstream (UUID resolved to host)', () => {
    const workers: Worker[] = [
      {
        id: 'w1',
        host: 'worker-1:9090',
        data: 'worker-1:8080',
        capacity: 10000,
        downstream: ['w2'],
        position: { x: 0, y: 0 },
      },
      {
        id: 'w2',
        host: 'worker-2:9090',
        data: 'worker-2:8080',
        capacity: 5000,
        downstream: [],
        position: { x: 0, y: 0 },
      },
    ];
    const result = storeToYaml(workers, [], [], [], []);
    expect(result).toContain('downstream:');
    expect(result).toContain('worker-2:9090');
  });

  it('serializes queries as query: string[]', () => {
    const queries: Query[] = [
      { id: 'q1', name: 'myQuery', sql: 'SELECT * FROM src' },
    ];
    const result = storeToYaml([], [], [], [], queries);
    expect(result).toContain('query:');
    expect(result).toContain('- SELECT * FROM src');
    // Should NOT use the queries: [{query, name}] format
    expect(result).not.toContain('queries:');
  });

  it('serializes multiple queries as query: string[]', () => {
    const queries: Query[] = [
      { id: 'q1', name: '', sql: 'SELECT * FROM src1 INTO sink1' },
      { id: 'q2', name: '', sql: 'SELECT * FROM src2 INTO sink2' },
    ];
    const result = storeToYaml([], [], [], [], queries);
    expect(result).toContain('- SELECT * FROM src1 INTO sink1');
    expect(result).toContain('- SELECT * FROM src2 INTO sink2');
  });

  it('serializes sinks with host UUID resolved and all required fields', () => {
    const workers: Worker[] = [
      {
        id: 'w1',
        host: 'worker-1:9090',
        data: 'worker-1:8080',
        capacity: 10000,
        downstream: [],
        position: { x: 0, y: 0 },
      },
    ];
    const sinks: Sink[] = [
      {
        id: 's1',
        name: 'VOID_SINK',
        hostWorkerId: 'w1',
        type: 'Void',
        schema: [{ name: 'field1', type: 'FLOAT64' }],
        config: {},
        parserConfig: {},
        position: { x: 0, y: 0 },
      },
    ];
    const result = storeToYaml(workers, [], [], sinks, []);
    expect(result).toContain('sinks:');
    expect(result).toContain('name: VOID_SINK');
    expect(result).toContain('host: worker-1:9090');
    expect(result).toContain('type: Void');
    // CLI requires config and parser_config
    expect(result).toContain('config: {}');
    expect(result).toContain('parser_config: {}');
  });

  it('serializes logical sources', () => {
    const logical: LogicalSource[] = [
      {
        id: 'ls1',
        name: 'GENERATOR_SOURCE',
        schema: [{ name: 'value', type: 'FLOAT64' }],
      },
    ];
    const result = storeToYaml([], logical, [], [], []);
    expect(result).toContain('logical:');
    expect(result).toContain('name: GENERATOR_SOURCE');
  });

  it('serializes physical sources with UUID resolution and all required fields', () => {
    const workers: Worker[] = [
      {
        id: 'w1',
        host: 'worker-1:9090',
        data: 'worker-1:8080',
        capacity: 10000,
        downstream: [],
        position: { x: 0, y: 0 },
      },
    ];
    const logical: LogicalSource[] = [
      { id: 'ls1', name: 'GENERATOR_SOURCE', schema: [] },
    ];
    const physical: PhysicalSource[] = [
      {
        id: 'ps1',
        logicalSourceId: 'ls1',
        hostWorkerId: 'w1',
        type: 'Generator',
        sourceConfig: { generator_rate_type: 'FIXED' },
        parserConfig: { type: 'CSV' },
        position: { x: 0, y: 0 },
      },
    ];
    const result = storeToYaml(workers, logical, physical, [], []);
    expect(result).toContain('physical:');
    expect(result).toContain('logical: GENERATOR_SOURCE');
    expect(result).toContain('host: worker-1:9090');
    expect(result).toContain('type: Generator');
    // CLI requires parser_config and source_config
    expect(result).toContain('parser_config:');
    expect(result).toContain('source_config:');
  });

  it('emits empty arrays for missing sections alongside populated ones', () => {
    const workers: Worker[] = [
      {
        id: 'w1',
        host: 'worker-1:9090',
        data: 'worker-1:8080',
        capacity: 10000,
        downstream: [],
        position: { x: 0, y: 0 },
      },
    ];
    const result = storeToYaml(workers, [], [], [], []);
    expect(result).toContain('query: []');
    expect(result).toContain('sinks: []');
    expect(result).toContain('logical: []');
    expect(result).toContain('physical: []');
    expect(result).toContain('workers:');
    expect(result).toContain('host: worker-1:9090');
  });

  it('follows key ordering: query, sinks, logical, physical, workers', () => {
    const workers: Worker[] = [
      {
        id: 'w1',
        host: 'worker-1:9090',
        data: 'worker-1:8080',
        capacity: 10000,
        downstream: [],
        position: { x: 0, y: 0 },
      },
    ];
    const logical: LogicalSource[] = [
      { id: 'ls1', name: 'SRC', schema: [] },
    ];
    const queries: Query[] = [
      { id: 'q1', name: 'q', sql: 'SELECT 1' },
    ];
    const result = storeToYaml(workers, logical, [], [], queries);
    const queryIdx = result.indexOf('query:');
    const logicalIdx = result.indexOf('logical:');
    const workersIdx = result.indexOf('workers:');
    expect(queryIdx).toBeLessThan(logicalIdx);
    expect(logicalIdx).toBeLessThan(workersIdx);
  });
});

describe('yamlToStore', () => {
  it('parses workers and creates UUIDs', () => {
    const yaml = `
workers:
  - host: worker-1:9090
    data: worker-1:8080
    max_operators: 10000
`;
    const result = yamlToStore(yaml);
    expect(result.workers).toHaveLength(1);
    expect(result.workers[0]!.host).toBe('worker-1:9090');
    expect(result.workers[0]!.data).toBe('worker-1:8080');
    expect(result.workers[0]!.capacity).toBe(10000);
    expect(result.workers[0]!.id).toBe('uuid-1');
    expect(result.workers[0]!.position).toEqual({ x: 0, y: 0 });
  });

  it('resolves downstream host references to UUIDs', () => {
    const yaml = `
workers:
  - host: worker-1:9090
    data: worker-1:8080
    max_operators: 10000
    downstream:
      - worker-2:9090
  - host: worker-2:9090
    data: worker-2:8080
    max_operators: 5000
`;
    const result = yamlToStore(yaml);
    expect(result.workers[0]!.downstream).toEqual([result.workers[1]!.id]);
  });

  it('parses logical sources with name-to-UUID map', () => {
    const yaml = `
logical:
  - name: GENERATOR_SOURCE
    schema:
      - name: value
        type: FLOAT64
`;
    const result = yamlToStore(yaml);
    expect(result.logicalSources).toHaveLength(1);
    expect(result.logicalSources[0]!.name).toBe('GENERATOR_SOURCE');
    expect(result.logicalSources[0]!.schema).toEqual([
      { name: 'value', type: 'FLOAT64' },
    ]);
  });

  it('parses physical sources with UUID resolution', () => {
    const yaml = `
workers:
  - host: worker-1:9090
    data: worker-1:8080
    max_operators: 10000
logical:
  - name: GENERATOR_SOURCE
    schema: []
physical:
  - logical: GENERATOR_SOURCE
    host: worker-1:9090
    type: Generator
    source_config:
      generator_rate_type: FIXED
    parser_config:
      type: CSV
`;
    const result = yamlToStore(yaml);
    expect(result.physicalSources).toHaveLength(1);
    const ps = result.physicalSources[0]!;
    expect(ps.logicalSourceId).toBe(result.logicalSources[0]!.id);
    expect(ps.hostWorkerId).toBe(result.workers[0]!.id);
    expect(ps.type).toBe('Generator');
    expect(ps.sourceConfig).toEqual({ generator_rate_type: 'FIXED' });
    expect(ps.parserConfig).toEqual({ type: 'CSV' });
  });

  it('parses sinks with host UUID resolution', () => {
    const yaml = `
workers:
  - host: worker-1:9090
    data: worker-1:8080
    max_operators: 10000
sinks:
  - name: VOID_SINK
    host: worker-1:9090
    schema:
      - name: field1
        type: FLOAT64
    type: Void
    config: {}
    parser_config: {}
`;
    const result = yamlToStore(yaml);
    expect(result.sinks).toHaveLength(1);
    expect(result.sinks[0]!.name).toBe('VOID_SINK');
    expect(result.sinks[0]!.hostWorkerId).toBe(result.workers[0]!.id);
    expect(result.sinks[0]!.type).toBe('Void');
    expect(result.sinks[0]!.parserConfig).toEqual({});
  });

  it('parses query: string', () => {
    const yaml = `
query: SELECT * FROM src
`;
    const result = yamlToStore(yaml);
    expect(result.queries).toHaveLength(1);
    expect(result.queries[0]!.sql).toBe('SELECT * FROM src');
    expect(result.queries[0]!.name).toBe('');
  });

  it('parses query: string[]', () => {
    const yaml = `
query:
  - SELECT * FROM src1
  - SELECT * FROM src2
`;
    const result = yamlToStore(yaml);
    expect(result.queries).toHaveLength(2);
    expect(result.queries[0]!.sql).toBe('SELECT * FROM src1');
    expect(result.queries[1]!.sql).toBe('SELECT * FROM src2');
  });

  it('throws YAMLException on invalid YAML', () => {
    const invalid = `
workers:
  - host: worker-1:9090
    bad_indent: [
`;
    expect(() => yamlToStore(invalid)).toThrow();
  });

  it('handles empty YAML document', () => {
    const result = yamlToStore('');
    expect(result.workers).toEqual([]);
    expect(result.logicalSources).toEqual([]);
    expect(result.physicalSources).toEqual([]);
    expect(result.sinks).toEqual([]);
    expect(result.queries).toEqual([]);
  });
});

describe('roundtrip', () => {
  it('yamlToStore(storeToYaml(topology)) preserves structure', () => {
    const workers: Worker[] = [
      {
        id: 'w1',
        host: 'worker-1:9090',
        data: 'worker-1:8080',
        capacity: 10000,
        downstream: ['w2'],
        position: { x: 100, y: 200 },
      },
      {
        id: 'w2',
        host: 'worker-2:9090',
        data: 'worker-2:8080',
        capacity: 5000,
        downstream: [],
        position: { x: 300, y: 200 },
      },
    ];
    const logical: LogicalSource[] = [
      {
        id: 'ls1',
        name: 'GENERATOR_SOURCE',
        schema: [{ name: 'value', type: 'FLOAT64' }],
      },
    ];
    const physical: PhysicalSource[] = [
      {
        id: 'ps1',
        logicalSourceId: 'ls1',
        hostWorkerId: 'w1',
        type: 'Generator',
        sourceConfig: { generator_rate_type: 'FIXED' },
        parserConfig: { type: 'CSV' },
        position: { x: 50, y: 100 },
      },
    ];
    const sinks: Sink[] = [
      {
        id: 's1',
        name: 'VOID_SINK',
        hostWorkerId: 'w1',
        type: 'Void',
        schema: [{ name: 'src$field', type: 'FLOAT64' }],
        config: {},
        parserConfig: {},
        position: { x: 50, y: 300 },
      },
    ];
    const queries: Query[] = [
      { id: 'q1', name: '', sql: 'SELECT * FROM GENERATOR_SOURCE' },
    ];

    const yamlStr = storeToYaml(workers, logical, physical, sinks, queries);
    const result = yamlToStore(yamlStr);

    // Structure matches (UUIDs differ)
    expect(result.workers).toHaveLength(2);
    expect(result.workers[0]!.host).toBe('worker-1:9090');
    expect(result.workers[0]!.downstream).toEqual([result.workers[1]!.id]);

    expect(result.logicalSources).toHaveLength(1);
    expect(result.logicalSources[0]!.name).toBe('GENERATOR_SOURCE');

    expect(result.physicalSources).toHaveLength(1);
    expect(result.physicalSources[0]!.logicalSourceId).toBe(
      result.logicalSources[0]!.id,
    );
    expect(result.physicalSources[0]!.hostWorkerId).toBe(
      result.workers[0]!.id,
    );

    expect(result.sinks).toHaveLength(1);
    expect(result.sinks[0]!.name).toBe('VOID_SINK');
    expect(result.sinks[0]!.hostWorkerId).toBe(result.workers[0]!.id);

    expect(result.queries).toHaveLength(1);
    expect(result.queries[0]!.sql).toBe(
      'SELECT * FROM GENERATOR_SOURCE',
    );
    // query: format doesn't preserve names
    expect(result.queries[0]!.name).toBe('');
  });
});
