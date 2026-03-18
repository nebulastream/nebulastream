import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);

describe('nes-validator WASM', async () => {
    let module;

    before(async () => {
        const factory = require('../build/nes-validator/nes-validator-wasm.cjs');
        module = await factory();
    });

    it('exports validateTopology function', () => {
        assert.equal(typeof module.validateTopology, 'function');
    });

    it('returns empty string for valid topology', () => {
        const validYaml = `
query: |
  SELECT * FROM SENSOR INTO VOID_SINK
sinks:
  - name: VOID_SINK
    schema:
      - name: SENSOR$ID
        type: INT64
      - name: SENSOR$VALUE
        type: FLOAT64
      - name: SENSOR$TIMESTAMP
        type: INT64
    type: Void
    config: {}
    parser_config: {}
logical:
  - name: sensor
    schema:
      - name: id
        type: INT64
      - name: value
        type: FLOAT64
      - name: timestamp
        type: INT64
physical:
  - logical: sensor
    type: Generator
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        SEQUENCE INT64 0 100 1
        NORMAL_DISTRIBUTION FLOAT64 0 1
        SEQUENCE INT64 0 100000 1000
`;
        const result = module.validateTopology(validYaml);
        assert.equal(result, '', `Expected empty string for valid topology, got: ${result}`);
    });

    it('returns error for invalid SQL', () => {
        const badSqlYaml = `
query: |
  SELCT * FORM sensor INTO VOID_SINK
sinks:
  - name: VOID_SINK
    schema:
      - name: SENSOR$ID
        type: INT64
    type: Void
    config: {}
    parser_config: {}
logical:
  - name: sensor
    schema:
      - name: id
        type: INT64
physical:
  - logical: sensor
    type: Generator
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        SEQUENCE INT64 0 100 1
`;
        const result = module.validateTopology(badSqlYaml);
        assert.notEqual(result, '', 'Invalid SQL should return non-empty error string');
    });

    it('returns error for missing source', () => {
        const missingSourceYaml = `
query: |
  SELECT * FROM NONEXISTENT_SOURCE INTO VOID_SINK
sinks:
  - name: VOID_SINK
    schema:
      - name: SENSOR$ID
        type: INT64
    type: Void
    config: {}
    parser_config: {}
logical:
  - name: sensor
    schema:
      - name: id
        type: INT64
physical:
  - logical: sensor
    type: Generator
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        SEQUENCE INT64 0 100 1
`;
        const result = module.validateTopology(missingSourceYaml);
        assert.notEqual(result, '', 'Missing source should return non-empty error string');
        assert.ok(result.includes('NONEXISTENT_SOURCE'), `Error should mention NONEXISTENT_SOURCE, got: ${result}`);
    });

    it('returns error for empty input', () => {
        const result = module.validateTopology('');
        assert.notEqual(result, '', 'Empty YAML should return non-empty error string');
    });
});
