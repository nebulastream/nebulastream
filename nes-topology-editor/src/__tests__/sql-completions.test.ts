import { describe, it, expect } from 'vitest';
import {
  detectContext,
  buildInlineSnippet,
  buildTimestampChoice,
  getCompletions,
} from '../lib/sqlCompletions';
import type { LogicalSource, Sink } from '../lib/types';

const SOURCES: LogicalSource[] = [
  { id: 'ls-1', name: 'sensor', schema: [
    { name: 'ts', type: 'UINT64' },
    { name: 'value', type: 'FLOAT64' },
    { name: 'id', type: 'UINT32' },
  ]},
  { id: 'ls-2', name: 'gps', schema: [
    { name: 'lat', type: 'FLOAT64' },
    { name: 'lon', type: 'FLOAT64' },
  ]},
];

const SINKS: Sink[] = [
  { id: 'sk-1', name: 'output_file', hostWorkerId: 'w-1', type: 'File', schema: [], config: {}, parserConfig: {}, position: { x: 0, y: 0 } },
  { id: 'sk-2', name: 'printer', hostWorkerId: 'w-1', type: 'Print', schema: [], config: {}, parserConfig: {}, position: { x: 0, y: 0 } },
  { id: 'sk-3', name: '', hostWorkerId: 'w-1', type: 'Void', schema: [], config: {}, parserConfig: {}, position: { x: 0, y: 0 } },
];

// ---------------------------------------------------------------------------
// detectContext
// ---------------------------------------------------------------------------
describe('detectContext', () => {
  it('detects afterSelect', () => {
    expect(detectContext('SELECT ').context).toBe('afterSelect');
    expect(detectContext('SELECT ts, ').context).toBe('afterSelect');
    expect(detectContext('SELECT v').context).toBe('afterSelect');
  });

  it('does not detect afterSelect once FROM is present', () => {
    expect(detectContext('SELECT ts FROM ').context).not.toBe('afterSelect');
  });

  it('detects afterFrom', () => {
    expect(detectContext('SELECT * FROM ').context).toBe('afterFrom');
    expect(detectContext('SELECT * FROM s').context).toBe('afterFrom');
    expect(detectContext('JOIN ').context).toBe('afterFrom');
  });

  it('detects afterInto', () => {
    expect(detectContext('SELECT * FROM sensor INTO ').context).toBe('afterInto');
    expect(detectContext('INTO P').context).toBe('afterInto');
  });

  it('detects afterWhere', () => {
    expect(detectContext('SELECT * FROM sensor WHERE ').context).toBe('afterWhere');
    expect(detectContext('WHERE x > 1 AND ').context).toBe('afterWhere');
    expect(detectContext('HAVING ').context).toBe('afterWhere');
  });

  it('detects afterDot with prefix', () => {
    const result = detectContext('SELECT sensor.');
    expect(result.context).toBe('afterDot');
    expect(result.dotPrefix).toBe('sensor');
  });

  it('returns default for empty or ambiguous input', () => {
    expect(detectContext('').context).toBe('default');
    expect(detectContext('SE').context).toBe('default');
  });
});

// ---------------------------------------------------------------------------
// buildInlineSnippet
// ---------------------------------------------------------------------------
describe('buildInlineSnippet', () => {
  it('generates HOST as first param with double quotes', () => {
    const result = buildInlineSnippet('SINK', 'Print', [], [
      { key: 'HOST', placeholder: 'worker:8080', quote: true },
    ]);
    expect(result.text).toContain('"${1:worker:8080}" AS `SINK`.`HOST`');
  });

  it('quotes non-numeric fields with single quotes', () => {
    const result = buildInlineSnippet('SINK', 'File', [
      { key: 'file_path', label: 'File Path', type: 'text', required: true, placeholder: '/out.csv' },
      { key: 'append', label: 'Append', type: 'boolean', defaultValue: 'false' },
    ]);
    expect(result.text).toContain("'${1:/out.csv}' AS `SINK`.`FILE_PATH`");
    expect(result.text).toContain("'${2:false}' AS `SINK`.`APPEND`");
  });

  it('does not quote numeric fields', () => {
    const result = buildInlineSnippet('SOURCE', 'Generator', [
      { key: 'seed', label: 'Seed', type: 'number', defaultValue: '1' },
    ]);
    expect(result.text).toContain('${1:1} AS `SOURCE`.`SEED`');
    expect(result.text).not.toContain("'${1:1}'");
  });

  it('returns $0 placeholder for types with no fields', () => {
    const result = buildInlineSnippet('SINK', 'Void', []);
    expect(result.text).toBe('Void($0)');
  });

  it('uses SOURCE prefix for source kind', () => {
    const result = buildInlineSnippet('SOURCE', 'File', [
      { key: 'file_path', label: 'File Path', type: 'text', required: true },
    ]);
    expect(result.text).toContain('`SOURCE`.`FILE_PATH`');
  });
});

// ---------------------------------------------------------------------------
// buildTimestampChoice
// ---------------------------------------------------------------------------
describe('buildTimestampChoice', () => {
  it('builds choice from logical source fields', () => {
    const result = buildTimestampChoice(SOURCES);
    expect(result).toBe('${1|ts,value,id,lat,lon|}');
  });

  it('deduplicates field names', () => {
    const sources: LogicalSource[] = [
      { id: '1', name: 'a', schema: [{ name: 'ts', type: 'UINT64' }] },
      { id: '2', name: 'b', schema: [{ name: 'ts', type: 'UINT64' }, { name: 'val', type: 'FLOAT64' }] },
    ];
    const result = buildTimestampChoice(sources);
    expect(result).toBe('${1|ts,val|}');
  });

  it('falls back to placeholder when no sources', () => {
    expect(buildTimestampChoice([])).toBe('${1:timestamp}');
  });
});

// ---------------------------------------------------------------------------
// getCompletions — context-aware
// ---------------------------------------------------------------------------
describe('getCompletions', () => {
  it('after SELECT: returns fields, *, aggregates, and keywords', () => {
    const results = getCompletions('SELECT ', SOURCES, SINKS);
    const labels = results.map((r) => r.label);

    expect(labels).toContain('ts');
    expect(labels).toContain('value');
    expect(labels).toContain('lat');
    expect(labels).toContain('*');
    expect(labels).toContain('COUNT');
    expect(labels).toContain('SUM');
    // Should also contain keywords
    expect(labels).toContain('FROM');
  });

  it('after SELECT: does not return sinks', () => {
    const results = getCompletions('SELECT ', SOURCES, SINKS);
    const labels = results.map((r) => r.label);
    expect(labels).not.toContain('output_file');
    expect(labels).not.toContain('printer');
  });

  it('after FROM: returns source names and inline source snippets', () => {
    const results = getCompletions('SELECT * FROM ', SOURCES, SINKS);
    const labels = results.map((r) => r.label);

    expect(labels).toContain('sensor');
    expect(labels).toContain('gps');
    expect(labels).toContain('Generator(...)');
    expect(labels).toContain('File(...)');
    expect(labels).toContain('TCP(...)');
    // Should not include keywords or sinks
    expect(labels).not.toContain('SELECT');
    expect(labels).not.toContain('output_file');
  });

  it('after FROM: inline source snippets include HOST', () => {
    const results = getCompletions('SELECT * FROM ', SOURCES, SINKS);
    const genSnippet = results.find((r) => r.label === 'Generator(...)');
    expect(genSnippet).toBeDefined();
    expect(genSnippet!.insertText).toContain('`SOURCE`.`HOST`');
    expect(genSnippet!.isSnippet).toBe(true);
  });

  it('after INTO: returns named sinks and inline sink snippets', () => {
    const results = getCompletions('SELECT * FROM sensor INTO ', SOURCES, SINKS);
    const labels = results.map((r) => r.label);

    expect(labels).toContain('output_file');
    expect(labels).toContain('printer');
    // Unnamed sinks should be excluded
    expect(labels).not.toContain('');
    expect(labels).toContain('File(...)');
    expect(labels).toContain('Print(...)');
    expect(labels).toContain('Void(...)');
    expect(labels).toContain('File()');
    expect(labels).toContain('Void()');
    // Should not include keywords or sources
    expect(labels).not.toContain('SELECT');
    expect(labels).not.toContain('sensor');
  });

  it('after INTO: inline sink snippets include HOST and quote non-numeric values', () => {
    const results = getCompletions('INTO ', SOURCES, SINKS);
    const fileSnippet = results.find((r) => r.label === 'File(...)');
    expect(fileSnippet).toBeDefined();
    expect(fileSnippet!.insertText).toContain('`SINK`.`HOST`');
    expect(fileSnippet!.insertText).toContain('`SINK`.`FILE_PATH`');
    // file_path is text → single-quoted
    expect(fileSnippet!.insertText).toMatch(/'[^']*' AS `SINK`.`FILE_PATH`/);
    // append is boolean → single-quoted
    expect(fileSnippet!.insertText).toMatch(/'[^']*' AS `SINK`.`APPEND`/);
  });

  it('after source.: returns only that source fields', () => {
    const results = getCompletions('SELECT sensor.', SOURCES, SINKS);
    const labels = results.map((r) => r.label);

    expect(labels).toEqual(['ts', 'value', 'id']);
  });

  it('after unknown source.: returns empty', () => {
    const results = getCompletions('SELECT unknown.', SOURCES, SINKS);
    expect(results).toHaveLength(0);
  });

  it('after WHERE: returns fields and keywords but no sinks', () => {
    const results = getCompletions('SELECT * FROM sensor WHERE ', SOURCES, SINKS);
    const labels = results.map((r) => r.label);

    expect(labels).toContain('ts');
    expect(labels).toContain('value');
    expect(labels).toContain('AND');
    expect(labels).not.toContain('output_file');
    expect(labels).not.toContain('*');
  });

  it('default context: includes keywords, sources, fields, sinks, and snippets', () => {
    const results = getCompletions('', SOURCES, SINKS);
    const labels = results.map((r) => r.label);

    expect(labels).toContain('SELECT');
    expect(labels).toContain('sensor');
    expect(labels).toContain('ts');
    expect(labels).toContain('output_file');
    expect(labels).toContain('SELECT ... FROM ... INTO ...');
    expect(labels).toContain('WINDOW TUMBLING');
    expect(labels).toContain('WINDOW SLIDING');
  });

  it('default context: window snippets use source fields for timestamp', () => {
    const results = getCompletions('', SOURCES, SINKS);
    const tumbling = results.find((r) => r.label === 'WINDOW TUMBLING');
    expect(tumbling).toBeDefined();
    expect(tumbling!.insertText).toContain('${1|ts,value,id,lat,lon|}');
  });

  it('default context: window snippets fall back when no sources', () => {
    const results = getCompletions('', [], []);
    const tumbling = results.find((r) => r.label === 'WINDOW TUMBLING');
    expect(tumbling!.insertText).toContain('${1:timestamp}');
  });

  it('window snippet has correct SLIDING syntax', () => {
    const results = getCompletions('', [], []);
    const sliding = results.find((r) => r.label === 'WINDOW SLIDING');
    expect(sliding!.insertText).toContain('SIZE');
    expect(sliding!.insertText).toContain('ADVANCE BY');
    expect(sliding!.insertText).toContain('${3|SEC,MS,MINUTE,HOUR,DAY|}');
  });
});
