import yaml from 'js-yaml';
import { generateId } from './ids';
import type {
  Worker,
  LogicalSource,
  PhysicalSource,
  Sink,
  Query,
} from './types';
import { SOURCE_CONFIGS, SINK_CONFIGS, PARSER_CONFIG } from './sourceConfigs';
import type { FormFieldDef } from './sourceConfigs';

/** Build key->defaultValue map from field definitions. */
function defaultsMap(fields: FormFieldDef[]): Map<string, string> {
  const m = new Map<string, string>();
  for (const f of fields) {
    if (f.defaultValue !== undefined) m.set(f.key, f.defaultValue);
  }
  return m;
}

/** Build set of required field keys that must never be stripped. */
function requiredKeys(fields: FormFieldDef[]): Set<string> {
  const s = new Set<string>();
  for (const f of fields) {
    if (f.required) s.add(f.key);
  }
  return s;
}

/** Strip entries whose value matches the field default, except required fields. */
function stripDefaults(
  config: Record<string, string>,
  defaults: Map<string, string>,
  required: Set<string> = new Set(),
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(config)) {
    if (required.has(k) || defaults.get(k) !== v) out[k] = v;
  }
  return out;
}

const parserDefaults = defaultsMap(PARSER_CONFIG);
const parserRequired = requiredKeys(PARSER_CONFIG);

export interface ParseResult {
  workers: Worker[];
  logicalSources: LogicalSource[];
  physicalSources: PhysicalSource[];
  sinks: Sink[];
  queries: Query[];
}

/**
 * Serialize store state arrays into NES-format YAML string.
 * Key ordering: queries, sinks, logical, physical, workers.
 * All sections are always emitted (empty arrays if no items) so the
 * WASM validator receives structurally valid YAML.
 */
export function storeToYaml(
  workers: Worker[],
  logicalSources: LogicalSource[],
  physicalSources: PhysicalSource[],
  sinks: Sink[],
  queries: Query[],
): string {
  // Build lookup maps for UUID -> display value resolution
  const workerHostMap = new Map(workers.map((w) => [w.id, w.host]));
  const logicalNameMap = new Map(logicalSources.map((ls) => [ls.id, ls.name]));

  const doc: Record<string, unknown> = {};

  // Queries: array of SQL strings (matches CLI `query:` key)
  doc.query = queries.map((q) => q.sql);

  // Sinks — all fields required by CLI: name, type, schema, host, config, parser_config
  doc.sinks = sinks.map((s) => ({
    name: s.name,
    type: s.type,
    schema: s.schema.map((f) => ({ name: f.name, type: f.type })),
    host: workerHostMap.get(s.hostWorkerId) ?? '',
    config: s.config,
    parser_config: s.parserConfig ?? {},
  }));

  // Logical sources
  doc.logical = logicalSources.map((ls) => ({
    name: ls.name,
    schema: ls.schema.map((f) => ({ name: f.name, type: f.type, ...(f.nullable ? { nullable: true } : {}) })),
  }));

  // Physical sources — all fields required by CLI: logical, type, host, parser_config, source_config
  doc.physical = physicalSources.map((ps) => ({
    logical: logicalNameMap.get(ps.logicalSourceId) ?? '',
    type: ps.type,
    host: workerHostMap.get(ps.hostWorkerId) ?? '',
    parser_config: stripDefaults(ps.parserConfig, parserDefaults, parserRequired),
    source_config: ps.sourceConfig,
  }));

  // Workers
  doc.workers = workers.map((w) => {
    const entry: Record<string, unknown> = {
      host: w.host,
      data: w.data,
      max_operators: w.capacity,
    };
    if (w.downstream.length > 0) {
      entry.downstream = w.downstream.map(
        (id) => workerHostMap.get(id) ?? '',
      );
    }
    return entry;
  });

  return yaml.dump(doc, { lineWidth: -1, noRefs: true, quotingType: '"' });
}

/**
 * Parse NES-format YAML string into typed store state arrays.
 * Generates fresh UUIDs for all entities and resolves cross-references.
 * Supports both old `query:` format and new `queries:` format.
 */
export function yamlToStore(yamlStr: string): ParseResult {
  const doc = yaml.load(yamlStr) as Record<string, unknown> | null;

  if (!doc || typeof doc !== 'object') {
    return {
      workers: [],
      logicalSources: [],
      physicalSources: [],
      sinks: [],
      queries: [],
    };
  }

  // 1. Parse workers, assign UUIDs, build host->id map
  const hostToId = new Map<string, string>();
  const rawWorkers = (doc.workers as any[]) ?? [];
  const workers: Worker[] = rawWorkers.map((w) => {
    const id = generateId();
    hostToId.set(w.host, id);
    return {
      id,
      host: w.host ?? '',
      data: w.data ?? '',
      capacity: w.max_operators ?? 10000,
      downstream: [],
      position: { x: 0, y: 0 },
    };
  });

  // 2. Resolve downstream references (host -> UUID)
  rawWorkers.forEach((w, i) => {
    if (w.downstream) {
      workers[i]!.downstream = (w.downstream as string[])
        .map((h: string) => hostToId.get(h))
        .filter(Boolean) as string[];
    }
  });

  // 3. Parse logical sources, build name->UUID map
  const logicalNameToId = new Map<string, string>();
  const logicalSources: LogicalSource[] = (
    (doc.logical as any[]) ?? []
  ).map((ls) => {
    const id = generateId();
    logicalNameToId.set(ls.name, id);
    return { id, name: ls.name, schema: (ls.schema ?? []).map((f: any) => ({ name: f.name, type: f.type, ...(f.nullable ? { nullable: true } : {}) })) };
  });

  // 4. Parse physical sources (resolve logical name -> UUID, host -> UUID)
  const physicalSources: PhysicalSource[] = (
    (doc.physical as any[]) ?? []
  ).map((ps) => ({
    id: generateId(),
    logicalSourceId: logicalNameToId.get(ps.logical) ?? '',
    hostWorkerId: hostToId.get(ps.host) ?? '',
    type: ps.type ?? '',
    sourceConfig: ps.source_config ?? {},
    parserConfig: ps.parser_config ?? {},
    position: { x: 0, y: 0 },
  }));

  // 5. Parse sinks
  const sinks: Sink[] = ((doc.sinks as any[]) ?? []).map((s) => ({
    id: generateId(),
    name: s.name ?? '',
    hostWorkerId: hostToId.get(s.host) ?? '',
    type: s.type ?? '',
    schema: (s.schema ?? []).map((f: any) => ({ name: f.name, type: f.type, ...(f.nullable ? { nullable: true } : {}) })),
    config: s.config ?? {},
    parserConfig: s.parser_config ?? {},
    position: { x: 0, y: 0 },
  }));

  // 6. Parse queries: query: string | string[] (CLI format)
  let queries: Query[] = [];
  if (doc.query) {
    const rawQueries = Array.isArray(doc.query)
      ? (doc.query as string[])
      : [doc.query as string];
    queries = rawQueries.map((sql) => ({
      id: generateId(),
      name: '',
      sql: typeof sql === 'string' ? sql.trim() : String(sql),
    }));
  }

  return { workers, logicalSources, physicalSources, sinks, queries };
}
