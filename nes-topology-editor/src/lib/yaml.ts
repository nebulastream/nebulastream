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

  // Queries (future format: queries: [{query, name}])
  doc.queries = queries.map((q) => ({
    query: q.sql,
    name: q.name,
  }));

  // Sinks
  doc.sinks = sinks.map((s) => {
    const sinkDefaults = defaultsMap(SINK_CONFIGS[s.type] ?? []);
    const config = stripDefaults(s.config, sinkDefaults);
    const entry: Record<string, unknown> = {
      name: s.name,
      host: workerHostMap.get(s.hostWorkerId) ?? '',
      schema: s.schema.map((f) => ({ name: f.name, type: f.type })),
      type: s.type,
    };
    if (Object.keys(config).length > 0) entry.config = config;
    return entry;
  });

  // Logical sources
  doc.logical = logicalSources.map((ls) => ({
    name: ls.name,
    schema: ls.schema.map((f) => ({ name: f.name, type: f.type })),
  }));

  // Physical sources
  doc.physical = physicalSources.map((ps) => {
    const srcFields = SOURCE_CONFIGS[ps.type] ?? [];
    const sourceConfig = stripDefaults(ps.sourceConfig, defaultsMap(srcFields), requiredKeys(srcFields));
    const parserConfig = stripDefaults(ps.parserConfig, parserDefaults, parserRequired);
    const entry: Record<string, unknown> = {
      logical: logicalNameMap.get(ps.logicalSourceId) ?? '',
      host: workerHostMap.get(ps.hostWorkerId) ?? '',
      type: ps.type,
    };
    if (Object.keys(sourceConfig).length > 0) {
      entry.source_config = sourceConfig;
    }
    if (Object.keys(parserConfig).length > 0) {
      entry.parser_config = parserConfig;
    }
    return entry;
  });

  // Workers
  doc.workers = workers.map((w) => {
    const entry: Record<string, unknown> = {
      host: w.host,
      grpc: w.grpc,
      capacity: w.capacity,
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
      grpc: w.grpc ?? '',
      capacity: w.capacity ?? 10000,
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
    return { id, name: ls.name, schema: ls.schema ?? [] };
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
    schema: s.schema ?? [],
    config: s.config ?? {},
    position: { x: 0, y: 0 },
  }));

  // 6. Parse queries (support both old and new format)
  let queries: Query[] = [];
  if (doc.queries && Array.isArray(doc.queries)) {
    // New format: queries: [{query, name}]
    queries = (doc.queries as any[]).map((q) => ({
      id: generateId(),
      name: q.name ?? '',
      sql: q.query ?? '',
    }));
  } else if (doc.query) {
    // Old format: query: string | string[]
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
