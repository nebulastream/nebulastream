/**
 * NES source/sink form field definitions.
 *
 * Contains hardcoded fallback configs (derived from C++ Config classes) and
 * helpers that convert dynamic WASM config metadata into the same FormFieldDef
 * format. The UI prefers dynamic metadata when available.
 */

import type { WasmFieldDef, ConfigMetadata } from './wasmValidation';

export interface FormFieldDef {
  key: string;
  label: string;
  type: 'text' | 'number' | 'select' | 'textarea' | 'boolean';
  required?: boolean;
  defaultValue?: string;
  options?: { value: string; label: string }[];
  placeholder?: string;
}

// ---------------------------------------------------------------------------
// Dynamic metadata conversion
// ---------------------------------------------------------------------------

/** Convert a parameter name like "file_path" to a label like "File Path". */
function nameToLabel(name: string): string {
  return name
    .split('_')
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}

/** Map C++ type names to UI form field types. */
function wasmTypeToFormType(t: WasmFieldDef['type']): FormFieldDef['type'] {
  switch (t) {
    case 'string': return 'text';
    case 'number': return 'number';
    case 'boolean': return 'boolean';
    case 'enum': return 'select';
    default: return 'text';
  }
}

/** Convert a WASM field definition to a FormFieldDef. */
function wasmFieldToFormField(field: WasmFieldDef): FormFieldDef {
  const def: FormFieldDef = {
    key: field.name,
    label: nameToLabel(field.name),
    type: wasmTypeToFormType(field.type),
    required: field.required,
  };
  if (field.defaultValue !== undefined) {
    def.defaultValue = field.defaultValue;
  }
  return def;
}

/** Convert an array of WASM field defs to FormFieldDef[]. */
export function wasmFieldsToFormFields(fields: WasmFieldDef[]): FormFieldDef[] {
  return fields.map(wasmFieldToFormField);
}

// ---------------------------------------------------------------------------
// Source Configs (hardcoded fallback)
// ---------------------------------------------------------------------------

export const GENERATOR_SOURCE_CONFIG: FormFieldDef[] = [
  { key: 'seed', label: 'Seed', type: 'number', defaultValue: '1' },
  {
    key: 'generator_schema',
    label: 'Generator Schema',
    type: 'textarea',
    required: true,
    placeholder: 'SEQUENCE UINT64 0 100 1',
  },
  {
    key: 'max_runtime_ms',
    label: 'Max Runtime (ms)',
    type: 'number',
    defaultValue: '-1',
    placeholder: '-1 for unlimited',
  },
  {
    key: 'stop_generator_when_sequence_finishes',
    label: 'Stop When Sequence Finishes',
    type: 'select',
    required: true,
    defaultValue: 'NONE',
    options: [
      { value: 'ALL', label: 'ALL' },
      { value: 'ONE', label: 'ONE' },
      { value: 'NONE', label: 'NONE' },
    ],
  },
  {
    key: 'generator_rate_type',
    label: 'Rate Type',
    type: 'select',
    defaultValue: 'FIXED',
    options: [
      { value: 'FIXED', label: 'FIXED' },
      { value: 'SINUS', label: 'SINUS' },
    ],
  },
  {
    key: 'generator_rate_config',
    label: 'Rate Config',
    type: 'text',
    defaultValue: 'emit_rate 1000',
  },
  {
    key: 'flush_interval_ms',
    label: 'Flush Interval (ms)',
    type: 'number',
    defaultValue: '10',
  },
  {
    key: 'max_inflight_buffers',
    label: 'Max Inflight Buffers',
    type: 'number',
    defaultValue: '0',
    placeholder: '0 = use default',
  },
];

export const FILE_SOURCE_CONFIG: FormFieldDef[] = [
  {
    key: 'file_path',
    label: 'File Path',
    type: 'text',
    required: true,
    placeholder: '/path/to/input.csv',
  },
  {
    key: 'max_inflight_buffers',
    label: 'Max Inflight Buffers',
    type: 'number',
    defaultValue: '0',
    placeholder: '0 = use default',
  },
];

export const TCP_SOURCE_CONFIG: FormFieldDef[] = [
  {
    key: 'socket_host',
    label: 'Socket Host',
    type: 'text',
    required: true,
    placeholder: '127.0.0.1',
  },
  {
    key: 'socket_port',
    label: 'Socket Port',
    type: 'number',
    required: true,
    placeholder: '0-65535',
  },
  {
    key: 'socket_domain',
    label: 'Socket Domain',
    type: 'select',
    defaultValue: 'AF_INET',
    options: [
      { value: 'AF_INET', label: 'AF_INET' },
      { value: 'AF_INET6', label: 'AF_INET6' },
    ],
  },
  {
    key: 'socket_type',
    label: 'Socket Type',
    type: 'select',
    defaultValue: 'SOCK_STREAM',
    options: [
      { value: 'SOCK_STREAM', label: 'SOCK_STREAM' },
      { value: 'SOCK_DGRAM', label: 'SOCK_DGRAM' },
    ],
  },
  {
    key: 'tuple_delimiter',
    label: 'Tuple Delimiter',
    type: 'text',
    defaultValue: '\n',
  },
  {
    key: 'flush_interval_ms',
    label: 'Flush Interval (ms)',
    type: 'number',
    defaultValue: '0',
  },
  {
    key: 'socket_buffer_size',
    label: 'Socket Buffer Size',
    type: 'number',
    defaultValue: '1024',
  },
  {
    key: 'bytes_sed_for_socket_buffer_size_transfer',
    label: 'Bytes for Buffer Size Transfer',
    type: 'number',
    defaultValue: '0',
  },
  {
    key: 'connect_timeout_seconds',
    label: 'Connect Timeout (s)',
    type: 'number',
    defaultValue: '10',
  },
  {
    key: 'max_inflight_buffers',
    label: 'Max Inflight Buffers',
    type: 'number',
    defaultValue: '0',
    placeholder: '0 = use default',
  },
];

export const PARSER_CONFIG: FormFieldDef[] = [
  {
    key: 'type',
    label: 'Parser Type',
    type: 'text',
    required: true,
    defaultValue: 'CSV',
  },
  {
    key: 'fieldDelimiter',
    label: 'Field Delimiter',
    type: 'text',
    defaultValue: ',',
  },
  {
    key: 'tupleDelimiter',
    label: 'Tuple Delimiter',
    type: 'text',
    defaultValue: '\\n',
  },
  {
    key: 'allowCommasInStrings',
    label: 'Allow Commas in Strings',
    type: 'boolean',
    defaultValue: 'false',
  },
];

/** Build a config object with all default values filled in. */
export function buildDefaults(fields: FormFieldDef[]): Record<string, string> {
  const config: Record<string, string> = {};
  for (const f of fields) {
    if (f.defaultValue !== undefined) {
      config[f.key] = f.defaultValue;
    }
  }
  return config;
}

/** Hardcoded fallback source configs. */
export const FALLBACK_SOURCE_CONFIGS: Record<string, FormFieldDef[]> = {
  Generator: GENERATOR_SOURCE_CONFIG,
  File: FILE_SOURCE_CONFIG,
  TCP: TCP_SOURCE_CONFIG,
};

// ---------------------------------------------------------------------------
// Sink Configs (hardcoded fallback)
// ---------------------------------------------------------------------------

export const FILE_SINK_CONFIG: FormFieldDef[] = [
  {
    key: 'file_path',
    label: 'File Path',
    type: 'text',
    required: true,
    placeholder: '/path/to/output.csv',
  },
  {
    key: 'append',
    label: 'Append',
    type: 'boolean',
    defaultValue: 'false',
  },
  {
    key: 'input_format',
    label: 'Input Format',
    type: 'select',
    defaultValue: 'CSV',
    options: [
      { value: 'CSV', label: 'CSV' },
      { value: 'JSON', label: 'JSON' },
    ],
  },
  {
    key: 'add_timestamp',
    label: 'Add Timestamp',
    type: 'boolean',
    defaultValue: 'false',
  },
];

export const PRINT_SINK_CONFIG: FormFieldDef[] = [
  {
    key: 'ingestion',
    label: 'Ingestion',
    type: 'number',
    defaultValue: '0',
  },
  {
    key: 'input_format',
    label: 'Input Format',
    type: 'select',
    defaultValue: 'CSV',
    options: [
      { value: 'CSV', label: 'CSV' },
      { value: 'JSON', label: 'JSON' },
    ],
  },
];

export const VOID_SINK_CONFIG: FormFieldDef[] = [];

export const CHECKSUM_SINK_CONFIG: FormFieldDef[] = [
  {
    key: 'file_path',
    label: 'File Path',
    type: 'text',
    required: true,
    placeholder: '/path/to/checksum.out',
  },
];

/** Hardcoded fallback sink configs. */
export const FALLBACK_SINK_CONFIGS: Record<string, FormFieldDef[]> = {
  File: FILE_SINK_CONFIG,
  Print: PRINT_SINK_CONFIG,
  Void: VOID_SINK_CONFIG,
  Checksum: CHECKSUM_SINK_CONFIG,
};

// ---------------------------------------------------------------------------
// Resolved configs: use WASM metadata if available, else fallback
// ---------------------------------------------------------------------------

/**
 * Get source config fields for a given type, using dynamic WASM metadata
 * when available and falling back to hardcoded definitions.
 */
export function getSourceFields(type: string, metadata: ConfigMetadata | null): FormFieldDef[] {
  if (metadata?.sourceConfigs) {
    // Registry keys are uppercase; try both original and uppercase lookup
    const fields = metadata.sourceConfigs[type] ?? metadata.sourceConfigs[type.toUpperCase()];
    if (fields) return wasmFieldsToFormFields(fields);
  }
  return FALLBACK_SOURCE_CONFIGS[type] ?? [];
}

/**
 * Get sink config fields for a given type, using dynamic WASM metadata
 * when available and falling back to hardcoded definitions.
 */
export function getSinkFields(type: string, metadata: ConfigMetadata | null): FormFieldDef[] {
  if (metadata?.sinkConfigs) {
    const fields = metadata.sinkConfigs[type] ?? metadata.sinkConfigs[type.toUpperCase()];
    if (fields) return wasmFieldsToFormFields(fields);
  }
  return FALLBACK_SINK_CONFIGS[type] ?? [];
}

/**
 * Get the list of registered source types.
 * Uses WASM metadata if available, otherwise returns hardcoded list.
 */
export function getSourceTypes(metadata: ConfigMetadata | null): string[] {
  if (metadata?.sourceTypes && metadata.sourceTypes.length > 0) {
    return metadata.sourceTypes;
  }
  return FALLBACK_SOURCE_TYPES;
}

/**
 * Get the list of registered sink types.
 * Uses WASM metadata if available, otherwise returns hardcoded list.
 */
export function getSinkTypes(metadata: ConfigMetadata | null): string[] {
  if (metadata?.sinkTypes && metadata.sinkTypes.length > 0) {
    return metadata.sinkTypes.filter((t) => !HIDDEN_TYPES.has(t.toUpperCase()));
  }
  return FALLBACK_SINK_TYPES;
}

// ---------------------------------------------------------------------------
// Backward-compatible exports (use fallback values)
// ---------------------------------------------------------------------------

export const SOURCE_CONFIGS: Record<string, FormFieldDef[]> = FALLBACK_SOURCE_CONFIGS;
export const SINK_CONFIGS: Record<string, FormFieldDef[]> = FALLBACK_SINK_CONFIGS;

const FALLBACK_SOURCE_TYPES: string[] = ['Generator', 'File', 'TCP'];
const FALLBACK_SINK_TYPES: string[] = ['File', 'Print', 'Void', 'Checksum'];

export const SOURCE_TYPES: string[] = FALLBACK_SOURCE_TYPES;
export const SINK_TYPES: string[] = FALLBACK_SINK_TYPES;
