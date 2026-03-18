/**
 * NES source/sink form field definitions.
 * Derived from the NES C++ Config classes for each physical source/sink type.
 */

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
// Source Configs
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
    defaultValue: '\\n',
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

export const SOURCE_CONFIGS: Record<string, FormFieldDef[]> = {
  Generator: GENERATOR_SOURCE_CONFIG,
  File: FILE_SOURCE_CONFIG,
  TCP: TCP_SOURCE_CONFIG,
};

// ---------------------------------------------------------------------------
// Sink Configs
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

export const SINK_CONFIGS: Record<string, FormFieldDef[]> = {
  File: FILE_SINK_CONFIG,
  Print: PRINT_SINK_CONFIG,
  Void: VOID_SINK_CONFIG,
  Checksum: CHECKSUM_SINK_CONFIG,
};

// ---------------------------------------------------------------------------
// Type lists for dropdowns
// ---------------------------------------------------------------------------

export const SOURCE_TYPES: string[] = ['Generator', 'File', 'TCP'];
export const SINK_TYPES: string[] = ['File', 'Print', 'Void', 'Checksum'];
