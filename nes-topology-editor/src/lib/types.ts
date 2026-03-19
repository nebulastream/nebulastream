export interface Position {
  x: number;
  y: number;
}

export interface Worker {
  id: string;
  host: string;
  data: string;
  capacity: number;
  downstream: string[];
  position: Position;
}

export interface SchemaField {
  name: string;
  type: string;
  nullable?: boolean;
}

export interface LogicalSource {
  id: string;
  name: string;
  schema: SchemaField[];
}

export interface PhysicalSource {
  id: string;
  logicalSourceId: string;
  hostWorkerId: string;
  type: 'Generator' | 'CSV' | 'TCP' | string;
  sourceConfig: Record<string, string>;
  parserConfig: Record<string, string>;
  position: Position;
}

export interface Sink {
  id: string;
  name: string;
  hostWorkerId: string;
  type: 'File' | 'Void' | 'Print' | 'Checksum' | string;
  schema: SchemaField[];
  config: Record<string, string>;
  parserConfig: Record<string, string>;
  position: Position;
}

export interface Query {
  id: string;
  name: string;
  sql: string;
}

export interface TopologyState {
  workers: Worker[];
  logicalSources: LogicalSource[];
  physicalSources: PhysicalSource[];
  sinks: Sink[];
}
