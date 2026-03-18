/**
 * WASM Validation Message Protocol
 *
 * Defines the message types exchanged between the main thread and the
 * validation Web Worker, along with WASM lifecycle status types.
 */

// -- Request messages (main thread -> worker) --

export interface ValidateRequest {
  type: 'validate';
  id: number;
  yaml: string;
}

// -- Response messages (worker -> main thread) --

export interface ValidateResponse {
  type: 'result';
  id: number;
  /** Empty string = valid topology, non-empty = error message */
  error: string;
}

/** Config field metadata from C++ via WASM. */
export interface WasmFieldDef {
  name: string;
  type: 'string' | 'number' | 'boolean' | 'enum';
  required: boolean;
  defaultValue?: string;
}

/** Config metadata sent with the ready message. */
export interface ConfigMetadata {
  sourceTypes: string[];
  sinkTypes: string[];
  sourceConfigs: Record<string, WasmFieldDef[]>;
  sinkConfigs: Record<string, WasmFieldDef[]>;
}

export interface ReadyMessage {
  type: 'ready';
  configMetadata?: ConfigMetadata;
}

export interface ErrorMessage {
  type: 'error';
  message: string;
}

/** Union of all messages the worker can send back to the main thread. */
export type WorkerMessage = ValidateResponse | ReadyMessage | ErrorMessage;

/** WASM module lifecycle status. */
export type WasmStatus = 'loading' | 'ready' | 'error' | 'validating';

/** Maximum number of WASM load retries before giving up. */
export const MAX_RETRIES = 3;
