/**
 * Validation Web Worker
 *
 * Loads the WASM validation module and exposes validateTopology()
 * via the postMessage protocol defined in wasmValidation.ts.
 */

import createModule from '../../wasm/build/nes-validator/nes-validator-wasm.mjs';
import type { ValidateRequest, ValidateResponse, ReadyMessage, ErrorMessage, ConfigMetadata, WasmFieldDef } from '../lib/wasmValidation';

interface WasmModule {
  validateTopology: (yaml: string) => string;
  getRegisteredSourceTypes?: () => string;
  getRegisteredSinkTypes?: () => string;
  getSourceConfigFields?: (type: string) => string;
  getSinkConfigFields?: (type: string) => string;
}

// Module instance, set once initialization completes.
let wasmModule: WasmModule | null = null;

/** Query config metadata from the WASM module. Returns undefined if functions are unavailable. */
function queryConfigMetadata(module: WasmModule): ConfigMetadata | undefined {
  if (!module.getRegisteredSourceTypes || !module.getRegisteredSinkTypes
      || !module.getSourceConfigFields || !module.getSinkConfigFields) {
    return undefined;
  }

  try {
    const sourceTypes: string[] = JSON.parse(module.getRegisteredSourceTypes());
    const sinkTypes: string[] = JSON.parse(module.getRegisteredSinkTypes());

    const sourceConfigs: Record<string, WasmFieldDef[]> = {};
    for (const t of sourceTypes) {
      sourceConfigs[t] = JSON.parse(module.getSourceConfigFields(t));
    }

    const sinkConfigs: Record<string, WasmFieldDef[]> = {};
    for (const t of sinkTypes) {
      sinkConfigs[t] = JSON.parse(module.getSinkConfigFields(t));
    }

    return { sourceTypes, sinkTypes, sourceConfigs, sinkConfigs };
  } catch {
    return undefined;
  }
}

// Initialize the WASM module on worker startup.
createModule()
  .then((module: WasmModule) => {
    wasmModule = module;
    const configMetadata = queryConfigMetadata(module);
    const ready: ReadyMessage = { type: 'ready', configMetadata };
    self.postMessage(ready);
  })
  .catch((err: unknown) => {
    const error: ErrorMessage = { type: 'error', message: String(err) };
    self.postMessage(error);
  });

// Handle validation requests from the main thread.
self.onmessage = (event: MessageEvent<ValidateRequest>) => {
  const { data } = event;

  if (data.type === 'validate') {
    if (!wasmModule) {
      const error: ErrorMessage = { type: 'error', message: 'WASM module not loaded' };
      self.postMessage(error);
      return;
    }

    try {
      const result = wasmModule.validateTopology(data.yaml);
      const response: ValidateResponse = {
        type: 'result',
        id: data.id,
        error: result,
      };
      self.postMessage(response);
    } catch (err: unknown) {
      const error: ErrorMessage = { type: 'error', message: String(err) };
      self.postMessage(error);
    }
  }
};
