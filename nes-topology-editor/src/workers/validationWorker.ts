/**
 * Validation Web Worker
 *
 * Loads the WASM validation module and exposes validateTopology()
 * via the postMessage protocol defined in wasmValidation.ts.
 */

import createModule from '../../wasm/build/nes-validator/nes-validator-wasm.mjs';
import type { ValidateRequest, ValidateResponse, ReadyMessage, ErrorMessage } from '../lib/wasmValidation';

// Module instance, set once initialization completes.
let wasmModule: { validateTopology: (yaml: string) => string } | null = null;

// Initialize the WASM module on worker startup.
createModule()
  .then((module: { validateTopology: (yaml: string) => string }) => {
    wasmModule = module;
    const ready: ReadyMessage = { type: 'ready' };
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
