import type { StateCreator } from 'zustand';
import type { WasmStatus } from '../lib/wasmValidation';

export interface ValidationSlice {
  /** WASM module lifecycle status. */
  wasmStatus: WasmStatus;
  /** Error message if WASM loading failed, null otherwise. */
  wasmError: string | null;
  /** Semantic validation result: null = not yet validated, empty = valid, non-empty = error text. */
  semanticError: string | null;
  /** Number of WASM load retry attempts so far. */
  wasmRetryCount: number;

  setWasmStatus: (status: WasmStatus) => void;
  setWasmError: (error: string | null) => void;
  setSemanticError: (error: string | null) => void;
  incrementRetryCount: () => void;
  resetRetryCount: () => void;
}

export const createValidationSlice: StateCreator<
  ValidationSlice,
  [],
  [],
  ValidationSlice
> = (set) => ({
  wasmStatus: 'loading',
  wasmError: null,
  semanticError: null,
  wasmRetryCount: 0,

  setWasmStatus: (status) => set({ wasmStatus: status }),
  setWasmError: (error) => set({ wasmError: error }),
  setSemanticError: (error) => set({ semanticError: error }),
  incrementRetryCount: () =>
    set((state) => ({ wasmRetryCount: state.wasmRetryCount + 1 })),
  resetRetryCount: () => set({ wasmRetryCount: 0 }),
});
