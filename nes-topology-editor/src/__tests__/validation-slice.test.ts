import { describe, it, expect } from 'vitest';
import { createStore } from 'zustand/vanilla';
import { createValidationSlice, type ValidationSlice } from '../store/validationSlice';

function makeStore() {
  return createStore<ValidationSlice>()((...args) => createValidationSlice(...args));
}

describe('validationSlice', () => {
  it('has correct initial state', () => {
    const store = makeStore();
    const state = store.getState();
    expect(state.wasmStatus).toBe('loading');
    expect(state.wasmError).toBeNull();
    expect(state.semanticError).toBeNull();
    expect(state.wasmRetryCount).toBe(0);
  });

  it('setWasmStatus updates wasmStatus', () => {
    const store = makeStore();
    store.getState().setWasmStatus('ready');
    expect(store.getState().wasmStatus).toBe('ready');

    store.getState().setWasmStatus('error');
    expect(store.getState().wasmStatus).toBe('error');

    store.getState().setWasmStatus('validating');
    expect(store.getState().wasmStatus).toBe('validating');
  });

  it('setSemanticError updates semanticError', () => {
    const store = makeStore();
    store.getState().setSemanticError('some error');
    expect(store.getState().semanticError).toBe('some error');

    store.getState().setSemanticError('');
    expect(store.getState().semanticError).toBe('');

    store.getState().setSemanticError(null);
    expect(store.getState().semanticError).toBeNull();
  });

  it('setWasmError updates wasmError', () => {
    const store = makeStore();
    store.getState().setWasmError('load failed');
    expect(store.getState().wasmError).toBe('load failed');

    store.getState().setWasmError(null);
    expect(store.getState().wasmError).toBeNull();
  });

  it('incrementRetryCount increments wasmRetryCount', () => {
    const store = makeStore();
    expect(store.getState().wasmRetryCount).toBe(0);

    store.getState().incrementRetryCount();
    expect(store.getState().wasmRetryCount).toBe(1);

    store.getState().incrementRetryCount();
    expect(store.getState().wasmRetryCount).toBe(2);
  });

  it('resetRetryCount resets wasmRetryCount to 0', () => {
    const store = makeStore();
    store.getState().incrementRetryCount();
    store.getState().incrementRetryCount();
    expect(store.getState().wasmRetryCount).toBe(2);

    store.getState().resetRetryCount();
    expect(store.getState().wasmRetryCount).toBe(0);
  });
});
