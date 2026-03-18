import { useEffect, useRef } from 'react';
import { useStore } from '../store';
import { storeToYaml } from '../lib/yaml';
import { MAX_RETRIES } from '../lib/wasmValidation';
import type { WorkerMessage, ValidateRequest } from '../lib/wasmValidation';

const DEBOUNCE_MS = 300;

/**
 * Hook that manages the WASM validation Web Worker lifecycle.
 *
 * - Creates the Worker on mount, terminates on unmount
 * - Handles ready/result/error messages from the Worker
 * - Subscribes to topology state and triggers debounced validation on changes
 * - Implements retry logic (up to MAX_RETRIES) on Worker errors
 * - Protects against stale results via request ID tracking
 */
export function useWasmValidation(): void {
  const workerRef = useRef<Worker | null>(null);
  const requestIdRef = useRef(0);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const retryCountRef = useRef(0);

  const setWasmStatus = useStore((s) => s.setWasmStatus);
  const setWasmError = useStore((s) => s.setWasmError);
  const setSemanticError = useStore((s) => s.setSemanticError);
  const incrementRetryCount = useStore((s) => s.incrementRetryCount);
  const resetRetryCount = useStore((s) => s.resetRetryCount);

  // Topology state for generating YAML
  const workers = useStore((s) => s.workers);
  const logicalSources = useStore((s) => s.logicalSources);
  const physicalSources = useStore((s) => s.physicalSources);
  const sinks = useStore((s) => s.sinks);
  const queries = useStore((s) => s.queries);

  // Create worker and attach message handler
  const createWorker = useRef<() => Worker>(() => {
    const w = new Worker(
      new URL('../workers/validationWorker.ts', import.meta.url),
      { type: 'module' },
    );

    w.onmessage = (event: MessageEvent<WorkerMessage>) => {
      const msg = event.data;

      switch (msg.type) {
        case 'ready':
          retryCountRef.current = 0;
          resetRetryCount();
          setWasmStatus('ready');
          // Trigger initial validation immediately
          triggerValidation();
          break;

        case 'result':
          // Only apply if this is the latest request (stale result protection)
          if (msg.id === requestIdRef.current) {
            setSemanticError(msg.error);
            setWasmStatus('ready');
          }
          break;

        case 'error':
          retryCountRef.current += 1;
          incrementRetryCount();
          if (retryCountRef.current < MAX_RETRIES) {
            // Re-create the worker for retry
            w.terminate();
            setWasmStatus('loading');
            workerRef.current = createWorker.current();
          } else {
            setWasmStatus('error');
            setWasmError(msg.message);
          }
          break;
      }
    };

    return w;
  });

  // Helper to trigger a validation request on the current worker
  const triggerValidation = () => {
    const w = workerRef.current;
    if (!w) return;

    const yaml = storeToYaml(
      useStore.getState().workers,
      useStore.getState().logicalSources,
      useStore.getState().physicalSources,
      useStore.getState().sinks,
      useStore.getState().queries,
    );

    requestIdRef.current += 1;
    const request: ValidateRequest = {
      type: 'validate',
      id: requestIdRef.current,
      yaml,
    };
    setWasmStatus('validating');
    w.postMessage(request);
  };

  // Mount: create worker; Unmount: terminate
  useEffect(() => {
    workerRef.current = createWorker.current();

    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      workerRef.current?.terminate();
      workerRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Debounced validation on topology state changes
  useEffect(() => {
    // Check wasmStatus from store directly to avoid re-trigger loop
    const status = useStore.getState().wasmStatus;
    if (status !== 'ready' && status !== 'validating') return;

    if (debounceRef.current) clearTimeout(debounceRef.current);

    debounceRef.current = setTimeout(() => {
      if (useStore.getState().wasmStatus === 'ready') {
        triggerValidation();
      }
    }, DEBOUNCE_MS);

    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workers, logicalSources, physicalSources, sinks, queries]);
}
