import type { Worker } from './types';

export function generateId(): string {
  return crypto.randomUUID();
}

export function generateWorkerHost(existingWorkers: Worker[]): string {
  const existingNumbers = existingWorkers
    .map((w) => {
      const match = w.host.match(/^worker-(\d+):/);
      return match ? parseInt(match[1]!, 10) : 0;
    })
    .filter((n) => n > 0);

  const next =
    existingNumbers.length > 0 ? Math.max(...existingNumbers) + 1 : 1;
  return `worker-${next}:8080`;
}
