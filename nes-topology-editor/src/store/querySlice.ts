import type { StateCreator } from 'zustand';
import type { Query } from '../lib/types';
import { generateId } from '../lib/ids';

export interface QuerySlice {
  queries: Query[];

  addQuery: () => void;
  removeQuery: (id: string) => void;
  updateQuery: (id: string, updates: Partial<Omit<Query, 'id'>>) => void;
  replaceQueries: (queries: Query[]) => void;
}

export const createQuerySlice: StateCreator<QuerySlice, [], [], QuerySlice> = (
  set,
) => ({
  queries: [],

  addQuery: () =>
    set((state) => ({
      queries: [
        ...state.queries,
        { id: generateId(), name: '', sql: '' },
      ],
    })),

  removeQuery: (id: string) =>
    set((state) => ({
      queries: state.queries.filter((q) => q.id !== id),
    })),

  updateQuery: (id: string, updates: Partial<Omit<Query, 'id'>>) =>
    set((state) => ({
      queries: state.queries.map((q) =>
        q.id === id ? { ...q, ...updates } : q,
      ),
    })),

  replaceQueries: (queries: Query[]) => set({ queries }),
});
