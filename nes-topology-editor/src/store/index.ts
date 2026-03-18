import { create } from 'zustand';
import { temporal } from 'zundo';
import { createTopologySlice, type TopologySlice } from './topologySlice';
import { createUiSlice, type UiSlice } from './uiSlice';
import { createQuerySlice, type QuerySlice } from './querySlice';
import { createValidationSlice, type ValidationSlice } from './validationSlice';

export type StoreState = TopologySlice & UiSlice & QuerySlice & ValidationSlice;

export const useStore = create<StoreState>()(
  temporal(
    (...args) => ({
      ...createTopologySlice(...args),
      ...createUiSlice(...args),
      ...createQuerySlice(...args),
      ...createValidationSlice(...args),
    }),
    {
      partialize: (state) => ({
        workers: state.workers,
        logicalSources: state.logicalSources,
        physicalSources: state.physicalSources,
        sinks: state.sinks,
        queries: state.queries,
      }),
    },
  ),
);
