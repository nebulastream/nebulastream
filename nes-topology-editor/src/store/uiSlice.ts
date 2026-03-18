import type { StateCreator } from 'zustand';
import type { ClipboardData } from '../lib/clipboard';

export type { ClipboardData };

export type SidebarTab = 'properties' | 'sources';

export interface UiSlice {
  selectedNodeId: string | null;
  clipboard: ClipboardData | null;
  sidebarTab: SidebarTab;
  selectedQueryId: string | null;
  yamlOverlayVisible: boolean;

  selectNode: (id: string | null) => void;
  setClipboard: (data: ClipboardData | null) => void;
  setSidebarTab: (tab: SidebarTab) => void;
  selectQuery: (id: string | null) => void;
  darkMode: boolean;
  toggleYamlOverlay: () => void;
  toggleDarkMode: () => void;
}

export const createUiSlice: StateCreator<UiSlice, [], [], UiSlice> = (
  set,
) => ({
  selectedNodeId: null,
  clipboard: null,
  sidebarTab: 'properties' as SidebarTab,
  selectedQueryId: null,
  yamlOverlayVisible: false,
  darkMode: typeof document !== 'undefined' && document.documentElement.classList.contains('dark'),

  selectNode: (id: string | null) => set({ selectedNodeId: id }),
  setClipboard: (data: ClipboardData | null) => set({ clipboard: data }),
  setSidebarTab: (tab: SidebarTab) => set({ sidebarTab: tab }),
  selectQuery: (id: string | null) => set({ selectedQueryId: id }),
  toggleYamlOverlay: () =>
    set((state) => ({ yamlOverlayVisible: !state.yamlOverlayVisible })),
  toggleDarkMode: () =>
    set((state) => {
      const next = !state.darkMode;
      document.documentElement.classList.toggle('dark', next);
      localStorage.setItem('theme', next ? 'dark' : 'light');
      return { darkMode: next };
    }),
});
