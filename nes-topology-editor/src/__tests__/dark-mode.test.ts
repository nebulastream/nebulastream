import { describe, it, expect, beforeEach } from 'vitest';
import { useStore } from '../store';

describe('Dark Mode', () => {
  beforeEach(() => {
    // Reset store and DOM state
    useStore.setState({ darkMode: false });
    document.documentElement.classList.remove('dark');
    localStorage.removeItem('theme');
  });

  it('toggleDarkMode flips darkMode state in store', () => {
    expect(useStore.getState().darkMode).toBe(false);
    useStore.getState().toggleDarkMode();
    expect(useStore.getState().darkMode).toBe(true);
    useStore.getState().toggleDarkMode();
    expect(useStore.getState().darkMode).toBe(false);
  });

  it('toggleDarkMode adds/removes dark class on document.documentElement', () => {
    expect(document.documentElement.classList.contains('dark')).toBe(false);
    useStore.getState().toggleDarkMode();
    expect(document.documentElement.classList.contains('dark')).toBe(true);
    useStore.getState().toggleDarkMode();
    expect(document.documentElement.classList.contains('dark')).toBe(false);
  });

  it('toggleDarkMode persists theme key in localStorage', () => {
    expect(localStorage.getItem('theme')).toBeNull();
    useStore.getState().toggleDarkMode();
    expect(localStorage.getItem('theme')).toBe('dark');
    useStore.getState().toggleDarkMode();
    expect(localStorage.getItem('theme')).toBe('light');
  });
});
