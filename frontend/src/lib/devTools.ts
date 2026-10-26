import { useEffect, useState } from 'react';

/**
 * The developer raw tools: Files and Terminal.
 *
 * They are off unless asked for. Two more trees over `git_repo/` beside dbt Studio
 * and the Marimo file tree is how a model ends up edited in the wrong place, so the
 * pair is a preference instead of a permanent part of the navigation. Module-level
 * state, like `toast`, because the sidebar and the Settings toggle do not share a
 * parent that could own it.
 */
const STORAGE_KEY = 'obelisk_dev_tools';

type Listener = (enabled: boolean) => void;

const read = (): boolean => {
  try {
    return localStorage.getItem(STORAGE_KEY) === 'on';
  } catch {
    return false;
  }
};

let enabled = read();
const listeners = new Set<Listener>();

export const devTools = {
  isEnabled: () => enabled,
  setEnabled: (next: boolean) => {
    enabled = next;
    try {
      localStorage.setItem(STORAGE_KEY, next ? 'on' : 'off');
    } catch {
      // A browser that refuses storage still gets a working toggle for this session.
    }
    listeners.forEach((listener) => listener(enabled));
  },
};

export const subscribeDevTools = (listener: Listener) => {
  listeners.add(listener);
  listener(enabled);
  return () => {
    listeners.delete(listener);
  };
};

/** Keeps the sidebar and the Settings toggle on the same value. */
export function useDevToolsEnabled(): [boolean, (next: boolean) => void] {
  const [value, setValue] = useState(enabled);
  useEffect(() => subscribeDevTools(setValue), []);
  return [value, devTools.setEnabled];
}
