import { useEffect, useState } from 'react';

/**
 * Developer tools: Terminal.
 *
 * It is off unless asked for. The toggle is a user preference saved in localStorage.
 * Module-level state, like `toast`, because the sidebar and the Settings toggle do not share a
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
