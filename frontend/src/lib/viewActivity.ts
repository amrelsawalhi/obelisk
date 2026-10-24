import { createContext, useContext, useEffect, useRef, useState } from 'react';

/**
 * Keeps mounted views honest about whether the user can actually see them.
 *
 * App keeps every visited tab mounted so its state survives navigation (editor
 * buffers, selected table, dock sizes, scroll position). The cost of that is
 * background work: an interval that used to die with an unmount now keeps
 * running for a view nobody is looking at. `usePolling` closes that gap - it
 * pauses when the tab is not the active one, and when the browser tab itself is
 * hidden, then refreshes once as soon as the view comes back.
 */
const ViewActivityContext = createContext(true);

export const ViewActivityProvider = ViewActivityContext.Provider;

/** True when this view's tab is the one rendered on screen. */
export function useViewActive(): boolean {
  return useContext(ViewActivityContext);
}

/**
 * Runs `callback` immediately, then on an interval, but only while the view is
 * visible. Returning to the tab triggers one immediate run so the first paint
 * after a switch is not stale (the previous state is already on screen, so the
 * refresh happens behind content rather than blocking it).
 */
export function usePolling(callback: () => void, intervalMs: number): void {
  const isActive = useViewActive();
  const [isDocumentVisible, setIsDocumentVisible] = useState(() => !document.hidden);
  const latest = useRef(callback);

  // Keep the timer pointed at the newest closure without restarting it.
  useEffect(() => {
    latest.current = callback;
  });

  useEffect(() => {
    const handleVisibility = () => setIsDocumentVisible(!document.hidden);
    document.addEventListener('visibilitychange', handleVisibility);
    return () => document.removeEventListener('visibilitychange', handleVisibility);
  }, []);

  useEffect(() => {
    if (!isActive || !isDocumentVisible) return;

    latest.current();
    const interval = window.setInterval(() => latest.current(), intervalMs);
    return () => window.clearInterval(interval);
  }, [isActive, isDocumentVisible, intervalMs]);
}
