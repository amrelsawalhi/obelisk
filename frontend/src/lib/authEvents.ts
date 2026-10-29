/**
 * A tiny pub/sub so the axios interceptor can tell the auth context that a
 * session died, without importing React (which would cycle: api.ts ← context
 * ← api.ts).
 */

type Listener = () => void;

const listeners = new Set<Listener>();

export function onLoggedOut(listener: Listener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function emitLoggedOut(): void {
  for (const listener of Array.from(listeners)) {
    try {
      listener();
    } catch {
      // A broken listener must not break the other subscribers.
    }
  }
}
