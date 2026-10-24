/**
 * A tiny toast store.
 *
 * Module-level state, not a context: any component can call ``toast.error(...)``
 * without threading a provider through the tree, and the host is mounted once. The
 * alternative — an inline banner per screen — is what made pipeline validation
 * failures invisible: the banner sat above the fold while the operator was looking at
 * a node.
 */
export type ToastKind = 'success' | 'error' | 'info';

export interface ToastItem {
  id: number;
  kind: ToastKind;
  message: string;
  detail?: string;
}

type Listener = (toasts: ToastItem[]) => void;

let items: ToastItem[] = [];
let nextId = 1;
const listeners = new Set<Listener>();
const timers = new Map<number, number>();

const emit = () => {
  const snapshot = items;
  listeners.forEach((listener) => listener(snapshot));
};

const dismiss = (id: number) => {
  const timer = timers.get(id);
  if (timer) window.clearTimeout(timer);
  timers.delete(id);
  items = items.filter((item) => item.id !== id);
  emit();
};

const push = (kind: ToastKind, message: string, detail?: string) => {
  const id = nextId++;
  items = [...items.slice(-3), { id, kind, message, detail }];
  emit();
  // Errors stay long enough to be read and copied; successes get out of the way.
  const ttl = kind === 'error' ? 9000 : 4200;
  timers.set(id, window.setTimeout(() => dismiss(id), ttl));
  return id;
};

export const toast = {
  success: (message: string) => push('success', message),
  info: (message: string) => push('info', message),
  error: (message: string, detail?: string) => push('error', message, detail),
  dismiss,
};

export const subscribeToasts = (listener: Listener) => {
  listeners.add(listener);
  listener(items);
  return () => {
    listeners.delete(listener);
  };
};

/** Everything a FastAPI error can carry, without guessing at the shape at each call. */
export interface ApiProblem {
  message: string;
  steps: string[];
  edges: Array<{ from_key: string; to_key: string }>;
}

export const readApiError = (error: unknown, fallback: string): ApiProblem => {
  const response = (error as { response?: { data?: unknown } })?.response;
  const data = response?.data as { detail?: unknown } | undefined;
  const detail = data?.detail;
  if (typeof detail === 'string' && detail.trim()) {
    return { message: detail, steps: [], edges: [] };
  }
  if (detail && typeof detail === 'object') {
    const payload = detail as {
      message?: unknown;
      steps?: unknown;
      edges?: unknown;
    };
    return {
      message:
        typeof payload.message === 'string' && payload.message.trim()
          ? payload.message
          : fallback,
      steps: Array.isArray(payload.steps)
        ? payload.steps.filter((value): value is string => typeof value === 'string')
        : [],
      edges: Array.isArray(payload.edges)
        ? payload.edges
            .filter(
              (value): value is { from_key: string; to_key: string } =>
                Boolean(value) &&
                typeof (value as { from_key?: unknown }).from_key === 'string' &&
                typeof (value as { to_key?: unknown }).to_key === 'string',
            )
            .map((value) => ({ from_key: value.from_key, to_key: value.to_key }))
        : [],
    };
  }
  if (error instanceof Error && error.message) {
    return { message: error.message, steps: [], edges: [] };
  }
  return { message: fallback, steps: [], edges: [] };
};
