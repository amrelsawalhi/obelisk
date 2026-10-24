import React, { useEffect, useRef } from 'react';
import { AlertTriangle } from 'lucide-react';

export interface ConfirmRequest {
  title: string;
  body?: string;
  confirmLabel?: string;
  destructive?: boolean;
  onConfirm: () => void | Promise<void>;
}

/**
 * A styled confirm, because ``window.confirm`` looks like a browser from 2009 and
 * blocks the event loop (which freezes the poll that paints the canvas).
 */
export const ConfirmDialog: React.FC<{
  request: ConfirmRequest | null;
  onClose: () => void;
}> = ({ request, onClose }) => {
  const confirmRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!request) return undefined;
    confirmRef.current?.focus();
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
      if (event.key === 'Enter') {
        event.preventDefault();
        void request.onConfirm();
        onClose();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [request, onClose]);

  if (!request) return null;

  return (
    <div className="fixed inset-0 z-[90] flex items-center justify-center bg-slate-900/30 p-4">
      <div
        role="dialog"
        aria-modal="true"
        aria-label={request.title}
        className="w-[420px] max-w-full rounded-xl border border-slate-200 bg-white shadow-2xl"
      >
        <div className="flex items-start gap-2.5 px-4 pt-4">
          {request.destructive && (
            <span className="mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-rose-50 text-rose-600">
              <AlertTriangle className="h-4 w-4" />
            </span>
          )}
          <div className="min-w-0">
            <h2 className="text-sm font-semibold text-slate-800">{request.title}</h2>
            {request.body && (
              <p className="mt-1 text-xs leading-relaxed text-slate-500">{request.body}</p>
            )}
          </div>
        </div>
        <div className="mt-4 flex justify-end gap-2 border-t border-slate-100 px-4 py-3">
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg border border-slate-300 px-3 py-1.5 text-[11px] font-semibold text-slate-600 hover:bg-slate-50"
          >
            Cancel
          </button>
          <button
            ref={confirmRef}
            type="button"
            onClick={() => {
              void request.onConfirm();
              onClose();
            }}
            className={`rounded-lg px-3 py-1.5 text-[11px] font-semibold text-white ${
              request.destructive
                ? 'bg-rose-600 hover:bg-rose-700'
                : 'bg-indigo-600 hover:bg-indigo-700'
            }`}
          >
            {request.confirmLabel ?? 'Confirm'}
          </button>
        </div>
      </div>
    </div>
  );
};
