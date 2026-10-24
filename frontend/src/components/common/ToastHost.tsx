import React, { useEffect, useState } from 'react';
import { AlertTriangle, CheckCircle2, Info, X } from 'lucide-react';
import { subscribeToasts, toast, type ToastItem } from '../../lib/toast';

const STYLES: Record<string, { wrap: string; icon: React.ElementType; iconClass: string }> = {
  success: {
    wrap: 'border-emerald-200 bg-white text-emerald-800',
    icon: CheckCircle2,
    iconClass: 'text-emerald-600',
  },
  error: {
    wrap: 'border-rose-200 bg-white text-rose-800',
    icon: AlertTriangle,
    iconClass: 'text-rose-600',
  },
  info: {
    wrap: 'border-slate-200 bg-white text-slate-700',
    icon: Info,
    iconClass: 'text-slate-500',
  },
};

/**
 * Bottom-right stack. Bottom-right because that is where the eye already is after
 * clicking Save, and because the top of this screen holds the header and the tabs.
 */
export const ToastHost: React.FC = () => {
  const [items, setItems] = useState<ToastItem[]>([]);

  useEffect(() => subscribeToasts(setItems), []);

  if (!items.length) return null;

  return (
    <div
      className="pointer-events-none fixed bottom-4 right-4 z-[80] flex w-[360px] max-w-[calc(100vw-2rem)] flex-col gap-2"
      role="status"
      aria-live="polite"
    >
      {items.map((item) => {
        const style = STYLES[item.kind] ?? STYLES.info;
        const Icon = style.icon;
        return (
          <div
            key={item.id}
            className={`pointer-events-auto flex items-start gap-2 rounded-xl border px-3 py-2.5 shadow-lg shadow-slate-900/5 ${style.wrap}`}
          >
            <Icon className={`mt-0.5 h-4 w-4 shrink-0 ${style.iconClass}`} />
            <div className="min-w-0 flex-1">
              <p className="text-xs font-semibold leading-snug">{item.message}</p>
              {item.detail && (
                <p className="mt-0.5 break-words text-[11px] leading-snug text-slate-500">
                  {item.detail}
                </p>
              )}
            </div>
            <button
              type="button"
              onClick={() => toast.dismiss(item.id)}
              className="rounded p-0.5 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
              aria-label="Dismiss notification"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          </div>
        );
      })}
    </div>
  );
};
