import React from 'react';

export interface TabItem<T extends string = string> {
  id: T;
  label: React.ReactNode;
  icon?: React.ReactNode;
  badge?: React.ReactNode;
}

export interface TabsProps<T extends string = string> {
  items: TabItem<T>[];
  activeId: T;
  onChange: (id: T) => void;
  variant?: 'line' | 'pill';
  className?: string;
  itemClassName?: string;
}

export function Tabs<T extends string = string>({
  items,
  activeId,
  onChange,
  variant = 'line',
  className = '',
  itemClassName = '',
}: TabsProps<T>) {
  if (variant === 'pill') {
    return (
      <div
        className={`inline-flex items-center rounded-lg bg-slate-200/70 p-1 text-xs font-medium text-slate-600 select-none ${className}`}
      >
        {items.map((item) => {
          const isActive = item.id === activeId;
          return (
            <button
              key={item.id}
              type="button"
              onClick={() => onChange(item.id)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md transition-all ${
                isActive
                  ? 'bg-white text-slate-900 font-semibold shadow-xs'
                  : 'hover:text-slate-900 text-slate-600'
              } ${itemClassName}`}
            >
              {item.icon && <span className="shrink-0">{item.icon}</span>}
              <span>{item.label}</span>
              {item.badge && <span className="shrink-0">{item.badge}</span>}
            </button>
          );
        })}
      </div>
    );
  }

  // Default: 'line' tabs
  return (
    <div className={`flex items-center gap-4 border-b border-slate-200 select-none ${className}`}>
      {items.map((item) => {
        const isActive = item.id === activeId;
        return (
          <button
            key={item.id}
            type="button"
            onClick={() => onChange(item.id)}
            className={`flex items-center gap-2 py-2.5 px-1 text-xs font-semibold border-b-2 transition-colors ${
              isActive
                ? 'border-sky-600 text-sky-700'
                : 'border-transparent text-slate-500 hover:text-slate-800 hover:border-slate-300'
            } ${itemClassName}`}
          >
            {item.icon && <span className="shrink-0">{item.icon}</span>}
            <span>{item.label}</span>
            {item.badge && <span className="shrink-0">{item.badge}</span>}
          </button>
        );
      })}
    </div>
  );
}
