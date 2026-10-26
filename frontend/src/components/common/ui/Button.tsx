import React from 'react';
import { Loader2 } from 'lucide-react';

export type ButtonVariant =
  | 'primary'
  | 'secondary'
  | 'outline'
  | 'ghost'
  | 'danger'
  | 'danger-outline';

export type ButtonSize = 'xs' | 'sm' | 'md';

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  loading?: boolean;
  icon?: React.ReactNode;
  iconRight?: React.ReactNode;
}

const VARIANT_STYLES: Record<ButtonVariant, string> = {
  primary:
    'bg-sky-600 hover:bg-sky-700 active:bg-sky-800 text-white border border-transparent shadow-2xs',
  secondary:
    'bg-slate-100 hover:bg-slate-200/80 active:bg-slate-200 text-slate-700 border border-slate-200/80',
  outline:
    'bg-white hover:bg-slate-50 active:bg-slate-100 text-slate-700 border border-slate-300 shadow-2xs',
  ghost:
    'bg-transparent hover:bg-slate-100 active:bg-slate-200 text-slate-600 hover:text-slate-900 border border-transparent',
  danger:
    'bg-rose-600 hover:bg-rose-700 active:bg-rose-800 text-white border border-transparent shadow-2xs',
  'danger-outline':
    'bg-white hover:bg-rose-50 active:bg-rose-100 text-rose-600 border border-rose-200 shadow-2xs',
};

const SIZE_STYLES: Record<ButtonSize, string> = {
  xs: 'px-2 py-1 text-[11px] gap-1 rounded-md font-medium',
  sm: 'px-3 py-1.5 text-xs gap-1.5 rounded-lg font-semibold',
  md: 'px-4 py-2 text-xs gap-2 rounded-lg font-semibold',
};

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      variant = 'secondary',
      size = 'sm',
      loading = false,
      disabled = false,
      icon,
      iconRight,
      className = '',
      children,
      ...props
    },
    ref,
  ) => {
    const isDisabled = disabled || loading;

    return (
      <button
        ref={ref}
        disabled={isDisabled}
        className={`inline-flex items-center justify-center select-none transition-colors focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-sky-500/40 disabled:opacity-50 disabled:cursor-not-allowed ${VARIANT_STYLES[variant]} ${SIZE_STYLES[size]} ${className}`}
        {...props}
      >
        {loading ? (
          <Loader2 className="w-3.5 h-3.5 animate-spin shrink-0" />
        ) : (
          icon && <span className="shrink-0 flex items-center">{icon}</span>
        )}
        {children && <span>{children}</span>}
        {!loading && iconRight && <span className="shrink-0 flex items-center">{iconRight}</span>}
      </button>
    );
  },
);

Button.displayName = 'Button';
