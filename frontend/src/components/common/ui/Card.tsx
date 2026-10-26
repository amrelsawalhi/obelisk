import React from 'react';

export interface CardProps extends React.HTMLAttributes<HTMLDivElement> {
  padding?: 'none' | 'sm' | 'md' | 'lg';
}

export const Card: React.FC<CardProps> = ({
  padding = 'md',
  className = '',
  children,
  ...props
}) => {
  const paddingClass = {
    none: 'p-0',
    sm: 'p-3.5',
    md: 'p-5',
    lg: 'p-6',
  }[padding];

  return (
    <div
      className={`rounded-xl border border-slate-200 bg-white shadow-2xs ${paddingClass} ${className}`}
      {...props}
    >
      {children}
    </div>
  );
};

export interface CardHeaderProps {
  title: React.ReactNode;
  subtitle?: React.ReactNode;
  icon?: React.ReactNode;
  action?: React.ReactNode;
  className?: string;
}

export const CardHeader: React.FC<CardHeaderProps> = ({
  title,
  subtitle,
  icon,
  action,
  className = '',
}) => {
  return (
    <div className={`flex items-center justify-between pb-3.5 mb-3.5 border-b border-slate-100 ${className}`}>
      <div className="flex items-center gap-2.5 min-w-0">
        {icon && <span className="text-sky-600 shrink-0">{icon}</span>}
        <div className="min-w-0">
          <h3 className="text-sm font-bold text-slate-900 truncate leading-tight">{title}</h3>
          {subtitle && <p className="text-xs text-slate-500 mt-0.5 truncate">{subtitle}</p>}
        </div>
      </div>
      {action && <div className="shrink-0 flex items-center gap-2">{action}</div>}
    </div>
  );
};
