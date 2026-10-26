import React from 'react';
import { ChevronDown, ChevronRight } from 'lucide-react';

/**
 * Shared tree rows for the Catalog Tree and the Query Studio schema explorer.
 *
 * Both panels browse the same catalog, so they share one row primitive: a
 * fixed 28px row, one chevron column, indent guides instead of margins, muted
 * right-aligned metadata and hover-revealed actions. Keeping the geometry here
 * is what stops the two trees from drifting apart again.
 */

export type TreeAccent = 'indigo' | 'sky';

/** One indent step, in pixels. Rails and nested padding both use it. */
export const TREE_INDENT = 14;

/** Left padding of a row at `depth` (0 = flush with the panel edge). */
export function treeIndent(depth: number): React.CSSProperties {
  return { paddingLeft: `${6 + Math.max(0, depth) * TREE_INDENT}px` };
}

const ACCENT: Record<TreeAccent, { selected: string; bar: string }> = {
  indigo: { selected: 'bg-indigo-50 text-indigo-950', bar: 'bg-indigo-500' },
  sky: { selected: 'bg-sky-50 text-sky-950', bar: 'bg-sky-500' },
};

/** Icon colours for the catalog hierarchy. Folder amber mirrors VS Code. */
export const TREE_ICON = {
  lakehouse: 'text-indigo-600',
  database: 'text-sky-600',
  folder: 'text-amber-500',
  schema: 'text-sky-600',
  table: 'text-slate-400',
  externalTable: 'text-sky-500',
} as const;

/** Neutral ghost button used inside row action slots. */
export const treeActionClass =
  'flex h-5 w-5 items-center justify-center rounded text-slate-400 transition-colors hover:bg-slate-200 hover:text-slate-700';

export interface TreeRowProps {
  /** Indent level; 0 for a section root. Each level draws one guide rail. */
  depth?: number;
  icon?: React.ReactNode;
  label: React.ReactNode;
  /** Muted, right-aligned metadata such as a table size. */
  meta?: React.ReactNode;
  /** Count chip rendered after the metadata. */
  badge?: React.ReactNode;
  /** Hover-revealed actions (rename, delete, ...). */
  actions?: React.ReactNode;
  expandable?: boolean;
  expanded?: boolean;
  selected?: boolean;
  accent?: TreeAccent;
  /** `section` is a bold root row (source or lakehouse). */
  variant?: 'row' | 'section';
  /** Keep actions visible instead of revealing them on hover. */
  actionsVisible?: boolean;
  onClick?: (event: React.MouseEvent) => void;
  /** When set, the chevron calls this instead of the row handler. */
  onToggle?: (event: React.MouseEvent) => void;
  title?: string;
  className?: string;
  labelClassName?: string;
}

export const TreeRow: React.FC<TreeRowProps> = ({
  depth = 0,
  icon,
  label,
  meta,
  badge,
  actions,
  expandable = false,
  expanded = false,
  selected = false,
  accent = 'indigo',
  variant = 'row',
  actionsVisible = false,
  onClick,
  onToggle,
  title,
  className = '',
  labelClassName = '',
}) => {
  const tone = ACCENT[accent];
  const isSection = variant === 'section';

  return (
    <div
      role="treeitem"
      aria-level={depth + 1}
      aria-expanded={expandable ? expanded : undefined}
      aria-selected={selected || undefined}
      title={title}
      onClick={onClick}
      className={[
        'group relative flex items-center pr-1.5 text-xs select-none',
        isSection ? 'h-8 font-semibold' : 'h-7',
        onClick || onToggle ? 'cursor-pointer' : '',
        selected
          ? tone.selected
          : isSection
          ? 'bg-slate-50/80 text-slate-800 hover:bg-slate-100'
          : 'text-slate-700 hover:bg-slate-100',
        className,
      ].join(' ')}
    >
      {selected && (
        <span aria-hidden className={`absolute inset-y-0 left-0 w-0.5 ${tone.bar}`} />
      )}

      {Array.from({ length: Math.max(0, depth) }).map((_, index) => (
        <span key={index} aria-hidden className="h-full w-3.5 shrink-0 border-l border-slate-200/80" />
      ))}

      {expandable ? (
        <button
          type="button"
          tabIndex={-1}
          aria-label={expanded ? 'Collapse' : 'Expand'}
          onClick={(event) => {
            event.stopPropagation();
            if (onToggle) onToggle(event);
            else onClick?.(event);
          }}
          className="flex h-4 w-4 shrink-0 items-center justify-center rounded text-slate-400 transition-colors hover:bg-slate-200/70 hover:text-slate-700"
        >
          {expanded ? (
            <ChevronDown className="h-3.5 w-3.5" />
          ) : (
            <ChevronRight className="h-3.5 w-3.5" />
          )}
        </button>
      ) : (
        <span aria-hidden className="h-4 w-4 shrink-0" />
      )}

      {icon && <span className="mr-1.5 flex h-4 w-4 shrink-0 items-center justify-center">{icon}</span>}

      <span className={`min-w-0 flex-1 truncate ${labelClassName}`}>{label}</span>

      {meta != null && meta !== '' && (
        <span className="ml-2 shrink-0 font-mono text-[10px] tabular-nums text-slate-400">{meta}</span>
      )}

      {badge != null && badge !== '' && (
        <span className="ml-1.5 shrink-0 rounded border border-slate-200 bg-white px-1.5 py-px font-mono text-[10px] font-medium tabular-nums text-slate-500">
          {badge}
        </span>
      )}

      {actions && (
        <span
          className={`ml-1 flex shrink-0 items-center gap-0.5 transition-opacity ${
            actionsVisible
              ? 'opacity-100'
              : 'opacity-0 group-focus-within:opacity-100 group-hover:opacity-100'
          }`}
        >
          {actions}
        </span>
      )}
    </div>
  );
};

/** Small uppercase heading that separates tree sections. */
export const TreeGroupLabel: React.FC<{
  children: React.ReactNode;
  meta?: React.ReactNode;
  className?: string;
}> = ({ children, meta, className = '' }) => (
  <div
    className={`flex items-center justify-between px-2 pb-1 pt-3 font-mono text-[10px] font-semibold uppercase tracking-wider text-slate-400 ${className}`}
  >
    <span className="truncate">{children}</span>
    {meta != null && meta !== '' && (
      <span className="ml-2 shrink-0 normal-case tracking-normal text-slate-400">{meta}</span>
    )}
  </div>
);

/** Muted placeholder for empty folders, empty filters and loading states. */
export const TreeMessage: React.FC<{
  children: React.ReactNode;
  /** Indent one level below `depth`, matching a child row. */
  depth?: number;
  className?: string;
}> = ({ children, depth, className = '' }) => (
  <div
    className={`flex h-7 items-center truncate pr-2 text-[11px] text-slate-400 ${className}`}
    style={depth == null ? undefined : treeIndent(depth + 1)}
  >
    {children}
  </div>
);
