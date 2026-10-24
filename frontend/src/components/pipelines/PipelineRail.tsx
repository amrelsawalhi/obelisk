import React, { useMemo, useState } from 'react';
import { Clock, Loader2, PauseCircle, Plus, Search, Zap } from 'lucide-react';
import type { PipelineSummary } from '../../lib/types';

const DOT: Record<string, string> = {
  success: 'bg-emerald-500',
  failed: 'bg-rose-500',
  running: 'bg-indigo-500',
  cancelled: 'bg-amber-500',
  interrupted: 'bg-orange-500',
  skipped: 'bg-slate-300',
  pending: 'bg-slate-300',
  waiting: 'bg-slate-300',
};

const fmtWhen = (value?: string | null) => {
  if (!value) return '—';
  const date = new Date(value.includes('+') || value.endsWith('Z') ? value : `${value}Z`);
  if (Number.isNaN(date.getTime())) return '—';
  return date.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
};

interface PipelineRailProps {
  pipelines: PipelineSummary[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onNew: () => void;
}

/**
 * The rail groups by what an operator is looking for: what is running now, what will
 * run next, and what is deliberately paused. A flat alphabetical list hides exactly
 * that, and searching 40 pipelines for the one that failed is the common case.
 */
export const PipelineRail: React.FC<PipelineRailProps> = ({
  pipelines,
  selectedId,
  onSelect,
  onNew,
}) => {
  const [query, setQuery] = useState('');

  const groups = useMemo(() => {
    const needle = query.trim().toLowerCase();
    const matches = pipelines.filter(
      (pipeline) =>
        !needle ||
        pipeline.name.toLowerCase().includes(needle) ||
        (pipeline.description ?? '').toLowerCase().includes(needle),
    );
    return [
      {
        key: 'running',
        label: 'Running now',
        items: matches.filter((pipeline) => pipeline.is_running),
      },
      {
        key: 'active',
        label: 'Scheduled',
        items: matches.filter((pipeline) => !pipeline.is_running && pipeline.is_enabled),
      },
      {
        key: 'paused',
        label: 'Paused',
        items: matches.filter((pipeline) => !pipeline.is_running && !pipeline.is_enabled),
      },
    ].filter((group) => group.items.length > 0);
  }, [pipelines, query]);

  const renderRow = (pipeline: PipelineSummary) => {
    const active = pipeline.id === selectedId;
    const dots = [...(pipeline.recent_statuses ?? [])].reverse().slice(-6);
    return (
      <button
        key={pipeline.id}
        type="button"
        onClick={() => onSelect(pipeline.id)}
        className={`flex w-full flex-col gap-1 rounded-lg px-2.5 py-2 text-left transition-colors ${
          active ? 'bg-indigo-50 ring-1 ring-inset ring-indigo-200' : 'hover:bg-slate-50'
        }`}
      >
        <span className="flex items-center gap-1.5">
          {pipeline.is_running ? (
            <Loader2 className="h-3 w-3 shrink-0 animate-spin text-indigo-600" />
          ) : pipeline.is_enabled ? (
            <Zap className="h-3 w-3 shrink-0 text-slate-400" />
          ) : (
            <PauseCircle className="h-3 w-3 shrink-0 text-slate-300" />
          )}
          <span
            className={`min-w-0 flex-1 truncate text-xs font-semibold ${
              active ? 'text-indigo-900' : 'text-slate-700'
            }`}
          >
            {pipeline.name}
          </span>
          <span className="shrink-0 text-[10px] font-mono text-slate-400">
            {pipeline.step_count}
          </span>
        </span>
        <span className="flex items-center justify-between gap-2">
          <span className="truncate text-[10px] font-mono text-slate-400">
            {pipeline.is_enabled ? pipeline.cron_expression : 'paused'}
            {pipeline.is_enabled && pipeline.next_run_at
              ? ` · ${fmtWhen(pipeline.next_run_at)}`
              : ''}
          </span>
          <span className="flex shrink-0 items-center gap-0.5" aria-label="Recent runs">
            {dots.length ? (
              dots.map((status, index) => (
                <span
                  key={`${pipeline.id}-${index}`}
                  className={`h-1.5 w-1.5 rounded-full ${DOT[status] ?? 'bg-slate-300'}`}
                  title={status}
                />
              ))
            ) : (
              <Clock className="h-2.5 w-2.5 text-slate-300" />
            )}
          </span>
        </span>
      </button>
    );
  };

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex items-center gap-1.5 border-b border-slate-200 px-2.5 py-2">
        <div className="relative min-w-0 flex-1">
          <Search className="pointer-events-none absolute left-2 top-1/2 h-3 w-3 -translate-y-1/2 text-slate-400" />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search pipelines"
            aria-label="Search pipelines"
            className="w-full rounded-lg border border-slate-200 bg-slate-50 py-1 pl-7 pr-2 text-[11px] text-slate-700 placeholder:text-slate-400 focus:border-indigo-400 focus:bg-white focus:outline-hidden"
          />
        </div>
        <button
          type="button"
          onClick={onNew}
          className="rounded-lg bg-indigo-600 p-1.5 text-white hover:bg-indigo-700"
          aria-label="New pipeline"
          title="New pipeline"
        >
          <Plus className="h-3.5 w-3.5" />
        </button>
      </div>

      <div className="min-h-0 flex-1 space-y-3 overflow-y-auto p-2">
        {!pipelines.length && (
          <p className="px-1.5 py-3 text-[11px] leading-relaxed text-slate-400">
            No pipelines yet. Create one to schedule dbt, scripts, notebooks, SQL or a webhook.
          </p>
        )}
        {pipelines.length > 0 && !groups.length && (
          <p className="px-1.5 py-3 text-[11px] text-slate-400">Nothing matches “{query}”.</p>
        )}
        {groups.map((group) => (
          <div key={group.key} className="space-y-0.5">
            <p className="px-1.5 pb-1 text-[9px] font-bold uppercase tracking-wider text-slate-400">
              {group.label} · {group.items.length}
            </p>
            {group.items.map(renderRow)}
          </div>
        ))}
      </div>
    </div>
  );
};
