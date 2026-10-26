import React from 'react';
import { Ban, CheckCircle2, CircleSlash, Clock, ExternalLink, Loader2, XCircle } from 'lucide-react';
import type { PipelineExecution } from '../../lib/types';

const STATUS: Record<string, { chip: string; icon: React.ElementType; label: string }> = {
  success: { chip: 'bg-emerald-50 text-emerald-700', icon: CheckCircle2, label: 'Success' },
  failed: { chip: 'bg-rose-50 text-rose-700', icon: XCircle, label: 'Failed' },
  running: { chip: 'bg-sky-50 text-sky-700', icon: Loader2, label: 'Running' },
  pending: { chip: 'bg-slate-100 text-slate-600', icon: Clock, label: 'Queued' },
  cancelled: { chip: 'bg-amber-50 text-amber-700', icon: Ban, label: 'Cancelled' },
  interrupted: { chip: 'bg-orange-50 text-orange-700', icon: Ban, label: 'Interrupted' },
  skipped: { chip: 'bg-slate-100 text-slate-500', icon: CircleSlash, label: 'Skipped' },
};

export const fmtWhen = (value?: string | null) => {
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

export const fmtDuration = (seconds?: number | null) => {
  if (seconds == null) return '—';
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${Math.round(seconds % 60)}s`;
};

interface PipelineRunsTableProps {
  runs: PipelineExecution[];
  selectedRunId?: string | null;
  onOpen: (run: PipelineExecution) => void;
  onCancel: (run: PipelineExecution) => void;
}

/**
 * The Runs tab: one row per execution, newest first, with the counts an operator
 * scans for. Opening a row loads it into the canvas and the log dock rather than
 * expanding a detail panel here, so the run is watched on the same picture it was
 * designed on.
 */
export const PipelineRunsTable: React.FC<PipelineRunsTableProps> = ({
  runs,
  selectedRunId,
  onOpen,
  onCancel,
}) => {
  if (!runs.length) {
    return (
      <div className="flex h-full items-center justify-center p-10 text-center">
        <p className="max-w-[320px] text-xs leading-relaxed text-slate-400">
          No runs yet. Run this pipeline once to see its steps execute, or wait for the
          schedule to fire.
        </p>
      </div>
    );
  }

  return (
    <div className="h-full overflow-auto">
      <table className="w-full border-collapse text-left">
        <thead className="sticky top-0 z-10 bg-slate-50/95 backdrop-blur">
          <tr className="text-[10px] font-bold uppercase tracking-wider text-slate-400">
            <th className="px-4 py-2 font-bold">Status</th>
            <th className="px-3 py-2 font-bold">Run</th>
            <th className="px-3 py-2 font-bold">Started</th>
            <th className="px-3 py-2 font-bold">Duration</th>
            <th className="px-3 py-2 font-bold">Steps</th>
            <th className="px-3 py-2 font-bold">Trigger</th>
            <th className="px-4 py-2" />
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => {
            const status = STATUS[run.status] ?? STATUS.pending;
            const Icon = status.icon;
            const selected = run.id === selectedRunId;
            const live = run.status === 'running' || run.status === 'pending';
            return (
              <tr
                key={run.id}
                onClick={() => onOpen(run)}
                className={`cursor-pointer border-t border-slate-100 text-xs transition-colors ${
                  selected ? 'bg-sky-50/70 text-sky-950 font-medium' : 'hover:bg-slate-50'
                }`}
              >
                <td className="px-4 py-2">
                  <span
                    className={`inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] font-bold uppercase ${status.chip}`}
                  >
                    <Icon className={`h-2.5 w-2.5 ${live ? 'animate-spin' : ''}`} />
                    {status.label}
                  </span>
                </td>
                <td className="px-3 py-2 font-mono text-[11px] text-slate-600">
                  {run.id.slice(0, 8)}
                  <span className="ml-1.5 rounded bg-slate-100 px-1 text-[9px] uppercase text-slate-500">
                    {run.kind}
                  </span>
                </td>
                <td className="px-3 py-2 text-[11px] text-slate-500">{fmtWhen(run.started_at)}</td>
                <td className="px-3 py-2 font-mono text-[11px] text-slate-500">
                  {fmtDuration(run.duration_seconds)}
                </td>
                <td className="px-3 py-2">
                  <span className="flex flex-wrap items-center gap-1">
                    {Object.entries(run.step_counts ?? {}).map(([key, count]) => (
                      <span
                        key={key}
                        className="rounded bg-slate-100 px-1 text-[9px] font-bold uppercase text-slate-500"
                        title={key}
                      >
                        {key.slice(0, 4)} {count}
                      </span>
                    ))}
                    {!Object.keys(run.step_counts ?? {}).length && (
                      <span className="text-[11px] text-slate-300">—</span>
                    )}
                  </span>
                </td>
                <td className="px-3 py-2 text-[11px] text-slate-500">
                  {run.triggered_by || (run.kind === "scheduled" ? "schedule" : "operator")}
                </td>
                <td className="px-4 py-2 text-right">
                  <span className="inline-flex items-center gap-1">
                    {live && (
                      <button
                        type="button"
                        onClick={(event) => {
                          event.stopPropagation();
                          onCancel(run);
                        }}
                        className="rounded border border-slate-300 px-1.5 py-0.5 text-[10px] font-semibold text-slate-600 hover:bg-white"
                      >
                        Cancel
                      </button>
                    )}
                    <ExternalLink className="h-3 w-3 text-slate-300" />
                  </span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};
