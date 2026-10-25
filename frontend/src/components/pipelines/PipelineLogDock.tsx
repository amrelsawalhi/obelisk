import React, { useEffect, useRef, useState } from 'react';
import {
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  ChevronUp,
  Copy,
  Loader2,
  RefreshCw,
  X,
} from 'lucide-react';
import type { StepRunItem } from '../../lib/types';
import { fmtDuration, fmtWhen } from './PipelineRunsTable';

export type DockTab = 'step' | 'run';

interface PipelineLogDockProps {
  open: boolean;
  tab: DockTab;
  onTabChange: (tab: DockTab) => void;
  /** Every step run in the watched run, in execution order, for the pager. */
  steps: StepRunItem[];
  selectedStepId: string | null;
  onSelectStep: (stepId: string | null) => void;
  stepLog: string;
  runLog: string | null;
  loadingStep: boolean;
  loadingRun: boolean;
  expanded: boolean;
  onToggleExpand: () => void;
  onRefresh: () => void;
  onClose: () => void;
}

/**
 * The bottom dock: one place for output.
 *
 * Logs used to live inside the inspector, so selecting another step replaced the
 * output you were reading. Here the pager walks the run's steps while the panel stays
 * put, and the Run tab shows the interleaved whole-run log.
 */
export const PipelineLogDock: React.FC<PipelineLogDockProps> = ({
  open,
  tab,
  onTabChange,
  steps,
  selectedStepId,
  onSelectStep,
  stepLog,
  runLog,
  loadingStep,
  loadingRun,
  expanded,
  onToggleExpand,
  onRefresh,
  onClose,
}) => {
  const [wrap, setWrap] = useState(true);
  const [follow, setFollow] = useState(true);
  const [copied, setCopied] = useState(false);
  const bodyRef = useRef<HTMLPreElement>(null);

  // Follow the running step until the operator picks one explicitly: during step 1 the
  // dock used to read "Select a step" while that step's output streamed past it.
  const followedStepId =
    selectedStepId ?? steps.find((step) => step.status === 'running')?.step_id ?? null;
  const index = steps.findIndex((step) => step.step_id === followedStepId);
  const current = index >= 0 ? steps[index] : null;
  const text = tab === 'step' ? stepLog : runLog ?? '';
  const loading = tab === 'step' ? loadingStep : loadingRun;

  // Follow the tail while a step produces output, but stop as soon as the operator
  // scrolls up to read something.
  useEffect(() => {
    if (!follow) return;
    const node = bodyRef.current;
    if (node) node.scrollTop = node.scrollHeight;
  }, [text, follow, tab, open]);

  useEffect(() => {
    setCopied(false);
  }, [tab, selectedStepId]);

  if (!open) return null;

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text || '');
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1600);
    } catch {
      setCopied(false);
    }
  };

  const move = (delta: number) => {
    const next = steps[index + delta];
    if (next) onSelectStep(next.step_id);
  };

  return (
    <div className="flex h-full min-h-0 flex-col bg-white">
      <div className="flex items-center gap-1 border-b border-slate-100 px-2 py-1">
        <div className="flex items-center rounded-lg bg-slate-100 p-0.5">
          <button
            type="button"
            onClick={() => onTabChange('step')}
            className={`rounded-md px-2 py-0.5 text-[10px] font-bold uppercase ${
              tab === 'step' ? 'bg-white text-slate-700 shadow-sm' : 'text-slate-500'
            }`}
          >
            Step
          </button>
          <button
            type="button"
            onClick={() => onTabChange('run')}
            className={`rounded-md px-2 py-0.5 text-[10px] font-bold uppercase ${
              tab === 'run' ? 'bg-white text-slate-700 shadow-sm' : 'text-slate-500'
            }`}
          >
            Run
          </button>
        </div>

        {tab === 'step' && steps.length > 0 && (
          <div className="flex min-w-0 items-center gap-1">
            <button
              type="button"
              onClick={() => move(-1)}
              disabled={index <= 0}
              className="rounded p-0.5 text-slate-500 hover:bg-slate-100 disabled:opacity-30"
              aria-label="Previous step"
            >
              <ChevronLeft className="h-3.5 w-3.5" />
            </button>
            <button
              type="button"
              onClick={() => move(1)}
              disabled={index < 0 || index >= steps.length - 1}
              className="rounded p-0.5 text-slate-500 hover:bg-slate-100 disabled:opacity-30"
              aria-label="Next step"
            >
              <ChevronRight className="h-3.5 w-3.5" />
            </button>
            <span className="truncate text-[11px] font-semibold text-slate-700">
              {current?.step_name ?? 'Select a step'}
            </span>
            {current && (
              <span className="shrink-0 font-mono text-[10px] text-slate-400">
                {Math.max(index + 1, 0)}/{steps.length} · {fmtDuration(current.duration_seconds)}
                {current.status !== 'success' ? ` · ${current.status}` : ''}
              </span>
            )}
          </div>
        )}

        {tab === 'run' && (
          <span className="truncate text-[11px] font-semibold text-slate-700">
            Whole-run log
            <span className="ml-1.5 font-mono text-[10px] font-normal text-slate-400">
              merged, ordered by start time
            </span>
          </span>
        )}
        <div className="ml-auto flex items-center gap-0.5">
          {loading && <Loader2 className="h-3.5 w-3.5 animate-spin text-indigo-500" />}
          <span className="mr-1 font-mono text-[10px] text-slate-300">
            {current?.started_at ? fmtWhen(current.started_at) : ""}
          </span>
          <button
            type="button"
            onClick={onRefresh}
            className="rounded p-1 text-slate-500 hover:bg-slate-100"
            aria-label="Reload logs"
            title="Reload"
          >
            <RefreshCw className="h-3.5 w-3.5" />
          </button>
          <button
            type="button"
            onClick={copy}
            className="rounded p-1 text-slate-500 hover:bg-slate-100"
            aria-label="Copy logs"
            title={copied ? "Copied" : "Copy"}
          >
            <Copy className={`h-3.5 w-3.5 ${copied ? "text-emerald-600" : ""}`} />
          </button>
          <button
            type="button"
            onClick={() => setWrap((value) => !value)}
            className={`rounded px-1 py-0.5 font-mono text-[10px] ${wrap ? "text-indigo-600" : "text-slate-400"} hover:bg-slate-100`}
            title="Toggle line wrapping"
          >
            wrap
          </button>
          <button
            type="button"
            onClick={() => setFollow((value) => !value)}
            className={`rounded px-1 py-0.5 font-mono text-[10px] ${follow ? "text-indigo-600" : "text-slate-400"} hover:bg-slate-100`}
            title="Follow new output"
          >
            tail
          </button>
          <button
            type="button"
            onClick={onToggleExpand}
            className="rounded p-1 text-slate-500 hover:bg-slate-100"
            aria-label={expanded ? "Collapse logs" : "Expand logs"}
          >
            {expanded ? (
              <ChevronDown className="h-3.5 w-3.5" />
            ) : (
              <ChevronUp className="h-3.5 w-3.5" />
            )}
          </button>
          <button
            type="button"
            onClick={onClose}
            className="rounded p-1 text-slate-500 hover:bg-slate-100"
            aria-label="Close logs"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>

      <pre
        ref={bodyRef}
        onScroll={(event) => {
          const node = event.currentTarget;
          const atBottom = node.scrollHeight - node.scrollTop - node.clientHeight < 24;
          if (atBottom !== follow) setFollow(atBottom);
        }}
        className={`min-h-0 flex-1 overflow-auto bg-slate-950 px-3 py-2 font-mono text-[11px] leading-relaxed text-slate-100 ${
          wrap ? "whitespace-pre-wrap break-words" : "whitespace-pre"
        }`}
      >
        {text
          ? text
          : loading
            ? "Loading…"
            : tab === "step"
              ? current
                ? "This step has not written any output yet."
                : "Select a step on the canvas to read its output."
              : "Run this pipeline to see the merged log."}
      </pre>
    </div>
  );
};
