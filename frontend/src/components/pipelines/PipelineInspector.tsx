import React, { useEffect, useState } from 'react';
import { AlertCircle, RefreshCw, Trash2 } from 'lucide-react';
import type { PipelineStepDef, PipelineTargets, StepRunItem, StepType } from '../../lib/types';
import { statusStyle, stepTypeLabel } from './PipelineCanvas';
import type { StepRuntime } from './PipelineCanvas';

const TYPE_OPTIONS: StepType[] = [
  'dbt_run',
  'dbt_test',
  'script',
  'notebook',
  'webhook',
  'raw_sql',
];

interface PipelineInspectorProps {
  step: PipelineStepDef | null;
  runtime?: StepRuntime;
  stepRun?: StepRunItem | null;
  editable: boolean;
  targets: PipelineTargets | null;
  logs?: string;
  loadingLogs?: boolean;
  onChange: (patch: Partial<PipelineStepDef>) => void;
  onDelete: () => void;
  onRerunFrom?: () => void;
}

const Field: React.FC<{ label: string; hint?: string; children: React.ReactNode }> = ({
  label,
  hint,
  children,
}) => (
  <label className="block">
    <span className="mb-1 block text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
      {label}
    </span>
    {children}
    {hint && <span className="mt-1 block text-[10px] text-slate-400">{hint}</span>}
  </label>
);

const inputClass =
  'w-full rounded-lg border border-slate-300 px-2.5 py-1.5 text-xs focus:border-indigo-500 focus:outline-hidden';

export const PipelineInspector: React.FC<PipelineInspectorProps> = ({
  step,
  runtime,
  stepRun,
  editable,
  targets,
  logs,
  loadingLogs,
  onChange,
  onDelete,
  onRerunFrom,
}) => {
  const [argsText, setArgsText] = useState('');

  useEffect(() => {
    setArgsText(
      step?.args && Object.keys(step.args).length ? JSON.stringify(step.args, null, 2) : '',
    );
  }, [step?.key, step?.args]);

  if (!step) {
    return (
      <div className="flex h-full items-center justify-center p-6 text-center">
        <p className="max-w-[220px] text-xs text-slate-400">
          Select a step on the canvas to edit it, or drag from a step&apos;s right port to the
          next step to add a dependency.
        </p>
      </div>
    );
  }

  const targetOptions =
    step.step_type === 'script'
      ? targets?.scripts ?? []
      : step.step_type === 'notebook'
        ? targets?.notebooks ?? []
        : step.step_type === 'dbt_run' || step.step_type === 'dbt_test'
          ? ['*', ...(targets?.dbt_directories ?? []), ...(targets?.dbt_models ?? [])]
          : [];

  const style = statusStyle(runtime?.status);

  return (
    <div className="flex h-full flex-col">
      <div className="border-b border-slate-200 px-4 py-3">
        <div className="flex items-center justify-between">
          <p className="text-sm font-semibold text-slate-800">{step.name}</p>
          {runtime?.status && (
            <span className={`rounded px-1.5 py-0.5 text-[10px] font-bold uppercase ${style.chip}`}>
              {style.text}
            </span>
          )}
        </div>
        <p className="text-[11px] font-mono uppercase tracking-wide text-slate-400">
          {stepTypeLabel(step.step_type)}
        </p>
      </div>

      <div className="flex-1 space-y-3 overflow-y-auto px-4 py-3">
        {editable ? (
          <>
            <Field label="Step name">
              <input
                className={inputClass}
                value={step.name}
                onChange={(event) => onChange({ name: event.target.value })}
              />
            </Field>

            <Field label="Step type">
              <select
                className={inputClass}
                value={step.step_type}
                onChange={(event) => onChange({ step_type: event.target.value as StepType })}
              >
                {TYPE_OPTIONS.map((type) => (
                  <option key={type} value={type}>
                    {stepTypeLabel(type)}
                  </option>
                ))}
              </select>
            </Field>

            <Field
              label="Target"
              hint={
                step.step_type === 'webhook'
                  ? 'Full URL, e.g. https://hooks.slack.com/...'
                  : step.step_type === 'raw_sql'
                    ? 'DuckDB SQL. {{param}} is replaced with the pipeline parameter.'
                    : step.step_type === 'dbt_run' || step.step_type === 'dbt_test'
                      ? 'dbt selector: *, model name, model+ or models/folder'
                      : 'File name inside git_repo/'
              }
            >
              {step.step_type === 'raw_sql' ? (
                <textarea
                  className={`${inputClass} h-28 font-mono`}
                  value={step.target}
                  onChange={(event) => onChange({ target: event.target.value })}
                />
              ) : targetOptions.length && step.step_type !== 'webhook' ? (
                <select
                  className={inputClass}
                  value={step.target}
                  onChange={(event) => onChange({ target: event.target.value })}
                >
                  {!targetOptions.includes(step.target) && step.target && (
                    <option value={step.target}>{step.target} (missing)</option>
                  )}
                  {targetOptions.map((option) => (
                    <option key={option} value={option}>
                      {option}
                    </option>
                  ))}
                </select>
              ) : (
                <input
                  className={inputClass}
                  value={step.target}
                  onChange={(event) => onChange({ target: event.target.value })}
                />
              )}
            </Field>

            {step.step_type === 'webhook' && (
              <Field label="Request body (JSON)" hint="Use {param} placeholders for parameters.">
                <textarea
                  className={`${inputClass} h-24 font-mono`}
                  value={argsText}
                  placeholder='{"message": "run {label}"}'
                  onChange={(event) => {
                    setArgsText(event.target.value);
                    try {
                      onChange({
                        args: event.target.value ? JSON.parse(event.target.value) : {},
                      });
                    } catch {
                      /* keep typing; invalid JSON is simply not committed yet */
                    }
                  }}
                />
              </Field>
            )}


            <div className="grid grid-cols-2 gap-2">
              <Field label="Timeout (s)">
                <input
                  type="number"
                  min={1}
                  className={inputClass}
                  value={step.timeout_seconds ?? ''}
                  placeholder="inherit"
                  onChange={(event) =>
                    onChange({
                      timeout_seconds: event.target.value ? Number(event.target.value) : null,
                    })
                  }
                />
              </Field>
              <Field label="Retries">
                <input
                  type="number"
                  min={0}
                  className={inputClass}
                  value={step.max_retries ?? ''}
                  placeholder="inherit"
                  onChange={(event) =>
                    onChange({
                      max_retries: event.target.value ? Number(event.target.value) : null,
                    })
                  }
                />
              </Field>
            </div>

            <button
              type="button"
              onClick={onDelete}
              className="flex w-full items-center justify-center gap-1.5 rounded-lg border border-rose-200 px-2 py-1.5 text-[11px] font-semibold text-rose-600 hover:bg-rose-50"
            >
              <Trash2 className="h-3.5 w-3.5" /> Remove step
            </button>
          </>
        ) : (
          <>
            <dl className="space-y-1.5 text-xs">
              <div className="flex justify-between">
                <dt className="text-slate-500">Target</dt>
                <dd className="max-w-[60%] truncate font-mono text-slate-700">
                  {step.target || '—'}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-slate-500">Attempt</dt>
                <dd className="font-mono text-slate-700">{stepRun?.attempt ?? 1}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-slate-500">Duration</dt>
                <dd className="font-mono text-slate-700">
                  {stepRun?.duration_seconds != null ? `${stepRun.duration_seconds}s` : '—'}
                </dd>
              </div>
            </dl>

            {stepRun?.error && (
              <div className="flex gap-1.5 rounded-lg bg-rose-50 p-2 text-[11px] text-rose-700">
                <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                <span className="break-words">{stepRun.error}</span>
              </div>
            )}

            {onRerunFrom && (
              <button
                type="button"
                onClick={onRerunFrom}
                className="flex w-full items-center justify-center gap-1.5 rounded-lg bg-indigo-600 px-2 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700"
              >
                <RefreshCw className="h-3.5 w-3.5" /> Re-run from this step
              </button>
            )}

            <div>
              <p className="mb-1 text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
                Logs
              </p>
              <pre className="h-64 overflow-auto rounded-lg bg-slate-900 p-2 text-[10px] leading-relaxed text-slate-100">
                {loadingLogs && !logs ? 'Loading…' : logs || 'No output yet.'}
              </pre>
            </div>
          </>
        )}
      </div>
    </div>
  );
};

