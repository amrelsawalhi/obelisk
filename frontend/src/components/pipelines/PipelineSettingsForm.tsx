import React, { useEffect, useMemo, useState } from 'react';
import { Loader2, Save, Trash2 } from 'lucide-react';
import type { CreatePipelinePayload, OverlapPolicy, PipelineDetail } from '../../lib/types';

const CRON_PRESETS: Array<{ label: string; expr: string }> = [
  { label: 'Every 15 min', expr: '*/15 * * * *' },
  { label: 'Hourly', expr: '0 * * * *' },
  { label: 'Nightly 02:00', expr: '0 2 * * *' },
  { label: 'Weekdays 06:00', expr: '0 6 * * 1-5' },
];

const inputClass =
  'w-full rounded-lg border border-slate-300 px-2.5 py-1.5 text-xs focus:border-indigo-500 focus:outline-hidden';

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
    {hint && <span className="mt-1 block text-[10px] leading-snug text-slate-400">{hint}</span>}
  </label>
);

type FormState = {
  name: string;
  description: string;
  cron_expression: string;
  timezone: string;
  is_enabled: boolean;
  overlap_policy: OverlapPolicy;
  max_parallel: number;
  keep_runs: number;
  timeout_seconds: number;
  max_retries: number;
  retry_delay_seconds: number;
  auto_rerun_interrupted: boolean;
};

const formFrom = (pipeline: PipelineDetail): FormState => ({
  name: pipeline.name,
  description: pipeline.description ?? '',
  cron_expression: pipeline.cron_expression,
  timezone: pipeline.timezone,
  is_enabled: pipeline.is_enabled,
  overlap_policy: pipeline.overlap_policy,
  max_parallel: pipeline.max_parallel,
  keep_runs: pipeline.keep_runs,
  timeout_seconds: pipeline.timeout_seconds,
  max_retries: pipeline.max_retries,
  retry_delay_seconds: pipeline.retry_delay_seconds,
  auto_rerun_interrupted: pipeline.auto_rerun_interrupted,
});

interface PipelineSettingsFormProps {
  pipeline: PipelineDetail;
  saving: boolean;
  onSave: (patch: Partial<CreatePipelinePayload>) => Promise<void>;
  onDelete: () => void;
}

/**
 * Schedule and execution policy, editable in one place.
 *
 * These fields used to live only in the API; the screen showed them as a subtitle.
 * An operator who needed "keep 5 runs, not 20" had to reach for curl.
 */
export const PipelineSettingsForm: React.FC<PipelineSettingsFormProps> = ({
  pipeline,
  saving,
  onSave,
  onDelete,
}) => {
  const [form, setForm] = useState<FormState>(() => formFrom(pipeline));

  useEffect(() => setForm(formFrom(pipeline)), [pipeline.id, pipeline.updated_at]);

  const dirty = useMemo(
    () => JSON.stringify(form) !== JSON.stringify(formFrom(pipeline)),
    [form, pipeline],
  );

  const patch = (update: Partial<FormState>) => setForm((current) => ({ ...current, ...update }));

  return (
    <div className="h-full overflow-y-auto">
      <div className="mx-auto max-w-3xl space-y-5 px-6 py-5">
        <section className="space-y-3">
          <h3 className="text-[11px] font-bold uppercase tracking-wider text-slate-400">
            Identity
          </h3>
          <Field label="Name">
            <input
              className={inputClass}
              value={form.name}
              onChange={(event) => patch({ name: event.target.value })}
            />
          </Field>
          <Field label="Description" hint="What this pipeline produces, and who depends on it.">
            <textarea
              className={`${inputClass} h-16`}
              value={form.description}
              onChange={(event) => patch({ description: event.target.value })}
            />
          </Field>
        </section>

        <section className="space-y-3">
          <h3 className="text-[11px] font-bold uppercase tracking-wider text-slate-400">
            Schedule
          </h3>
          <div className="grid grid-cols-[1.2fr_1fr] gap-2">
            <Field label="Cron" hint="Five fields: minute hour day month weekday.">
              <input
                className={`${inputClass} font-mono`}
                value={form.cron_expression}
                onChange={(event) => patch({ cron_expression: event.target.value })}
              />
            </Field>
            <Field label="Timezone" hint="IANA name, e.g. Europe/Berlin.">
              <input
                className={`${inputClass} font-mono`}
                value={form.timezone}
                onChange={(event) => patch({ timezone: event.target.value })}
              />
            </Field>
          </div>
          <div className="flex flex-wrap gap-1.5">
            {CRON_PRESETS.map((preset) => (
              <button
                key={preset.expr}
                type="button"
                onClick={() => patch({ cron_expression: preset.expr })}
                className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold ${
                  form.cron_expression === preset.expr
                    ? 'border-indigo-300 bg-indigo-50 text-indigo-700'
                    : 'border-slate-300 text-slate-600 hover:bg-slate-50'
                }`}
              >
                {preset.label}
              </button>
            ))}
          </div>
          <label className="flex items-center gap-2 text-xs text-slate-700">
            <input
              type="checkbox"
              checked={form.is_enabled}
              onChange={(event) => patch({ is_enabled: event.target.checked })}
            />
            Schedule is active
            {pipeline.next_run_at && form.is_enabled && (
              <span className="text-[11px] text-slate-400">
                next run {new Date(pipeline.next_run_at).toLocaleString()}
              </span>
            )}
          </label>
        </section>

        <section className="space-y-3">
          <h3 className="text-[11px] font-bold uppercase tracking-wider text-slate-400">
            Execution
          </h3>
          <div className="grid grid-cols-3 gap-2">
            <Field label="Overlap" hint="What a second trigger does.">
              <select
                className={inputClass}
                value={form.overlap_policy}
                onChange={(event) =>
                  patch({ overlap_policy: event.target.value as OverlapPolicy })
                }
              >
                <option value="skip">skip</option>
                <option value="queue">queue</option>
                <option value="kill">kill</option>
              </select>
            </Field>
            <Field label="Max parallel">
              <input
                type="number"
                min={1}
                className={inputClass}
                value={form.max_parallel}
                onChange={(event) => patch({ max_parallel: Number(event.target.value) || 1 })}
              />
            </Field>
            <Field label="Keep runs">
              <input
                type="number"
                min={1}
                className={inputClass}
                value={form.keep_runs}
                onChange={(event) => patch({ keep_runs: Number(event.target.value) || 1 })}
              />
            </Field>
          </div>
          <div className="grid grid-cols-3 gap-2">
            <Field label="Step timeout (s)">
              <input
                type="number"
                min={1}
                className={inputClass}
                value={form.timeout_seconds}
                onChange={(event) =>
                  patch({ timeout_seconds: Number(event.target.value) || 3600 })
                }
              />
            </Field>
            <Field label="Retries">
              <input
                type="number"
                min={0}
                className={inputClass}
                value={form.max_retries}
                onChange={(event) => patch({ max_retries: Number(event.target.value) || 0 })}
              />
            </Field>
            <Field label="Retry delay (s)">
              <input
                type="number"
                min={0}
                className={inputClass}
                value={form.retry_delay_seconds}
                onChange={(event) =>
                  patch({ retry_delay_seconds: Number(event.target.value) || 0 })
                }
              />
            </Field>
          </div>
          <label className="flex items-center gap-2 text-xs text-slate-700">
            <input
              type="checkbox"
              checked={form.auto_rerun_interrupted}
              onChange={(event) => patch({ auto_rerun_interrupted: event.target.checked })}
            />
            Re-run automatically after an interrupted run
          </label>
        </section>

        <div className="flex items-center justify-between gap-2 border-t border-slate-200 pt-4">
          <button
            type="button"
            onClick={onDelete}
            className="flex items-center gap-1.5 rounded-lg border border-rose-200 px-2.5 py-1.5 text-[11px] font-semibold text-rose-600 hover:bg-rose-50"
          >
            <Trash2 className="h-3.5 w-3.5" /> Delete pipeline
          </button>
          <div className="flex items-center gap-2">
            {dirty && <span className="text-[11px] text-amber-600">Unsaved changes</span>}
            <button
              type="button"
              disabled={!dirty || saving}
              onClick={() => void onSave(form)}
              className="flex items-center gap-1.5 rounded-lg bg-indigo-600 px-3 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700 disabled:opacity-40"
            >
              {saving ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Save className="h-3.5 w-3.5" />
              )}
              Save settings
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};
