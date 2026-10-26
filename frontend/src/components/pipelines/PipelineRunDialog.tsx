import React, { useEffect, useMemo, useState } from 'react';
import { Loader2, Play, RotateCcw } from 'lucide-react';
import type { PipelineDetail, PipelineParamDef } from '../../lib/types';

const storageKey = (pipelineId: string) => `obelisk.pipelines.params.${pipelineId}`;

const defaults = (params: PipelineParamDef[]): Record<string, string> =>
  Object.fromEntries(params.map((param) => [param.name, param.default_value ?? '']));

/** Values are kept as strings in the form and coerced on submit, like HTML inputs. */
const coerce = (params: PipelineParamDef[], values: Record<string, string>) => {
  const payload: Record<string, unknown> = {};
  for (const param of params) {
    const raw = (values[param.name] ?? '').trim();
    if (!raw) continue;
    if (param.type === 'int') payload[param.name] = Number.parseInt(raw, 10);
    else if (param.type === 'float') payload[param.name] = Number.parseFloat(raw);
    else if (param.type === 'bool') payload[param.name] = raw === 'true';
    else payload[param.name] = raw;
  }
  return payload;
};

const inputClass =
  'w-full rounded-lg border border-slate-300 px-3 py-1.5 text-xs text-slate-800 placeholder-slate-400 focus:border-sky-500 focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 transition-colors';

interface PipelineRunDialogProps {
  pipeline: PipelineDetail;
  open: boolean;
  running: boolean;
  onClose: () => void;
  onRun: (params: Record<string, unknown>) => void;
}

/**
 * Run inputs.
 *
 * Required parameters previously had no way to be supplied: the header button always
 * started a run with no overrides, so the engine answered "Parameter 'x' is required."
 * and the pipeline was un-runnable from the UI. Values are remembered per pipeline in
 * ``localStorage`` — the "same scope as yesterday" case, without a presets table nobody
 * asked for yet.
 */
export const PipelineRunDialog: React.FC<PipelineRunDialogProps> = ({
  pipeline,
  open,
  running,
  onClose,
  onRun,
}) => {
  const params = pipeline.params;
  const [values, setValues] = useState<Record<string, string>>(() => defaults(params));
  const [touched, setTouched] = useState(false);

  useEffect(() => {
    if (!open) return;
    let remembered: Record<string, string> = {};
    try {
      remembered = JSON.parse(window.localStorage.getItem(storageKey(pipeline.id)) ?? '{}');
    } catch {
      remembered = {};
    }
    setValues({ ...defaults(params), ...remembered });
    setTouched(false);
  }, [open, pipeline.id, params]);

  const missing = useMemo(
    () =>
      params
        .filter((param) => param.required && !(values[param.name] ?? "").trim())
        .map((param) => param.name),
    [params, values],
  );

  if (!open) return null;

  const submit = () => {
    setTouched(true);
    if (missing.length) return;
    const payload = coerce(params, values);
    try {
      window.localStorage.setItem(storageKey(pipeline.id), JSON.stringify(values));
    } catch {
      /* private mode: a remembered value is not worth blocking a run */
    }
    onRun(payload);
  };

  return (
    <div className="fixed inset-0 z-[75] flex items-center justify-center bg-slate-900/40 backdrop-blur-xs p-4 animate-in fade-in duration-150">
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Run pipeline"
        className="w-[440px] max-w-full rounded-xl border border-slate-200 bg-white shadow-2xl overflow-hidden"
      >
        <div className="border-b border-slate-100 bg-slate-50/50 px-5 py-3.5">
          <h2 className="text-sm font-bold text-slate-900">Run {pipeline.name}</h2>
          <p className="mt-0.5 text-xs text-slate-500">
            Values apply to this run only. Scheduled runs use the defaults.
          </p>
        </div>

        <div className="max-h-[60vh] space-y-2.5 overflow-y-auto px-4 py-3">
          {params.map((param) => {
            const invalid = touched && param.required && !(values[param.name] ?? "").trim();
            return (
              <label key={param.name} className="block">
                <span className="mb-1 flex items-center gap-1 text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
                  {param.name}
                  <span className="font-sans text-[9px] font-normal normal-case text-slate-400">
                    {param.type}
                  </span>
                  {param.required && <span className="text-rose-500">*</span>}
                </span>
                {param.type === "bool" ? (
                  <select
                    className={inputClass}
                    value={values[param.name] || "false"}
                    onChange={(event) =>
                      setValues((current) => ({ ...current, [param.name]: event.target.value }))
                    }
                  >
                    <option value="true">true</option>
                    <option value="false">false</option>
                  </select>
                ) : (
                  <input
                    className={`${inputClass} ${invalid ? "border-rose-400" : ""}`}
                    type={param.type === "int" || param.type === "float" ? "number" : "text"}
                    value={values[param.name] ?? ""}
                    placeholder={param.default_value ?? ""}
                    onChange={(event) =>
                      setValues((current) => ({ ...current, [param.name]: event.target.value }))
                    }
                    onKeyDown={(event) => {
                      if (event.key === "Enter") {
                        event.preventDefault();
                        submit();
                      }
                    }}
                  />
                )}
                {invalid && (
                  <span className="mt-1 block text-[10px] text-rose-600">
                    This parameter is required.
                  </span>
                )}
              </label>
            );
          })}
        </div>

        <div className="flex items-center justify-between gap-2 border-t border-slate-100 bg-slate-50/50 px-5 py-3">
          <button
            type="button"
            onClick={() => setValues(defaults(params))}
            className="flex items-center gap-1 text-xs font-medium text-slate-500 hover:text-slate-800 transition-colors"
          >
            <RotateCcw className="h-3 w-3" /> Reset to defaults
          </button>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={onClose}
              className="rounded-lg border border-slate-300 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-50 transition-colors shadow-2xs"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={submit}
              disabled={running}
              className="flex items-center gap-1.5 rounded-lg bg-sky-600 px-3.5 py-1.5 text-xs font-semibold text-white hover:bg-sky-700 transition-colors shadow-2xs disabled:opacity-50"
            >
              {running ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Play className="h-3.5 w-3.5 fill-current" />
              )}
              Run now
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};
