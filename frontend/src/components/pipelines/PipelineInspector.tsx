import React, { useEffect, useState } from 'react';
import { AlertCircle, Plus, RefreshCw, ScrollText, Terminal, Trash2, X } from 'lucide-react';
import type {
  EdgeCondition,
  PipelineStepDef,
  PipelineTargets,
  StepRunItem,
  StepType,
} from '../../lib/types';
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
  /** Names of the direct parents, used to suggest the tokens this step may read. */
  parents?: string[];
  /** Edges touching this step, so they can be re-conditioned or removed here. */
  connections?: Array<{
    direction: 'in' | 'out';
    from_key: string;
    to_key: string;
    condition: EdgeCondition;
    name: string;
  }>;
  onEditEdge?: (fromKey: string, toKey: string) => void;
  onRemoveEdge?: (fromKey: string, toKey: string) => void;
  onChange: (patch: Partial<PipelineStepDef>) => void;
  onDelete: () => void;
  onRerunFrom?: () => void;
  onOpenLogs?: () => void;
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
    {hint && <span className="mt-1 block text-[10px] leading-snug text-slate-400">{hint}</span>}
  </label>
);

const inputClass =
  'w-full rounded-lg border border-slate-300 px-2.5 py-1.5 text-xs focus:border-indigo-500 focus:outline-hidden';

const token = (name: string, output: string) => `{{steps.${name}.${output}}}`;

const slug = (value: string) => value.replace(/[^A-Za-z0-9_]+/g, '_');

/** Shows a captured output; sensitive ones stay masked until asked for. */
const OutputValue: React.FC<{ name: string; output: { type: string; value: unknown }; sensitive: boolean }> = ({
  name,
  output,
  sensitive,
}) => {
  const [revealed, setRevealed] = useState(false);
  const hidden = sensitive && !revealed;
  const rendered =
    output.value == null
      ? 'null'
      : typeof output.value === 'string'
        ? output.value
        : JSON.stringify(output.value);

  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 px-2 py-1.5">
      <div className="flex items-center justify-between gap-2">
        <span className="font-mono text-[10px] font-bold text-slate-600">{name}</span>
        <span className="rounded bg-white px-1 text-[9px] font-bold uppercase text-slate-400">
          {output.type}
        </span>
      </div>
      <p className="mt-0.5 break-words font-mono text-[10px] leading-snug text-slate-700">
        {hidden ? '••••••••' : rendered}
      </p>
      {sensitive && (
        <button
          type="button"
          onClick={() => setRevealed((current) => !current)}
          className="mt-1 text-[10px] font-semibold text-indigo-600 hover:underline"
        >
          {revealed ? 'Hide value' : 'Reveal value'}
        </button>
      )}
    </div>
  );
};

export const PipelineInspector: React.FC<PipelineInspectorProps> = ({
  step,
  runtime,
  stepRun,
  editable,
  targets,
  parents,
  connections,
  onEditEdge,
  onRemoveEdge,
  onChange,
  onDelete,
  onRerunFrom,
  onOpenLogs,
}) => {
  const [argsText, setArgsText] = useState("");
  const [newOutput, setNewOutput] = useState("");

  useEffect(() => {
    setArgsText(
      step?.args && Object.keys(step.args).length ? JSON.stringify(step.args, null, 2) : "",
    );
    setNewOutput("");
  }, [step?.key, step?.args]);

  if (!step) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-2 p-6 text-center">
        <Terminal className="h-5 w-5 text-slate-300" />
        <p className="max-w-[220px] text-xs leading-relaxed text-slate-400">
          Select a step to edit it, or drag from a step&apos;s right port to the next step to
          add a dependency.
        </p>
      </div>
    );
  }

  const args = step.args ?? {};
  const setArgs = (patch: Record<string, unknown>) =>
    onChange({ args: { ...args, ...patch } });
  const dropArg = (key: string) => {
    const next = { ...args };
    delete next[key];
    onChange({ args: next });
  };

  const declared = (args.outputs as Record<string, { type?: string; sensitive?: boolean }>) ?? {};
  const materialize = args.materialize as { layer?: string; name?: string } | undefined;
  const isRawSql = step.step_type === "raw_sql";
  const isDbt = step.step_type === "dbt_run" || step.step_type === "dbt_test";
  const isProcess = step.step_type === "script" || step.step_type === "notebook";
  const style = statusStyle(runtime?.status);

  const targetOptions =
    step.step_type === "script"
      ? targets?.scripts ?? []
      : step.step_type === "notebook"
        ? targets?.notebooks ?? []
        : isDbt
          ? ["*", ...(targets?.dbt_directories ?? []), ...(targets?.dbt_models ?? [])]
          : [];

  const available = [
    ...Object.entries(declared).map(([name, spec]) => ({
      name,
      type: spec?.type === "table" ? "table" : "scalar",
      derived: false,
    })),
    ...(materialize ? [{ name: "table", type: "table", derived: true }] : []),
    ...(isDbt ? [{ name: "dbt_summary", type: "scalar", derived: true }] : []),
  ];

  const addOutput = () => {
    const name = slug(newOutput.trim());
    if (!name || declared[name] || name === "table") return;
    setArgs({ outputs: { ...declared, [name]: { type: "scalar" } } });
    setNewOutput("");
  };

  return (
    <div className="flex h-full flex-col">
      <div className="border-b border-slate-200 px-4 py-3">
        <div className="flex items-center justify-between gap-2">
          <p className="truncate text-sm font-semibold text-slate-800">{step.name}</p>
          {runtime?.status && (
            <span className={`shrink-0 rounded px-1.5 py-0.5 text-[10px] font-bold uppercase ${style.chip}`}>
              {style.text}
            </span>
          )}
        </div>
        <p className="text-[11px] font-mono uppercase tracking-wide text-slate-400">
          {stepTypeLabel(step.step_type)}
          {step.key ? ` · ${step.key.slice(0, 8)}` : ""}
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
                step.step_type === "webhook"
                  ? "Full URL. {param} and {steps.name.output} are substituted at run time."
                  : isRawSql
                    ? "DuckDB SQL. {{param}} for parameters, {{steps.name.table}} for a parent table, {{steps.name.output}} for a parent value."
                    : isDbt
                      ? "dbt selector: *, model name, model+ or models/folder"
                      : "File name inside git_repo/"
              }
            >
              {isRawSql ? (
                <textarea
                  className={`${inputClass} h-32 font-mono`}
                  value={step.target}
                  onChange={(event) => onChange({ target: event.target.value })}
                />
              ) : targetOptions.length && step.step_type !== "webhook" ? (
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

            {step.step_type === "webhook" && (
              <Field label="Request body (JSON)" hint="Use {param} placeholders for parameters.">
                <textarea
                  className={`${inputClass} h-24 font-mono`}
                  value={argsText}
                  placeholder={JSON.stringify({ message: "run {label}" })}
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
                  value={step.timeout_seconds ?? ""}
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
                  value={step.max_retries ?? ""}
                  placeholder="inherit"
                  onChange={(event) =>
                    onChange({
                      max_retries: event.target.value ? Number(event.target.value) : null,
                    })
                  }
                />
              </Field>
            </div>
            <div className="rounded-lg border border-slate-200">
              <div className="flex items-center justify-between border-b border-slate-100 px-2.5 py-1.5">
                <span className="text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
                  Connections
                </span>
                <span className="text-[10px] text-slate-400">
                  {connections?.length ?? 0}
                </span>
              </div>
              <div className="space-y-1 p-2">
                {(connections ?? []).map((edge) => (
                  <div
                    key={`${edge.direction}-${edge.from_key}-${edge.to_key}`}
                    className="flex items-center gap-1.5 rounded-md bg-slate-50 px-1.5 py-1"
                  >
                    <span className="w-3 shrink-0 text-center font-mono text-[10px] text-slate-400">
                      {edge.direction === 'in' ? '?' : '?'}
                    </span>
                    <span className="min-w-0 flex-1 truncate text-[11px] text-slate-700">
                      {edge.name}
                    </span>
                    <button
                      type="button"
                      onClick={() => onEditEdge?.(edge.from_key, edge.to_key)}
                      className="shrink-0 rounded border border-slate-300 bg-white px-1 text-[9px] font-bold uppercase text-slate-500 hover:border-indigo-300 hover:text-indigo-600"
                      title="Change when this edge runs"
                    >
                      {edge.condition}
                    </button>
                    <button
                      type="button"
                      onClick={() => onRemoveEdge?.(edge.from_key, edge.to_key)}
                      className="shrink-0 rounded p-0.5 text-slate-400 hover:bg-rose-50 hover:text-rose-600"
                      aria-label={`Remove connection to ${edge.name}`}
                    >
                      <X className="h-3 w-3" />
                    </button>
                  </div>
                ))}
                {!(connections?.length) && (
                  <p className="px-0.5 py-1 text-[10px] leading-snug text-slate-400">
                    No connections. Drag from this step&apos;s right port to a step to add
                    one, then click the line to change or remove it.
                  </p>
                )}
              </div>
            </div>

            <div className="rounded-lg border border-slate-200">
              <div className="flex items-center justify-between border-b border-slate-100 px-2.5 py-1.5">
                <span className="text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
                  Outputs
                </span>
                <span className="text-[10px] text-slate-400">{available.length} declared</span>
              </div>
              <div className="space-y-2 p-2.5">
                {isRawSql && (
                  <div className="space-y-1.5">
                    <label className="flex items-center gap-1.5 text-[11px] font-semibold text-slate-700">
                      <input
                        type="checkbox"
                        checked={Boolean(materialize)}
                        onChange={(event) =>
                          event.target.checked
                            ? setArgs({ materialize: { layer: "bronze", name: "" } })
                            : dropArg("materialize")
                        }
                      />
                      Write the result to a lakehouse table
                    </label>
                    {materialize && (
                      <div className="grid grid-cols-[1fr_1.4fr] gap-1.5">
                        <input
                          className={inputClass}
                          value={materialize.layer ?? "bronze"}
                          aria-label="Layer"
                          placeholder="layer"
                          onChange={(event) =>
                            setArgs({
                              materialize: { ...materialize, layer: event.target.value },
                            })
                          }
                        />
                        <input
                          className={`${inputClass} font-mono`}
                          value={materialize.name ?? ""}
                          aria-label="Table name"
                          placeholder="table_name"
                          onChange={(event) =>
                            setArgs({
                              materialize: { ...materialize, name: event.target.value },
                            })
                          }
                        />
                      </div>
                    )}
                    {materialize?.name && (
                      <p className="rounded bg-slate-50 px-1.5 py-1 font-mono text-[10px] text-slate-600">
                        {`${materialize.layer || "bronze"}.${materialize.name}`}
                      </p>
                    )}
                  </div>
                )}

                {Object.entries(declared).map(([name, spec]) => (
                  <div key={name} className="flex items-center gap-1.5">
                    <span className="min-w-0 flex-1 truncate font-mono text-[11px] text-slate-700">
                      {name}
                    </span>
                    <select
                      className="rounded border border-slate-300 px-1 py-0.5 text-[10px]"
                      value={spec?.type === "table" ? "table" : "scalar"}
                      aria-label={`Type of ${name}`}
                      onChange={(event) =>
                        setArgs({
                          outputs: {
                            ...declared,
                            [name]: { ...spec, type: event.target.value },
                          },
                        })
                      }
                    >
                      <option value="scalar">scalar</option>
                      <option value="table" disabled={!materialize}>
                        table
                      </option>
                    </select>
                    <label
                      className="flex items-center gap-1 text-[10px] text-slate-500"
                      title="Hide this value behind a reveal in the run view"
                    >
                      <input
                        type="checkbox"
                        checked={Boolean(spec?.sensitive)}
                        onChange={(event) =>
                          setArgs({
                            outputs: {
                              ...declared,
                              [name]: { ...spec, sensitive: event.target.checked },
                            },
                          })
                        }
                      />
                      secret
                    </label>
                    <button
                      type="button"
                      onClick={() => {
                        const next = { ...declared };
                        delete next[name];
                        setArgs({ outputs: next });
                      }}
                      className="rounded p-0.5 text-slate-400 hover:bg-rose-50 hover:text-rose-600"
                      aria-label={`Remove output ${name}`}
                    >
                      <X className="h-3 w-3" />
                    </button>
                  </div>
                ))}

                {isProcess && (
                  <div className="flex items-center gap-1">
                    <input
                      className={`${inputClass} py-1 font-mono`}
                      value={newOutput}
                      placeholder="output_name"
                      onChange={(event) => setNewOutput(event.target.value)}
                      onKeyDown={(event) => {
                        if (event.key === "Enter") {
                          event.preventDefault();
                          addOutput();
                        }
                      }}
                    />
                    <button
                      type="button"
                      onClick={addOutput}
                      className="flex items-center gap-0.5 rounded-lg border border-slate-300 px-1.5 py-1 text-[10px] font-semibold text-slate-600 hover:bg-slate-50"
                    >
                      <Plus className="h-3 w-3" /> Add
                    </button>
                  </div>
                )}
                {available.length > 0 && (
                  <div className="space-y-1 rounded-lg bg-slate-50 px-2 py-1.5">
                    <p className="text-[10px] font-semibold text-slate-500">
                      Children can read these:
                    </p>
                    {available.map((entry) => (
                      <code
                        key={entry.name}
                        className="block truncate font-mono text-[10px] text-indigo-700"
                      >
                        {token(slug(step.name), entry.name)}
                        {entry.derived ? "  (automatic)" : "  (below, in the child)"}
                      </code>
                    ))}
                    {isProcess && (
                      <p className="text-[10px] leading-snug text-slate-500">
                        Write JSON to <code className="font-mono">os.environ["OBELISK_OUTPUT_FILE"]</code>;
                        direct parents also arrive as{" "}
                        <code className="font-mono">OBELISK_STEP_&lt;STEP&gt;_&lt;OUTPUT&gt;</code>.
                      </p>
                    )}
                    {!parents?.length && (
                      <p className="text-[10px] leading-snug text-slate-400">
                        No parent steps yet, so nothing flows in.
                      </p>
                    )}
                  </div>
                )}
                {available.length > 0 && slug(step.name) !== step.name && (
                  <p className="rounded bg-slate-50 px-1.5 py-1 text-[10px] leading-snug text-slate-500">
                    Children address this step as{" "}
                    <code className="font-mono">{slug(step.name)}</code>.
                  </p>
                )}
              </div>
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
              <div className="flex justify-between gap-2">
                <dt className="text-slate-500">Target</dt>
                <dd className="max-w-[62%] truncate font-mono text-slate-700">
                  {step.target || "—"}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-slate-500">Attempt</dt>
                <dd className="font-mono text-slate-700">{stepRun?.attempt ?? 1}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-slate-500">Duration</dt>
                <dd className="font-mono text-slate-700">
                  {stepRun?.duration_seconds != null ? `${stepRun.duration_seconds}s` : "—"}
                </dd>
              </div>
            </dl>

            {stepRun?.error && (
              <div className="flex gap-1.5 rounded-lg bg-rose-50 p-2 text-[11px] text-rose-700">
                <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                <span className="break-words">{stepRun.error}</span>
              </div>
            )}

            {stepRun?.outputs && Object.keys(stepRun.outputs).length > 0 && (
              <div className="space-y-1.5">
                <p className="text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
                  Captured outputs
                </p>
                {Object.entries(stepRun.outputs).map(([name, output]) => (
                  <OutputValue
                    key={name}
                    name={name}
                    output={output}
                    sensitive={Boolean(declared[name]?.sensitive)}
                  />
                ))}
              </div>
            )}

            {onOpenLogs && (
              <button
                type="button"
                onClick={onOpenLogs}
                className="flex w-full items-center justify-center gap-1.5 rounded-lg border border-slate-300 px-2 py-1.5 text-[11px] font-semibold text-slate-700 hover:bg-slate-50"
              >
                <ScrollText className="h-3.5 w-3.5" /> Open logs
              </button>
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
          </>
        )}
      </div>
    </div>
  );
};
