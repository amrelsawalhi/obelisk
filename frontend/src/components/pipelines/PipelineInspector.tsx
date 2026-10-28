import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  AlertCircle,
  ArrowDownToLine,
  Copy,
  CornerDownLeft,
  Database,
  Folder,
  Layers,
  Plus,
  RefreshCw,
  ScrollText,
  Search,
  Table,
  Terminal,
  Trash2,
  X,
} from 'lucide-react';
import { api } from '../../lib/api';
import type {
  CatalogNode,
  EdgeCondition,
  PipelineStepDef,
  PipelineSummary,
  PipelineTargets,
  StepRunItem,
  StepType,
} from '../../lib/types';
import { statusStyle, stepTypeLabel } from './PipelineCanvas';
import type { StepRuntime } from './PipelineCanvas';

const TYPE_OPTIONS: StepType[] = [
  'ingest',
  'dbt_run',
  'dbt_test',
  'script',
  'notebook',
  'webhook',
  'raw_sql',
  'pipeline',
];

interface PipelineInspectorProps {
  step: PipelineStepDef | null;
  runtime?: StepRuntime;
  stepRun?: StepRunItem | null;
  editable: boolean;
  targets: PipelineTargets | null;
  /** Offered as targets for a 'pipeline' step (this pipeline excluded). */
  pipelines?: PipelineSummary[];
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
  'w-full rounded-lg border border-slate-300 px-2.5 py-1.5 text-xs focus:border-sky-500 focus:ring-1 focus:ring-sky-500 focus:outline-hidden transition-all';

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
  const summary =
    name === 'dbt_summary' && output.value && typeof output.value === 'object'
      ? (output.value as Record<string, unknown>)
      : null;

  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 px-2 py-1.5">
      <div className="flex items-center justify-between gap-2">
        <span className="font-mono text-[10px] font-bold text-slate-600">{name}</span>
        <span className="rounded bg-white px-1 text-[9px] font-bold uppercase text-slate-400">
          {output.type}
        </span>
      </div>
      {summary && !hidden ? (
        <div className="mt-1 flex flex-wrap gap-1">
          {(['total', 'ok', 'failed', 'skipped'] as const).map((key) => (
            <span
              key={key}
              className={`rounded px-1 py-0.5 font-mono text-[9px] ${
                key === 'failed' && Number(summary.failed) > 0
                  ? 'bg-rose-100 text-rose-700'
                  : 'bg-white text-slate-600'
              }`}
            >
              {key} {String(summary[key] ?? 0)}
            </span>
          ))}
        </div>
      ) : (
        <p className="mt-0.5 break-words font-mono text-[10px] leading-snug text-slate-700">
          {hidden ? 'masked' : rendered}
        </p>
      )}
      {sensitive && (
        <button
          type="button"
          onClick={() => setRevealed((current) => !current)}
          className="mt-1 text-[10px] font-semibold text-sky-600 hover:underline"
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
  pipelines,
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
  const targetRef = useRef<HTMLTextAreaElement | null>(null);
  const [copiedToken, setCopiedToken] = useState<string | null>(null);
  const [catalogNodes, setCatalogNodes] = useState<CatalogNode[]>([]);
  const [tableSearch, setTableSearch] = useState("");

  useEffect(() => {
    let active = true;
    api.getCatalogTree().then((nodes) => {
      if (active) setCatalogNodes(nodes.filter((n) => n.type !== "lakehouse"));
    }).catch(() => {});
    return () => { active = false; };
  }, []);

  const connectionOptions = useMemo(() => {
    const list: Array<{ id: string; name: string; source_type: string }> = [];
    const seen = new Set<string>();
    for (const c of targets?.connections ?? []) {
      if (!seen.has(c.id)) {
        seen.add(c.id);
        list.push({ id: c.id, name: c.name, source_type: c.source_type });
      }
    }
    for (const node of catalogNodes) {
      if (!seen.has(node.id)) {
        seen.add(node.id);
        list.push({ id: node.id, name: node.name, source_type: node.type });
      }
    }
    return list;
  }, [targets?.connections, catalogNodes]);

  const stepArgs = step?.args ?? {};
  const currentConnectionId = (stepArgs.connection_id as string) || (stepArgs.connection as string) || "";
  const matchedNode = useMemo(() => {
    return catalogNodes.find(
      (n) => n.id === currentConnectionId || n.name === currentConnectionId
    );
  }, [catalogNodes, currentConnectionId]);

  const availableTables = useMemo(() => {
    if (!matchedNode?.tables) return [];
    return matchedNode.tables.map((t) => (t.schema ? `${t.schema}.${t.name}` : t.name));
  }, [matchedNode]);

  const availableSchemas = useMemo(() => {
    if (!matchedNode?.tables) return [];
    const set = new Set<string>();
    for (const t of matchedNode.tables) {
      if (t.schema) set.add(t.schema);
    }
    return Array.from(set).sort();
  }, [matchedNode]);

  const selectedTableObj = useMemo(() => {
    if (!matchedNode?.tables || !step?.target) return null;
    const target = step.target.trim();
    return (
      matchedNode.tables.find(
        (t) => (t.schema ? `${t.schema}.${t.name}` : t.name) === target || t.name === target
      ) || null
    );
  }, [matchedNode, step?.target]);

  const tableColumns = useMemo(() => {
    return selectedTableObj?.columns ?? [];
  }, [selectedTableObj]);

  const currentScope: "table" | "tables" | "schema" | "source" = useMemo(() => {
    if (stepArgs.scope === "source" || step?.target === "*") return "source";
    if (stepArgs.scope === "schema" || (step?.target && step.target.endsWith(".*"))) return "schema";
    if (
      stepArgs.scope === "tables" ||
      (stepArgs.tables && Array.isArray(stepArgs.tables) && stepArgs.tables.length > 0) ||
      (step?.target && step.target.includes(","))
    )
      return "tables";
    return "table";
  }, [step?.target, stepArgs.scope, stepArgs.tables]);

  const selectedTablesList = useMemo(() => {
    if (stepArgs.tables && Array.isArray(stepArgs.tables)) {
      return stepArgs.tables as string[];
    }
    if (step?.target && step.target.includes(",")) {
      return step.target.split(",").map((s) => s.trim()).filter(Boolean);
    }
    return [];
  }, [stepArgs.tables, step?.target]);

  const filteredAvailableTables = useMemo(() => {
    if (!availableTables) return [];
    if (!tableSearch.trim()) return availableTables;
    const q = tableSearch.toLowerCase().trim();
    return availableTables.filter((t) => t.toLowerCase().includes(q));
  }, [availableTables, tableSearch]);

  const allFilteredSelected = useMemo(() => {
    return (
      filteredAvailableTables.length > 0 &&
      filteredAvailableTables.every((t) => selectedTablesList.includes(t))
    );
  }, [filteredAvailableTables, selectedTablesList]);

  const copyToken = async (value: string) => {
    try {
      await navigator.clipboard.writeText(value);
      setCopiedToken(value);
      window.setTimeout(() => setCopiedToken(null), 1500);
    } catch {
      setCopiedToken(null);
    }
  };

  // Insert at the caret rather than appending: the token usually belongs inside a
  // WHERE clause the operator has already started typing.
  const insertToken = (value: string) => {
    if (!step) return;
    const field = targetRef.current;
    if (!field) {
      onChange({ target: `${step.target} ${value}`.trim() });
      return;
    }
    const start = field.selectionStart ?? step.target.length;
    const end = field.selectionEnd ?? start;
    onChange({ target: step.target.slice(0, start) + value + step.target.slice(end) });
    window.requestAnimationFrame(() => {
      field.focus();
      field.setSelectionRange(start + value.length, start + value.length);
    });
  };

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

  const toggleTableInSelection = (tableName: string) => {
    const next = selectedTablesList.includes(tableName)
      ? selectedTablesList.filter((t) => t !== tableName)
      : [...selectedTablesList, tableName];
    onChange({ target: next.join(", ") });
    setArgs({
      scope: "tables",
      tables: next,
      entity: undefined,
      schema_name: undefined,
      target_name: undefined,
    });
  };

  const toggleSelectAll = () => {
    const next = allFilteredSelected
      ? selectedTablesList.filter((t) => !filteredAvailableTables.includes(t))
      : Array.from(new Set([...selectedTablesList, ...filteredAvailableTables]));
    onChange({ target: next.join(", ") });
    setArgs({
      scope: "tables",
      tables: next,
      entity: undefined,
      schema_name: undefined,
      target_name: undefined,
    });
  };

  const switchScope = (newScope: "table" | "tables" | "schema" | "source") => {
    if (newScope === "table") {
      const firstTbl = availableTables[0] || "";
      const cleanStem = firstTbl.includes(".") ? firstTbl.split(".").slice(-1)[0] : firstTbl;
      onChange({ target: firstTbl });
      setArgs({
        scope: "table",
        entity: firstTbl,
        tables: undefined,
        schema_name: undefined,
        target_name: cleanStem,
      });
    } else if (newScope === "tables") {
      const initial = selectedTablesList.length > 0 ? selectedTablesList : availableTables.slice(0, 3);
      onChange({ target: initial.join(", ") });
      setArgs({
        scope: "tables",
        tables: initial,
        entity: undefined,
        schema_name: undefined,
        target_name: undefined,
      });
    } else if (newScope === "schema") {
      const firstSchema = availableSchemas[0] || (availableTables[0]?.includes(".") ? availableTables[0].split(".")[0] : "public");
      onChange({ target: `${firstSchema}.*` });
      setArgs({
        scope: "schema",
        schema_name: firstSchema,
        entity: undefined,
        tables: undefined,
        target_name: undefined,
      });
    } else if (newScope === "source") {
      onChange({ target: "*" });
      setArgs({
        scope: "source",
        entity: undefined,
        tables: undefined,
        schema_name: undefined,
        target_name: undefined,
      });
    }
  };

  const declared = (args.outputs as Record<string, { type?: string; sensitive?: boolean }>) ?? {};
  const materialize = args.materialize as { folder?: string; name?: string } | undefined;
  const isIngest = step.step_type === "ingest";
  const isRawSql = step.step_type === "raw_sql";
  const isDbt = step.step_type === "dbt_run" || step.step_type === "dbt_test";
  const isProcess = step.step_type === "script" || step.step_type === "notebook";
  const style = statusStyle(runtime?.status);

  const isPipeline = step.step_type === "pipeline";
  const targetOptions =
    step.step_type === "script"
      ? targets?.scripts ?? []
      : step.step_type === "notebook"
        ? targets?.notebooks ?? []
        : isDbt
          ? ["*", ...(targets?.dbt_directories ?? []), ...(targets?.dbt_models ?? [])]
          : [];
  const pipelineOptions = (pipelines ?? []).map((pipeline) => ({
    id: pipeline.id,
    label: `${pipeline.name}${pipeline.is_enabled ? " (scheduled)" : ""}`,
    enabled: pipeline.is_enabled,
  }));

  const available = [
    ...Object.entries(declared).map(([name, spec]) => ({
      name,
      type: spec?.type === "table" ? "table" : "scalar",
      derived: false,
    })),
    ...(isRawSql ? [{ name: "rows", type: "scalar", derived: true }] : []),
    ...(isIngest ? [
      { name: "table", type: "table", derived: true },
      { name: "rows", type: "scalar", derived: true },
    ] : []),
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
                onChange={(event) => {
                  const nextType = event.target.value as StepType;
                  const patch: Partial<PipelineStepDef> = { step_type: nextType };
                  if (nextType === "ingest" && !args.target_folder) {
                    patch.args = { ...args, target_folder: "raw", mode: "snapshot", write_strategy: "append" };
                  }
                  onChange(patch);
                }}
              >
                {TYPE_OPTIONS.map((type) => (
                  <option key={type} value={type}>
                    {stepTypeLabel(type)}
                  </option>
                ))}
              </select>
            </Field>

            {isIngest ? (
              <>
                <Field
                  label="Source connection"
                  hint="Select the external database or data source to ingest from."
                >
                  <select
                    className={inputClass}
                    value={currentConnectionId}
                    onChange={(event) => {
                      const connId = event.target.value;
                      const found = connectionOptions.find((c) => c.id === connId);
                      setArgs({
                        connection_id: connId,
                        connection: found ? found.name : connId,
                      });
                    }}
                  >
                    <option value="">Select a source connection...</option>
                    {connectionOptions.map((conn) => (
                      <option key={conn.id} value={conn.id}>
                        {conn.name} ({conn.source_type})
                      </option>
                    ))}
                  </select>
                </Field>

                {/* Scope selector tabs */}
                <div>
                  <span className="mb-1 block text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
                    Ingestion Scope
                  </span>
                  <div className="grid grid-cols-4 gap-1 rounded-lg bg-slate-100 p-1">
                    <button
                      type="button"
                      onClick={() => switchScope("table")}
                      className={`flex items-center justify-center gap-1.5 rounded-md py-1.5 text-[11px] font-semibold transition-all ${
                        currentScope === "table"
                          ? "bg-white text-sky-700 shadow-xs"
                          : "text-slate-600 hover:text-slate-900"
                      }`}
                    >
                      <Table className="h-3.5 w-3.5" />
                      <span>Table</span>
                    </button>
                    <button
                      type="button"
                      onClick={() => switchScope("tables")}
                      className={`flex items-center justify-center gap-1.5 rounded-md py-1.5 text-[11px] font-semibold transition-all ${
                        currentScope === "tables"
                          ? "bg-white text-sky-700 shadow-xs"
                          : "text-slate-600 hover:text-slate-900"
                      }`}
                    >
                      <Layers className="h-3.5 w-3.5" />
                      <span>Batch</span>
                    </button>
                    <button
                      type="button"
                      onClick={() => switchScope("schema")}
                      className={`flex items-center justify-center gap-1.5 rounded-md py-1.5 text-[11px] font-semibold transition-all ${
                        currentScope === "schema"
                          ? "bg-white text-sky-700 shadow-xs"
                          : "text-slate-600 hover:text-slate-900"
                      }`}
                    >
                      <Folder className="h-3.5 w-3.5" />
                      <span>Schema</span>
                    </button>
                    <button
                      type="button"
                      onClick={() => switchScope("source")}
                      className={`flex items-center justify-center gap-1.5 rounded-md py-1.5 text-[11px] font-semibold transition-all ${
                        currentScope === "source"
                          ? "bg-white text-sky-700 shadow-xs"
                          : "text-slate-600 hover:text-slate-900"
                      }`}
                    >
                      <Database className="h-3.5 w-3.5" />
                      <span>All (*)</span>
                    </button>
                  </div>
                </div>

                {/* Scope-dependent target input */}
                {currentScope === "table" && (
                  <Field
                    label="Source Table / Entity"
                    hint="Table, view, or collection to extract from the source."
                  >
                    {availableTables.length > 0 ? (
                      <div className="space-y-1.5">
                        <select
                          className={inputClass}
                          value={step.target}
                          onChange={(event) => {
                            const val = event.target.value;
                            const cleanStem = val.includes(".") ? val.split(".").slice(-1)[0] : val;
                            onChange({ target: val });
                            setArgs({
                              scope: "table",
                              entity: val,
                              ...(!args.target_name ? { target_name: cleanStem } : {}),
                            });
                          }}
                        >
                          <option value="">Choose discovered table...</option>
                          {!availableTables.includes(step.target) && step.target && (
                            <option value={step.target}>{step.target} (custom)</option>
                          )}
                          {availableTables.map((tbl) => (
                            <option key={tbl} value={tbl}>
                              {tbl}
                            </option>
                          ))}
                        </select>
                        <input
                          className={inputClass}
                          placeholder="Or custom: schema.table_name"
                          value={step.target}
                          onChange={(event) => {
                            const val = event.target.value;
                            const cleanStem = val.includes(".") ? val.split(".").slice(-1)[0] : val;
                            onChange({ target: val });
                            setArgs({
                              scope: "table",
                              entity: val,
                              ...(!args.target_name ? { target_name: cleanStem } : {}),
                            });
                          }}
                        />
                      </div>
                    ) : (
                      <input
                        className={inputClass}
                        placeholder="e.g. sales.sales_order or public.customers"
                        value={step.target}
                        onChange={(event) => {
                          const val = event.target.value;
                          const cleanStem = val.includes(".") ? val.split(".").slice(-1)[0] : val;
                          onChange({ target: val });
                          setArgs({
                            scope: "table",
                            entity: val,
                            ...(!args.target_name ? { target_name: cleanStem } : {}),
                          });
                        }}
                      />
                    )}
                  </Field>
                )}

                {currentScope === "tables" && (
                  <div>
                    <div className="flex items-center justify-between mb-1.5">
                      <span className="text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
                        Select Tables to Ingest
                      </span>
                      <span className="px-2 py-0.5 text-[10px] font-mono bg-sky-100 text-sky-800 rounded-full font-semibold">
                        {selectedTablesList.length} / {availableTables.length} Selected
                      </span>
                    </div>

                    <div className="flex items-center gap-2 mb-2">
                      <div className="relative flex-1">
                        <Search className="w-3.5 h-3.5 text-slate-400 absolute left-2.5 top-2" />
                        <input
                          type="text"
                          placeholder="Filter tables..."
                          value={tableSearch}
                          onChange={(e) => setTableSearch(e.target.value)}
                          className="w-full pl-8 pr-2.5 py-1 text-xs bg-slate-50 border border-slate-200 rounded-lg outline-hidden focus:border-sky-500 focus:bg-white transition-all"
                        />
                      </div>
                      <button
                        type="button"
                        onClick={toggleSelectAll}
                        className="px-2.5 py-1 text-[11px] font-medium text-sky-700 bg-sky-50 hover:bg-sky-100 border border-sky-200 rounded-lg shrink-0 transition-colors"
                      >
                        {allFilteredSelected ? "Deselect All" : "Select All"}
                      </button>
                    </div>

                    <div className="max-h-44 overflow-y-auto border border-slate-200 rounded-lg divide-y divide-slate-100 bg-white">
                      {filteredAvailableTables.length === 0 ? (
                        <div className="p-3 text-center text-xs text-slate-400">
                          {availableTables.length === 0
                            ? "No tables discovered for this connection."
                            : "No tables match your search."}
                        </div>
                      ) : (
                        filteredAvailableTables.map((tbl) => {
                          const isSelected = selectedTablesList.includes(tbl);
                          return (
                            <label
                              key={tbl}
                              className={`flex items-center gap-2.5 px-3 py-1.5 text-xs cursor-pointer transition-colors ${
                                isSelected ? "bg-sky-50/60 text-slate-900" : "hover:bg-slate-50 text-slate-700"
                              }`}
                            >
                              <input
                                type="checkbox"
                                checked={isSelected}
                                onChange={() => toggleTableInSelection(tbl)}
                                className="rounded border-slate-300 text-sky-600 focus:ring-sky-500"
                              />
                              <span className="font-mono text-[11px] truncate">{tbl}</span>
                            </label>
                          );
                        })
                      )}
                    </div>
                  </div>
                )}

                {currentScope === "schema" && (
                  <Field
                    label="Source Schema"
                    hint="All tables belonging to this schema will be extracted and published into the lakehouse."
                  >
                    {availableSchemas.length > 0 ? (
                      <select
                        className={inputClass}
                        value={
                          (args.schema_name as string) ||
                          (step.target.endsWith(".*")
                            ? step.target.slice(0, -2)
                            : availableSchemas[0] || "")
                        }
                        onChange={(e) => {
                          const sch = e.target.value;
                          onChange({ target: `${sch}.*` });
                          setArgs({
                            scope: "schema",
                            schema_name: sch,
                            entity: undefined,
                            tables: undefined,
                          });
                        }}
                      >
                        {availableSchemas.map((sch) => {
                          const count =
                            matchedNode?.tables.filter((t) => t.schema === sch).length ?? 0;
                          return (
                            <option key={sch} value={sch}>
                              {sch} ({count} table{count === 1 ? "" : "s"})
                            </option>
                          );
                        })}
                      </select>
                    ) : (
                      <input
                        className={inputClass}
                        placeholder="e.g. public, sales"
                        value={
                          (args.schema_name as string) ||
                          (step.target.endsWith(".*") ? step.target.slice(0, -2) : "")
                        }
                        onChange={(e) => {
                          const sch = e.target.value;
                          onChange({ target: `${sch}.*` });
                          setArgs({
                            scope: "schema",
                            schema_name: sch,
                            entity: undefined,
                            tables: undefined,
                          });
                        }}
                      />
                    )}
                  </Field>
                )}

                {currentScope === "source" && (
                  <div className="rounded-lg border border-sky-200 bg-sky-50/50 p-3 text-xs text-sky-900">
                    <div className="flex items-center gap-2 font-semibold text-sky-950">
                      <Database className="w-4 h-4 text-sky-600" />
                      <span>Entire Source Ingestion</span>
                    </div>
                    <p className="mt-1 text-[11px] leading-relaxed text-sky-800">
                      Extracts all{" "}
                      {availableTables.length > 0 ? `${availableTables.length} discovered ` : ""}
                      tables from source connection{" "}
                      <strong>{matchedNode?.name || currentConnectionId}</strong>. Each table is
                      published into its own Parquet file under the target folder.
                    </p>
                  </div>
                )}

                {/* Lakehouse folder and target stem */}
                <div className={`grid ${currentScope === "table" ? "grid-cols-2" : "grid-cols-1"} gap-2`}>
                  <Field label="Lakehouse folder" hint="e.g. raw, bronze">
                    <input
                      className={inputClass}
                      value={(args.target_folder as string) ?? "raw"}
                      placeholder="raw"
                      onChange={(event) => setArgs({ target_folder: event.target.value })}
                    />
                  </Field>
                  {currentScope === "table" && (
                    <Field label="Table name stem" hint="Parquet table stem">
                      <input
                        className={`${inputClass} font-mono`}
                        value={(args.target_name as string) ?? ""}
                        placeholder={
                          step.target
                            ? step.target.includes(".")
                              ? step.target.split(".").slice(-1)[0]
                              : step.target
                            : "table_name"
                        }
                        onChange={(event) => setArgs({ target_name: event.target.value })}
                      />
                    </Field>
                  )}
                </div>

                {/* Ingestion Mode */}
                <Field label="Ingestion mode">
                  <div className="grid grid-cols-2 gap-2">
                    <button
                      type="button"
                      onClick={() => setArgs({ mode: "snapshot" })}
                      className={`py-2 px-2.5 text-xs font-medium rounded-lg border text-left transition-all ${
                        (args.mode ?? "snapshot") === "snapshot"
                          ? "border-sky-500 bg-sky-50 text-sky-900 ring-1 ring-sky-500 font-semibold shadow-xs"
                          : "border-slate-200 bg-white text-slate-600 hover:bg-slate-50"
                      }`}
                    >
                      <div className="font-semibold flex items-center gap-1.5">
                        <RefreshCw className="w-3.5 h-3.5 text-sky-600" />
                        <span>Full Snapshot</span>
                      </div>
                      <div className="text-[10px] text-slate-500 mt-0.5">
                        Direct streaming snapshot to Parquet
                      </div>
                    </button>
                    <button
                      type="button"
                      onClick={() => {
                        const patch: Record<string, unknown> = { mode: "incremental" };
                        if (!args.watermark_column && tableColumns.length > 0) {
                          const cand =
                            tableColumns.find(
                              (c) =>
                                c.name.toLowerCase().includes("updated") ||
                                c.name.toLowerCase().includes("time") ||
                                c.name.toLowerCase().includes("date") ||
                                c.name.toLowerCase().includes("created")
                            )?.name ||
                            tableColumns.find((c) => c.name.toLowerCase().includes("id"))?.name ||
                            tableColumns[0]?.name;
                          if (cand) patch.watermark_column = cand;
                        }
                        setArgs(patch);
                      }}
                      className={`py-2 px-2.5 text-xs font-medium rounded-lg border text-left transition-all ${
                        args.mode === "incremental"
                          ? "border-sky-500 bg-sky-50 text-sky-900 ring-1 ring-sky-500 font-semibold shadow-xs"
                          : "border-slate-200 bg-white text-slate-600 hover:bg-slate-50"
                      }`}
                    >
                      <div className="font-semibold flex items-center gap-1.5">
                        <ArrowDownToLine className="w-3.5 h-3.5 text-sky-600" />
                        <span>Incremental</span>
                      </div>
                      <div className="text-[10px] text-slate-500 mt-0.5">
                        Extracts deltas ($gt watermark) & merges
                      </div>
                    </button>
                  </div>
                </Field>

                {/* Incremental Configuration */}
                {args.mode === "incremental" && (
                  <div className="space-y-3 p-3 bg-sky-50/50 border border-sky-200 rounded-xl">
                    <div>
                      <span className="block text-xs font-semibold text-slate-800 mb-1">
                        Watermark Column (Timestamp or Sequence)
                      </span>
                      {currentScope === "table" && tableColumns.length > 0 ? (
                        <select
                          className="w-full text-xs bg-white border border-slate-300 rounded-lg px-2.5 py-1.5 outline-hidden focus:border-sky-500 focus:ring-1 focus:ring-sky-500 font-mono text-slate-800"
                          value={(args.watermark_column as string) ?? ""}
                          onChange={(e) => setArgs({ watermark_column: e.target.value })}
                        >
                          <option value="">Select watermark column...</option>
                          {tableColumns.map((c) => (
                            <option key={c.name} value={c.name}>
                              {c.name} ({c.data_type})
                            </option>
                          ))}
                          {Boolean(args.watermark_column) &&
                            !tableColumns.some((c) => c.name === args.watermark_column) && (
                              <option value={String(args.watermark_column)}>
                                {String(args.watermark_column)} (custom)
                              </option>
                            )}
                        </select>
                      ) : (
                        <input
                          className={inputClass}
                          placeholder="e.g. updated_at, created_at, modified_date"
                          value={(args.watermark_column as string) ?? ""}
                          onChange={(e) => setArgs({ watermark_column: e.target.value })}
                        />
                      )}
                      <p className="text-[10px] text-slate-500 mt-1">
                        Obelisk inspects the existing Parquet file to calculate the current maximum watermark scalar.
                      </p>
                    </div>

                    {/* Write Strategy: Append vs Upsert */}
                    <div>
                      <span className="block text-xs font-semibold text-slate-800 mb-1">
                        Delta Merge Strategy
                      </span>
                      <div className="grid grid-cols-2 gap-2">
                        <button
                          type="button"
                          onClick={() => setArgs({ write_strategy: "append" })}
                          className={`py-1.5 px-2.5 text-xs rounded-lg border text-left transition-all ${
                            (args.write_strategy ?? "append") === "append"
                              ? "border-sky-500 bg-white text-sky-900 font-semibold shadow-xs ring-1 ring-sky-500"
                              : "border-slate-200 bg-white/70 text-slate-600 hover:bg-white"
                          }`}
                        >
                          <div className="text-xs font-semibold">Append Only</div>
                          <div className="text-[9px] text-slate-500 mt-0.5">Fast log-append</div>
                        </button>

                        <button
                          type="button"
                          onClick={() => {
                            const patch: Record<string, unknown> = { write_strategy: "upsert" };
                            if (!args.primary_key && tableColumns.length > 0) {
                              const pkCand =
                                tableColumns.find((c) => c.is_primary_key)?.name ||
                                tableColumns.find(
                                  (c) =>
                                    c.name.toLowerCase() === "id" ||
                                    c.name.toLowerCase().endsWith("_id")
                                )?.name ||
                                tableColumns[0]?.name;
                              if (pkCand) patch.primary_key = pkCand;
                            }
                            setArgs(patch);
                          }}
                          className={`py-1.5 px-2.5 text-xs rounded-lg border text-left transition-all ${
                            args.write_strategy === "upsert"
                              ? "border-sky-500 bg-white text-sky-900 font-semibold shadow-xs ring-1 ring-sky-500"
                              : "border-slate-200 bg-white/70 text-slate-600 hover:bg-white"
                          }`}
                        >
                          <div className="text-xs font-semibold">Upsert (Deduplicate)</div>
                          <div className="text-[9px] text-slate-500 mt-0.5">Updates matching PK</div>
                        </button>
                      </div>
                    </div>

                    {/* Primary Key for Upsert */}
                    {args.write_strategy === "upsert" && (
                      <div className="pt-0.5">
                        <span className="block text-xs font-semibold text-slate-800 mb-1">
                          Primary Key / Deduplication Column
                        </span>
                        {currentScope === "table" && tableColumns.length > 0 ? (
                          <select
                            className="w-full text-xs bg-white border border-slate-300 rounded-lg px-2.5 py-1.5 outline-hidden focus:border-sky-500 focus:ring-1 focus:ring-sky-500 font-mono text-slate-800"
                            value={(args.primary_key as string) ?? ""}
                            onChange={(e) => setArgs({ primary_key: e.target.value })}
                          >
                            <option value="">Select primary key column...</option>
                            {tableColumns.map((c) => (
                              <option key={c.name} value={c.name}>
                                {c.name} ({c.data_type}){c.is_primary_key ? " 🔑 PK" : ""}
                              </option>
                            ))}
                            {Boolean(args.primary_key) &&
                              !tableColumns.some((c) => c.name === args.primary_key) && (
                                <option value={String(args.primary_key)}>
                                  {String(args.primary_key)} (custom)
                                </option>
                              )}
                          </select>
                        ) : (
                          <input
                            className={inputClass}
                            placeholder="e.g. id"
                            value={(args.primary_key as string) ?? ""}
                            onChange={(e) => setArgs({ primary_key: e.target.value })}
                          />
                        )}
                      </div>
                    )}
                  </div>
                )}

                <Field label="Row limit (optional)" hint="Sample / cap rows during extraction. Blank = all.">
                  <input
                    type="number"
                    min={1}
                    className={inputClass}
                    placeholder="e.g. 10000"
                    value={(args.limit as number) ?? ""}
                    onChange={(event) =>
                      setArgs({ limit: event.target.value ? Number(event.target.value) : null })
                    }
                  />
                </Field>
              </>
            ) : (
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
                {isPipeline ? (
                  <select
                    className={inputClass}
                    value={step.target}
                    onChange={(event) => onChange({ target: event.target.value })}
                  >
                    <option value="">Choose a pipeline?</option>
                    {!pipelineOptions.some((option) => option.id === step.target) &&
                      step.target && (
                        <option value={step.target}>{step.target} (missing)</option>
                      )}
                    {pipelineOptions.map((option) => (
                      <option key={option.id} value={option.id}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                ) : isRawSql ? (
                  <textarea
                    ref={targetRef}
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
            )}

            {isPipeline && (
              <div className="space-y-1 rounded-lg bg-slate-50 px-2 py-1.5">
                <label className="flex items-center gap-1.5 text-[11px] font-semibold text-slate-700">
                  <input
                    type="checkbox"
                    checked={args.wait !== false}
                    onChange={(event) => setArgs({ wait: event.target.checked })}
                  />
                  Wait for the child run
                </label>
                <p className="text-[10px] leading-snug text-slate-500">
                  {args.wait === false
                    ? "Starts it and moves on: its failures will not reach this pipeline."
                    : "This step succeeds only if the child run succeeds; cancelling here cancels there."}
                </p>
              </div>
            )}

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
                      {edge.direction === 'in' ? '\u2190' : '\u2192'}
                    </span>
                    <span className="min-w-0 flex-1 truncate text-[11px] text-slate-700">
                      {edge.name}
                    </span>
                    <button
                      type="button"
                      onClick={() => onEditEdge?.(edge.from_key, edge.to_key)}
                      className="shrink-0 rounded border border-slate-300 bg-white px-1 text-[9px] font-bold uppercase text-slate-500 hover:border-sky-300 hover:text-sky-700"
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
                <span className="text-[10px] text-slate-400">{available.length} available</span>
              </div>
              <p className="border-b border-slate-100 px-2.5 py-1 text-[10px] leading-snug text-slate-400">
                A result for child steps: a value, not data (at most 4 KB). Rows belong in a
                table.
              </p>
              <div className="space-y-2 p-2.5">
                {isRawSql && (
                  <div className="space-y-1.5">
                    <label className="flex items-center gap-1.5 text-[11px] font-semibold text-slate-700">
                      <input
                        type="checkbox"
                        checked={Boolean(materialize)}
                        onChange={(event) =>
                          event.target.checked
                            ? setArgs({ materialize: { folder: "", name: "" } })
                            : dropArg("materialize")
                        }
                      />
                      Write the result to a lakehouse table
                    </label>
                    {materialize && (
                      <div className="grid grid-cols-[1fr_1.4fr] gap-1.5">
                        <input
                          className={inputClass}
                          value={materialize.folder ?? ""}
                          aria-label="Folder"
                          placeholder="folder (blank = root)"
                          onChange={(event) =>
                            setArgs({
                              materialize: { ...materialize, folder: event.target.value },
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
                        {`${materialize.folder ? materialize.folder + "/" : ""}${materialize.name}`}
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
                  <div className="space-y-1.5 rounded-lg bg-slate-50 px-2 py-1.5">
                    <p className="text-[10px] font-semibold text-slate-500">
                      Children can read these
                    </p>
                    {available.map((entry) => {
                      const value = token(slug(step.name), entry.name);
                      return (
                        <div key={entry.name} className="flex items-center gap-1">
                          <code className="min-w-0 flex-1 truncate font-mono text-[10px] text-sky-800">
                            {value}
                          </code>
                          {entry.derived && (
                            <span className="shrink-0 rounded bg-white px-1 text-[8px] font-bold uppercase text-slate-400">
                              auto
                            </span>
                          )}
                          <button
                            type="button"
                            onClick={() => void copyToken(value)}
                            className="shrink-0 rounded p-0.5 text-slate-400 hover:bg-white hover:text-slate-600"
                            aria-label={`Copy ${entry.name}`}
                          >
                            <Copy className="h-3 w-3" />
                          </button>
                          {isRawSql && (
                            <button
                              type="button"
                              onClick={() => insertToken(value)}
                              className="shrink-0 rounded p-0.5 text-slate-400 hover:bg-white hover:text-sky-700"
                              aria-label={`Insert ${entry.name}`}
                              title="Insert at the caret"
                            >
                              <CornerDownLeft className="h-3 w-3" />
                            </button>
                          )}
                        </div>
                      );
                    })}
                    {copiedToken && <p className="text-[9px] text-emerald-600">Copied {copiedToken}</p>}
                    {!parents?.length && (
                      <p className="text-[10px] leading-snug text-slate-400">
                        No parent steps yet, so nothing flows in.
                      </p>
                    )}
                  </div>
                )}

                {isProcess && (
                  <details className="rounded-lg bg-slate-50 px-2 py-1.5">
                    <summary className="cursor-pointer text-[10px] font-semibold text-slate-500">
                      How to publish a value
                    </summary>
                    <pre className="mt-1 overflow-x-auto whitespace-pre-wrap font-mono text-[9px] leading-relaxed text-slate-600">
{`import json, os

with open(os.environ["OBELISK_OUTPUT_FILE"], "w") as fh:
    json.dump({"max_id": max_id}, fh)`}
                    </pre>
                    <p className="mt-1 text-[9px] leading-snug text-slate-400">
                      Direct parents arrive as OBELISK_STEP_&lt;STEP&gt;_&lt;OUTPUT&gt;. Use a table
                      for rows.
                    </p>
                  </details>
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
                className="flex w-full items-center justify-center gap-1.5 rounded-lg bg-sky-600 px-2 py-1.5 text-[11px] font-semibold text-white hover:bg-sky-700 shadow-2xs transition-colors"
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
