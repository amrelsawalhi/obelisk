import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  AlertCircle,
  Ban,
  CheckCircle2,
  CircleSlash,
  Clock,
  Loader2,
  Play,
  Plus,
  RefreshCw,
  Save,
  Settings2,
  Square,
  Trash2,
  X,
} from 'lucide-react';
import { api } from '../../lib/api';
import { usePolling } from '../../lib/viewActivity';
import type {
  EdgeCondition,
  EdgePayload,
  ExecutionStatus,
  PipelineDetail,
  PipelineExecution,
  PipelineExecutionDetail,
  PipelineStepDef,
  PipelineSummary,
  PipelineTargets,
  StepRunItem,
  StepType,
} from '../../lib/types';
import { PipelineCanvas, EDGE_COLOURS, type StepRuntime } from './PipelineCanvas';
import { PipelineInspector } from './PipelineInspector';

interface PipelinesViewProps {
  onOpenInQuery?: (sql: string) => void;
  onNavigateToSettings?: () => void;
}

const CRON_PRESETS: Array<{ label: string; expr: string }> = [
  { label: 'Every 15 min', expr: '*/15 * * * *' },
  { label: 'Hourly', expr: '0 * * * *' },
  { label: 'Nightly 02:00', expr: '0 2 * * *' },
  { label: 'Weekdays 06:00', expr: '0 6 * * 1-5' },
];

const STATUS_CHIP: Record<string, string> = {
  success: 'bg-emerald-50 text-emerald-700 border-emerald-200',
  failed: 'bg-rose-50 text-rose-700 border-rose-200',
  running: 'bg-indigo-50 text-indigo-700 border-indigo-200',
  pending: 'bg-slate-50 text-slate-600 border-slate-200',
  waiting: 'bg-slate-50 text-slate-600 border-slate-200',
  skipped: 'bg-slate-50 text-slate-500 border-slate-200',
  cancelled: 'bg-amber-50 text-amber-700 border-amber-200',
  interrupted: 'bg-orange-50 text-orange-700 border-orange-200',
  idle: 'bg-slate-50 text-slate-500 border-slate-200',
};

const STATUS_ICON: Record<string, React.ElementType> = {
  success: CheckCircle2,
  failed: AlertCircle,
  running: Loader2,
  cancelled: Ban,
  interrupted: Square,
  skipped: CircleSlash,
  pending: Clock,
  waiting: Clock,
  idle: Clock,
};

const STEP_TEMPLATES: Array<{ type: StepType; name: string; target: string }> = [
  { type: 'dbt_run', name: 'dbt run', target: '*' },
  { type: 'dbt_test', name: 'dbt test', target: '*' },
  { type: 'script', name: 'Script', target: '' },
  { type: 'notebook', name: 'Notebook', target: '' },
  { type: 'raw_sql', name: 'SQL', target: 'SELECT 1 AS ok' },
  { type: 'webhook', name: 'Webhook', target: '' },
];

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

const fmtDuration = (seconds?: number | null) => {
  if (seconds == null) return '—';
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${Math.round(seconds % 60)}s`;
};

const newKey = () =>
  typeof crypto !== 'undefined' && 'randomUUID' in crypto
    ? crypto.randomUUID()
    : `step-${Date.now()}-${Math.random().toString(16).slice(2)}`;

interface DraftState {
  steps: PipelineStepDef[];
  edges: EdgePayload[];
}

const draftFrom = (detail: PipelineDetail): DraftState => ({
  steps: detail.steps.map((step) => ({ ...step })),
  edges: detail.edges.map((edge) => ({
    from_key: edge.from_step,
    to_key: edge.to_step,
    condition: edge.condition,
  })),
});

const TERMINAL: ExecutionStatus[] = ['success', 'failed', 'cancelled', 'interrupted', 'skipped'];

export const PipelinesView: React.FC<PipelinesViewProps> = ({ onNavigateToSettings }) => {
  const [pipelines, setPipelines] = useState<PipelineSummary[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<PipelineDetail | null>(null);
  const [draft, setDraft] = useState<DraftState>({ steps: [], edges: [] });
  const [targets, setTargets] = useState<PipelineTargets | null>(null);

  const [loadingDetail, setLoadingDetail] = useState(false);
  const [saving, setSaving] = useState(false);
  const [starting, setStarting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const [selectedStepKey, setSelectedStepKey] = useState<string | null>(null);
  const [pendingEdge, setPendingEdge] = useState<{ from: string; to: string } | null>(null);
  const [showNew, setShowNew] = useState(false);

  const [executions, setExecutions] = useState<PipelineExecution[]>([]);
  const [monitor, setMonitor] = useState<PipelineExecutionDetail | null>(null);
  // The newest finished run, used to paint the canvas while designing. Without it
  // a pipeline that ran a minute ago still shows "Not run yet" on every step, which
  // makes the picture disagree with the history underneath it.
  const [latestRun, setLatestRun] = useState<PipelineExecutionDetail | null>(null);
  const [stepLogs, setStepLogs] = useState('');
  const [logsOffset, setLogsOffset] = useState(0);
  const [loadingLogs, setLoadingLogs] = useState(false);

  const dirty = useMemo(() => {
    if (!detail) return false;
    const saved = draftFrom(detail);
    return JSON.stringify(saved) !== JSON.stringify(draft);
  }, [detail, draft]);

  const selectedStep = useMemo(
    () => draft.steps.find((step) => step.key === selectedStepKey) ?? null,
    [draft.steps, selectedStepKey],
  );

  const monitorByStep = useMemo(() => {
    const map: Record<string, StepRunItem> = {};
    for (const stepRun of monitor?.step_runs ?? []) map[stepRun.step_id] = stepRun;
    return map;
  }, [monitor]);

  const paintedByStep = useMemo(() => {
    const source = monitor ?? latestRun;
    const map: Record<string, StepRunItem> = {};
    for (const stepRun of source?.step_runs ?? []) map[stepRun.step_id] = stepRun;
    return map;
  }, [monitor, latestRun]);

  const runtimeByStep = useMemo(() => {
    const map: Record<string, StepRuntime> = {};
    for (const [stepId, stepRun] of Object.entries(paintedByStep)) {
      map[stepId] = {
        status: stepRun.status,
        duration_seconds: stepRun.duration_seconds,
        attempt: stepRun.attempt,
        error: stepRun.error,
      };
    }
    return map;
  }, [paintedByStep]);

  const selectedStepRun = selectedStepKey ? monitorByStep[selectedStepKey] ?? null : null;
  const monitorActive = monitor != null && !TERMINAL.includes(monitor.status);

  const refreshList = useCallback(async () => {
    try {
      const list = await api.listPipelines();
      setPipelines(list);
      setSelectedId((current) => current ?? list[0]?.id ?? null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not load pipelines.');
    }
  }, []);

  const openPipeline = useCallback(async (pipelineId: string) => {
    setLoadingDetail(true);
    setError(null);
    try {
      const data = await api.getPipeline(pipelineId);
      setDetail(data);
      setDraft(draftFrom(data));
      setSelectedStepKey(null);
      setMonitor(null);
      setStepLogs('');
      setLogsOffset(0);
      const runs = await api.listPipelineExecutions({ pipeline_id: pipelineId, limit: 40 });
      setExecutions(runs);
      const newest = runs.find((run) => TERMINAL.includes(run.status)) ?? runs[0] ?? null;
      setLatestRun(newest ? await api.getPipelineExecution(newest.id) : null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not load this pipeline.');
    } finally {
      setLoadingDetail(false);
    }
  }, []);

  useEffect(() => {
    refreshList();
    api.getPipelineTargets().then(setTargets).catch(() => setTargets(null));
  }, [refreshList]);

  useEffect(() => {
    if (selectedId) openPipeline(selectedId);
  }, [selectedId, openPipeline]);

  const refreshExecutions = useCallback(async () => {
    if (!selectedId) return;
    try {
      setExecutions(await api.listPipelineExecutions({ pipeline_id: selectedId, limit: 40 }));
    } catch {
      /* list refresh is best-effort */
    }
  }, [selectedId]);

  const refreshMonitor = useCallback(async () => {
    if (!monitor?.id) return;
    try {
      const fresh = await api.getPipelineExecution(monitor.id);
      setMonitor(fresh);
      if (TERMINAL.includes(fresh.status)) {
        // Keep the design view painted with the run the operator just watched.
        setLatestRun(fresh);
        refreshExecutions();
      }
    } catch {
      /* keep the last known picture */
    }
  }, [monitor?.id, refreshExecutions]);

  const refreshLogs = useCallback(async () => {
    if (!selectedStepRun) {
      setStepLogs('');
      return;
    }
    try {
      setLoadingLogs(true);
      const tail = await api.getStepRunLogs(selectedStepRun.id, logsOffset);
      if (tail.logs) setStepLogs((current) => current + tail.logs);
      setLogsOffset(tail.size);
    } catch {
      /* log tail is best-effort */
    } finally {
      setLoadingLogs(false);
    }
  }, [selectedStepRun, logsOffset]);

  // Three cadences: the list when idle, the run while it is live, the log tail while
  // its step is. All pause when the tab is not visible (usePolling).
  usePolling(refreshList, 15000);
  usePolling(refreshExecutions, monitorActive ? 2500 : 10000);
  usePolling(() => (monitorActive ? refreshMonitor() : undefined), 1500);
  usePolling(
    () => (selectedStepRun && !TERMINAL.includes(selectedStepRun.status as ExecutionStatus)
      ? refreshLogs()
      : undefined),
    1500,
  );

  useEffect(() => {
    setStepLogs('');
    setLogsOffset(0);
  }, [selectedStepKey, monitor?.id]);


  // ------------------------------------------------------------------ actions

  const saveGraph = async () => {
    if (!detail) return;
    setSaving(true);
    setError(null);
    try {
      await api.replacePipelineGraph(detail.id, {
        steps: draft.steps.map((step) => ({
          key: step.key,
          name: step.name.trim(),
          step_type: step.step_type,
          target: step.target,
          args: step.args ?? {},
          timeout_seconds: step.timeout_seconds,
          max_retries: step.max_retries,
          retry_delay_seconds: step.retry_delay_seconds,
          position_x: step.position_x,
          position_y: step.position_y,
        })),
        edges: draft.edges,
        params: detail.params,
      });
      const fresh = await api.getPipeline(detail.id);
      setDetail(fresh);
      setDraft(draftFrom(fresh));
      setNotice('Pipeline saved.');
      await refreshList();
    } catch (err) {
      setError(
        err instanceof Error
          ? (err as { response?: { data?: { detail?: string } } }).response?.data?.detail ??
              err.message
          : 'Could not save the pipeline.',
      );
    } finally {
      setSaving(false);
    }
  };

  const runNow = async () => {
    if (!detail) return;
    setStarting(true);
    setError(null);
    try {
      const { run_id } = await api.runPipelineNow(detail.id);
      const fresh = await api.getPipelineExecution(run_id);
      setMonitor(fresh);
      setSelectedStepKey(null);
      await refreshExecutions();
      await refreshList();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not start the run.');
    } finally {
      setStarting(false);
    }
  };

  const cancelRun = async (runId: string) => {
    try {
      await api.cancelPipelineExecution(runId);
      await refreshMonitor();
      await refreshExecutions();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not cancel this run.');
    }
  };

  const rerunFromSelected = async () => {
    if (!monitor || !selectedStepKey) return;
    try {
      const { run_id } = await api.rerunFromStep(monitor.id, selectedStepKey);
      const fresh = await api.getPipelineExecution(run_id);
      setMonitor(fresh);
      await refreshExecutions();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not re-run from this step.');
    }
  };

  const togglePipeline = async (pipeline: PipelineSummary) => {
    try {
      await api.togglePipeline(pipeline.id);
      await refreshList();
      if (selectedId === pipeline.id) await openPipeline(pipeline.id);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not change the schedule.');
    }
  };

  const deletePipeline = async (pipeline: PipelineSummary) => {
    if (!window.confirm(`Delete pipeline '${pipeline.name}' and its run history?`)) return;
    try {
      await api.deletePipeline(pipeline.id);
      setDetail(null);
      setMonitor(null);
      setSelectedId(null);
      await refreshList();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not delete the pipeline.');
    }
  };

  const addStep = (type: StepType) => {
    const template = STEP_TEMPLATES.find((entry) => entry.type === type) ?? STEP_TEMPLATES[0];
    const index = draft.steps.length;
    const key = newKey();
    const other = draft.steps.some((step) => step.step_type === type);
    const step: PipelineStepDef = {
      id: key,
      key,
      name: `${template.name}${other ? ` ${index + 1}` : ''}`,
      step_type: type,
      target: template.target,
      args: {},
      timeout_seconds: null,
      max_retries: null,
      retry_delay_seconds: null,
      position_x: 80 + (index % 4) * 260,
      position_y: 80 + Math.floor(index / 4) * 170,
    };
    setDraft((current) => ({ ...current, steps: [...current.steps, step] }));
    setSelectedStepKey(key);
  };

  const updateStep = (patch: Partial<PipelineStepDef>) => {
    if (!selectedStepKey) return;
    setDraft((current) => ({
      ...current,
      steps: current.steps.map((step) =>
        step.key === selectedStepKey ? { ...step, ...patch } : step,
      ),
    }));
  };

  const moveStep = (stepKey: string, position: { x: number; y: number }) => {
    setDraft((current) => ({
      ...current,
      steps: current.steps.map((step) =>
        step.key === stepKey ? { ...step, position_x: position.x, position_y: position.y } : step,
      ),
    }));
  };

  const removeStep = (stepKey: string) => {
    setDraft((current) => ({
      steps: current.steps.filter((step) => step.key !== stepKey),
      edges: current.edges.filter((edge) => edge.from_key !== stepKey && edge.to_key !== stepKey),
    }));
    if (selectedStepKey === stepKey) setSelectedStepKey(null);
  };

  const confirmEdge = (condition: EdgeCondition) => {
    if (!pendingEdge) return;
    const { from, to } = pendingEdge;
    setDraft((current) => ({
      ...current,
      edges: [
        ...current.edges.filter((edge) => !(edge.from_key === from && edge.to_key === to)),
        { from_key: from, to_key: to, condition },
      ],
    }));
    setPendingEdge(null);
  };

  const removeEdge = (fromKey: string, toKey: string) => {
    setDraft((current) => ({
      ...current,
      edges: current.edges.filter(
        (edge) => !(edge.from_key === fromKey && edge.to_key === toKey),
      ),
    }));
    setPendingEdge(null);
  };

  const stepName = (key: string) =>
    draft.steps.find((step) => step.key === key)?.name ?? 'deleted step';


  // --------------------------------------------------------------------- view

  return (
    <div className="flex h-full w-full min-h-0 flex-1 flex-col bg-slate-50">
      <div className="flex items-center justify-between border-b border-slate-200 bg-white px-6 py-3">
        <div>
          <h1 className="text-sm font-bold text-slate-800">Pipelines</h1>
          <p className="text-[11px] text-slate-500">
            Dependencies, schedules and run history for everything Obelisk executes.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={refreshList}
            className="flex items-center gap-1.5 rounded-lg border border-slate-300 px-2.5 py-1.5 text-[11px] font-semibold text-slate-600 hover:bg-slate-50"
          >
            <RefreshCw className="h-3.5 w-3.5" /> Refresh
          </button>
          <button
            type="button"
            onClick={() => setShowNew(true)}
            className="flex items-center gap-1.5 rounded-lg bg-indigo-600 px-3 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700"
          >
            <Plus className="h-3.5 w-3.5" /> New pipeline
          </button>
        </div>
      </div>

      {(error || notice) && (
        <div className="px-4 pt-3">
          {error && (
            <div className="flex items-start gap-2 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-700">
              <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
              <span className="flex-1 break-words">{error}</span>
              <button type="button" onClick={() => setError(null)} aria-label="Dismiss">
                <X className="h-3.5 w-3.5" />
              </button>
            </div>
          )}
          {notice && (
            <div className="mt-2 flex items-start gap-2 rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-700">
              <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0" />
              <span className="flex-1">{notice}</span>
              <button type="button" onClick={() => setNotice(null)} aria-label="Dismiss">
                <X className="h-3.5 w-3.5" />
              </button>
            </div>
          )}
        </div>
      )}

      <div className="flex min-h-0 flex-1">
        <aside className="w-[286px] shrink-0 overflow-y-auto border-r border-slate-200 bg-white">
          {pipelines.length === 0 && (
            <p className="p-4 text-xs text-slate-400">No pipelines yet. Create one to begin.</p>
          )}
          {pipelines.map((pipeline) => {
            const active = pipeline.id === selectedId;
            const Icon = STATUS_ICON[pipeline.last_status] ?? Clock;
            const chip = STATUS_CHIP[pipeline.last_status] ?? STATUS_CHIP.idle;
            return (
              <button
                key={pipeline.id}
                type="button"
                onClick={() => setSelectedId(pipeline.id)}
                className={`flex w-full flex-col gap-1 border-b border-slate-100 px-4 py-3 text-left transition-colors ${
                  active ? 'bg-indigo-50/60' : 'hover:bg-slate-50'
                }`}
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="truncate text-xs font-semibold text-slate-800">
                    {pipeline.name}
                  </span>
                  {pipeline.is_running ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin text-indigo-600" />
                  ) : (
                    <span
                      className={`flex items-center gap-1 rounded border px-1.5 py-0.5 text-[9px] font-bold uppercase ${chip}`}
                    >
                      <Icon className="h-2.5 w-2.5" />
                      {pipeline.last_status}
                    </span>
                  )}
                </div>
                <span className="truncate text-[10px] font-mono text-slate-400">
                  {pipeline.is_enabled ? 'every' : 'paused ·'} {pipeline.cron_expression} ·{' '}
                  {pipeline.step_count} step{pipeline.step_count === 1 ? '' : 's'}
                </span>
                <span className="text-[10px] text-slate-400">
                  Next: {pipeline.is_enabled ? fmtWhen(pipeline.next_run_at) : '—'}
                </span>
              </button>
            );
          })}
        </aside>


        <section className="flex min-w-0 flex-1 flex-col">
          {!detail && (
            <div className="flex flex-1 items-center justify-center">
              <p className="text-xs text-slate-400">
                {loadingDetail ? 'Loading…' : 'Select a pipeline on the left.'}
              </p>
            </div>
          )}

          {detail && (
            <>
              <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-200 bg-white px-4 py-2.5">
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                    <h2 className="truncate text-sm font-semibold text-slate-800">
                      {detail.name}
                    </h2>
                    <span
                      className={`rounded border px-1.5 py-0.5 text-[9px] font-bold uppercase ${
                        STATUS_CHIP[monitor?.status ?? detail.last_status] ?? STATUS_CHIP.idle
                      }`}
                    >
                      {monitor?.status ?? detail.last_status}
                    </span>
                    {dirty && (
                      <span className="rounded bg-amber-100 px-1.5 py-0.5 text-[9px] font-bold uppercase text-amber-700">
                        Unsaved
                      </span>
                    )}
                  </div>
                  <p className="truncate text-[10px] font-mono text-slate-400">
                    {detail.is_enabled ? 'Scheduled' : 'Paused'} · {detail.cron_expression} ·{' '}
                    {detail.timezone} · overlap {detail.overlap_policy} · max {detail.max_parallel}{' '}
                    parallel
                  </p>
                </div>

                <div className="flex flex-wrap items-center gap-1.5">
                  <button
                    type="button"
                    onClick={() => togglePipeline(detail)}
                    className={`rounded-lg border px-2.5 py-1.5 text-[11px] font-semibold ${
                      detail.is_enabled
                        ? 'border-slate-300 text-slate-600 hover:bg-slate-50'
                        : 'border-emerald-300 text-emerald-700 hover:bg-emerald-50'
                    }`}
                  >
                    {detail.is_enabled ? 'Pause' : 'Enable'}
                  </button>
                  <button
                    type="button"
                    onClick={runNow}
                    disabled={starting}
                    className="flex items-center gap-1.5 rounded-lg bg-indigo-600 px-2.5 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700 disabled:opacity-50"
                  >
                    {starting ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    ) : (
                      <Play className="h-3.5 w-3.5" />
                    )}
                    Run now
                  </button>
                  <button
                    type="button"
                    onClick={saveGraph}
                    disabled={!dirty || saving}
                    className="flex items-center gap-1.5 rounded-lg border border-indigo-300 bg-indigo-50 px-2.5 py-1.5 text-[11px] font-semibold text-indigo-700 hover:bg-indigo-100 disabled:opacity-40"
                  >
                    {saving ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    ) : (
                      <Save className="h-3.5 w-3.5" />
                    )}
                    Save
                  </button>
                  <button
                    type="button"
                    onClick={() => deletePipeline(detail)}
                    className="rounded-lg border border-rose-200 p-1.5 text-rose-600 hover:bg-rose-50"
                    aria-label="Delete pipeline"
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </button>
                </div>
              </div>


              {monitor && (
                <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 bg-slate-100/70 px-4 py-2">
                  <div className="flex flex-wrap items-center gap-2 text-[11px] text-slate-600">
                    <span className="font-semibold">Watching run</span>
                    <span className="font-mono text-slate-500">{monitor.id.slice(0, 8)}</span>
                    <span className="text-slate-400">
                      {monitor.kind} · started {fmtWhen(monitor.started_at)} ·{' '}
                      {fmtDuration(monitor.duration_seconds)}
                    </span>
                    {Object.entries(monitor.step_counts).map(([key, count]) => (
                      <span
                        key={key}
                        className={`rounded border px-1.5 py-0.5 text-[9px] font-bold uppercase ${
                          STATUS_CHIP[key] ?? STATUS_CHIP.idle
                        }`}
                      >
                        {key} {count}
                      </span>
                    ))}
                  </div>
                  <div className="flex items-center gap-1.5">
                    {monitorActive && (
                      <button
                        type="button"
                        onClick={() => cancelRun(monitor.id)}
                        className="flex items-center gap-1 rounded-lg border border-amber-300 bg-white px-2 py-1 text-[11px] font-semibold text-amber-700 hover:bg-amber-50"
                      >
                        <Ban className="h-3 w-3" /> Cancel run
                      </button>
                    )}
                    <button
                      type="button"
                      onClick={() => setMonitor(null)}
                      className="rounded-lg border border-slate-300 bg-white px-2 py-1 text-[11px] font-semibold text-slate-600 hover:bg-slate-50"
                    >
                      Back to design
                    </button>
                  </div>
                </div>
              )}

              <div className="flex min-h-0 flex-1">
                <div className="flex min-w-0 flex-1 flex-col">
                  {!monitor && (
                    <div className="flex flex-wrap items-center gap-1.5 border-b border-slate-200 bg-white px-3 py-2">
                      <span className="mr-1 flex items-center gap-1 text-[10px] font-bold uppercase tracking-wide text-slate-400 font-mono">
                        <Plus className="h-3 w-3" /> Add step
                      </span>
                      {STEP_TEMPLATES.map((template) => (
                        <button
                          key={template.type}
                          type="button"
                          onClick={() => addStep(template.type)}
                          className="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] font-medium text-slate-600 hover:border-indigo-300 hover:text-indigo-700"
                        >
                          {template.name}
                        </button>
                      ))}
                    </div>
                  )}

                  <div className="min-h-0 flex-1">
                    {draft.steps.length === 0 ? (
                      <div className="flex h-full flex-col items-center justify-center gap-2 text-center">
                        <p className="text-xs text-slate-400">
                          This pipeline has no steps yet. Add one above.
                        </p>
                      </div>
                    ) : (
                      <PipelineCanvas
                        steps={draft.steps}
                        edges={draft.edges}
                        editable={!monitor}
                        fitKey={detail.id}
                        runtimeByStep={runtimeByStep}
                        selectedStepId={selectedStepKey}
                        onSelectStep={setSelectedStepKey}
                        onConnectRequest={(from, to) => setPendingEdge({ from, to })}
                        onDeleteStep={removeStep}
                        onMoveStep={moveStep}
                      />
                    )}
                  </div>
                </div>

                <aside className="w-[320px] shrink-0 border-l border-slate-200 bg-white">
                  <PipelineInspector
                    step={selectedStep}
                    runtime={selectedStepKey ? runtimeByStep[selectedStepKey] : undefined}
                    stepRun={selectedStepRun}
                    editable={!monitor}
                    targets={targets}
                    logs={stepLogs}
                    loadingLogs={loadingLogs}
                    onChange={updateStep}
                    onDelete={() => selectedStepKey && removeStep(selectedStepKey)}
                    onRerunFrom={monitor && selectedStepRun ? rerunFromSelected : undefined}
                  />
                </aside>
              </div>


              <div className="h-[196px] shrink-0 overflow-y-auto border-t border-slate-200 bg-white">
                <div className="sticky top-0 flex items-center justify-between border-b border-slate-100 bg-white px-4 py-1.5">
                  <span className="text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
                    Runs ({executions.length})
                  </span>
                  <button
                    type="button"
                    onClick={refreshExecutions}
                    className="flex items-center gap-1 text-[10px] font-semibold text-slate-500 hover:text-indigo-600"
                  >
                    <RefreshCw className="h-3 w-3" /> Refresh
                  </button>
                </div>
                {executions.length === 0 && (
                  <p className="px-4 py-3 text-[11px] text-slate-400">
                    No runs yet — use “Run now”, or enable the schedule.
                  </p>
                )}
                {executions.map((run) => {
                  const Icon = STATUS_ICON[run.status] ?? Clock;
                  return (
                    <div
                      key={run.id}
                      className={`flex items-center justify-between gap-2 border-b border-slate-50 px-4 py-1.5 text-[11px] hover:bg-slate-50 ${
                        monitor?.id === run.id ? 'bg-indigo-50/50' : ''
                      }`}
                    >
                      <div className="flex min-w-0 items-center gap-2">
                        <span
                          className={`flex items-center gap-1 rounded border px-1.5 py-0.5 text-[9px] font-bold uppercase ${
                            STATUS_CHIP[run.status] ?? STATUS_CHIP.idle
                          }`}
                        >
                          <Icon className="h-2.5 w-2.5" />
                          {run.status}
                        </span>
                        <span className="font-mono text-slate-400">{run.id.slice(0, 8)}</span>
                        <span className="text-slate-500">{run.kind}</span>
                        <span className="text-slate-400">{fmtWhen(run.started_at ?? run.scheduled_for)}</span>
                        <span className="font-mono text-slate-400">
                          {fmtDuration(run.duration_seconds)}
                        </span>
                        {run.error && (
                          <span className="max-w-[280px] truncate text-rose-600">{run.error}</span>
                        )}
                      </div>
                      <div className="flex items-center gap-1.5">
                        <button
                          type="button"
                          onClick={async () => {
                            const fresh = await api.getPipelineExecution(run.id);
                            setMonitor(fresh);
                            setSelectedStepKey(null);
                          }}
                          className="rounded border border-slate-300 px-1.5 py-0.5 text-[10px] font-semibold text-slate-600 hover:bg-white"
                        >
                          Watch
                        </button>
                        {!TERMINAL.includes(run.status) && (
                          <button
                            type="button"
                            onClick={() => cancelRun(run.id)}
                            className="rounded border border-amber-300 px-1.5 py-0.5 text-[10px] font-semibold text-amber-700 hover:bg-amber-50"
                          >
                            Cancel
                          </button>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            </>
          )}
        </section>
      </div>


      {pendingEdge && (
        <EdgeConditionModal
          from={stepName(pendingEdge.from)}
          to={stepName(pendingEdge.to)}
          current={
            draft.edges.find(
              (edge) => edge.from_key === pendingEdge.from && edge.to_key === pendingEdge.to,
            )?.condition ?? null
          }
          onPick={confirmEdge}
          onRemove={() => removeEdge(pendingEdge.from, pendingEdge.to)}
          onClose={() => setPendingEdge(null)}
        />
      )}

      {showNew && (
        <NewPipelineModal
          onClose={() => setShowNew(false)}
          onOpenSettings={onNavigateToSettings}
          onCreated={async (id) => {
            setShowNew(false);
            await refreshList();
            setSelectedId(id);
          }}
        />
      )}
    </div>
  );
};

const CONDITIONS: Array<{ key: EdgeCondition; label: string; hint: string }> = [
  { key: 'success', label: 'On success', hint: 'Run only when the previous step succeeded' },
  { key: 'failure', label: 'On failure', hint: 'Run only when the previous step failed' },
  { key: 'completion', label: 'On completion', hint: 'Run once the previous step finished, either way' },
  { key: 'skipped', label: 'On skip', hint: 'Run when the previous step was skipped' },
];

const EdgeConditionModal: React.FC<{
  from: string;
  to: string;
  current: EdgeCondition | null;
  onPick: (condition: EdgeCondition) => void;
  onRemove: () => void;
  onClose: () => void;
}> = ({ from, to, current, onPick, onRemove, onClose }) => (
  <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4">
    <div className="w-full max-w-sm rounded-xl bg-white p-4 shadow-xl">
      <div className="mb-3 flex items-start justify-between">
        <div>
          <h3 className="text-sm font-bold text-slate-800">When should it run?</h3>
          <p className="text-[11px] text-slate-500">
            <span className="font-semibold">{from}</span> →{' '}
            <span className="font-semibold">{to}</span>
          </p>
        </div>
        <button type="button" onClick={onClose} aria-label="Close">
          <X className="h-4 w-4 text-slate-400" />
        </button>
      </div>
      <div className="space-y-1.5">
        {CONDITIONS.map((option) => (
          <button
            key={option.key}
            type="button"
            onClick={() => onPick(option.key)}
            className={`flex w-full items-center justify-between rounded-lg border px-3 py-2 text-left hover:bg-slate-50 ${
              current === option.key ? 'border-indigo-400 ring-1 ring-indigo-200' : 'border-slate-200'
            }`}
          >
            <span>
              <span className="block text-xs font-semibold text-slate-800">{option.label}</span>
              <span className="block text-[10px] text-slate-500">{option.hint}</span>
            </span>
            <span
              className="h-2.5 w-2.5 rounded-full"
              style={{ background: EDGE_COLOURS[option.key] }}
            />
          </button>
        ))}
      </div>
      {current && (
        <button
          type="button"
          onClick={onRemove}
          className="mt-3 w-full rounded-lg border border-rose-200 px-2 py-1.5 text-[11px] font-semibold text-rose-600 hover:bg-rose-50"
        >
          Remove this dependency
        </button>
      )}
    </div>
  </div>
);


const NewPipelineModal: React.FC<{
  onClose: () => void;
  onCreated: (id: string) => void | Promise<void>;
  onOpenSettings?: () => void;
}> = ({ onClose, onCreated, onOpenSettings }) => {
  const [name, setName] = useState('');
  const [cron, setCron] = useState('0 2 * * *');
  const [timezone, setTimezone] = useState('UTC');
  const [enabled, setEnabled] = useState(false);
  const [stepType, setStepType] = useState<StepType>('dbt_run');
  const [target, setTarget] = useState('*');
  const [preview, setPreview] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const handle = window.setTimeout(() => {
      api
        .previewSchedule(cron, timezone, 3)
        .then((res) => {
          setPreview(res.next_runs);
          setError(null);
        })
        .catch(() => {
          setPreview([]);
          setError('That cron expression or timezone is not valid.');
        });
    }, 350);
    return () => window.clearTimeout(handle);
  }, [cron, timezone]);

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const key = newKey();
      const created = await api.createPipeline({
        name: name.trim(),
        description: '',
        cron_expression: cron.trim(),
        timezone: timezone.trim() || 'UTC',
        is_enabled: enabled,
        overlap_policy: 'skip',
        max_parallel: 3,
        keep_runs: 100,
        timeout_seconds: 3600,
        max_retries: 0,
        retry_delay_seconds: 60,
        auto_rerun_interrupted: false,
        graph: {
          steps: [
            {
              key,
              name: stepType === 'dbt_test' ? 'dbt test' : 'Step 1',
              step_type: stepType,
              target,
              args: {},
              timeout_seconds: null,
              max_retries: null,
              retry_delay_seconds: null,
              position_x: 80,
              position_y: 100,
            },
          ],
          edges: [],
          params: [],
        },
      });
      await onCreated(created.id);
    } catch (err) {
      const detail = (err as { response?: { data?: { detail?: string } } }).response?.data?.detail;
      setError(detail ?? (err instanceof Error ? err.message : 'Could not create the pipeline.'));
    } finally {
      setBusy(false);
    }
  };

  const input =
    'w-full rounded-lg border border-slate-300 px-2.5 py-1.5 text-xs focus:border-indigo-500 focus:outline-hidden';

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4">
      <form onSubmit={submit} className="w-full max-w-md rounded-xl bg-white p-4 shadow-xl">
        <div className="mb-3 flex items-start justify-between">
          <div>
            <h3 className="text-sm font-bold text-slate-800">New pipeline</h3>
            <p className="text-[11px] text-slate-500">
              Start with one step; add the rest on the canvas.
            </p>
          </div>
          <button type="button" onClick={onClose} aria-label="Close">
            <X className="h-4 w-4 text-slate-400" />
          </button>
        </div>

        <div className="space-y-3">
          <label className="block">
            <span className="mb-1 block text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
              Name
            </span>
            <input
              className={input}
              value={name}
              required
              placeholder="Nightly dbt build"
              onChange={(event) => setName(event.target.value)}
            />
          </label>

          <div>
            <span className="mb-1 block text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
              Schedule
            </span>
            <div className="mb-1.5 flex flex-wrap gap-1">
              {CRON_PRESETS.map((preset) => (
                <button
                  key={preset.expr}
                  type="button"
                  onClick={() => setCron(preset.expr)}
                  className={`rounded px-1.5 py-0.5 text-[10px] font-mono ${
                    cron === preset.expr
                      ? 'bg-indigo-600 text-white'
                      : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                  }`}
                >
                  {preset.label}
                </button>
              ))}
            </div>
            <div className="grid grid-cols-3 gap-2">
              <input
                className={`${input} col-span-2 font-mono`}
                value={cron}
                onChange={(event) => setCron(event.target.value)}
              />
              <input
                className={`${input} font-mono`}
                value={timezone}
                onChange={(event) => setTimezone(event.target.value)}
                placeholder="UTC"
              />
            </div>
            <p className="mt-1 text-[10px] text-slate-400">
              {preview.length
                ? `Next: ${preview.map((value) => fmtWhen(value)).join(' · ')}`
                : 'Five-field cron, interpreted in the timezone.'}
            </p>
          </div>


          <div className="grid grid-cols-2 gap-2">
            <label className="block">
              <span className="mb-1 block text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
                First step
              </span>
              <select
                className={input}
                value={stepType}
                onChange={(event) => setStepType(event.target.value as StepType)}
              >
                {STEP_TEMPLATES.map((template) => (
                  <option key={template.type} value={template.type}>
                    {template.name}
                  </option>
                ))}
              </select>
            </label>
            <label className="block">
              <span className="mb-1 block text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
                Target
              </span>
              <input
                className={`${input} font-mono`}
                value={target}
                onChange={(event) => setTarget(event.target.value)}
              />
            </label>
          </div>

          <label className="flex items-center gap-2 text-xs text-slate-600">
            <input
              type="checkbox"
              checked={enabled}
              onChange={(event) => setEnabled(event.target.checked)}
            />
            Enable the schedule immediately
          </label>

          {error && <p className="text-[11px] text-rose-600">{error}</p>}

          {onOpenSettings && (
            <button
              type="button"
              onClick={onOpenSettings}
              className="flex items-center gap-1 text-[10px] font-semibold text-indigo-600 hover:underline"
            >
              <Settings2 className="h-3 w-3" /> Variables and secrets live in Settings
            </button>
          )}
        </div>

        <div className="mt-4 flex items-center justify-end gap-2 border-t border-slate-200 pt-3">
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg px-3 py-1.5 text-[11px] font-semibold text-slate-600 hover:bg-slate-100"
          >
            Cancel
          </button>
          <button
            type="submit"
            disabled={busy || !name.trim()}
            className="flex items-center gap-1.5 rounded-lg bg-indigo-600 px-3 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700 disabled:opacity-50"
          >
            {busy && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
            Create pipeline
          </button>
        </div>
      </form>
    </div>
  );
};

