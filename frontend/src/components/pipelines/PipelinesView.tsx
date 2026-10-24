import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  AlertCircle,
  ChevronRight,
  Loader2,
  MoreHorizontal,
  Pause,
  Play,
  Plus,
  RefreshCw,
  Save,
  ScrollText,
  Trash2,
  Zap,
} from 'lucide-react';
import { api } from '../../lib/api';
import { usePolling } from '../../lib/viewActivity';
import { readApiError, toast } from '../../lib/toast';
import { useDragHandleProps, useResizableSize } from '../../lib/useResizableSize';
import type {
  CreatePipelinePayload,
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
import { ConfirmDialog, type ConfirmRequest } from '../common/ConfirmDialog';
import { EDGE_COLOURS, PipelineCanvas, type StepRuntime } from './PipelineCanvas';
import { PipelineInspector } from './PipelineInspector';
import { PipelineRail } from './PipelineRail';
import { PipelineRunsTable, fmtDuration } from './PipelineRunsTable';
import { PipelineSettingsForm } from './PipelineSettingsForm';
import { PipelineLogDock, type DockTab } from './PipelineLogDock';
import { PipelineRunDialog } from './PipelineRunDialog';

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

const STEP_TEMPLATES: Array<{ type: StepType; name: string; target: string }> = [
  { type: 'dbt_run', name: 'dbt run', target: '*' },
  { type: 'dbt_test', name: 'dbt test', target: '*' },
  { type: 'script', name: 'Script', target: '' },
  { type: 'notebook', name: 'Notebook', target: '' },
  { type: 'raw_sql', name: 'SQL', target: 'SELECT 1 AS ok' },
  { type: 'webhook', name: 'Webhook', target: '' },
  { type: 'pipeline', name: 'Pipeline run', target: '' },
];

const TERMINAL: ExecutionStatus[] = ['success', 'failed', 'cancelled', 'interrupted', 'skipped'];

const STATUS_TEXT: Record<string, string> = {
  success: 'Last run succeeded',
  failed: 'Last run failed',
  running: 'Running now',
  cancelled: 'Last run cancelled',
  interrupted: 'Last run interrupted',
  skipped: 'Last run skipped',
};

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

/** Key-ordered JSON, so comparing two graphs cannot hinge on object key order. */
const stable = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(stable).join(',')}]`;
  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stable(record[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value) ?? 'null';
};

/**
 * A comparison key for a graph. Edges are sorted and object keys normalised: the
 * server may return edges in a different order after a save (a re-conditioned edge
 * is deleted and re-inserted), and that must not read as an unsaved change.
 */
const graphSignature = (state: DraftState): string =>
  stable({
    steps: state.steps.map((step) => ({
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
    edges: [...state.edges]
      .map((edge) => `${edge.from_key}->${edge.to_key}:${edge.condition}`)
      .sort(),
  });

const newKey = () =>
  typeof crypto !== 'undefined' && 'randomUUID' in crypto
    ? crypto.randomUUID()
    : `step-${Date.now()}-${Math.random().toString(16).slice(2)}`;

type ViewTab = "graph" | "runs" | "settings";

export const PipelinesView: React.FC<PipelinesViewProps> = () => {
  const [pipelines, setPipelines] = useState<PipelineSummary[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<PipelineDetail | null>(null);
  const [draft, setDraft] = useState<DraftState>({ steps: [], edges: [] });
  const [targets, setTargets] = useState<PipelineTargets | null>(null);

  const [loadingDetail, setLoadingDetail] = useState(false);
  const [saving, setSaving] = useState(false);
  const [savingSettings, setSavingSettings] = useState(false);
  const [starting, setStarting] = useState(false);

  const [selectedStepKey, setSelectedStepKey] = useState<string | null>(null);
  // A connection dialog. ``existing`` decides whether the condition is added or
  // rewritten, so the same panel creates and edits.
  const [runDialogOpen, setRunDialogOpen] = useState(false);
  const [edgeDialog, setEdgeDialog] = useState<{
    from: string;
    to: string;
    existing: boolean;
  } | null>(null);
  const [tab, setTab] = useState<ViewTab>("graph");

  const [executions, setExecutions] = useState<PipelineExecution[]>([]);
  const [monitor, setMonitor] = useState<PipelineExecutionDetail | null>(null);
  // The newest finished run paints the designer; without it a pipeline that ran a
  // minute ago still claims no step has ever run.
  const [latestRun, setLatestRun] = useState<PipelineExecutionDetail | null>(null);
  const [stepLogs, setStepLogs] = useState("");
  const [logsOffset, setLogsOffset] = useState(0);
  const [loadingLogs, setLoadingLogs] = useState(false);
  const [runLog, setRunLog] = useState<string | null>(null);
  const [loadingRunLog, setLoadingRunLog] = useState(false);

  const [dockOpen, setDockOpen] = useState(false);
  const [dockTab, setDockTab] = useState<DockTab>("step");
  const [dockExpanded, setDockExpanded] = useState(false);

  const [problemSteps, setProblemSteps] = useState<string[]>([]);
  const [problemEdges, setProblemEdges] = useState<Array<{ from_key: string; to_key: string }>>([]);
  const [confirmRequest, setConfirmRequest] = useState<ConfirmRequest | null>(null);
  const [menuOpen, setMenuOpen] = useState(false);

  const [showNew, setShowNew] = useState(false);
  const [showAdd, setShowAdd] = useState(false);
  const [newName, setNewName] = useState("");
  const [newCron, setNewCron] = useState("0 2 * * *");
  const [newTimezone, setNewTimezone] = useState("UTC");
  const [creating, setCreating] = useState(false);

  const rail = useResizableSize("obelisk.pipelines.rail", 268, 190, 420);
  const inspector = useResizableSize("obelisk.pipelines.inspector", 320, 240, 520);
  const dock = useResizableSize("obelisk.pipelines.dock", 260, 140, 640);
  const railHandle = useDragHandleProps(rail.startDrag, () => rail.setSize(268));
  const inspectorHandle = useDragHandleProps(inspector.startDrag, () => inspector.setSize(320), {
    invert: true,
  });
  const dockHandle = useDragHandleProps(dock.startDrag, () => dock.setSize(260), {
    invert: true,
    axis: "y",
  });

  // ------------------------------------------------------------------ derived

  const dirty = useMemo(() => {
    if (!detail) return false;
    return graphSignature(draftFrom(detail)) !== graphSignature(draft);
  }, [detail, draft]);

  const selectedStep = useMemo(
    () => draft.steps.find((step) => step.key === selectedStepKey) ?? null,
    [draft.steps, selectedStepKey],
  );

  const painted = monitor ?? latestRun;
  const paintedByStep = useMemo(() => {
    const map: Record<string, StepRunItem> = {};
    for (const stepRun of painted?.step_runs ?? []) map[stepRun.step_id] = stepRun;
    return map;
  }, [painted]);

  const runtimeByStep = useMemo(() => {
    const map: Record<string, StepRuntime> = {};
    for (const [stepId, stepRun] of Object.entries(paintedByStep)) {
      map[stepId] = {
        status: stepRun.status,
        duration_seconds: stepRun.duration_seconds,
        attempt: stepRun.attempt,
        error: stepRun.error,
        outputs: Object.keys(stepRun.outputs ?? {}).length,
      };
    }
    return map;
  }, [paintedByStep]);

  const selectedStepRun = selectedStepKey
    ? (monitor?.step_runs ?? []).find((run) => run.step_id === selectedStepKey) ?? null
    : null;
  const monitorActive = monitor != null && !TERMINAL.includes(monitor.status);
  // Watching any run (live or finished) is a read-only mode: the inspector shows what
  // happened, and 'Back to design' returns to editing the graph.
  const watching = monitor != null;

  const dockSteps = useMemo(() => {
    const rows = [...(monitor?.step_runs ?? [])];
    rows.sort((left, right) => {
      const leftKey = left.started_at ?? left.claimed_at ?? "";
      const rightKey = right.started_at ?? right.claimed_at ?? "";
      return leftKey.localeCompare(rightKey);
    });
    return rows;
  }, [monitor]);

  const parentsByStep = useMemo(() => {
    const names = new Map(draft.steps.map((step) => [step.key, step.name]));
    const map: Record<string, string[]> = {};
    for (const edge of draft.edges) {
      const name = names.get(edge.from_key);
      if (!name) continue;
      map[edge.to_key] = [...(map[edge.to_key] ?? []), name];
    }
    return map;
  }, [draft.steps, draft.edges]);

  const connections = useMemo(() => {
    if (!selectedStepKey) return [];
    const name = (key: string) =>
      draft.steps.find((step) => step.key === key)?.name ?? 'deleted step';
    const incoming = draft.edges
      .filter((edge) => edge.to_key === selectedStepKey)
      .map((edge) => ({
        direction: 'in' as const,
        from_key: edge.from_key,
        to_key: edge.to_key,
        condition: edge.condition,
        name: name(edge.from_key),
      }));
    const outgoing = draft.edges
      .filter((edge) => edge.from_key === selectedStepKey)
      .map((edge) => ({
        direction: 'out' as const,
        from_key: edge.from_key,
        to_key: edge.to_key,
        condition: edge.condition,
        name: name(edge.to_key),
      }));
    return [...incoming, ...outgoing];
  }, [draft.edges, draft.steps, selectedStepKey]);

  const clearProblems = useCallback(() => {
    setProblemSteps([]);
    setProblemEdges([]);
  }, []);

  // ------------------------------------------------------------------- loaders

  const refreshList = useCallback(async () => {
    try {
      const list = await api.listPipelines();
      setPipelines(list);
      setSelectedId((current) => current ?? list[0]?.id ?? null);
    } catch (error) {
      toast.error(readApiError(error, "Could not load pipelines.").message);
    }
  }, []);

  const openPipeline = useCallback(async (pipelineId: string) => {
    setLoadingDetail(true);
    clearProblems();
    try {
      const data = await api.getPipeline(pipelineId);
      setDetail(data);
      setDraft(draftFrom(data));
      setSelectedStepKey(null);
      setEdgeDialog(null);
      setMonitor(null);
      setStepLogs("");
      setLogsOffset(0);
      setRunLog(null);
      const runs = await api.listPipelineExecutions({ pipeline_id: pipelineId, limit: 40 });
      setExecutions(runs);
      const newest = runs.find((run) => TERMINAL.includes(run.status)) ?? runs[0] ?? null;
      setLatestRun(newest ? await api.getPipelineExecution(newest.id) : null);
    } catch (error) {
      toast.error(readApiError(error, "Could not load this pipeline.").message);
    } finally {
      setLoadingDetail(false);
    }
  }, [clearProblems]);

  useEffect(() => {
    void refreshList();
    api.getPipelineTargets().then(setTargets).catch(() => setTargets(null));
  }, [refreshList]);

  useEffect(() => {
    if (selectedId) void openPipeline(selectedId);
  }, [selectedId, openPipeline]);

  const refreshExecutions = useCallback(async () => {
    if (!selectedId) return;
    try {
      setExecutions(await api.listPipelineExecutions({ pipeline_id: selectedId, limit: 40 }));
    } catch {
      /* the list refresh is best effort */
    }
  }, [selectedId]);

  const refreshMonitor = useCallback(async () => {
    if (!monitor?.id) return;
    try {
      const fresh = await api.getPipelineExecution(monitor.id);
      setMonitor(fresh);
      if (TERMINAL.includes(fresh.status)) {
        setLatestRun(fresh);
        void refreshExecutions();
      }
    } catch {
      /* keep the last known picture */
    }
  }, [monitor?.id, refreshExecutions]);

  const loadStepLog = useCallback(async (stepRunId: string) => {
    setLoadingLogs(true);
    try {
      const tail = await api.getStepRunLogs(stepRunId, 0);
      setStepLogs(tail.logs ?? "");
      setLogsOffset(tail.size);
    } catch {
      setStepLogs("");
    } finally {
      setLoadingLogs(false);
    }
  }, []);

  const loadRunLog = useCallback(async (runId: string) => {
    setLoadingRunLog(true);
    try {
      const res = await api.getRunLogs(runId);
      setRunLog(res.logs ?? "");
    } catch {
      setRunLog("Could not load this run log.");
    } finally {
      setLoadingRunLog(false);
    }
  }, []);

  const appendStepLog = useCallback(async () => {
    if (!selectedStepRun) return;
    try {
      const tail = await api.getStepRunLogs(selectedStepRun.id, logsOffset);
      if (tail.logs) setStepLogs((current) => current + tail.logs);
      setLogsOffset(tail.size);
    } catch {
      /* the tail is best effort */
    }
  }, [selectedStepRun, logsOffset]);

  // Three cadences: the list when idle, the run while it is live, the tail while its
  // step is. All of them pause when the tab is hidden (usePolling).
  usePolling(refreshList, 15000);
  usePolling(refreshExecutions, monitorActive ? 2500 : 10000);
  usePolling(() => (monitorActive ? refreshMonitor() : undefined), 1500);
  usePolling(
    () =>
      selectedStepRun && !TERMINAL.includes(selectedStepRun.status as ExecutionStatus)
        ? appendStepLog()
        : undefined,
    1500,
  );
  usePolling(
    () =>
      dockOpen && dockTab === "run" && monitorActive && monitor?.id
        ? loadRunLog(monitor.id)
        : undefined,
    2500,
  );

  useEffect(() => {
    if (selectedStepRun?.id) {
      void loadStepLog(selectedStepRun.id);
      return;
    }
    setStepLogs("");
    setLogsOffset(0);
  }, [selectedStepRun?.id, loadStepLog]);

  // A run that has been opened in the Runs tab is also the run the dock shows.
  useEffect(() => {
    if (dockOpen && dockTab === "run" && monitor?.id) void loadRunLog(monitor.id);
  }, [dockOpen, dockTab, monitor?.id, loadRunLog]);

  // ------------------------------------------------------------------- actions

  const saveGraph = useCallback(async () => {
    if (!detail) return;
    setSaving(true);
    clearProblems();
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
      toast.success("Pipeline saved.");
      void refreshList();
    } catch (error) {
      const problem = readApiError(error, "Could not save the pipeline.");
      setProblemSteps(problem.steps);
      setProblemEdges(problem.edges);
      // Errors carry the keys of the offending nodes; painting them is the whole
      // point of the structured payload.
      if (problem.steps.length === 1) setSelectedStepKey(problem.steps[0]);
      toast.error(
        problem.message,
        problem.steps.length || problem.edges.length
          ? "The offending step or edge is highlighted on the canvas."
          : undefined,
      );
    } finally {
      setSaving(false);
    }
  }, [detail, draft, clearProblems, refreshList]);

  const startRun = useCallback(
    async (params?: Record<string, unknown>) => {
      if (!detail) return;
      setStarting(true);
      try {
        const { run_id } = await api.runPipelineNow(detail.id, params);
        const fresh = await api.getPipelineExecution(run_id);
        setMonitor(fresh);
        setTab("graph");
        setSelectedStepKey(fresh.step_runs[0]?.step_id ?? null);
        setDockOpen(true);
        setDockTab("step");
        setRunDialogOpen(false);
        void refreshExecutions();
        void refreshList();
      } catch (error) {
        toast.error(readApiError(error, "Could not start the run.").message);
      } finally {
        setStarting(false);
      }
    },
    [detail, refreshExecutions, refreshList],
  );

  // Required parameters cannot be guessed, so a pipeline that declares any asks for
  // them first instead of failing with 'Parameter x is required.'
  const requestRun = useCallback(() => {
    if (!detail) return;
    if (detail.params.length) {
      setRunDialogOpen(true);
      return;
    }
    void startRun();
  }, [detail, startRun]);

  const openRun = useCallback(async (run: PipelineExecution) => {
    try {
      const fresh = await api.getPipelineExecution(run.id);
      setMonitor(fresh);
      setTab("graph");
      setSelectedStepKey(fresh.step_runs[0]?.step_id ?? null);
      setDockOpen(true);
      setDockTab("step");
    } catch (error) {
      toast.error(readApiError(error, "Could not open this run.").message);
    }
  }, []);

  const cancelRun = useCallback(
    async (runId: string) => {
      try {
        await api.cancelPipelineExecution(runId);
        await refreshMonitor();
        await refreshExecutions();
        toast.info("Cancellation requested.");
      } catch (error) {
        toast.error(readApiError(error, "Could not cancel this run.").message);
      }
    },
    [refreshMonitor, refreshExecutions],
  );

  const rerunFromSelected = async () => {
    if (!monitor || !selectedStepKey) return;
    try {
      const { run_id } = await api.rerunFromStep(monitor.id, selectedStepKey);
      const fresh = await api.getPipelineExecution(run_id);
      setMonitor(fresh);
      setDockOpen(true);
      void refreshExecutions();
      toast.success("Re-running from the selected step.");
    } catch (error) {
      toast.error(readApiError(error, "Could not re-run from this step.").message);
    }
  };

  const togglePipeline = async () => {
    if (!detail) return;
    try {
      await api.togglePipeline(detail.id);
      await openPipeline(detail.id);
      void refreshList();
      toast.success(detail.is_enabled ? "Schedule paused." : "Schedule enabled.");
    } catch (error) {
      toast.error(readApiError(error, "Could not change the schedule.").message);
    }
    setMenuOpen(false);
  };

  const deletePipeline = () => {
    if (!detail) return;
    setMenuOpen(false);
    setConfirmRequest({
      title: `Delete “${detail.name}”?`,
      body: "The graph, its schedules and its run history are removed. This cannot be undone.",
      confirmLabel: "Delete pipeline",
      destructive: true,
      onConfirm: async () => {
        try {
          await api.deletePipeline(detail.id);
          setDetail(null);
          setMonitor(null);
          setLatestRun(null);
          setSelectedId(null);
          await refreshList();
          toast.success("Pipeline deleted.");
        } catch (error) {
          toast.error(readApiError(error, "Could not delete the pipeline.").message);
        }
      },
    });
  };

  const saveSettings = async (patch: Partial<CreatePipelinePayload>) => {
    if (!detail) return;
    setSavingSettings(true);
    try {
      await api.updatePipeline(detail.id, patch);
      const fresh = await api.getPipeline(detail.id);
      setDetail(fresh);
      void refreshList();
      toast.success("Settings saved.");
    } catch (error) {
      toast.error(readApiError(error, "Could not save the settings.").message);
    } finally {
      setSavingSettings(false);
    }
  };

  const createPipeline = async () => {
    if (!newName.trim()) {
      toast.error("Give the pipeline a name.");
      return;
    }
    setCreating(true);
    try {
      const created = await api.createPipeline({
        name: newName.trim(),
        cron_expression: newCron.trim() || "0 2 * * *",
        timezone: newTimezone.trim() || "UTC",
        is_enabled: false,
        overlap_policy: "skip",
        max_parallel: 4,
        keep_runs: 20,
        timeout_seconds: 3600,
        max_retries: 0,
        retry_delay_seconds: 60,
        auto_rerun_interrupted: false,
      });
      setShowNew(false);
      setNewName("");
      await refreshList();
      setSelectedId(created.id);
      setTab("graph");
      toast.success("Pipeline created. Add its first step.");
    } catch (error) {
      toast.error(readApiError(error, "Could not create the pipeline.").message);
    } finally {
      setCreating(false);
    }
  };

  // ------------------------------------------------------------- graph editing

  const addStep = (type: StepType) => {
    const template = STEP_TEMPLATES.find((entry) => entry.type === type) ?? STEP_TEMPLATES[0];
    const index = draft.steps.length;
    const key = newKey();
    const duplicate = draft.steps.some((step) => step.step_type === type);
    const step: PipelineStepDef = {
      id: key,
      key,
      name: `${template.name}${duplicate ? ` ${index + 1}` : ""}`,
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
    setShowAdd(false);
    setTab("graph");
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
    const step = draft.steps.find((entry) => entry.key === stepKey);
    if (!step) return;
    setConfirmRequest({
      title: `Remove “${step.name}”?`,
      body: "Its edges are removed too. Nothing runs until you save.",
      confirmLabel: "Remove step",
      destructive: true,
      onConfirm: () => {
        setDraft((current) => ({
          steps: current.steps.filter((entry) => entry.key !== stepKey),
          edges: current.edges.filter(
            (edge) => edge.from_key !== stepKey && edge.to_key !== stepKey,
          ),
        }));
        setSelectedStepKey((current) => (current === stepKey ? null : current));
        // An edge dialog pointing at this step is now meaningless.
        setEdgeDialog(null);
      },
    });
  };

  const setEdgeCondition = (condition: EdgeCondition) => {
    if (!edgeDialog) return;
    const { from, to } = edgeDialog;
    setDraft((current) => ({
      ...current,
      edges: [
        // One edge per pair: a rewritten condition replaces the old edge.
        ...current.edges.filter((edge) => !(edge.from_key === from && edge.to_key === to)),
        { from_key: from, to_key: to, condition },
      ],
    }));
    setEdgeDialog(null);
  };

  const removeEdge = (fromKey: string, toKey: string) => {
    setDraft((current) => ({
      ...current,
      edges: current.edges.filter(
        (edge) => !(edge.from_key === fromKey && edge.to_key === toKey),
      ),
    }));
    setEdgeDialog(null);
  };

  // Shortcuts live here, not in the canvas: saving and running are view actions, and
  // the canvas is a child that must not own the page's keys.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const meta = event.metaKey || event.ctrlKey;
      if (meta && event.key.toLowerCase() === "s") {
        event.preventDefault();
        if (dirty) void saveGraph();
        return;
      }
      if (meta && event.key === "Enter") {
        event.preventDefault();
        if (!starting) requestRun();
        return;
      }
      if (event.key === "Escape") {
        setEdgeDialog(null);
        setRunDialogOpen(false);
        setMenuOpen(false);
        setShowAdd(false);
        setShowNew(false);
        clearProblems();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [dirty, saveGraph, starting, clearProblems, requestRun]);

  // ---------------------------------------------------------------------- view

  const status = painted?.status ?? detail?.last_status ?? "idle";
  const runningLabel = monitorActive ? STATUS_TEXT.running : STATUS_TEXT[status] ?? "Never run";
  const showDock = dockOpen && tab === "graph";

  return (
    <div className="flex h-full w-full min-h-0 flex-1 flex-col bg-slate-50">
      <header className="flex items-center gap-3 border-b border-slate-200 bg-white px-4 py-2">
        <nav className="flex min-w-0 items-center gap-1 text-xs" aria-label="Breadcrumb">
          <span className="font-semibold text-slate-400">Pipelines</span>
          {detail && (
            <>
              <ChevronRight className="h-3 w-3 text-slate-300" />
              <span className="truncate font-semibold text-slate-800">{detail.name}</span>
            </>
          )}
        </nav>

        {detail && (
          <div className="flex min-w-0 items-center gap-1.5">
            <span
              className={`shrink-0 rounded-full border px-2 py-0.5 text-[10px] font-bold uppercase ${
                detail.is_enabled
                  ? "border-emerald-200 bg-emerald-50 text-emerald-700"
                  : "border-slate-200 bg-slate-50 text-slate-500"
              }`}
            >
              {detail.is_enabled ? "Scheduled" : "Paused"}
            </span>
            <span className="hidden truncate font-mono text-[10px] text-slate-400 lg:inline">
              {detail.cron_expression} · {detail.timezone}
              {detail.next_run_at && detail.is_enabled
                ? ` · next ${new Date(detail.next_run_at).toLocaleString()}`
                : ""}
            </span>
            <span
              className={`shrink-0 rounded-full px-2 py-0.5 text-[10px] font-semibold ${
                monitorActive ? "bg-indigo-50 text-indigo-700" : "bg-slate-100 text-slate-500"
              }`}
            >
              {monitorActive && <Loader2 className="mr-1 inline h-2.5 w-2.5 animate-spin" />}
              {runningLabel}
            </span>
            {monitor?.parent?.pipeline_name && (
              <button
                type="button"
                onClick={() =>
                  monitor.parent?.pipeline_id && setSelectedId(monitor.parent.pipeline_id)
                }
                className="shrink-0 rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[10px] font-semibold text-slate-600 hover:border-indigo-300 hover:text-indigo-700"
                title="Open the pipeline that started this run"
              >
                Started by {monitor.parent.pipeline_name}
                {monitor.parent.step_name ? ` ? ${monitor.parent.step_name}` : ""}
              </button>
            )}
            {dirty && (
              <span className="shrink-0 rounded-full bg-amber-50 px-2 py-0.5 text-[10px] font-bold uppercase text-amber-700">
                Unsaved
              </span>
            )}
          </div>
        )}

        <div className="ml-auto flex shrink-0 items-center gap-1.5">
          {detail && (
            <>
              <button
                type="button"
                onClick={requestRun}
                disabled={starting}
                className="flex items-center gap-1.5 rounded-lg bg-indigo-600 px-2.5 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700 disabled:opacity-50"
                title="Run now (Ctrl+Enter)"
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
                onClick={() => void saveGraph()}
                disabled={!dirty || saving}
                className="flex items-center gap-1.5 rounded-lg border border-indigo-300 bg-indigo-50 px-2.5 py-1.5 text-[11px] font-semibold text-indigo-700 hover:bg-indigo-100 disabled:opacity-40"
                title="Save graph (Ctrl+S)"
              >
                {saving ? (
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                ) : (
                  <Save className="h-3.5 w-3.5" />
                )}
                Save
              </button>
              <div className="relative">
                <button
                  type="button"
                  onClick={() => setMenuOpen((open) => !open)}
                  className="rounded-lg border border-slate-300 p-1.5 text-slate-600 hover:bg-slate-50"
                  aria-label="Pipeline actions"
                  aria-expanded={menuOpen}
                >
                  <MoreHorizontal className="h-3.5 w-3.5" />
                </button>
                {menuOpen && (
                  <div className="absolute right-0 top-9 z-30 w-[184px] overflow-hidden rounded-lg border border-slate-200 bg-white py-1 shadow-lg">
                    <button
                      type="button"
                      onClick={() => void togglePipeline()}
                      className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[11px] text-slate-700 hover:bg-slate-50"
                    >
                      {detail.is_enabled ? (
                        <Pause className="h-3.5 w-3.5" />
                      ) : (
                        <Zap className="h-3.5 w-3.5" />
                      )}
                      {detail.is_enabled ? "Pause schedule" : "Enable schedule"}
                    </button>
                    <button
                      type="button"
                      onClick={() => {
                        setMenuOpen(false);
                        setTab("settings");
                      }}
                      className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[11px] text-slate-700 hover:bg-slate-50"
                    >
                      <RefreshCw className="h-3.5 w-3.5" /> Pipeline settings
                    </button>
                    <button
                      type="button"
                      onClick={deletePipeline}
                      className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[11px] text-rose-600 hover:bg-rose-50"
                    >
                      <Trash2 className="h-3.5 w-3.5" /> Delete pipeline
                    </button>
                  </div>
                )}
              </div>
            </>
          )}
          {!detail && (
            <button
              type="button"
              onClick={() => setShowNew(true)}
              className="rounded-lg bg-indigo-600 px-2.5 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700"
            >
              New pipeline
            </button>
          )}
        </div>
      </header>

      <div className="flex min-h-0 flex-1">
        <aside
          className="shrink-0 border-r border-slate-200 bg-white"
          style={{ width: rail.size }}
        >
          <PipelineRail
            pipelines={pipelines}
            selectedId={selectedId}
            onSelect={(id) => {
              setSelectedId(id);
              setTab("graph");
            }}
            onNew={() => setShowNew(true)}
          />
        </aside>
        <div
          {...railHandle}
          className="w-1 shrink-0 cursor-col-resize bg-transparent transition-colors hover:bg-indigo-300"
          aria-label="Resize pipeline list"
        />

        <main className="flex min-w-0 flex-1 flex-col">
          <div className="flex items-center gap-1 border-b border-slate-200 bg-white px-3">
            {([
              ["graph", "Graph"],
              ["runs", `Runs${executions.length ? ` · ${executions.length}` : ""}`],
              ["settings", "Settings"],
            ] as Array<[ViewTab, string]>).map(([key, label]) => (
              <button
                key={key}
                type="button"
                onClick={() => setTab(key)}
                className={`-mb-px border-b-2 px-2.5 py-2 text-[11px] font-semibold ${
                  tab === key
                    ? "border-indigo-600 text-indigo-700"
                    : "border-transparent text-slate-500 hover:text-slate-700"
                }`}
              >
                {label}
              </button>
            ))}
            {detail && tab === "graph" && (
              <div className="ml-auto flex items-center gap-2 py-1">
                {watching && (
                  <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-semibold text-slate-500">
                    Read-only while watching a run
                  </span>
                )}
                {monitor && (
                  <span className="flex items-center gap-1.5 rounded-md bg-slate-100 px-2 py-0.5 text-[10px] text-slate-600">
                    Watching <span className="font-mono">{monitor.id.slice(0, 8)}</span>
                    <span className="text-slate-400">·</span>
                    <span>{fmtDuration(monitor.duration_seconds)}</span>
                    <button
                      type="button"
                      onClick={() => setMonitor(null)}
                      className="font-semibold text-indigo-600 hover:underline"
                    >
                      Back to design
                    </button>
                  </span>
                )}
              </div>
            )}
          </div>

          {!detail && (
            <div className="flex flex-1 items-center justify-center p-10 text-center">
              <div className="max-w-[320px] space-y-2">
                {loadingDetail ? (
                  <Loader2 className="mx-auto h-5 w-5 animate-spin text-indigo-500" />
                ) : (
                  <>
                    <p className="text-sm font-semibold text-slate-700">No pipeline selected</p>
                    <p className="text-[11px] leading-relaxed text-slate-500">
                      Pick one on the left, or create a pipeline to schedule dbt, a script, a
                      notebook, SQL or a webhook.
                    </p>
                    <button
                      type="button"
                      onClick={() => setShowNew(true)}
                      className="rounded-lg bg-indigo-600 px-3 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700"
                    >
                      New pipeline
                    </button>
                  </>
                )}
              </div>
            </div>
          )}

          {detail && tab === "graph" && (
            <div className="flex min-h-0 flex-1 flex-col">
              <div className="flex items-center gap-2 border-b border-slate-200 bg-white px-3 py-1.5">
                <button
                  type="button"
                  onClick={() => setShowAdd(true)}
                  className="flex items-center gap-1.5 rounded-lg bg-indigo-600 px-2.5 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700"
                >
                  <Plus className="h-3.5 w-3.5" /> Add step
                </button>
                <span className="text-[11px] text-slate-500">
                  {draft.steps.length} step{draft.steps.length === 1 ? "" : "s"} ?{" "}
                  {draft.edges.length} connection{draft.edges.length === 1 ? "" : "s"}
                </span>
                {!draft.steps.length && (
                  <span className="rounded-full bg-amber-50 px-2 py-0.5 text-[10px] font-semibold text-amber-700">
                    Add a step before saving
                  </span>
                )}
                <div className="ml-auto flex items-center gap-2">
                  <div className="hidden items-center gap-2 xl:flex">
                    {(["success", "failure", "completion", "skipped"] as const).map((condition) => (
                      <span
                        key={condition}
                        className="flex items-center gap-1 text-[10px] text-slate-400"
                      >
                        <span
                          className="h-1.5 w-3 rounded-full"
                          style={{ background: EDGE_COLOURS[condition] }}
                        />
                        {condition}
                      </span>
                    ))}
                  </div>
                  <button
                    type="button"
                    onClick={() => setDockOpen((open) => !open)}
                    className={`flex items-center gap-1 rounded-md border px-1.5 py-0.5 text-[10px] font-semibold ${
                      dockOpen
                        ? "border-indigo-200 bg-indigo-50 text-indigo-700"
                        : "border-slate-200 text-slate-600 hover:bg-slate-50"
                    }`}
                  >
                    <ScrollText className="h-3 w-3" /> Logs
                  </button>
                </div>
              </div>
              <div className={`min-h-0 flex-1 ${dockExpanded && showDock ? "hidden" : "flex"}`}>
                <div className="min-w-0 flex-1 bg-slate-50">
                  <PipelineCanvas
                    steps={draft.steps}
                    edges={draft.edges}
                    editable={!watching}
                    fitKey={`${detail.id}:${detail.updated_at ?? ""}`}
                    runtimeByStep={runtimeByStep}
                    selectedStepId={selectedStepKey}
                    problemSteps={problemSteps}
                    problemEdges={problemEdges}
                    onSelectStep={(key) => {
                      setSelectedStepKey(key);
                      // Choosing a step means the edge panel is no longer the subject.
                      setEdgeDialog(null);
                      // A step inside a run has output worth seeing; open the dock so
                      // clicking a node is enough to read it.
                      if (key && monitor) setDockOpen(true);
                    }}
                    onConnectRequest={(from, to) =>
                      setEdgeDialog({ from, to, existing: false })
                    }
                    selectedEdge={
                      edgeDialog ? { from_key: edgeDialog.from, to_key: edgeDialog.to } : null
                    }
                    onEdgeClick={(edge) =>
                      setEdgeDialog({ from: edge.from_key, to: edge.to_key, existing: true })
                    }
                    onEdgeDelete={removeEdge}
                    onDeleteStep={(key) => removeStep(key)}
                    onMoveStep={moveStep}
                    onAddStep={() => setShowAdd(true)}
                  />
                </div>
                <div
                  {...inspectorHandle}
                  className="w-1 shrink-0 cursor-col-resize bg-transparent transition-colors hover:bg-indigo-300"
                  aria-label="Resize step inspector"
                />
                <div
                  className="shrink-0 overflow-hidden border-l border-slate-200 bg-white"
                  style={{ width: inspector.size }}
                >
                  <PipelineInspector
                    step={selectedStep}
                    runtime={selectedStepKey ? runtimeByStep[selectedStepKey] : undefined}
                    stepRun={selectedStepRun}
                    editable={!watching}
                    targets={targets}
                    pipelines={pipelines.filter((item) => item.id !== detail.id)}
                    parents={selectedStepKey ? parentsByStep[selectedStepKey] ?? [] : []}
                    connections={connections}
                    onEditEdge={(from, to) => setEdgeDialog({ from, to, existing: true })}
                    onRemoveEdge={removeEdge}
                    onChange={updateStep}
                    onDelete={() => selectedStepKey && removeStep(selectedStepKey)}
                    onOpenLogs={() => {
                      setDockOpen(true);
                      setDockTab("step");
                    }}
                    onRerunFrom={
                      watching && selectedStepRun && !monitorActive ? rerunFromSelected : undefined
                    }
                  />
                </div>
              </div>

              {showDock && (
                <>
                  <div
                    {...dockHandle}
                    className={`h-1 shrink-0 cursor-row-resize transition-colors hover:bg-indigo-300 ${
                      dockExpanded ? "hidden" : "bg-slate-200"
                    }`}
                    aria-label="Resize log panel"
                  />
                  <div
                    className="min-h-0 shrink-0"
                    style={dockExpanded ? { flex: 1 } : { height: dock.size }}
                  >
                    <PipelineLogDock
                      open={dockOpen}
                      tab={dockTab}
                      onTabChange={setDockTab}
                      steps={dockSteps}
                      selectedStepId={selectedStepKey}
                      onSelectStep={setSelectedStepKey}
                      stepLog={stepLogs}
                      runLog={runLog}
                      loadingStep={loadingLogs}
                      loadingRun={loadingRunLog}
                      expanded={dockExpanded}
                      onToggleExpand={() => setDockExpanded((value) => !value)}
                      onRefresh={() => {
                        if (dockTab === "run" && monitor?.id) void loadRunLog(monitor.id);
                        else if (selectedStepRun) void loadStepLog(selectedStepRun.id);
                      }}
                      onClose={() => {
                        setDockOpen(false);
                        setDockExpanded(false);
                      }}
                    />
                  </div>
                </>
              )}
            </div>
          )}

          {detail && tab === "runs" && (
            <div className="min-h-0 flex-1 overflow-hidden bg-white">
              <PipelineRunsTable
                runs={executions}
                selectedRunId={monitor?.id ?? null}
                onOpen={(run) => void openRun(run)}
                onCancel={(run) => void cancelRun(run.id)}
              />
            </div>
          )}

          {detail && tab === "settings" && (
            <div className="min-h-0 flex-1 bg-white">
              <PipelineSettingsForm
                pipeline={detail}
                saving={savingSettings}
                onSave={saveSettings}
                onDelete={deletePipeline}
              />
            </div>
          )}
        </main>
      </div>

      {edgeDialog && (
        <div className="fixed inset-0 z-[70] flex items-center justify-center bg-slate-900/25 p-4">
          <div className="w-[380px] max-w-full rounded-xl border border-slate-200 bg-white p-4 shadow-xl">
            <h2 className="text-sm font-semibold text-slate-800">
              {edgeDialog.existing ? "Edit connection" : "When should this run?"}
            </h2>
            <p className="mt-1 flex items-center gap-1 text-[11px] leading-relaxed text-slate-500">
              <span className="truncate font-semibold text-slate-700">
                {draft.steps.find((step) => step.key === edgeDialog.from)?.name ?? "Step"}
              </span>
              <ChevronRight className="h-3 w-3 shrink-0" />
              <span className="truncate font-semibold text-slate-700">
                {draft.steps.find((step) => step.key === edgeDialog.to)?.name ?? "step"}
              </span>
            </p>
            <p className="mt-2 text-[10px] font-bold uppercase tracking-wide text-slate-400 font-mono">
              Run the target when the source
            </p>
            <div className="mt-1 grid grid-cols-2 gap-1.5">
              {(
                [
                  ["success", "Succeeds"],
                  ["failure", "Fails"],
                  ["completion", "Completes (either)"],
                  ["skipped", "Is skipped"],
                ] as Array<[EdgeCondition, string]>
              ).map(([condition, label]) => {
                const current =
                  draft.edges.find(
                    (edge) =>
                      edge.from_key === edgeDialog.from && edge.to_key === edgeDialog.to,
                  )?.condition === condition;
                return (
                  <button
                    key={condition}
                    type="button"
                    onClick={() => setEdgeCondition(condition)}
                    className={`rounded-lg border px-2 py-1.5 text-[11px] font-semibold ${
                      current
                        ? "border-indigo-500 bg-indigo-50 text-indigo-700"
                        : "border-slate-300 text-slate-700 hover:border-indigo-300 hover:bg-indigo-50"
                    }`}
                  >
                    {label}
                    {current && <span className="ml-1 text-[9px] uppercase">current</span>}
                  </button>
                );
              })}
            </div>
            <div className="mt-3 flex items-center justify-between">
              {edgeDialog.existing ? (
                <button
                  type="button"
                  onClick={() => removeEdge(edgeDialog.from, edgeDialog.to)}
                  className="flex items-center gap-1 text-[11px] font-semibold text-rose-600 hover:underline"
                >
                  <Trash2 className="h-3 w-3" /> Delete connection
                </button>
              ) : (
                <span className="text-[10px] text-slate-400">
                  Esc cancels
                </span>
              )}
              <button
                type="button"
                onClick={() => setEdgeDialog(null)}
                className="rounded-lg border border-slate-300 px-2.5 py-1 text-[11px] font-semibold text-slate-600 hover:bg-slate-50"
              >
                {edgeDialog.existing ? "Close" : "Cancel"}
              </button>
            </div>
          </div>
        </div>
      )}

      {showAdd && (
        <div className="fixed inset-0 z-[70] flex items-center justify-center bg-slate-900/25 p-4">
          <div className="w-[420px] max-w-full rounded-xl border border-slate-200 bg-white p-4 shadow-xl">
            <h2 className="text-sm font-semibold text-slate-800">Add a step</h2>
            <p className="mt-1 text-[11px] text-slate-500">
              Pick what this step runs; its settings appear in the inspector.
            </p>
            <div className="mt-3 grid grid-cols-2 gap-1.5">
              {STEP_TEMPLATES.map((template) => (
                <button
                  key={template.type}
                  type="button"
                  onClick={() => addStep(template.type)}
                  className="rounded-lg border border-slate-300 px-2 py-2 text-left text-[11px] font-semibold text-slate-700 hover:border-indigo-300 hover:bg-indigo-50"
                >
                  {template.name}
                  <span className="mt-0.5 block font-mono text-[9px] font-normal uppercase text-slate-400">
                    {template.type}
                  </span>
                </button>
              ))}
            </div>
            <div className="mt-3 flex justify-end">
              <button
                type="button"
                onClick={() => setShowAdd(false)}
                className="rounded-lg border border-slate-300 px-2.5 py-1 text-[11px] font-semibold text-slate-600 hover:bg-slate-50"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {showNew && (
        <div className="fixed inset-0 z-[70] flex items-center justify-center bg-slate-900/25 p-4">
          <div className="w-[440px] max-w-full rounded-xl border border-slate-200 bg-white p-4 shadow-xl">
            <h2 className="text-sm font-semibold text-slate-800">New pipeline</h2>
            <p className="mt-1 text-[11px] text-slate-500">
              Start paused with an empty graph — you add steps on the canvas next.
            </p>
            <div className="mt-3 space-y-2">
              <label className="block">
                <span className="mb-1 block text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
                  Name
                </span>
                <input
                  autoFocus
                  value={newName}
                  onChange={(event) => setNewName(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") void createPipeline();
                  }}
                  className="w-full rounded-lg border border-slate-300 px-2.5 py-1.5 text-xs focus:border-indigo-500 focus:outline-hidden"
                  placeholder="Daily orders"
                />
              </label>
              <div className="grid grid-cols-[1.3fr_1fr] gap-2">
                <label className="block">
                  <span className="mb-1 block text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
                    Cron
                  </span>
                  <input
                    value={newCron}
                    onChange={(event) => setNewCron(event.target.value)}
                    className="w-full rounded-lg border border-slate-300 px-2.5 py-1.5 font-mono text-xs focus:border-indigo-500 focus:outline-hidden"
                  />
                </label>
                <label className="block">
                  <span className="mb-1 block text-[10px] font-bold uppercase tracking-wide text-slate-500 font-mono">
                    Timezone
                  </span>
                  <input
                    value={newTimezone}
                    onChange={(event) => setNewTimezone(event.target.value)}
                    className="w-full rounded-lg border border-slate-300 px-2.5 py-1.5 font-mono text-xs focus:border-indigo-500 focus:outline-hidden"
                  />
                </label>
              </div>
              <div className="flex flex-wrap gap-1.5">
                {CRON_PRESETS.map((preset) => (
                  <button
                    key={preset.expr}
                    type="button"
                    onClick={() => setNewCron(preset.expr)}
                    className="rounded-full border border-slate-300 px-2 py-0.5 text-[10px] font-semibold text-slate-600 hover:bg-slate-50"
                  >
                    {preset.label}
                  </button>
                ))}
              </div>
            </div>
            <div className="mt-4 flex items-center justify-between">
              <button
                type="button"
                onClick={() => setShowNew(false)}
                className="rounded-lg border border-slate-300 px-2.5 py-1 text-[11px] font-semibold text-slate-600 hover:bg-slate-50"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => void createPipeline()}
                disabled={creating}
                className="flex items-center gap-1.5 rounded-lg bg-indigo-600 px-3 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700 disabled:opacity-50"
              >
                {creating && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                Create pipeline
              </button>
            </div>
          </div>
        </div>
      )}

      {detail && runDialogOpen && (
        <PipelineRunDialog
          pipeline={detail}
          open={runDialogOpen}
          running={starting}
          onClose={() => setRunDialogOpen(false)}
          onRun={(params) => void startRun(params)}
        />
      )}

      <ConfirmDialog request={confirmRequest} onClose={() => setConfirmRequest(null)} />

      {problemSteps.length + problemEdges.length > 0 && (
        <div className="pointer-events-none fixed bottom-4 left-1/2 z-[60] -translate-x-1/2">
          <span className="flex items-center gap-1.5 rounded-full bg-rose-600 px-3 py-1 text-[11px] font-semibold text-white shadow-lg">
            <AlertCircle className="h-3.5 w-3.5" />
            {problemSteps.length + problemEdges.length} problem
            {problemSteps.length + problemEdges.length === 1 ? "" : "s"} highlighted
          </span>
        </div>
      )}
    </div>
  );
};
