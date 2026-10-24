import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import ReactFlow, {
  Background,
  MarkerType,
  MiniMap,
  Position,
  Handle,
  ReactFlowProvider,
  applyEdgeChanges,
  applyNodeChanges,
  useReactFlow,
  type Connection,
  type Edge,
  type EdgeChange,
  type Node,
  type NodeChange,
  type NodeProps,
} from 'reactflow';
import 'reactflow/dist/style.css';
import {
  Activity,
  AlertTriangle,
  Code2,
  Database,
  FlaskConical,
  Globe,
  Maximize2,
  Minus,
  NotebookPen,
  Play,
  Plus,
  Workflow,
} from 'lucide-react';
import type {
  EdgeCondition,
  EdgePayload,
  PipelineStepDef,
  StepRunStatus,
  StepType,
} from '../../lib/types';

export const EDGE_COLOURS: Record<EdgeCondition, string> = {
  success: '#16a34a',
  failure: '#dc2626',
  completion: '#2563eb',
  skipped: '#94a3b8',
};

export const PROBLEM_COLOUR = '#e11d48';

/** An edge as the canvas reports it back to the view. */
export interface CanvasEdge {
  from_key: string;
  to_key: string;
  condition: EdgeCondition;
}

const STEP_ICONS: Record<StepType, React.ElementType> = {
  dbt_run: Play,
  dbt_test: FlaskConical,
  script: Code2,
  notebook: NotebookPen,
  webhook: Globe,
  raw_sql: Database,
  pipeline: Workflow,
};

const STEP_LABELS: Record<StepType, string> = {
  dbt_run: 'dbt run',
  dbt_test: 'dbt test',
  script: 'Python script',
  notebook: 'Notebook',
  webhook: 'Webhook',
  raw_sql: 'Raw SQL',
  pipeline: 'Pipeline run',
};

const STATUS_STYLES: Record<
  string,
  { ring: string; chip: string; text: string; accent: string; icon: string }
> = {
  success: {
    ring: '#16a34a',
    chip: 'bg-emerald-50 text-emerald-700',
    text: 'Success',
    accent: 'bg-emerald-500',
    icon: 'bg-emerald-50 text-emerald-600',
  },
  failed: {
    ring: '#dc2626',
    chip: 'bg-rose-50 text-rose-700',
    text: 'Failed',
    accent: 'bg-rose-500',
    icon: 'bg-rose-50 text-rose-600',
  },
  running: {
    ring: '#4f46e5',
    chip: 'bg-indigo-50 text-indigo-700',
    text: 'Running',
    accent: 'bg-indigo-500',
    icon: 'bg-indigo-50 text-indigo-600',
  },
  pending: {
    ring: '#cbd5e1',
    chip: 'bg-slate-100 text-slate-600',
    text: 'Pending',
    accent: 'bg-slate-300',
    icon: 'bg-slate-50 text-slate-500',
  },
  waiting_deps: {
    ring: '#cbd5e1',
    chip: 'bg-slate-100 text-slate-600',
    text: 'Waiting',
    accent: 'bg-slate-300',
    icon: 'bg-slate-50 text-slate-500',
  },
  skipped: {
    ring: '#94a3b8',
    chip: 'bg-slate-100 text-slate-500',
    text: 'Skipped',
    accent: 'bg-slate-300',
    icon: 'bg-slate-50 text-slate-400',
  },
  cancelled: {
    ring: '#f59e0b',
    chip: 'bg-amber-50 text-amber-700',
    text: 'Cancelled',
    accent: 'bg-amber-500',
    icon: 'bg-amber-50 text-amber-600',
  },
  interrupted: {
    ring: '#f97316',
    chip: 'bg-orange-50 text-orange-700',
    text: 'Interrupted',
    accent: 'bg-orange-500',
    icon: 'bg-orange-50 text-orange-600',
  },
};

export interface StepRuntime {
  status: StepRunStatus | string;
  duration_seconds?: number | null;
  attempt?: number;
  error?: string | null;
  outputs?: number;
}

interface StepNodeData {
  step: PipelineStepDef;
  runtime?: StepRuntime;
  selected?: boolean;
  editable: boolean;
  problem: boolean;
}

const StepNode: React.FC<NodeProps<StepNodeData>> = ({ data }) => {
  const { step, runtime, selected, editable, problem } = data;
  const Icon = STEP_ICONS[step.step_type] ?? Play;
  const status = runtime?.status;
  const style = status ? STATUS_STYLES[status] ?? STATUS_STYLES.pending : null;
  const border = problem
    ? "border-rose-400 ring-2 ring-rose-100"
    : selected
      ? "border-indigo-400 ring-2 ring-indigo-100"
      : "border-slate-200 hover:border-slate-300";

  return (
    <div
      className={`group relative w-[226px] overflow-hidden rounded-xl border bg-white shadow-sm transition-all hover:shadow-md ${border}`}
    >
      {/* The left bar is the run outcome, in the same colour code as the legend: it is
          readable from across the canvas without reading a chip. */}
      <span
        className={`absolute inset-y-0 left-0 w-[3px] ${
          problem ? "bg-rose-500" : style?.accent ?? "bg-slate-200"
        }`}
      />
      <Handle
        type="target"
        position={Position.Left}
        className="!h-2.5 !w-2.5 !border-2 !border-white !bg-slate-400 opacity-0 transition-opacity group-hover:opacity-100"
      />
      <div className="flex items-center gap-2 px-3.5 py-2.5 pl-4">
        <span
          className={`flex h-7 w-7 shrink-0 items-center justify-center rounded-lg ${
            style?.icon ?? "bg-indigo-50 text-indigo-600"
          }`}
        >
          <Icon className="h-3.5 w-3.5" />
        </span>
        <div className="min-w-0 flex-1">
          <p className="truncate text-xs font-semibold text-slate-800">{step.name}</p>
          <p className="truncate text-[10px] font-mono uppercase tracking-wide text-slate-400">
            {STEP_LABELS[step.step_type] ?? step.step_type}
          </p>
        </div>
        {problem ? (
          <AlertTriangle className="h-3.5 w-3.5 shrink-0 text-rose-500" />
        ) : (
          status && (
            <span
              className={`shrink-0 rounded px-1.5 py-0.5 text-[9px] font-bold uppercase ${style?.chip ?? ""}`}
            >
              {style?.text ?? status}
            </span>
          )
        )}
      </div>
      <div className="border-t border-slate-100 px-3.5 py-1.5 pl-4">
        <p className="truncate text-[10px] font-mono text-slate-500">
          {step.target || (editable ? "Set a target in the inspector" : "no target")}
        </p>
        <div className="mt-0.5 flex items-center justify-between gap-2 text-[10px] text-slate-400">
          <span className="truncate">
            {runtime?.duration_seconds != null ? `${runtime.duration_seconds.toFixed(1)}s` : ""}
          </span>
          <span className="flex shrink-0 items-center gap-1.5">
            {runtime?.outputs ? (
              <span className="rounded bg-slate-50 px-1 font-mono text-[9px] text-slate-500">
                {runtime.outputs} out
              </span>
            ) : null}
            {runtime?.attempt != null && runtime.attempt > 1 && (
              <span className="font-mono text-amber-600">retry #{runtime.attempt}</span>
            )}
          </span>
        </div>
      </div>
      {runtime?.error && (
        <p className="line-clamp-2 border-t border-rose-100 bg-rose-50/60 px-3.5 py-1.5 pl-4 text-[10px] leading-snug text-rose-700">
          {runtime.error}
        </p>
      )}
      <Handle
        type="source"
        position={Position.Right}
        className="!h-2.5 !w-2.5 !border-2 !border-white !bg-indigo-500 opacity-0 transition-opacity group-hover:opacity-100"
      />
    </div>
  );
};

const nodeTypes = { step: StepNode };

export interface PipelineCanvasProps {
  steps: PipelineStepDef[];
  edges: Array<EdgePayload & { id?: string }>;
  editable: boolean;
  /** Changes when a different pipeline is opened, so the view refits. */
  fitKey?: string;
  runtimeByStep?: Record<string, StepRuntime>;
  selectedStepId?: string | null;
  /** Step keys the server rejected; painted red so the toast has somewhere to point. */
  problemSteps?: string[];
  problemEdges?: Array<{ from_key: string; to_key: string }>;
  /** The edge whose dialog is open, drawn emphasised. */
  selectedEdge?: { from_key: string; to_key: string } | null;
  onEdgeClick?: (edge: CanvasEdge) => void;
  onEdgeDelete?: (fromKey: string, toKey: string) => void;
  onSelectStep?: (stepId: string | null) => void;
  onConnectRequest?: (fromKey: string, toKey: string) => void;
  onDeleteStep?: (stepId: string) => void;
  onMoveStep?: (stepId: string, position: { x: number; y: number }) => void;
  onAddStep?: () => void;
}

/**
 * The canvas behind both the designer and the monitor.
 *
 * One component, two modes: ``editable`` turns on dragging and connecting, and
 * ``runtimeByStep`` paints the same boxes with a run's outcome. That is the point:
 * the picture an operator edits is the picture they watch run.
 */
const CanvasInner: React.FC<PipelineCanvasProps> = ({
  steps,
  edges,
  editable,
  fitKey,
  runtimeByStep,
  selectedStepId,
  problemSteps,
  problemEdges,
  selectedEdge,
  onEdgeClick,
  onEdgeDelete,
  onSelectStep,
  onConnectRequest,
  onDeleteStep,
  onMoveStep,
  onAddStep,
}) => {
  const { fitView, zoomIn, zoomOut } = useReactFlow();
  const [hoveredEdge, setHoveredEdge] = useState<string | null>(null);
  const problemStepSet = useMemo(() => new Set(problemSteps ?? []), [problemSteps]);
  const problemEdgeSet = useMemo(
    () => new Set((problemEdges ?? []).map((edge) => `${edge.from_key}->${edge.to_key}`)),
    [problemEdges],
  );

  const nodes: Node<StepNodeData>[] = useMemo(
    () =>
      steps.map((step) => ({
        id: step.key,
        type: "step",
        position: { x: step.position_x, y: step.position_y },
        data: {
          step,
          runtime: runtimeByStep?.[step.key] ?? runtimeByStep?.[step.id],
          selected: selectedStepId === step.key,
          editable,
          problem: problemStepSet.has(step.key),
        },
        draggable: editable,
        selectable: true,
      })),
    [steps, runtimeByStep, selectedStepId, editable, problemStepSet],
  );

  const flowEdges: Edge[] = useMemo(
    () =>
      edges.map((edge) => {
        const id = edge.id ?? `${edge.from_key}-${edge.to_key}`;
        const problem = problemEdgeSet.has(`${edge.from_key}->${edge.to_key}`);
        const colour = problem ? PROBLEM_COLOUR : EDGE_COLOURS[edge.condition] ?? EDGE_COLOURS.success;
        // Labels are noise until asked for: success is the default path, so only an
        // exception branch or a hovered edge spells its condition out.
        const active =
          selectedEdge?.from_key === edge.from_key && selectedEdge?.to_key === edge.to_key;
        const showLabel =
          problem || active || edge.condition !== "success" || hoveredEdge === id;
        const sourceStatus = runtimeByStep?.[edge.from_key]?.status;
        return {
          id,
          source: edge.from_key,
          target: edge.to_key,
          label: showLabel ? (problem ? "invalid" : edge.condition) : undefined,
          labelStyle: { fill: colour, fontSize: 10, fontWeight: 700 },
          labelBgStyle: { fill: "#ffffff", fillOpacity: 0.95 },
          labelBgPadding: [4, 2] as [number, number],
          labelBgBorderRadius: 3,
          style: problem
            ? { stroke: colour, strokeWidth: 2.5, strokeDasharray: "6 4" }
            : active
              ? { stroke: colour, strokeWidth: 3.5 }
              : { stroke: colour, strokeWidth: 2 },
          // A 2px line is a 2px target; the hit area is deliberately wider.
          interactionWidth: 26,
          markerEnd: { type: MarkerType.ArrowClosed, color: colour },
          animated: problem || sourceStatus === "running" || edge.condition === "failure",
        };
      }),
    [edges, problemEdgeSet, hoveredEdge, runtimeByStep, selectedEdge],
  );

  const [liveNodes, setLiveNodes] = useState<Node<StepNodeData>[]>(nodes);
  const [liveEdges, setLiveEdges] = useState<Edge[]>(flowEdges);

  useEffect(() => setLiveNodes(nodes), [nodes]);
  // A poll that repaints the run must not silently drop the edge the operator just
  // selected: keyboard deletion of an edge works off that selection.
  useEffect(() => {
    setLiveEdges((current) => {
      const selected = new Set(
        current.filter((edge) => edge.selected).map((edge) => edge.id),
      );
      return flowEdges.map((edge) =>
        selected.has(edge.id) ? { ...edge, selected: true } : edge,
      );
    });
  }, [flowEdges]);

  // fitView on mount only covers the first render: opening another pipeline, or adding
  // a step to a graph that already fills the canvas, would leave boxes off screen.
  const stepKeys = useMemo(() => steps.map((step) => step.key).join(","), [steps]);
  useEffect(() => {
    const handle = window.setTimeout(
      () => fitView({ padding: 0.25, maxZoom: 1, duration: 200 }),
      80,
    );
    return () => window.clearTimeout(handle);
  }, [fitKey, stepKeys, fitView]);
  // 'F' fits the graph, matching the toolbar button; ignored while typing.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() !== "f" || event.metaKey || event.ctrlKey || event.altKey) {
        return;
      }
      const target = event.target as HTMLElement | null;
      const tag = target?.tagName?.toLowerCase();
      if (tag === "input" || tag === "textarea" || tag === "select" || target?.isContentEditable) {
        return;
      }
      event.preventDefault();
      fitView({ padding: 0.25, maxZoom: 1, duration: 250 });
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [fitView]);

  // The key handler reads the newest edges through a ref so it is registered once.
  const edgesRef = useRef(liveEdges);
  useEffect(() => {
    edgesRef.current = liveEdges;
  }, [liveEdges]);

  // Delete/Backspace on a selected edge is handled here rather than through
  // onEdgesDelete: React Flow also reports the edges of a node being deleted, which
  // would tear the graph before the operator confirmed removing that node.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (!editable || (event.key !== "Delete" && event.key !== "Backspace")) return;
      const target = event.target as HTMLElement | null;
      const tag = target?.tagName?.toLowerCase();
      if (tag === "input" || tag === "textarea" || tag === "select" || target?.isContentEditable) {
        return;
      }
      const selected = edgesRef.current.filter((edge) => edge.selected);
      if (!selected.length) return;
      event.preventDefault();
      selected.forEach((edge) => onEdgeDelete?.(edge.source, edge.target));
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [editable, onEdgeDelete]);

  const handleNodesChange = useCallback(
    (changes: NodeChange[]) => {
      setLiveNodes((current) => applyNodeChanges(changes, current));
      if (!editable || !onMoveStep) return;
      for (const change of changes) {
        if (change.type === "position" && change.position && change.dragging === false) {
          onMoveStep(change.id, change.position);
        }
      }
    },
    [editable, onMoveStep],
  );

  const handleEdgesChange = useCallback((changes: EdgeChange[]) => {
    // React Flow asks for local removals first; the draft is the source of truth, so
    // removal is forwarded (onEdgesDelete) and the props re-render the edge away.
    const local = changes.filter((change) => change.type !== "remove");
    if (local.length) setLiveEdges((current) => applyEdgeChanges(local, current));
  }, []);

  const handleConnect = useCallback(
    (connection: Connection) => {
      if (!editable || !connection.source || !connection.target) return;
      if (connection.source === connection.target) return;
      onConnectRequest?.(connection.source, connection.target);
    },
    [editable, onConnectRequest],
  );

  const showMinimap = steps.length > 6;

  return (
    <div className="relative h-full w-full">
      <ReactFlow
        nodes={liveNodes}
        edges={liveEdges}
        nodeTypes={nodeTypes}
        onNodesChange={handleNodesChange}
        onEdgesChange={handleEdgesChange}
        onConnect={handleConnect}
        onNodeClick={(_event, node) => onSelectStep?.(node.id)}
        onPaneClick={() => onSelectStep?.(null)}
        onNodesDelete={(deleted) => deleted.forEach((node) => onDeleteStep?.(node.id))}
        onEdgeMouseEnter={(_event, edge) => setHoveredEdge(edge.id)}
        onEdgeMouseLeave={() => setHoveredEdge(null)}
        onEdgeClick={(_event, edge) => {
          if (!editable) return;
          // Match on endpoints, not on id: an id is derived, endpoints are the data.
          const match = edges.find(
            (item) => item.from_key === edge.source && item.to_key === edge.target,
          );
          if (match) onEdgeClick?.(match);
        }}
        nodesConnectable={editable}
        elementsSelectable
        deleteKeyCode={editable ? ["Backspace", "Delete"] : null}
        fitView
        fitViewOptions={{ padding: 0.25, maxZoom: 1 }}
        minZoom={0.3}
        maxZoom={1.6}
        proOptions={{ hideAttribution: true }}
        className="bg-slate-50"
      >
        <Background gap={18} size={1} color="#e2e8f0" />
        {showMinimap && (
          <MiniMap
            pannable
            zoomable
            nodeColor={(node) => {
              const runtime = (node.data as StepNodeData)?.runtime;
              return runtime?.status ? STATUS_STYLES[runtime.status]?.ring ?? "#cbd5e1" : "#cbd5e1";
            }}
            maskColor="rgba(241, 245, 249, 0.55)"
            style={{ width: 150, height: 100 }}
            className="!rounded-lg !border !border-slate-200 !bg-white"
          />
        )}
      </ReactFlow>

      {/* Zoom and fit only. 'Add step' lives in the graph bar above the canvas: one
          primary action, in the place people look for it. */}
      {/* A floating toolbar replaces the boxy default control stack, and hides the
          buttons that do nothing in read-only mode. */}
      <div className="absolute bottom-3 left-3 z-10 flex items-center gap-0.5 rounded-lg border border-slate-200 bg-white/95 p-0.5 shadow-sm backdrop-blur">
        <button
          type="button"
          onClick={() => zoomOut({ duration: 150 })}
          className="rounded p-1.5 text-slate-500 hover:bg-slate-100 hover:text-slate-700"
          aria-label="Zoom out"
        >
          <Minus className="h-3.5 w-3.5" />
        </button>
        <button
          type="button"
          onClick={() => zoomIn({ duration: 150 })}
          className="rounded p-1.5 text-slate-500 hover:bg-slate-100 hover:text-slate-700"
          aria-label="Zoom in"
        >
          <Plus className="h-3.5 w-3.5" />
        </button>
        <button
          type="button"
          onClick={() => fitView({ padding: 0.25, maxZoom: 1, duration: 250 })}
          className="rounded p-1.5 text-slate-500 hover:bg-slate-100 hover:text-slate-700"
          aria-label="Fit graph"
          title="Fit graph (F)"
        >
          <Maximize2 className="h-3.5 w-3.5" />
        </button>
      </div>

      {!steps.length && (
        <div className="pointer-events-none absolute inset-0 flex items-center justify-center p-6">
          <div className="pointer-events-auto flex w-[320px] flex-col items-center gap-2 rounded-2xl border-2 border-dashed border-slate-300 bg-white/70 px-6 py-8 text-center">
            <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-indigo-50 text-indigo-600">
              <Activity className="h-5 w-5" />
            </span>
            <p className="text-sm font-semibold text-slate-700">This graph is empty</p>
            <p className="text-[11px] leading-relaxed text-slate-500">
              A pipeline is a set of steps and the conditions between them. Start with one
              step; connect its right port to the next step to add a dependency.
            </p>
            {editable && onAddStep && (
              <button
                type="button"
                onClick={onAddStep}
                className="mt-1 flex items-center gap-1.5 rounded-lg bg-indigo-600 px-3 py-1.5 text-[11px] font-semibold text-white hover:bg-indigo-700"
              >
                <Plus className="h-3.5 w-3.5" /> Add the first step
              </button>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

/**
 * Wrapped in its own provider so the inner component can call ``useReactFlow``:
 * refitting needs the instance, not just the props.
 */
export const PipelineCanvas: React.FC<PipelineCanvasProps> = (props) => (
  <ReactFlowProvider>
    <CanvasInner {...props} />
  </ReactFlowProvider>
);

export const stepTypeLabel = (type: StepType) => STEP_LABELS[type] ?? type;
export const stepTypeIcon = (type: StepType) => STEP_ICONS[type] ?? Activity;
export const statusStyle = (status: string | undefined) =>
  (status && STATUS_STYLES[status]) || STATUS_STYLES.pending;
