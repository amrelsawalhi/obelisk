import React, { useCallback, useEffect, useMemo, useState } from 'react';
import ReactFlow, {
  Background,
  Controls,
  MarkerType,
  MiniMap,
  Position,
  Handle,
  applyEdgeChanges,
  applyNodeChanges,
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
  Code2,
  Database,
  FlaskConical,
  Globe,
  NotebookPen,
  Play,
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

const STEP_ICONS: Record<StepType, React.ElementType> = {
  dbt_run: Play,
  dbt_test: FlaskConical,
  script: Code2,
  notebook: NotebookPen,
  webhook: Globe,
  raw_sql: Database,
};

const STEP_LABELS: Record<StepType, string> = {
  dbt_run: 'dbt run',
  dbt_test: 'dbt test',
  script: 'Python script',
  notebook: 'Notebook',
  webhook: 'Webhook',
  raw_sql: 'Raw SQL',
};

const STATUS_STYLES: Record<string, { ring: string; chip: string; text: string }> = {
  success: { ring: '#16a34a', chip: 'bg-emerald-50 text-emerald-700', text: 'Success' },
  failed: { ring: '#dc2626', chip: 'bg-rose-50 text-rose-700', text: 'Failed' },
  running: { ring: '#4f46e5', chip: 'bg-indigo-50 text-indigo-700', text: 'Running' },
  pending: { ring: '#cbd5e1', chip: 'bg-slate-100 text-slate-600', text: 'Pending' },
  waiting_deps: { ring: '#cbd5e1', chip: 'bg-slate-100 text-slate-600', text: 'Waiting' },
  skipped: { ring: '#94a3b8', chip: 'bg-slate-100 text-slate-500', text: 'Skipped' },
  cancelled: { ring: '#f59e0b', chip: 'bg-amber-50 text-amber-700', text: 'Cancelled' },
  interrupted: { ring: '#f97316', chip: 'bg-orange-50 text-orange-700', text: 'Interrupted' },
};

export interface StepRuntime {
  status: StepRunStatus | string;
  duration_seconds?: number | null;
  attempt?: number;
  error?: string | null;
}

interface StepNodeData {
  step: PipelineStepDef;
  runtime?: StepRuntime;
  selected?: boolean;
  editable: boolean;
}

const StepNode: React.FC<NodeProps<StepNodeData>> = ({ data }) => {
  const { step, runtime, selected, editable } = data;
  const Icon = STEP_ICONS[step.step_type] ?? Play;
  const status = runtime?.status;
  const style = status ? STATUS_STYLES[status] ?? STATUS_STYLES.pending : null;

  return (
    <div
      className={`w-[228px] rounded-xl border bg-white shadow-sm transition-all ${
        selected ? 'border-indigo-500 ring-2 ring-indigo-200' : 'border-slate-200'
      }`}
      style={style ? { borderColor: style.ring } : undefined}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!h-2.5 !w-2.5 !border-2 !border-white !bg-slate-400"
      />
      <div className="flex items-center gap-2 border-b border-slate-100 px-3 py-2">
        <span className="flex h-7 w-7 items-center justify-center rounded-lg bg-indigo-50 text-indigo-600">
          <Icon className="h-3.5 w-3.5" />
        </span>
        <div className="min-w-0 flex-1">
          <p className="truncate text-xs font-semibold text-slate-800">{step.name}</p>
          <p className="truncate text-[10px] font-mono uppercase tracking-wide text-slate-400">
            {STEP_LABELS[step.step_type]}
          </p>
        </div>
      </div>
      <div className="space-y-1 px-3 py-2">
        <p className="truncate text-[11px] font-mono text-slate-500">{step.target || '—'}</p>
        <div className="flex items-center justify-between">
          {style ? (
            <span className={`rounded px-1.5 py-0.5 text-[10px] font-bold uppercase ${style.chip}`}>
              {style.text}
            </span>
          ) : (
            <span className="text-[10px] uppercase tracking-wide text-slate-300">
              {editable ? 'Not run yet' : ''}
            </span>
          )}
          {runtime?.duration_seconds != null && (
            <span className="text-[10px] font-mono text-slate-400">
              {runtime.duration_seconds.toFixed(1)}s
            </span>
          )}
          {runtime?.attempt != null && runtime.attempt > 1 && (
            <span className="text-[10px] font-mono text-amber-600">#{runtime.attempt}</span>
          )}
        </div>
        {runtime?.error && <p className="line-clamp-2 text-[10px] text-rose-600">{runtime.error}</p>}
      </div>
      <Handle
        type="source"
        position={Position.Right}
        className="!h-2.5 !w-2.5 !border-2 !border-white !bg-indigo-500"
      />
    </div>
  );
};

const nodeTypes = { step: StepNode };

export interface PipelineCanvasProps {
  steps: PipelineStepDef[];
  edges: Array<EdgePayload & { id?: string }>;
  editable: boolean;
  runtimeByStep?: Record<string, StepRuntime>;
  selectedStepId?: string | null;
  onSelectStep?: (stepId: string | null) => void;
  onConnectRequest?: (fromKey: string, toKey: string) => void;
  onDeleteStep?: (stepId: string) => void;
  onMoveStep?: (stepId: string, position: { x: number; y: number }) => void;
}

/**
 * The canvas behind both the designer and the monitor.
 *
 * One component, two modes: ``editable`` turns on dragging and connecting, and
 * ``runtimeByStep`` paints the same boxes with a run's outcome. That is the point —
 * the picture an operator edits is the picture they watch run.
 */
export const PipelineCanvas: React.FC<PipelineCanvasProps> = ({
  steps,
  edges,
  editable,
  runtimeByStep,
  selectedStepId,
  onSelectStep,
  onConnectRequest,
  onDeleteStep,
  onMoveStep,
}) => {
  const nodes: Node<StepNodeData>[] = useMemo(
    () =>
      steps.map((step) => ({
        id: step.key,
        type: 'step',
        position: { x: step.position_x, y: step.position_y },
        data: {
          step,
          runtime: runtimeByStep?.[step.key] ?? runtimeByStep?.[step.id],
          selected: selectedStepId === step.key,
          editable,
        },
        draggable: editable,
        selectable: true,
      })),
    [steps, runtimeByStep, selectedStepId, editable],
  );

  const flowEdges: Edge[] = useMemo(
    () =>
      edges.map((edge) => {
        const colour = EDGE_COLOURS[edge.condition] ?? EDGE_COLOURS.success;
        return {
          id: edge.id ?? `${edge.from_key}-${edge.to_key}`,
          source: edge.from_key,
          target: edge.to_key,
          label: edge.condition === 'success' ? undefined : edge.condition,
          labelStyle: { fill: colour, fontSize: 10, fontWeight: 700 },
          labelBgStyle: { fill: '#ffffff', fillOpacity: 0.9 },
          style: { stroke: colour, strokeWidth: 2 },
          markerEnd: { type: MarkerType.ArrowClosed, color: colour },
          animated: edge.condition === 'failure',
        };
      }),
    [edges],
  );

  const [liveNodes, setLiveNodes] = useState<Node<StepNodeData>[]>(nodes);
  const [liveEdges, setLiveEdges] = useState<Edge[]>(flowEdges);

  useEffect(() => setLiveNodes(nodes), [nodes]);
  useEffect(() => setLiveEdges(flowEdges), [flowEdges]);

  const handleNodesChange = useCallback(
    (changes: NodeChange[]) => {
      setLiveNodes((current) => applyNodeChanges(changes, current));
      if (!editable || !onMoveStep) return;
      for (const change of changes) {
        if (change.type === 'position' && change.position && change.dragging === false) {
          onMoveStep(change.id, change.position);
        }
      }
    },
    [editable, onMoveStep],
  );

  const handleEdgesChange = useCallback(
    (changes: EdgeChange[]) => setLiveEdges((current) => applyEdgeChanges(changes, current)),
    [],
  );

  const handleConnect = useCallback(
    (connection: Connection) => {
      if (!editable || !connection.source || !connection.target) return;
      if (connection.source === connection.target) return;
      onConnectRequest?.(connection.source, connection.target);
    },
    [editable, onConnectRequest],
  );

  return (
    <div className="h-full w-full">
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
        nodesConnectable={editable}
        elementsSelectable
        deleteKeyCode={editable ? ['Backspace', 'Delete'] : null}
        fitView
        proOptions={{ hideAttribution: true }}
        className="bg-slate-50"
      >
        <Background gap={18} size={1} color="#e2e8f0" />
        <Controls showInteractive={false} />
        <MiniMap
          pannable
          zoomable
          nodeColor={(node) => {
            const runtime = (node.data as StepNodeData)?.runtime;
            return runtime?.status ? STATUS_STYLES[runtime.status]?.ring ?? '#cbd5e1' : '#cbd5e1';
          }}
          className="!bg-white"
        />
      </ReactFlow>
    </div>
  );
};

export const stepTypeLabel = (type: StepType) => STEP_LABELS[type] ?? type;
export const stepTypeIcon = (type: StepType) => STEP_ICONS[type] ?? Activity;
export const statusStyle = (status: string | undefined) =>
  (status && STATUS_STYLES[status]) || STATUS_STYLES.pending;

