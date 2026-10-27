import React, { useState } from 'react';
import {
  Cpu,
  HardDrive,
  Layers,
  ArrowUpRight,
  ExternalLink,
  Database,
  Clock,
  BookOpen,
  Terminal,
  Plus,
  Activity,
  Files,
  Archive,
  CheckCircle2,
  XCircle,
  Loader2,
  Ban,
  Copy,
  Check,
  X,
  RefreshCw,
} from 'lucide-react';
import { useTelemetry } from '../../lib/useTelemetry';
import { usePolling } from '../../lib/viewActivity';
import { api } from '../../lib/api';
import { formatBytes } from '../../lib/format';
import { serviceUrl } from '../../lib/urls';
import type {
  ServiceStatus,
  CatalogNode,
  Connection,
  Notebook,
  NotebookServerStatus,
  AssetGraphNode,
  PipelineSummary,
  PipelineExecution,
  ServiceCatalogEntry,
  LakehouseDiagnostics,
} from '../../lib/types';
import type { NavigationTab } from '../layout/Sidebar';

interface HomeViewProps {
  onNavigate: (tab: NavigationTab) => void;
}

export const HomeView: React.FC<HomeViewProps> = ({ onNavigate }) => {
  const { telemetry } = useTelemetry();
  const [services, setServices] = useState<ServiceStatus[]>([]);
  const [serviceCatalog, setServiceCatalog] = useState<ServiceCatalogEntry[]>([]);
  const [catalogNodes, setCatalogNodes] = useState<CatalogNode[]>([]);
  const [connections, setConnections] = useState<Connection[]>([]);
  const [notebooks, setNotebooks] = useState<Notebook[]>([]);
  const [notebookStatus, setNotebookStatus] = useState<NotebookServerStatus | null>(null);
  const [assetNodes, setAssetNodes] = useState<AssetGraphNode[]>([]);
  const [pipelines, setPipelines] = useState<PipelineSummary[]>([]);
  const [recentRuns, setRecentRuns] = useState<PipelineExecution[]>([]);
  const [diagnostics, setDiagnostics] = useState<LakehouseDiagnostics | null>(null);

  // Execution Log Modal state
  const [selectedRunForLogs, setSelectedRunForLogs] = useState<PipelineExecution | null>(null);
  const [runLogsText, setRunLogsText] = useState<string>('');
  const [runLogsLoading, setRunLogsLoading] = useState<boolean>(false);
  const [copiedLogs, setCopiedLogs] = useState<boolean>(false);

  const loadData = async () => {
    try {
      const [
        svcData,
        svcCatalogData,
        catData,
        connsData,
        nbData,
        nbStatus,
        assetGraphData,
        pipelineData,
        runsData,
        diagData,
      ] = await Promise.all([
        api.listServices().catch(() => []),
        api.getServiceCatalog().catch(() => []),
        api.getCatalogTree().catch(() => []),
        api.listConnections().catch(() => []),
        api.listNotebooks().catch(() => ({ notebooks: [], total: 0 })),
        api.getNotebookServerStatus().catch(() => null),
        api.getAssetGraph().catch(() => ({ nodes: [], edges: [] })),
        api.listPipelines().catch(() => []),
        api.listPipelineExecutions({ limit: 5 }).catch(() => []),
        api.getLakehouseDiagnostics().catch(() => null),
      ]);
      setServices(svcData);
      setServiceCatalog(svcCatalogData);
      setCatalogNodes(catData);
      setConnections(connsData);
      setNotebooks(nbData.notebooks || []);
      setNotebookStatus(nbStatus);
      setAssetNodes(assetGraphData.nodes || []);
      setPipelines(pipelineData);
      setRecentRuns(runsData);
      setDiagnostics(diagData);
    } catch (err) {
      console.error('Failed to load home view metrics:', err);
    }
  };

  usePolling(loadData, 15000);

  // Dynamic Asset & Platform Metrics
  const externalConnsCount =
    connections.length > 0
      ? connections.length
      : catalogNodes.filter((n) => n.id !== 'lakehouse' && n.type !== 'lakehouse').length;
  const sourceTypes = Array.from(new Set(connections.map((c) => c.source_type.toUpperCase())));
  const sourceTypesSummary = sourceTypes.length > 0 ? sourceTypes.join(', ') : 'Vault Connected';

  const activePipelineCount = pipelines.filter((p) => p.is_enabled).length;
  const totalPipelineCount = pipelines.length;

  const stagingModels = assetNodes.filter((n) => n.folder === 'staging');
  const martModels = assetNodes.filter((n) => n.folder === 'marts');
  const totalDbtModels = stagingModels.length + martModels.length;

  const notebooksCount = notebooks.length;
  const isMarimoRunning = notebookStatus?.is_running ?? false;

  const runningServicesCount = services.filter((s) => s.status === 'running' || s.status === 'connected').length;

  const handleOpenRunLogs = async (run: PipelineExecution) => {
    setSelectedRunForLogs(run);
    setRunLogsLoading(true);
    setRunLogsText('');
    setCopiedLogs(false);
    try {
      const res = await api.getRunLogs(run.id);
      setRunLogsText(res.logs || run.error || 'No stdout/stderr logs produced by this run.');
    } catch {
      setRunLogsText(run.error || 'No execution log details available for this run.');
    } finally {
      setRunLogsLoading(false);
    }
  };

  const refreshRunLogs = async (runId: string) => {
    setRunLogsLoading(true);
    try {
      const res = await api.getRunLogs(runId);
      setRunLogsText(res.logs || 'No execution logs recorded.');
    } catch {
      setRunLogsText('Could not refresh run log.');
    } finally {
      setRunLogsLoading(false);
    }
  };

  const handleCopyLogs = async () => {
    if (!runLogsText) return;
    try {
      await navigator.clipboard.writeText(runLogsText);
      setCopiedLogs(true);
      setTimeout(() => setCopiedLogs(false), 2000);
    } catch {
      setCopiedLogs(false);
    }
  };

  const formatRunTime = (value?: string | null) => {
    if (!value) return '—';
    const date = new Date(value.includes('+') || value.endsWith('Z') ? value : `${value}Z`);
    if (Number.isNaN(date.getTime())) return '—';
    return date.toLocaleString(undefined, {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  };

  const formatRunDuration = (seconds?: number | null) => {
    if (seconds == null) return '—';
    if (seconds < 60) return `${seconds.toFixed(1)}s`;
    const minutes = Math.floor(seconds / 60);
    return `${minutes}m ${Math.round(seconds % 60)}s`;
  };

  const STATUS_CONFIG: Record<
    string,
    { label: string; icon: React.ElementType; chipClass: string }
  > = {
    success: {
      label: 'Success',
      icon: CheckCircle2,
      chipClass: 'bg-emerald-50 text-emerald-700 border-emerald-200/80',
    },
    failed: {
      label: 'Failed',
      icon: XCircle,
      chipClass: 'bg-rose-50 text-rose-700 border-rose-200/80',
    },
    running: {
      label: 'Running',
      icon: Loader2,
      chipClass: 'bg-sky-50 text-sky-700 border-sky-200/80',
    },
    pending: {
      label: 'Queued',
      icon: Clock,
      chipClass: 'bg-slate-100 text-slate-700 border-slate-200',
    },
    cancelled: {
      label: 'Cancelled',
      icon: Ban,
      chipClass: 'bg-amber-50 text-amber-700 border-amber-200/80',
    },
    interrupted: {
      label: 'Interrupted',
      icon: Ban,
      chipClass: 'bg-orange-50 text-orange-700 border-orange-200/80',
    },
    skipped: {
      label: 'Skipped',
      icon: Ban,
      chipClass: 'bg-slate-100 text-slate-500 border-slate-200',
    },
  };

  // Resolve 5 runs: prefer executions from /pipeline-runs; fallback to pipelines with a recorded last_run_at
  const displayedRuns: PipelineExecution[] =
    recentRuns.length > 0
      ? recentRuns.slice(0, 5)
      : pipelines
          .filter((p) => p.last_run_at)
          .sort((a, b) => new Date(b.last_run_at!).getTime() - new Date(a.last_run_at!).getTime())
          .slice(0, 5)
          .map((p) => ({
            id: p.id,
            pipeline_id: p.id,
            pipeline_name: p.name,
            kind: 'manual' as const,
            status: (p.last_status as any) || 'pending',
            params: {},
            scheduled_for: null,
            started_at: p.last_run_at ?? null,
            ended_at: null,
            duration_seconds: null,
            error: null,
            step_counts: {},
          }));

  return (
    <div className="flex-1 overflow-y-auto bg-slate-100/75 p-6 space-y-6">
      {/* 1. Platform Key Metrics */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        {/* Data Sources */}
        <div
          onClick={() => onNavigate('catalog')}
          className="p-5 rounded-xl border border-slate-200/90 bg-white hover:border-sky-300 hover:shadow-md transition-all cursor-pointer group shadow-xs flex flex-col justify-between"
        >
          <div className="flex items-center justify-between">
            <span className="text-[11px] font-bold uppercase tracking-wider text-slate-500">
              Data Sources
            </span>
            <span className="p-2 rounded-lg bg-sky-50 text-sky-600 border border-sky-100/80 group-hover:scale-105 transition-transform">
              <Database className="w-4 h-4" />
            </span>
          </div>
          <div className="text-3xl font-extrabold font-mono text-slate-900 mt-2">
            {externalConnsCount}
          </div>
          <div className="mt-3 pt-2.5 border-t border-slate-100 text-xs text-slate-500 truncate flex items-center gap-1.5">
            {externalConnsCount > 0 ? (
              <>
                <span className="w-2 h-2 rounded-full bg-emerald-500 shrink-0" />
                <span className="font-semibold text-emerald-700 truncate">{sourceTypesSummary}</span>
              </>
            ) : (
              <span className="text-slate-400">Lakehouse active • No external DBs</span>
            )}
          </div>
        </div>

        {/* Pipelines */}
        <div
          onClick={() => onNavigate('pipelines')}
          className="p-5 rounded-xl border border-slate-200/90 bg-white hover:border-indigo-300 hover:shadow-md transition-all cursor-pointer group shadow-xs flex flex-col justify-between"
        >
          <div className="flex items-center justify-between">
            <span className="text-[11px] font-bold uppercase tracking-wider text-slate-500">
              Pipelines
            </span>
            <span className="p-2 rounded-lg bg-indigo-50 text-indigo-600 border border-indigo-100/80 group-hover:scale-105 transition-transform">
              <Clock className="w-4 h-4" />
            </span>
          </div>
          <div className="text-3xl font-extrabold font-mono text-slate-900 mt-2">
            {totalPipelineCount}
          </div>
          <div className="mt-3 pt-2.5 border-t border-slate-100 text-xs text-slate-500 truncate flex items-center gap-1.5">
            <span
              className={`w-2 h-2 rounded-full shrink-0 ${
                activePipelineCount > 0 ? 'bg-emerald-500' : 'bg-slate-300'
              }`}
            />
            <span className="truncate">
              {activePipelineCount > 0
                ? `${activePipelineCount} active schedules`
                : 'No active schedules'}
            </span>
          </div>
        </div>

        {/* dbt Models */}
        <div
          onClick={() => onNavigate('dbt')}
          className="p-5 rounded-xl border border-slate-200/90 bg-white hover:border-purple-300 hover:shadow-md transition-all cursor-pointer group shadow-xs flex flex-col justify-between"
        >
          <div className="flex items-center justify-between">
            <span className="text-[11px] font-bold uppercase tracking-wider text-slate-500">
              dbt Models
            </span>
            <span className="p-2 rounded-lg bg-purple-50 text-purple-600 border border-purple-100/80 group-hover:scale-105 transition-transform">
              <Layers className="w-4 h-4" />
            </span>
          </div>
          <div className="text-3xl font-extrabold font-mono text-slate-900 mt-2">
            {totalDbtModels}
          </div>
          <div className="mt-3 pt-2.5 border-t border-slate-100 text-xs text-slate-500 truncate">
            {totalDbtModels > 0
              ? `${stagingModels.length} staging • ${martModels.length} marts`
              : 'Zero dbt models defined'}
          </div>
        </div>

        {/* Notebooks */}
        <div
          onClick={() => onNavigate('notebooks')}
          className="p-5 rounded-xl border border-slate-200/90 bg-white hover:border-amber-300 hover:shadow-md transition-all cursor-pointer group shadow-xs flex flex-col justify-between"
        >
          <div className="flex items-center justify-between">
            <span className="text-[11px] font-bold uppercase tracking-wider text-slate-500">
              Notebooks
            </span>
            <span className="p-2 rounded-lg bg-amber-50 text-amber-600 border border-amber-100/80 group-hover:scale-105 transition-transform">
              <BookOpen className="w-4 h-4" />
            </span>
          </div>
          <div className="text-3xl font-extrabold font-mono text-slate-900 mt-2">
            {notebooksCount}
          </div>
          <div className="mt-3 pt-2.5 border-t border-slate-100 text-xs text-slate-500 truncate flex items-center gap-1.5">
            <span
              className={`w-2 h-2 rounded-full shrink-0 ${
                isMarimoRunning ? 'bg-emerald-500' : 'bg-slate-300'
              }`}
            />
            <span className="truncate">
              {isMarimoRunning ? 'Marimo Engine Live' : 'Reactive Python Engine'}
            </span>
          </div>
        </div>
      </div>

      {/* 2. Recent Pipeline Executions Card (5 Runs) */}
      <div className="rounded-xl border border-slate-200/90 bg-white shadow-xs overflow-hidden">
        {/* Card Header */}
        <div className="p-4 border-b border-slate-100 flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2.5">
            <span className="p-2 rounded-lg bg-indigo-50 text-indigo-600 border border-indigo-100/80">
              <Activity className="w-4 h-4" />
            </span>
            <div>
              <div className="flex items-center gap-2">
                <h3 className="text-sm font-bold text-slate-900 tracking-tight">
                  Recent Pipeline Runs
                </h3>
                <span className="px-2 py-0.5 rounded-full text-[10px] font-semibold bg-slate-100 text-slate-600 border border-slate-200/60 font-mono">
                  {displayedRuns.length} {displayedRuns.length === 1 ? 'recent run' : 'recent runs'}
                </span>
              </div>
              <p className="text-[11px] text-slate-400 mt-0.5">
                Execution status, timing, and direct runtime logs
              </p>
            </div>
          </div>

          <button
            onClick={() => onNavigate('pipelines')}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-indigo-600 hover:bg-indigo-700 active:bg-indigo-800 text-white rounded-lg text-xs font-semibold transition-all shadow-2xs"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Create Pipeline</span>
          </button>
        </div>

        {/* Card Body: Runs Table or Empty State */}
        {displayedRuns.length === 0 ? (
          <div className="py-10 px-4 text-center">
            <p className="text-xs text-slate-500">Pipeline engine ready • No runs recorded yet</p>
            <p className="text-[11px] text-slate-400 mt-1">
              Trigger a pipeline in the orchestrator to view live step execution and output logs here.
            </p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full border-collapse text-left text-xs">
              <thead>
                <tr className="border-b border-slate-100 bg-slate-50/60 text-[10px] font-bold uppercase tracking-wider text-slate-400 font-mono">
                  <th className="py-2.5 px-4 font-bold">Status</th>
                  <th className="py-2.5 px-4 font-bold">Pipeline</th>
                  <th className="py-2.5 px-3 font-bold">Trigger</th>
                  <th className="py-2.5 px-3 font-bold">Executed At</th>
                  <th className="py-2.5 px-3 font-bold">Duration</th>
                  <th className="py-2.5 px-4 text-right font-bold">Execution Logs</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {displayedRuns.map((run) => {
                  const statusInfo = STATUS_CONFIG[run.status] || STATUS_CONFIG.pending;
                  const StatusIcon = statusInfo.icon;
                  const isLive = run.status === 'running';

                  return (
                    <tr
                      key={run.id}
                      className="hover:bg-slate-50/80 transition-colors group"
                    >
                      <td className="py-3 px-4 whitespace-nowrap">
                        <span
                          className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[10px] font-semibold uppercase border ${statusInfo.chipClass}`}
                        >
                          <StatusIcon
                            className={`w-3 h-3 ${isLive ? 'animate-spin' : ''}`}
                          />
                          <span>{statusInfo.label}</span>
                        </span>
                      </td>
                      <td className="py-3 px-4">
                        <div className="flex items-center gap-2">
                          <span className="font-semibold text-slate-900 truncate max-w-[200px]">
                            {run.pipeline_name || 'Pipeline'}
                          </span>
                          <span className="text-[10px] font-mono text-slate-400">
                            #{run.id.slice(0, 8)}
                          </span>
                        </div>
                      </td>
                      <td className="py-3 px-3 whitespace-nowrap">
                        <span className="font-mono text-[11px] text-slate-500 uppercase tracking-wide">
                          {run.kind || 'manual'}
                        </span>
                      </td>
                      <td className="py-3 px-3 whitespace-nowrap text-slate-600 font-mono text-[11px]">
                        {formatRunTime(run.started_at || run.scheduled_for)}
                      </td>
                      <td className="py-3 px-3 whitespace-nowrap text-slate-600 font-mono text-[11px]">
                        {formatRunDuration(run.duration_seconds)}
                      </td>
                      <td className="py-3 px-4 whitespace-nowrap text-right">
                        <button
                          type="button"
                          onClick={() => handleOpenRunLogs(run)}
                          className="inline-flex items-center gap-1.5 px-2.5 py-1 text-[11px] font-semibold text-sky-700 bg-sky-50 hover:bg-sky-100/80 border border-sky-200/80 rounded-md transition-all shadow-2xs active:scale-[0.98]"
                          title={`View execution log for ${run.pipeline_name || run.id}`}
                        >
                          <Terminal className="w-3.5 h-3.5 text-sky-600" />
                          <span>View Logs</span>
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* 3. Two-Column Layout: Managed Services (2/3) + Stacked Engine & Storage Cards (1/3) */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Managed Services Grid (Left Column 2/3) */}
        <div className="lg:col-span-2 bg-white rounded-xl border border-slate-200/90 p-5 shadow-xs flex flex-col justify-between h-full space-y-4">
          <div className="flex items-center justify-between border-b border-slate-100 pb-3">
            <div>
              <h3 className="text-sm font-bold text-slate-900 tracking-tight">
                Managed Services & Data Engines
              </h3>
              <p className="text-xs text-slate-500">
                {runningServicesCount} of {serviceCatalog.length} services currently active
              </p>
            </div>
            <button
              onClick={() => onNavigate('services')}
              className="text-xs font-semibold text-sky-600 hover:text-sky-700 flex items-center gap-1 transition-colors"
            >
              <span>Manage Services</span>
              <ArrowUpRight className="w-3.5 h-3.5" />
            </button>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
            {serviceCatalog.map((entry) => {
              const key = entry.name;
              const status = services.find((s) => s.service_name.toLowerCase() === key);
              const isRunning = status && (status.status === 'running' || status.status === 'connected');
              const port = status?.port || entry.default_host_port;

              return (
                <div
                  key={key}
                  className="p-4 rounded-xl border border-slate-200/80 bg-slate-50/50 hover:bg-white hover:border-slate-300 hover:shadow-xs transition-all flex flex-col justify-between space-y-3"
                >
                  <div className="flex items-start justify-between">
                    <div>
                      <div className="font-bold text-slate-900 text-xs leading-tight">
                        {entry.display_name}
                      </div>
                      <div className="text-[11px] text-slate-400 font-mono mt-0.5">
                        {entry.category} • Port :{port}
                      </div>
                    </div>

                    <span
                      className={`px-2 py-0.5 rounded-full text-[10px] font-semibold uppercase ${
                        isRunning
                          ? 'bg-emerald-50 text-emerald-700 border border-emerald-200'
                          : 'bg-slate-200/70 text-slate-500'
                      }`}
                    >
                      {isRunning ? 'Running' : 'Stopped'}
                    </span>
                  </div>

                  <div className="pt-2.5 border-t border-slate-200/60 flex items-center justify-between text-xs">
                    {isRunning ? (
                      <span className="font-mono text-[11px] text-slate-700 font-medium">
                        {status?.allocated_memory_mb && status.allocated_memory_mb > 0
                          ? `${(status.allocated_memory_mb / 1024).toFixed(1)} GB RAM`
                          : 'Localhost Node'}
                      </span>
                    ) : (
                      <span className="text-[11px] text-slate-400">Standby</span>
                    )}

                    <div className="flex items-center gap-1.5">
                      {isRunning && entry.web_ui && (
                        <a
                          href={serviceUrl(port, entry.web_ui_path)}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="inline-flex items-center gap-1 px-2.5 py-1 rounded-lg bg-sky-50 text-sky-700 hover:bg-sky-100 font-semibold text-xs border border-sky-200 transition-colors"
                        >
                          <ExternalLink className="w-3 h-3" />
                          <span>Open UI</span>
                        </a>
                      )}
                      <button
                        onClick={() => onNavigate('services')}
                        className="px-2.5 py-1 rounded-lg text-slate-600 hover:bg-slate-200/60 font-medium text-xs transition-colors"
                      >
                        Configure
                      </button>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Right Stacked Column (1/3): 1. Compute & System Resources + 2. Lakehouse Storage & Engine */}
        <div className="flex flex-col gap-4 h-full">
          {/* Card 1: Compute & System Resources */}
          <div className="bg-white rounded-xl border border-slate-200/90 p-5 shadow-xs flex-1 flex flex-col justify-between space-y-4">
            <div className="flex items-center justify-between border-b border-slate-100 pb-3">
              <div className="flex items-center gap-2">
                <Activity className="w-4 h-4 text-sky-600" />
                <h3 className="text-xs font-bold text-slate-800 uppercase tracking-wider">
                  Compute & Resources
                </h3>
              </div>
              <span className="text-[11px] font-medium text-emerald-700 bg-emerald-50 border border-emerald-200/80 px-2 py-0.5 rounded-full flex items-center gap-1.5">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                Healthy
              </span>
            </div>

            {telemetry ? (
              <div className="space-y-3.5">
                {/* RAM Usage Gauge */}
                <div>
                  <div className="flex justify-between text-xs mb-1.5">
                    <span className="text-slate-600 font-medium flex items-center gap-1">
                      <HardDrive className="w-3.5 h-3.5 text-sky-600" /> Host Memory
                    </span>
                    <span className="font-mono font-bold text-slate-900">
                      {telemetry.ram_used_gb} / {telemetry.total_ram_gb} GB ({telemetry.ram_percent_used}%)
                    </span>
                  </div>
                  <div className="w-full bg-slate-100 rounded-full h-2 overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all duration-500 ${
                        telemetry.ram_percent_used > 85
                          ? 'bg-red-500'
                          : telemetry.ram_percent_used > 65
                            ? 'bg-amber-500'
                            : 'bg-sky-600'
                      }`}
                      style={{ width: `${Math.min(telemetry.ram_percent_used, 100)}%` }}
                    />
                  </div>
                </div>

                {/* CPU Load Gauge */}
                <div>
                  <div className="flex justify-between text-xs mb-1.5">
                    <span className="text-slate-600 font-medium flex items-center gap-1">
                      <Cpu className="w-3.5 h-3.5 text-amber-600" /> Host CPU ({telemetry.cpu_count} Cores)
                    </span>
                    <span className="font-mono font-bold text-slate-900">{telemetry.cpu_percent}%</span>
                  </div>
                  <div className="w-full bg-slate-100 rounded-full h-2 overflow-hidden">
                    <div
                      className="h-full rounded-full bg-amber-500 transition-all duration-500"
                      style={{ width: `${Math.min(telemetry.cpu_percent, 100)}%` }}
                    />
                  </div>
                </div>

                {/* Active Containers Stat */}
                <div className="pt-2 border-t border-slate-100 flex items-center justify-between text-xs text-slate-600">
                  <span className="font-medium">Active Containers:</span>
                  <span className="font-mono font-bold text-slate-800 bg-slate-100 px-2 py-0.5 rounded text-[11px]">
                    {telemetry.active_containers_count} running ({telemetry.allocated_ram_gb} GB allocated)
                  </span>
                </div>
              </div>
            ) : (
              <div className="py-4 text-center text-xs text-slate-400">
                Loading telemetry...
              </div>
            )}
          </div>

          {/* Card 2: Lakehouse Storage & Engine Health */}
          <div className="bg-white rounded-xl border border-slate-200/90 p-5 shadow-xs flex-1 flex flex-col justify-between space-y-4">
            <div className="flex items-center justify-between border-b border-slate-100 pb-3">
              <div className="flex items-center gap-2">
                <Database className="w-4 h-4 text-emerald-600" />
                <h3 className="text-xs font-bold text-slate-800 uppercase tracking-wider">
                  Lakehouse Storage
                </h3>
              </div>
              <span className="text-[10px] font-mono text-slate-600 bg-slate-100 border border-slate-200 px-2 py-0.5 rounded-md font-semibold">
                Parquet Native
              </span>
            </div>

            <div className="grid grid-cols-2 gap-3">
              {/* Storage Footprint */}
              <div className="p-3 rounded-lg bg-slate-50/70 border border-slate-200/70">
                <div className="flex items-center gap-1.5 text-[11px] text-slate-500 font-medium mb-1">
                  <Archive className="w-3.5 h-3.5 text-emerald-600" />
                  <span>Data Size</span>
                </div>
                <div className="text-base font-bold font-mono text-slate-900">
                  {formatBytes(diagnostics?.total_data_bytes) ?? '0 B'}
                </div>
              </div>

              {/* Data Files */}
              <div className="p-3 rounded-lg bg-slate-50/70 border border-slate-200/70">
                <div className="flex items-center gap-1.5 text-[11px] text-slate-500 font-medium mb-1">
                  <Files className="w-3.5 h-3.5 text-sky-600" />
                  <span>Data Files</span>
                </div>
                <div className="text-base font-bold font-mono text-slate-900">
                  {diagnostics?.active_data_files ?? 0}
                </div>
              </div>

              {/* Memory Spill Ceiling */}
              <div className="p-3 rounded-lg bg-slate-50/70 border border-slate-200/70">
                <div className="text-[11px] text-slate-500 font-medium mb-1 truncate">
                  Spill Ceiling
                </div>
                <div className="text-sm font-bold font-mono text-slate-800">
                  {telemetry ? `${telemetry.max_safe_allocation_gb} GB` : '85%'}
                </div>
              </div>

              {/* Lakehouse Tables */}
              <div className="p-3 rounded-lg bg-slate-50/70 border border-slate-200/70">
                <div className="text-[11px] text-slate-500 font-medium mb-1 truncate">
                  Active Tables
                </div>
                <div className="text-sm font-bold font-mono text-slate-800">
                  {diagnostics?.active_table_count ?? catalogNodes.filter((n) => n.type === 'lakehouse').length}
                </div>
              </div>
            </div>

            <button
              onClick={() => onNavigate('settings')}
              className="w-full text-center text-xs font-semibold text-sky-600 hover:text-sky-700 pt-2 border-t border-slate-100 flex items-center justify-center gap-1 transition-colors"
            >
              <span>Lakehouse Diagnostics & Maintenance</span>
              <ArrowUpRight className="w-3.5 h-3.5" />
            </button>
          </div>
        </div>
      </div>
      {/* Execution Log Modal */}
      {selectedRunForLogs && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 backdrop-blur-xs p-4 sm:p-6 animate-in fade-in duration-150"
          onClick={() => setSelectedRunForLogs(null)}
        >
          <div
            className="bg-slate-950 rounded-xl border border-slate-700 w-full max-w-4xl h-[75vh] flex flex-col overflow-hidden shadow-2xl"
            onClick={(e) => e.stopPropagation()}
          >
            {/* Modal Header */}
            <div className="flex items-center justify-between px-4 py-3 border-b border-slate-800 bg-slate-900">
              <div className="flex items-center gap-2.5 min-w-0">
                <Terminal className="w-4 h-4 text-emerald-400 shrink-0" />
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-bold text-slate-100 font-mono truncate">
                      Execution Log — {selectedRunForLogs.pipeline_name || 'Pipeline'}
                    </span>
                    <span
                      className={`px-2 py-0.5 rounded text-[9px] font-bold font-mono uppercase tracking-wider ${
                        selectedRunForLogs.status === 'success'
                          ? 'bg-emerald-950/80 text-emerald-400 border border-emerald-800'
                          : selectedRunForLogs.status === 'failed'
                            ? 'bg-rose-950/80 text-rose-400 border border-rose-800'
                            : 'bg-sky-950/80 text-sky-400 border border-sky-800'
                      }`}
                    >
                      {selectedRunForLogs.status}
                    </span>
                  </div>
                  <p className="text-[10px] text-slate-400 font-mono truncate mt-0.5">
                    Run ID: {selectedRunForLogs.id}
                  </p>
                </div>
              </div>

              <div className="flex items-center gap-2 shrink-0">
                <button
                  type="button"
                  onClick={handleCopyLogs}
                  className="inline-flex items-center gap-1 px-2.5 py-1 rounded text-[11px] font-semibold text-slate-300 hover:text-white bg-slate-800 hover:bg-slate-700 border border-slate-700 transition-colors"
                >
                  {copiedLogs ? (
                    <>
                      <Check className="w-3 h-3 text-emerald-400" />
                      <span className="text-emerald-400">Copied</span>
                    </>
                  ) : (
                    <>
                      <Copy className="w-3 h-3" />
                      <span>Copy</span>
                    </>
                  )}
                </button>

                <button
                  type="button"
                  onClick={() => refreshRunLogs(selectedRunForLogs.id)}
                  disabled={runLogsLoading}
                  className="inline-flex items-center gap-1 px-2.5 py-1 rounded text-[11px] font-semibold text-slate-300 hover:text-white bg-slate-800 hover:bg-slate-700 border border-slate-700 transition-colors disabled:opacity-50"
                >
                  <RefreshCw className={`w-3 h-3 ${runLogsLoading ? 'animate-spin' : ''}`} />
                  <span>Refresh</span>
                </button>

                <button
                  type="button"
                  onClick={() => {
                    setSelectedRunForLogs(null);
                    onNavigate('pipelines');
                  }}
                  className="inline-flex items-center gap-1 px-2.5 py-1 rounded text-[11px] font-semibold text-sky-400 hover:text-sky-300 bg-sky-950/60 hover:bg-sky-900/60 border border-sky-800 transition-colors"
                >
                  <ExternalLink className="w-3 h-3" />
                  <span>Open Orchestrator</span>
                </button>

                <button
                  type="button"
                  onClick={() => setSelectedRunForLogs(null)}
                  className="p-1 rounded text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
                >
                  <X className="w-4 h-4" />
                </button>
              </div>
            </div>

            {/* Modal Body */}
            <pre className="flex-1 overflow-auto p-4 text-[11px] leading-relaxed font-mono text-emerald-300/90 bg-slate-950 whitespace-pre-wrap select-text custom-scrollbar">
              {runLogsLoading ? (
                <div className="flex items-center gap-2 text-slate-400">
                  <RefreshCw className="w-3.5 h-3.5 animate-spin text-sky-400" />
                  <span>Loading execution logs...</span>
                </div>
              ) : (
                runLogsText || 'No logs recorded for this execution.'
              )}
            </pre>

            {/* Modal Footer */}
            <div className="px-4 py-2.5 border-t border-slate-800 bg-slate-900 flex items-center justify-between text-[11px] font-mono text-slate-400">
              <div className="flex items-center gap-4">
                <span>Started: {formatRunTime(selectedRunForLogs.started_at)}</span>
                <span>Duration: {formatRunDuration(selectedRunForLogs.duration_seconds)}</span>
                <span>Trigger: {selectedRunForLogs.kind || 'manual'}</span>
              </div>
              <button
                type="button"
                onClick={() => setSelectedRunForLogs(null)}
                className="px-3 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white font-sans text-xs transition-colors"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
