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
  Sparkles,
} from 'lucide-react';
import { useTelemetry } from '../../lib/useTelemetry';
import { usePolling } from '../../lib/viewActivity';
import { api } from '../../lib/api';
import type {
  ServiceStatus,
  CatalogNode,
  Connection,
  Notebook,
  NotebookServerStatus,
  AssetGraphNode,
  PipelineSummary,
  ServiceCatalogEntry,
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
      ] = await Promise.all([
        api.listServices().catch(() => []),
        api.getServiceCatalog().catch(() => []),
        api.getCatalogTree().catch(() => []),
        api.listConnections().catch(() => []),
        api.listNotebooks().catch(() => ({ notebooks: [], total: 0 })),
        api.getNotebookServerStatus().catch(() => null),
        api.getAssetGraph().catch(() => ({ nodes: [], edges: [] })),
        api.listPipelines().catch(() => []),
      ]);
      setServices(svcData);
      setServiceCatalog(svcCatalogData);
      setCatalogNodes(catData);
      setConnections(connsData);
      setNotebooks(nbData.notebooks || []);
      setNotebookStatus(nbStatus);
      setAssetNodes(assetGraphData.nodes || []);
      setPipelines(pipelineData);
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

  const latestPipelineRun =
    [...pipelines]
      .filter((p) => p.last_run_at)
      .sort((a, b) => new Date(b.last_run_at!).getTime() - new Date(a.last_run_at!).getTime())[0] ??
    null;

  const runningServicesCount = services.filter((s) => s.status === 'running' || s.status === 'connected').length;

  return (
    <div className="flex-1 overflow-y-auto bg-slate-100/75 p-6 space-y-6">
      {/* 1. Platform Key Metrics (Distinct cards with contrast against slate-100 canvas) */}
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

      {/* 2. Pipeline Orchestration Activity Banner */}
      <div className="p-4 rounded-xl border border-slate-200/90 bg-white shadow-xs flex flex-wrap items-center justify-between gap-3 text-xs">
        <div className="flex items-center gap-3">
          <span className="font-bold text-slate-800 uppercase tracking-wider text-[11px]">
            Orchestrator:
          </span>
          {latestPipelineRun ? (
            <div className="flex items-center gap-2">
              <span className="font-semibold text-slate-900">{latestPipelineRun.name}</span>
              <span
                className={`px-2.5 py-0.5 rounded-full font-mono text-[10px] font-semibold uppercase ${
                  latestPipelineRun.last_status === 'success'
                    ? 'bg-emerald-50 text-emerald-700 border border-emerald-200'
                    : latestPipelineRun.last_status === 'failed'
                      ? 'bg-rose-50 text-rose-700 border border-rose-200'
                      : 'bg-amber-50 text-amber-700 border border-amber-200'
                }`}
              >
                {latestPipelineRun.last_status || 'Ready'}
              </span>
              {latestPipelineRun.last_run_at && (
                <span className="text-slate-400 text-[11px] font-mono">
                  ({new Date(latestPipelineRun.last_run_at).toLocaleTimeString()})
                </span>
              )}
              <button
                onClick={() => onNavigate('pipelines')}
                className="inline-flex items-center gap-1 text-[11px] font-semibold text-sky-600 hover:text-sky-700 transition-colors ml-1"
              >
                <Terminal className="w-3 h-3" />
                <span>View execution log</span>
              </button>
            </div>
          ) : (
            <span className="text-slate-500">
              Pipeline engine ready • No runs recorded yet
            </span>
          )}
        </div>

        <button
          onClick={() => onNavigate('pipelines')}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-slate-50 hover:bg-slate-100 active:bg-slate-200/80 text-slate-700 rounded-lg text-xs font-semibold border border-slate-200 transition-all shadow-2xs"
        >
          <Plus className="w-3.5 h-3.5 text-slate-500" />
          <span>Create Pipeline</span>
        </button>
      </div>

      {/* 3. Two-Column Layout: Managed Services (2/3) + Engine Health (1/3) */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Managed Services Grid */}
        <div className="lg:col-span-2 bg-white rounded-xl border border-slate-200/90 p-5 shadow-xs space-y-4">
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
                          href={entry.web_ui_path || `http://localhost:${port}`}
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

        {/* Engine & Hardware Telemetry (Clean, compact 1/3 card) */}
        <div className="bg-white rounded-xl border border-slate-200/90 p-5 shadow-xs flex flex-col justify-between space-y-4">
          <div className="flex items-center justify-between border-b border-slate-100 pb-3">
            <div className="flex items-center gap-2">
              <Sparkles className="w-4 h-4 text-sky-600" />
              <h3 className="text-xs font-bold text-slate-800 uppercase tracking-wider">
                DuckDB Lakehouse Core
              </h3>
            </div>
            <span className="text-[11px] font-mono text-emerald-700 font-semibold bg-emerald-50 border border-emerald-200 px-2 py-0.5 rounded-full">
              Safe Max: {telemetry ? `${telemetry.max_safe_allocation_gb} GB` : '85%'}
            </span>
          </div>

          {telemetry ? (
            <div className="space-y-4">
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
                <div className="w-full bg-slate-100 rounded-full h-2.5 overflow-hidden">
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
                <p className="text-[10px] text-slate-400 mt-1">
                  Automatic out-of-core spilling activated above safe threshold
                </p>
              </div>

              {/* CPU Load Gauge */}
              <div>
                <div className="flex justify-between text-xs mb-1.5">
                  <span className="text-slate-600 font-medium flex items-center gap-1">
                    <Cpu className="w-3.5 h-3.5 text-amber-600" /> Host CPU ({telemetry.cpu_count} Cores)
                  </span>
                  <span className="font-mono font-bold text-slate-900">{telemetry.cpu_percent}%</span>
                </div>
                <div className="w-full bg-slate-100 rounded-full h-2.5 overflow-hidden">
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
            <div className="py-8 text-center text-xs text-slate-400">
              Loading engine telemetry...
            </div>
          )}

          <button
            onClick={() => onNavigate('settings')}
            className="w-full text-center text-xs font-semibold text-sky-600 hover:text-sky-700 pt-2 border-t border-slate-100 flex items-center justify-center gap-1 transition-colors"
          >
            <span>View Hardware Sizing in Settings</span>
            <ArrowUpRight className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>
    </div>
  );
};
