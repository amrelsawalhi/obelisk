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
  Code2,
  Boxes,
  GitFork,
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

  const quickActions = [
    {
      title: 'Query Studio',
      description: 'Run high-speed analytical SQL queries on DuckDB',
      icon: Code2,
      tab: 'query' as NavigationTab,
      badge: 'SQL Editor',
      color: 'text-sky-600 bg-sky-50 border-sky-100 hover:border-sky-300',
    },
    {
      title: 'Data Catalog',
      description: 'Explore schemas, ingest Parquet/CSV, or attach databases',
      icon: Database,
      tab: 'catalog' as NavigationTab,
      badge: 'Lakehouse',
      color: 'text-emerald-600 bg-emerald-50 border-emerald-100 hover:border-emerald-300',
    },
    {
      title: 'dbt Studio',
      description: 'Build transformation models with live SQL and lineage',
      icon: Boxes,
      tab: 'dbt' as NavigationTab,
      badge: 'Transform',
      color: 'text-purple-600 bg-purple-50 border-purple-100 hover:border-purple-300',
    },
    {
      title: 'Pipelines',
      description: 'Orchestrate multi-step DAG workflows and schedules',
      icon: GitFork,
      tab: 'pipelines' as NavigationTab,
      badge: 'Automate',
      color: 'text-amber-600 bg-amber-50 border-amber-100 hover:border-amber-300',
    },
  ];

  return (
    <div className="flex-1 overflow-y-auto bg-slate-50/50 p-6 space-y-6">
      {/* 1. Quick Actions Hub */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <div>
            <h2 className="text-sm font-bold text-slate-900 tracking-tight">Quick Workflows</h2>
            <p className="text-xs text-slate-500">Jump directly into analytics, data modeling, or pipeline orchestration</p>
          </div>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3.5">
          {quickActions.map((action) => {
            const Icon = action.icon;
            return (
              <button
                key={action.tab}
                onClick={() => onNavigate(action.tab)}
                className={`p-4 rounded-xl border bg-white shadow-2xs hover:shadow-md transition-all text-left flex flex-col justify-between group ${action.color}`}
              >
                <div>
                  <div className="flex items-center justify-between mb-2">
                    <span className="p-2 rounded-lg bg-white border border-slate-200/80 shadow-2xs">
                      <Icon className="w-4 h-4" />
                    </span>
                    <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500 font-mono px-2 py-0.5 rounded-md bg-slate-100/80">
                      {action.badge}
                    </span>
                  </div>
                  <h3 className="font-bold text-slate-900 text-sm group-hover:text-sky-600 transition-colors">
                    {action.title}
                  </h3>
                  <p className="text-xs text-slate-500 mt-1 leading-relaxed">
                    {action.description}
                  </p>
                </div>
                <div className="mt-3 pt-2.5 border-t border-slate-100 flex items-center justify-between text-xs font-semibold text-slate-600 group-hover:text-sky-600">
                  <span>Open workspace</span>
                  <ArrowUpRight className="w-3.5 h-3.5 group-hover:translate-x-0.5 group-hover:-translate-y-0.5 transition-transform" />
                </div>
              </button>
            );
          })}
        </div>
      </div>

      {/* 2. Platform Key Metrics */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3.5">
        {/* Data Sources */}
        <div
          onClick={() => onNavigate('catalog')}
          className="p-4 rounded-xl border border-slate-200 bg-white hover:border-slate-300 transition-all cursor-pointer group shadow-2xs"
        >
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold text-slate-600">Data Sources</span>
            <Database className="w-4 h-4 text-sky-600 group-hover:scale-110 transition-transform" />
          </div>
          <div className="text-2xl font-bold font-mono text-slate-900 mt-1.5">
            {externalConnsCount}
          </div>
          <div className="mt-2 pt-2 border-t border-slate-100 text-[11px] text-slate-500 truncate flex items-center gap-1.5">
            {externalConnsCount > 0 ? (
              <>
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
                <span className="font-medium text-emerald-700">{sourceTypesSummary}</span>
              </>
            ) : (
              <span>Lakehouse active • No external DBs</span>
            )}
          </div>
        </div>

        {/* Pipelines */}
        <div
          onClick={() => onNavigate('pipelines')}
          className="p-4 rounded-xl border border-slate-200 bg-white hover:border-slate-300 transition-all cursor-pointer group shadow-2xs"
        >
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold text-slate-600">Pipelines</span>
            <Clock className="w-4 h-4 text-indigo-600 group-hover:scale-110 transition-transform" />
          </div>
          <div className="text-2xl font-bold font-mono text-slate-900 mt-1.5">
            {totalPipelineCount}
          </div>
          <div className="mt-2 pt-2 border-t border-slate-100 text-[11px] text-slate-500 truncate flex items-center gap-1.5">
            <span className={`w-1.5 h-1.5 rounded-full ${activePipelineCount > 0 ? 'bg-emerald-500' : 'bg-slate-300'}`} />
            <span>{activePipelineCount > 0 ? `${activePipelineCount} active schedules` : 'No active schedules'}</span>
          </div>
        </div>

        {/* dbt Models */}
        <div
          onClick={() => onNavigate('dbt')}
          className="p-4 rounded-xl border border-slate-200 bg-white hover:border-slate-300 transition-all cursor-pointer group shadow-2xs"
        >
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold text-slate-600">dbt Models</span>
            <Layers className="w-4 h-4 text-purple-600 group-hover:scale-110 transition-transform" />
          </div>
          <div className="text-2xl font-bold font-mono text-slate-900 mt-1.5">
            {totalDbtModels}
          </div>
          <div className="mt-2 pt-2 border-t border-slate-100 text-[11px] text-slate-500 truncate">
            {totalDbtModels > 0 ? `${stagingModels.length} staging • ${martModels.length} marts` : 'Zero dbt models defined'}
          </div>
        </div>

        {/* Notebooks */}
        <div
          onClick={() => onNavigate('notebooks')}
          className="p-4 rounded-xl border border-slate-200 bg-white hover:border-slate-300 transition-all cursor-pointer group shadow-2xs"
        >
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold text-slate-600">Notebooks</span>
            <BookOpen className="w-4 h-4 text-amber-600 group-hover:scale-110 transition-transform" />
          </div>
          <div className="text-2xl font-bold font-mono text-slate-900 mt-1.5">
            {notebooksCount}
          </div>
          <div className="mt-2 pt-2 border-t border-slate-100 text-[11px] text-slate-500 truncate flex items-center gap-1.5">
            <span className={`w-1.5 h-1.5 rounded-full ${isMarimoRunning ? 'bg-emerald-500' : 'bg-slate-300'}`} />
            <span>{isMarimoRunning ? 'Marimo Engine Live' : 'Reactive Python Engine'}</span>
          </div>
        </div>
      </div>

      {/* 3. Pipeline Orchestration Activity Banner */}
      <div className="p-3.5 rounded-xl border border-slate-200 bg-white shadow-2xs flex flex-wrap items-center justify-between gap-3 text-xs">
        <div className="flex items-center gap-2.5">
          <span className="font-semibold text-slate-800">Orchestrator:</span>
          {latestPipelineRun ? (
            <div className="flex items-center gap-2">
              <span className="font-medium text-slate-900">{latestPipelineRun.name}</span>
              <span
                className={`px-2 py-0.5 rounded-full font-mono text-[10px] font-semibold uppercase ${
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

        <div className="flex items-center gap-3">
          <button
            onClick={() => onNavigate('pipelines')}
            className="inline-flex items-center gap-1.5 px-3 py-1 bg-slate-100 hover:bg-slate-200/80 text-slate-700 rounded-lg text-xs font-semibold border border-slate-200 transition-colors"
          >
            <Plus className="w-3.5 h-3.5 text-slate-500" />
            <span>Create Pipeline</span>
          </button>
        </div>
      </div>

      {/* 4. Two-Column Layout: Managed Services (2/3) + Engine Health (1/3) */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Managed Services Grid */}
        <div className="lg:col-span-2 bg-white rounded-xl border border-slate-200 p-5 shadow-2xs space-y-4">
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
                  className="p-3.5 rounded-xl border border-slate-200 bg-white hover:border-slate-300 transition-all shadow-2xs flex flex-col justify-between space-y-3"
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
                          : 'bg-slate-100 text-slate-500'
                      }`}
                    >
                      {isRunning ? 'Running' : 'Stopped'}
                    </span>
                  </div>

                  <div className="pt-2 border-t border-slate-100 flex items-center justify-between text-xs">
                    {isRunning ? (
                      <span className="font-mono text-[11px] text-slate-600">
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
                        className="px-2.5 py-1 rounded-lg text-slate-600 hover:bg-slate-100 font-medium text-xs transition-colors"
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

        {/* Engine & Hardware Telemetry (Clean, compact 1/3 sidebar) */}
        <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-2xs flex flex-col justify-between space-y-4">
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
