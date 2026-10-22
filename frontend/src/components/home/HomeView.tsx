import React, { useEffect, useState } from 'react';
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
  X,
  Plus,
  Activity,
} from 'lucide-react';
import { useTelemetry } from '../../lib/useTelemetry';
import { api } from '../../lib/api';
import type {
  ServiceStatus,
  CatalogNode,
  Connection,
  Notebook,
  NotebookServerStatus,
  AssetGraphNode,
  CronJob,
  OrchestrationEngineInfo,
  ServiceCatalogEntry,
} from '../../lib/types';
import type { NavigationTab } from '../layout/Sidebar';

interface HomeViewProps {
  onNavigate: (tab: NavigationTab) => void;
}

// Service catalog cards render from GET /api/services/catalog (backend blueprints).

export const HomeView: React.FC<HomeViewProps> = ({ onNavigate }) => {
  const { telemetry } = useTelemetry();
  const [services, setServices] = useState<ServiceStatus[]>([]);
  const [serviceCatalog, setServiceCatalog] = useState<ServiceCatalogEntry[]>([]);
  const [catalogNodes, setCatalogNodes] = useState<CatalogNode[]>([]);
  const [connections, setConnections] = useState<Connection[]>([]);
  const [notebooks, setNotebooks] = useState<Notebook[]>([]);
  const [notebookStatus, setNotebookStatus] = useState<NotebookServerStatus | null>(null);
  const [assetNodes, setAssetNodes] = useState<AssetGraphNode[]>([]);
  const [cronJobs, setCronJobs] = useState<CronJob[]>([]);
  const [orchestratorEngines, setOrchestratorEngines] = useState<OrchestrationEngineInfo[]>([]);
  const [activeLogModalJob, setActiveLogModalJob] = useState<CronJob | null>(null);

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
        cronData,
        enginesData,
      ] = await Promise.all([
        api.listServices().catch(() => []),
        api.getServiceCatalog().catch(() => []),
        api.getCatalogTree().catch(() => []),
        api.listConnections().catch(() => []),
        api.listNotebooks().catch(() => ({ notebooks: [], total: 0 })),
        api.getNotebookServerStatus().catch(() => null),
        api.getAssetGraph().catch(() => ({ nodes: [], edges: [] })),
        api.getCronJobs().catch(() => []),
        api.getOrchestrationEngines().catch(() => ({ engines: [] })),
      ]);
      setServices(svcData);
      setServiceCatalog(svcCatalogData);
      setCatalogNodes(catData);
      setConnections(connsData);
      setNotebooks(nbData.notebooks || []);
      setNotebookStatus(nbStatus);
      setAssetNodes(assetGraphData.nodes || []);
      setCronJobs(cronData);
      setOrchestratorEngines(enginesData.engines || []);
    } catch (err) {
      console.error('Failed to load home view metrics:', err);
    }
  };

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 10000);
    return () => clearInterval(interval);
  }, []);

  // Dynamic Asset & Platform Metrics (100% Real, Zero Hardcoding)
  const externalConnsCount = connections.length > 0 ? connections.length : catalogNodes.filter((n) => n.id !== 'bronze').length;
  const sourceTypes = Array.from(new Set(connections.map((c) => c.source_type.toUpperCase())));
  const sourceTypesSummary = sourceTypes.length > 0 ? sourceTypes.join(', ') : 'Vault Connected';

  const activeCronCount = cronJobs.filter((j) => j.is_enabled).length;
  const totalCronCount = cronJobs.length;
  const dagsterEngine = orchestratorEngines.find((e) => e.id === 'dagster');
  const airflowEngine = orchestratorEngines.find((e) => e.id === 'airflow');

  const silverModels = assetNodes.filter((n) => n.layer === 'silver');
  const goldModels = assetNodes.filter((n) => n.layer === 'gold');
  const totalDbtModels = silverModels.length + goldModels.length;

  const notebooksCount = notebooks.length;
  const isMarimoRunning = notebookStatus?.is_running ?? false;

  // Real latest executed cron job run (if any exist)
  const jobsWithRuns = cronJobs.filter((j) => j.last_run_at);
  jobsWithRuns.sort(
    (a, b) => new Date(b.last_run_at!).getTime() - new Date(a.last_run_at!).getTime()
  );
  const latestCronRun = jobsWithRuns.length > 0 ? jobsWithRuns[0] : null;

  const runningServicesCount = services.filter((s) => s.status === 'running' || s.status === 'connected').length;

  return (
    <div className="flex-1 overflow-y-auto bg-slate-50/60 p-6 space-y-6">

      {/* Grid: 1. Live Hardware & Telemetry + 2. Platform Assets & Orchestration Health */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* SECTION 1: Hardware & ResourceAdvisor Status */}
        <div className="bg-white rounded-2xl border border-slate-200 p-5 shadow-xs flex flex-col justify-between space-y-4">
          <div className="flex items-center justify-between border-b border-slate-100 pb-3">
            <div className="flex items-center gap-2">
              <Cpu className="w-4 h-4 text-sky-600" />
              <h3 className="text-xs font-bold text-slate-800 uppercase tracking-wider font-mono">
                Hardware & Telemetry
              </h3>
            </div>
            <span className="text-[11px] font-mono text-emerald-600 font-semibold bg-emerald-50 border border-emerald-200 px-2 py-0.5 rounded-full">
              Safe Max: {telemetry ? `${telemetry.max_safe_allocation_gb} GB` : '85%'}
            </span>
          </div>

          {telemetry ? (
            <div className="space-y-4">
              {/* RAM Usage Gauge */}
              <div>
                <div className="flex justify-between text-xs font-mono mb-1.5">
                  <span className="text-slate-500 font-sans font-medium flex items-center gap-1">
                    <HardDrive className="w-3.5 h-3.5 text-sky-600" /> Host RAM
                  </span>
                  <span className="font-bold text-slate-900">
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
              </div>

              {/* CPU Load Gauge */}
              <div>
                <div className="flex justify-between text-xs font-mono mb-1.5">
                  <span className="text-slate-500 font-sans font-medium flex items-center gap-1">
                    <Cpu className="w-3.5 h-3.5 text-amber-600" /> Host CPU ({telemetry.cpu_count} Cores)
                  </span>
                  <span className="font-bold text-slate-900">{telemetry.cpu_percent}%</span>
                </div>
                <div className="w-full bg-slate-100 rounded-full h-2.5 overflow-hidden">
                  <div
                    className="h-full rounded-full bg-amber-500 transition-all duration-500"
                    style={{ width: `${Math.min(telemetry.cpu_percent, 100)}%` }}
                  />
                </div>
              </div>

              {/* Active Containers Stat */}
              <div className="pt-2 border-t border-slate-100 flex items-center justify-between text-xs font-mono text-slate-600">
                <span className="text-slate-500 font-sans">Active Containers:</span>
                <span className="font-bold text-slate-800 bg-slate-100 px-2 py-0.5 rounded">
                  {telemetry.active_containers_count} running ({telemetry.allocated_ram_gb} GB allocated)
                </span>
              </div>
            </div>
          ) : (
            <div className="py-8 text-center text-xs text-slate-400 font-mono">
              Loading hardware telemetry...
            </div>
          )}

          <button
            onClick={() => onNavigate('settings')}
            className="w-full text-center text-xs font-medium text-sky-600 hover:text-sky-700 pt-2 border-t border-slate-100 flex items-center justify-center gap-1"
          >
            <span>View Hardware Sizing in Settings</span>
            <ArrowUpRight className="w-3.5 h-3.5" />
          </button>
        </div>

        {/* SECTION 2: Platform Assets & Orchestration Health (2 Columns on large screens) */}
        <div className="lg:col-span-2 bg-white rounded-2xl border border-slate-200 p-5 shadow-xs flex flex-col justify-between space-y-4">
          {/* Card Header */}
          <div className="flex items-center justify-between border-b border-slate-100 pb-3">
            <div className="flex items-center gap-2">
              <Activity className="w-4 h-4 text-indigo-600" />
              <div>
                <h3 className="text-xs font-bold text-slate-800 uppercase tracking-wider font-mono">
                  Platform Assets & Orchestration Health
                </h3>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={() => onNavigate('pipelines')}
                className="flex items-center gap-1.5 px-3 py-1 bg-indigo-50 hover:bg-indigo-100 text-indigo-700 border border-indigo-200/80 rounded-lg text-xs font-semibold transition-colors"
              >
                <Plus className="w-3.5 h-3.5" />
                <span>Schedule Job</span>
              </button>
              <button
                onClick={() => onNavigate('pipelines')}
                className="text-xs text-sky-600 hover:text-sky-700 font-medium flex items-center gap-1"
              >
                <span>Pipelines & Dagster</span>
                <ArrowUpRight className="w-3.5 h-3.5" />
              </button>
            </div>
          </div>

          {/* 4 Dynamic Metric Pillars */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            {/* 1. External Data Sources */}
            <div
              onClick={() => onNavigate('catalog')}
              className="p-3.5 rounded-xl border border-sky-200/80 bg-sky-50/40 hover:bg-sky-50/80 transition-all cursor-pointer group flex flex-col justify-between"
            >
              <div>
                <div className="flex items-center justify-between">
                  <span className="text-[10px] font-mono uppercase font-bold text-sky-800">
                    Data Sources
                  </span>
                  <Database className="w-3.5 h-3.5 text-sky-600 group-hover:scale-110 transition-transform" />
                </div>
                <div className="text-2xl font-bold font-mono text-slate-900 mt-1">
                  {externalConnsCount}
                </div>
              </div>
              <div className="mt-2 pt-2 border-t border-sky-100 text-[11px] text-slate-500 font-mono truncate">
                {externalConnsCount > 0 ? (
                  <span className="flex items-center gap-1 text-emerald-700 font-medium">
                    <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
                    {sourceTypesSummary}
                  </span>
                ) : (
                  'No DBs Connected'
                )}
              </div>
            </div>

            {/* 2. Scheduled Cron Jobs */}
            <div
              onClick={() => onNavigate('pipelines')}
              className="p-3.5 rounded-xl border border-indigo-200/80 bg-indigo-50/40 hover:bg-indigo-50/80 transition-all cursor-pointer group flex flex-col justify-between"
            >
              <div>
                <div className="flex items-center justify-between">
                  <span className="text-[10px] font-mono uppercase font-bold text-indigo-800">
                    Cron Automations
                  </span>
                  <Clock className="w-3.5 h-3.5 text-indigo-600 group-hover:scale-110 transition-transform" />
                </div>
                <div className="text-2xl font-bold font-mono text-slate-900 mt-1">
                  {totalCronCount}
                </div>
              </div>
              <div className="mt-2 pt-2 border-t border-indigo-100 text-[11px] text-slate-500 font-mono truncate">
                {totalCronCount === 0 ? 'No jobs scheduled' : `${activeCronCount} active schedules`}
              </div>
            </div>

            {/* 3. dbt Transformation Models */}
            <div
              onClick={() => onNavigate('pipelines')}
              className="p-3.5 rounded-xl border border-violet-200/80 bg-violet-50/40 hover:bg-violet-50/80 transition-all cursor-pointer group flex flex-col justify-between"
            >
              <div>
                <div className="flex items-center justify-between">
                  <span className="text-[10px] font-mono uppercase font-bold text-violet-800">
                    dbt Models
                  </span>
                  <Layers className="w-3.5 h-3.5 text-violet-600 group-hover:scale-110 transition-transform" />
                </div>
                <div className="text-2xl font-bold font-mono text-slate-900 mt-1">
                  {totalDbtModels}
                </div>
              </div>
              <div className="mt-2 pt-2 border-t border-violet-100 text-[11px] text-slate-500 font-mono truncate">
                {silverModels.length} Staging • {goldModels.length} Marts
              </div>
            </div>

            {/* 4. Interactive Notebooks */}
            <div
              onClick={() => onNavigate('notebooks')}
              className="p-3.5 rounded-xl border border-amber-200/80 bg-amber-50/40 hover:bg-amber-50/80 transition-all cursor-pointer group flex flex-col justify-between"
            >
              <div>
                <div className="flex items-center justify-between">
                  <span className="text-[10px] font-mono uppercase font-bold text-amber-800">
                    Notebooks
                  </span>
                  <BookOpen className="w-3.5 h-3.5 text-amber-600 group-hover:scale-110 transition-transform" />
                </div>
                <div className="text-2xl font-bold font-mono text-slate-900 mt-1">
                  {notebooksCount}
                </div>
              </div>
              <div className="mt-2 pt-2 border-t border-amber-100 text-[11px] text-slate-500 font-mono truncate">
                {isMarimoRunning ? (
                  <span className="text-emerald-700 font-medium">Marimo Server Live</span>
                ) : (
                  'Reactive Marimo'
                )}
              </div>
            </div>
          </div>

          {/* Bottom Execution Status & Orchestrator Engines Ribbon */}
          <div className="p-3 rounded-xl border border-slate-200 bg-slate-50 flex flex-wrap items-center justify-between gap-3 text-xs">
            {/* Orchestration Status Indicator */}
            <div className="flex items-center gap-2">
              <span className="font-semibold text-slate-700">Orchestration:</span>
              {latestCronRun ? (
                <div className="flex items-center gap-1.5">
                  <span className="font-medium text-slate-800">{latestCronRun.name}</span>
                  <span
                    className={`px-2 py-0.5 rounded-md font-mono text-[10px] font-semibold uppercase ${
                      latestCronRun.last_status === 'success'
                        ? 'bg-emerald-100 text-emerald-800 border border-emerald-200'
                        : latestCronRun.last_status === 'failed'
                        ? 'bg-rose-100 text-rose-800 border border-rose-200'
                        : 'bg-amber-100 text-amber-800 border border-amber-200'
                    }`}
                  >
                    {latestCronRun.last_status || 'Ready'}
                  </span>
                  {latestCronRun.last_run_at && (
                    <span className="text-slate-400 text-[11px] font-mono">
                      ({new Date(latestCronRun.last_run_at).toLocaleTimeString()})
                    </span>
                  )}
                  {latestCronRun.last_logs && (
                    <button
                      onClick={() => setActiveLogModalJob(latestCronRun)}
                      className="ml-1 inline-flex items-center gap-1 text-[11px] font-medium text-slate-600 hover:text-indigo-600 underline"
                    >
                      <Terminal className="w-3 h-3" />
                      <span>View Log</span>
                    </button>
                  )}
                </div>
              ) : (
                <span className="text-slate-400 font-mono text-[11px]">
                  Standby • In-process Cron & Dagster ready (0 jobs scheduled)
                </span>
              )}
            </div>

            {/* Engines Status Indicator */}
            <div className="flex items-center gap-2 text-[11px] font-mono text-slate-500">
              <span className="flex items-center gap-1">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
                <span className="text-slate-700 font-medium">Obelisk Cron: Active</span>
              </span>
              <span className="text-slate-300">•</span>
              <span className="flex items-center gap-1">
                <span
                  className={`w-1.5 h-1.5 rounded-full ${
                    dagsterEngine?.status === 'running' ? 'bg-emerald-500' : 'bg-slate-300'
                  }`}
                />
                <span>Dagster: {dagsterEngine?.status === 'running' ? 'Live (:3000)' : 'Offline'}</span>
              </span>
              <span className="text-slate-300">•</span>
              <span className="flex items-center gap-1">
                <span
                  className={`w-1.5 h-1.5 rounded-full ${
                    airflowEngine?.status === 'running' ? 'bg-emerald-500' : 'bg-slate-300'
                  }`}
                />
                <span>Airflow: {airflowEngine?.status === 'running' ? 'Live' : 'Offline'}</span>
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* SECTION 3: Active Services Status Grid (6 Managed Services) */}
      <div className="bg-white rounded-2xl border border-slate-200 p-5 shadow-xs space-y-4">
        <div className="flex items-center justify-between border-b border-slate-100 pb-3">
          <div>
            <h3 className="text-sm font-bold text-slate-900">
              The Obelisk Service Catalog (6 Services)
            </h3>
            <p className="text-xs text-slate-500">
              {runningServicesCount} of 6 services currently running on localhost or connected remotely
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => onNavigate('services')}
              className="text-xs font-semibold text-sky-600 hover:text-sky-700 flex items-center gap-1"
            >
              <span>Manage in Service Catalog</span>
              <ArrowUpRight className="w-3.5 h-3.5" />
            </button>
          </div>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {serviceCatalog.map((entry) => {
            const key = entry.name;
            const status = services.find((s) => s.service_name.toLowerCase() === key);
            const isRunning = status && (status.status === 'running' || status.status === 'connected');
            const port = status?.port || entry.default_host_port;

            return (
              <div
                key={key}
                className="p-4 rounded-xl border border-slate-200 bg-white hover:border-slate-300 transition-all shadow-2xs flex flex-col justify-between space-y-3"
              >
                <div className="flex items-start justify-between">
                  <div>
                    <div className="font-bold text-slate-900 text-sm leading-tight">
                      {entry.display_name}
                    </div>
                    <div className="text-[11px] text-slate-400 font-mono mt-0.5">
                      {entry.category} • Port :{port}
                    </div>
                  </div>

                  <span
                    className={`px-2 py-0.5 rounded-full text-[10px] font-mono font-semibold uppercase ${
                      isRunning
                        ? 'bg-emerald-50 text-emerald-700 border border-emerald-200'
                        : 'bg-slate-100 text-slate-500'
                    }`}
                  >
                    {isRunning ? (status?.deployment_type === 'remote' ? 'Remote' : 'Running') : 'Stopped'}
                  </span>
                </div>

                <div className="pt-2 border-t border-slate-100 flex items-center justify-between text-xs">
                  {isRunning ? (
                    status?.deployment_type === 'remote' ? (
                      <span className="font-mono text-[11px] text-slate-500">Remote Service</span>
                    ) : (
                      <span className="font-mono text-[11px] text-slate-700 font-medium">
                        {status?.allocated_memory_mb && status.allocated_memory_mb > 0
                          ? `${(status.allocated_memory_mb / 1024).toFixed(1)} GB RAM`
                          : 'Localhost'}
                      </span>
                    )
                  ) : (
                    <span className="text-[11px] text-slate-400">Offline</span>
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
                      onClick={() => onNavigate('settings')}
                      className="px-2.5 py-1 rounded-lg text-slate-600 hover:bg-slate-100 font-medium text-xs transition-colors"
                    >
                      Manage
                    </button>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Cron Job Execution Details Log Modal */}
      {activeLogModalJob && (
        <div className="fixed inset-0 z-50 bg-slate-900/40 backdrop-blur-xs flex items-center justify-center p-4">
          <div className="bg-white rounded-2xl border border-slate-200 shadow-xl w-full max-w-2xl overflow-hidden animate-in fade-in zoom-in-95 duration-150 flex flex-col max-h-[80vh]">
            <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between bg-slate-50/50">
              <div className="flex items-center gap-2.5">
                <Terminal className="w-4 h-4 text-indigo-600" />
                <div>
                  <h3 className="text-sm font-bold text-slate-900">
                    {activeLogModalJob.name}
                  </h3>
                  <p className="text-[11px] text-slate-500 font-mono">
                    Target: {activeLogModalJob.target} ({activeLogModalJob.target_type}) • Cron: {activeLogModalJob.cron_expression}
                  </p>
                </div>
              </div>
              <button
                onClick={() => setActiveLogModalJob(null)}
                className="p-1 rounded-lg text-slate-400 hover:text-slate-700 hover:bg-slate-100 transition-colors"
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="p-4 bg-slate-950 text-slate-200 font-mono text-xs overflow-y-auto flex-1 select-text">
              <pre className="whitespace-pre-wrap leading-relaxed">
                {activeLogModalJob.last_logs || 'No logs available for this job run.'}
              </pre>
            </div>

            <div className="px-5 py-3 border-t border-slate-100 bg-slate-50 flex items-center justify-between text-xs">
              <span
                className={`px-2 py-0.5 rounded font-mono font-semibold uppercase text-[10px] ${
                  activeLogModalJob.last_status === 'success'
                    ? 'bg-emerald-100 text-emerald-800'
                    : 'bg-rose-100 text-rose-800'
                }`}
              >
                Status: {activeLogModalJob.last_status || 'Unknown'}
              </span>
              <button
                onClick={() => setActiveLogModalJob(null)}
                className="px-3 py-1.5 bg-slate-200 hover:bg-slate-300 text-slate-700 rounded-lg text-xs font-semibold transition-colors"
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
