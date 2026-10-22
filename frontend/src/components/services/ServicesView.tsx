import React, { useState, useEffect } from 'react';
import {
  Layers,
  Zap,
  Globe,
  RefreshCw,
  StopCircle,
  AlertTriangle,
  ExternalLink,
  Unlink,
  Terminal,
  Trash2,
} from 'lucide-react';
import { api } from '../../lib/api';
import { DeployModal } from './DeployModal';
import type {
  ServiceStatus,
  ServiceRecommendation,
  ServiceCatalogEntry,
  ServiceEngineStatus,
  DeployLocalResponse,
  DeployProgressResponse,
} from '../../lib/types';

interface ServiceMeta {
  id: string;
  displayName: string;
  category: string;
  description: string;
  defaultPort: number;
  badge: string;
  webUi?: boolean;
}

const SERVICE_CATALOG_META: ServiceMeta[] = [
  {
    id: 'postgres',
    displayName: 'PostgreSQL 16',
    category: 'Core Warehouse',
    description: 'Relational storage for Silver & Gold layers and dbt-core analytical data marts.',
    defaultPort: 5432,
    badge: 'Warehouse',
  },
  {
    id: 'dagster',
    displayName: 'Dagster Engine',
    category: 'Orchestration',
    description: 'Asset scheduling, Python pipeline runs, log streaming, and dagster-dbt execution.',
    defaultPort: 3000,
    badge: 'Orchestration',
    webUi: true,
  },
  {
    id: 'superset',
    displayName: 'Apache Superset',
    category: 'BI & Analytics',
    description: 'Interactive data visualization, SQL Lab, and automated business dashboards.',
    defaultPort: 8088,
    badge: 'Dashboards',
    webUi: true,
  },
  {
    id: 'clickhouse',
    displayName: 'ClickHouse OLAP',
    category: 'High-Speed Analytics',
    description: 'Ultra-fast columnar database for real-time analytics and big data aggregation.',
    defaultPort: 8123,
    badge: 'Columnar OLAP',
  },
  {
    id: 'redpanda',
    displayName: 'Redpanda Streaming',
    category: 'Event Streaming',
    description: 'Lightweight, C++ Kafka-compatible streaming engine for CDC and event streams.',
    defaultPort: 9092,
    badge: 'Streaming',
  },
  {
    id: 'gitea',
    displayName: 'Gitea Git Server',
    category: 'Version Control',
    description: 'Lightweight air-gapped Git service for versioning notebooks, models, and pipelines.',
    defaultPort: 3001,
    badge: 'Air-Gapped Git',
    webUi: true,
  },
];

const extractErrorMessage = (e: unknown, fallback: string): string => {
  if (e && typeof e === 'object' && 'response' in e) {
    const resp = (e as { response?: { data?: { detail?: string } } }).response;
    if (resp?.data?.detail) {
      return resp.data.detail;
    }
  }
  if (e instanceof Error) {
    return e.message;
  }
  return fallback;
};

export const ServicesView: React.FC = () => {
  const [services, setServices] = useState<Record<string, ServiceStatus>>({});
  const [recommendations, setRecommendations] = useState<Record<string, ServiceRecommendation>>({});
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Per-service UI input states
  const [deployModes, setDeployModes] = useState<Record<string, 'local' | 'remote'>>({
    postgres: 'local',
    dagster: 'local',
    superset: 'local',
    clickhouse: 'local',
    redpanda: 'local',
    gitea: 'local',
  });
  const [customMem, setCustomMem] = useState<Record<string, number>>({});
  const [remoteHosts, setRemoteHosts] = useState<Record<string, string>>({});
  const [remotePorts, setRemotePorts] = useState<Record<string, number>>({});
  const [operatingService, setOperatingService] = useState<string | null>(null);
  const [engineStatus, setEngineStatus] = useState<ServiceEngineStatus | null>(null);
  const [catalog, setCatalog] = useState<Record<string, ServiceCatalogEntry>>({});
  const [deployResult, setDeployResult] = useState<DeployLocalResponse | null>(null);
  const [logsFor, setLogsFor] = useState<string | null>(null);
  const [logsText, setLogsText] = useState('');
  const [logsLoading, setLogsLoading] = useState(false);
  const [customPorts, setCustomPorts] = useState<Record<string, number>>({});
  const [deployProgress, setDeployProgress] = useState<DeployProgressResponse | null>(null);
  const [customImages, setCustomImages] = useState<Record<string, string>>({});
  const [deployModalSvc, setDeployModalSvc] = useState<string | null>(null);
  const [deployError, setDeployError] = useState<string | null>(null);

  const loadData = async (silent = false) => {
    try {
      if (!silent) setLoading(true);
      else setRefreshing(true);
      setError(null);

      const servicesData = await api.listServices();

      const [engineData, catalogData] = await Promise.all([
        api.getServicesEngine().catch(() => null),
        api.getServiceCatalog().catch(() => [] as ServiceCatalogEntry[]),
      ]);
      setEngineStatus(engineData);
      const cMap: Record<string, ServiceCatalogEntry> = {};
      for (const entry of catalogData) cMap[entry.name] = entry;
      setCatalog(cMap);

      const sMap: Record<string, ServiceStatus> = {};
      const newModes: Record<string, 'local' | 'remote'> = {};
      const newHosts: Record<string, string> = {};
      const newPorts: Record<string, number> = {};

      servicesData.forEach((s) => {
        const key = s.service_name.toLowerCase();
        sMap[key] = s;
        if (s.deployment_type === 'remote') {
          newModes[key] = 'remote';
          newHosts[key] = s.host;
          newPorts[key] = s.port;
        } else if (s.deployment_type === 'local') {
          newModes[key] = 'local';
        }
      });
      setServices(sMap);
      setDeployModes((prev) => ({ ...prev, ...newModes }));
      setRemoteHosts((prev) => ({ ...prev, ...newHosts }));
      setRemotePorts((prev) => ({ ...prev, ...newPorts }));

      // Fetch dynamic hardware recommendations for all 6 services
      const recs: Record<string, ServiceRecommendation> = {};
      await Promise.all(
        SERVICE_CATALOG_META.map(async (meta) => {
          try {
            const rec = await api.getRecommendation(meta.id);
            recs[meta.id] = rec;
          } catch {
            // Ignore individual recommendation failure
          }
        })
      );
      setRecommendations(recs);
    } catch (e: unknown) {
      const msg = extractErrorMessage(e, 'Failed to fetch services data');
      setError(msg);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    loadData();
  }, []);

  const handleDeployLocal = async (svcId: string) => {
    setDeployModalSvc(svcId);
    setDeployError(null);
    setOperatingService(svcId);
    setError(null);
    setDeployResult(null);
    setDeployProgress({
      service_name: svcId,
      phase: 'pre-flight',
      detail: 'Initializing pre-flight checks...',
      pull_percent: 0,
      logs: [],
      active: true,
      error: null,
    });
    let timer: ReturnType<typeof setInterval> | null = null;
    try {
      timer = setInterval(async () => {
        try {
          const p = await api.getServiceDeployProgress(svcId);
          setDeployProgress(p);
        } catch { /* polling is best-effort */ }
      }, 1000);
      const rec = recommendations[svcId];
      const memMb = customMem[svcId] || (rec ? rec.recommended_ram_mb : 1024);
      const res = await api.deployLocalService({
        service_name: svcId,
        memory_mb: memMb,
        host_port: customPorts[svcId] || undefined,
        image: customImages[svcId] || undefined,
      });
      setDeployResult(res);
      await loadData(true);
    } catch (e: unknown) {
      const msg = extractErrorMessage(e, `Failed to deploy ${svcId}`);
      setDeployError(msg);
      setDeployProgress((prev) => ({
        service_name: svcId,
        phase: 'failed',
        detail: msg,
        pull_percent: prev?.pull_percent || 0,
        logs: prev?.logs || [],
        active: false,
        error: msg,
      }));
    } finally {
      if (timer) clearInterval(timer);
      setOperatingService(null);
    }
  };

  const handleShowLogs = async (svcId: string) => {
    setLogsFor(svcId);
    setLogsLoading(true);
    try {
      const res = await api.getServiceLogs(svcId, 200);
      setLogsText(res.logs || '(no logs yet)');
    } catch (e: unknown) {
      setLogsText(extractErrorMessage(e, 'Failed to fetch logs'));
    } finally {
      setLogsLoading(false);
    }
  };

  const handleDestroyService = async (svcId: string) => {
    const confirmed = window.confirm(
      `Destroy ${svcId}? This permanently deletes the container AND its data volume — all persisted data will be lost.`
    );
    if (!confirmed) return;
    setOperatingService(svcId);
    setError(null);
    try {
      await api.destroyService(svcId);
      setDeployResult(null);
      await loadData(true);
    } catch (e: unknown) {
      const msg = extractErrorMessage(e, `Failed to destroy ${svcId}`);
      setError(msg);
    } finally {
      setOperatingService(null);
    }
  };

  const handleConnectRemote = async (svcId: string, defaultPort: number) => {
    setOperatingService(svcId);
    setError(null);
    try {
      const host = remoteHosts[svcId] || 'localhost';
      const port = remotePorts[svcId] || defaultPort;
      await api.connectRemoteService({
        service_name: svcId,
        host,
        port,
      });
      await loadData(true);
    } catch (e: unknown) {
      const msg = extractErrorMessage(e, `Failed to connect remote ${svcId}`);
      setError(msg);
    } finally {
      setOperatingService(null);
    }
  };

  const handleStopService = async (svcId: string) => {
    setOperatingService(svcId);
    setError(null);
    try {
      await api.stopService(svcId);
      await loadData(true);
    } catch (e: unknown) {
      const msg = extractErrorMessage(e, `Failed to stop ${svcId}`);
      setError(msg);
    } finally {
      setOperatingService(null);
    }
  };

  if (loading && Object.keys(services).length === 0) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center p-12 bg-slate-50">
        <RefreshCw className="w-8 h-8 text-sky-600 animate-spin mb-3" />
        <div className="text-sm font-semibold text-slate-700">Loading Service Catalog...</div>
        <div className="text-xs text-slate-400 font-mono mt-1">Connecting to Docker daemon & services</div>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto bg-slate-50/60 p-6 space-y-6">
      {error && (
        <div className="p-3 bg-red-50 border border-red-200 rounded-lg flex items-center gap-2.5 text-xs text-red-700 animate-in fade-in">
          <AlertTriangle className="w-4 h-4 text-red-500 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {engineStatus && !engineStatus.docker.available && (
        <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg flex items-center gap-2.5 text-xs text-amber-800 animate-in fade-in">
          <AlertTriangle className="w-4 h-4 text-amber-500 shrink-0" />
          <span>
            <strong>Docker engine is not reachable.</strong> Start Docker Desktop (or set DOCKER_HOST) to enable 1-click local deploys.
            {engineStatus.docker.error ? (
              <span className="block font-mono text-[10px] text-amber-700 mt-0.5">{engineStatus.docker.error}</span>
            ) : null}
          </span>
        </div>
      )}

      {/* 1-Click Deployment Progress & Lifecycle Modal */}
      {deployModalSvc && (
        <DeployModal
          isOpen={!!deployModalSvc}
          onClose={() => {
            setDeployModalSvc(null);
            setDeployError(null);
            setDeployProgress(null);
          }}
          serviceId={deployModalSvc}
          serviceMeta={SERVICE_CATALOG_META.find((m) => m.id === deployModalSvc)}
          catalogEntry={catalog[deployModalSvc]}
          progress={deployProgress}
          deployResult={deployResult}
          error={deployError}
          isOperating={operatingService === deployModalSvc}
          onRetry={() => handleDeployLocal(deployModalSvc)}
        />
      )}

      {/* 6-Service Catalog (Deploy Local vs Connect Remote) */}
      <section className="space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Layers className="w-4 h-4 text-sky-600" />
            <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider font-mono">
              6 Managed Engines
            </h3>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs text-slate-400 font-mono hidden md:inline">
              Localhost Docker Provisioning OR Connect Remote Enterprise Instances
            </span>
            <button
              onClick={() => loadData(true)}
              disabled={refreshing}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 bg-white hover:bg-slate-50 text-xs font-medium text-slate-700 shadow-2xs transition-all"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin text-sky-600' : 'text-slate-500'}`} />
              <span>Sync</span>
            </button>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
          {SERVICE_CATALOG_META.map((meta) => {
            const currentStatus = services[meta.id];
            const isLocalRunning = currentStatus && currentStatus.status === 'running' && currentStatus.deployment_type === 'local';
            const isRemoteConnected = currentStatus && (currentStatus.status === 'connected' || currentStatus.status === 'running') && currentStatus.deployment_type === 'remote';
            const isRunning = isLocalRunning || isRemoteConnected;
            const rec = recommendations[meta.id];
            const mode = deployModes[meta.id] || (isRemoteConnected ? 'remote' : 'local');
            const isBusy = operatingService === meta.id;
            const entry = catalog[meta.id];
            const canDeploy = entry ? entry.deployable : true;
            const engineDown = engineStatus ? !engineStatus.docker.available : false;

            return (
              <div
                key={meta.id}
                className="bg-white rounded-xl border border-slate-200 shadow-xs p-5 flex flex-col justify-between space-y-4 transition-all hover:border-slate-300"
              >
                {/* Header: Name, Badge, Status */}
                <div>
                  <div className="flex items-start justify-between">
                    <div>
                      <div className="flex items-center gap-2">
                        <h4 className="text-base font-bold text-slate-900 leading-none">
                          {meta.displayName}
                        </h4>
                        <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-slate-100 text-slate-600 border border-slate-200">
                          {meta.badge}
                        </span>
                      </div>
                      <p className="text-xs text-slate-500 mt-1 line-clamp-1">{meta.description}</p>
                    </div>

                    {/* Status Pill & Open UI */}
                    <div className="flex items-center gap-2">
                      {isRunning ? (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-semibold bg-emerald-50 text-emerald-700 border border-emerald-200">
                          <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse"></span>
                          {currentStatus.deployment_type === 'remote' ? 'Connected Remote' : 'Running (Local)'}
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-medium bg-slate-100 text-slate-500">
                          Stopped
                        </span>
                      )}

                      {/* Open Web UI Button */}
                      {isRunning && meta.webUi && (
                        <a
                          href={`http://localhost:${currentStatus.host_port || (meta.id === 'gitea' ? 3001 : currentStatus.port || meta.defaultPort)}`}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-[11px] font-semibold bg-sky-50 text-sky-700 hover:bg-sky-100 border border-sky-200 transition-colors shadow-2xs"
                        >
                          <ExternalLink className="w-3 h-3" />
                          <span>Open UI</span>
                        </a>
                      )}
                    </div>
                  </div>

                  {/* Active Service Telemetry (If Running) */}
                  {currentStatus && isRunning && (
                    <div className="mt-3.5 p-2.5 rounded-lg bg-slate-50 border border-slate-200/80 grid grid-cols-3 gap-2 text-xs font-mono">
                      <div>
                        <span className="text-[10px] text-slate-400 uppercase block">Host</span>
                        <span className="font-semibold text-slate-800">{currentStatus.host}</span>
                      </div>
                      <div>
                        <span className="text-[10px] text-slate-400 uppercase block">Port</span>
                        <span className="font-semibold text-slate-800">:{currentStatus.host_port || currentStatus.port}</span>
                      </div>
                      <div>
                        <span className="text-[10px] text-slate-400 uppercase block">Allocated RAM</span>
                        <span className="font-semibold text-slate-800">
                          {currentStatus.allocated_memory_mb > 0
                            ? `${(currentStatus.allocated_memory_mb / 1024).toFixed(1)} GB`
                            : 'Uncapped'}
                        </span>
                      </div>
                    </div>
                  )}
                </div>

                {/* Provisioning Control Tabs: Local vs Remote */}
                <div className="border-t border-slate-100 pt-3 space-y-3">
                  <div className="flex rounded-lg bg-slate-100 p-0.5 text-xs font-medium text-slate-600">
                    <button
                      onClick={() => setDeployModes((prev) => ({ ...prev, [meta.id]: 'local' }))}
                      className={`flex-1 py-1.5 rounded-md flex items-center justify-center gap-1.5 transition-all ${mode === 'local'
                          ? 'bg-white text-slate-900 font-semibold shadow-2xs'
                          : 'hover:text-slate-900'
                        }`}
                    >
                      <Zap className="w-3.5 h-3.5 text-amber-500" />
                      1-Click Deploy Locally
                    </button>
                    <button
                      onClick={() => setDeployModes((prev) => ({ ...prev, [meta.id]: 'remote' }))}
                      className={`flex-1 py-1.5 rounded-md flex items-center justify-center gap-1.5 transition-all ${mode === 'remote'
                          ? 'bg-white text-slate-900 font-semibold shadow-2xs'
                          : 'hover:text-slate-900'
                        }`}
                    >
                      <Globe className="w-3.5 h-3.5 text-sky-600" />
                      Connect Remote Instance
                    </button>
                  </div>

                  {/* Mode Content */}
                  {mode === 'local' ? (
                    <div className="space-y-3">
                      {rec && (
                        <div className="flex items-center justify-between text-xs text-slate-500 font-mono bg-amber-50/60 border border-amber-100 px-3 py-1.5 rounded-md">
                          <span>Recommended RAM:</span>
                          <span className="font-bold text-amber-900">
                            {(rec.recommended_ram_mb / 1024).toFixed(1)} GB ({rec.recommended_ram_mb} MB)
                          </span>
                        </div>
                      )}

                      <div className="flex items-center gap-2">
                        <div className="flex-1">
                          <label className="text-[11px] font-medium text-slate-500 block mb-1">
                            Memory Cap (MB)
                          </label>
                          <input
                            type="number"
                            min="256"
                            step="256"
                            placeholder={rec ? String(rec.recommended_ram_mb) : '2048'}
                            value={customMem[meta.id] || ''}
                            onChange={(e) =>
                              setCustomMem((prev) => ({
                                ...prev,
                                [meta.id]: parseInt(e.target.value) || 0,
                              }))
                            }
                            className="w-full px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500"
                          />
                        </div>

                        <div className="w-32">
                          <label className="text-[11px] font-medium text-slate-500 block mb-1">
                            Host Port
                          </label>
                          <input
                            type="number"
                            placeholder={String(entry?.default_host_port || meta.defaultPort)}
                            value={customPorts[meta.id] || ''}
                            onChange={(e) =>
                              setCustomPorts((prev) => ({
                                ...prev,
                                [meta.id]: parseInt(e.target.value) || 0,
                              }))
                            }
                            className="w-full px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500"
                          />
                        </div>

                        <div className="w-40">
                          <label className="text-[11px] font-medium text-slate-500 block mb-1">
                            Image Tag (optional)
                          </label>
                          <input
                            type="text"
                            placeholder={entry?.image || 'blueprint default'}
                            value={customImages[meta.id] || ''}
                            onChange={(e) =>
                              setCustomImages((prev) => ({
                                ...prev,
                                [meta.id]: e.target.value,
                              }))
                            }
                            className="w-full px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500"
                          />
                        </div>

                        <div className="pt-5">
                          {isLocalRunning ? (
                            <button
                              onClick={() => handleStopService(meta.id)}
                              disabled={isBusy}
                              className="px-3.5 py-1.5 rounded-lg border border-red-200 bg-red-50 hover:bg-red-100 text-red-700 text-xs font-semibold flex items-center gap-1.5 transition-all disabled:opacity-50"
                            >
                              <StopCircle className="w-3.5 h-3.5" />
                              <span>{isBusy ? 'Stopping...' : 'Stop Container'}</span>
                            </button>
                          ) : (
                            <button
                              onClick={() => handleDeployLocal(meta.id)}
                              disabled={isBusy || !canDeploy || (engineDown && entry?.run_mode !== 'process')}
                              className="px-3.5 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold flex items-center gap-1.5 shadow-2xs transition-all disabled:opacity-50"
                            >
                              <Zap className="w-3.5 h-3.5" />
                              <span>{isBusy ? 'Deploying...' : 'Deploy Container'}</span>
                            </button>
                          )}
                        </div>
                      </div>

                      {entry && (
                        <div className="text-[10px] font-mono text-slate-400 break-all">
                          {entry.run_mode === 'process'
                            ? 'Runs as local processes (no container) · logs in data/logs/'
                            : `Image: ${entry.image}${entry.volume_name ? ` · persistent volume: ${entry.volume_name}` : ''}`}
                        </div>
                      )}

                      {!canDeploy && (
                        <div className="text-[11px] text-amber-700 bg-amber-50 border border-amber-100 rounded-md px-3 py-2 leading-relaxed">
                          Local deploy unavailable: {entry?.reason_not_deployable}
                        </div>
                      )}

                      {isLocalRunning && (
                        <div className="flex items-center gap-2">
                          <button
                            onClick={() => handleShowLogs(meta.id)}
                            className="px-2.5 py-1.5 rounded-lg border border-slate-200 bg-white hover:bg-slate-50 text-slate-600 text-xs font-semibold flex items-center gap-1.5 transition-all"
                            title="View container logs"
                          >
                            <Terminal className="w-3.5 h-3.5" />
                            <span>Logs</span>
                          </button>
                          <button
                            onClick={() => handleDestroyService(meta.id)}
                            className="px-2.5 py-1.5 rounded-lg border border-red-200 bg-white hover:bg-red-50 text-red-600 text-xs font-semibold flex items-center gap-1.5 transition-all"
                            title="Stop container and permanently delete its data volume"
                          >
                            <Trash2 className="w-3.5 h-3.5" />
                            <span>Destroy Data</span>
                          </button>
                        </div>
                      )}
                    </div>
                  ) : (
                    /* Connect Remote Form */
                    <div className="space-y-3">
                      <div className="grid grid-cols-3 gap-2">
                        <div className="col-span-2">
                          <label className="text-[11px] font-medium text-slate-500 block mb-1">
                            Remote Host / IP
                          </label>
                          <input
                            type="text"
                            placeholder="db.internal.corp"
                            value={remoteHosts[meta.id] || ''}
                            onChange={(e) =>
                              setRemoteHosts((prev) => ({
                                ...prev,
                                [meta.id]: e.target.value,
                              }))
                            }
                            className="w-full px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500"
                          />
                        </div>

                        <div>
                          <label className="text-[11px] font-medium text-slate-500 block mb-1">
                            Port
                          </label>
                          <input
                            type="number"
                            placeholder={String(meta.defaultPort)}
                            value={remotePorts[meta.id] || ''}
                            onChange={(e) =>
                              setRemotePorts((prev) => ({
                                ...prev,
                                [meta.id]: parseInt(e.target.value) || meta.defaultPort,
                              }))
                            }
                            className="w-full px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500"
                          />
                        </div>
                      </div>

                      <div className="flex justify-end gap-2 pt-1">
                        {isRemoteConnected ? (
                          <button
                            onClick={() => handleStopService(meta.id)}
                            disabled={isBusy}
                            className="px-3.5 py-1.5 rounded-lg border border-slate-200 bg-slate-100 hover:bg-slate-200 text-slate-700 text-xs font-semibold flex items-center gap-1.5 transition-all disabled:opacity-50"
                          >
                            <Unlink className="w-3.5 h-3.5 text-slate-500" />
                            <span>{isBusy ? 'Disconnecting...' : 'Disconnect Remote'}</span>
                          </button>
                        ) : (
                          <button
                            onClick={() => handleConnectRemote(meta.id, meta.defaultPort)}
                            disabled={isBusy}
                            className="px-3.5 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold flex items-center gap-1.5 shadow-2xs transition-all disabled:opacity-50"
                          >
                            <Globe className="w-3.5 h-3.5" />
                            <span>{isBusy ? 'Connecting...' : 'Save & Connect'}</span>
                          </button>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </section>

      {/* Container Logs Modal */}
      {logsFor && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 p-8"
          onClick={() => setLogsFor(null)}
        >
          <div
            className="bg-slate-950 rounded-xl border border-slate-700 w-full max-w-4xl h-[70vh] flex flex-col overflow-hidden"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-4 py-2.5 border-b border-slate-700 bg-slate-900">
              <div className="flex items-center gap-2 text-xs font-mono text-slate-200">
                <Terminal className="w-4 h-4 text-emerald-400" />
                Container logs — {logsFor} (last 200 lines)
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => handleShowLogs(logsFor)}
                  className="px-2 py-1 rounded text-[11px] text-slate-300 hover:text-white hover:bg-slate-700"
                >
                  Refresh
                </button>
                <button
                  onClick={() => setLogsFor(null)}
                  className="px-2 py-1 rounded text-[11px] text-slate-300 hover:text-white hover:bg-slate-700"
                >
                  Close ✕
                </button>
              </div>
            </div>
            <pre className="flex-1 overflow-auto p-4 text-[11px] leading-relaxed font-mono text-emerald-300 whitespace-pre-wrap">
              {logsLoading ? 'Loading logs...' : logsText}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
};
