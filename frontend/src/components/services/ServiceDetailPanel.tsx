import React, { useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import {
  X,
  Zap,
  Play,
  StopCircle,
  RotateCcw,
  ExternalLink,
  Terminal,
  Trash2,
  ChevronDown,
  Server,
} from 'lucide-react';
import { api } from '../../lib/api';
import { serviceUrl } from '../../lib/urls';
import { DeployModal } from './DeployModal';
import { SERVICE_ICONS } from './serviceIcons';
import { ConfirmDialog } from '../common/ConfirmDialog';
import type { ConfirmRequest } from '../common/ConfirmDialog';
import type {
  ServiceCatalogEntry,
  ServiceStatus,
  ServiceEngineStatus,
  ServiceRecommendation,
  DeployLocalResponse,
  DeployProgressResponse,
} from '../../lib/types';

interface ServiceDetailPanelProps {
  service: ServiceCatalogEntry;
  status?: ServiceStatus;
  engineStatus: ServiceEngineStatus | null;
  onClose: () => void;
  onChanged: () => void;
}

const extractErrorMessage = (e: unknown, fallback: string): string => {
  if (e && typeof e === 'object' && 'response' in e) {
    const resp = (e as { response?: { data?: { detail?: string } } }).response;
    if (resp?.data?.detail) return resp.data.detail;
  }
  if (e instanceof Error) return e.message;
  return fallback;
};

export const ServiceDetailPanel: React.FC<ServiceDetailPanelProps> = ({
  service,
  status,
  engineStatus,
  onClose,
  onChanged,
}) => {
  const Icon = SERVICE_ICONS[service.name] || Server;
  const isProcess = service.run_mode === 'process';

  const [recommendation, setRecommendation] = useState<ServiceRecommendation | null>(null);
  const [customMem, setCustomMem] = useState('');
  const [customPort, setCustomPort] = useState('');
  const [customImage, setCustomImage] = useState('');
  const [customUsername, setCustomUsername] = useState('');
  const [customPassword, setCustomPassword] = useState('');
  const [customDatabase, setCustomDatabase] = useState('');
  const [operating, setOperating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Destroying a service deletes its volume, so it asks in-app where the warning is readable.
  const [confirmRequest, setConfirmRequest] = useState<ConfirmRequest | null>(null);
  // Deployment configuration is shown by default so operators immediately see customizable settings.
  const [expanded, setExpanded] = useState(true);
  const [deployResult, setDeployResult] = useState<DeployLocalResponse | null>(null);
  const [deployProgress, setDeployProgress] = useState<DeployProgressResponse | null>(null);
  const [deployError, setDeployError] = useState<string | null>(null);
  const [deployOpen, setDeployOpen] = useState(false);
  const [logsOpen, setLogsOpen] = useState(false);
  const [logsText, setLogsText] = useState('');
  const [logsLoading, setLogsLoading] = useState(false);

  useEffect(() => {
    api.getRecommendation(service.name).then(setRecommendation).catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [service.name]);

  const isLocalRunning = status?.status === 'running' && status?.deployment_type === 'local';
  const isRunning = isLocalRunning;
  const engineDown = engineStatus ? !engineStatus.docker.available : false;
  // A container that exists — running or stopped — is deployed, full stop. It can be
  // started again exactly as it was, or destroyed (which deletes its data); only the
  // absence of a container allows a fresh deploy with new credentials. Postgres applies
  // POSTGRES_USER/PASSWORD/DB on first boot only, so "redeploying" over a stopped
  // container is how a service ends up advertising credentials its volume never had.
  const containerExists = status?.container_exists === true;
  const canDeploy = !containerExists && !isLocalRunning;
  // Services that accept user-supplied credentials at deploy time.
  const acceptsCredentials = service.needs_credentials || service.name === 'postgres' || service.name === 'superset';
  const acceptsDatabase = service.name === 'postgres' || service.name === 'clickhouse';

  const handleDeploy = async () => {
    if (containerExists) {
      setError(`${service.display_name} is already deployed. Start it to keep its credentials and data, or destroy it before deploying again.`);
      return;
    }
    setDeployOpen(true);
    setDeployError(null);
    setOperating(true);
    setError(null);
    setDeployResult(null);
    setDeployProgress({
      service_name: service.name,
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
          const p = await api.getServiceDeployProgress(service.name);
          setDeployProgress(p);
        } catch { /* polling is best-effort */ }
      }, 1000);
      const memMb = parseInt(customMem) || recommendation?.recommended_ram_mb || 1024;
      const res = await api.deployLocalService({
        service_name: service.name,
        memory_mb: memMb,
        host_port: parseInt(customPort) || undefined,
        image: customImage || undefined,
        public_url: window.location.origin,
        username: customUsername.trim() || undefined,
        password: customPassword || undefined,
        database: customDatabase.trim() || undefined,
      });
      setDeployResult(res);
      onChanged();
    } catch (e: unknown) {
      const msg = extractErrorMessage(e, `Failed to deploy ${service.name}`);
      setDeployError(msg);
      setDeployProgress((prev) => ({
        service_name: service.name,
        phase: 'failed',
        detail: msg,
        pull_percent: prev?.pull_percent || 0,
        logs: prev?.logs || [],
        active: false,
        error: msg,
      }));
    } finally {
      if (timer) clearInterval(timer);
      setOperating(false);
    }
  };

  const handleStop = async () => {
    setOperating(true);
    setError(null);
    try {
      await api.stopService(service.name);
      onChanged();
    } catch (e: unknown) {
      setError(extractErrorMessage(e, `Failed to stop ${service.name}`));
    } finally {
      setOperating(false);
    }
  };

  const handleStart = async () => {
    setOperating(true);
    setError(null);
    try {
      const res = await api.startService(service.name);
      if (!res.ready) {
        setError(`${service.display_name} started, but it is not answering yet: ${res.message || 'no detail'}`);
      }
      onChanged();
    } catch (e: unknown) {
      setError(extractErrorMessage(e, `Failed to start ${service.name}`));
    } finally {
      setOperating(false);
    }
  };

  const handleRestart = async () => {
    setOperating(true);
    setError(null);
    try {
      const res = await api.restartService(service.name);
      if (!res.ready) {
        setError(`${service.display_name} restarted, but it is not answering yet: ${res.message || 'no detail'}`);
      }
      onChanged();
    } catch (e: unknown) {
      setError(extractErrorMessage(e, `Failed to restart ${service.name}`));
    } finally {
      setOperating(false);
    }
  };

  const handleDestroy = () => {
    setConfirmRequest({
      title: `Destroy ${service.name}?`,
      body: isProcess
        ? 'This stops the engine processes and permanently deletes its home directory — all persisted data will be lost.'
        : 'This permanently deletes the container AND its data volume — all persisted data will be lost.',
      confirmLabel: 'Destroy service',
      destructive: true,
      onConfirm: async () => {
        setOperating(true);
        setError(null);
        try {
          await api.destroyService(service.name);
          setDeployResult(null);
          onChanged();
        } catch (e: unknown) {
          setError(extractErrorMessage(e, `Failed to destroy ${service.name}`));
        } finally {
          setOperating(false);
        }
      },
    });
  };

  const handleShowLogs = async () => {
    setLogsOpen(true);
    setLogsLoading(true);
    try {
      const res = await api.getServiceLogs(service.name, 200);
      setLogsText(res.logs || '(no logs yet)');
    } catch (e: unknown) {
      setLogsText(extractErrorMessage(e, 'Failed to fetch logs'));
    } finally {
      setLogsLoading(false);
    }
  };

  return createPortal(
    <>
      {/* Drawer */}
      <div className="fixed inset-0 z-50 flex justify-end">
        <div className="absolute inset-0 bg-slate-900/50" onClick={onClose} />
        <div className="relative h-screen w-[420px] max-w-full bg-white shadow-2xl border-l border-slate-200 flex flex-col overflow-hidden">
          {/* Header */}
          <div className="px-5 py-4 border-b border-slate-200 flex items-start gap-3 shrink-0">
            <div className={`w-10 h-10 rounded-lg flex items-center justify-center shrink-0 ${isRunning ? 'bg-emerald-50 text-emerald-600' : 'bg-slate-100 text-slate-400'}`}>
              <Icon className="w-5 h-5" />
            </div>
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 flex-wrap">
                <h2 className="text-base font-bold text-slate-900 leading-none">{service.display_name}</h2>
                <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-slate-100 text-slate-600 border border-slate-200">{service.category}</span>
                <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded border ${isProcess ? 'bg-sky-50 text-sky-700 border-sky-200' : 'bg-slate-100 text-slate-500 border-slate-200'}`}>
                  {isProcess ? 'Process' : 'Container'}
                </span>
              </div>
              <p className="text-xs text-slate-500 mt-1">{service.description}</p>
            </div>
            <button onClick={onClose} className="p-1.5 rounded-lg text-slate-400 hover:text-slate-700 hover:bg-slate-100 transition-colors shrink-0" aria-label="Close">
              <X className="w-4 h-4" />
            </button>
          </div>

          {/* Body */}
          <div className="flex-1 overflow-y-auto px-5 py-4 space-y-4">
            <div className="flex items-center justify-between">
              {isRunning ? (
                <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-semibold bg-emerald-600 text-white">
                  <span className="w-1.5 h-1.5 rounded-full bg-white animate-pulse"></span>
                  Running
                </span>
              ) : (
                <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-medium bg-slate-200 text-slate-500">
                  <span className="w-1.5 h-1.5 rounded-full bg-slate-400"></span>
                  Stopped
                </span>
              )}
              {isRunning && service.web_ui && (
                <a href={serviceUrl(status?.host_port || status?.port || service.default_host_port, service.web_ui_path)} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-[11px] font-semibold bg-sky-50 text-sky-700 hover:bg-sky-100 border border-sky-200 transition-colors">
                  <ExternalLink className="w-3 h-3" />
                  <span>Open UI</span>
                </a>
              )}
            </div>

            {error && (
              <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-xs text-red-700">{error}</div>
            )}

            {status && isRunning && (
              <div className="p-2.5 rounded-lg bg-slate-50 border border-slate-200/80 grid grid-cols-3 gap-2 text-xs font-mono">
                <div>
                  <span className="text-[10px] text-slate-400 uppercase block">Host</span>
                  <span className="font-semibold text-slate-800">{status.host}</span>
                </div>
                <div>
                  <span className="text-[10px] text-slate-400 uppercase block">Port</span>
                  <span className="font-semibold text-slate-800">:{status.host_port || status.port}</span>
                </div>
                <div>
                  <span className="text-[10px] text-slate-400 uppercase block">RAM</span>
                  <span className="font-semibold text-slate-800">
                    {status.allocated_memory_mb > 0 ? `${(status.allocated_memory_mb / 1024).toFixed(1)} GB` : 'Uncapped'}
                  </span>
                </div>
              </div>
            )}

              <div className="space-y-3">
                <div className="flex items-center gap-2 flex-wrap">
                  {isLocalRunning ? (
                    <>
                      <button
                        onClick={handleRestart}
                        disabled={operating}
                        className="px-3.5 py-1.5 rounded-lg border border-slate-200 bg-white hover:bg-slate-50 text-slate-700 text-xs font-semibold flex items-center gap-1.5 shadow-2xs transition-all disabled:opacity-50"
                        title="Restart this service container"
                      >
                        <RotateCcw className={`w-3.5 h-3.5 ${operating ? 'animate-spin text-sky-600' : 'text-slate-500'}`} />
                        <span>{operating ? 'Restarting...' : 'Restart'}</span>
                      </button>
                      <button
                        onClick={handleStop}
                        disabled={operating}
                        className="px-3.5 py-1.5 rounded-lg border border-red-200 bg-red-50 hover:bg-red-100 text-red-700 text-xs font-semibold flex items-center gap-1.5 transition-all disabled:opacity-50"
                      >
                        <StopCircle className="w-3.5 h-3.5" />
                        <span>{operating ? 'Stopping...' : isProcess ? 'Stop Engine' : 'Stop Container'}</span>
                      </button>
                    </>
                  ) : containerExists ? (
                    <button onClick={handleStart} disabled={operating || (engineDown && !isProcess)} className="px-3.5 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-700 text-white text-xs font-semibold flex items-center gap-1.5 shadow-2xs transition-all disabled:opacity-50" title="Start this container exactly as it was deployed — same credentials, same data">
                      <Play className="w-3.5 h-3.5" />
                      <span>{operating ? 'Starting...' : 'Start Container'}</span>
                    </button>
                  ) : (
                    <button onClick={handleDeploy} disabled={operating || !service.deployable || (engineDown && !isProcess)} className="px-3.5 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold flex items-center gap-1.5 shadow-2xs transition-all disabled:opacity-50">
                      <Zap className="w-3.5 h-3.5" />
                      <span>{operating ? 'Deploying...' : isProcess ? 'Start Engine' : 'Deploy Container'}</span>
                    </button>
                  )}

                  {!isProcess && canDeploy && (
                    <button
                      onClick={() => setExpanded((v) => !v)}
                      className="px-3 py-1.5 rounded-lg border border-slate-200 bg-white hover:bg-slate-50 text-slate-600 text-xs font-semibold flex items-center gap-1.5 transition-all"
                      title={expanded ? 'Hide configuration' : 'Configure memory, port, and image'}
                    >
                      <ChevronDown className={`w-3.5 h-3.5 transition-transform ${expanded ? 'rotate-180' : ''}`} />
                      <span>{expanded ? 'Hide Config' : 'Configure'}</span>
                    </button>
                  )}

                  {(isLocalRunning || containerExists) && (
                    <>
                      <button onClick={handleShowLogs} className="px-2.5 py-1.5 rounded-lg border border-slate-200 bg-white hover:bg-slate-50 text-slate-600 text-xs font-semibold flex items-center gap-1.5 transition-all" title="View logs">
                        <Terminal className="w-3.5 h-3.5" />
                        <span>Logs</span>
                      </button>
                      <button onClick={handleDestroy} className="px-2.5 py-1.5 rounded-lg border border-red-200 bg-white hover:bg-red-50 text-red-600 text-xs font-semibold flex items-center gap-1.5 transition-all" title={isProcess ? 'Stop processes and delete its home directory' : 'Remove the container and permanently delete its data volume'}>
                        <Trash2 className="w-3.5 h-3.5" />
                        <span>Destroy</span>
                      </button>
                    </>
                  )}
                </div>

                {containerExists && (
                  <div className="p-2.5 rounded-lg bg-slate-50 border border-slate-200/80 text-[11px] text-slate-600 leading-relaxed">
                    Deployed and fixed: this container keeps the credentials and configuration it was
                    created with. <span className="font-semibold text-slate-700">Start</span> resumes it
                    exactly as it was; <span className="font-semibold text-slate-700">Destroy</span> removes
                    it and its data, after which you can deploy again with different settings.
                  </div>
                )}

                {expanded && !isProcess && canDeploy && (
                  <div className="space-y-3 border-t border-slate-100 pt-3">
                    <div className="flex items-center justify-between text-[11px] font-bold uppercase tracking-wider text-slate-500">
                      <span>Deployment Configuration</span>
                    </div>
                    {recommendation && (
                      <div className="flex items-center justify-between text-xs text-slate-500 font-mono bg-amber-50/60 border border-amber-100 px-3 py-1.5 rounded-md">
                        <span>Recommended RAM:</span>
                        <span className="font-bold text-amber-900">{(recommendation.recommended_ram_mb / 1024).toFixed(1)} GB ({recommendation.recommended_ram_mb} MB)</span>
                      </div>
                    )}
                    <div className="grid grid-cols-3 gap-2">
                      <div>
                        <label className="text-[11px] font-medium text-slate-500 block mb-1">Memory (MB)</label>
                        <input type="number" min="256" step="256" placeholder={recommendation ? String(recommendation.recommended_ram_mb) : '2048'} value={customMem} onChange={(e) => setCustomMem(e.target.value)} className="w-full px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500" />
                      </div>
                      <div>
                        <label className="text-[11px] font-medium text-slate-500 block mb-1">Host Port</label>
                        <input type="number" placeholder={String(service.default_host_port)} value={customPort} onChange={(e) => setCustomPort(e.target.value)} className="w-full px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500" />
                      </div>
                      <div>
                        <label className="text-[11px] font-medium text-slate-500 block mb-1">Image Tag</label>
                        <input type="text" placeholder={service.image || 'blueprint default'} value={customImage} onChange={(e) => setCustomImage(e.target.value)} className="w-full px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500" />
                      </div>
                    </div>

                    {acceptsCredentials && (
                      <div className={`grid gap-2 ${acceptsDatabase ? 'grid-cols-3' : 'grid-cols-2'}`}>
                        <div>
                          <label className="text-[11px] font-medium text-slate-500 block mb-1">Username</label>
                          <input type="text" placeholder={service.default_username || 'postgres'} value={customUsername} onChange={(e) => setCustomUsername(e.target.value)} className="w-full px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500" />
                        </div>
                        <div>
                          <label className="text-[11px] font-medium text-slate-500 block mb-1">Password</label>
                          <input type="password" autoComplete="new-password" placeholder="Auto-generate if blank" value={customPassword} onChange={(e) => setCustomPassword(e.target.value)} className="w-full px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500" />
                        </div>
                        {acceptsDatabase && (
                          <div>
                            <label className="text-[11px] font-medium text-slate-500 block mb-1">Database</label>
                            <input type="text" placeholder={service.default_database || 'obelisk'} value={customDatabase} onChange={(e) => setCustomDatabase(e.target.value)} className="w-full px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500" />
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                )}

                {!service.deployable && (
                  <div className="text-[11px] text-amber-700 bg-amber-50 border border-amber-100 rounded-md px-3 py-2 leading-relaxed">
                    Local deploy unavailable: {service.reason_not_deployable}
                  </div>
                )}
              </div>
          </div>
        </div>
      </div>

      {deployOpen && (
        <DeployModal
          isOpen={deployOpen}
          onClose={() => {
            setDeployOpen(false);
            setDeployError(null);
            setDeployProgress(null);
          }}
          serviceId={service.name}
          catalogEntry={service}
          progress={deployProgress}
          deployResult={deployResult}
          error={deployError}
          isOperating={operating}
          onRetry={handleDeploy}
        />
      )}

      {logsOpen && (
        <div className="fixed inset-0 z-[60] flex items-center justify-center bg-slate-900/50 p-8" onClick={() => setLogsOpen(false)}>
          <div className="bg-slate-950 rounded-xl border border-slate-700 w-full max-w-4xl h-[70vh] flex flex-col overflow-hidden" onClick={(e) => e.stopPropagation()}>
            <div className="flex items-center justify-between px-4 py-2.5 border-b border-slate-700 bg-slate-900">
              <div className="flex items-center gap-2 text-xs font-mono text-slate-200">
                <Terminal className="w-4 h-4 text-emerald-400" />
                Logs — {service.name} (last 200 lines)
              </div>
              <div className="flex items-center gap-2">
                <button onClick={handleShowLogs} className="px-2 py-1 rounded text-[11px] text-slate-300 hover:text-white hover:bg-slate-700">Refresh</button>
                <button onClick={() => setLogsOpen(false)} className="px-2 py-1 rounded text-[11px] text-slate-300 hover:text-white hover:bg-slate-700">Close ✕</button>
              </div>
            </div>
            <pre className="flex-1 overflow-auto p-4 text-[11px] leading-relaxed font-mono text-emerald-300 whitespace-pre-wrap">
              {logsLoading ? 'Loading logs...' : logsText}
            </pre>
          </div>
        </div>
      )}

      <ConfirmDialog request={confirmRequest} onClose={() => setConfirmRequest(null)} />
    </>,
    document.body
  );
};
