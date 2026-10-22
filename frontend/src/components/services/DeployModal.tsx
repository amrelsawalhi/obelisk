import React, { useState, useRef, useEffect } from 'react';
import {
  X,
  Zap,
  CheckCircle2,
  AlertCircle,
  RefreshCw,
  Terminal,
  ExternalLink,
  Copy,
  Check,
  ShieldCheck,
  RotateCcw,
} from 'lucide-react';
import type {
  DeployLocalResponse,
  DeployProgressResponse,
  ServiceCatalogEntry,
} from '../../lib/types';

interface DeployModalProps {
  isOpen: boolean;
  onClose: () => void;
  serviceId: string;
  catalogEntry?: ServiceCatalogEntry;
  progress: DeployProgressResponse | null;
  deployResult: DeployLocalResponse | null;
  error: string | null;
  isOperating: boolean;
  onRetry: () => void;
}

interface StepDef {
  id: string;
  label: string;
  desc: string;
}

const DEPLOY_STEPS: StepDef[] = [
  { id: 'pre-flight', label: 'Pre-flight', desc: 'Docker engine & host port check' },
  { id: 'pulling', label: 'Pull Image', desc: 'Fetch container image layers' },
  { id: 'creating', label: 'Create Container', desc: 'Mount volumes & configure RAM cap' },
  { id: 'probing', label: 'Readiness Probe', desc: 'Wait for service health response' },
  { id: 'ready', label: 'Catalog Registered', desc: 'Auto-connected to Obelisk Catalog' },
];

export const DeployModal: React.FC<DeployModalProps> = ({
  isOpen,
  onClose,
  serviceId,
  catalogEntry,
  progress,
  deployResult,
  error,
  isOperating,
  onRetry,
}) => {
  const [copiedKey, setCopiedKey] = useState<string | null>(null);
  const [showLogs, setShowLogs] = useState(true);
  const terminalRef = useRef<HTMLDivElement>(null);

  // Auto-scroll terminal log when new log messages arrive
  useEffect(() => {
    if (terminalRef.current && showLogs) {
      terminalRef.current.scrollTop = terminalRef.current.scrollHeight;
    }
  }, [progress?.logs, progress?.detail, showLogs]);

  if (!isOpen) return null;

  const currentPhase = progress?.phase || (isOperating ? 'pre-flight' : deployResult ? 'ready' : error ? 'failed' : 'idle');
  const isFailed = !!error || currentPhase === 'failed';
  const isCompleted = !isOperating && !isFailed && (deployResult?.ready || currentPhase === 'ready');

  // Determine the index of the currently active step (0-4)
  const getStepIndex = (phase: string): number => {
    switch (phase) {
      case 'idle':
        return 0;
      case 'pre-flight':
        return 0;
      case 'pulling':
        return 1;
      case 'creating':
        return 2;
      case 'initializing':
      case 'probing':
      case 'starting':
        return 3;
      case 'ready':
        return 4;
      default:
        return 0;
    }
  };

  const activeStepIndex = isCompleted ? 5 : getStepIndex(currentPhase);

  const handleCopy = (text: string, key: string) => {
    navigator.clipboard.writeText(text);
    setCopiedKey(key);
    setTimeout(() => setCopiedKey(null), 2000);
  };

  const displayName = catalogEntry?.display_name || serviceId;
  const hostPort = deployResult?.port || catalogEntry?.default_host_port || 3000;
  const webUiUrl = `http://localhost:${hostPort}`;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 animate-in fade-in duration-150">
      <div className="w-full max-w-xl bg-white rounded-2xl shadow-2xl border border-slate-200 overflow-hidden flex flex-col max-h-[92vh] animate-in zoom-in-95 duration-150">
        
        {/* Header */}
        <div className="px-6 py-4 border-b border-slate-200 flex items-center justify-between bg-slate-50/80 shrink-0">
          <div className="flex items-center gap-3">
            <div className={`p-2.5 rounded-xl ${
              isCompleted
                ? 'bg-emerald-100 text-emerald-700'
                : isFailed
                ? 'bg-rose-100 text-rose-700'
                : 'bg-sky-100 text-sky-700'
            }`}>
              {isCompleted ? (
                <CheckCircle2 className="w-5 h-5" />
              ) : isFailed ? (
                <AlertCircle className="w-5 h-5" />
              ) : (
                <Zap className="w-5 h-5 animate-pulse" />
              )}
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h3 className="text-sm font-bold text-slate-900">
                  {isCompleted ? 'Service Ready' : isFailed ? 'Deployment Failed' : `Deploying ${displayName}`}
                </h3>
                <span className={`text-[10px] font-mono px-2 py-0.5 rounded-full font-bold uppercase ${
                  isCompleted
                    ? 'bg-emerald-100 text-emerald-800 border border-emerald-200'
                    : isFailed
                    ? 'bg-rose-100 text-rose-800 border border-rose-200'
                    : 'bg-sky-100 text-sky-800 border border-sky-200'
                }`}>
                  {isCompleted ? 'Online' : isFailed ? 'Failed' : currentPhase}
                </span>
              </div>
              <p className="text-xs text-slate-500 font-mono mt-0.5">
                Localhost Engine · {catalogEntry?.run_mode === 'process' ? 'Process' : 'Container'}: <span className="text-slate-700 font-semibold">obelisk_{serviceId}</span>
              </p>
            </div>
          </div>
          
          <button
            onClick={onClose}
            disabled={isOperating && !isFailed}
            title={isOperating ? 'Deployment in progress...' : 'Close popup'}
            className="text-slate-400 hover:text-slate-600 disabled:opacity-30 disabled:hover:text-slate-400 p-1.5 rounded-lg hover:bg-slate-200/50 transition-colors"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Modal Body */}
        <div className="p-6 space-y-5 overflow-y-auto custom-scrollbar flex-1">
          
          {/* Stepper / Phase Tracker */}
          <div className="bg-slate-50/70 border border-slate-200 rounded-xl p-4">
            <div className="text-[11px] font-bold text-slate-500 uppercase tracking-wider mb-3">
              Deployment Pipeline
            </div>
            <div className="space-y-3">
              {DEPLOY_STEPS.map((step, idx) => {
                const isStepFinished = isCompleted || activeStepIndex > idx;
                const isStepActive = !isCompleted && !isFailed && activeStepIndex === idx;
                const isStepFailed = isFailed && activeStepIndex === idx;

                return (
                  <div key={step.id} className="flex items-start gap-3">
                    {/* Step Icon */}
                    <div className="mt-0.5 shrink-0">
                      {isStepFinished ? (
                        <div className="w-5 h-5 rounded-full bg-emerald-500 text-white flex items-center justify-center shadow-xs">
                          <Check className="w-3 h-3 stroke-[3]" />
                        </div>
                      ) : isStepFailed ? (
                        <div className="w-5 h-5 rounded-full bg-rose-500 text-white flex items-center justify-center shadow-xs">
                          <X className="w-3 h-3 stroke-[3]" />
                        </div>
                      ) : isStepActive ? (
                        <div className="w-5 h-5 rounded-full bg-sky-500 text-white flex items-center justify-center shadow-xs">
                          <RefreshCw className="w-3 h-3 animate-spin stroke-[2.5]" />
                        </div>
                      ) : (
                        <div className="w-5 h-5 rounded-full bg-slate-200 text-slate-500 flex items-center justify-center text-[10px] font-bold font-mono">
                          {idx + 1}
                        </div>
                      )}
                    </div>

                    {/* Step Text */}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between">
                        <span className={`text-xs font-semibold ${
                          isStepFinished
                            ? 'text-emerald-900'
                            : isStepActive
                            ? 'text-sky-900 font-bold'
                            : isStepFailed
                            ? 'text-rose-900 font-bold'
                            : 'text-slate-400'
                        }`}>
                          {step.label}
                        </span>
                        {isStepActive && (
                          <span className="text-[10px] text-sky-600 font-mono animate-pulse font-semibold">
                            In progress...
                          </span>
                        )}
                        {isStepFinished && (
                          <span className="text-[10px] text-emerald-600 font-mono font-medium">
                            Done
                          </span>
                        )}
                        {isStepFailed && (
                          <span className="text-[10px] text-rose-600 font-mono font-medium">
                            Error
                          </span>
                        )}
                      </div>
                      <p className={`text-[11px] ${isStepActive ? 'text-sky-700' : 'text-slate-400'} truncate`}>
                        {isStepActive && progress?.detail ? progress.detail : step.desc}
                      </p>

                      {/* Pull Progress Bar (only during pulling phase) */}
                      {isStepActive && step.id === 'pulling' && progress && (
                        <div className="mt-2 space-y-1">
                          <div className="w-full bg-sky-100 rounded-full h-1.5 overflow-hidden">
                            <div
                              className="bg-sky-600 h-1.5 rounded-full transition-all duration-300"
                              style={{ width: `${Math.max(5, progress.pull_percent)}%` }}
                            />
                          </div>
                          <div className="flex justify-between text-[10px] text-sky-700 font-mono">
                            <span>Pulling layers...</span>
                            <span className="font-bold">{progress.pull_percent}%</span>
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Error Banner */}
          {isFailed && (
            <div className="p-4 rounded-xl border border-rose-200 bg-rose-50/80 text-rose-900 space-y-2 animate-in fade-in">
              <div className="flex items-center gap-2 font-semibold text-xs text-rose-800">
                <AlertCircle className="w-4 h-4 text-rose-600 shrink-0" />
                <span>Deployment Stopped</span>
              </div>
              <p className="text-xs font-mono text-rose-800 leading-relaxed bg-white/70 p-3 rounded-lg border border-rose-200 break-words">
                {error || progress?.error || 'Unknown error occurred during deployment.'}
              </p>
              <div className="text-[11px] text-rose-700">
                Tip: If the Docker engine was not reachable, launch Docker Desktop, verify the daemon is running, and click Retry.
              </div>
            </div>
          )}

          {/* Success Summary & Vaulted Credentials */}
          {isCompleted && deployResult && (
            <div className="space-y-3 animate-in fade-in">
              <div className="p-4 rounded-xl border border-emerald-200 bg-emerald-50 text-emerald-900 space-y-2.5">
                <div className="flex items-center justify-between">
                  <div className="font-bold text-xs flex items-center gap-2">
                    <ShieldCheck className="w-4 h-4 text-emerald-600" />
                    <span>Container is running and healthy!</span>
                  </div>
                  {deployResult.registered_connection && (
                    <span className="text-[10px] bg-emerald-200 text-emerald-900 px-2 py-0.5 rounded font-mono font-medium">
                      {deployResult.registered_connection}
                    </span>
                  )}
                </div>

                {deployResult.message && (
                  <p className="text-xs text-emerald-800 leading-relaxed">
                    {deployResult.message}
                  </p>
                )}

                {/* Vaulted Credentials Display */}
                {deployResult.credentials && (
                  <div className="mt-3 bg-white/80 rounded-lg p-3 border border-emerald-200 text-xs font-mono space-y-1.5">
                    <div className="text-[10px] uppercase font-bold text-slate-500 font-sans tracking-wider mb-1 flex items-center justify-between">
                      <span>Vaulted Connection Credentials</span>
                      <span className="text-emerald-700 font-normal">Encrypted at rest</span>
                    </div>

                    <div className="grid grid-cols-2 gap-2 text-[11px]">
                      <div>
                        <span className="text-slate-400">Host:</span>{' '}
                        <span className="text-slate-800 font-bold">localhost</span>
                      </div>
                      <div>
                        <span className="text-slate-400">Port:</span>{' '}
                        <span className="text-slate-800 font-bold">{hostPort}</span>
                      </div>
                      {deployResult.credentials.username && (
                        <div>
                          <span className="text-slate-400">User:</span>{' '}
                          <span className="text-slate-800 font-bold">{deployResult.credentials.username}</span>
                        </div>
                      )}
                      {deployResult.credentials.database && (
                        <div>
                          <span className="text-slate-400">DB:</span>{' '}
                          <span className="text-slate-800 font-bold">{deployResult.credentials.database}</span>
                        </div>
                      )}
                    </div>

                    {deployResult.credentials.password && (
                      <div className="pt-1.5 flex items-center justify-between border-t border-emerald-100">
                        <span className="text-[11px] text-slate-400">Password:</span>
                        <div className="flex items-center gap-1.5">
                          <code className="text-[11px] bg-slate-100 px-1.5 py-0.5 rounded font-bold text-slate-800 select-all">
                            {deployResult.credentials.password}
                          </code>
                          <button
                            type="button"
                            onClick={() => handleCopy(deployResult.credentials?.password || '', 'pw')}
                            className="p-1 hover:bg-slate-200 rounded text-slate-500 hover:text-slate-800 transition-colors"
                            title="Copy Password"
                          >
                            {copiedKey === 'pw' ? (
                              <Check className="w-3.5 h-3.5 text-emerald-600" />
                            ) : (
                              <Copy className="w-3.5 h-3.5" />
                            )}
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Terminal / Live Logs View */}
          <div className="rounded-xl border border-slate-800 bg-slate-950 overflow-hidden shadow-inner">
            <div className="px-3 py-2 bg-slate-900 border-b border-slate-800 flex items-center justify-between text-xs text-slate-300">
              <div className="flex items-center gap-2 font-mono text-[11px]">
                <Terminal className="w-3.5 h-3.5 text-sky-400" />
                <span>Deployment Logs</span>
                {progress?.logs?.length ? (
                  <span className="text-[10px] text-slate-500 font-sans">
                    ({progress.logs.length} entries)
                  </span>
                ) : null}
              </div>
              <button
                type="button"
                onClick={() => setShowLogs(!showLogs)}
                className="text-[10px] text-slate-400 hover:text-slate-200 transition-colors"
              >
                {showLogs ? 'Hide' : 'Show'}
              </button>
            </div>

            {showLogs && (
              <div
                ref={terminalRef}
                className="p-3 text-[11px] font-mono text-slate-200 max-h-36 min-h-[70px] overflow-y-auto space-y-1 custom-scrollbar"
              >
                {progress?.detail && (
                  <div className="text-sky-400 flex items-center gap-1.5">
                    <span className="text-slate-600">›</span>
                    <span>{progress.detail}</span>
                  </div>
                )}
                {progress?.logs && progress.logs.length > 0 ? (
                  progress.logs.map((line, idx) => (
                    <div key={idx} className="text-slate-300 whitespace-pre-wrap leading-relaxed">
                      {line}
                    </div>
                  ))
                ) : !progress?.detail ? (
                  <div className="text-slate-500 italic">Connecting to engine...</div>
                ) : null}

                {isCompleted && (
                  <div className="text-emerald-400 font-semibold pt-1">
                    ✓ Deployment completed successfully. Ready on port {hostPort}.
                  </div>
                )}

                {isFailed && (
                  <div className="text-rose-400 font-semibold pt-1">
                    ✗ Process terminated: {error || progress?.error || 'Operation failed'}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        {/* Footer Actions */}
        <div className="px-6 py-4 border-t border-slate-200 bg-slate-50 flex items-center justify-between shrink-0">
          <div>
            {isOperating && (
              <div className="flex items-center gap-2 text-xs text-slate-500">
                <RefreshCw className="w-3.5 h-3.5 text-sky-600 animate-spin" />
                <span>Deploying engine in background...</span>
              </div>
            )}
          </div>

          <div className="flex items-center gap-2.5">
            {isFailed && (
              <button
                type="button"
                onClick={onRetry}
                className="px-3.5 py-1.5 bg-slate-900 hover:bg-slate-800 text-white rounded-lg text-xs font-semibold flex items-center gap-1.5 transition-colors shadow-2xs"
              >
                <RotateCcw className="w-3.5 h-3.5" />
                <span>Retry Deployment</span>
              </button>
            )}

            {isCompleted && catalogEntry?.web_ui && (
              <a
                href={webUiUrl}
                target="_blank"
                rel="noreferrer"
                className="px-3.5 py-1.5 bg-sky-600 hover:bg-sky-700 text-white rounded-lg text-xs font-semibold flex items-center gap-1.5 transition-colors shadow-2xs"
              >
                <span>Open Web UI</span>
                <ExternalLink className="w-3.5 h-3.5" />
              </a>
            )}

            <button
              type="button"
              onClick={onClose}
              disabled={isOperating && !isFailed}
              className={`px-4 py-1.5 rounded-lg text-xs font-semibold transition-colors ${
                isCompleted
                  ? 'bg-slate-900 hover:bg-slate-800 text-white shadow-2xs'
                  : 'bg-white border border-slate-200 text-slate-700 hover:bg-slate-100 disabled:opacity-40'
              }`}
            >
              {isCompleted ? 'Done' : 'Close'}
            </button>
          </div>
        </div>

      </div>
    </div>
  );
};
