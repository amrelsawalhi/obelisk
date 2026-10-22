import React, { useState, useEffect } from 'react';
import {
  Play,
  RefreshCw,
  CheckCircle2,
  AlertCircle,
  Terminal,
  Database,
  Plus,
  Trash2,
  Clock,
  ExternalLink,
  Search,
  X,
  Loader2,
  Power,
  BookOpen,
  Code2,
  Sparkles,
  Maximize2,
  Minimize2,
} from 'lucide-react';
import { api } from '../../lib/api';
import type {
  OrchestrationEngineInfo,
  CronJob,
  CreateCronJobRequest,
  CronTargetsResponse,
} from '../../lib/types';
import { DagsterNativePanel } from './DagsterNativePanel';

interface PipelinesViewProps {
  onOpenInQuery?: (sql: string) => void;
  onNavigateToSettings?: () => void;
}

export const PipelinesView: React.FC<PipelinesViewProps> = ({ onNavigateToSettings }) => {
  // Engines & Active View State
  const [engines, setEngines] = useState<OrchestrationEngineInfo[]>([]);
  const [selectedEngineId, setSelectedEngineId] = useState<string>('cron');
  const [loadingEngines, setLoadingEngines] = useState<boolean>(true);

  // Obelisk Cron State
  const [cronJobs, setCronJobs] = useState<CronJob[]>([]);
  const [cronTargets, setCronTargets] = useState<CronTargetsResponse | null>(null);
  const [loadingJobs, setLoadingJobs] = useState<boolean>(false);
  const [filterType, setFilterType] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');

  // Immediate Run & Logs State
  const [runningJobId, setRunningJobId] = useState<string | null>(null);
  const [activeLogJob, setActiveLogJob] = useState<CronJob | null>(null);
  const [isLogDrawerOpen, setIsLogDrawerOpen] = useState<boolean>(false);

  // New Cron Job Modal State
  const [isCreateModalOpen, setIsCreateModalOpen] = useState<boolean>(false);
  const [newJobName, setNewJobName] = useState<string>('');
  const [newJobDesc, setNewJobDesc] = useState<string>('');
  const [newJobType, setNewJobType] = useState<'dbt' | 'script' | 'notebook'>('script');
  const [newJobTarget, setNewJobTarget] = useState<string>('');
  const [newJobCron, setNewJobCron] = useState<string>('0 4 * * *');
  const [creatingJob, setCreatingJob] = useState<boolean>(false);

  // Embedded Iframe Controls
  const [iframeKey, setIframeKey] = useState<number>(Date.now());
  const [isFullscreen, setIsFullscreen] = useState<boolean>(false);

  // Global Error & Feedback
  const [feedback, setFeedback] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  // Fetch all engines and cron jobs
  const fetchEnginesAndJobs = async () => {
    try {
      setLoadingEngines(true);
      const res = await api.getOrchestrationEngines();
      setEngines(res.engines);

      // Default to Dagster if running, otherwise use server recommendation or cron
      const dagsterRunning = res.engines.find((e) => e.id === 'dagster' && e.status === 'running');
      if (dagsterRunning) {
        setSelectedEngineId((prev) => (prev === 'cron' ? 'dagster' : prev));
      } else {
        setSelectedEngineId((prev) => prev || res.default_engine || 'cron');
      }

      await loadCronData();
    } catch (err: unknown) {
      setFeedback({
        type: 'error',
        message: err instanceof Error ? err.message : 'Failed to load orchestration engines',
      });
    } finally {
      setLoadingEngines(false);
    }
  };

  const loadCronData = async () => {
    try {
      setLoadingJobs(true);
      const [jobs, targets] = await Promise.all([
        api.getCronJobs(),
        api.getCronTargets().catch(() => null),
      ]);
      setCronJobs(jobs);
      if (targets) {
        setCronTargets(targets);
        // Default target if empty
        if (!newJobTarget) {
          if (newJobType === 'script' && targets.scripts.length > 0) {
            setNewJobTarget(targets.scripts[0].filename);
          } else if (newJobType === 'dbt' && targets.dbt_selectors.length > 0) {
            setNewJobTarget(targets.dbt_selectors[0].value);
          } else if (newJobType === 'notebook' && targets.notebooks.length > 0) {
            setNewJobTarget(targets.notebooks[0].filename);
          }
        }
      }
    } catch (err: unknown) {
      setFeedback({
        type: 'error',
        message: err instanceof Error ? err.message : 'Failed to fetch cron data',
      });
    } finally {
      setLoadingJobs(false);
    }
  };

  useEffect(() => {
    fetchEnginesAndJobs();
  }, []);

  // Update default target when target type changes in modal
  const handleTypeChange = (type: 'dbt' | 'script' | 'notebook') => {
    setNewJobType(type);
    if (!cronTargets) return;
    if (type === 'script') {
      setNewJobTarget(cronTargets.scripts[0]?.filename || 'daily_kpi_summary.py');
    } else if (type === 'dbt') {
      setNewJobTarget(cronTargets.dbt_selectors[0]?.value || '*');
    } else if (type === 'notebook') {
      setNewJobTarget(cronTargets.notebooks[0]?.filename || '');
    }
  };

  // Immediate manual run
  const handleRunNow = async (job: CronJob) => {
    try {
      setRunningJobId(job.id);
      setFeedback(null);
      const res = await api.runCronJobNow(job.id);
      setFeedback({
        type: res.status === 'success' ? 'success' : 'error',
        message: `Job '${job.name}' finished with status: ${res.status.toUpperCase()} (${res.duration_seconds}s)`,
      });
      await loadCronData();
      // Auto open log drawer for convenience
      const updated = {
        ...job,
        last_status: res.status,
        last_duration_seconds: res.duration_seconds,
        last_logs: res.logs,
      };
      setActiveLogJob(updated);
      setIsLogDrawerOpen(true);
    } catch (err: unknown) {
      setFeedback({
        type: 'error',
        message: err instanceof Error ? err.message : `Failed to execute job '${job.name}'`,
      });
    } finally {
      setRunningJobId(null);
    }
  };

  // Toggle enable/disable
  const handleToggleJob = async (id: string) => {
    try {
      await api.toggleCronJob(id);
      await loadCronData();
    } catch (err: unknown) {
      setFeedback({
        type: 'error',
        message: err instanceof Error ? err.message : 'Failed to toggle job status',
      });
    }
  };

  // Delete job
  const handleDeleteJob = async (job: CronJob) => {
    if (!confirm(`Are you sure you want to delete scheduled job '${job.name}'?`)) return;
    try {
      await api.deleteCronJob(job.id);
      setFeedback({ type: 'success', message: `Job '${job.name}' deleted successfully.` });
      await loadCronData();
      if (activeLogJob?.id === job.id) {
        setIsLogDrawerOpen(false);
        setActiveLogJob(null);
      }
    } catch (err: unknown) {
      setFeedback({
        type: 'error',
        message: err instanceof Error ? err.message : 'Failed to delete job',
      });
    }
  };

  // Create new cron job
  const handleCreateJob = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newJobName.trim() || !newJobTarget.trim() || !newJobCron.trim()) {
      setFeedback({ type: 'error', message: 'Please fill in all required job fields' });
      return;
    }

    try {
      setCreatingJob(true);
      const payload: CreateCronJobRequest = {
        name: newJobName.trim(),
        description: newJobDesc.trim(),
        target_type: newJobType,
        target: newJobTarget.trim(),
        cron_expression: newJobCron.trim(),
      };
      await api.createCronJob(payload);
      setFeedback({ type: 'success', message: `Job '${newJobName.trim()}' scheduled successfully.` });
      setIsCreateModalOpen(false);
      setNewJobName('');
      setNewJobDesc('');
      await loadCronData();
    } catch (err: unknown) {
      setFeedback({
        type: 'error',
        message: err instanceof Error ? err.message : 'Failed to schedule job',
      });
    } finally {
      setCreatingJob(false);
    }
  };

  // 1-Click Deploy Service from Pipeline Tab
  const handleDeployLocal = async (serviceId: string) => {
    try {
      setFeedback({ type: 'success', message: `Deploying ${serviceId} locally on localhost...` });
      await api.deployLocalService({
        service_name: serviceId,
      });
      setFeedback({
        type: 'success',
        message: `${serviceId} deployment initiated successfully! Refreshing status...`,
      });
      await fetchEnginesAndJobs();
    } catch (err: unknown) {
      setFeedback({
        type: 'error',
        message: err instanceof Error ? err.message : `Failed to deploy ${serviceId}`,
      });
    }
  };

  // Filtered Cron Jobs
  const filteredJobs = cronJobs.filter((job) => {
    if (filterType !== 'all' && job.target_type !== filterType) return false;
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      return (
        job.name.toLowerCase().includes(q) ||
        job.target.toLowerCase().includes(q) ||
        job.description?.toLowerCase().includes(q) ||
        job.cron_expression.toLowerCase().includes(q)
      );
    }
    return true;
  });

  const selectedEngine = engines.find((e) => e.id === selectedEngineId) || engines[0];

  // Helper for human-readable cron presets
  const cronPresets = [
    { label: 'Every 15 min', expr: '*/15 * * * *' },
    { label: 'Hourly', expr: '0 * * * *' },
    { label: 'Daily at Midnight', expr: '0 0 * * *' },
    { label: 'Daily at 04:00 UTC', expr: '0 4 * * *' },
    { label: 'Weekly (Sunday)', expr: '0 0 * * 0' },
  ];

  return (
    <div
      className={`flex-1 w-full h-full flex flex-col min-w-0 bg-slate-50 overflow-hidden ${
        isFullscreen ? 'fixed inset-0 z-50 bg-white' : ''
      }`}
    >
      {/* Top Multi-Orchestrator Switcher & Action Bar */}
      <div className="bg-white border-b border-slate-200 px-6 py-2.5 flex flex-wrap items-center justify-between gap-4 shrink-0 shadow-xs">
        {/* Engine Switcher Tabs */}
        <div className="flex items-center bg-slate-100 p-1 rounded-xl border border-slate-200/80">
          <button
            onClick={() => setSelectedEngineId('cron')}
            className={`flex items-center gap-2 px-3.5 py-1.5 rounded-lg text-xs font-semibold transition-all ${
              selectedEngineId === 'cron'
                ? 'bg-white text-indigo-700 shadow-xs border border-slate-200/50'
                : 'text-slate-600 hover:text-slate-900 hover:bg-slate-200/50'
            }`}
          >
            <Clock className="w-3.5 h-3.5 text-indigo-600" />
            <span>Obelisk Cron</span>
            <span className="px-1.5 py-0.2 bg-emerald-100 text-emerald-800 text-[10px] font-mono rounded">
              Active
            </span>
          </button>

          <button
            onClick={() => setSelectedEngineId('dagster')}
            className={`flex items-center gap-2 px-3.5 py-1.5 rounded-lg text-xs font-semibold transition-all ${
              selectedEngineId === 'dagster'
                ? 'bg-white text-indigo-700 shadow-xs border border-slate-200/50'
                : 'text-slate-600 hover:text-slate-900 hover:bg-slate-200/50'
            }`}
          >
            <RefreshCw className="w-3.5 h-3.5 text-violet-600" />
            <span>Dagster</span>
            {engines.find((e) => e.id === 'dagster')?.status === 'running' ? (
              <span className="flex items-center gap-1 text-[10px] text-emerald-600 font-mono">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                Live
              </span>
            ) : (
              <span className="px-1.5 py-0.2 bg-slate-200 text-slate-600 text-[10px] font-mono rounded">
                Offline
              </span>
            )}
          </button>

          <button
            onClick={() => setSelectedEngineId('airflow')}
            className={`flex items-center gap-2 px-3.5 py-1.5 rounded-lg text-xs font-semibold transition-all ${
              selectedEngineId === 'airflow'
                ? 'bg-white text-indigo-700 shadow-xs border border-slate-200/50'
                : 'text-slate-600 hover:text-slate-900 hover:bg-slate-200/50'
            }`}
          >
            <Sparkles className="w-3.5 h-3.5 text-sky-600" />
            <span>Apache Airflow</span>
            {engines.find((e) => e.id === 'airflow')?.status === 'running' ? (
              <span className="flex items-center gap-1 text-[10px] text-emerald-600 font-mono">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
                Live
              </span>
            ) : (
              <span className="px-1.5 py-0.2 bg-slate-200 text-slate-600 text-[10px] font-mono rounded">
                Offline
              </span>
            )}
          </button>
        </div>

        {/* Global Action Toolbar */}
        <div className="flex items-center gap-2">
          {selectedEngineId === 'cron' ? (
            <button
              onClick={() => {
                if (cronTargets) {
                  setNewJobType('script');
                  setNewJobTarget(cronTargets.scripts[0]?.filename || 'daily_kpi_summary.py');
                }
                setIsCreateModalOpen(true);
              }}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-semibold rounded-lg shadow-xs transition-colors"
            >
              <Plus className="w-3.5 h-3.5" />
              <span>New Scheduled Job</span>
            </button>
          ) : (
            <div className="flex items-center gap-2">
              <button
                onClick={() => setIframeKey(Date.now())}
                title="Reload Iframe"
                className="p-1.5 text-slate-500 hover:text-slate-800 hover:bg-slate-100 rounded-lg border border-slate-200 transition-colors"
              >
                <RefreshCw className="w-3.5 h-3.5" />
              </button>
              <button
                onClick={() => setIsFullscreen(!isFullscreen)}
                title={isFullscreen ? 'Exit Fullscreen' : 'Fullscreen'}
                className="p-1.5 text-slate-500 hover:text-slate-800 hover:bg-slate-100 rounded-lg border border-slate-200 transition-colors"
              >
                {isFullscreen ? <Minimize2 className="w-3.5 h-3.5" /> : <Maximize2 className="w-3.5 h-3.5" />}
              </button>
              {selectedEngine?.web_url && (
                <a
                  href={selectedEngine.web_url}
                  target="_blank"
                  rel="noreferrer"
                  className="flex items-center gap-1 px-2.5 py-1.5 text-xs text-slate-700 hover:text-indigo-600 border border-slate-200 rounded-lg hover:bg-slate-50 transition-colors"
                >
                  <ExternalLink className="w-3.5 h-3.5" />
                  <span>Open Tab</span>
                </a>
              )}
            </div>
          )}

          <button
            onClick={fetchEnginesAndJobs}
            disabled={loadingEngines || loadingJobs}
            className="p-1.5 text-slate-500 hover:text-slate-800 hover:bg-slate-100 rounded-lg border border-slate-200 transition-colors disabled:opacity-50"
            title="Refresh Status"
          >
            <RefreshCw
              className={`w-3.5 h-3.5 ${loadingEngines || loadingJobs ? 'animate-spin' : ''}`}
            />
          </button>
        </div>
      </div>

      {/* Global Feedback Alert */}
      {feedback && (
        <div
          className={`mx-6 mt-3 px-4 py-2.5 rounded-xl border flex items-center justify-between text-xs transition-all ${
            feedback.type === 'success'
              ? 'bg-emerald-50 border-emerald-200 text-emerald-800'
              : 'bg-rose-50 border-rose-200 text-rose-800'
          }`}
        >
          <div className="flex items-center gap-2">
            {feedback.type === 'success' ? (
              <CheckCircle2 className="w-4 h-4 text-emerald-600 shrink-0" />
            ) : (
              <AlertCircle className="w-4 h-4 text-rose-600 shrink-0" />
            )}
            <span className="font-medium">{feedback.message}</span>
          </div>
          <button
            onClick={() => setFeedback(null)}
            className="text-slate-400 hover:text-slate-600"
          >
            <X className="w-3.5 h-3.5" />
          </button>
        </div>
      )}

      {/* VIEW: OBELISK CRON */}
      {selectedEngineId === 'cron' && (
        <div className="flex-1 flex flex-col p-6 overflow-hidden">
          {/* Jobs Table & Filter Toolbar */}
          <div className="flex-1 bg-white rounded-xl border border-slate-200 shadow-xs flex flex-col overflow-hidden">
            <div className="px-5 py-3 border-b border-slate-200 flex flex-wrap items-center justify-between gap-3 shrink-0">
              {/* Type Filter Buttons & Active Count */}
              <div className="flex items-center gap-3">
                <div className="flex items-center gap-1.5">
                  {[
                    { id: 'all', label: 'All Jobs' },
                    { id: 'script', label: 'Python Scripts' },
                    { id: 'dbt', label: 'dbt Models' },
                    { id: 'notebook', label: 'Notebooks' },
                  ].map((tab) => (
                    <button
                      key={tab.id}
                      onClick={() => setFilterType(tab.id)}
                      className={`px-3 py-1 rounded-lg text-xs font-medium transition-colors ${
                        filterType === tab.id
                          ? 'bg-indigo-50 text-indigo-700 font-bold border border-indigo-200/80'
                          : 'text-slate-600 hover:text-slate-900 hover:bg-slate-100'
                      }`}
                    >
                      {tab.label}
                    </button>
                  ))}
                </div>
                <span className="text-[11px] font-mono text-slate-500 bg-slate-100 px-2.5 py-0.5 rounded-full font-medium border border-slate-200/60">
                  {cronJobs.length} {cronJobs.length === 1 ? 'job' : 'jobs'} ({cronJobs.filter((j) => j.is_enabled).length} active)
                </span>
              </div>

              {/* Search Box */}
              <div className="relative w-64">
                <Search className="w-3.5 h-3.5 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
                <input
                  type="text"
                  placeholder="Filter jobs by name, selector..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="w-full pl-8 pr-3 py-1.5 text-xs bg-slate-50 rounded-lg border border-slate-200 focus:outline-hidden focus:border-indigo-500 focus:bg-white"
                />
              </div>
            </div>

            {/* Table or Empty State */}
            <div className="flex-1 overflow-auto">
              {filteredJobs.length === 0 ? (
                <div className="h-full flex flex-col items-center justify-center p-8 text-center">
                  <div className="w-14 h-14 rounded-2xl bg-indigo-50 border border-indigo-100 flex items-center justify-center text-indigo-600 mb-3">
                    <Clock className="w-7 h-7" />
                  </div>
                  <h3 className="text-sm font-bold text-slate-800">No scheduled cron jobs found</h3>
                  <p className="text-xs text-slate-500 max-w-md mt-1 mb-4 leading-relaxed">
                    Create your first scheduled job to automate dbt data marts, standalone Python
                    scripts in <code className="px-1 py-0.5 bg-slate-100 rounded text-slate-700 font-mono">git_repo/scripts/</code>,
                    or reactive Marimo notebooks.
                  </p>
                  <button
                    onClick={() => {
                      if (cronTargets) {
                        setNewJobType('script');
                        setNewJobTarget(cronTargets.scripts[0]?.filename || 'daily_kpi_summary.py');
                      }
                      setIsCreateModalOpen(true);
                    }}
                    className="flex items-center gap-1.5 px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-semibold rounded-lg shadow-xs transition-colors"
                  >
                    <Plus className="w-4 h-4" />
                    <span>Create First Scheduled Job</span>
                  </button>
                </div>
              ) : (
                <table className="w-full text-left border-collapse">
                  <thead>
                    <tr className="border-b border-slate-200 bg-slate-50/70 text-[11px] font-bold text-slate-500 uppercase tracking-wider font-mono">
                      <th className="py-2.5 px-4">Status</th>
                      <th className="py-2.5 px-4">Job Name</th>
                      <th className="py-2.5 px-4">Type</th>
                      <th className="py-2.5 px-4">Target / Selector</th>
                      <th className="py-2.5 px-4">Schedule (Cron)</th>
                      <th className="py-2.5 px-4">Next Run</th>
                      <th className="py-2.5 px-4">Last Run</th>
                      <th className="py-2.5 px-4 text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100 text-xs">
                    {filteredJobs.map((job) => {
                      const isRunning = runningJobId === job.id;
                      return (
                        <tr
                          key={job.id}
                          className="hover:bg-slate-50/80 transition-colors group"
                        >
                          {/* Active / Enabled Toggle */}
                          <td className="py-3 px-4">
                            <button
                              onClick={() => handleToggleJob(job.id)}
                              title={job.is_enabled ? 'Pause schedule' : 'Enable schedule'}
                              className={`w-7 h-4 rounded-full transition-colors relative p-0.5 ${
                                job.is_enabled ? 'bg-indigo-600' : 'bg-slate-300'
                              }`}
                            >
                              <div
                                className={`w-3 h-3 rounded-full bg-white transition-transform ${
                                  job.is_enabled ? 'translate-x-3' : 'translate-x-0'
                                }`}
                              />
                            </button>
                          </td>

                          {/* Name & Description */}
                          <td className="py-3 px-4">
                            <div className="font-semibold text-slate-900 group-hover:text-indigo-600 transition-colors">
                              {job.name}
                            </div>
                            {job.description && (
                              <div className="text-[11px] text-slate-400 mt-0.5 max-w-xs truncate">
                                {job.description}
                              </div>
                            )}
                          </td>

                          {/* Type Badge */}
                          <td className="py-3 px-4">
                            {job.target_type === 'dbt' && (
                              <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-violet-50 text-violet-700 border border-violet-200">
                                <Database className="w-3 h-3" />
                                dbt
                              </span>
                            )}
                            {job.target_type === 'script' && (
                              <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-emerald-50 text-emerald-700 border border-emerald-200">
                                <Code2 className="w-3 h-3" />
                                script
                              </span>
                            )}
                            {job.target_type === 'notebook' && (
                              <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-amber-50 text-amber-700 border border-amber-200">
                                <BookOpen className="w-3 h-3" />
                                notebook
                              </span>
                            )}
                          </td>

                          {/* Target Selector */}
                          <td className="py-3 px-4">
                            <code className="px-1.5 py-0.5 bg-slate-100 text-slate-800 rounded font-mono text-[11px] font-semibold">
                              {job.target}
                            </code>
                          </td>

                          {/* Cron Expression */}
                          <td className="py-3 px-4 font-mono text-slate-700 text-[11px]">
                            <div className="font-semibold text-indigo-900">{job.cron_expression}</div>
                          </td>

                          {/* Next Run */}
                          <td className="py-3 px-4 text-slate-500 font-mono text-[11px]">
                            {job.is_enabled && job.next_run_at ? (
                              new Date(job.next_run_at).toLocaleString(undefined, {
                                month: 'short',
                                day: 'numeric',
                                hour: '2-digit',
                                minute: '2-digit',
                              })
                            ) : (
                              <span className="text-slate-400 italic">Paused</span>
                            )}
                          </td>

                          {/* Last Run Pill */}
                          <td className="py-3 px-4">
                            {job.last_status === 'success' && (
                              <span className="inline-flex items-center gap-1 text-emerald-700 font-semibold text-[11px]">
                                <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500" />
                                Success ({job.last_duration_seconds}s)
                              </span>
                            )}
                            {job.last_status === 'failed' && (
                              <span className="inline-flex items-center gap-1 text-rose-700 font-semibold text-[11px]">
                                <AlertCircle className="w-3.5 h-3.5 text-rose-500" />
                                Failed
                              </span>
                            )}
                            {!job.last_status && (
                              <span className="text-slate-400 text-[11px] italic">Never run</span>
                            )}
                          </td>

                          {/* Actions */}
                          <td className="py-3 px-4 text-right">
                            <div className="flex items-center justify-end gap-1.5">
                              <button
                                onClick={() => handleRunNow(job)}
                                disabled={isRunning}
                                title="Run immediately"
                                className="flex items-center gap-1 px-2.5 py-1 bg-indigo-50 hover:bg-indigo-100 text-indigo-700 rounded-lg text-xs font-semibold transition-colors disabled:opacity-50"
                              >
                                {isRunning ? (
                                  <Loader2 className="w-3 h-3 animate-spin" />
                                ) : (
                                  <Play className="w-3 h-3 fill-current" />
                                )}
                                <span>{isRunning ? 'Running...' : 'Run'}</span>
                              </button>

                              {job.last_logs && (
                                <button
                                  onClick={() => {
                                    setActiveLogJob(job);
                                    setIsLogDrawerOpen(true);
                                  }}
                                  title="View execution logs"
                                  className="p-1.5 text-slate-500 hover:text-slate-800 hover:bg-slate-100 rounded-lg border border-slate-200 transition-colors"
                                >
                                  <Terminal className="w-3.5 h-3.5" />
                                </button>
                              )}

                              <button
                                onClick={() => handleDeleteJob(job)}
                                title="Delete job"
                                className="p-1.5 text-slate-400 hover:text-rose-600 hover:bg-rose-50 rounded-lg transition-colors"
                              >
                                <Trash2 className="w-3.5 h-3.5" />
                              </button>
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </div>
      )}

      {/* VIEW: DAGSTER OR AIRFLOW (EMBEDDED OR OFFLINE SERVICE CARD) */}
      {selectedEngineId !== 'cron' && (
        <div className="flex-1 flex flex-col overflow-hidden bg-slate-50">
          {selectedEngine?.status === 'running' && selectedEngineId === 'dagster' ? (
            <DagsterNativePanel webUrl={selectedEngine?.web_url} />
          ) : selectedEngine?.status === 'running' ? (
            <div className="flex-1 flex flex-col relative bg-white">
              <iframe
                key={iframeKey}
                src={selectedEngine.web_url || '/dagster'}
                title={selectedEngine.name}
                className="w-full flex-1 border-0"
              />
            </div>
          ) : (
            <div className="flex-1 flex items-center justify-center p-8">
              <div className="max-w-xl w-full bg-white rounded-2xl border border-slate-200 p-8 shadow-sm text-center">
                <div className="w-16 h-16 rounded-2xl bg-indigo-50 border border-indigo-100 flex items-center justify-center text-indigo-600 mx-auto mb-4">
                  {selectedEngineId === 'dagster' ? (
                    <RefreshCw className="w-8 h-8 text-violet-600" />
                  ) : (
                    <Sparkles className="w-8 h-8 text-sky-600" />
                  )}
                </div>

                <div className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-slate-100 text-slate-600 border border-slate-200 mb-3">
                  <span className="w-2 h-2 rounded-full bg-slate-400" />
                  Service Offline
                </div>

                <h2 className="text-xl font-bold text-slate-900 mb-2">
                  {selectedEngine?.name || 'Orchestration Engine'}
                </h2>
                <p className="text-xs text-slate-600 leading-relaxed mb-6">
                  {selectedEngine?.description}
                </p>

                <div className="flex flex-col sm:flex-row items-center justify-center gap-3">
                  {selectedEngine?.can_deploy_local && (
                    <button
                      onClick={() => handleDeployLocal(selectedEngine.id)}
                      className="w-full sm:w-auto px-5 py-2.5 bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-bold rounded-xl shadow-xs transition-colors flex items-center justify-center gap-2"
                    >
                      <Power className="w-4 h-4" />
                      <span>1-Click Deploy {selectedEngine.name} Locally</span>
                    </button>
                  )}

                  {onNavigateToSettings ? (
                    <button
                      onClick={onNavigateToSettings}
                      className="w-full sm:w-auto px-5 py-2.5 bg-white hover:bg-slate-50 text-slate-700 border border-slate-200 text-xs font-bold rounded-xl transition-colors"
                    >
                      Connect Remote Instance
                    </button>
                  ) : null}

                  <button
                    onClick={() => setSelectedEngineId('cron')}
                    className="w-full sm:w-auto px-4 py-2.5 text-xs text-indigo-600 hover:text-indigo-800 font-semibold"
                  >
                    Use Obelisk Cron Instead →
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* CREATE NEW SCHEDULED JOB MODAL */}
      {isCreateModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 backdrop-blur-xs p-4">
          <div className="bg-white rounded-2xl border border-slate-200 w-full max-w-lg shadow-xl overflow-hidden animate-in fade-in zoom-in-95 duration-150">
            <div className="px-6 py-4 border-b border-slate-200 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Clock className="w-5 h-5 text-indigo-600" />
                <h3 className="text-base font-bold text-slate-900">New Scheduled Cron Job</h3>
              </div>
              <button
                onClick={() => setIsCreateModalOpen(false)}
                className="text-slate-400 hover:text-slate-600"
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            <form onSubmit={handleCreateJob} className="p-6 space-y-4">
              {/* Job Name */}
              <div>
                <label className="block text-xs font-bold text-slate-700 uppercase font-mono mb-1">
                  Job Name *
                </label>
                <input
                  type="text"
                  required
                  placeholder="e.g. Daily KPI Aggregation"
                  value={newJobName}
                  onChange={(e) => setNewJobName(e.target.value)}
                  className="w-full px-3 py-2 text-xs rounded-lg border border-slate-300 focus:outline-hidden focus:border-indigo-500"
                />
              </div>

              {/* Target Type Picker */}
              <div>
                <label className="block text-xs font-bold text-slate-700 uppercase font-mono mb-1.5">
                  Target Execution Type *
                </label>
                <div className="grid grid-cols-3 gap-2">
                  {[
                    {
                      id: 'script' as const,
                      title: 'Python Script',
                      desc: 'git_repo/scripts/',
                      icon: <Code2 className="w-4 h-4" />,
                    },
                    {
                      id: 'dbt' as const,
                      title: 'dbt Model',
                      desc: 'Silver & Gold Marts',
                      icon: <Database className="w-4 h-4" />,
                    },
                    {
                      id: 'notebook' as const,
                      title: 'Notebook',
                      desc: 'git_repo/notebooks/',
                      icon: <BookOpen className="w-4 h-4" />,
                    },
                  ].map((type) => (
                    <div
                      key={type.id}
                      onClick={() => handleTypeChange(type.id)}
                      className={`p-3 rounded-xl border cursor-pointer text-left transition-all ${
                        newJobType === type.id
                          ? 'border-indigo-600 bg-indigo-50/80 ring-2 ring-indigo-200'
                          : 'border-slate-200 hover:border-indigo-300 hover:bg-slate-50'
                      }`}
                    >
                      <div className="text-indigo-600 mb-1">{type.icon}</div>
                      <div className="text-xs font-bold text-slate-800">{type.title}</div>
                      <div className="text-[10px] text-slate-500 mt-0.5">{type.desc}</div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Target Selection Dropdown */}
              <div>
                <label className="block text-xs font-bold text-slate-700 uppercase font-mono mb-1">
                  Target / Selector *
                </label>
                {newJobType === 'script' && (
                  <div>
                    <select
                      value={newJobTarget}
                      onChange={(e) => setNewJobTarget(e.target.value)}
                      className="w-full px-3 py-2 text-xs rounded-lg border border-slate-300 focus:outline-hidden focus:border-indigo-500 font-mono"
                    >
                      {cronTargets?.scripts?.map((s) => (
                        <option key={s.filename} value={s.filename}>
                          {s.filename} ({Math.round(s.size_bytes / 1024)} KB)
                        </option>
                      ))}
                    </select>
                    <p className="text-[11px] text-slate-400 mt-1">
                      Target script from <code className="text-indigo-600">git_repo/scripts/</code>
                    </p>
                  </div>
                )}

                {newJobType === 'dbt' && (
                  <div>
                    <select
                      value={newJobTarget}
                      onChange={(e) => setNewJobTarget(e.target.value)}
                      className="w-full px-3 py-2 text-xs rounded-lg border border-slate-300 focus:outline-hidden focus:border-indigo-500 font-mono"
                    >
                      {cronTargets?.dbt_selectors?.map((sel) => (
                        <option key={sel.value} value={sel.value}>
                          {sel.label} - {sel.description}
                        </option>
                      ))}
                    </select>
                    <p className="text-[11px] text-slate-400 mt-1">
                      Compiles and executes dbt models in PostgreSQL
                    </p>
                  </div>
                )}

                {newJobType === 'notebook' && (
                  <div>
                    <select
                      value={newJobTarget}
                      onChange={(e) => setNewJobTarget(e.target.value)}
                      className="w-full px-3 py-2 text-xs rounded-lg border border-slate-300 focus:outline-hidden focus:border-indigo-500 font-mono"
                    >
                      {cronTargets?.notebooks?.map((nb) => (
                        <option key={nb.filename} value={nb.filename}>
                          {nb.filename}
                        </option>
                      ))}
                    </select>
                    <p className="text-[11px] text-slate-400 mt-1">
                      Runs Marimo notebook headlessly in <code className="text-indigo-600">git_repo/notebooks/</code>
                    </p>
                  </div>
                )}
              </div>

              {/* Cron Expression & Presets */}
              <div>
                <div className="flex items-center justify-between mb-1">
                  <label className="text-xs font-bold text-slate-700 uppercase font-mono">
                    Schedule (Standard Cron) *
                  </label>
                  <span className="text-[10px] text-slate-400 font-mono">UTC Timezone</span>
                </div>

                <div className="flex flex-wrap gap-1.5 mb-2">
                  {cronPresets.map((preset) => (
                    <button
                      key={preset.expr}
                      type="button"
                      onClick={() => setNewJobCron(preset.expr)}
                      className={`px-2 py-0.5 rounded text-[11px] font-mono transition-colors ${
                        newJobCron === preset.expr
                          ? 'bg-indigo-600 text-white font-bold'
                          : 'bg-slate-100 text-slate-700 hover:bg-slate-200'
                      }`}
                    >
                      {preset.label}
                    </button>
                  ))}
                </div>

                <input
                  type="text"
                  required
                  placeholder="e.g. 0 4 * * *"
                  value={newJobCron}
                  onChange={(e) => setNewJobCron(e.target.value)}
                  className="w-full px-3 py-2 text-xs font-mono rounded-lg border border-slate-300 focus:outline-hidden focus:border-indigo-500"
                />
              </div>

              {/* Description */}
              <div>
                <label className="block text-xs font-bold text-slate-700 uppercase font-mono mb-1">
                  Description (Optional)
                </label>
                <input
                  type="text"
                  placeholder="e.g. Calculates daily KPIs and updates gold marts"
                  value={newJobDesc}
                  onChange={(e) => setNewJobDesc(e.target.value)}
                  className="w-full px-3 py-2 text-xs rounded-lg border border-slate-300 focus:outline-hidden focus:border-indigo-500"
                />
              </div>

              {/* Modal Actions */}
              <div className="flex items-center justify-end gap-2 pt-3 border-t border-slate-200">
                <button
                  type="button"
                  onClick={() => setIsCreateModalOpen(false)}
                  className="px-4 py-2 rounded-lg text-xs font-semibold text-slate-600 hover:bg-slate-100 transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={creatingJob || !newJobName.trim()}
                  className="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-xs font-semibold shadow-xs transition-colors disabled:opacity-50 flex items-center gap-1.5"
                >
                  {creatingJob ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : null}
                  <span>{creatingJob ? 'Scheduling...' : 'Create Scheduled Job'}</span>
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* EXECUTION LOG DRAWER */}
      {isLogDrawerOpen && activeLogJob && (
        <div className="fixed inset-y-0 right-0 z-50 w-full max-w-2xl bg-slate-900 text-slate-100 shadow-2xl flex flex-col animate-in slide-in-from-right duration-200 border-l border-slate-800">
          <div className="px-6 py-4 border-b border-slate-800 flex items-center justify-between shrink-0 bg-slate-950">
            <div className="flex items-center gap-2.5">
              <Terminal className="w-5 h-5 text-emerald-400" />
              <div>
                <h3 className="text-sm font-bold text-white font-mono">{activeLogJob.name}</h3>
                <p className="text-[11px] text-slate-400 font-mono">
                  {activeLogJob.target_type}: {activeLogJob.target}
                </p>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <span
                className={`px-2 py-0.5 rounded text-[10px] font-mono font-bold uppercase ${
                  activeLogJob.last_status === 'success'
                    ? 'bg-emerald-950 text-emerald-400 border border-emerald-800'
                    : 'bg-rose-950 text-rose-400 border border-rose-800'
                }`}
              >
                {activeLogJob.last_status || 'UNKNOWN'} (
                {activeLogJob.last_duration_seconds || 0}s)
              </span>
              <button
                onClick={() => setIsLogDrawerOpen(false)}
                className="text-slate-400 hover:text-white p-1"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
          </div>

          <div className="flex-1 p-5 overflow-auto font-mono text-xs leading-relaxed bg-slate-900/90 select-text">
            <pre className="whitespace-pre-wrap text-slate-300 font-mono">
              {activeLogJob.last_logs || 'No execution logs recorded yet.'}
            </pre>
          </div>

          <div className="p-3 border-t border-slate-800 bg-slate-950 flex items-center justify-between text-[11px] text-slate-400 font-mono">
            <span>Obelisk Cron In-Process Subprocess Runner</span>
            <button
              onClick={() => {
                if (activeLogJob.last_logs) {
                  navigator.clipboard.writeText(activeLogJob.last_logs);
                  setFeedback({ type: 'success', message: 'Logs copied to clipboard.' });
                }
              }}
              className="text-indigo-400 hover:text-indigo-300 font-semibold"
            >
              Copy Logs
            </button>
          </div>
        </div>
      )}
    </div>
  );
};
