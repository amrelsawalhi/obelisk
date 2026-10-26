import React, { useState, useRef, useEffect } from 'react';
import {
  RefreshCw,
  CheckCircle2,
  AlertCircle,
  XCircle,
  Maximize2,
  Minus,
  X,
  Layers,
  StopCircle,
  Trash2,
  Database
} from 'lucide-react';
import { useIngestTracker } from '../../context/IngestContext';

export const IngestActivityDock: React.FC = () => {
  const {
    jobs,
    activeJobId,
    activeJob,
    isMinimized,
    isDrawerOpen,
    activeLogs,
    runningCount,
    selectJob,
    minimizeDrawer,
    restoreDrawer,
    dismissJob,
    clearCompleted,
    cancelJob,
    cancelAll,
  } = useIngestTracker();

  const [showLogs, setShowLogs] = useState(true);
  const terminalRef = useRef<HTMLDivElement>(null);

  // Auto-scroll terminal as new logs arrive for the active job
  useEffect(() => {
    if (terminalRef.current && showLogs) {
      terminalRef.current.scrollTop = terminalRef.current.scrollHeight;
    }
  }, [activeLogs, showLogs, activeJobId]);

  const jobList = Object.values(jobs);
  if (!isDrawerOpen || jobList.length === 0) return null;

  const currentJob = activeJob || jobList[0];
  const isRunning = currentJob?.status === 'running';
  const isSuccess = currentJob?.status === 'completed';
  const isCancelled = currentJob?.status === 'cancelled';

  // Percent is completed/total tables, so a single-table job can only be 0 or 100: the bar
  // sat empty while 500k rows streamed in. Show it as indeterminate and let rows carry it.
  const indeterminate = isRunning && (currentJob?.total_tables ?? 1) <= 1;

  // Calculate aggregate stats across all jobs
  const totalJobs = jobList.length;
  const completedCount = jobList.filter((j) => j.status === 'completed').length;
  const avgPercent = Math.round(
    jobList.reduce((acc, j) => acc + (j.percent || 0), 0) / Math.max(totalJobs, 1)
  );
  const totalExtractedRows = jobList.reduce(
    (acc, j) => acc + (j.total_rows || 0) + (j.staged_rows || 0),
    0
  );

  // 1. Minimized Floating Pill View (Option A Collapsed)
  if (isMinimized) {
    return (
      <div className="fixed bottom-5 right-5 z-50 flex items-center gap-3 bg-slate-900/95 text-white px-4 py-2.5 rounded-full shadow-2xl border border-slate-700/80 backdrop-blur-md animate-in slide-in-from-bottom-3 duration-200">
        <div
          onClick={restoreDrawer}
          className="flex items-center gap-2.5 cursor-pointer hover:text-sky-300 transition-colors"
        >
          {runningCount > 0 ? (
            <RefreshCw className="w-4 h-4 text-sky-400 animate-spin" />
          ) : completedCount === totalJobs ? (
            <CheckCircle2 className="w-4 h-4 text-emerald-400" />
          ) : (
            <AlertCircle className="w-4 h-4 text-amber-400" />
          )}

          <div className="flex items-center gap-2 text-xs font-semibold">
            {totalJobs === 1 ? (
              <>
                <span className="truncate max-w-[200px] font-mono">{currentJob.title}</span>
                <span className="bg-sky-900/80 text-sky-300 px-1.5 py-0.5 rounded text-[10px] font-bold font-mono">
                  {currentJob.percent}%
                </span>
              </>
            ) : (
              <>
                <span className="font-sans font-bold">
                  {runningCount > 0 ? `${runningCount} Ingesting` : 'Ingestions'}
                </span>
                <span className="bg-sky-900/80 text-sky-300 px-1.5 py-0.5 rounded text-[10px] font-bold font-mono">
                  {avgPercent}% avg
                </span>
                <span className="text-[10px] text-slate-400 font-mono hidden sm:inline">
                  ({totalJobs} jobs)
                </span>
              </>
            )}

            {totalExtractedRows > 0 && (
              <span className="text-[10px] text-slate-400 font-mono hidden md:inline">
                • {totalExtractedRows.toLocaleString()} rows
              </span>
            )}
          </div>
        </div>

        <div className="flex items-center gap-1 border-l border-slate-700/80 pl-2">
          <button
            onClick={restoreDrawer}
            title="Expand Activity Drawer"
            className="p-1 text-slate-400 hover:text-white rounded hover:bg-slate-800 transition-colors"
          >
            <Maximize2 className="w-3.5 h-3.5" />
          </button>

          {runningCount === 0 && (
            <button
              onClick={clearCompleted}
              title="Clear Completed"
              className="p-1 text-slate-400 hover:text-white rounded hover:bg-slate-800 transition-colors"
            >
              <X className="w-3.5 h-3.5" />
            </button>
          )}
        </div>
      </div>
    );
  }

  // 2. Expanded Multi-Job Activity Drawer View (Option A Expanded)
  return (
    <div className="fixed bottom-5 right-5 z-50 w-full max-w-xl bg-white rounded-2xl shadow-2xl border border-slate-200 overflow-hidden flex flex-col max-h-[85vh] animate-in slide-in-from-bottom-4 duration-200">
      {/* Main Drawer Header */}
      <div className="px-4 py-3 bg-slate-900 text-white flex items-center justify-between shrink-0">
        <div className="flex items-center gap-2.5">
          <div className="p-1.5 bg-slate-800 text-sky-400 rounded-lg">
            <Layers className="w-4 h-4" />
          </div>
          <div>
            <h4 className="text-xs font-bold text-white flex items-center gap-2">
              <span>Lakehouse Ingestion Activity</span>
              <span
                className={`text-[9px] font-mono px-1.5 py-0.5 rounded font-bold uppercase ${
                  runningCount > 0
                    ? 'bg-sky-900/80 text-sky-300'
                    : 'bg-emerald-900/80 text-emerald-300'
                }`}
              >
                {runningCount > 0 ? `${runningCount} Running` : 'All Done'}
              </span>
            </h4>
            <p className="text-[10px] text-slate-400 font-mono">
              {totalJobs} total task{totalJobs > 1 ? 's' : ''} • {totalExtractedRows.toLocaleString()} rows lakehouse bronze
            </p>
          </div>
        </div>

        <div className="flex items-center gap-1.5">
          {totalJobs > runningCount && (
            <button
              onClick={clearCompleted}
              title="Clear Completed Ingestions"
              className="text-[11px] text-slate-400 hover:text-white px-2 py-1 rounded hover:bg-slate-800 transition-colors flex items-center gap-1"
            >
              <Trash2 className="w-3 h-3" />
              <span>Clear Done</span>
            </button>
          )}

          <button
            onClick={minimizeDrawer}
            title="Minimize to Bottom Pill"
            className="p-1.5 text-slate-400 hover:text-white rounded-md hover:bg-slate-800 transition-colors"
          >
            <Minus className="w-4 h-4" />
          </button>

          {runningCount === 0 && (
            <button
              onClick={clearCompleted}
              title="Close Drawer"
              className="p-1.5 text-slate-400 hover:text-white rounded-md hover:bg-slate-800 transition-colors"
            >
              <X className="w-4 h-4" />
            </button>
          )}
        </div>
      </div>

      {/* Multi-Job Tabs Bar (If > 1 job or for clear tab identity) */}
      <div className="bg-slate-950 px-2 pt-1.5 flex items-center gap-1.5 overflow-x-auto border-b border-slate-800 shrink-0 custom-scrollbar">
        {jobList.map((job) => {
          const isSelected = job.id === currentJob.id;
          const isJobRunning = job.status === 'running';
          const isJobDone = job.status === 'completed';
          const isJobFailed = job.status === 'failed';

          return (
            <div
              key={job.id}
              onClick={() => selectJob(job.id)}
              className={`group flex items-center gap-2 px-3 py-1.5 rounded-t-lg cursor-pointer transition-all border-b-2 text-xs select-none shrink-0 ${
                isSelected
                  ? 'bg-slate-800/90 text-white border-sky-500 font-semibold shadow-sm'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-900 border-transparent font-normal'
              }`}
            >
              {isJobRunning ? (
                <RefreshCw className="w-3 h-3 text-sky-400 animate-spin shrink-0" />
              ) : isJobDone ? (
                <CheckCircle2 className="w-3 h-3 text-emerald-400 shrink-0" />
              ) : isJobFailed ? (
                <XCircle className="w-3 h-3 text-rose-400 shrink-0" />
              ) : (
                <AlertCircle className="w-3 h-3 text-amber-400 shrink-0" />
              )}

              <span className="truncate max-w-[130px] font-mono text-[11px]">
                {job.title}
              </span>

              <span className="text-[10px] font-mono bg-slate-900 px-1 py-0.2 rounded text-slate-300">
                {job.percent}%
              </span>

              <button
                onClick={(e) => {
                  e.stopPropagation();
                  dismissJob(job.id);
                }}
                title="Dismiss task"
                className="opacity-0 group-hover:opacity-100 hover:text-rose-400 p-0.5 rounded transition-opacity"
              >
                <X className="w-2.5 h-2.5" />
              </button>
            </div>
          );
        })}
      </div>

      {/* Active Job Progress & Metrics Card */}
      <div className="p-4 space-y-3 bg-slate-50/70 border-b border-slate-200 shrink-0">
        <div>
          <div className="flex items-center justify-between text-xs font-semibold text-slate-700 mb-1">
            <span className="flex items-center gap-1.5 truncate max-w-[380px]">
              {isRunning && <RefreshCw className="w-3 h-3 text-sky-600 animate-spin shrink-0" />}
              <span className="truncate">{currentJob.current_phase || (isRunning ? 'Processing...' : 'Completed')}</span>
            </span>
            <span className="font-mono text-sky-600 font-bold shrink-0">
              {indeterminate ? '—' : `${currentJob.percent}%`}
            </span>
          </div>

          <div className="w-full bg-slate-200 h-2 rounded-full overflow-hidden">
            {indeterminate ? (
              <div className="h-full w-full bg-gradient-to-r from-sky-400 via-sky-600 to-sky-400 animate-pulse" />
            ) : (
              <div
                className={`h-full transition-all duration-300 ${
                  isRunning
                    ? 'bg-gradient-to-r from-sky-500 to-sky-600'
                    : isSuccess
                    ? 'bg-emerald-500'
                    : isCancelled
                    ? 'bg-amber-500'
                    : 'bg-rose-500'
                }`}
                style={{ width: `${currentJob.percent}%` }}
              />
            )}
          </div>
        </div>

        {/* 3 Metric Cards */}
        <div className="grid grid-cols-3 gap-2">
          <div className="p-2.5 bg-white rounded-lg border border-slate-200">
            <div className="text-[10px] text-slate-500 uppercase font-bold tracking-wider">
              {currentJob.job_type === 'batch' ? 'Tables' : 'Table'}
            </div>
            <div className="text-sm font-bold text-slate-800 font-mono mt-0.5">
              {currentJob.completed_tables} / {currentJob.total_tables}
            </div>
          </div>

          <div className="p-2.5 bg-white rounded-lg border border-slate-200">
            <div className="text-[10px] text-slate-500 uppercase font-bold tracking-wider">
              Rows Extracted
            </div>
            <div className="text-sm font-bold text-sky-600 font-mono mt-0.5">
              {(
                (currentJob.total_rows || 0) + (currentJob.staged_rows || 0)
              ).toLocaleString()}
            </div>
          </div>

          <div className="p-2.5 bg-white rounded-lg border border-slate-200">
            <div className="text-[10px] text-slate-500 uppercase font-bold tracking-wider">
              Status
            </div>
            <div
              className={`text-sm font-bold capitalize mt-0.5 ${
                isRunning
                  ? 'text-sky-600'
                  : isSuccess
                  ? 'text-emerald-600'
                  : isCancelled
                  ? 'text-amber-600'
                  : 'text-rose-600'
              }`}
            >
              {currentJob.status}
            </div>
          </div>
        </div>
      </div>

      {/* Live Terminal Log Stream */}
      <div className="p-3 bg-white flex flex-col flex-1 min-h-[160px] max-h-[260px]">
        <div className="flex items-center justify-between text-xs font-semibold text-slate-600 mb-1.5 shrink-0">
          <span className="flex items-center gap-1.5 font-mono text-[11px]">
            <Database className="w-3.5 h-3.5 text-sky-600" />
            <span>Execution Terminal ({activeLogs.length} lines)</span>
          </span>
          <button
            onClick={() => setShowLogs(!showLogs)}
            className="text-[10px] text-sky-600 hover:underline"
          >
            {showLogs ? 'Hide' : 'Show'}
          </button>
        </div>

        {showLogs && (
          <div
            ref={terminalRef}
            className="bg-slate-950 text-slate-200 font-mono text-[11px] p-3 rounded-xl overflow-y-auto flex-1 space-y-1 shadow-inner border border-slate-800 custom-scrollbar select-text"
          >
            {activeLogs.length === 0 ? (
              <div className="text-slate-500 italic py-2">
                Listening for stream events from backend worker...
              </div>
            ) : (
              activeLogs.map((log, index) => {
                const isError = log.includes('✗') || log.includes('failed') || log.includes('ERROR');
                const isCheck = log.includes('✓') || log.includes('Success');
                const isStart = log.includes('Starting') || log.includes('Processing');

                return (
                  <div
                    key={index}
                    className={`leading-relaxed break-all ${
                      isError
                        ? 'text-rose-400 font-semibold'
                        : isCheck
                        ? 'text-emerald-400 font-semibold'
                        : isStart
                        ? 'text-sky-300'
                        : 'text-slate-300'
                    }`}
                  >
                    {log}
                  </div>
                );
              })
            )}
          </div>
        )}
      </div>

      {/* Footer Controls */}
      <div className="px-4 py-2.5 bg-slate-50 border-t border-slate-200 flex items-center justify-between shrink-0">
        <div className="text-[10px] text-slate-500 font-mono">
          {isRunning ? (
            <span className="flex items-center gap-1 text-sky-600 font-semibold">
              <span className="w-1.5 h-1.5 rounded-full bg-sky-600 animate-pulse" />
              Worker active in background
            </span>
          ) : (
            <span>Finished at {currentJob.completed_at ? new Date(currentJob.completed_at).toLocaleTimeString() : 'N/A'}</span>
          )}
        </div>

        <div className="flex items-center gap-2">
          {runningCount > 1 && (
            <button
              onClick={cancelAll}
              className="px-2.5 py-1 text-xs font-semibold text-rose-600 hover:bg-rose-50 rounded-lg border border-rose-200 transition-colors"
            >
              Stop All ({runningCount})
            </button>
          )}

          {isRunning ? (
            <button
              onClick={() => cancelJob(currentJob.id)}
              className="px-3 py-1.5 text-xs font-semibold bg-rose-50 text-rose-600 hover:bg-rose-100 rounded-lg border border-rose-200 transition-colors flex items-center gap-1.5"
            >
              <StopCircle className="w-3.5 h-3.5" />
              <span>Stop This Ingestion</span>
            </button>
          ) : (
            <button
              onClick={() => dismissJob(currentJob.id)}
              className="px-3 py-1.5 text-xs font-semibold bg-slate-900 text-white hover:bg-slate-800 rounded-lg transition-colors"
            >
              Done
            </button>
          )}
        </div>
      </div>
    </div>
  );
};
