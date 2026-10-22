import React, { useState, useEffect, useMemo } from 'react';
import {
  RefreshCw,
  ExternalLink,
  CheckCircle2,
  AlertCircle,
  Clock,
  ListChecks,
  History,
  Sparkles,
  X,
  Activity,
} from 'lucide-react';
import type { DagsterSchedule, DagsterRun } from '../../lib/types';
import { api } from '../../lib/api';

interface DagsterNativePanelProps {
  webUrl?: string | null;
}

type LoadState<T> =
  | { status: 'idle' | 'loading' }
  | { status: 'ready'; data: T };

const STATUS_PILL: Record<string, string> = {
  SUCCESS: 'bg-emerald-100 text-emerald-700 border-emerald-200',
  FAILURE: 'bg-red-100 text-red-700 border-red-200',
  CANCELED: 'bg-amber-100 text-amber-700 border-amber-200',
  CANCELLED: 'bg-amber-100 text-amber-700 border-amber-200',
  STARTED: 'bg-blue-100 text-blue-700 border-blue-200',
  QUEUED: 'bg-slate-100 text-slate-700 border-slate-200',
  NOT_STARTED: 'bg-slate-100 text-slate-600 border-slate-200',
  UNKNOWN: 'bg-slate-100 text-slate-600 border-slate-200',
};

const SCHEDULE_PILL: Record<string, string> = {
  RUNNING: 'bg-emerald-100 text-emerald-700 border-emerald-200',
  STOPPED: 'bg-slate-100 text-slate-600 border-slate-200',
  PAUSED: 'bg-amber-100 text-amber-700 border-amber-200',
};

function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null) return '\u2014';
  if (seconds < 1) return `${Math.round(seconds * 1000)} ms`;
  if (seconds < 60) return `${seconds.toFixed(2)} s`;
  const mins = Math.floor(seconds / 60);
  const secs = Math.round(seconds % 60);
  return `${mins}m ${secs}s`;
}

function formatRelative(iso: string | null | undefined): string {
  if (iso == null || iso === '') return 'no recent activity';
  const t = Date.parse(iso);
  if (isNaN(t)) return 'no recent activity';
  const delta = Math.round((Date.now() - t) / 1000);
  if (delta < 0) return 'just now';          // future-dated record (clock skew)
  if (delta < 5) return 'just now';
  if (delta < 60) return `${delta}s ago`;
  if (delta < 3600) return `${Math.floor(delta / 60)}m ago`;
  if (delta < 86400) return `${Math.floor(delta / 3600)}h ago`;
  // Anything older than 30 days is treated as "no recent activity" so that an
  // obviously stale/garbage run record from the Dagster DB doesn't dominate the
  // panel (e.g. a run reported as started 77 years ago).
  if (delta < 30 * 86400) return `${Math.floor(delta / 86400)}d ago`;
  return 'no recent activity';
}

function formatClockTime(epoch: number): string {
  try {
    const d = new Date(epoch);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  } catch {
    return '';
  }
}

function Pill({ label, kind }: { label: string; kind: 'STATUS' | 'SCHEDULE' | string }) {
  const map = kind === 'SCHEDULE' ? SCHEDULE_PILL : STATUS_PILL;
  const cls = map[label?.toUpperCase()] || 'bg-slate-100 text-slate-600 border-slate-200';
  return (
    <span className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md text-[10px] font-bold border ${cls}`}>
      {label}
    </span>
  );
}

const EmptyState: React.FC<{ icon: React.ReactNode; title?: string; body: string }> = ({ icon, title, body }) => (
  <div className="flex flex-col items-center justify-center py-8 text-center">
    <div className="w-10 h-10 rounded-xl bg-slate-100 border border-slate-200 flex items-center justify-center mb-2">
      {icon}
    </div>
    {title && <div className="text-xs font-semibold text-slate-700 mb-0.5">{title}</div>}
    <div className="text-[11px] text-slate-500 max-w-md">{body}</div>
  </div>
);

// ---------------------------------------------------------------------------
// Subcomponents
// ---------------------------------------------------------------------------

const EngineStrip: React.FC<{
  webUrl?: string | null;
  schedules: LoadState<DagsterSchedule[]>;
  runs: LoadState<DagsterRun[]>;
  onRefresh: () => void;
  refreshing: boolean;
  lastFetchedAt: number | null;
}> = ({ webUrl, schedules, runs, onRefresh, refreshing, lastFetchedAt }) => {
  const nsch = schedules.status === 'ready' ? schedules.data.length : 0;
  const nruns = runs.status === 'ready' ? runs.data.length : 0;
  const refreshedLabel = lastFetchedAt
    ? `updated ${formatClockTime(lastFetchedAt)}`
    : 'never fetched';
  return (
    <div className="flex items-center justify-between gap-3 px-4 py-2 border-b border-slate-200 bg-gradient-to-r from-violet-50/40 via-white to-indigo-50/40">
      <div className="flex items-center gap-2 min-w-0">
        <div className="inline-flex items-center gap-1.5 px-1.5 py-0.5 rounded-md bg-violet-100 text-violet-800 border border-violet-200 shrink-0">
          <Sparkles className="w-3 h-3" />
          <span className="font-bold uppercase tracking-wider text-[10px]">Dagster</span>
        </div>
        <span className="text-[10px] text-slate-500 font-mono truncate">
          {nsch} schedule{nsch === 1 ? '' : 's'} · {nruns} recent run{nruns === 1 ? '' : 's'}
        </span>
        <span className="text-[9px] text-slate-400 font-mono shrink-0" title={refreshedLabel}>
          · {refreshedLabel}{refreshing ? ' · refreshing…' : ''}
        </span>
      </div>
      <div className="flex items-center gap-1 shrink-0">
        <button onClick={onRefresh} title="Refresh"
          className="p-1 text-slate-500 hover:text-slate-800 hover:bg-slate-100 rounded transition-colors">
          <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin' : ''}`} />
        </button>
        <a href={webUrl || 'http://localhost:3000'} target="_blank" rel="noreferrer"
          title="Open in Dagster"
          className="p-1 text-indigo-600 hover:text-indigo-800 hover:bg-indigo-50 rounded transition-colors">
          <ExternalLink className="w-3.5 h-3.5" />
        </a>
      </div>
    </div>
  );
};

const SectionCard: React.FC<{
  title: string;
  hint?: string;
  icon: React.ReactNode;
  children: React.ReactNode;
}> = ({ title, hint, icon, children }) => (
  <section className="px-4 py-3 border-b border-slate-200 last:border-b-0">
    <div className="mb-1.5">
      <div className="flex items-center gap-2">
        <div className="text-slate-500 shrink-0">{icon}</div>
        <h3 className="text-[11px] font-bold uppercase tracking-wider text-slate-700">{title}</h3>
      </div>
      {hint && <p className="text-[10px] text-slate-400 italic leading-snug mt-0.5 pl-5">{hint}</p>}
    </div>
    {children}
  </section>
);

const SchedulesSection: React.FC<{
  schedules: LoadState<DagsterSchedule[]>;
  webUrl?: string | null;
}> = ({ schedules, webUrl }) => {
  if (schedules.status === 'idle' || schedules.status === 'loading') {
    return <EmptyState icon={<RefreshCw className="w-4 h-4 text-slate-400 animate-spin" />} body="Reading Dagster schedules\u2026" />;
  }
  const list = schedules.status === 'ready' ? schedules.data : [];
  if (list.length === 0) {
    return (
      <EmptyState icon={<ListChecks className="w-4 h-4 text-slate-400" />}
        title="No schedules"
        body="Schedules registered with Dagster will appear here. Cron jobs created from Obelisk Cron are tracked in the Obelisk Cron tab." />
    );
  }
  return (
    <div className="rounded-lg border border-slate-200 overflow-hidden">
      <div className="grid grid-cols-[1fr_160px_120px_28px] bg-slate-50 px-3 py-1.5 text-[10px] font-bold text-slate-500 uppercase tracking-wider">
        <div>Schedule</div><div>Cron</div><div>State</div><div></div>
      </div>
      {list.map((s) => (
        <div key={s.id || s.name} className="grid grid-cols-[1fr_160px_120px_28px] items-center px-3 py-1.5 text-xs border-t border-slate-100 hover:bg-slate-50/60">
          <div className="min-w-0">
            <div className="font-mono text-slate-800 truncate">{s.name}</div>
            {s.description && <div className="text-[10px] text-slate-500 truncate mt-0.5">{s.description}</div>}
          </div>
          <div>
            <div className="font-mono text-[11px] text-slate-700">{s.cron || '\u2014'}</div>
            <div className="text-[9px] text-slate-400 font-mono">{s.timezone || 'UTC'}</div>
          </div>
          <div><Pill label={s.state} kind="SCHEDULE" /></div>
          <div className="text-right">
            <a href={`${(webUrl || 'http://localhost:3000').replace(/\/$/, '')}/automation/schedules`}
              target="_blank" rel="noreferrer"
              className="inline-flex p-1 rounded text-indigo-600 hover:bg-indigo-50 hover:text-indigo-800 transition-colors"
              title="Open schedules in Dagster">
              <ExternalLink className="w-3 h-3" />
            </a>
          </div>
        </div>
      ))}
    </div>
  );
};

const RecentActivitySection: React.FC<{
  runs: LoadState<DagsterRun[]>;
}> = ({ runs }) => {
  if (runs.status === 'idle' || runs.status === 'loading') {
    return <EmptyState icon={<RefreshCw className="w-4 h-4 text-slate-400 animate-spin" />} body="Reading recent runs\u2026" />;
  }
  const list = runs.status === 'ready' ? runs.data.slice(0, 8) : [];
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-2">
        <div className="text-[10px] text-slate-500 font-mono">
          Showing {list.length} most recent run{list.length === 1 ? '' : 's'}
        </div>
      </div>
      {list.length === 0 ? (
        <EmptyState icon={<History className="w-4 h-4 text-slate-400" />}
          title="No runs yet"
          body="No Dagster runs yet. Trigger one in Dagster directly." />
      ) : (
        <div className="rounded-lg border border-slate-200 overflow-hidden">
          <div className="grid grid-cols-[90px_1fr_120px_120px_28px] bg-slate-50 px-3 py-1.5 text-[10px] font-bold text-slate-500 uppercase tracking-wider">
            <div>Status</div><div>Job</div><div>Started</div><div>Duration</div><div></div>
          </div>
          {list.map((r) => (
            <div key={r.id} className="grid grid-cols-[90px_1fr_120px_120px_28px] items-center px-3 py-1.5 text-xs border-t border-slate-100 hover:bg-slate-50/60">
              <div><Pill label={r.status} kind="STATUS" /></div>
              <div className="min-w-0">
                <div className="font-mono text-slate-800 truncate">{r.job || '\u2014'}</div>
                <div className="text-[9px] text-slate-400 font-mono truncate">{r.id}</div>
              </div>
              <div className="text-[10px] text-slate-600 font-mono">{formatRelative(r.started_at)}</div>
              <div className="text-[10px] text-slate-600 font-mono flex items-center gap-1">
                <Clock className="w-2.5 h-2.5 text-slate-400" />
                {formatDuration(r.duration_seconds)}
              </div>
              <div className="text-right">
                <a href={r.dagster_url} target="_blank" rel="noreferrer"
                  className="inline-flex p-1 rounded text-indigo-600 hover:bg-indigo-50 hover:text-indigo-800 transition-colors"
                  title="Open run in Dagster">
                  <ExternalLink className="w-3 h-3" />
                </a>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

// ---------------------------------------------------------------------------
// Top-level panel
// ---------------------------------------------------------------------------

// sessionStorage cache: lets the panel survive outer-app tab switches without
// a full re-fetch. Keyed by a cache-version stamp so shape changes invalidate.
const CACHE_KEY = 'obelisk:dagster:native:cache:v1';

interface CacheShape {
  schedules: DagsterSchedule[];
  runs: DagsterRun[];
  saved_at: number;
}

function loadCache(gMaxAgeMs: number): CacheShape | null {
  if (typeof window === 'undefined' || !window.sessionStorage) return null;
  try {
    const raw = window.sessionStorage.getItem(CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as CacheShape;
    if (!parsed || !Array.isArray(parsed.schedules) || !Array.isArray(parsed.runs)) return null;
    // Discard cache if older than gMaxAgeMs (30s default). The panel always
    // revalidates on mount; this just speeds up the perceived freshness
    // without flashing empty state.
    if (Date.now() - (parsed.saved_at || 0) > gMaxAgeMs) return null;
    return parsed;
  } catch {
    return null;
  }
}

function saveCache(schedules: DagsterSchedule[], runs: DagsterRun[]) {
  if (typeof window === 'undefined' || !window.sessionStorage) return;
  try {
    const payload: CacheShape = { schedules, runs, saved_at: Date.now() };
    window.sessionStorage.setItem(CACHE_KEY, JSON.stringify(payload));
  } catch {
    /* quota / private-mode cache miss is fine */
  }
}

function clearCache() {
  if (typeof window === 'undefined' || !window.sessionStorage) return;
  try {
    window.sessionStorage.removeItem(CACHE_KEY);
  } catch {
    /* ignore */
  }
}

const CACHE_MAX_AGE_MS = 30 * 1000;

export const DagsterNativePanel: React.FC<DagsterNativePanelProps> = ({ webUrl }) => {
  // Hydrate from sessionStorage on first render so re-mounting (e.g. the user
  // navigates Pipelines → Home → Pipelines) restores the last snapshot instantly.
  const initialCache = useMemo(() => loadCache(CACHE_MAX_AGE_MS), []);
  const initialSchedules = initialCache ? { status: 'ready' as const, data: initialCache.schedules } : { status: 'idle' as const };
  const initialRuns = initialCache ? { status: 'ready' as const, data: initialCache.runs } : { status: 'idle' as const };

  const [schedules, setSchedules] = useState<LoadState<DagsterSchedule[]>>(initialSchedules);
  const [runs, setRuns] = useState<LoadState<DagsterRun[]>>(initialRuns);
  const [feedback, setFeedback] = useState<{ kind: 'success' | 'error'; message: string } | null>(null);
  const [syncKey, setSyncKey] = useState<number>(0);
  const [refreshing, setRefreshing] = useState<boolean>(false);
  const [lastFetchedAt, setLastFetchedAt] = useState<number | null>(initialCache?.saved_at ?? null);

  useEffect(() => {
    let cancelled = false;
    setRefreshing(true);
    (async () => {
      try {
        const [s, r] = await Promise.all([
          api.getDagsterSchedules(),
          api.getDagsterRuns(20),
        ]);
        if (cancelled) return;
        const ds = s ?? [], dr = r ?? [];
        setSchedules({ status: 'ready', data: ds });
        setRuns({ status: 'ready', data: dr });
        saveCache(ds, dr);
        setLastFetchedAt(Date.now());
        setFeedback(null);
      } catch (e) {
        if (cancelled) return;
        const msg = e instanceof Error ? e.message : String(e);
        setSchedules({ status: 'ready', data: [] });
        setRuns({ status: 'ready', data: [] });
        clearCache();
        setFeedback({ kind: 'error', message: `Dagster unreachable: ${msg.slice(0, 200)}` });
      } finally {
        if (!cancelled) setRefreshing(false);
      }
    })();
    return () => { cancelled = true; };
  }, [syncKey]);

  // Auto-refresh every 60 s so the panel stays live without manual clicks.
  useEffect(() => {
    const id = setInterval(() => {
      clearCache();
      setSyncKey((k) => k + 1);
    }, 60_000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="flex-1 flex flex-col bg-white overflow-hidden relative">
      <EngineStrip webUrl={webUrl} schedules={schedules} runs={runs}
        onRefresh={() => { clearCache(); setSyncKey((k) => k + 1); }}
        refreshing={refreshing}
        lastFetchedAt={lastFetchedAt} />
      <div className="flex-1 overflow-y-auto">
        <SectionCard title="Schedules"
          hint="Dagster-managed cron."
          icon={<ListChecks className="w-3.5 h-3.5" />}>
          <SchedulesSection schedules={schedules} webUrl={webUrl} />
        </SectionCard>
        <SectionCard title="Recent activity"
          hint="Last 8 runs. Click into Dagster for full history."
          icon={<Activity className="w-3.5 h-3.5" />}>
          <RecentActivitySection runs={runs} />
        </SectionCard>
      </div>
      {feedback && (
        <div className={`absolute bottom-3 right-3 max-w-md flex items-start gap-2 px-3 py-2 rounded-lg shadow-lg border text-xs
          ${feedback.kind === 'success' ? 'bg-emerald-50 border-emerald-200 text-emerald-800' : 'bg-red-50 border-red-200 text-red-800'}`}>
          <div className="mt-0.5">
            {feedback.kind === 'success'
              ? <CheckCircle2 className="w-3.5 h-3.5 text-emerald-600" />
              : <AlertCircle className="w-3.5 h-3.5 text-red-600" />}
          </div>
          <span className="font-mono text-[11px] leading-snug break-all">{feedback.message}</span>
          <button onClick={() => setFeedback(null)} className="ml-1 mt-0.5 text-slate-400 hover:text-slate-700 transition-colors" title="Dismiss">
            <X className="w-3 h-3" />
          </button>
        </div>
      )}
    </div>
  );
};
