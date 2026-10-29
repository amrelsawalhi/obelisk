import React, { useCallback, useEffect, useState } from 'react';
import {
  AlertCircle,
  BarChart3,
  CheckCircle2,
  Database,
  ExternalLink,
  RefreshCw,
  Server,
  LayoutDashboard,
  LineChart,
  Table2,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { api } from '../../lib/api';
import { serviceUrl } from '../../lib/urls';
import type {
  ServiceStatus,
  SupersetInventory,
  SupersetSourceReport,
  SupersetSyncReport,
} from '../../lib/types';

const SUPERSET_IMAGE = 'apache/superset:4.1.1';

/** The metric cards, in the order Superset's own navigation uses. */
const METRICS: { key: string; label: string; icon: LucideIcon }[] = [
  { key: 'dashboards', label: 'Dashboards', icon: LayoutDashboard },
  { key: 'charts', label: 'Charts', icon: LineChart },
  { key: 'datasets', label: 'Datasets', icon: Table2 },
  { key: 'databases', label: 'Databases', icon: Database },
];

const ACTION_STYLES: Record<string, string> = {
  created: 'bg-emerald-50 text-emerald-700 border-emerald-200',
  updated: 'bg-sky-50 text-sky-700 border-sky-200',
  deleted: 'bg-slate-50 text-slate-600 border-slate-200',
  absent: 'bg-slate-50 text-slate-500 border-slate-200',
  pending: 'bg-slate-50 text-slate-600 border-slate-200',
  'would-upsert': 'bg-slate-50 text-slate-600 border-slate-200',
  skipped: 'bg-amber-50 text-amber-800 border-amber-200',
  failed: 'bg-rose-50 text-rose-700 border-rose-200',
};

const formatWhen = (value?: string | null): string => {
  if (!value) return '';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return '';
  return parsed.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
};

interface DashboardsViewProps {
  /**
   * Sends the operator to Catalog → Services with Superset preselected, where deploying
   * lives. This page used to offer its own deploy button, which meant the same service
   * was deployed from two places with two different memory defaults.
   */
  onDeploySuperset: () => void;
}

const MetricCard: React.FC<{
  label: string;
  value: number | null | undefined;
  icon: LucideIcon;
}> = ({ label, value, icon: Icon }) => (
  <div className="p-3.5 bg-white border border-slate-200 rounded-xl shadow-2xs flex items-center justify-between">
    <div>
      <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-400">{label}</div>
      <div className="mt-0.5 text-xl font-bold text-slate-900 tabular-nums">
        {typeof value === 'number' ? value : '—'}
      </div>
    </div>
    <div className="w-8 h-8 rounded-lg bg-slate-50 border border-slate-100 flex items-center justify-center text-slate-400 shrink-0">
      <Icon className="w-4 h-4" />
    </div>
  </div>
);

const SourceRow: React.FC<{ source: SupersetSourceReport }> = ({ source }) => (
  <li className="px-4 py-2.5 flex items-start gap-2">
    <div className="min-w-0 flex-1">
      <div className="flex items-center gap-2">
        <span className="text-xs font-medium text-slate-800 truncate">{source.name}</span>
        <span
          className={`shrink-0 px-1.5 py-px rounded-full border text-[10px] font-semibold ${
            ACTION_STYLES[source.action] ?? ACTION_STYLES.pending
          }`}
        >
          {source.action.replace('-', ' ')}
        </span>
      </div>
      {source.uri ? (
        <div className="mt-0.5 text-[10px] font-mono text-slate-400 truncate" title={source.uri}>
          {source.uri}
        </div>
      ) : null}
      {source.detail ? (
        <div className="mt-0.5 text-[10px] text-slate-500 truncate" title={source.detail}>
          {source.detail}
        </div>
      ) : null}
    </div>
  </li>
);

export const DashboardsView: React.FC<DashboardsViewProps> = ({ onDeploySuperset }) => {
  const [supersetStatus, setSupersetStatus] = useState<ServiceStatus | null>(null);
  const [inventory, setInventory] = useState<SupersetInventory | null>(null);
  const [sources, setSources] = useState<SupersetSyncReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [syncError, setSyncError] = useState<string | null>(null);

  const isRunning =
    supersetStatus?.status === 'running' || supersetStatus?.status === 'connected';
  const port = supersetStatus?.host_port || supersetStatus?.port || 8088;
  // Superset answers at the root of its own port. `serviceUrl` keeps whatever host the
  // browser used, so the link works from another machine on the LAN, not just localhost.
  const baseUrl = serviceUrl(port);

  /**
   * Status first: it decides whether there is anything to ask Superset about. Both
   * Superset calls then tolerate failure — an unfriendly Superset must not take down the
   * page that exists to describe it.
   */
  const load = useCallback(async (silent = false) => {
    try {
      if (silent) setRefreshing(true);
      else setLoading(true);
      setError(null);

      const services = await api.listServices();
      const status = services.find((s) => s.service_name.toLowerCase() === 'superset') || null;
      setSupersetStatus(status);

      const running = status?.status === 'running' || status?.status === 'connected';
      if (!running) {
        setInventory(null);
        setSources(null);
        return;
      }

      const [nextInventory, nextSources] = await Promise.all([
        api.getSupersetInventory().catch(() => null),
        api.getSupersetSources().catch(() => null),
      ]);
      setInventory(nextInventory);
      setSources(nextSources);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to query Superset status');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const handleSync = async () => {
    try {
      setSyncing(true);
      setSyncError(null);
      const report = await api.syncSupersetSources();
      setSources(report);
      // A sync changes the dataset count, so the numbers are re-read instead of being
      // left to describe a Superset that has moved on.
      const nextInventory = await api.getSupersetInventory().catch(() => null);
      if (nextInventory) setInventory(nextInventory);
    } catch (e: unknown) {
      setSyncError(e instanceof Error ? e.message : 'Sync failed');
    } finally {
      setSyncing(false);
    }
  };

  const inventoryUnavailable = inventory !== null && !inventory.available;
  const dashboards = inventory?.dashboards ?? [];
  const dashboardCount = inventory?.counts?.dashboards;
  const sourceCounts = sources?.counts ?? {};
  const readyToSync =
    (sourceCounts.created ?? 0) + (sourceCounts.updated ?? 0) + (sourceCounts['would-upsert'] ?? 0);

  return (
    <div className="flex-1 flex flex-col h-full bg-slate-50 overflow-hidden">
      {/* Main Content Area */}
      <div className="flex-1 bg-slate-50 flex flex-col overflow-hidden relative">
        {error && (
          <div className="m-4 p-3 bg-rose-50 border border-rose-200 rounded-lg text-xs text-rose-700 flex items-center gap-2">
            <AlertCircle className="w-4 h-4 shrink-0" />
            <span>{error}</span>
          </div>
        )}

        {!isRunning ? (
          <div className="flex-1 flex flex-col items-center justify-center p-8 bg-slate-50/50">
            <div className="max-w-md w-full bg-white p-8 rounded-2xl border border-slate-200/90 shadow-xs flex flex-col items-center text-center">
              <div className="w-14 h-14 rounded-2xl bg-sky-50 border border-sky-100 flex items-center justify-center text-sky-600 mb-4 shadow-2xs">
                <BarChart3 className="w-7 h-7" />
              </div>

              <h3 className="text-base font-bold text-slate-900 tracking-tight">
                Business Intelligence & Visual Dashboards
              </h3>

              <p className="text-xs text-slate-500 mt-2 leading-relaxed">
                Apache Superset turns your Lakehouse and PostgreSQL marts into interactive
                charts and shared dashboards. Obelisk deploys it and pushes your registered
                connections in as data sources.
              </p>

              <div className="w-full grid grid-cols-2 gap-2 my-5 pt-4 border-t border-slate-100 text-[11px] font-mono text-left">
                <div className="p-2.5 rounded-lg bg-slate-50 border border-slate-200/60">
                  <span className="text-slate-400 block text-[10px] uppercase font-sans font-semibold">
                    Engine
                  </span>
                  <span className="text-slate-800 font-medium truncate block">{SUPERSET_IMAGE}</span>
                </div>
                <div className="p-2.5 rounded-lg bg-slate-50 border border-slate-200/60">
                  <span className="text-slate-400 block text-[10px] uppercase font-sans font-semibold">
                    Address
                  </span>
                  <span className="text-slate-800 font-medium">:{port}</span>
                </div>
              </div>

              <p className="text-[11px] text-slate-500 mb-4">
                {supersetStatus?.container_exists
                  ? 'Superset is deployed but stopped. Start it from Catalog → Services.'
                  : 'Superset is not deployed yet. Deploy it from Catalog → Services.'}
              </p>

              <button
                onClick={onDeploySuperset}
                className="w-full flex items-center justify-center gap-2 px-4 py-2.5 bg-sky-600 hover:bg-sky-700 active:bg-sky-800 text-white rounded-xl text-xs font-semibold shadow-xs transition-colors"
              >
                <Server className="w-4 h-4" />
                <span>Open Services to Deploy</span>
              </button>
            </div>
          </div>
        ) : (
          <div className="flex-1 overflow-y-auto p-6 space-y-4">
            {/* Launcher Card */}
            <section className="flex flex-wrap items-center justify-between gap-4 p-5 bg-white border border-slate-200 rounded-2xl shadow-2xs">
              <div className="flex items-center gap-3 min-w-0">
                <div className="w-11 h-11 shrink-0 rounded-xl bg-sky-50 border border-sky-100 flex items-center justify-center text-sky-600">
                  <BarChart3 className="w-5 h-5" />
                </div>
                <div className="flex items-center gap-2.5">
                  <span
                    className={`w-2.5 h-2.5 rounded-full ${
                      isRunning ? 'bg-emerald-500 animate-pulse' : 'bg-slate-300'
                    }`}
                  />
                  <h3 className="text-sm font-bold text-slate-900 tracking-tight">
                    Apache Superset
                  </h3>
                </div>
              </div>

              <div className="flex items-center gap-2">
                <button
                  onClick={() => load(true)}
                  title="Refresh status"
                  className="p-2 border border-slate-200 rounded-xl text-slate-700 hover:text-slate-900 bg-white hover:bg-slate-50 shadow-2xs transition-colors"
                >
                  <RefreshCw className={`w-3.5 h-3.5 ${loading || refreshing ? 'animate-spin' : ''}`} />
                </button>
                <a
                  href={baseUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="flex items-center gap-1.5 px-4 py-2 bg-sky-600 hover:bg-sky-700 text-white rounded-xl text-xs font-semibold shadow-xs transition-colors"
                >
                  <span>Open Superset</span>
                  <ExternalLink className="w-3.5 h-3.5" />
                </a>
              </div>
            </section>

            {/* Numbers come from Superset itself. When it cannot answer the page says
                why, because four zeros would read as an empty install. */}
            {inventoryUnavailable ? (
              <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg text-xs text-amber-800 flex items-start gap-2">
                <AlertCircle className="w-4 h-4 shrink-0 mt-0.5" />
                <span>
                  <strong>Superset is running, but its API did not answer.</strong>{' '}
                  {inventory?.detail ?? 'The admin account may still be initializing.'} Open it
                  directly to see its current state.
                </span>
              </div>
            ) : (
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
                {METRICS.map((metric) => (
                  <MetricCard
                    key={metric.key}
                    label={metric.label}
                    value={inventory?.counts?.[metric.key] ?? null}
                    icon={metric.icon}
                  />
                ))}
              </div>
            )}

            <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
              {/* Recent dashboards, as Superset orders them: most recently changed first. */}
              <section className="bg-white border border-slate-200 rounded-2xl shadow-2xs overflow-hidden flex flex-col">
                <header className="px-4 py-3 border-b border-slate-100 flex items-center justify-between gap-2">
                  <div className="flex items-center gap-2">
                    <h4 className="text-xs font-bold text-slate-800 uppercase tracking-wider font-mono">
                      Recent Dashboards
                    </h4>
                    {typeof dashboardCount === 'number' ? (
                      <span className="px-1.5 py-px rounded-full bg-slate-100 border border-slate-200 text-[10px] font-semibold text-slate-600">
                        {dashboardCount}
                      </span>
                    ) : null}
                  </div>
                  <a
                    href={`${baseUrl}/superset/dashboard/list/`}
                    target="_blank"
                    rel="noreferrer"
                    className="text-[11px] text-sky-700 font-medium hover:underline"
                  >
                    Browse all
                  </a>
                </header>

                {dashboards.length > 0 ? (
                  <ul className="divide-y divide-slate-100">
                    {dashboards.map((dashboard) => {
                      const rawPath = dashboard.path ?? `/dashboard/${dashboard.id}/`;
                      const fullPath = rawPath.startsWith('/superset')
                        ? rawPath
                        : `/superset${rawPath.startsWith('/') ? '' : '/'}${rawPath}`;
                      return (
                        <li key={`${dashboard.id}-${dashboard.title}`}>
                          <a
                            href={`${baseUrl}${fullPath}`}
                            target="_blank"
                            rel="noreferrer"
                            className="px-4 py-2.5 flex items-center justify-between gap-3 hover:bg-slate-50 transition-colors"
                          >
                            <div className="min-w-0">
                              <div className="text-xs font-medium text-slate-800 truncate">
                                {dashboard.title}
                              </div>
                            <div className="mt-0.5 flex items-center gap-2 text-[10px] text-slate-400">
                              {dashboard.published ? (
                                <span className="inline-flex items-center gap-1 text-emerald-600">
                                  <CheckCircle2 className="w-3 h-3" /> published
                                </span>
                              ) : (
                                <span>draft</span>
                              )}
                              {formatWhen(dashboard.changed_on) ? (
                                <span>changed {formatWhen(dashboard.changed_on)}</span>
                              ) : null}
                            </div>
                          </div>
                          <ExternalLink className="w-3.5 h-3.5 text-slate-300 shrink-0" />
                        </a>
                      </li>
                    );
                  })}
                  </ul>
                ) : (
                  <div className="px-4 py-8 text-center">
                    <BarChart3 className="w-6 h-6 text-slate-300 mx-auto mb-2" />
                    <p className="text-xs text-slate-500">
                      {inventory?.available === false
                        ? 'Dashboards could not be listed.'
                        : 'No dashboards yet.'}
                    </p>
                    <p className="text-[11px] text-slate-400 mt-1">
                      Build the first one in Superset from the sources below.
                    </p>
                  </div>
                )}
              </section>

              {/* Data sources: the connections Obelisk would push into Superset, with
                  passwords masked. "Sync now" runs the same endpoint a deploy runs. */}
              <section className="bg-white border border-slate-200 rounded-2xl shadow-2xs overflow-hidden flex flex-col">
                <header className="px-4 py-3 border-b border-slate-100 flex items-center justify-between gap-2">
                  <div className="flex items-center gap-2">
                    <h4 className="text-xs font-bold text-slate-800 uppercase tracking-wider font-mono">
                      Data Sources
                    </h4>
                    {sources ? (
                      <span className="px-1.5 py-px rounded-full bg-slate-100 border border-slate-200 text-[10px] font-semibold text-slate-600">
                        {sources.sources.length}
                      </span>
                    ) : null}
                  </div>
                  <button
                    onClick={handleSync}
                    disabled={syncing}
                    className="flex items-center gap-1 px-2 py-1 border border-slate-300 rounded-lg text-[11px] font-semibold text-slate-700 bg-white hover:bg-slate-50 disabled:opacity-50 transition-colors"
                  >
                    <RefreshCw className={`w-3 h-3 ${syncing ? 'animate-spin' : ''}`} />
                    <span>Sync now</span>
                  </button>
                </header>

                {syncError && (
                  <div className="px-4 py-2 bg-rose-50 border-b border-rose-100 text-[11px] text-rose-700 flex items-start gap-1.5">
                    <AlertCircle className="w-3.5 h-3.5 shrink-0 mt-px" />
                    <span>{syncError}</span>
                  </div>
                )}

                {sources && sources.sources.length > 0 ? (
                  <>
                    <ul className="divide-y divide-slate-100 flex-1">
                      {sources.sources.map((source) => (
                        <SourceRow key={source.name} source={source} />
                      ))}
                    </ul>
                    <footer className="px-4 py-2 border-t border-slate-100 text-[10px] text-slate-500 flex items-center gap-2">
                      <Database className="w-3 h-3 text-slate-400" />
                      <span>
                        {readyToSync} of {sources.sources.length} ready
                        {(sourceCounts.skipped ?? 0) > 0
                          ? ` · ${sourceCounts.skipped} skipped`
                          : ''}
                        {(sourceCounts.failed ?? 0) > 0 ? ` · ${sourceCounts.failed} failed` : ''}
                      </span>
                    </footer>
                  </>
                ) : (
                  <div className="px-4 py-8 text-center">
                    <Database className="w-6 h-6 text-slate-300 mx-auto mb-2" />
                    <p className="text-xs text-slate-500">No connections registered yet.</p>
                    <p className="text-[11px] text-slate-400 mt-1">
                      Add a connection in the Catalog and it appears here.
                    </p>
                  </div>
                )}
              </section>
            </div>

            {inventory?.available && inventory.detail ? (
              <p className="text-[10px] text-slate-400 font-mono">{inventory.detail}</p>
            ) : null}
          </div>
        )}
      </div>
    </div>
  );
};
