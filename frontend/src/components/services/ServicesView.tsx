import React, { useState, useEffect, useMemo } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import {
  RefreshCw,
  AlertTriangle,
  ExternalLink,
  ChevronRight,
  Server,
  Search,
  CheckCircle2,
  Cpu,
  HardDrive,
  SlidersHorizontal,
  X,
  Zap,
} from 'lucide-react';
import { api } from '../../lib/api';
import { serviceUrl } from '../../lib/urls';
import { TAB_PATHS } from '../../lib/navigation';
import { ServiceDetailPanel } from './ServiceDetailPanel';
import { SERVICE_ICONS } from './serviceIcons';
import type {
  ServiceStatus,
  ServiceCatalogEntry,
  ServiceEngineStatus,
} from '../../lib/types';

// Service catalog display metadata (name, display_name, category, description,
// default_host_port, web_ui, run_mode) is sourced from GET /api/services/catalog —
// the backend blueprints are the single source of truth.

export const ServicesView: React.FC = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const [services, setServices] = useState<Record<string, ServiceStatus>>({});
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [engineStatus, setEngineStatus] = useState<ServiceEngineStatus | null>(null);
  const [catalog, setCatalog] = useState<ServiceCatalogEntry[]>([]);
  const [selectedService, setSelectedService] = useState<string | null>(null);

  // Filters & Search
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState<'all' | 'running' | 'stopped'>('all');
  const [categoryFilter, setCategoryFilter] = useState<string>('all');

  const loadData = async (silent = false) => {
    try {
      if (!silent) setLoading(true);
      else setRefreshing(true);
      setError(null);

      const [servicesData, engineData, catalogData] = await Promise.all([
        api.listServices().catch(() => []),
        api.getServicesEngine().catch(() => null),
        api.getServiceCatalog().catch(() => [] as ServiceCatalogEntry[]),
      ]);
      setEngineStatus(engineData);
      setCatalog(catalogData);

      const sMap: Record<string, ServiceStatus> = {};
      servicesData.forEach((s) => {
        sMap[s.service_name.toLowerCase()] = s;
      });
      setServices(sMap);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to fetch services data');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    loadData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Arriving from the Dashboards page ("Open Services to Deploy") opens that service's
  // panel. The router state is then cleared, so coming back to Services later does not
  // reopen a panel the operator already closed.
  useEffect(() => {
    const wanted = (location.state as { service?: string } | null)?.service;
    if (!wanted) return;
    setSelectedService(wanted);
    navigate(TAB_PATHS.services, { replace: true });
  }, [location.state, navigate]);

  // Categories list
  const categories = useMemo(() => {
    return Array.from(new Set(catalog.map((c) => c.category))).filter(Boolean);
  }, [catalog]);

  // Sorted catalog: active/running first, then deployed/stopped, then standby, secondary alphabetical
  const sortedCatalog = useMemo(() => {
    return [...catalog].sort((a, b) => {
      const rank = (e: ServiceCatalogEntry) => {
        const s = services[e.name];
        if (!s) return 3;
        if (s.status === 'running' || s.status === 'connected') return 0;
        if (s.container_exists || s.status === 'stopped') return 1;
        return 2;
      };
      const diff = rank(a) - rank(b);
      if (diff !== 0) return diff;
      return a.display_name.localeCompare(b.display_name);
    });
  }, [catalog, services]);

  const runningCount = useMemo(() => {
    return catalog.filter((e) => {
      const s = services[e.name];
      return !!s && (s.status === 'running' || s.status === 'connected');
    }).length;
  }, [catalog, services]);

  const stoppedCount = catalog.length - runningCount;

  // Total RAM allocated to running services
  const totalAllocatedMb = useMemo(() => {
    return Object.values(services).reduce((sum, s) => {
      if ((s.status === 'running' || s.status === 'connected') && s.allocated_memory_mb) {
        return sum + s.allocated_memory_mb;
      }
      return sum;
    }, 0);
  }, [services]);

  // Filtered catalog based on search, status filter, and category filter
  const filteredCatalog = useMemo(() => {
    return sortedCatalog.filter((entry) => {
      const s = services[entry.name];
      const isRunning = !!s && (s.status === 'running' || s.status === 'connected');

      // Status filter
      if (statusFilter === 'running' && !isRunning) return false;
      if (statusFilter === 'stopped' && isRunning) return false;

      // Category filter
      if (
        categoryFilter !== 'all' &&
        entry.category.toLowerCase() !== categoryFilter.toLowerCase()
      ) {
        return false;
      }

      // Search query
      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase();
        const matchName =
          entry.display_name.toLowerCase().includes(q) ||
          entry.name.toLowerCase().includes(q);
        const matchCategory = entry.category.toLowerCase().includes(q);
        const matchDesc = entry.description.toLowerCase().includes(q);
        if (!matchName && !matchCategory && !matchDesc) return false;
      }

      return true;
    });
  }, [sortedCatalog, services, statusFilter, categoryFilter, searchQuery]);

  const selected = selectedService ? catalog.find((e) => e.name === selectedService) : undefined;

  if (loading && Object.keys(services).length === 0) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center p-12 bg-slate-50">
        <RefreshCw className="w-8 h-8 text-sky-600 animate-spin mb-3" />
        <div className="text-sm font-semibold text-slate-700">Loading Service Catalog...</div>
        <div className="text-xs text-slate-400 font-mono mt-1">Connecting to services</div>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto bg-slate-100/75 p-6 space-y-6">
      {/* Error alert */}
      {error && (
        <div className="p-3 bg-red-50 border border-red-200 rounded-xl flex items-center gap-2.5 text-xs text-red-700 animate-in fade-in shadow-xs">
          <AlertTriangle className="w-4 h-4 text-red-500 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {/* Docker unreachable alert */}
      {engineStatus && !engineStatus.docker.available && (
        <div className="p-3.5 bg-amber-50 border border-amber-200 rounded-xl flex items-start gap-3 text-xs text-amber-900 animate-in fade-in shadow-xs">
          <AlertTriangle className="w-5 h-5 text-amber-600 shrink-0 mt-0.5" />
          <div>
            <strong className="font-semibold">Docker engine is not reachable.</strong> Start Docker Desktop (or configure DOCKER_HOST) to enable 1-click local service deployments.
            {engineStatus.docker.error ? (
              <span className="block font-mono text-[11px] text-amber-700 mt-1 bg-amber-100/70 p-1.5 rounded border border-amber-200">
                {engineStatus.docker.error}
              </span>
            ) : null}
          </div>
        </div>
      )}

      {/* 1. Header & Page Title */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-xl font-bold text-slate-900 tracking-tight">Service Catalog</h2>
          <p className="text-xs text-slate-500 mt-0.5">
            Manage containerized data engines, storage layers, and analytical tooling.
          </p>
        </div>

        <div className="flex items-center gap-2.5 shrink-0">
          <button
            onClick={() => loadData(true)}
            disabled={refreshing}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 bg-white hover:bg-slate-50 text-xs font-semibold text-slate-700 shadow-2xs transition-all"
            title="Refresh service status and engine telemetry"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin text-sky-600' : 'text-slate-500'}`} />
            <span>Sync</span>
          </button>
        </div>
      </div>

      {/* 2. Top Infrastructure KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {/* Card 1: Active Engines */}
        <div className="bg-white rounded-xl border border-slate-200/90 p-4 shadow-xs flex items-center justify-between">
          <div className="space-y-1">
            <span className="text-[11px] font-bold uppercase tracking-wider text-slate-500">
              Active Engines
            </span>
            <div className="flex items-baseline gap-2">
              <span className="text-2xl font-extrabold font-mono text-slate-900">
                {runningCount}
                <span className="text-sm font-semibold text-slate-400 font-sans"> / {catalog.length}</span>
              </span>
            </div>
            <p className="text-[11px] text-slate-400">
              {runningCount} running • {stoppedCount} on standby
            </p>
          </div>
          <div className="w-10 h-10 rounded-xl bg-emerald-50 text-emerald-600 border border-emerald-100 flex items-center justify-center shrink-0">
            <CheckCircle2 className="w-5 h-5" />
          </div>
        </div>

        {/* Card 2: Allocated Memory */}
        <div className="bg-white rounded-xl border border-slate-200/90 p-4 shadow-xs flex items-center justify-between">
          <div className="space-y-1">
            <span className="text-[11px] font-bold uppercase tracking-wider text-slate-500">
              Allocated RAM
            </span>
            <div className="flex items-baseline gap-2">
              <span className="text-2xl font-extrabold font-mono text-slate-900">
                {(totalAllocatedMb / 1024).toFixed(1)} <span className="text-sm font-semibold text-slate-500 font-sans">GB</span>
              </span>
            </div>
            <p className="text-[11px] text-slate-400">
              {runningCount > 0 ? `Across ${runningCount} active engine${runningCount > 1 ? 's' : ''}` : 'No active memory limit'}
            </p>
          </div>
          <div className="w-10 h-10 rounded-xl bg-slate-100 text-slate-600 border border-slate-200 flex items-center justify-center shrink-0">
            <Cpu className="w-5 h-5" />
          </div>
        </div>

        {/* Card 3: Docker Engine Host */}
        <div className="bg-white rounded-xl border border-slate-200/90 p-4 shadow-xs flex items-center justify-between">
          <div className="space-y-1">
            <span className="text-[11px] font-bold uppercase tracking-wider text-slate-500">
              Engine Daemon
            </span>
            <div className="flex items-baseline gap-2">
              <span className="text-base font-bold text-slate-900 flex items-center gap-1.5">
                <span className={`w-2 h-2 rounded-full ${engineStatus?.docker.available ? 'bg-emerald-500 animate-pulse' : 'bg-rose-500'}`} />
                {engineStatus?.docker.available ? 'Docker Ready' : 'Engine Offline'}
              </span>
            </div>
            <p className="text-[11px] text-slate-400 font-mono">
              {engineStatus?.disk
                ? `${engineStatus.disk.free_gb.toFixed(0)} GB Free (${engineStatus.disk.percent_used}% disk used)`
                : 'Host socket monitored'}
            </p>
          </div>
          <div className="w-10 h-10 rounded-xl bg-slate-100 text-slate-600 border border-slate-200 flex items-center justify-center shrink-0">
            <HardDrive className="w-5 h-5" />
          </div>
        </div>
      </div>

      {/* 3. Search and Category Filters Toolbar */}
      <div className="bg-white rounded-xl border border-slate-200/90 p-3 shadow-xs space-y-3">
        <div className="flex flex-col sm:flex-row items-stretch sm:items-center justify-between gap-3">
          {/* Search Bar */}
          <div className="relative flex-1 max-w-md">
            <Search className="w-4 h-4 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
            <input
              type="text"
              placeholder="Search engines by name, category, or description..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-9 pr-8 py-1.5 rounded-lg border border-slate-200 text-xs bg-slate-50/60 focus:bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500 transition-all"
            />
            {searchQuery && (
              <button
                onClick={() => setSearchQuery('')}
                className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600 p-0.5"
              >
                <X className="w-3.5 h-3.5" />
              </button>
            )}
          </div>

          {/* Status Filter Tabs */}
          <div className="flex items-center gap-1 bg-slate-100/80 p-1 rounded-lg border border-slate-200/60 text-xs font-semibold shrink-0">
            <button
              onClick={() => setStatusFilter('all')}
              className={`px-3 py-1 rounded-md transition-all ${
                statusFilter === 'all'
                  ? 'bg-white text-slate-900 shadow-2xs font-bold'
                  : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              All ({catalog.length})
            </button>
            <button
              onClick={() => setStatusFilter('running')}
              className={`px-3 py-1 rounded-md transition-all flex items-center gap-1.5 ${
                statusFilter === 'running'
                  ? 'bg-white text-emerald-800 shadow-2xs font-bold'
                  : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
              <span>Running ({runningCount})</span>
            </button>
            <button
              onClick={() => setStatusFilter('stopped')}
              className={`px-3 py-1 rounded-md transition-all flex items-center gap-1.5 ${
                statusFilter === 'stopped'
                  ? 'bg-white text-slate-800 shadow-2xs font-bold'
                  : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              <span className="w-1.5 h-1.5 rounded-full bg-slate-400" />
              <span>Stopped ({stoppedCount})</span>
            </button>
          </div>
        </div>

        {/* Category Filter Chips */}
        {categories.length > 0 && (
          <div className="flex items-center gap-1.5 overflow-x-auto pt-2 border-t border-slate-100 text-xs">
            <span className="text-[11px] font-semibold text-slate-400 flex items-center gap-1 mr-1 shrink-0">
              <SlidersHorizontal className="w-3 h-3" />
              <span>Category:</span>
            </span>
            <button
              onClick={() => setCategoryFilter('all')}
              className={`px-2.5 py-0.5 rounded-full text-[11px] font-medium transition-colors shrink-0 ${
                categoryFilter === 'all'
                  ? 'bg-slate-900 text-white font-semibold'
                  : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
              }`}
            >
              All
            </button>
            {categories.map((cat) => (
              <button
                key={cat}
                onClick={() => setCategoryFilter(cat)}
                className={`px-2.5 py-0.5 rounded-full text-[11px] font-medium transition-colors shrink-0 ${
                  categoryFilter.toLowerCase() === cat.toLowerCase()
                    ? 'bg-slate-900 text-white font-semibold'
                    : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                }`}
              >
                {cat}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* 4. Modern Engine Grid */}
      {filteredCatalog.length === 0 ? (
        <div className="p-12 text-center bg-white rounded-xl border border-slate-200/90 shadow-xs space-y-3">
          <div className="w-12 h-12 rounded-xl bg-slate-100 text-slate-400 flex items-center justify-center mx-auto">
            <Server className="w-6 h-6" />
          </div>
          <div>
            <h4 className="text-sm font-bold text-slate-800">No services match your filters</h4>
            <p className="text-xs text-slate-400 mt-1">
              Try adjusting your search terms or clearing status and category filters.
            </p>
          </div>
          <button
            onClick={() => {
              setSearchQuery('');
              setStatusFilter('all');
              setCategoryFilter('all');
            }}
            className="px-3.5 py-1.5 rounded-lg border border-slate-200 bg-white hover:bg-slate-50 text-xs font-semibold text-slate-700 shadow-2xs transition-all"
          >
            Clear all filters
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-5">
          {filteredCatalog.map((entry) => {
            const Icon = SERVICE_ICONS[entry.name] || Server;
            const status = services[entry.name];
            const isRunning =
              !!status && (status.status === 'running' || status.status === 'connected');
            const isLocal = status?.deployment_type !== 'remote';
            const hostPort = status?.host_port || status?.port || entry.default_host_port;

            return (
              <div
                key={entry.name}
                onClick={() => setSelectedService(entry.name)}
                className={`bg-white rounded-xl border p-5 shadow-xs transition-all duration-200 cursor-pointer flex flex-col justify-between space-y-4 group hover:shadow-md ${
                  isRunning
                    ? 'border-slate-200/90 hover:border-emerald-300'
                    : 'border-slate-200/70 hover:border-slate-300 opacity-90 hover:opacity-100'
                }`}
              >
                {/* Top: Icon + Title + Status Pill */}
                <div className="space-y-3">
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex items-center gap-3">
                      <div
                        className={`w-11 h-11 rounded-xl flex items-center justify-center shrink-0 border transition-all ${
                          isRunning
                            ? 'bg-emerald-50 text-emerald-600 border-emerald-200 group-hover:scale-105 group-hover:bg-emerald-100/80'
                            : 'bg-slate-100 text-slate-400 border-slate-200 group-hover:scale-105 group-hover:bg-slate-200/60'
                        }`}
                      >
                        <Icon className="w-5 h-5" />
                      </div>
                      <div>
                        <h4 className="text-sm font-bold text-slate-900 group-hover:text-sky-600 transition-colors leading-tight">
                          {entry.display_name}
                        </h4>
                        <div className="flex items-center gap-1.5 mt-1">
                          <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-slate-100 text-slate-600 border border-slate-200/80">
                            {entry.category}
                          </span>
                          <span
                            className={`text-[10px] font-mono px-1.5 py-0.5 rounded border ${
                              entry.run_mode === 'process'
                                ? 'bg-sky-50 text-sky-700 border-sky-200'
                                : 'bg-slate-100 text-slate-500 border-slate-200/80'
                            }`}
                          >
                            {entry.run_mode === 'process' ? 'Process' : 'Container'}
                          </span>
                        </div>
                      </div>
                    </div>

                    {/* Status Pill */}
                    <div className="shrink-0">
                      {isRunning ? (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-semibold bg-emerald-50 text-emerald-700 border border-emerald-200">
                          <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                          <span>{isLocal ? 'Running' : 'Connected'}</span>
                        </span>
                      ) : status?.container_exists ? (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-medium bg-slate-100 text-slate-600 border border-slate-200">
                          <span className="w-1.5 h-1.5 rounded-full bg-slate-400" />
                          <span>Stopped</span>
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-medium bg-slate-50 text-slate-400 border border-slate-200/80">
                          <span className="w-1.5 h-1.5 rounded-full bg-slate-300" />
                          <span>Standby</span>
                        </span>
                      )}
                    </div>
                  </div>

                  {/* Description */}
                  <p className="text-xs text-slate-500 leading-relaxed line-clamp-2 min-h-[36px]">
                    {entry.description}
                  </p>
                </div>

                {/* Resource Specifications Strip */}
                <div className="space-y-3 pt-3 border-t border-slate-100">
                  <div className="grid grid-cols-3 gap-2 text-[11px] font-mono text-slate-600">
                    <div className="bg-slate-50 border border-slate-100 p-2 rounded-lg">
                      <div className="text-[10px] text-slate-400 uppercase font-sans font-medium">Host Port</div>
                      <div className="font-bold text-slate-800 truncate">:{hostPort}</div>
                    </div>
                    <div className="bg-slate-50 border border-slate-100 p-2 rounded-lg">
                      <div className="text-[10px] text-slate-400 uppercase font-sans font-medium">RAM Cap</div>
                      <div className="font-bold text-slate-800 truncate">
                        {status?.allocated_memory_mb && status.allocated_memory_mb > 0
                          ? `${status.allocated_memory_mb} MB`
                          : isRunning
                          ? 'Host Node'
                          : 'Standby'}
                      </div>
                    </div>
                    <div className="bg-slate-50 border border-slate-100 p-2 rounded-lg">
                      <div className="text-[10px] text-slate-400 uppercase font-sans font-medium">Container</div>
                      <div className="font-semibold text-slate-700 truncate" title={`obelisk_${entry.name}`}>
                        {entry.run_mode === 'process' ? 'obelisk' : `obelisk_${entry.name}`}
                      </div>
                    </div>
                  </div>

                  {/* Card Actions Footer */}
                  <div className="flex items-center justify-between pt-1">
                    <div>
                      {isRunning && entry.web_ui ? (
                        <a
                          href={serviceUrl(hostPort, entry.web_ui_path)}
                          target="_blank"
                          rel="noopener noreferrer"
                          onClick={(e) => e.stopPropagation()}
                          className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold bg-sky-50 text-sky-700 hover:bg-sky-100 border border-sky-200 transition-colors shadow-2xs"
                          title={`Open ${entry.display_name} in new browser tab`}
                        >
                          <ExternalLink className="w-3.5 h-3.5" />
                          <span>Open Web UI</span>
                        </a>
                      ) : !isRunning && entry.deployable ? (
                        <span className="inline-flex items-center gap-1 text-[11px] font-medium text-slate-400">
                          <Zap className="w-3 h-3 text-slate-400" />
                          <span>Ready to deploy</span>
                        </span>
                      ) : null}
                    </div>

                    <div className="flex items-center gap-1 text-xs font-semibold text-slate-600 group-hover:text-sky-600 transition-colors">
                      <span>Manage</span>
                      <ChevronRight className="w-4 h-4 group-hover:translate-x-0.5 transition-transform" />
                    </div>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Detail drawer */}
      {selected && (
        <ServiceDetailPanel
          service={selected}
          status={services[selected.name]}
          engineStatus={engineStatus}
          onClose={() => setSelectedService(null)}
          onChanged={() => loadData(true)}
        />
      )}
    </div>
  );
};
