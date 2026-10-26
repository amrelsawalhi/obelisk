import React, { useState, useEffect } from 'react';
import {
  Layers,
  RefreshCw,
  AlertTriangle,
  ExternalLink,
  ChevronRight,
  Server,
} from 'lucide-react';
import { api } from '../../lib/api';
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
  const [services, setServices] = useState<Record<string, ServiceStatus>>({});
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [engineStatus, setEngineStatus] = useState<ServiceEngineStatus | null>(null);
  const [catalog, setCatalog] = useState<ServiceCatalogEntry[]>([]);
  const [selectedService, setSelectedService] = useState<string | null>(null);

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

  const sortedCatalog = [...catalog].sort((a, b) => {
    const rank = (e: ServiceCatalogEntry) => {
      const s = services[e.name];
      if (!s) return 2;
      if (s.status === 'running' && s.deployment_type === 'local') return 0;
      return 2;
    };
    return rank(a) - rank(b);
  });
  const runningCount = sortedCatalog.filter((e) => {
    const s = services[e.name];
    return !!s && s.status === 'running' && s.deployment_type === 'local';
  }).length;
  const stoppedCount = catalog.length - runningCount;

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

      <section className="space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Layers className="w-4 h-4 text-sky-600" />
            <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider font-mono">
              {catalog.length} Managed Engines
            </h3>
          </div>
          <div className="flex items-center gap-3">
            <div className="hidden md:flex items-center gap-3 text-xs font-mono">
              <span className="inline-flex items-center gap-1.5 text-emerald-600"><span className="w-2 h-2 rounded-full bg-emerald-500"></span>{runningCount} running</span>
              <span className="inline-flex items-center gap-1.5 text-slate-400"><span className="w-2 h-2 rounded-full bg-slate-300"></span>{stoppedCount} stopped</span>
            </div>
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

        {/* Compact service list */}
        <div className="space-y-2">
          {sortedCatalog.map((entry) => {
            const Icon = SERVICE_ICONS[entry.name] || Server;
            const status = services[entry.name];
            const isRunning = !!status && (status.status === 'running' || status.status === 'connected');
            const isLocal = status?.deployment_type !== 'remote';

            return (
              <button
                key={entry.name}
                onClick={() => setSelectedService(entry.name)}
                className={`w-full flex items-center gap-4 p-3 rounded-xl border text-left transition-all ${
                  isRunning
                    ? 'bg-white border-emerald-200 hover:border-emerald-300 shadow-xs'
                    : 'bg-white border-slate-200 hover:border-slate-300 opacity-80 hover:opacity-100'
                }`}
              >
                <div className={`w-10 h-10 rounded-lg flex items-center justify-center shrink-0 ${isRunning ? 'bg-emerald-50 text-emerald-600' : 'bg-slate-100 text-slate-400'}`}>
                  <Icon className="w-5 h-5" />
                </div>

                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-bold text-slate-900">{entry.display_name}</span>
                    <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-slate-100 text-slate-600 border border-slate-200">{entry.category}</span>
                    <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded border ${entry.run_mode === 'process' ? 'bg-indigo-50 text-indigo-700 border-indigo-200' : 'bg-slate-100 text-slate-500 border-slate-200'}`}>
                      {entry.run_mode === 'process' ? 'Process' : 'Container'}
                    </span>
                  </div>
                  <p className="text-xs text-slate-500 mt-0.5 truncate">{entry.description}</p>
                </div>

                <div className="flex items-center gap-2 shrink-0">
                  {isRunning ? (
                    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-semibold bg-emerald-600 text-white">
                      <span className="w-1.5 h-1.5 rounded-full bg-white animate-pulse"></span>
                      {isLocal ? 'Running' : 'Connected'}
                    </span>
                  ) : (
                    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-medium bg-slate-200 text-slate-500">
                      <span className="w-1.5 h-1.5 rounded-full bg-slate-400"></span>
                      Stopped
                    </span>
                  )}

                  {isRunning && entry.web_ui && (
                    <a
                      href={entry.web_ui_path || `http://localhost:${status?.host_port || status?.port || entry.default_host_port}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      onClick={(e) => e.stopPropagation()}
                      className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-[11px] font-semibold bg-sky-50 text-sky-700 hover:bg-sky-100 border border-sky-200 transition-colors"
                    >
                      <ExternalLink className="w-3 h-3" />
                      <span>Open UI</span>
                    </a>
                  )}

                  <ChevronRight className="w-4 h-4 text-slate-300" />
                </div>
              </button>
            );
          })}
        </div>
      </section>

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
