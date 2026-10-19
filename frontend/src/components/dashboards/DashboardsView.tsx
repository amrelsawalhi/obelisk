import React, { useState, useEffect } from 'react';
import {
  ExternalLink,
  Play,
  RefreshCw,
  AlertCircle,
  BarChart3,
} from 'lucide-react';
import { api } from '../../lib/api';
import type { ServiceStatus } from '../../lib/types';

export const DashboardsView: React.FC = () => {
  const [supersetStatus, setSupersetStatus] = useState<ServiceStatus | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [deploying, setDeploying] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const fetchStatus = async () => {
    try {
      setLoading(true);
      setError(null);
      const services = await api.listServices();
      const ss = services.find((s) => s.service_name.toLowerCase() === 'superset') || null;
      setSupersetStatus(ss);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to query Superset status');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchStatus();
  }, []);

  const handleDeployLocal = async () => {
    try {
      setDeploying(true);
      await api.deployLocalService({
        service_name: 'superset',
        memory_mb: 2048,
      });
      await fetchStatus();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to deploy Superset');
    } finally {
      setDeploying(false);
    }
  };

  const isRunning = supersetStatus?.status === 'running' || supersetStatus?.status === 'connected';

  return (
    <div className="flex-1 flex flex-col h-full bg-slate-50 overflow-hidden">
      {/* Top Bar */}
      <div className="h-14 border-b border-slate-200 bg-white px-6 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <span
              className={`w-2.5 h-2.5 rounded-full ${
                isRunning ? 'bg-emerald-500 animate-pulse' : 'bg-slate-300'
              }`}
            />
            <span className="text-xs font-bold text-slate-800">
              Apache Superset {isRunning ? 'Running' : 'Offline'}
            </span>
            <span className="text-[11px] font-mono text-slate-500 bg-slate-100 px-2 py-0.5 rounded border border-slate-200">
              Port 8088 • /superset
            </span>
          </div>

          <span className="text-slate-300">|</span>

          {!isRunning && (
            <button
              onClick={handleDeployLocal}
              disabled={deploying}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-sky-600 hover:bg-sky-700 text-white rounded-lg text-xs font-semibold shadow-xs transition-colors disabled:opacity-50"
            >
              {deploying ? (
                <>
                  <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                  <span>Provisioning Container...</span>
                </>
              ) : (
                <>
                  <Play className="w-3.5 h-3.5 fill-current" />
                  <span>Deploy Superset Locally</span>
                </>
              )}
            </button>
          )}
        </div>

        <div className="flex items-center gap-2">
          {isRunning && (
            <a
              href="/superset"
              target="_blank"
              rel="noreferrer"
              className="flex items-center gap-1 text-xs text-sky-700 font-medium hover:underline mr-2"
            >
              <span>Open in new window</span>
              <ExternalLink className="w-3 h-3" />
            </a>
          )}
          <button
            onClick={fetchStatus}
            title="Refresh status"
            className="p-1.5 border border-slate-200 rounded-lg text-slate-500 hover:text-slate-800 hover:bg-slate-100 transition-colors"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Main Content Area */}
      <div className="flex-1 bg-slate-100 flex flex-col overflow-hidden relative">
        {error && (
          <div className="m-4 p-3 bg-rose-50 border border-rose-200 rounded-lg text-xs text-rose-700 flex items-center gap-2">
            <AlertCircle className="w-4 h-4 shrink-0" />
            <span>{error}</span>
          </div>
        )}

        {isRunning ? (
          <iframe
            src="/superset"
            title="Apache Superset Dashboards"
            className="w-full h-full border-0 bg-white"
          />
        ) : (
          <div className="flex-1 flex flex-col items-center justify-center p-8 text-center space-y-4">
            <div className="w-16 h-16 rounded-2xl bg-white border border-slate-200 flex items-center justify-center shadow-xs text-slate-400">
              <BarChart3 className="w-8 h-8 text-sky-600" />
            </div>
            <div>
              <h3 className="text-sm font-bold text-slate-800">
                Apache Superset BI Engine is Offline
              </h3>
              <p className="text-xs text-slate-500 max-w-md mt-1">
                Obelisk integrates Apache Superset for indexed SQL visual dashboards querying your Gold dimensional marts in PostgreSQL and ClickHouse.
              </p>
            </div>
            <div className="flex items-center gap-3">
              <button
                onClick={handleDeployLocal}
                disabled={deploying}
                className="flex items-center gap-2 px-4 py-2 bg-sky-600 hover:bg-sky-700 text-white rounded-lg text-xs font-semibold shadow-xs transition-colors"
              >
                <Play className="w-3.5 h-3.5 fill-current" />
                <span>1-Click Deploy Locally (2 GB RAM)</span>
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};
