import React from 'react';
import { Search, HardDrive, Cpu, Activity } from 'lucide-react';
import { useTelemetry } from '../../lib/useTelemetry';
import { MOD_KEY } from '../../lib/platform';
import type { NavigationTab } from './Sidebar';

interface HeaderProps {
  activeTab: NavigationTab;
  onOpenCommand: () => void;
}

export const Header: React.FC<HeaderProps> = ({ activeTab, onOpenCommand }) => {
  const { telemetry, isLive } = useTelemetry();

  const titles: Record<NavigationTab, { title: string; subtitle: string }> = {
    home: {
      title: 'Platform Overview',
      subtitle: 'Real-time telemetry, platform assets, orchestration health & services',
    },
    catalog: {
      title: 'Data Catalog',
      subtitle: 'Explore local lakehouse & external databases',
    },
    query: {
      title: 'Query Studio',
      subtitle: 'Monaco-powered SQL workspace executing in DuckDB',
    },
    dbt: {
      title: 'dbt Studio',
      subtitle: 'Native dbt workspace with Jinja Monaco editing, live compiled SQL, ad-hoc previews & lineage',
    },
    notebooks: {
      title: 'Interactive Notebooks',
      subtitle: 'Git-controlled reactive Marimo environment with zero-config obelisk SDK',
    },
    pipelines: {
      title: 'Pipelines',
      subtitle: 'Schedule dbt models, scripts, and notebooks or connect Dagster',
    },
    dashboards: {
      title: 'Dashboards & BI',
      subtitle: 'Apache Superset visual analytics & analytical dashboards',
    },
    files: {
      title: 'git_repo File Manager',
      subtitle: 'Version-controlled repository workspace, dbt projects, scripts & notebooks',
    },
    terminal: {
      title: 'Developer Terminal',
      subtitle: 'Interactive real-time shell with direct access to git_repo/ and Obelisk CLI',
    },
    services: {
      title: 'Service Catalog',
      subtitle: 'Provision localhost containers or connect remote instances',
    },
    settings: {
      title: 'Settings & Security',
      subtitle: 'Manage environment variables, AES-256 encrypted secrets, connection vault, lakehouse storage & diagnostics',
    },
  };


  const current = titles[activeTab] || titles.home;

  return (
    <header className="h-16 border-b border-slate-200 bg-white px-6 flex items-center justify-between shrink-0">
      {/* Title & Subtitle */}
      <div>
        <h1 className="text-base font-bold text-slate-900 leading-tight">
          {current.title}
        </h1>
        <p className="text-xs text-slate-500 font-normal">{current.subtitle}</p>
      </div>

      {/* Right Controls: Command Search & Live Hardware Specs */}
      <div className="flex items-center gap-3">
        {/* Cmd+K Search Trigger */}
        <button
          onClick={onOpenCommand}
          className="flex items-center gap-2 px-3 py-1.5 text-xs text-slate-500 bg-slate-100 hover:bg-slate-200/80 rounded-lg border border-slate-200 transition-colors"
        >
          <Search className="w-3.5 h-3.5 text-slate-400" />
          <span>Search catalog & actions...</span>
          <kbd className="font-mono text-[10px] bg-white border border-slate-300 rounded px-1.5 py-0.5 text-slate-600 shadow-2xs">
            {MOD_KEY}K
          </kbd>
        </button>

        {/* Live Hardware Meter Badge */}
        {telemetry && (
          <div className="flex items-center gap-3 px-3 py-1 bg-slate-50 border border-slate-200 rounded-lg text-xs font-mono text-slate-600">
            <div className="flex items-center gap-1.5">
              <span
                className={`w-2 h-2 rounded-full ${isLive ? 'bg-emerald-500 animate-pulse' : 'bg-amber-400'}`}
                title={isLive ? 'Live WebSocket Telemetry (2s)' : 'Polling Fallback Telemetry (2s)'}
              />
              <HardDrive className="w-3.5 h-3.5 text-sky-600" />
              <span>
                RAM: {Math.round(telemetry.ram_used_gb)}/{Math.round(telemetry.total_ram_gb)} GB ({telemetry.ram_percent_used}%)
              </span>
            </div>
            <span className="text-slate-300">|</span>
            <div className="flex items-center gap-1.5">
              <Cpu className="w-3.5 h-3.5 text-amber-600" />
              <span>CPU: {telemetry.cpu_percent}%</span>
            </div>
            {telemetry.active_containers_count > 0 && (
              <>
                <span className="text-slate-300">|</span>
                <div className="flex items-center gap-1 text-[11px] text-slate-500">
                  <Activity className="w-3 h-3 text-emerald-600" />
                  <span>
                    {telemetry.active_containers_count} {telemetry.active_containers_count === 1 ? 'container' : 'containers'}
                  </span>
                </div>
              </>
            )}
          </div>
        )}
      </div>
    </header>
  );
};
