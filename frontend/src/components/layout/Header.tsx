import React, { useState, useRef, useEffect } from 'react';
import {
  Search,
  HardDrive,
  Cpu,
  Activity,
  ChevronDown,
  ExternalLink,
  ShieldCheck,
  LogOut,
  Server,
} from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import { useTelemetry } from '../../lib/useTelemetry';
import { MOD_KEY } from '../../lib/platform';
import type { NavigationTab } from './Sidebar';

interface HeaderProps {
  activeTab: NavigationTab;
  onOpenCommand: () => void;
  onNavigate?: (tab: NavigationTab) => void;
}

export const Header: React.FC<HeaderProps> = ({ activeTab, onOpenCommand, onNavigate }) => {
  const { telemetry, isLive } = useTelemetry();
  const { user, authEnabled, logout } = useAuth();
  const navigate = useNavigate();

  const [isTelemetryOpen, setIsTelemetryOpen] = useState(false);
  const [isUserMenuOpen, setIsUserMenuOpen] = useState(false);

  const popoverRef = useRef<HTMLDivElement>(null);
  const userMenuRef = useRef<HTMLDivElement>(null);

  // Close popovers when clicking outside
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) {
        setIsTelemetryOpen(false);
      }
      if (userMenuRef.current && !userMenuRef.current.contains(e.target as Node)) {
        setIsUserMenuOpen(false);
      }
    };
    if (isTelemetryOpen || isUserMenuOpen) {
      document.addEventListener('mousedown', handleClickOutside);
    }
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [isTelemetryOpen, isUserMenuOpen]);

  const titles: Record<NavigationTab, { title: string; subtitle: string }> = {
    home: {
      title: 'Workspace Overview',
      subtitle: 'Real-time telemetry, platform assets, and orchestration health',
    },
    catalog: {
      title: 'Data Catalog',
      subtitle: 'Explore Lakehouse tables, schemas, and connected sources',
    },
    query: {
      title: 'Query Studio',
      subtitle: 'High-performance analytical SQL workspace',
    },
    dbt: {
      title: 'dbt Studio',
      subtitle: 'Jinja data models, live compiled SQL, and DAG lineage',
    },
    scripts: {
      title: 'Scripts Studio',
      subtitle: 'Author, test, and run Python pipeline jobs and data quality gates',
    },
    notebooks: {
      title: 'Notebooks',
      subtitle: 'Interactive exploratory data analysis powered by Marimo',
    },
    pipelines: {
      title: 'Pipelines',
      subtitle: 'Workflow orchestration, dependencies, and automated schedules',
    },
    dashboards: {
      title: 'Dashboards',
      subtitle: 'Visual business intelligence and Superset analytics',
    },
    terminal: {
      title: 'Developer Terminal',
      subtitle: 'Integrated system shell and Obelisk platform CLI',
    },
    services: {
      title: 'Service Catalog',
      subtitle: 'Manage containerized engines, databases, and plugins',
    },
    settings: {
      title: 'Settings & Security',
      subtitle: 'Credential vault, storage configuration, and system diagnostics',
    },
  };

  const current = titles[activeTab] || titles.home;

  return (
    <header className="h-16 border-b border-slate-200 bg-white px-6 flex items-center justify-between shrink-0 select-none">
      {/* Title & Subtitle */}
      <div>
        <h1 className="text-base font-bold text-slate-900 tracking-tight leading-tight">
          {current.title}
        </h1>
        <p className="text-xs text-slate-500 font-normal">{current.subtitle}</p>
      </div>

      {/* Right Controls: Search & Interactive Engine Popover */}
      <div className="flex items-center gap-2.5">
        {/* Cmd+K Search Trigger */}
        <button
          onClick={onOpenCommand}
          className="flex items-center gap-2.5 px-3 py-1.5 text-xs text-slate-500 bg-slate-50 hover:bg-slate-100 active:bg-slate-200/70 rounded-lg border border-slate-200/80 transition-all shadow-2xs"
        >
          <Search className="w-3.5 h-3.5 text-slate-400" />
          <span className="hidden md:inline">Search catalog & actions...</span>
          <span className="md:hidden">Search...</span>
          <kbd className="font-mono text-[10px] bg-white border border-slate-200 rounded px-1.5 py-0.5 text-slate-600 shadow-2xs">
            {MOD_KEY}K
          </kbd>
        </button>

        {/* Interactive Engine & Telemetry Popover */}
        <div className="relative" ref={popoverRef}>
          <button
            onClick={() => setIsTelemetryOpen((prev) => !prev)}
            className={`flex items-center gap-2 px-2.5 py-1.5 rounded-lg border text-xs font-medium transition-all shadow-2xs ${
              isTelemetryOpen
                ? 'bg-slate-100 border-slate-300 text-slate-900 ring-2 ring-sky-500/10'
                : 'bg-white hover:bg-slate-50 border-slate-200 text-slate-700'
            }`}
          >
            <span className="relative flex h-2 w-2">
              <span
                className={`animate-ping absolute inline-flex h-full w-full rounded-full ${
                  isLive ? 'bg-emerald-400 opacity-75' : 'bg-amber-400 opacity-75'
                }`}
              />
              <span
                className={`relative inline-flex rounded-full h-2 w-2 ${
                  isLive ? 'bg-emerald-500' : 'bg-amber-500'
                }`}
              />
            </span>
            <span className="font-semibold text-slate-800">Engine Ready</span>
            {telemetry && (
              <span className="font-mono text-[11px] text-slate-500 bg-slate-100 px-1.5 py-0.5 rounded hidden lg:inline">
                {telemetry.cpu_percent}% CPU
              </span>
            )}
            <ChevronDown
              className={`w-3.5 h-3.5 text-slate-400 transition-transform ${
                isTelemetryOpen ? 'rotate-180' : ''
              }`}
            />
          </button>

          {/* Popover Flyout */}
          {isTelemetryOpen && (
            <div className="absolute right-0 mt-2 w-72 rounded-xl bg-white border border-slate-200 shadow-xl p-4 z-50 text-xs space-y-3.5 animate-in fade-in-50 zoom-in-95 duration-100">
              {/* Popover Header */}
              <div className="flex items-center justify-between border-b border-slate-100 pb-2.5">
                <div className="flex items-center gap-1.5">
                  <Activity className="w-3.5 h-3.5 text-sky-600" />
                  <span className="font-bold text-slate-800">Compute & Engine Telemetry</span>
                </div>
                <span className="text-[10px] font-semibold text-emerald-700 bg-emerald-50 border border-emerald-200 px-1.5 py-0.5 rounded-full">
                  {isLive ? 'Live 2s' : 'Polling'}
                </span>
              </div>

              {telemetry ? (
                <div className="space-y-3">
                  {/* RAM Meter */}
                  <div>
                    <div className="flex justify-between text-slate-600 mb-1">
                      <span className="flex items-center gap-1 font-medium text-slate-700">
                        <HardDrive className="w-3.5 h-3.5 text-sky-600" /> Host RAM
                      </span>
                      <span className="font-mono font-bold text-slate-900">
                        {Math.round(telemetry.ram_used_gb)}/{Math.round(telemetry.total_ram_gb)} GB
                      </span>
                    </div>
                    <div className="w-full bg-slate-100 rounded-full h-2 overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all duration-300 ${
                          telemetry.ram_percent_used > 85
                            ? 'bg-red-500'
                            : telemetry.ram_percent_used > 65
                              ? 'bg-amber-500'
                              : 'bg-sky-600'
                        }`}
                        style={{ width: `${Math.min(telemetry.ram_percent_used, 100)}%` }}
                      />
                    </div>
                    <p className="text-[10px] text-slate-400 mt-0.5">
                      Safe Max: {telemetry.max_safe_allocation_gb} GB • Out-of-core spill enabled
                    </p>
                  </div>

                  {/* CPU Meter */}
                  <div>
                    <div className="flex justify-between text-slate-600 mb-1">
                      <span className="flex items-center gap-1 font-medium text-slate-700">
                        <Cpu className="w-3.5 h-3.5 text-sky-600" /> Host CPU ({telemetry.cpu_count} Cores)
                      </span>
                      <span className="font-mono font-bold text-slate-900">{telemetry.cpu_percent}%</span>
                    </div>
                    <div className="w-full bg-slate-100 rounded-full h-2 overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all duration-300 ${
                          telemetry.cpu_percent > 85
                            ? 'bg-red-500'
                            : telemetry.cpu_percent > 65
                              ? 'bg-amber-500'
                              : 'bg-sky-600'
                        }`}
                        style={{ width: `${Math.min(telemetry.cpu_percent, 100)}%` }}
                      />
                    </div>
                  </div>

                  {/* DuckDB Concurrency, Spill & Disk Headroom */}
                  <div className="pt-2 border-t border-slate-100 space-y-1.5 text-[11px] text-slate-600">
                    <div className="flex items-center justify-between">
                      <span className="font-medium text-slate-700">DuckDB Heavy Slots</span>
                      <span className="font-mono font-bold text-slate-900 bg-slate-100 px-1.5 py-0.5 rounded">
                        {telemetry.active_heavy_queries ?? 0}/{telemetry.max_concurrent_heavy ?? 2} active
                        {(telemetry.waiting_heavy_queries ?? 0) > 0
                          ? ` (${telemetry.waiting_heavy_queries} queued)`
                          : ''}
                      </span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="font-medium text-slate-700">Temp Spill Usage</span>
                      <span className="font-mono text-slate-800">
                        {telemetry.temp_spill_mb ?? 0} MB / {telemetry.temp_spill_limit ?? '50GB'}
                      </span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="font-medium text-slate-700">Lakehouse Headroom</span>
                      <span className="font-mono text-emerald-700 font-semibold">
                        {telemetry.lakehouse_free_gb ?? '—'} GB free
                      </span>
                    </div>
                  </div>

                  {/* Active Containers */}
                  <div className="pt-2 border-t border-slate-100 flex items-center justify-between text-slate-600">
                    <span className="flex items-center gap-1 font-medium text-slate-700">
                      <Server className="w-3.5 h-3.5 text-sky-600" /> Active Services
                    </span>
                    <span className="font-mono font-bold text-slate-800 bg-slate-100 px-2 py-0.5 rounded text-[11px]">
                      {telemetry.active_containers_count} running
                    </span>
                  </div>
                </div>
              ) : (
                <div className="py-4 text-center text-slate-400">Loading telemetry...</div>
              )}

              {/* Popover Footer Link */}
              {onNavigate && (
                <button
                  onClick={() => {
                    setIsTelemetryOpen(false);
                    onNavigate('settings');
                  }}
                  className="w-full text-center text-xs font-semibold text-sky-600 hover:text-sky-700 pt-2 border-t border-slate-100 flex items-center justify-center gap-1 transition-colors"
                >
                  <span>System Diagnostics in Settings</span>
                  <ExternalLink className="w-3 h-3" />
                </button>
              )}
            </div>
          )}
        </div>

        {/* User Account Popover */}
        {authEnabled && user && (
          <div className="relative" ref={userMenuRef}>
            <button
              onClick={() => setIsUserMenuOpen((prev) => !prev)}
              aria-label="User account menu"
              className={`flex items-center gap-2 pl-2 pr-2.5 py-1.5 rounded-lg border text-xs font-medium transition-all shadow-2xs ${
                isUserMenuOpen
                  ? 'bg-slate-100 border-slate-300 text-slate-900 ring-2 ring-sky-500/10'
                  : 'bg-white hover:bg-slate-50 border-slate-200 text-slate-700'
              }`}
            >
              <div className="w-5.5 h-5.5 rounded-md bg-sky-100 border border-sky-200/80 flex items-center justify-center text-[10px] font-bold text-sky-800 shrink-0 uppercase">
                {user.username.slice(0, 2)}
              </div>
              <span className="font-semibold text-slate-800 max-w-[120px] truncate">
                {user.username}
              </span>
              <ChevronDown
                className={`w-3.5 h-3.5 text-slate-400 transition-transform ${
                  isUserMenuOpen ? 'rotate-180' : ''
                }`}
              />
            </button>

            {/* User Dropdown Menu */}
            {isUserMenuOpen && (
              <div className="absolute right-0 mt-2 w-56 rounded-xl bg-white border border-slate-200 shadow-xl p-2 z-50 text-xs animate-in fade-in-50 zoom-in-95 duration-100">
                {/* User Info Header */}
                <div className="px-3 py-2 border-b border-slate-100 mb-1">
                  <div className="font-bold text-slate-900 truncate">{user.username}</div>
                  <div className="flex items-center gap-1.5 mt-1">
                    <span className="text-[10px] font-semibold text-sky-700 bg-sky-50 border border-sky-200/70 px-1.5 py-0.5 rounded-md uppercase">
                      {user.role}
                    </span>
                    <span className="text-[10px] text-slate-400">Authenticated</span>
                  </div>
                </div>

                {/* Navigation Links */}
                {onNavigate && (
                  <button
                    onClick={() => {
                      setIsUserMenuOpen(false);
                      onNavigate('settings');
                    }}
                    className="w-full flex items-center gap-2 px-3 py-2 text-slate-700 hover:bg-slate-50 rounded-lg transition-colors text-left font-medium"
                  >
                    <ShieldCheck className="w-4 h-4 text-slate-400" />
                    <span>Settings & Security</span>
                  </button>
                )}

                {/* Sign Out Action */}
                <div className="pt-1 mt-1 border-t border-slate-100">
                  <button
                    onClick={async () => {
                      setIsUserMenuOpen(false);
                      await logout();
                      navigate('/login', { replace: true });
                    }}
                    className="w-full flex items-center gap-2 px-3 py-2 text-rose-600 hover:bg-rose-50 rounded-lg transition-colors text-left font-medium"
                  >
                    <LogOut className="w-4 h-4 text-rose-500" />
                    <span>Sign out</span>
                  </button>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </header>
  );
};
