import {
  Home,
  Database,
  Code2,
  BookOpen,
  GitFork,
  BarChart3,
  Server,
  Settings,
  Cpu,
  FolderCode,
  SquareTerminal,
  Boxes,
} from 'lucide-react';
import { ObeliskIcon } from '../common/ObeliskIcon';

export type NavigationTab =
  | 'home'
  | 'catalog'
  | 'query'
  | 'dbt'
  | 'notebooks'
  | 'pipelines'
  | 'dashboards'
  | 'files'
  | 'terminal'
  | 'services'
  | 'settings';

interface SidebarProps {
  activeTab: NavigationTab;
  onSelectTab: (tab: NavigationTab) => void;
}

export const Sidebar: React.FC<SidebarProps> = ({ activeTab, onSelectTab }) => {
  const navItems = [
    { id: 'home' as NavigationTab, label: 'Home', icon: Home },
    { id: 'catalog' as NavigationTab, label: 'Data Catalog', icon: Database },
    { id: 'query' as NavigationTab, label: 'Query Studio', icon: Code2 },
    { id: 'dbt' as NavigationTab, label: 'dbt Studio', icon: Boxes },
    { id: 'notebooks' as NavigationTab, label: 'Notebooks', icon: BookOpen },
    { id: 'pipelines' as NavigationTab, label: 'Pipelines', icon: GitFork },
    { id: 'dashboards' as NavigationTab, label: 'Dashboards', icon: BarChart3 },
    { id: 'files' as NavigationTab, label: 'Files', icon: FolderCode },
    { id: 'terminal' as NavigationTab, label: 'Terminal', icon: SquareTerminal },
    { id: 'services' as NavigationTab, label: 'Service Catalog', icon: Server },
    { id: 'settings' as NavigationTab, label: 'Settings', icon: Settings },
  ];


  return (
    <aside className="w-64 border-r border-slate-200 bg-white flex flex-col select-none shrink-0">
      {/* Brand Header */}
      <div className="h-16 border-b border-slate-200 flex items-center px-5 gap-3">
        <div className="w-8 h-8 rounded-lg bg-sky-600 flex items-center justify-center text-white shadow-sm shadow-sky-200">
          <ObeliskIcon className="w-5 h-5" />
        </div>
        <div>
          <div className="font-bold text-slate-900 tracking-tight leading-none text-base">
            OBELISK
          </div>
          <div className="text-[10px] text-slate-500 font-mono tracking-wider uppercase mt-1">
            Data Platform
          </div>
        </div>
      </div>

      {/* Navigation Links */}
      <nav className="flex-1 py-4 px-3 space-y-1">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive = activeTab === item.id;
          return (
            <button
              key={item.id}
              onClick={() => onSelectTab(item.id)}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all ${
                isActive
                  ? 'bg-sky-50 text-sky-700 font-semibold shadow-xs'
                  : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
              }`}
            >
              <Icon
                className={`w-4 h-4 transition-colors ${
                  isActive ? 'text-sky-600' : 'text-slate-400'
                }`}
              />
              <span>{item.label}</span>
            </button>
          );
        })}
      </nav>

      {/* Footer System Status Indicator */}
      <div className="p-4 border-t border-slate-200 bg-slate-50/70">
        <div className="flex items-center justify-between text-xs text-slate-600 mb-1.5">
          <span className="flex items-center gap-1.5 font-medium">
            <Cpu className="w-3.5 h-3.5 text-slate-400" /> Control Plane
          </span>
          <span className="flex items-center gap-1 text-[11px] font-semibold text-emerald-600">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse"></span>
            Online
          </span>
        </div>
        <div className="text-[11px] text-slate-400 font-mono">
          Strictly Real • Localhost Core
        </div>
      </div>
    </aside>
  );
};
