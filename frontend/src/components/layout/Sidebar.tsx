import {
  Home,
  Database,
  Code2,
  BookOpen,
  GitFork,
  BarChart3,
  Server,
  Settings,
  FolderCode,
  SquareTerminal,
  Boxes,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { ObeliskIcon } from '../common/ObeliskIcon';
import { useDevToolsEnabled } from '../../lib/devTools';

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

interface NavItem {
  id: NavigationTab;
  label: string;
  icon: LucideIcon;
}

interface NavSection {
  label: string;
  items: NavItem[];
}

export const Sidebar: React.FC<SidebarProps> = ({ activeTab, onSelectTab }) => {
  // Eleven flat rows made every screen equally loud. Three sections say what kind of
  // work each one is for, and the two raw tools over git_repo/ are a preference
  // rather than a peer of dbt Studio (see `lib/devTools.ts`).
  const [devToolsEnabled] = useDevToolsEnabled();

  const sections: NavSection[] = [
    {
      label: 'Explore & Analyze',
      items: [
        { id: 'home', label: 'Home', icon: Home },
        { id: 'catalog', label: 'Data Catalog', icon: Database },
        { id: 'query', label: 'Query Studio', icon: Code2 },
        { id: 'notebooks', label: 'Notebooks', icon: BookOpen },
        { id: 'dashboards', label: 'Dashboards', icon: BarChart3 },
      ],
    },
    {
      label: 'Build & Orchestrate',
      items: [
        { id: 'dbt', label: 'dbt Studio', icon: Boxes },
        { id: 'pipelines', label: 'Pipelines', icon: GitFork },
      ],
    },
    {
      label: 'Platform',
      items: [
        { id: 'services', label: 'Service Catalog', icon: Server },
        { id: 'settings', label: 'Settings', icon: Settings },
        ...(devToolsEnabled
          ? ([
              { id: 'files', label: 'Files', icon: FolderCode },
              { id: 'terminal', label: 'Terminal', icon: SquareTerminal },
            ] as NavItem[])
          : []),
      ],
    },
  ];

  return (
    <aside className="w-64 border-r border-slate-200 bg-white flex flex-col select-none shrink-0">
      {/* Brand Header */}
      <div className="h-16 border-b border-slate-200 flex items-center px-4 gap-3 bg-gradient-to-b from-slate-50/50 to-transparent">
        <div className="w-9 h-9 rounded-xl bg-white border border-slate-200/90 shadow-2xs flex items-center justify-center shrink-0 ring-2 ring-slate-100">
          <ObeliskIcon className="w-6.5 h-6.5" />
        </div>
        <div className="flex flex-col min-w-0">
          <span className="text-[18px] font-black tracking-tight text-slate-950 leading-none">
            Obelisk
          </span>
          <span className="text-[10px] font-bold text-sky-600 tracking-wider uppercase mt-1">
            Data Platform
          </span>
        </div>
      </div>

      {/* Navigation Links, grouped by the kind of work they start */}
      <nav className="flex-1 overflow-y-auto py-4 px-3 space-y-5">
        {sections.map((section) => (
          <div key={section.label} className="space-y-1">
            <div className="px-3 pb-1 text-[10px] font-semibold uppercase tracking-wider text-slate-400">
              {section.label}
            </div>
            {section.items.map((item) => {
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
          </div>
        ))}
      </nav>

      {/* Footer Workspace Status */}
      <div className="p-3 border-t border-slate-200/80 bg-slate-50/50">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 min-w-0">
            <span className="relative flex h-2 w-2 shrink-0">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
              <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
            </span>
            <span className="text-xs font-semibold text-slate-800 leading-tight truncate">
              DuckDB Lakehouse
            </span>
          </div>
          <span className="text-[10px] font-semibold text-emerald-700 bg-emerald-50 border border-emerald-200/80 px-2 py-0.5 rounded-full shrink-0">
            Online
          </span>
        </div>
      </div>
    </aside>
  );
};
