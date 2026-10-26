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
