import React, { useState, useEffect } from 'react';
import { Search, Database, Terminal, GitFork, BookOpen, Server, Settings, X, ArrowRight, FolderCode, SquareTerminal, Boxes } from 'lucide-react';
import type { NavigationTab } from './Sidebar';

interface CommandMenuProps {
  isOpen: boolean;
  onClose: () => void;
  onNavigate: (tab: NavigationTab) => void;
}

export const CommandMenu: React.FC<CommandMenuProps> = ({
  isOpen,
  onClose,
  onNavigate,
}) => {
  const [search, setSearch] = useState('');

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        if (isOpen) onClose();
        else onNavigate('catalog'); // open if closed
      }
      if (e.key === 'Escape' && isOpen) {
        onClose();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [isOpen, onClose, onNavigate]);

  if (!isOpen) return null;

  const actions = [
    {
      label: 'Explore Lakehouse Catalog & Tables',
      category: 'Data Catalog',
      icon: Database,
      action: () => {
        onNavigate('catalog');
        onClose();
      },
    },
    {
      label: 'Open Query Studio & Execute SQL in DuckDB',
      category: 'Query Studio',
      icon: Terminal,
      action: () => {
        onNavigate('query');
        onClose();
      },
    },
    {
      label: 'Open dbt Studio (Transformations, Compile & Lineage)',
      category: 'dbt Studio',
      icon: Boxes,
      action: () => {
        onNavigate('dbt');
        onClose();
      },
    },
    {
      label: 'Launch Reactive Marimo Notebook Workspace',
      category: 'Notebooks',
      icon: BookOpen,
      action: () => {
        onNavigate('notebooks');
        onClose();
      },
    },
    {
      label: 'Trigger End-to-End Medallion dbt Pipeline',
      category: 'Pipelines',
      icon: GitFork,
      action: () => {
        onNavigate('pipelines');
        onClose();
      },
    },
    {
      label: 'Open git_repo File Manager & Code Studio',
      category: 'Files',
      icon: FolderCode,
      action: () => {
        onNavigate('files');
        onClose();
      },
    },
    {
      label: 'Launch Developer Web Terminal & Shell',
      category: 'Terminal',
      icon: SquareTerminal,
      action: () => {
        onNavigate('terminal');
        onClose();
      },
    },
    {
      label: 'Provision Localhost Containers & Connect Services',
      category: 'Service Catalog',
      icon: Server,
      action: () => {
        onNavigate('services');
        onClose();
      },
    },
    {
      label: 'Manage Environment Variables, Secrets & Vault',
      category: 'Settings',
      icon: Settings,
      action: () => {
        onNavigate('settings');
        onClose();
      },
    },
  ];


  const filtered = actions.filter((a) =>
    a.label.toLowerCase().includes(search.toLowerCase()) ||
    a.category.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center pt-20 bg-slate-900/40 backdrop-blur-xs p-4 animate-in fade-in duration-150">
      <div className="w-full max-w-xl bg-white rounded-xl shadow-2xl border border-slate-200 overflow-hidden flex flex-col">
        {/* Search Input Bar */}
        <div className="flex items-center px-4 py-3 border-b border-slate-200 gap-3">
          <Search className="w-4 h-4 text-slate-400 shrink-0" />
          <input
            type="text"
            autoFocus
            placeholder="Search tables, actions, or tools..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="flex-1 text-sm bg-transparent outline-hidden text-slate-800 placeholder:text-slate-400"
          />
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-600 p-1 rounded-md"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Results List */}
        <div className="max-h-80 overflow-y-auto p-2 space-y-1">
          {filtered.length === 0 ? (
            <div className="p-6 text-center text-xs text-slate-400 font-mono">
              No matching actions or assets found.
            </div>
          ) : (
            filtered.map((item, idx) => {
              const Icon = item.icon;
              return (
                <button
                  key={idx}
                  onClick={item.action}
                  className="w-full flex items-center justify-between p-2.5 rounded-lg text-left text-sm text-slate-700 hover:bg-slate-100 hover:text-slate-900 transition-colors group"
                >
                  <div className="flex items-center gap-3">
                    <div className="p-1.5 rounded-md bg-slate-100 text-slate-500 group-hover:bg-sky-100 group-hover:text-sky-700 transition-colors">
                      <Icon className="w-4 h-4" />
                    </div>
                    <div>
                      <div className="font-medium text-slate-900 text-xs">
                        {item.label}
                      </div>
                      <div className="text-[10px] text-slate-400 font-mono">
                        {item.category}
                      </div>
                    </div>
                  </div>
                  <ArrowRight className="w-3.5 h-3.5 text-slate-400 opacity-0 group-hover:opacity-100 transition-opacity" />
                </button>
              );
            })
          )}
        </div>

        {/* Footer Navigation Tip */}
        <div className="px-4 py-2 border-t border-slate-100 bg-slate-50 flex items-center justify-between text-[11px] text-slate-400 font-mono">
          <span>Use ⌘K to trigger anywhere</span>
          <span>ESC to close</span>
        </div>
      </div>
    </div>
  );
};
