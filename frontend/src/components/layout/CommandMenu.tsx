import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import type { NavigateFunction } from 'react-router-dom';
import {
  Search,
  Database,
  Terminal,
  GitFork,
  BookOpen,
  Server,
  Settings,
  X,
  ArrowRight,
  FolderCode,
  SquareTerminal,
  Boxes,
  Table2,
  Plug,
  Loader2,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import type { CatalogNode, CatalogTable, Connection, DbtTreeNode, Notebook } from '../../lib/types';
import { api } from '../../lib/api';
import { TAB_PATHS } from '../../lib/navigation';
import { MOD_KEY } from '../../lib/platform';
import type { NavigationTab } from './Sidebar';

type CommandItemKind = 'action' | 'table' | 'connection' | 'notebook' | 'model';

interface CommandItem {
  id: string;
  kind: CommandItemKind;
  label: string;
  subtitle: string;
  keywords: string;
  icon: LucideIcon;
  run: () => void;
}

interface PaletteData {
  nodes: CatalogNode[];
  connections: Connection[];
  notebooks: Notebook[];
  dbtFiles: DbtTreeNode[];
}

interface CommandMenuProps {
  isOpen: boolean;
  onClose: () => void;
}

const KIND_HEADERS: Record<CommandItemKind, string> = {
  action: 'Actions',
  table: 'Tables',
  connection: 'Connections',
  notebook: 'Notebooks',
  model: 'dbt Models',
};

const NAV_ACTIONS: { tab: NavigationTab; label: string; category: string; icon: LucideIcon }[] = [
  { tab: 'catalog', label: 'Explore Lakehouse Catalog & Tables', category: 'Data Catalog', icon: Database },
  { tab: 'query', label: 'Open Query Studio & Execute SQL in DuckDB', category: 'Query Studio', icon: Terminal },
  { tab: 'dbt', label: 'Open dbt Studio (Transformations, Compile & Lineage)', category: 'dbt Studio', icon: Boxes },
  { tab: 'notebooks', label: 'Launch Reactive Marimo Notebook Workspace', category: 'Notebooks', icon: BookOpen },
  { tab: 'pipelines', label: 'Trigger End-to-End Medallion dbt Pipeline', category: 'Pipelines', icon: GitFork },
  { tab: 'files', label: 'Open git_repo File Manager & Code Studio', category: 'Files', icon: FolderCode },
  { tab: 'terminal', label: 'Launch Developer Web Terminal & Shell', category: 'Terminal', icon: SquareTerminal },
  { tab: 'services', label: 'Provision Localhost Containers & Connect Services', category: 'Service Catalog', icon: Server },
  { tab: 'settings', label: 'Manage Environment Variables, Secrets & Vault', category: 'Settings', icon: Settings },
];

function collectSqlFiles(node: DbtTreeNode | null | undefined, acc: DbtTreeNode[] = []): DbtTreeNode[] {
  if (!node) return acc;
  if (node.type === 'file' && node.extension === '.sql') acc.push(node);
  (node.children || []).forEach((child) => collectSqlFiles(child, acc));
  return acc;
}

function makeTableItem(node: CatalogNode, table: CatalogTable, fqn: string, navigate: NavigateFunction): CommandItem {
  const sql = `-- Query Table\nSELECT * FROM ${fqn} LIMIT 50;\n`;
  const rows = table.row_count != null ? ` • ${table.row_count.toLocaleString()} rows` : '';
  return {
    id: `table:${node.id}:${table.name}`,
    kind: 'table',
    label: table.name,
    subtitle: `${fqn}${rows}`,
    keywords: `${fqn} ${node.name} ${node.type}`,
    icon: Table2,
    run: () => navigate('/query', { state: { sql } }),
  };
}

function buildItems(data: PaletteData | null, navigate: NavigateFunction): CommandItem[] {
  const items: CommandItem[] = NAV_ACTIONS.map((a) => ({
    id: `action:${a.tab}`,
    kind: 'action',
    label: a.label,
    subtitle: a.category,
    keywords: a.category,
    icon: a.icon,
    run: () => navigate(TAB_PATHS[a.tab]),
  }));

  if (!data) return items;

  for (const node of data.nodes) {
    if (node.type === 'lakehouse') {
      for (const dir of node.directories || []) {
        for (const table of dir.tables) {
          const fqn = table.fqn || `lakehouse.${dir.path.replace(/\//g, '.')}.${table.name}`;
          items.push(makeTableItem(node, table, fqn, navigate));
        }
      }
    } else {
      for (const table of node.tables || []) {
        const fqn = table.fqn || `${node.name}.${table.schema_name || 'public'}.${table.name}`;
        items.push(makeTableItem(node, table, fqn, navigate));
      }
    }
  }

  for (const conn of data.connections) {
    items.push({
      id: `connection:${conn.id}`,
      kind: 'connection',
      label: conn.name,
      subtitle: `${conn.source_type} • ${conn.host}:${conn.port}/${conn.database}`,
      keywords: `${conn.source_type} ${conn.host} ${conn.database} ${conn.username}`,
      icon: Plug,
      run: () => navigate('/catalog'),
    });
  }

  for (const nb of data.notebooks) {
    items.push({
      id: `notebook:${nb.filename}`,
      kind: 'notebook',
      label: nb.name,
      subtitle: 'Marimo Notebook',
      keywords: `notebook marimo ${nb.filename}`,
      icon: BookOpen,
      run: () => navigate('/notebooks'),
    });
  }

  for (const file of data.dbtFiles) {
    items.push({
      id: `model:${file.path}`,
      kind: 'model',
      label: file.name.replace(/\.sql$/, ''),
      subtitle: `dbt model • ${file.path}`,
      keywords: `dbt model ${file.path} ${file.layer || ''}`,
      icon: Boxes,
      run: () => navigate('/dbt'),
    });
  }

  return items;
}

function rank(items: CommandItem[], query: string): CommandItem[] {
  const q = query.trim().toLowerCase();
  if (!q) return items.filter((i) => i.kind === 'action');

  const kindRank: Record<CommandItemKind, number> = { table: 4, connection: 3, notebook: 2, model: 1, action: 0 };
  const tokens = q.split(/\s+/).filter(Boolean);

  const scored = items
    .map((item) => {
      const label = item.label.toLowerCase();
      const subtitle = item.subtitle.toLowerCase();
      const hay = `${item.label} ${item.subtitle} ${item.keywords}`.toLowerCase();
      if (!tokens.every((t) => hay.includes(t))) return null;

      let score: number;
      if (label.startsWith(q)) score = 500;
      else if (label.includes(q)) score = 400;
      else if (subtitle.includes(q)) score = 300;
      else score = 200;

      return { item, score: score + kindRank[item.kind] };
    })
    .filter((x): x is { item: CommandItem; score: number } => x !== null)
    .sort((a, b) => b.score - a.score);

  return scored.map((s) => s.item);
}

function groupItems(items: CommandItem[]) {
  const groups: { kind: CommandItemKind; items: { item: CommandItem; flatIndex: number }[] }[] = [];
  const indexByKind: Record<string, number> = {};
  items.forEach((item, flatIndex) => {
    if (indexByKind[item.kind] === undefined) {
      indexByKind[item.kind] = groups.length;
      groups.push({ kind: item.kind, items: [] });
    }
    groups[indexByKind[item.kind]].items.push({ item, flatIndex });
  });
  return groups;
}

export const CommandMenu: React.FC<CommandMenuProps> = ({ isOpen, onClose }) => {
  const navigate = useNavigate();
  const [search, setSearch] = useState('');
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [data, setData] = useState<PaletteData | null>(null);
  const [loading, setLoading] = useState(false);
  const activeRef = useRef<HTMLButtonElement>(null);

  // Fetch the live search index once, on first open.
  useEffect(() => {
    if (!isOpen || data) return;
    setLoading(true);
    Promise.allSettled([
      api.getCatalogTree(),
      api.listConnections(),
      api.listNotebooks(),
      api.getDbtTree(),
    ]).then(([nodes, connections, notebooks, dbtTree]) => {
      setData({
        nodes: nodes.status === 'fulfilled' ? nodes.value : [],
        connections: connections.status === 'fulfilled' ? connections.value : [],
        notebooks: notebooks.status === 'fulfilled' ? notebooks.value.notebooks : [],
        dbtFiles: collectSqlFiles(dbtTree.status === 'fulfilled' ? dbtTree.value : null),
      });
      setLoading(false);
    });
  }, [isOpen, data]);

  // Reset the palette state every time it opens.
  useEffect(() => {
    if (isOpen) {
      setSearch('');
      setSelectedIndex(0);
    }
  }, [isOpen]);

  const items = useMemo(() => buildItems(data, navigate), [data, navigate]);
  const filtered = useMemo(() => rank(items, search), [items, search]);

  // Reset selection when the query changes.
  useEffect(() => {
    setSelectedIndex(0);
  }, [search]);

  // Keep the active item visible.
  useEffect(() => {
    activeRef.current?.scrollIntoView({ block: 'nearest' });
  }, [selectedIndex]);

  if (!isOpen) return null;

  const groups = groupItems(filtered);
  const showHeaders = search.trim().length > 0 || groups.length > 1;

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSelectedIndex((i) => Math.min(i + 1, Math.max(0, filtered.length - 1)));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setSelectedIndex((i) => Math.max(i - 1, 0));
    } else if (e.key === 'Home') {
      e.preventDefault();
      setSelectedIndex(0);
    } else if (e.key === 'End') {
      e.preventDefault();
      setSelectedIndex(Math.max(0, filtered.length - 1));
    } else if (e.key === 'Enter') {
      e.preventDefault();
      const item = filtered[selectedIndex];
      if (item) {
        item.run();
        onClose();
      }
    }
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-start justify-center pt-20 bg-slate-900/40 backdrop-blur-xs p-4 animate-in fade-in duration-150"
      onClick={onClose}
    >
      <div
        className="w-full max-w-xl bg-white rounded-xl shadow-2xl border border-slate-200 overflow-hidden flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Search Input Bar */}
        <div className="flex items-center px-4 py-3 border-b border-slate-200 gap-3">
          <Search className="w-4 h-4 text-slate-400 shrink-0" />
          <input
            type="text"
            autoFocus
            placeholder="Search tables, connections, notebooks, models..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onKeyDown={handleKeyDown}
            className="flex-1 text-sm bg-transparent outline-hidden text-slate-800 placeholder:text-slate-400"
          />
          <button
            onClick={onClose}
            aria-label="Close command palette"
            className="text-slate-400 hover:text-slate-600 p-1 rounded-md"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Results List */}
        <div className="max-h-80 overflow-y-auto p-2 space-y-1">
          {loading ? (
            <div className="p-6 flex items-center justify-center gap-2 text-xs text-slate-400 font-mono">
              <Loader2 className="w-4 h-4 animate-spin" />
              Loading search index...
            </div>
          ) : filtered.length === 0 ? (
            <div className="p-6 text-center text-xs text-slate-400 font-mono">
              No matching tables, connections, notebooks, models, or actions.
            </div>
          ) : (
            groups.map((group) => (
              <div key={group.kind}>
                {showHeaders && (
                  <div className="px-2 pt-2 pb-1 text-[10px] font-semibold uppercase tracking-wider text-slate-400">
                    {KIND_HEADERS[group.kind]}
                  </div>
                )}
                {group.items.map(({ item, flatIndex }) => {
                  const Icon = item.icon;
                  const isActive = flatIndex === selectedIndex;
                  return (
                    <button
                      key={item.id}
                      ref={isActive ? activeRef : undefined}
                      onMouseEnter={() => setSelectedIndex(flatIndex)}
                      onClick={() => {
                        item.run();
                        onClose();
                      }}
                      className={`w-full flex items-center justify-between p-2.5 rounded-lg text-left text-sm transition-colors group ${
                        isActive
                          ? 'bg-sky-50 text-sky-900'
                          : 'text-slate-700 hover:bg-slate-100 hover:text-slate-900'
                      }`}
                    >
                      <div className="flex items-center gap-3 min-w-0">
                        <div
                          className={`p-1.5 rounded-md transition-colors shrink-0 ${
                            isActive
                              ? 'bg-sky-100 text-sky-700'
                              : 'bg-slate-100 text-slate-500 group-hover:bg-sky-100 group-hover:text-sky-700'
                          }`}
                        >
                          <Icon className="w-4 h-4" />
                        </div>
                        <div className="min-w-0">
                          <div className="font-medium text-slate-900 text-xs">{item.label}</div>
                          <div className="text-[10px] text-slate-400 font-mono truncate">{item.subtitle}</div>
                        </div>
                      </div>
                      <ArrowRight
                        className={`w-3.5 h-3.5 shrink-0 transition-opacity ${
                          isActive ? 'opacity-100 text-sky-500' : 'opacity-0 group-hover:opacity-100 text-slate-400'
                        }`}
                      />
                    </button>
                  );
                })}
              </div>
            ))
          )}
        </div>

        {/* Footer Navigation Tip */}
        <div className="px-4 py-2 border-t border-slate-100 bg-slate-50 flex items-center justify-between text-[11px] text-slate-400 font-mono">
          <span>Use {MOD_KEY}K to trigger anywhere</span>
          <span>↑↓ to navigate • Enter to select • ESC to close</span>
        </div>
      </div>
    </div>
  );
};

