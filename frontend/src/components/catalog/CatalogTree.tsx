import React, { useState } from 'react';
import { tableSizeLabel } from '../../lib/format';
import {
  ChevronRight,
  ChevronDown,
  FileSpreadsheet,
  Plus,
  RefreshCw,
  Search,
  Server,
  Edit2,
  Trash2,
  Activity,
  CheckCircle2,
  XCircle,
  Folder,
  FolderOpen,
  FolderPlus,
  FolderInput,
  Layers,
  Check,
  X,
  ArrowDownToLine,
  AlertTriangle,
  CornerDownRight,
  Terminal,
  BookOpen,
  ChevronsUpDown,
} from 'lucide-react';
import { api } from '../../lib/api';
import type { CatalogNode, CatalogTable, LakehouseDirectory } from '../../lib/types';

interface CatalogTreeProps {
  nodes: CatalogNode[];
  loading: boolean;
  onRefresh: () => void;
  onSelectTable: (sourceId: string, table: CatalogTable, isLakehouse: boolean, sourceName?: string) => void;
  selectedTableKey: string | null;
  onOpenAddConnection: () => void;
  onEditConnection?: (connId: string) => void;
  onDeleteConnection?: (connId: string) => void;
  onOpenBatchIngest?: (connectionId: string, connectionName: string, schemaName?: string, tables?: CatalogTable[]) => void;
  onOpenInQuery?: (sql: string) => void;
  onOpenInNotebook?: (table: CatalogTable, fqn?: string) => void;
}

export const CatalogTree: React.FC<CatalogTreeProps> = ({
  nodes,
  loading,
  onRefresh,
  onSelectTable,
  selectedTableKey,
  onOpenAddConnection,
  onEditConnection,
  onDeleteConnection,
  onOpenBatchIngest,
  onOpenInQuery,
  onOpenInNotebook,
}) => {
  const [expandedNodes, setExpandedNodes] = useState<Record<string, boolean>>({
    lakehouse: true,
  });
  const [searchTerm, setSearchTerm] = useState('');
  const [testingConnId, setTestingConnId] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<
    Record<string, { status: 'success' | 'error'; latency_ms: number; message: string }>
  >({});

  // Top-level Lakehouse directory creation
  const [isCreatingDir, setIsCreatingDir] = useState(false);
  const [newDirName, setNewDirName] = useState('');
  const [createDirLoading, setCreateDirLoading] = useState(false);
  const [createDirError, setCreateDirError] = useState<string | null>(null);

  // Subfolder creation state
  const [creatingSubdirParent, setCreatingSubdirParent] = useState<string | null>(null);
  const [subfolderName, setSubfolderName] = useState('');
  const [subfolderLoading, setSubfolderLoading] = useState(false);

  // Directory rename state
  const [renamingDir, setRenamingDir] = useState<{ path: string; currentName: string; newName: string } | null>(null);
  const [renameDirLoading, setRenameDirLoading] = useState(false);

  // Table rename state
  const [renamingTable, setRenamingTable] = useState<{ folder: string; currentName: string; newName: string } | null>(null);
  const [renameTableLoading, setRenameTableLoading] = useState(false);

  // Table move state
  const [movingTable, setMovingTable] = useState<{ folder: string; tableName: string; targetFolder: string } | null>(null);
  const [moveTableLoading, setMoveTableLoading] = useState(false);

  // Delete confirmation state (folder or table)
  const [deletingItem, setDeletingItem] = useState<{
    type: 'folder' | 'table';
    path: string;
    name: string;
    tableCount?: number;
  } | null>(null);
  const [deleteLoading, setDeleteLoading] = useState(false);

  const toggleNode = (id: string, defaultExpanded: boolean = false) => {
    setExpandedNodes((prev) => {
      const current = id in prev ? !!prev[id] : defaultExpanded;
      return { ...prev, [id]: !current };
    });
  };

  const handleTestConnection = async (e: React.MouseEvent, connId: string) => {
    e.stopPropagation();
    setTestingConnId(connId);
    try {
      const res = await api.testConnection(connId);
      setTestResults((prev) => ({
        ...prev,
        [connId]: { status: res.status, latency_ms: res.latency_ms, message: res.message },
      }));
    } catch (err: unknown) {
      const msg =
        err && typeof err === 'object' && 'response' in err && (err as { response?: { data?: { detail?: string } } }).response?.data?.detail
          ? (err as { response: { data: { detail: string } } }).response.data.detail
          : err instanceof Error
          ? err.message
          : 'Test failed';
      setTestResults((prev) => ({
        ...prev,
        [connId]: { status: 'error', latency_ms: 0, message: msg },
      }));
    } finally {
      setTestingConnId(null);
    }
  };

  // Top-level directory creation
  const handleCreateDirectory = async (e: React.FormEvent) => {
    e.preventDefault();
    const clean = newDirName.trim().toLowerCase();
    if (!clean) return;
    setCreateDirLoading(true);
    setCreateDirError(null);
    try {
      await api.createLakehouseDirectory(clean);
      setNewDirName('');
      setIsCreatingDir(false);
      onRefresh();
      setExpandedNodes((prev) => ({
        ...prev,
        lakehouse: true,
        [`lakehouse:${clean}`]: true,
      }));
    } catch (err: unknown) {
      const msg =
        err && typeof err === 'object' && 'response' in err && (err as { response?: { data?: { detail?: string } } }).response?.data?.detail
          ? (err as { response: { data: { detail: string } } }).response.data.detail
          : err instanceof Error
          ? err.message
          : 'Failed to create directory';
      setCreateDirError(msg);
    } finally {
      setCreateDirLoading(false);
    }
  };

  // Subfolder creation under an existing directory
  const handleCreateSubdirectory = async (e: React.FormEvent, parentPath: string) => {
    e.preventDefault();
    const cleanSub = subfolderName.trim().toLowerCase();
    if (!cleanSub) return;
    setSubfolderLoading(true);
    try {
      const fullPath = `${parentPath}/${cleanSub}`;
      await api.createLakehouseDirectory(fullPath);
      setSubfolderName('');
      setCreatingSubdirParent(null);
      onRefresh();
      setExpandedNodes((prev) => ({
        ...prev,
        [`lakehouse:${parentPath}`]: true,
        [`lakehouse:${fullPath}`]: true,
      }));
    } catch (err: unknown) {
      const msg =
        err && typeof err === 'object' && 'response' in err && (err as { response?: { data?: { detail?: string } } }).response?.data?.detail
          ? (err as { response: { data: { detail: string } } }).response.data.detail
          : err instanceof Error
          ? err.message
          : 'Failed to create subfolder';
      alert(msg);
    } finally {
      setSubfolderLoading(false);
    }
  };

  // Directory Rename
  const handleRenameDirectory = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!renamingDir || !renamingDir.newName.trim()) return;
    setRenameDirLoading(true);
    try {
      await api.renameLakehouseDirectory(renamingDir.path, renamingDir.newName.trim());
      setRenamingDir(null);
      onRefresh();
    } catch (err: unknown) {
      const msg =
        err && typeof err === 'object' && 'response' in err && (err as { response?: { data?: { detail?: string } } }).response?.data?.detail
          ? (err as { response: { data: { detail: string } } }).response.data.detail
          : err instanceof Error
          ? err.message
          : 'Failed to rename directory';
      alert(msg);
    } finally {
      setRenameDirLoading(false);
    }
  };

  // Table Rename
  const handleRenameTable = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!renamingTable || !renamingTable.newName.trim()) return;
    setRenameTableLoading(true);
    try {
      await api.renameLakehouseTable(
        renamingTable.folder,
        renamingTable.currentName,
        renamingTable.newName.trim()
      );
      setRenamingTable(null);
      onRefresh();
    } catch (err: unknown) {
      const msg =
        err && typeof err === 'object' && 'response' in err && (err as { response?: { data?: { detail?: string } } }).response?.data?.detail
          ? (err as { response: { data: { detail: string } } }).response.data.detail
          : err instanceof Error
          ? err.message
          : 'Failed to rename table';
      alert(msg);
    } finally {
      setRenameTableLoading(false);
    }
  };

  // Table Move
  const handleMoveTable = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!movingTable || !movingTable.targetFolder) return;
    setMoveTableLoading(true);
    try {
      await api.moveLakehouseTable(
        movingTable.folder,
        movingTable.targetFolder,
        movingTable.tableName
      );
      setMovingTable(null);
      onRefresh();
    } catch (err: unknown) {
      const msg =
        err && typeof err === 'object' && 'response' in err && (err as { response?: { data?: { detail?: string } } }).response?.data?.detail
          ? (err as { response: { data: { detail: string } } }).response.data.detail
          : err instanceof Error
          ? err.message
          : 'Failed to move table';
      alert(msg);
    } finally {
      setMoveTableLoading(false);
    }
  };

  // Confirm Delete
  const handleConfirmDelete = async () => {
    if (!deletingItem) return;
    setDeleteLoading(true);
    try {
      if (deletingItem.type === 'folder') {
        await api.deleteLakehouseDirectory(deletingItem.path, true);
      } else {
        await api.deleteLakehouseTable(deletingItem.path, deletingItem.name);
      }
      setDeletingItem(null);
      onRefresh();
    } catch (err: unknown) {
      const msg =
        err && typeof err === 'object' && 'response' in err && (err as { response?: { data?: { detail?: string } } }).response?.data?.detail
          ? (err as { response: { data: { detail: string } } }).response.data.detail
          : err instanceof Error
          ? err.message
          : 'Failed to delete';
      alert(msg);
    } finally {
      setDeleteLoading(false);
    }
  };

  // Find Lakehouse node and external connections
  const rawLakehouse = nodes.find((n) => n.id === 'lakehouse' || n.type === 'lakehouse');
  const lakehouseNode = rawLakehouse || (
    nodes.find((n) => n.id === 'bronze')
      ? {
          id: 'lakehouse',
          name: 'Lakehouse',
          type: 'lakehouse',
          directories: [
            {
              id: 'lakehouse:bronze',
              name: 'bronze',
              tier: 'bronze',
              path: 'bronze',
              tables: nodes.find((n) => n.id === 'bronze')?.tables || [],
            },
          ],
          tables: nodes.find((n) => n.id === 'bronze')?.tables || [],
        }
      : undefined
  );

  const externalNodes = nodes.filter(
    (n) => n.id !== 'lakehouse' && n.type !== 'lakehouse' && n.id !== 'bronze'
  );

  // Filter tables by search term
  const filterTables = (tables: CatalogTable[]) =>
    (tables || []).filter((tbl) =>
      tbl.name.toLowerCase().includes(searchTerm.toLowerCase())
    );

  const isLakehouseExpanded = 'lakehouse' in expandedNodes ? !!expandedNodes['lakehouse'] : true;
  const totalLakehouseTables = lakehouseNode?.directories
    ? lakehouseNode.directories.reduce((acc, d) => acc + d.tables.length, 0)
    : lakehouseNode?.tables?.length || 0;

  const availableFolders = (lakehouseNode?.directories || []).map((d) => d.path);

  return (
    <div className="w-80 border-r border-slate-200 bg-white flex flex-col shrink-0 h-full select-none relative">
      {/* Search & Actions Bar */}
      <div className="p-3 border-b border-slate-200 space-y-2 shrink-0">
        <div className="flex items-center justify-between">
          <span className="text-xs font-bold text-slate-700 tracking-wide uppercase font-mono">
            Catalog Tree
          </span>
          <div className="flex items-center gap-1">
            <button
              onClick={() => {
                const isLakehouseOpen = 'lakehouse' in expandedNodes ? !!expandedNodes['lakehouse'] : true;
                const hasExpanded =
                  isLakehouseOpen ||
                  Object.values(expandedNodes).some(Boolean);

                if (hasExpanded) {
                  // Explicitly collapse Lakehouse, all directories, external connections, and schemas
                  const allCollapsed: Record<string, boolean> = { lakehouse: false };
                  if (lakehouseNode?.directories) {
                    lakehouseNode.directories.forEach((d) => {
                      allCollapsed[d.id] = false;
                    });
                  }
                  externalNodes.forEach((n) => {
                    allCollapsed[n.id] = false;
                    (n.tables || []).forEach((t) => {
                      const schema = t.schema_name || (t.name.includes('.') ? t.name.split('.')[0] : 'public');
                      allCollapsed[`${n.id}:schema:${schema}`] = false;
                    });
                  });
                  setExpandedNodes(allCollapsed);
                } else {
                  // Expand all
                  const allExpanded: Record<string, boolean> = { lakehouse: true };
                  if (lakehouseNode?.directories) {
                    lakehouseNode.directories.forEach((d) => {
                      allExpanded[d.id] = true;
                    });
                  }
                  externalNodes.forEach((n) => {
                    allExpanded[n.id] = true;
                    (n.tables || []).forEach((t) => {
                      const schema = t.schema_name || (t.name.includes('.') ? t.name.split('.')[0] : 'public');
                      allExpanded[`${n.id}:schema:${schema}`] = true;
                    });
                  });
                  setExpandedNodes(allExpanded);
                }
              }}
              title="Toggle Expand/Collapse All"
              className="p-1 rounded text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-colors"
            >
              <ChevronsUpDown className="w-3.5 h-3.5" />
            </button>
            <button
              onClick={onRefresh}
              title="Refresh Catalog"
              className="p-1 rounded text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-colors"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
            </button>
            <button
              onClick={onOpenAddConnection}
              className="flex items-center gap-1 px-2 py-1 bg-sky-600 hover:bg-sky-700 text-white rounded text-xs font-medium transition-colors shadow-2xs"
            >
              <Plus className="w-3 h-3" />
              <span>Connect</span>
            </button>
          </div>
        </div>

        {/* Filter input */}
        <div className="relative">
          <Search className="w-3.5 h-3.5 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2" />
          <input
            type="text"
            placeholder="Filter tables..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full text-xs pl-8 pr-3 py-1.5 bg-slate-50 border border-slate-200 rounded-md focus:bg-white focus:border-indigo-500 outline-hidden transition-all text-slate-800"
          />
        </div>
      </div>

      {/* Main Hierarchy Tree */}
      <div className="flex-1 overflow-y-auto p-2 space-y-3 custom-scrollbar">
        {/* 1. Lakehouse Medallion Node */}
        {lakehouseNode && (
          <div>
            <div className="border border-slate-200 rounded-lg bg-slate-50/50 overflow-hidden">
              {/* Lakehouse Root Header */}
              <div
                onClick={() => toggleNode('lakehouse', true)}
                className="w-full flex items-center justify-between px-3 py-2 text-left bg-slate-100/70 hover:bg-slate-200/50 transition-colors cursor-pointer group"
              >
                <div className="flex items-center gap-2 text-xs font-bold text-slate-800 truncate">
                  {isLakehouseExpanded ? (
                    <ChevronDown className="w-3.5 h-3.5 text-slate-500 shrink-0" />
                  ) : (
                    <ChevronRight className="w-3.5 h-3.5 text-slate-400 shrink-0" />
                  )}
                  {isLakehouseExpanded ? (
                    <FolderOpen className="w-4 h-4 text-indigo-600 shrink-0" />
                  ) : (
                    <Folder className="w-4 h-4 text-indigo-600 shrink-0" />
                  )}
                  <span className="tracking-wide">Lakehouse</span>
                </div>

                <div className="flex items-center gap-1.5 shrink-0" onClick={(e) => e.stopPropagation()}>
                  <button
                    onClick={() => setIsCreatingDir((prev) => !prev)}
                    title="New Root Folder"
                    className="p-1 rounded text-slate-400 hover:text-indigo-700 hover:bg-indigo-50 transition-colors"
                  >
                    <FolderPlus className="w-3.5 h-3.5" />
                  </button>
                  <span className="min-w-[20px] text-center text-[10px] font-mono font-medium px-1.5 py-0.5 rounded bg-white border border-slate-200 text-slate-600">
                    {totalLakehouseTables}
                  </span>
                </div>
              </div>

              {/* Inline Top-Level Directory Creation Form */}
              {isCreatingDir && (
                <form
                  onSubmit={handleCreateDirectory}
                  className="p-2 bg-slate-50 border-b border-slate-200 flex flex-col gap-1.5 animate-in fade-in duration-100"
                >
                  <div className="flex items-center justify-between text-[11px] font-semibold text-slate-800">
                    <span>Create Lakehouse Directory</span>
                    <button
                      type="button"
                      onClick={() => setIsCreatingDir(false)}
                      className="text-slate-500 hover:text-slate-800"
                    >
                      <X className="w-3 h-3" />
                    </button>
                  </div>
                  <div className="flex items-center gap-1">
                    <input
                      type="text"
                      autoFocus
                      placeholder="e.g. silver, gold, raw..."
                      value={newDirName}
                      onChange={(e) => setNewDirName(e.target.value)}
                      className="flex-1 text-xs bg-white border border-slate-300 rounded px-2 py-1 outline-hidden focus:border-indigo-500 font-mono text-slate-800"
                    />
                    <button
                      type="submit"
                      disabled={createDirLoading || !newDirName.trim()}
                      className="p-1 bg-indigo-600 hover:bg-indigo-700 text-white rounded disabled:opacity-50 transition-colors"
                    >
                      {createDirLoading ? (
                        <RefreshCw className="w-3 h-3 animate-spin" />
                      ) : (
                        <Check className="w-3 h-3" />
                      )}
                    </button>
                  </div>
                  {createDirError && (
                    <p className="text-[10px] text-red-600 font-mono">{createDirError}</p>
                  )}
                </form>
              )}

              {/* Lakehouse Directory Hierarchy (supports nested subdirectories) */}
              {isLakehouseExpanded && (
                <div className="p-1.5 space-y-1 bg-white/60">
                  {!lakehouseNode.directories || lakehouseNode.directories.length === 0 ? (
                    <div className="py-2 px-6 text-[11px] text-slate-400 font-mono">
                      No directories found in Lakehouse.
                    </div>
                  ) : (
                    lakehouseNode.directories.map((dir: LakehouseDirectory) => {
                      const defaultDirExpanded = dir.tables.length > 0;
                      const isDirExpanded =
                        dir.id in expandedNodes ? !!expandedNodes[dir.id] : defaultDirExpanded;
                      const filteredDirTables = filterTables(dir.tables);
                      const isRenamingThisDir = renamingDir?.path === dir.path;
                      const isCreatingSubdir = creatingSubdirParent === dir.path;

                      // Indentation for nested subfolders
                      const indentPadding = Math.min((dir.depth || 0) * 12, 36);

                      return (
                        <div
                          key={dir.id}
                          style={{ marginLeft: `${indentPadding}px` }}
                          className="border border-slate-200/80 rounded-md overflow-hidden bg-slate-50/40"
                        >
                          {/* Directory Header */}
                          <div
                            onClick={() => toggleNode(dir.id, defaultDirExpanded)}
                            className="w-full flex items-center justify-between px-2.5 py-1.5 text-left hover:bg-slate-100 transition-colors cursor-pointer group"
                          >
                            <div className="flex items-center gap-2 text-xs font-semibold text-slate-700 truncate flex-1">
                              {isDirExpanded ? (
                                <ChevronDown className="w-3 h-3 text-slate-400 shrink-0" />
                              ) : (
                                <ChevronRight className="w-3 h-3 text-slate-400 shrink-0" />
                              )}
                              <Folder className="w-3.5 h-3.5 shrink-0 text-slate-500" />

                              {isRenamingThisDir ? (
                                <form
                                  onSubmit={handleRenameDirectory}
                                  onClick={(e) => e.stopPropagation()}
                                  className="flex items-center gap-1 flex-1"
                                >
                                  <input
                                    type="text"
                                    autoFocus
                                    value={renamingDir.newName}
                                    onChange={(e) =>
                                      setRenamingDir({ ...renamingDir, newName: e.target.value })
                                    }
                                    className="text-xs bg-white border border-indigo-400 rounded px-1.5 py-0.5 outline-hidden font-mono text-slate-900 w-28"
                                  />
                                  <button
                                    type="submit"
                                    disabled={renameDirLoading}
                                    className="p-0.5 text-emerald-600 hover:bg-emerald-50 rounded"
                                  >
                                    <Check className="w-3 h-3" />
                                  </button>
                                  <button
                                    type="button"
                                    onClick={() => setRenamingDir(null)}
                                    className="p-0.5 text-slate-400 hover:text-slate-600"
                                  >
                                    <X className="w-3 h-3" />
                                  </button>
                                </form>
                              ) : (
                                <span className="font-mono text-[11px] font-bold text-slate-800 truncate">
                                  {dir.name}
                                </span>
                              )}
                            </div>

                            {/* Governed Folder Actions */}
                            {!isRenamingThisDir && (
                              <div
                                className="flex items-center gap-1 shrink-0"
                                onClick={(e) => e.stopPropagation()}
                              >
                                {/* Add Subfolder */}
                                <button
                                  onClick={() => {
                                    setCreatingSubdirParent(
                                      creatingSubdirParent === dir.path ? null : dir.path
                                    );
                                    setSubfolderName('');
                                  }}
                                  title={`Add subfolder inside '${dir.path}'`}
                                  className="opacity-0 group-hover:opacity-100 p-0.5 rounded text-slate-400 hover:text-indigo-600 hover:bg-indigo-50 transition-all"
                                >
                                  <FolderPlus className="w-3 h-3" />
                                </button>

                                {/* Rename Folder */}
                                <button
                                  onClick={() =>
                                    setRenamingDir({
                                      path: dir.path,
                                      currentName: dir.name,
                                      newName: dir.name,
                                    })
                                  }
                                  title={`Rename folder '${dir.name}'`}
                                  className="opacity-0 group-hover:opacity-100 p-0.5 rounded text-slate-400 hover:text-slate-700 hover:bg-slate-200 transition-all"
                                >
                                  <Edit2 className="w-3 h-3" />
                                </button>

                                {/* Delete Folder */}
                                <button
                                  onClick={() =>
                                    setDeletingItem({
                                      type: 'folder',
                                      path: dir.path,
                                      name: dir.name,
                                      tableCount: dir.tables.length,
                                    })
                                  }
                                  title={`Delete folder '${dir.path}'`}
                                  className="opacity-0 group-hover:opacity-100 p-0.5 rounded text-slate-400 hover:text-rose-600 hover:bg-rose-50 transition-all"
                                >
                                  <Trash2 className="w-3 h-3" />
                                </button>

                                <span className="text-[9px] font-mono px-1.5 py-0.5 rounded font-semibold border bg-slate-100 text-slate-700 border-slate-200">
                                  {filteredDirTables.length}
                                </span>
                              </div>
                            )}
                          </div>

                          {/* Inline Subfolder Creation Form */}
                          {isCreatingSubdir && (
                            <form
                              onSubmit={(e) => handleCreateSubdirectory(e, dir.path)}
                              className="p-2 bg-indigo-50/50 border-t border-indigo-100 flex items-center gap-1.5 animate-in fade-in duration-100"
                            >
                              <CornerDownRight className="w-3.5 h-3.5 text-indigo-500 shrink-0" />
                              <input
                                type="text"
                                autoFocus
                                placeholder={`Subfolder under ${dir.name}...`}
                                value={subfolderName}
                                onChange={(e) => setSubfolderName(e.target.value)}
                                className="flex-1 text-xs bg-white border border-indigo-200 rounded px-2 py-1 outline-hidden focus:border-indigo-500 font-mono text-slate-800"
                              />
                              <button
                                type="submit"
                                disabled={subfolderLoading || !subfolderName.trim()}
                                className="p-1 bg-indigo-600 hover:bg-indigo-700 text-white rounded transition-colors disabled:opacity-50"
                              >
                                {subfolderLoading ? (
                                  <RefreshCw className="w-3 h-3 animate-spin" />
                                ) : (
                                  <Check className="w-3 h-3" />
                                )}
                              </button>
                              <button
                                type="button"
                                onClick={() => setCreatingSubdirParent(null)}
                                className="p-1 text-slate-400 hover:text-slate-600"
                              >
                                <X className="w-3 h-3" />
                              </button>
                            </form>
                          )}

                          {/* Tables in Directory */}
                          {isDirExpanded && (
                            <div className="p-1 space-y-0.5 bg-white border-t border-slate-100">
                              {filteredDirTables.length === 0 ? (
                                <div className="py-1.5 px-6 text-[11px] text-slate-400 font-mono">
                                  {searchTerm ? 'No matching tables' : 'No tables in this directory'}
                                </div>
                              ) : (
                                filteredDirTables.map((table) => {
                                  const tableKey = `${dir.id}:${table.name}`;
                                  const isSelected = selectedTableKey === tableKey;

                                  return (
                                    <div
                                      key={table.name}
                                      className={`group/table flex items-center justify-between px-2.5 py-1 rounded text-xs transition-all ${
                                        isSelected
                                          ? 'bg-indigo-50 text-indigo-950 font-semibold border border-indigo-200'
                                          : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
                                      }`}
                                    >
                                      <button
                                        onClick={() => onSelectTable(dir.id, table, true, dir.name)}
                                        className="flex items-center gap-1.5 truncate flex-1 text-left"
                                      >
                                        <FileSpreadsheet className="w-3.5 h-3.5 shrink-0 text-slate-500" />
                                        <span className="truncate">{table.name}</span>
                                      </button>

                                      <div className="flex items-center gap-1 shrink-0">
                                        {/* Hover Actions for Lakehouse Table */}
                                        <div className="opacity-0 group-hover/table:opacity-100 flex items-center gap-0.5 transition-opacity">
                                          {/* Quick Query Table */}
                                          {onOpenInQuery && (
                                            <button
                                              onClick={(e) => {
                                                e.stopPropagation();
                                                const cleanName = table.name.replace(/\.parquet$/i, '');
                                                const lakehouseFqn = table.fqn || `lakehouse.${dir.path.replace(/\//g, '.')}.${cleanName}`;
                                                const sql = `-- Query Lakehouse Table\nSELECT * FROM ${lakehouseFqn} LIMIT 50;\n`;
                                                onOpenInQuery(sql);
                                              }}
                                              title="Query in Query Studio"
                                              className="p-1 text-slate-400 hover:text-sky-600 hover:bg-sky-50 rounded transition-colors"
                                            >
                                              <Terminal className="w-2.5 h-2.5" />
                                            </button>
                                          )}

                                          {/* Quick Analyze in Notebook */}
                                          {onOpenInNotebook && (
                                            <button
                                              onClick={(e) => {
                                                e.stopPropagation();
                                                const cleanName = table.name.replace(/\.parquet$/i, '');
                                                const lakehouseFqn = table.fqn || `lakehouse.${dir.path.replace(/\//g, '.')}.${cleanName}`;
                                                onOpenInNotebook(table, lakehouseFqn);
                                              }}
                                              title="Analyze in Notebook"
                                              className="p-1 text-slate-400 hover:text-amber-600 hover:bg-amber-50 rounded transition-colors"
                                            >
                                              <BookOpen className="w-2.5 h-2.5" />
                                            </button>
                                          )}
                                          {/* Rename Table */}
                                          <button
                                            onClick={(e) => {
                                              e.stopPropagation();
                                              setRenamingTable({
                                                folder: dir.path,
                                                currentName: table.name,
                                                newName: table.name,
                                              });
                                            }}
                                            title={`Rename table '${table.name}'`}
                                            className="p-1 text-slate-400 hover:text-slate-700 hover:bg-slate-200 rounded transition-colors"
                                          >
                                            <Edit2 className="w-2.5 h-2.5" />
                                          </button>

                                          {/* Move Table */}
                                          <button
                                            onClick={(e) => {
                                              e.stopPropagation();
                                              setMovingTable({
                                                folder: dir.path,
                                                tableName: table.name,
                                                targetFolder:
                                                  availableFolders.find((f) => f !== dir.path) ||
                                                  'bronze',
                                              });
                                            }}
                                            title={`Move table '${table.name}' to another folder`}
                                            className="p-1 text-slate-400 hover:text-indigo-600 hover:bg-indigo-50 rounded transition-colors"
                                          >
                                            <FolderInput className="w-2.5 h-2.5" />
                                          </button>

                                          {/* Delete Table */}
                                          <button
                                            onClick={(e) => {
                                              e.stopPropagation();
                                              setDeletingItem({
                                                type: 'table',
                                                path: dir.path,
                                                name: table.name,
                                              });
                                            }}
                                            title={`Delete table '${table.name}'`}
                                            className="p-1 text-slate-400 hover:text-rose-600 hover:bg-rose-50 rounded transition-colors"
                                          >
                                            <Trash2 className="w-2.5 h-2.5" />
                                          </button>
                                        </div>

                                        {tableSizeLabel(table) != null && (
                                          <span className="text-[10px] font-mono text-slate-400 shrink-0">
                                            {tableSizeLabel(table)}
                                          </span>
                                        )}
                                      </div>
                                    </div>
                                  );
                                })
                              )}
                            </div>
                          )}
                        </div>
                      );
                    })
                  )}
                </div>
              )}
            </div>
          </div>
        )}

        {/* 2. External Sources Section */}
        <div className="pt-2">
          <div className="px-1 pb-1 flex items-center justify-between">
            <span className="text-[11px] font-bold text-slate-500 uppercase font-mono tracking-wider">
              External Sources
            </span>
            <span className="text-[10px] font-mono text-slate-400">
              {externalNodes.length} connected
            </span>
          </div>

          {externalNodes.length === 0 ? (
            <div className="p-4 text-center text-xs text-slate-400 border border-dashed border-slate-200 rounded-lg bg-slate-50/50">
              <p>No external databases connected.</p>
              <button
                onClick={onOpenAddConnection}
                className="mt-1.5 text-xs text-sky-600 hover:text-sky-700 font-medium inline-flex items-center gap-1"
              >
                <Plus className="w-3 h-3" />
                <span>Connect PostgreSQL, Mongo, MySQL...</span>
              </button>
            </div>
          ) : (
            <div className="space-y-2">
              {externalNodes.map((node) => {
                const isExpanded = !!expandedNodes[node.id];
                const isTesting = testingConnId === node.id;
                const testRes = testResults[node.id];
                const filteredConnTables = filterTables(node.tables);

                return (
                  <div
                    key={node.id}
                    className="border border-slate-200 rounded-lg bg-slate-50/50 overflow-hidden"
                  >
                    {/* Node Header */}
                    <div
                      onClick={() => toggleNode(node.id, false)}
                      className="w-full flex items-center justify-between px-3 py-2 text-left bg-slate-100/70 hover:bg-slate-200/50 transition-colors cursor-pointer group"
                    >
                      <div className="flex items-center gap-2 text-xs font-semibold text-slate-800 truncate">
                        {isExpanded ? (
                          <ChevronDown className="w-3.5 h-3.5 text-slate-400 shrink-0" />
                        ) : (
                          <ChevronRight className="w-3.5 h-3.5 text-slate-400 shrink-0" />
                        )}
                        <Server className="w-3.5 h-3.5 text-sky-600 shrink-0" />
                        <span className="truncate">{node.name}</span>
                      </div>

                      <div
                        className="flex items-center gap-1.5 shrink-0"
                        onClick={(e) => e.stopPropagation()}
                      >
                        <div className="flex items-center gap-1">
                          {/* Test Button */}
                          <button
                            onClick={(e) => handleTestConnection(e, node.id)}
                            disabled={isTesting}
                            title={testRes ? testRes.message : 'Test Connection'}
                            className={`p-1 rounded text-xs transition-colors ${
                              testRes?.status === 'success'
                                ? 'text-emerald-600 bg-emerald-50'
                                : testRes?.status === 'error'
                                ? 'text-red-600 bg-red-50'
                                : 'text-slate-400 hover:text-slate-700 hover:bg-slate-200'
                            }`}
                          >
                            {isTesting ? (
                              <RefreshCw className="w-3 h-3 animate-spin text-sky-600" />
                            ) : testRes?.status === 'success' ? (
                              <CheckCircle2 className="w-3 h-3 text-emerald-600" />
                            ) : testRes?.status === 'error' ? (
                              <XCircle className="w-3 h-3 text-red-600" />
                            ) : (
                              <Activity className="w-3 h-3" />
                            )}
                          </button>

                          {/* Batch Ingest DB Button */}
                          {onOpenBatchIngest && node.tables && node.tables.length > 0 && (
                            <button
                              onClick={(e) => {
                                e.stopPropagation();
                                onOpenBatchIngest(node.id, node.name, undefined, node.tables);
                              }}
                              title="Batch Ingest Database to Lakehouse"
                              className="p-1 rounded text-slate-400 hover:text-indigo-600 hover:bg-indigo-50 transition-colors"
                            >
                              <ArrowDownToLine className="w-3 h-3" />
                            </button>
                          )}

                          {/* Edit Button */}
                          {onEditConnection && (
                            <button
                              onClick={(e) => {
                                e.stopPropagation();
                                onEditConnection(node.id);
                              }}
                              title="Edit Connection"
                              className="p-1 rounded text-slate-400 hover:text-slate-700 hover:bg-slate-200 transition-colors"
                            >
                              <Edit2 className="w-3 h-3" />
                            </button>
                          )}

                          {/* Delete Button */}
                          {onDeleteConnection && (
                            <button
                              onClick={(e) => {
                                e.stopPropagation();
                                onDeleteConnection(node.id);
                              }}
                              title="Delete Connection"
                              className="p-1 rounded text-slate-400 hover:text-red-600 hover:bg-red-50 transition-colors"
                            >
                              <Trash2 className="w-3 h-3" />
                            </button>
                          )}
                        </div>

                        <span className="min-w-[20px] text-center text-[10px] font-mono font-medium px-1.5 py-0.5 rounded bg-white border border-slate-200 text-slate-600">
                          {node.tables.length}
                        </span>
                      </div>
                    </div>

                    {/* Schemas & Tables Hierarchy */}
                    {isExpanded && (() => {
                      const schemasMap: Record<string, CatalogTable[]> = {};
                      (node.tables || []).forEach((table) => {
                        const schema =
                          table.schema_name ||
                          (table.name.includes('.') ? table.name.split('.')[0] : 'public');
                        if (!schemasMap[schema]) {
                          schemasMap[schema] = [];
                        }
                        schemasMap[schema].push(table);
                      });

                      const schemaNames = Object.keys(schemasMap).sort((a, b) => {
                        if (a === 'public') return -1;
                        if (b === 'public') return 1;
                        return a.localeCompare(b);
                      });

                      return (
                        <div className="p-1.5 space-y-1 bg-white/60">
                          {node.tables.length === 0 ? (
                            <div className="py-2 px-6 text-[11px] text-slate-400 font-mono">
                              No tables discovered.
                            </div>
                          ) : filteredConnTables.length === 0 ? (
                            <div className="py-2 px-6 text-[11px] text-slate-400 font-mono">
                              No tables matching "{searchTerm}".
                            </div>
                          ) : (
                            schemaNames.map((schemaName) => {
                              const schemaTables = schemasMap[schemaName];
                              const filteredSchemaTables = filterTables(schemaTables);

                              if (searchTerm.trim() !== '' && filteredSchemaTables.length === 0) {
                                return null;
                              }

                              const schemaKey = `${node.id}:schema:${schemaName}`;
                              const isSchemaExpanded =
                                schemaKey in expandedNodes
                                  ? !!expandedNodes[schemaKey]
                                  : (schemaName === 'public' && schemaTables.length <= 10);
                              const shouldDisplaySchema =
                                isSchemaExpanded ||
                                (searchTerm.trim() !== '' && filteredSchemaTables.length > 0);

                              return (
                                <div
                                  key={schemaKey}
                                  className="border border-slate-200/80 rounded-md overflow-hidden bg-slate-50/40"
                                >
                                  {/* Schema Header */}
                                  <div
                                    onClick={() => toggleNode(schemaKey, true)}
                                    className="w-full flex items-center justify-between px-2.5 py-1.5 text-left hover:bg-slate-100 transition-colors cursor-pointer group"
                                  >
                                    <div className="flex items-center gap-2 text-xs font-semibold text-slate-700 truncate">
                                      {shouldDisplaySchema ? (
                                        <ChevronDown className="w-3 h-3 text-slate-400 shrink-0" />
                                      ) : (
                                        <ChevronRight className="w-3 h-3 text-slate-400 shrink-0" />
                                      )}
                                      <Layers className="w-3.5 h-3.5 text-sky-600 shrink-0" />
                                      <span className="font-mono text-[11px] font-bold text-slate-800 truncate">
                                        {schemaName}
                                      </span>
                                    </div>

                                    <div
                                      className="flex items-center gap-1.5 shrink-0"
                                      onClick={(e) => e.stopPropagation()}
                                    >
                                      {onOpenBatchIngest && schemaTables && schemaTables.length > 0 && (
                                        <button
                                          onClick={(e) => {
                                            e.stopPropagation();
                                            onOpenBatchIngest(node.id, node.name, schemaName, schemaTables);
                                          }}
                                          title={`Batch Ingest '${schemaName}' Schema to Lakehouse`}
                                          className="p-1 rounded text-slate-400 hover:text-indigo-600 hover:bg-indigo-50 transition-colors"
                                        >
                                          <ArrowDownToLine className="w-3 h-3" />
                                        </button>
                                      )}
                                      <span className="text-[9px] font-mono px-1.5 py-0.5 rounded font-semibold border bg-sky-50 text-sky-700 border-sky-200">
                                        {filteredSchemaTables.length}
                                      </span>
                                    </div>
                                  </div>

                                  {/* Tables in Schema */}
                                  {shouldDisplaySchema && (
                                    <div className="p-1 space-y-0.5 bg-white border-t border-slate-100">
                                      {filteredSchemaTables.length === 0 ? (
                                        <div className="py-1.5 px-6 text-[11px] text-slate-400 font-mono">
                                          {searchTerm
                                            ? 'No matching tables'
                                            : 'No tables in this schema'}
                                        </div>
                                      ) : (
                                        filteredSchemaTables.map((table) => {
                                          const tableKey = `${node.id}:${table.name}`;
                                          const isSelected = selectedTableKey === tableKey;
                                          const displayName = table.name.startsWith(
                                            `${schemaName}.`
                                          )
                                            ? table.name.slice(schemaName.length + 1)
                                            : table.name;

                                          return (
                                            <button
                                              key={table.name}
                                              onClick={() => onSelectTable(node.id, table, false, node.name)}
                                              className={`w-full flex items-center justify-between px-2.5 py-1 rounded text-xs text-left transition-all group ${
                                                isSelected
                                                  ? 'bg-sky-100/90 text-sky-950 font-semibold border border-sky-300'
                                                  : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
                                              }`}
                                            >
                                              <div className="flex items-center gap-1.5 truncate">
                                                <FileSpreadsheet className="w-3.5 h-3.5 shrink-0 text-sky-500" />
                                                <span className="truncate">{displayName}</span>
                                              </div>
                                              <div className="flex items-center gap-1 shrink-0">
                                                <div className="opacity-0 group-hover:opacity-100 flex items-center gap-0.5 transition-opacity">
                                                  {onOpenInQuery && (
                                                    <span
                                                      onClick={(e) => {
                                                        e.stopPropagation();
                                                        const sourceAlias = node.name.toLowerCase().replace(/[^a-z0-9_]/g, '_');
                                                        const externalFqn = table.fqn || `${sourceAlias}.${schemaName}.${displayName}`;
                                                        const sql = `-- Query External Table\nSELECT * FROM ${externalFqn} LIMIT 50;\n`;
                                                        onOpenInQuery(sql);
                                                      }}
                                                      title="Query in Query Studio"
                                                      className="p-1 text-slate-400 hover:text-sky-600 hover:bg-sky-50 rounded transition-colors cursor-pointer"
                                                    >
                                                      <Terminal className="w-2.5 h-2.5" />
                                                    </span>
                                                  )}
                                                  {onOpenInNotebook && (
                                                    <span
                                                      onClick={(e) => {
                                                        e.stopPropagation();
                                                        const sourceAlias = node.name.toLowerCase().replace(/[^a-z0-9_]/g, '_');
                                                        const externalFqn = table.fqn || `${sourceAlias}.${schemaName}.${displayName}`;
                                                        onOpenInNotebook(table, externalFqn);
                                                      }}
                                                      title="Analyze in Notebook"
                                                      className="p-1 text-slate-400 hover:text-amber-600 hover:bg-amber-50 rounded transition-colors cursor-pointer"
                                                    >
                                                      <BookOpen className="w-2.5 h-2.5" />
                                                    </span>
                                                  )}
                                                </div>
                                                {tableSizeLabel(table) != null && (
                                                  <span className="text-[10px] font-mono text-slate-400 shrink-0">
                                                    {tableSizeLabel(table)}
                                                  </span>
                                                )}
                                              </div>
                                            </button>
                                          );
                                        })
                                      )}
                                    </div>
                                  )}
                                </div>
                              );
                            })
                          )}
                        </div>
                      );
                    })()}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {/* --- Safe Action Modals --- */}

      {/* 1. Delete Confirmation Modal (Folder or Table) */}
      {deletingItem && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 backdrop-blur-xs p-4 animate-in fade-in duration-150">
          <div className="w-full max-w-sm bg-white rounded-xl shadow-2xl border border-slate-200 overflow-hidden p-5 space-y-4">
            <div className="flex items-start gap-3">
              <div className="p-2 bg-rose-100 text-rose-600 rounded-lg shrink-0">
                <AlertTriangle className="w-5 h-5" />
              </div>
              <div>
                <h3 className="text-sm font-bold text-slate-900">
                  Delete Lakehouse {deletingItem.type === 'folder' ? 'Folder' : 'Table'}?
                </h3>
                <p className="text-xs text-slate-600 mt-1 font-mono">
                  {deletingItem.type === 'folder'
                    ? `data/lakehouse/${deletingItem.path}`
                    : `data/lakehouse/${deletingItem.path}/${deletingItem.name}.parquet`}
                </p>
                {deletingItem.type === 'folder' && (deletingItem.tableCount || 0) > 0 && (
                  <p className="text-xs text-rose-600 font-semibold mt-2">
                    ⚠️ This folder contains {deletingItem.tableCount} Parquet table(s). All files and catalog records will be permanently removed.
                  </p>
                )}
              </div>
            </div>

            <div className="flex items-center justify-end gap-2 pt-2 border-t border-slate-100">
              <button
                type="button"
                onClick={() => setDeletingItem(null)}
                className="px-3 py-1.5 text-xs text-slate-600 hover:bg-slate-100 rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleConfirmDelete}
                disabled={deleteLoading}
                className="px-3.5 py-1.5 text-xs font-semibold bg-rose-600 hover:bg-rose-700 text-white rounded-lg transition-colors flex items-center gap-1.5 disabled:opacity-50"
              >
                {deleteLoading && <RefreshCw className="w-3 h-3 animate-spin" />}
                <span>Delete {deletingItem.type === 'folder' ? 'Folder' : 'Table'}</span>
              </button>
            </div>
          </div>
        </div>
      )}

      {/* 2. Table Rename Modal */}
      {renamingTable && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 backdrop-blur-xs p-4 animate-in fade-in duration-150">
          <form
            onSubmit={handleRenameTable}
            className="w-full max-w-sm bg-white rounded-xl shadow-2xl border border-slate-200 overflow-hidden p-5 space-y-4"
          >
            <div>
              <h3 className="text-sm font-bold text-slate-900">Rename Lakehouse Table</h3>
              <p className="text-xs text-slate-500 font-mono mt-0.5">
                Folder: data/lakehouse/{renamingTable.folder}/
              </p>
            </div>

            <div>
              <label className="block text-xs font-semibold text-slate-700 mb-1">
                New Table Name
              </label>
              <div className="flex items-center">
                <input
                  type="text"
                  autoFocus
                  value={renamingTable.newName}
                  onChange={(e) =>
                    setRenamingTable({ ...renamingTable, newName: e.target.value })
                  }
                  className="w-full text-xs bg-slate-50 border border-slate-300 rounded-l-lg px-3 py-2 font-mono text-slate-800 outline-hidden focus:border-indigo-500 focus:bg-white"
                />
                <span className="bg-slate-100 border border-l-0 border-slate-300 text-slate-500 text-xs px-2.5 py-2 rounded-r-lg font-mono">
                  .parquet
                </span>
              </div>
            </div>

            <div className="flex items-center justify-end gap-2 pt-2 border-t border-slate-100">
              <button
                type="button"
                onClick={() => setRenamingTable(null)}
                className="px-3 py-1.5 text-xs text-slate-600 hover:bg-slate-100 rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={renameTableLoading || !renamingTable.newName.trim()}
                className="px-3.5 py-1.5 text-xs font-semibold bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg transition-colors flex items-center gap-1.5 disabled:opacity-50"
              >
                {renameTableLoading && <RefreshCw className="w-3 h-3 animate-spin" />}
                <span>Save New Name</span>
              </button>
            </div>
          </form>
        </div>
      )}

      {/* 3. Table Move Modal */}
      {movingTable && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 backdrop-blur-xs p-4 animate-in fade-in duration-150">
          <form
            onSubmit={handleMoveTable}
            className="w-full max-w-sm bg-white rounded-xl shadow-2xl border border-slate-200 overflow-hidden p-5 space-y-4"
          >
            <div>
              <h3 className="text-sm font-bold text-slate-900">Move Lakehouse Table</h3>
              <p className="text-xs text-slate-500 font-mono mt-0.5">
                Moving <span className="font-bold text-slate-800">{movingTable.tableName}.parquet</span> from <span className="font-bold text-slate-800">{movingTable.folder}</span>
              </p>
            </div>

            <div>
              <label className="block text-xs font-semibold text-slate-700 mb-1">
                Destination Folder
              </label>
              <select
                value={movingTable.targetFolder}
                onChange={(e) =>
                  setMovingTable({ ...movingTable, targetFolder: e.target.value })
                }
                className="w-full text-xs bg-slate-50 border border-slate-300 rounded-lg px-3 py-2 font-mono text-slate-800 outline-hidden focus:border-indigo-500 focus:bg-white"
              >
                {availableFolders.map((f) => (
                  <option key={f} value={f} disabled={f === movingTable.folder}>
                    {f} {f === movingTable.folder ? '(current)' : ''}
                  </option>
                ))}
              </select>
            </div>

            <div className="flex items-center justify-end gap-2 pt-2 border-t border-slate-100">
              <button
                type="button"
                onClick={() => setMovingTable(null)}
                className="px-3 py-1.5 text-xs text-slate-600 hover:bg-slate-100 rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={moveTableLoading || movingTable.targetFolder === movingTable.folder}
                className="px-3.5 py-1.5 text-xs font-semibold bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg transition-colors flex items-center gap-1.5 disabled:opacity-50"
              >
                {moveTableLoading && <RefreshCw className="w-3 h-3 animate-spin" />}
                <span>Move Table</span>
              </button>
            </div>
          </form>
        </div>
      )}
    </div>
  );
};
