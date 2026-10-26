import React, { useState } from 'react';
import { tableSizeLabel } from '../../lib/format';
import { toast } from '../../lib/toast';
import {
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
import type { CatalogNode, CatalogTable } from '../../lib/types';
import { LakehouseTree } from './LakehouseTree';
import {
  TREE_ICON,
  TreeGroupLabel,
  TreeMessage,
  TreeRow,
  treeActionClass,
  treeIndent,
} from '../common/Tree';

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
      toast.error(msg);
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
      toast.error(msg);
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
      toast.error(msg);
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
      toast.error(msg);
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
      toast.error(msg);
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
      <div className="shrink-0 space-y-2 border-b border-slate-200 px-3 py-2">
        <div className="flex items-center justify-between">
          <span className="flex items-center gap-1.5 text-xs font-bold text-slate-800">
            <Layers className="h-3.5 w-3.5 text-indigo-600" />
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
      <div role="tree" className="flex-1 overflow-y-auto py-1.5 custom-scrollbar">
        {/* 1. Lakehouse */}
        {lakehouseNode && (
          <div>
            <TreeRow
              variant="section"
              expandable
              expanded={isLakehouseExpanded}
              onClick={() => toggleNode('lakehouse', true)}
              icon={
                isLakehouseExpanded ? (
                  <FolderOpen className={`h-4 w-4 ${TREE_ICON.lakehouse}`} />
                ) : (
                  <Folder className={`h-4 w-4 ${TREE_ICON.lakehouse}`} />
                )
              }
              label={<span className="tracking-wide">Lakehouse</span>}
              badge={totalLakehouseTables}
              actionsVisible
              actions={
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    setIsCreatingDir((prev) => !prev);
                  }}
                  title="New root folder"
                  className={treeActionClass}
                >
                  <FolderPlus className="h-3.5 w-3.5" />
                </button>
              }
            />

            {isCreatingDir && (
              <form
                onSubmit={handleCreateDirectory}
                style={treeIndent(1)}
                className="flex h-9 items-center gap-1.5 pr-2"
              >
                <CornerDownRight className="h-3.5 w-3.5 shrink-0 text-indigo-500" />
                <input
                  type="text"
                  autoFocus
                  placeholder="New root folder (e.g. silver, gold)"
                  value={newDirName}
                  onChange={(event) => setNewDirName(event.target.value)}
                  className="min-w-0 flex-1 rounded border border-indigo-200 bg-white px-2 py-1 font-mono text-[11px] text-slate-800 outline-hidden focus:border-indigo-500"
                />
                <button
                  type="submit"
                  disabled={createDirLoading || !newDirName.trim()}
                  title="Create folder"
                  className="rounded bg-indigo-600 p-1 text-white transition-colors hover:bg-indigo-700 disabled:opacity-50"
                >
                  {createDirLoading ? (
                    <RefreshCw className="h-3 w-3 animate-spin" />
                  ) : (
                    <Check className="h-3 w-3" />
                  )}
                </button>
                <button
                  type="button"
                  onClick={() => setIsCreatingDir(false)}
                  title="Cancel"
                  className="rounded p-1 text-slate-400 transition-colors hover:bg-slate-100 hover:text-slate-600"
                >
                  <X className="h-3 w-3" />
                </button>
              </form>
            )}
            {isCreatingDir && createDirError && (
              <p style={treeIndent(1)} className="pr-2 pb-1 text-[10px] text-rose-600">
                {createDirError}
              </p>
            )}

            {isLakehouseExpanded && (
              <div className="pb-1">
                <LakehouseTree
                  directories={lakehouseNode.directories || []}
                  expandedNodes={expandedNodes}
                  onToggleNode={toggleNode}
                  searchTerm={searchTerm}
                  selectedTableKey={selectedTableKey}
                  onSelectTable={onSelectTable}
                  onOpenInQuery={onOpenInQuery}
                  onOpenInNotebook={onOpenInNotebook}
                  renamingDir={renamingDir}
                  onRenamingDirChange={setRenamingDir}
                  renameDirLoading={renameDirLoading}
                  onRenameDirectory={handleRenameDirectory}
                  creatingSubdirParent={creatingSubdirParent}
                  onCreatingSubdirParentChange={setCreatingSubdirParent}
                  subfolderName={subfolderName}
                  onSubfolderNameChange={setSubfolderName}
                  subfolderLoading={subfolderLoading}
                  onCreateSubdirectory={handleCreateSubdirectory}
                  onDeleteFolder={(folder) => setDeletingItem({ type: 'folder', ...folder })}
                  onRenameTable={(folder, tableName) =>
                    setRenamingTable({ folder, currentName: tableName, newName: tableName })
                  }
                  onMoveTable={(folder, tableName) =>
                    setMovingTable({
                      folder,
                      tableName,
                      targetFolder: availableFolders.find((path) => path !== folder) || '',
                    })
                  }
                  onDeleteTable={(folder, tableName) =>
                    setDeletingItem({ type: 'table', path: folder, name: tableName })
                  }
                />
              </div>
            )}
          </div>
        )}

        {/* 2. External sources */}
        <TreeGroupLabel meta={`${externalNodes.length} connected`}>External Sources</TreeGroupLabel>

        {externalNodes.length === 0 ? (
          <div className="mx-1 rounded-lg border border-dashed border-slate-200 bg-slate-50/60 px-3 py-3 text-center">
            <p className="text-[11px] text-slate-500">No external databases connected.</p>
            <button
              type="button"
              onClick={onOpenAddConnection}
              className="mt-1 inline-flex items-center gap-1 text-[11px] font-medium text-sky-600 hover:text-sky-700"
            >
              <Plus className="h-3 w-3" />
              <span>Connect PostgreSQL, Mongo, MySQL...</span>
            </button>
          </div>
        ) : (
          <div>
            {externalNodes.map((node) => {
              const isExpanded = !!expandedNodes[node.id];
              const isTesting = testingConnId === node.id;
              const testRes = testResults[node.id];
              const filteredConnTables = filterTables(node.tables);
              const isSearching = searchTerm.trim() !== '';

              const schemasMap: Record<string, CatalogTable[]> = {};
              (node.tables || []).forEach((table) => {
                const schema =
                  table.schema_name || (table.name.includes('.') ? table.name.split('.')[0] : 'public');
                if (!schemasMap[schema]) schemasMap[schema] = [];
                schemasMap[schema].push(table);
              });

              const schemaNames = Object.keys(schemasMap).sort((a, b) => {
                if (a === 'public') return -1;
                if (b === 'public') return 1;
                return a.localeCompare(b);
              });

              return (
                <div key={node.id}>
                  <TreeRow
                    variant="section"
                    expandable
                    expanded={isExpanded}
                    onClick={() => toggleNode(node.id, false)}
                    icon={<Server className={`h-4 w-4 ${TREE_ICON.database}`} />}
                    label={node.name}
                    badge={node.tables.length}
                    actionsVisible
                    actions={
                      <>
                        <button
                          type="button"
                          onClick={(event) => handleTestConnection(event, node.id)}
                          disabled={isTesting}
                          title={testRes ? testRes.message : 'Test connection'}
                          className={`${treeActionClass} ${
                            testRes?.status === 'success'
                              ? 'bg-emerald-50 text-emerald-600'
                              : testRes?.status === 'error'
                              ? 'bg-rose-50 text-rose-600'
                              : ''
                          }`}
                        >
                          {isTesting ? (
                            <RefreshCw className="h-3 w-3 animate-spin text-sky-600" />
                          ) : testRes?.status === 'success' ? (
                            <CheckCircle2 className="h-3 w-3 text-emerald-600" />
                          ) : testRes?.status === 'error' ? (
                            <XCircle className="h-3 w-3 text-rose-600" />
                          ) : (
                            <Activity className="h-3 w-3" />
                          )}
                        </button>

                        {onOpenBatchIngest && node.tables && node.tables.length > 0 && (
                          <button
                            type="button"
                            onClick={(event) => {
                              event.stopPropagation();
                              onOpenBatchIngest(node.id, node.name, undefined, node.tables);
                            }}
                            title="Batch ingest database to lakehouse"
                            className={`${treeActionClass} hover:bg-indigo-50 hover:text-indigo-600`}
                          >
                            <ArrowDownToLine className="h-3 w-3" />
                          </button>
                        )}

                        {onEditConnection && (
                          <button
                            type="button"
                            onClick={(event) => {
                              event.stopPropagation();
                              onEditConnection(node.id);
                            }}
                            title="Edit connection"
                            className={treeActionClass}
                          >
                            <Edit2 className="h-3 w-3" />
                          </button>
                        )}

                        {onDeleteConnection && (
                          <button
                            type="button"
                            onClick={(event) => {
                              event.stopPropagation();
                              onDeleteConnection(node.id);
                            }}
                            title="Delete connection"
                            className={`${treeActionClass} hover:bg-rose-50 hover:text-rose-600`}
                          >
                            <Trash2 className="h-3 w-3" />
                          </button>
                        )}
                      </>
                    }
                  />
                  {isExpanded && (
                    <div className="pb-1">
                      {node.tables.length === 0 ? (
                        <TreeMessage depth={0}>No tables discovered.</TreeMessage>
                      ) : filteredConnTables.length === 0 ? (
                        <TreeMessage depth={0}>
                          No tables matching "{searchTerm.trim()}".
                        </TreeMessage>
                      ) : (
                        schemaNames.map((schemaName) => {
                          const schemaTables = schemasMap[schemaName];
                          const filteredSchemaTables = filterTables(schemaTables);
                          if (isSearching && filteredSchemaTables.length === 0) return null;

                          const schemaKey = `${node.id}:schema:${schemaName}`;
                          const isSchemaExpanded =
                            schemaKey in expandedNodes
                              ? !!expandedNodes[schemaKey]
                              : schemaName === 'public' && schemaTables.length <= 10;
                          const shouldDisplaySchema =
                            isSchemaExpanded || (isSearching && filteredSchemaTables.length > 0);

                          return (
                            <React.Fragment key={schemaKey}>
                              <TreeRow
                                depth={0}
                                expandable
                                expanded={shouldDisplaySchema}
                                onClick={() => toggleNode(schemaKey, true)}
                                icon={<Layers className={`h-3.5 w-3.5 ${TREE_ICON.schema}`} />}
                                label={schemaName}
                                labelClassName="font-mono text-[11px] font-semibold"
                                badge={filteredSchemaTables.length}
                                actions={
                                  onOpenBatchIngest && schemaTables.length > 0 ? (
                                    <button
                                      type="button"
                                      onClick={(event) => {
                                        event.stopPropagation();
                                        onOpenBatchIngest(
                                          node.id,
                                          node.name,
                                          schemaName,
                                          schemaTables
                                        );
                                      }}
                                      title={`Batch ingest '${schemaName}' schema to lakehouse`}
                                      className={`${treeActionClass} hover:bg-indigo-50 hover:text-indigo-600`}
                                    >
                                      <ArrowDownToLine className="h-3 w-3" />
                                    </button>
                                  ) : undefined
                                }
                              />
                              {shouldDisplaySchema && (
                                <>
                                  {filteredSchemaTables.map((table) => {
                                    const tableKey = `${node.id}:${table.name}`;
                                    const displayName = table.name.startsWith(`${schemaName}.`)
                                      ? table.name.slice(schemaName.length + 1)
                                      : table.name;
                                    const sourceAlias = node.name
                                      .toLowerCase()
                                      .replace(/[^a-z0-9_]/g, '_');
                                    const externalFqn =
                                      table.fqn || `${sourceAlias}.${schemaName}.${displayName}`;

                                    return (
                                      <TreeRow
                                        key={tableKey}
                                        depth={1}
                                        selected={selectedTableKey === tableKey}
                                        onClick={() =>
                                          onSelectTable(node.id, table, false, node.name)
                                        }
                                        icon={
                                          <FileSpreadsheet
                                            className={`h-3.5 w-3.5 ${TREE_ICON.externalTable}`}
                                          />
                                        }
                                        label={displayName}
                                        labelClassName="font-mono text-[11px]"
                                        meta={tableSizeLabel(table)}
                                        title={table.name}
                                        actions={
                                          <>
                                            {onOpenInQuery && (
                                              <button
                                                type="button"
                                                onClick={(event) => {
                                                  event.stopPropagation();
                                                  onOpenInQuery(
                                                    `-- Query External Table\nSELECT * FROM ${externalFqn} LIMIT 50;\n`
                                                  );
                                                }}
                                                title="Query in Query Studio"
                                                className={`${treeActionClass} hover:bg-sky-50 hover:text-sky-600`}
                                              >
                                                <Terminal className="h-3 w-3" />
                                              </button>
                                            )}
                                            {onOpenInNotebook && (
                                              <button
                                                type="button"
                                                onClick={(event) => {
                                                  event.stopPropagation();
                                                  onOpenInNotebook(table, externalFqn);
                                                }}
                                                title="Analyze in Notebook"
                                                className={`${treeActionClass} hover:bg-amber-50 hover:text-amber-600`}
                                              >
                                                <BookOpen className="h-3 w-3" />
                                              </button>
                                            )}
                                          </>
                                        }
                                      />
                                    );
                                  })}
                                  {filteredSchemaTables.length === 0 && (
                                    <TreeMessage depth={0}>
                                      {isSearching ? 'No matching tables' : 'No tables in this schema'}
                                    </TreeMessage>
                                  )}
                                </>
                              )}
                            </React.Fragment>
                          );
                        })
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
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
