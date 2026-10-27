import React, { useState, useEffect, useRef, useCallback } from 'react';
import Editor from '@monaco-editor/react';
import {
  FileCode,
  FileText,
  Play,
  CheckCircle2,
  Columns2,
  Split,
  Save,
  Zap,
  Terminal,
  Database,
  Table2,
  X,
  AlertCircle,
  ChevronDown,
  ChevronUp,
  Boxes,
  Maximize2,
  Minimize2,
} from 'lucide-react';

import { api } from '../../lib/api';
import type { DbtTarget } from '../../lib/types';
import type {
  DbtTreeNode,
  DbtPreviewResponse,
  DbtLineageResponse,
} from '../../lib/types';
import { DbtProjectTree } from './DbtProjectTree';
import { DbtLineageGraph } from './DbtLineageGraph';
import { NewModelModal } from './NewModelModal';
import { NewYamlModal } from './NewYamlModal';
import { DataGrid } from '../query/DataGrid';
import { ConfirmDialog } from '../common/ConfirmDialog';
import type { ConfirmRequest } from '../common/ConfirmDialog';
import { toast } from '../../lib/toast';

interface OpenEditorTab {
  path: string;
  name: string;
  content: string;
  originalContent: string;
  isDirty: boolean;
  compiledSql?: string;
  durationMs?: number;
}

/** Where a target actually writes, short enough to sit inside a <select>. */
const targetDestination = (target: DbtTarget): string => {
  const detail = target.detail ?? {};
  if (target.adapter === 'duckdb') return 'lakehouse catalog';
  const where = detail.host
    ? `${detail.host}:${detail.port ?? ''}${detail.database ? `/${detail.database}` : ''}`
    : detail.path ?? detail.source ?? target.adapter;
  const who = target.connection ? `${target.connection} · ` : '';
  const blocked = target.read_only ? ' · read-only' : '';
  return `${who}${where}${blocked}`;
};


export const DbtStudio: React.FC = () => {
  // Tree & Explorer State
  const [treeRoot, setTreeRoot] = useState<DbtTreeNode | null>(null);
  const [treeLoading, setTreeLoading] = useState(false);

  // Tabs & Editor State
  const [openTabs, setOpenTabs] = useState<OpenEditorTab[]>([]);
  const [activeTabPath, setActiveTabPath] = useState<string | null>(null);
  const [isSplitView, setIsSplitView] = useState(true);

  // Target Database. ``targetDb`` holds the binding key (``output|connection_id``) so a
  // Postgres output appears once per connection: picking the row *is* picking the
  // warehouse, and the backend is told both halves.
  const [targetDb, setTargetDb] = useState('');
  const [targets, setTargets] = useState<DbtTarget[]>([]);
  const [defaultTarget, setDefaultTarget] = useState('');
  const targetKey = (item: DbtTarget) => `${item.name}|${item.connection_id ?? ''}`;
  const activeTarget = targets.find((item) => targetKey(item) === targetDb);
  const activeTargetName = activeTarget?.name ?? targetDb.split('|')[0] ?? '';
  const activeConnectionId = activeTarget?.connection_id ?? null;
  // A read-only connection is a source: the API refuses to publish into it, so say it
  // here rather than letting the operator find out from a failed build.
  const targetReadOnly = Boolean(activeTarget?.read_only);

  // Modals
  const [isNewModelOpen, setIsNewModelOpen] = useState(false);
  const [newModelFolder, setNewModelFolder] = useState<'staging' | 'marts' | undefined>();
  const [isNewYamlOpen, setIsNewYamlOpen] = useState(false);

  // Bottom Dock State
  const [dockTab, setDockTab] = useState<'preview' | 'lineage' | 'terminal'>('preview');
  const [dockHeight, setDockHeight] = useState(300);
  const [isDockCollapsed, setIsDockCollapsed] = useState(false);
  const [isDockMaximized, setIsDockMaximized] = useState(false);
  const isResizingDock = useRef(false);

  // Execution & Action States
  const [isCompiling, setIsCompiling] = useState(false);
  const [isPreviewing, setIsPreviewing] = useState(false);
  const [isRunningDbt, setIsRunningDbt] = useState(false);
  const [isRunningTest, setIsRunningTest] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  // One dialog for every destructive dbt action: closing a dirty tab, deleting a file.
  const [confirmRequest, setConfirmRequest] = useState<ConfirmRequest | null>(null);

  // Results State. Each panel records the model that produced it, so switching
  // models can never display the previous model's rows or graph while the new
  // request is still running.
  const [previewData, setPreviewData] = useState<DbtPreviewResponse | null>(null);
  const [previewModel, setPreviewModel] = useState<string | null>(null);
  const [lineageData, setLineageData] = useState<DbtLineageResponse | null>(null);
  const [lineageModel, setLineageModel] = useState<string | null>(null);
  const [lineageLoading, setLineageLoading] = useState(false);
  const lineageRequestId = useRef(0);
  const [terminalLogs, setTerminalLogs] = useState<string>('');
  const [lastExecutionStatus, setLastExecutionStatus] = useState<'success' | 'failed' | null>(null);

  const activeTab = openTabs.find((t) => t.path === activeTabPath);
  const isSqlModel = activeTab?.name.endsWith('.sql');
  const activeModelName = activeTab ? activeTab.name.replace(/\.sql$/, '') : '';
  const previewForActiveModel = previewModel === activeModelName ? previewData : null;
  const lineageForActiveModel = lineageModel === activeModelName ? lineageData : null;

  // 1. Fetch Project Tree. Opening the studio lands on a blank canvas: the
  // tree is loaded for browsing, but no file is opened (and nothing is
  // compiled) until the user picks one.
  const fetchTree = useCallback(async () => {
    try {
      setTreeLoading(true);
      setTreeRoot(await api.getDbtTree());
    } catch (err) {
      console.error('Failed to load dbt project tree', err);
    } finally {
      setTreeLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchTree();
  }, [fetchTree]);

  // Targets live in profiles.yml: whatever is defined there is the list.
  useEffect(() => {
    api
      .getDbtTargets()
      .then((res) => {
        setTargets(res.targets);
        setDefaultTarget(res.default || '');
        // Prefer the binding the operator selected in Settings, then the profile default.
        const key = (item: DbtTarget) => `${item.name}|${item.connection_id ?? ''}`;
        const selected = res.targets.find(
          (item) => item.name === res.default && item.connection_id === (res.warehouse?.id ?? null),
        );
        setTargetDb((current) => current || (selected ? key(selected) : '') || (res.targets[0] ? key(res.targets[0]) : ''));
      })
      .catch(() => setTargets([]));
  }, []);

  // 2. Open File into Tab. Idempotent by path, including when two opens race
  // (StrictMode mounts every effect twice in dev), which used to leave the same
  // file open in two tabs.
  const handleOpenFile = async (path: string, _name?: string) => {
    if (openTabs.some((t) => t.path === path)) {
      setActiveTabPath(path);
      return;
    }

    try {
      const fileData = await api.getDbtFile(path);
      const newTab: OpenEditorTab = {
        path,
        name: fileData.name,
        content: fileData.content,
        originalContent: fileData.content,
        isDirty: false,
      };

      setOpenTabs((prev) => (prev.some((t) => t.path === path) ? prev : [...prev, newTab]));
      setActiveTabPath(path);
    } catch (err) {
      console.error('Failed to open file', err);
    }
  };

  /** Resolves a model name to its real path in the project tree. */
  const findModelPath = useCallback(
    (modelName: string): string | null => {
      const target = `${modelName}.sql`;
      const walk = (node: DbtTreeNode): string | null => {
        if (node.type === 'file') return node.name === target ? node.path : null;
        for (const child of node.children ?? []) {
          const found = walk(child);
          if (found) return found;
        }
        return null;
      };
      return treeRoot ? walk(treeRoot) : null;
    },
    [treeRoot]
  );

  // Clicking a lineage node opens that model. The path comes from the tree
  // instead of being guessed, so models outside the staging folder resolve too.
  const handleSelectModelFromLineage = useCallback(
    (model: string) => {
      const open = openTabs.find((t) => t.name === `${model}.sql`);
      if (open) {
        setActiveTabPath(open.path);
        return;
      }
      const path = findModelPath(model);
      if (path) handleOpenFile(path, `${model}.sql`);
    },
    [openTabs, findModelPath]
  );

  // Close Tab
  const closeTabNow = (path: string) => {
    const filtered = openTabs.filter((t) => t.path !== path);
    setOpenTabs(filtered);

    if (activeTabPath === path) {
      if (filtered.length > 0) {
        setActiveTabPath(filtered[filtered.length - 1].path);
      } else {
        setActiveTabPath(null);
      }
    }
  };

  const handleCloseTab = (path: string, e?: React.MouseEvent) => {
    e?.stopPropagation();
    const tabToClose = openTabs.find((t) => t.path === path);
    if (tabToClose?.isDirty) {
      // Blocking `window.confirm` froze the canvas timers; the dialog does not.
      setConfirmRequest({
        title: `Close "${tabToClose.name}"?`,
        body: 'It has unsaved changes. Closing the tab discards them.',
        confirmLabel: 'Close without saving',
        destructive: true,
        onConfirm: () => closeTabNow(path),
      });
      return;
    }
    closeTabNow(path);
  };

  // Editor Content Change
  const handleContentChange = (val: string | undefined) => {
    if (val === undefined || !activeTab) return;
    setOpenTabs((prev) =>
      prev.map((t) =>
        t.path === activeTab.path
          ? { ...t, content: val, isDirty: val !== t.originalContent }
          : t
      )
    );
  };

  // 3. Save Active File (Ctrl+S)
  const handleSaveActiveFile = async () => {
    if (!activeTab || isSaving) return;
    try {
      setIsSaving(true);
      await api.saveDbtFile(activeTab.path, activeTab.content);
      setOpenTabs((prev) =>
        prev.map((t) =>
          t.path === activeTab.path
            ? { ...t, originalContent: t.content, isDirty: false }
            : t
        )
      );

      // If SQL model, recompile automatically after save
      if (activeTab.name.endsWith('.sql')) {
        handleCompile();
      }
    } catch (err: any) {
      toast.error(`Failed to save file: ${err?.message ?? 'unknown error'}`);
    } finally {
      setIsSaving(false);
    }
  };

  // 4. Compile Active Model
  const handleCompile = async () => {
    if (!activeTab || !isSqlModel || isCompiling) return;
    try {
      setIsCompiling(true);
      // Auto-save if dirty before compile
      if (activeTab.isDirty) {
        await api.saveDbtFile(activeTab.path, activeTab.content);
        setOpenTabs((prev) =>
          prev.map((t) =>
            t.path === activeTab.path
              ? { ...t, originalContent: t.content, isDirty: false }
              : t
          )
        );
      }

      const res = await api.compileDbtModel(activeModelName, activeTargetName, activeConnectionId);
      setOpenTabs((prev) =>
        prev.map((t) =>
          t.path === activeTab.path
            ? { ...t, compiledSql: res.compiled_sql, durationMs: res.duration_ms }
            : t
        )
      );

      if (!isSplitView) {
        setIsSplitView(true); // Open split view so user sees compiled SQL
      }

      if (dockTab === 'lineage') loadLineage(activeModelName);
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err?.message || 'Compilation error';
      setTerminalLogs(`[Compilation Error]:\n${msg}`);
      setDockTab('terminal');
      setIsDockCollapsed(false);
    } finally {
      setIsCompiling(false);
    }
  };

  // 5. Preview Data
  const handlePreviewData = async () => {
    if (!activeTab || !isSqlModel || isPreviewing) return;
    // Capture the model: the preview endpoint compiles server-side and can take
    // several seconds, during which the user may switch tabs.
    const modelName = activeModelName;
    try {
      setIsPreviewing(true);
      // Auto-save if dirty
      if (activeTab.isDirty) {
        await api.saveDbtFile(activeTab.path, activeTab.content);
        setOpenTabs((prev) =>
          prev.map((t) =>
            t.path === activeTab.path
              ? { ...t, originalContent: t.content, isDirty: false }
              : t
          )
        );
      }

      const res = await api.previewDbtModel(modelName, activeTargetName, 50, activeConnectionId);
      setPreviewData(res);
      setPreviewModel(modelName);
      setDockTab('preview');
      setIsDockCollapsed(false);
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err?.message || 'Preview execution failed';
      setPreviewData({
        columns: [],
        rows: [],
        row_count: 0,
        duration_ms: 0,
        status: 'error',
        error: msg,
      });
      setDockTab('preview');
      setIsDockCollapsed(false);
    } finally {
      setIsPreviewing(false);
    }
  };

  // 6. Run Scoped dbt run
  const handleRunDbt = async () => {
    if (!activeTab || !isSqlModel || isRunningDbt) return;
    try {
      setIsRunningDbt(true);
      setDockTab('terminal');
      setIsDockCollapsed(false);
      setTerminalLogs(`$ dbt run --select ${activeModelName} --target ${activeTargetName}\nExecuting dbt models...\n`);

      if (activeTab.isDirty) {
        await api.saveDbtFile(activeTab.path, activeTab.content);
        setOpenTabs((prev) =>
          prev.map((t) =>
            t.path === activeTab.path
              ? { ...t, originalContent: t.content, isDirty: false }
              : t
          )
        );
      }

      const res = await api.runDbtModels(activeModelName, activeTargetName, activeConnectionId);
      setTerminalLogs(res.output || res.logs || 'dbt run finished with no output.');
      setLastExecutionStatus(res.success ? 'success' : 'failed');

      // Refresh lineage after run
      if (dockTab === 'lineage') loadLineage(activeModelName);
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err?.message || 'dbt run failed';
      setTerminalLogs(`[ERROR]: ${msg}`);
      setLastExecutionStatus('failed');
    } finally {
      setIsRunningDbt(false);
    }
  };

  // 7. Run Scoped dbt test
  const handleRunTest = async () => {
    if (!activeTab || !isSqlModel || isRunningTest) return;
    try {
      setIsRunningTest(true);
      setDockTab('terminal');
      setIsDockCollapsed(false);
      setTerminalLogs(`$ dbt test --select ${activeModelName} --target ${activeTargetName}\nRunning test validations...\n`);

      const res = await api.testDbtModels(activeModelName, activeTargetName, activeConnectionId);
      setTerminalLogs(res.output || res.logs || 'dbt test finished with no output.');
      setLastExecutionStatus(res.success ? 'success' : 'failed');
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err?.message || 'dbt test failed';
      setTerminalLogs(`[ERROR]: ${msg}`);
      setLastExecutionStatus('failed');
    } finally {
      setIsRunningTest(false);
    }
  };

  // 8. Load Lineage DAG. Only the newest request may write: switching models
  // mid-flight drops the older response instead of painting it over the new one.
  const loadLineage = useCallback(async (modelName: string) => {
    if (!modelName) return;
    const requestId = ++lineageRequestId.current;
    setLineageLoading(true);
    try {
      const lin = await api.getDbtLineage(modelName);
      if (requestId !== lineageRequestId.current) return;
      setLineageData(lin);
      setLineageModel(modelName);
    } catch (err) {
      console.error('Failed to load lineage', err);
    } finally {
      if (requestId === lineageRequestId.current) setLineageLoading(false);
    }
  }, []);

  // The dock follows the active model lazily: lineage is fetched when the panel
  // is actually on screen, not as a side effect of opening a file.
  useEffect(() => {
    if (dockTab !== 'lineage' || isDockCollapsed) return;
    if (!activeModelName || lineageModel === activeModelName) return;
    loadLineage(activeModelName);
  }, [dockTab, isDockCollapsed, activeModelName, lineageModel, loadLineage]);

  // 9. Delete File from Tree
  const handleDeleteFile = (path: string) => {
    const fileName = path.split('/').pop() || path;
    setConfirmRequest({
      title: `Delete "${fileName}"?`,
      body: 'This deletes the file from git_repo/dbt_project on disk. Any model or pipeline that references it breaks on the next run.',
      confirmLabel: 'Delete file',
      destructive: true,
      onConfirm: async () => {
        try {
          await api.deleteDbtFile(path);
          // The file is gone, so close its tab without re-asking about unsaved edits.
          closeTabNow(path);
          fetchTree();
          toast.success(`Deleted ${fileName}.`);
        } catch (err: any) {
          toast.error(`Failed to delete file: ${err?.message ?? 'unknown error'}`);
        }
      },
    });
  };

  // Keyboard shortcuts read the latest handlers through a ref, so the listener
  // is attached once instead of being torn down and rebuilt on every keystroke.
  const shortcutsRef = useRef({ handleSaveActiveFile, handlePreviewData });
  useEffect(() => {
    shortcutsRef.current = { handleSaveActiveFile, handlePreviewData };
  });

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 's') {
        e.preventDefault();
        shortcutsRef.current.handleSaveActiveFile();
      }
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        shortcutsRef.current.handlePreviewData();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, []);

  // Bottom dock resizer mouse handlers
  const handleDockResizeStart = (e: React.MouseEvent) => {
    e.preventDefault();
    isResizingDock.current = true;
    const startY = e.clientY;
    const startHeight = dockHeight;

    const onMouseMove = (moveEvent: MouseEvent) => {
      if (!isResizingDock.current) return;
      const deltaY = startY - moveEvent.clientY;
      const newHeight = Math.max(140, Math.min(700, startHeight + deltaY));
      setDockHeight(newHeight);
      if (isDockCollapsed) setIsDockCollapsed(false);
    };

    const onMouseUp = () => {
      isResizingDock.current = false;
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };

    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
  };

  return (
    <div className="flex-1 flex min-h-0 w-full overflow-hidden bg-slate-100 select-none">
      {/* 1. Left Sidebar: dbt Project Explorer (260px) */}
      <DbtProjectTree
        root={treeRoot}
        loading={treeLoading}
        selectedPath={activeTabPath}
        onSelectFile={(node) => handleOpenFile(node.path, node.name)}
        onRefresh={fetchTree}
        onOpenNewModel={(folder) => {
          setNewModelFolder(folder);
          setIsNewModelOpen(true);
        }}
        onOpenNewYaml={() => setIsNewYamlOpen(true)}
        onDeleteFile={handleDeleteFile}
      />

      {/* Center & Right Workspace: Editor + Output Dock */}
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden bg-white">
        {/* Top Ribbon: Tab Bar & Actions Toolbar */}
        <div className="h-10 border-b border-slate-200 bg-slate-50/70 flex items-center justify-between px-2 gap-2 shrink-0">
          {/* Tab Ribbon */}
          <div className="flex items-center gap-1 overflow-x-auto min-w-0 h-full py-1">
            {openTabs.map((tab) => {
              const isActive = tab.path === activeTabPath;
              const isSql = tab.name.endsWith('.sql');
              return (
                <div
                  key={tab.path}
                  onClick={() => setActiveTabPath(tab.path)}
                  className={`group h-8 px-2.5 rounded-t-md flex items-center gap-1.5 text-xs font-mono cursor-pointer transition-all border-t-2 ${
                    isActive
                      ? 'bg-white border-t-sky-600 text-slate-900 font-semibold shadow-2xs border-x border-slate-200 -mb-[1px] z-10'
                      : 'border-t-transparent bg-slate-200/50 hover:bg-slate-200/90 text-slate-500 hover:text-slate-800'
                  }`}
                  title={tab.path}
                >
                  {isSql ? (
                    <FileCode className="w-3.5 h-3.5 text-sky-600 shrink-0" />
                  ) : (
                    <FileText className="w-3.5 h-3.5 text-emerald-600 shrink-0" />
                  )}
                  <span className="truncate max-w-[140px] text-[11px]">{tab.name}</span>

                  {/* Dirty indicator */}
                  {tab.isDirty && (
                    <span
                      className="w-1.5 h-1.5 rounded-full bg-amber-500 shrink-0"
                      title="Unsaved changes"
                    />
                  )}

                  {/* Close Tab button */}
                  <button
                    onClick={(e) => handleCloseTab(tab.path, e)}
                    className="opacity-0 group-hover:opacity-100 p-0.5 rounded hover:bg-slate-200 text-slate-400 hover:text-slate-700 transition-opacity ml-1"
                    title="Close"
                  >
                    <X className="w-3 h-3" />
                  </button>
                </div>
              );
            })}

            {openTabs.length === 0 && (
              <span className="text-xs text-slate-400 italic px-2">No files open</span>
            )}
          </div>

          {/* Editor Action Toolbar */}
          <div className="flex items-center gap-1.5 shrink-0">
            {/* Target Database Selector */}
            <div className="flex items-center gap-1 mr-1">
              <Database className="w-3.5 h-3.5 text-slate-400" />
              <div className="relative">
                <select
                  value={targetDb}
                  onChange={(e) => setTargetDb(e.target.value)}
                  title={(() => {
                    const active = targets.find((item) => targetKey(item) === targetDb);
                    if (!active) return 'No dbt target defined in profiles.yml';
                    const d = active.detail;
                    const where = d.host ? `${d.host}:${d.port}/${d.database ?? ''}` : d.path ?? d.source;
                    const access = active.read_only ? ' \u00b7 read-only, publishing is refused' : '';
                    return `${active.adapter}${where ? ' \u00b7 ' + where : ''}${access}${active.available ? '' : ' \u00b7 adapter not installed'}`;
                  })()}
                  className="appearance-none bg-white border border-slate-200 hover:border-slate-300 rounded-md pl-2 pr-6 py-1 text-[11px] font-semibold text-slate-700 shadow-2xs focus:outline-sky-500 cursor-pointer"
                >
                  {targets.length === 0 && <option value="">No targets in profiles.yml</option>}
                  {targets.map((item) => (
                    <option
                      key={targetKey(item)}
                      value={targetKey(item)}
                      disabled={!item.available || Boolean(item.read_only)}
                    >
                      {item.name}
                      {item.name === defaultTarget ? ' (default)' : ''} — {targetDestination(item)}
                      {item.available ? '' : ' (adapter not installed)'}
                    </option>
                  ))}
                </select>
                <ChevronDown className="w-3 h-3 text-slate-400 absolute right-1.5 top-2 pointer-events-none" />
              </div>
              {targetReadOnly && (
                <span
                  className="px-1.5 py-0.5 rounded text-[10px] font-semibold uppercase bg-rose-50 text-rose-700 border border-rose-200"
                  title={`"${activeTarget?.connection ?? activeTarget?.name}" is read-only: obelisk will not publish dbt models into it. Mark it writable in Settings → Connections, or pick another warehouse.`}
                >
                  Read-only
                </span>
              )}
            </div>

            {/* Save Button (Ctrl+S) */}
            <button
              onClick={handleSaveActiveFile}
              disabled={!activeTab || isSaving}
              title="Save Changes (Ctrl+S)"
              className={`flex items-center gap-1 px-2.5 py-1 rounded-md text-xs font-semibold shadow-2xs transition-all border ${
                activeTab?.isDirty
                  ? 'bg-amber-500 hover:bg-amber-600 text-white border-amber-600'
                  : 'bg-white hover:bg-slate-100 text-slate-700 border-slate-200'
              } disabled:opacity-40`}
            >
              <Save className="w-3 h-3" />
              <span>Save</span>
            </button>

            {/* Split View Toggle */}
            {isSqlModel && (
              <button
                onClick={() => setIsSplitView((prev) => !prev)}
                title={isSplitView ? 'Switch to Single View' : 'Switch to Side-by-Side Split View'}
                className={`p-1.5 rounded-md border text-xs font-semibold shadow-2xs transition-all ${
                  isSplitView
                    ? 'bg-sky-50 text-sky-700 border-sky-300 font-bold'
                    : 'bg-white hover:bg-slate-100 text-slate-600 border-slate-200'
                }`}
              >
                <Columns2 className="w-3.5 h-3.5" />
              </button>
            )}

            {/* Compile Model */}
            {isSqlModel && (
              <button
                onClick={handleCompile}
                disabled={isCompiling}
                title="Compile Jinja to SQL (Ctrl+Enter)"
                className="flex items-center gap-1 px-2.5 py-1 rounded-md bg-white hover:bg-slate-100 text-slate-700 border border-slate-200 text-xs font-semibold shadow-2xs transition-all disabled:opacity-40"
              >
                <Zap className={`w-3 h-3 ${isCompiling ? 'text-amber-500 animate-pulse' : 'text-amber-600'}`} />
                <span>{isCompiling ? 'Compiling...' : 'Compile'}</span>
              </button>
            )}

            {/* Preview Data */}
            {isSqlModel && (
              <button
                onClick={handlePreviewData}
                disabled={isPreviewing}
                title="Execute Data Preview in DataGrid"
                className="flex items-center gap-1 px-2.5 py-1 rounded-md bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold shadow-2xs transition-all disabled:opacity-40"
              >
                <Play className="w-3 h-3" />
                <span>{isPreviewing ? 'Querying...' : 'Preview Data'}</span>
              </button>
            )}

            {/* dbt run */}
            {isSqlModel && (
              <button
                onClick={handleRunDbt}
                disabled={isRunningDbt || targetReadOnly}
                title={
                  targetReadOnly
                    ? 'This target is a read-only source: mark the connection writable in Settings → Connections.'
                    : `Run dbt run --select ${activeModelName}`
                }
                className="flex items-center gap-1 px-2.5 py-1 rounded-md bg-slate-900 hover:bg-slate-800 text-white text-xs font-semibold shadow-2xs transition-all disabled:opacity-40"
              >
                <Boxes className="w-3 h-3 text-sky-400" />
                <span>{isRunningDbt ? 'Running...' : 'dbt run'}</span>
              </button>
            )}

            {/* dbt test */}
            {isSqlModel && (
              <button
                onClick={handleRunTest}
                disabled={isRunningTest}
                title={`Run dbt test --select ${activeModelName}`}
                className="flex items-center gap-1 px-2.5 py-1 rounded-md bg-white hover:bg-slate-100 text-slate-700 border border-slate-200 text-xs font-semibold shadow-2xs transition-all disabled:opacity-40"
              >
                <CheckCircle2 className="w-3 h-3 text-emerald-600" />
                <span>{isRunningTest ? 'Testing...' : 'Run Tests'}</span>
              </button>
            )}
          </div>
        </div>

        {/* Center Monaco Editor Pane */}
        <div className="flex-1 flex min-h-0 overflow-hidden relative">
          {activeTab ? (
            isSqlModel && isSplitView ? (
              /* Side-by-Side Split View */
              <div className="flex-1 flex min-h-0 overflow-hidden divide-x divide-slate-200">
                {/* Left: Jinja Source Code */}
                <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
                  <div className="px-3 py-1 bg-slate-50 border-b border-slate-200 text-[10px] font-mono font-bold text-slate-500 uppercase tracking-wider flex items-center justify-between">
                    <span className="flex items-center gap-1.5">
                      <FileCode className="w-3 h-3 text-sky-600" />
                      Jinja SQL Source ({activeTab.name})
                    </span>
                    {activeTab.isDirty && (
                      <span className="text-amber-600 font-semibold lowercase">unsaved</span>
                    )}
                  </div>
                  <div className="flex-1 min-h-0">
                    <Editor
                      height="100%"
                      defaultLanguage="sql"
                      theme="light"
                      value={activeTab.content}
                      onChange={handleContentChange}
                      options={{
                        minimap: { enabled: false },
                        fontSize: 13,
                        fontFamily: "JetBrains Mono, Menlo, Monaco, 'Courier New', monospace",
                        lineNumbers: 'on',
                        scrollBeyondLastLine: false,
                        wordWrap: 'on',
                        automaticLayout: true,
                        padding: { top: 10, bottom: 10 },
                      }}
                    />
                  </div>
                </div>

                {/* Right: Compiled SQL Output */}
                <div className="flex-1 flex flex-col min-w-0 overflow-hidden bg-slate-50/30">
                  <div className="px-3 py-1 bg-slate-50 border-b border-slate-200 text-[10px] font-mono font-bold text-slate-500 uppercase tracking-wider flex items-center justify-between">
                    <span className="flex items-center gap-1.5">
                      <Zap className="w-3 h-3 text-amber-600" />
                      Compiled SQL ({targetDb})
                    </span>
                    {activeTab.durationMs != null && (
                      <span className="text-slate-400 font-mono text-[9px]">
                        compiled in {activeTab.durationMs}ms
                      </span>
                    )}
                  </div>
                  <div className="flex-1 min-h-0">
                    <Editor
                      height="100%"
                      defaultLanguage="sql"
                      theme="light"
                      value={activeTab.compiledSql || '-- Click [Compile] or [Preview Data] to view compiled SQL output'}
                      options={{
                        readOnly: true,
                        minimap: { enabled: false },
                        fontSize: 13,
                        fontFamily: "JetBrains Mono, Menlo, Monaco, 'Courier New', monospace",
                        lineNumbers: 'on',
                        scrollBeyondLastLine: false,
                        wordWrap: 'on',
                        automaticLayout: true,
                        padding: { top: 10, bottom: 10 },
                      }}
                    />
                  </div>
                </div>
              </div>
            ) : (
              /* Single View */
              <div className="flex-1 flex flex-col min-h-0 overflow-hidden">
                <Editor
                  height="100%"
                  defaultLanguage={activeTab.name.endsWith('.yml') || activeTab.name.endsWith('.yaml') ? 'yaml' : 'sql'}
                  theme="light"
                  value={activeTab.content}
                  onChange={handleContentChange}
                  options={{
                    minimap: { enabled: false },
                    fontSize: 13,
                    fontFamily: "JetBrains Mono, Menlo, Monaco, 'Courier New', monospace",
                    lineNumbers: 'on',
                    scrollBeyondLastLine: false,
                    wordWrap: 'on',
                    automaticLayout: true,
                    padding: { top: 10, bottom: 10 },
                  }}
                />
              </div>
            )
          ) : (
            /* Empty State */
            <div className="flex-1 flex flex-col items-center justify-center p-8 text-center bg-slate-50/50">
              <Boxes className="w-12 h-12 text-slate-300 mb-3" />
              <h3 className="text-sm font-bold text-slate-700">dbt Studio Workspace</h3>
              <p className="text-xs text-slate-400 max-w-sm mt-1">
                Select a model from the left explorer or click "+ New Model" to start authoring and compiling transformations.
              </p>
              <button
                onClick={() => setIsNewModelOpen(true)}
                className="mt-4 px-3 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold shadow-2xs transition-colors"
              >
                + New Model
              </button>
            </div>
          )}
        </div>

        {/* Resizable Dock Divider Bar */}
        <div
          onMouseDown={handleDockResizeStart}
          className="h-1 bg-slate-200 hover:bg-slate-300 active:bg-slate-400 cursor-row-resize transition-colors shrink-0"
          title="Drag to resize output dock"
        />

        {/* 3. Bottom Dock: Multi-Tab Output Panel */}
        <div
          style={{ height: isDockCollapsed ? '32px' : isDockMaximized ? 'calc(100% - 44px)' : `${dockHeight}px` }}
          className="border-t border-slate-200 bg-white flex flex-col shrink-0 overflow-hidden transition-all duration-100"
        >
          {/* Dock Header & Tab Navigation */}
          <div className="h-8 border-b border-slate-200 bg-slate-50 flex items-center justify-between px-3 shrink-0">
            <div className="flex items-center gap-1">
              {/* Tab 1: Data Preview */}
              <button
                onClick={() => {
                  setDockTab('preview');
                  if (isDockCollapsed) setIsDockCollapsed(false);
                }}
                className={`flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-semibold transition-all ${
                  dockTab === 'preview' && !isDockCollapsed
                    ? 'bg-white text-sky-800 shadow-2xs border border-slate-200 font-bold'
                    : 'text-slate-500 hover:text-slate-900'
                }`}
              >
                <Table2 className="w-3.5 h-3.5 text-sky-600" />
                <span>Data Preview</span>
                {previewForActiveModel && (
                  <span className="text-[10px] font-mono px-1.5 py-0.2 bg-slate-100 rounded text-slate-600 border border-slate-200">
                    {previewForActiveModel.row_count} rows
                  </span>
                )}
              </button>

              {/* Tab 2: Model Lineage DAG */}
              <button
                onClick={() => {
                  setDockTab('lineage');
                  if (isDockCollapsed) setIsDockCollapsed(false);
                  if (activeModelName && lineageModel !== activeModelName) loadLineage(activeModelName);
                }}
                className={`flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-semibold transition-all ${
                  dockTab === 'lineage' && !isDockCollapsed
                    ? 'bg-white text-sky-800 shadow-2xs border border-slate-200 font-bold'
                    : 'text-slate-500 hover:text-slate-900'
                }`}
              >
                <Split className="w-3.5 h-3.5 text-sky-600" />
                <span>Model Lineage DAG</span>
              </button>

              {/* Tab 3: Terminal & Logs */}
              <button
                onClick={() => {
                  setDockTab('terminal');
                  if (isDockCollapsed) setIsDockCollapsed(false);
                }}
                className={`flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-semibold transition-all ${
                  dockTab === 'terminal' && !isDockCollapsed
                    ? 'bg-white text-slate-900 shadow-2xs border border-slate-200 font-bold'
                    : 'text-slate-500 hover:text-slate-900'
                }`}
              >
                <Terminal className="w-3.5 h-3.5 text-slate-600" />
                <span>Terminal & Logs</span>
                {lastExecutionStatus && (
                  <span
                    className={`w-2 h-2 rounded-full ${
                      lastExecutionStatus === 'success' ? 'bg-emerald-500' : 'bg-rose-500'
                    }`}
                  />
                )}
              </button>
            </div>

            {/* Right dock controls: Maximize / Minimize / Collapse / Expand */}
            <div className="flex items-center gap-1">
              <button
                onClick={() => {
                  setIsDockMaximized((prev) => !prev);
                  if (isDockCollapsed) setIsDockCollapsed(false);
                }}
                className="p-1 rounded hover:bg-slate-200 text-slate-400 hover:text-slate-600 transition-colors"
                title={isDockMaximized ? 'Restore dock height' : 'Maximize dock'}
              >
                {isDockMaximized ? <Minimize2 className="w-3.5 h-3.5" /> : <Maximize2 className="w-3.5 h-3.5" />}
              </button>
              <button
                onClick={() => {
                  setIsDockCollapsed((prev) => !prev);
                  if (isDockMaximized) setIsDockMaximized(false);
                }}
                className="p-1 rounded hover:bg-slate-200 text-slate-400 hover:text-slate-600 transition-colors"
                title={isDockCollapsed ? 'Expand dock' : 'Collapse dock'}
              >
                {isDockCollapsed ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
              </button>
            </div>
          </div>

          {/* Dock Content Body */}
          {!isDockCollapsed && (
            <div className="flex-1 min-h-0 overflow-hidden relative">
              {/* Data Preview Tab */}
              {dockTab === 'preview' && (
                <div className="h-full flex flex-col overflow-hidden">
                  {previewForActiveModel?.error ? (
                    <div className="p-4 bg-rose-50 text-rose-800 text-xs font-mono whitespace-pre-wrap overflow-auto">
                      <div className="flex items-center gap-2 font-bold mb-1 text-rose-900">
                        <AlertCircle className="w-4 h-4 text-rose-600" />
                        Preview Error
                      </div>
                      {previewForActiveModel.error}
                    </div>
                  ) : previewForActiveModel && previewForActiveModel.columns.length > 0 ? (
                    <div className="h-full flex flex-col overflow-hidden">
                      <div className="px-3 py-1 border-b border-slate-200 bg-slate-50/50 flex items-center justify-between text-[11px] text-slate-500">
                        <span className="font-mono">
                          {previewForActiveModel.row_count} rows previewed in {previewForActiveModel.duration_ms}ms (Target: {targetDb})
                        </span>
                        <span className="text-[10px] text-slate-400">SELECT * FROM ({activeModelName}) LIMIT 50</span>
                      </div>
                      <div className="flex-1 min-h-0">
                        <DataGrid
                          columns={previewForActiveModel.columns}
                          rows={previewForActiveModel.rows}
                          loading={isPreviewing}
                        />
                      </div>
                    </div>
                  ) : (
                    <div className="h-full flex flex-col items-center justify-center p-8 text-center text-slate-400 text-xs">
                      <Table2 className="w-8 h-8 text-slate-300 mb-2" />
                      <p>No preview data available.</p>
                      <button
                        onClick={handlePreviewData}
                        disabled={!isSqlModel || isPreviewing}
                        className="mt-3 px-3 py-1.5 rounded-md bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold shadow-2xs transition-colors"
                      >
                        Preview {activeModelName || 'Model'}
                      </button>
                    </div>
                  )}
                </div>
              )}

              {/* Model Lineage DAG Tab */}
              {dockTab === 'lineage' && (
                <div className="h-full overflow-hidden">
                  <DbtLineageGraph
                    lineage={lineageForActiveModel}
                    loading={lineageLoading}
                    onSelectModel={handleSelectModelFromLineage}
                  />
                </div>
              )}

              {/* Terminal & Logs Tab */}
              {dockTab === 'terminal' && (
                <div className="h-full flex flex-col bg-slate-950 text-slate-100 font-mono text-xs overflow-hidden">
                  <div className="px-3 py-1 bg-slate-900 border-b border-slate-800 flex items-center justify-between text-[11px] text-slate-400">
                    <span className="flex items-center gap-1.5 text-slate-300">
                      <Terminal className="w-3.5 h-3.5 text-sky-400" />
                      dbt CLI Logs
                    </span>
                    <button
                      onClick={() => setTerminalLogs('')}
                      className="hover:text-slate-200 text-[10px]"
                    >
                      Clear
                    </button>
                  </div>
                  <div className="flex-1 overflow-auto p-3 whitespace-pre-wrap leading-relaxed select-text font-mono text-[11px]">
                    {terminalLogs || 'No dbt execution logs yet. Run [dbt run] or [Run Tests] to stream terminal output.'}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* New Model Modal */}
      <NewModelModal
        isOpen={isNewModelOpen}
        initialFolder={newModelFolder}
        onClose={() => setIsNewModelOpen(false)}
        onSuccess={(filePath) => {
          fetchTree();
          const fileName = filePath.split('/').pop() || filePath;
          handleOpenFile(filePath, fileName);
        }}
      />

      {/* New Schema YAML Modal */}
      <NewYamlModal
        isOpen={isNewYamlOpen}
        onClose={() => setIsNewYamlOpen(false)}
        onSuccess={(filePath) => {
          fetchTree();
          const fileName = filePath.split('/').pop() || filePath;
          handleOpenFile(filePath, fileName);
        }}
      />

      <ConfirmDialog request={confirmRequest} onClose={() => setConfirmRequest(null)} />
    </div>
  );
};
