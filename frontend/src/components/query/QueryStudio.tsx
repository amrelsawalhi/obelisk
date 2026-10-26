import React, { useState, useEffect, useRef } from 'react';
import { tableSizeLabel } from '../../lib/format';
import Editor from '@monaco-editor/react';
import axios from 'axios';
import {
  Play,
  Download,
  History,
  Clock,
  Hash,
  AlertCircle,
  RefreshCw,
  FileSpreadsheet,
  Table2,
  Database,
  ChevronRight,
  ChevronDown,
  Folder,
  Layers,
  Plus,
  X,
  FileCode,
  ExternalLink,
  Sparkles,
  Map,
  CheckCircle2,
  AlertTriangle,
} from 'lucide-react';
import { api, apiClient } from '../../lib/api';
import type { QueryExecuteResponse, QueryHistoryItem, CatalogNode, CatalogTable, QueryTarget } from '../../lib/types';
import { DataGrid } from './DataGrid';
import { formatSql } from '../../lib/sqlFormatter';

const FALLBACK_DEFAULT_SQL = `-- Obelisk Query Studio (DuckDB In-Process Analytical Engine)
-- Fast federation across Bronze Parquet and connected PostgreSQL sources

SELECT 
    'Orders & Revenue' AS analysis_type,
    CURRENT_DATE AS query_date,
    5 AS total_records;
`;

export interface QueryTab {
  id: string;
  title: string;
  sql: string;
  targetId?: string;
  schema?: string;
  result: QueryExecuteResponse | null;
  error: string | null;
  loading: boolean;
}

interface QueryStudioProps {
  initialSql?: string;
  isPopout?: boolean;
}

export const QueryStudio: React.FC<QueryStudioProps> = ({ initialSql, isPopout = false }) => {
  const getInitialSql = () => {
    if (initialSql) return initialSql;
    if (isPopout) {
      try {
        const saved = localStorage.getItem('obelisk_popout_sql');
        if (saved && saved.trim()) return saved;
      } catch {}
    }
    return FALLBACK_DEFAULT_SQL;
  };

  const getInitialTitle = () => {
    if (isPopout) {
      try {
        const saved = localStorage.getItem('obelisk_popout_title');
        if (saved && saved.trim()) return saved;
      } catch {}
    }
    return 'Query 1';
  };

  const [tabs, setTabs] = useState<QueryTab[]>([
    {
      id: 'tab-1',
      title: getInitialTitle(),
      sql: getInitialSql(),
      targetId: 'duckdb',
      schema: 'main',
      result: null,
      error: null,
      loading: false,
    },
  ]);
  const [activeTabId, setActiveTabId] = useState<string>('tab-1');
  const [editingTabId, setEditingTabId] = useState<string | null>(null);
  const [editingTitle, setEditingTitle] = useState<string>('');

  const [targets, setTargets] = useState<QueryTarget[]>([]);

  const fetchTargets = async () => {
    try {
      const data = await api.getQueryTargets();
      if (data && data.length > 0) {
        setTargets(data);
      }
    } catch {
      // Quiet target load
    }
  };

  const [historyOpen, setHistoryOpen] = useState<boolean>(false);
  const [schemaOpen, setSchemaOpen] = useState<boolean>(true);
  const [history, setHistory] = useState<QueryHistoryItem[]>([]);
  const [catalogNodes, setCatalogNodes] = useState<CatalogNode[]>([]);
  const [expandedTables, setExpandedTables] = useState<Record<string, boolean>>({});
  const [collapsedGroups, setCollapsedGroups] = useState<Record<string, boolean>>({});
  const [exporting, setExporting] = useState<string | null>(null);
  const [toast, setToast] = useState<{ message: string; type: 'info' | 'success' | 'error' } | null>(null);

  const showToast = (message: string, type: 'info' | 'success' | 'error' = 'info', duration = 3500) => {
    setToast({ message, type });
    setTimeout(() => {
      setToast((curr) => (curr?.message === message ? null : curr));
    }, duration);
  };

  // SQLite Persistence Debounce Timer
  const saveDebounceTimer = useRef<any>(null);

  // Advanced Monaco States & Resizing
  const editorRef = useRef<any>(null);
  const monacoRef = useRef<any>(null);
  const [showMinimap, setShowMinimap] = useState<boolean>(false);
  const [schemaWidth, setSchemaWidth] = useState<number>(260);
  const [editorHeight, setEditorHeight] = useState<number>(isPopout ? 340 : 230);

  const activeTab: QueryTab = tabs.find((t) => t.id === activeTabId) || tabs[0] || {
    id: 'tab-1',
    title: 'Query 1',
    sql: FALLBACK_DEFAULT_SQL,
    targetId: 'duckdb',
    schema: 'main',
    result: null,
    error: null,
    loading: false,
  };

  const getTabTarget = (tab: QueryTab): QueryTarget => {
    const found = targets.find((t) => t.id === tab.targetId);
    if (found) return found;
    return (
      targets.find((t) => t.id === 'duckdb') || {
        id: 'duckdb',
        name: 'DuckDB',
        engine: 'duckdb',
        is_default: true,
        capabilities: ['Lakehouse Parquet', 'Federated SQL'],
        status: 'ready',
        schemas: ['main'],
      }
    );
  };

  const activeTarget = getTabTarget(activeTab);

  const persistTabToBackend = (tab: QueryTab, order?: number) => {
    api.saveOpenQueryTab({
      id: tab.id,
      title: tab.title,
      sql: tab.sql,
      target_id: tab.targetId,
      schema_name: tab.schema,
      tab_order: order,
    }).catch(() => {});
  };

  const handleSetTarget = (targetId: string) => {
    const tgt = targets.find((t) => t.id === targetId);
    const defaultSchema = tgt?.schemas?.[0] || (tgt?.engine === 'postgres' ? 'public' : undefined);
    setTabs((prev) =>
      prev.map((t) => {
        if (t.id === activeTabId) {
          const updated = { ...t, targetId, schema: defaultSchema };
          persistTabToBackend(updated);
          return updated;
        }
        return t;
      })
    );
  };

  const handleSetSchema = (schema: string) => {
    setTabs((prev) =>
      prev.map((t) => {
        if (t.id === activeTabId) {
          const updated = { ...t, schema };
          persistTabToBackend(updated);
          return updated;
        }
        return t;
      })
    );
  };

  const updateActiveTabSql = (newSql: string) => {
    setTabs((prev) =>
      prev.map((t) => {
        if (t.id === activeTabId) {
          const updated = { ...t, sql: newSql };
          if (saveDebounceTimer.current) clearTimeout(saveDebounceTimer.current);
          saveDebounceTimer.current = setTimeout(() => {
            persistTabToBackend(updated);
          }, 800);
          return updated;
        }
        return t;
      })
    );
  };

  const handleAddTab = (
    initialQuery?: string,
    initialTitle?: string,
    initialTargetId?: string,
    initialSchema?: string
  ): QueryTab => {
    const newId = `tab-${Date.now()}`;
    const newTitle = initialTitle || `Query ${tabs.length + 1}`;
    const newTab: QueryTab = {
      id: newId,
      title: newTitle,
      sql: initialQuery ?? FALLBACK_DEFAULT_SQL,
      targetId: initialTargetId || activeTab.targetId || 'duckdb',
      schema: initialSchema || activeTab.schema,
      result: null,
      error: null,
      loading: false,
    };
    setTabs((prev) => [...prev, newTab]);
    setActiveTabId(newId);
    persistTabToBackend(newTab, tabs.length);
    return newTab;
  };

  const handleCloseTab = (tabIdToClose: string, e?: React.MouseEvent) => {
    e?.stopPropagation();
    // Remove from SQLite brain to avoid bloat
    api.deleteOpenQueryTab(tabIdToClose).catch(() => {});

    if (tabs.length <= 1) {
      const freshTab: QueryTab = {
        id: `tab-${Date.now()}`,
        title: 'Query 1',
        sql: FALLBACK_DEFAULT_SQL,
        targetId: 'duckdb',
        schema: 'main',
        result: null,
        error: null,
        loading: false,
      };
      setTabs([freshTab]);
      setActiveTabId(freshTab.id);
      persistTabToBackend(freshTab, 0);
      return;
    }

    const tabIndex = tabs.findIndex((t) => t.id === tabIdToClose);
    const remaining = tabs.filter((t) => t.id !== tabIdToClose);
    setTabs(remaining);

    if (activeTabId === tabIdToClose) {
      const nextActive = remaining[Math.max(0, tabIndex - 1)];
      setActiveTabId(nextActive.id);
    }
  };

  const handleStartRenameTab = (tab: QueryTab, e: React.MouseEvent) => {
    e.stopPropagation();
    setEditingTabId(tab.id);
    setEditingTitle(tab.title);
  };

  const handleSaveRenameTab = () => {
    if (editingTabId && editingTitle.trim()) {
      const trimmed = editingTitle.trim();
      setTabs((prev) =>
        prev.map((t) => {
          if (t.id === editingTabId) {
            const updated = { ...t, title: trimmed };
            persistTabToBackend(updated);
            return updated;
          }
          return t;
        })
      );
    }
    setEditingTabId(null);
  };

  const toggleGroup = (key: string) => {
    setCollapsedGroups((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const fetchHistory = async () => {
    try {
      const data = await api.getQueryHistory();
      setHistory(data);
    } catch {
      // Quiet background polling
    }
  };

  const fetchCatalog = async () => {
    try {
      const nodes = await api.getCatalogTree();
      setCatalogNodes(nodes);

      // The default query used to hardcode 'bronze': a lakehouse without that folder
      // opened on `SELECT * FROM bronze.` and an error. Take the first folder that
      // actually holds tables and qualify it the way DuckLake resolves it.
      const lakehouseNode = nodes.find((n) => n.id === 'lakehouse' || n.type === 'lakehouse');
      const firstDirWithTables = lakehouseNode?.directories?.find(
        (d) => d.tables && d.tables.length > 0,
      );
      const firstTable = firstDirWithTables?.tables?.[0] ?? lakehouseNode?.tables?.[0];
      if (!initialSql && firstTable) {
        const folderPath = firstDirWithTables?.path || firstDirWithTables?.name || '';
        const qualified = [folderPath.replace(/\//g, '.'), firstTable.name]
          .filter(Boolean)
          .join('.');
        const initialQuery = `-- Obelisk Query Studio (DuckDB In-Process Analytical Engine)\nSELECT * FROM lakehouse.${qualified} LIMIT 50;\n`;
        setTabs((prev) =>
          prev.map((t, idx) =>
            idx === 0 && !t.result && t.sql === FALLBACK_DEFAULT_SQL
              ? { ...t, title: firstTable.name, sql: initialQuery }
              : t
          )
        );
      }
    } catch {
      // Quiet catalog load
    }
  };

  const executeQuerySql = async (queryToRun: string, targetTabId?: string) => {
    if (!queryToRun.trim()) return;
    const tabId = targetTabId || activeTabId;
    const currentTab = tabs.find((t) => t.id === tabId) || activeTab;

    // Set loading for this specific tab
    setTabs((prev) =>
      prev.map((t) =>
        t.id === tabId ? { ...t, loading: true, error: null } : t
      )
    );

    try {
      const data = await api.executeQuery({
        sql: queryToRun.trim(),
        title: currentTab.title,
        target: currentTab.targetId || 'duckdb',
        schema_name: currentTab.schema,
        timeout_seconds: 30,
      });
      setTabs((prev) =>
        prev.map((t) =>
          t.id === tabId
            ? { ...t, result: data, error: null, loading: false }
            : t
        )
      );
      fetchHistory();
    } catch (err: unknown) {
      let msg = 'Query execution failed';
      if (axios.isAxiosError(err)) {
        msg = err.response?.data?.detail || err.message;
      } else if (err instanceof Error) {
        msg = err.message;
      }
      setTabs((prev) =>
        prev.map((t) =>
          t.id === tabId
            ? { ...t, error: msg, loading: false }
            : t
        )
      );
      fetchHistory();
    }
  };

  const handleExecute = async () => {
    await executeQuerySql(activeTab.sql, activeTab.id);
  };

  // Monaco registers its keymap once, when the editor mounts, so the action it calls has
  // to read the handler through a ref. Captured directly, Ctrl+Enter kept running the tab
  // — and the SQL text — that were active at mount time.
  const runHandlerRef = useRef(handleExecute);
  const formatHandlerRef = useRef<() => void>(() => {});
  useEffect(() => {
    runHandlerRef.current = handleExecute;
  });

  useEffect(() => {
    fetchTargets();
    fetchHistory();
    fetchCatalog();

    const loadSavedTabs = async () => {
      try {
        const saved = await api.getOpenQueryTabs();
        if (saved && saved.length > 0) {
          const loaded: QueryTab[] = saved.map((st) => ({
            id: st.id,
            title: st.title,
            sql: st.sql,
            targetId: st.target_id || 'duckdb',
            schema: st.schema_name || undefined,
            result: null,
            error: null,
            loading: false,
          }));
          setTabs(loaded);
          setActiveTabId(loaded[0].id);
        } else {
          // Initialize first tab in SQLite
          api.saveOpenQueryTab({
            id: 'tab-1',
            title: getInitialTitle(),
            sql: getInitialSql(),
            target_id: 'duckdb',
            schema_name: 'main',
            tab_order: 0,
          }).catch(() => {});
        }
      } catch {
        // Quiet fallback
      }
    };
    loadSavedTabs();
  }, []);

  useEffect(() => {
    if (initialSql) {
      // Singleton check: if an existing tab has this SQL, focus it
      const existingTab = tabs.find(
        (t) => t.sql.trim() === initialSql.trim()
      );
      if (existingTab) {
        setActiveTabId(existingTab.id);
        return;
      }

      const isCurrentPristine =
        !activeTab.result &&
        !activeTab.error &&
        (activeTab.sql === FALLBACK_DEFAULT_SQL || !activeTab.sql.trim());
      if (isCurrentPristine) {
        setTabs((prev) =>
          prev.map((t) =>
            t.id === activeTab.id ? { ...t, sql: initialSql, title: 'Query' } : t
          )
        );
        persistTabToBackend({ ...activeTab, sql: initialSql, title: 'Query' }, 0);
      } else {
        handleAddTab(initialSql, 'Query');
      }
    }
  }, [initialSql]);

  const handleTableClick = (
    tableOrName: CatalogTable | string,
    options?: { isLakehouse?: boolean; folderPath?: string; sourceName?: string; schemaName?: string; fqn?: string }
  ) => {
    let fqn: string;
    let tabTitle: string;

    if (typeof tableOrName === 'object' && tableOrName.fqn) {
      fqn = tableOrName.fqn;
      tabTitle = tableOrName.name;
    } else if (options?.fqn) {
      fqn = options.fqn;
      tabTitle = typeof tableOrName === 'string' ? tableOrName : tableOrName.name;
    } else if (options?.isLakehouse) {
      // No layer fallback: a lakehouse row either names its folder or sits at the root.
      const folder = (options.folderPath ?? '').replace(/\//g, '.');
      const cleanName = (typeof tableOrName === 'string' ? tableOrName : tableOrName.name).replace(/\.parquet$/i, '');
      fqn = ['lakehouse', folder, cleanName].filter(Boolean).join('.');
      tabTitle = cleanName;
    } else if (options?.sourceName) {
      const sourceAlias = options.sourceName.toLowerCase().replace(/[^a-z0-9_]/g, '_');
      const schema = options.schemaName || 'public';
      let bareTable = typeof tableOrName === 'string' ? tableOrName : tableOrName.name;
      if (bareTable.startsWith(`${schema}.`)) {
        bareTable = bareTable.slice(schema.length + 1);
      } else if (bareTable.includes('.')) {
        bareTable = bareTable.split('.').slice(1).join('.');
      }
      fqn = `${sourceAlias}.${schema}.${bareTable}`;
      tabTitle = `${schema}.${bareTable}`;
    } else {
      fqn = typeof tableOrName === 'string' ? tableOrName : tableOrName.name;
      tabTitle = fqn;
    }

    const newQuery = `-- Query Table\nSELECT * FROM ${fqn} LIMIT 50;\n`;

    // INDUSTRY STANDARD SINGLETON TAB BEHAVIOR (DEDUPLICATION):
    // If a tab for this table or entity is already open, activate/focus it!
    const existingTab = tabs.find(
      (t) =>
        t.title.toLowerCase() === tabTitle.toLowerCase() ||
        t.sql.trim() === newQuery.trim()
    );
    if (existingTab) {
      setActiveTabId(existingTab.id);
      return;
    }

    // Check if the current tab is pristine (has no results yet and is default fallback)
    const isCurrentTabPristine =
      !activeTab.result &&
      !activeTab.error &&
      (activeTab.sql === FALLBACK_DEFAULT_SQL || !activeTab.sql.trim());

    if (isCurrentTabPristine) {
      setTabs((prev) =>
        prev.map((t) =>
          t.id === activeTab.id ? { ...t, title: tabTitle, sql: newQuery } : t
        )
      );
      persistTabToBackend({ ...activeTab, title: tabTitle, sql: newQuery }, 0);
      // NOTE: Auto-execution disabled per user request
    } else {
      // OPEN IN A NEW TAB
      const newId = `tab-${Date.now()}`;
      const newTab: QueryTab = {
        id: newId,
        title: tabTitle,
        sql: newQuery,
        result: null,
        error: null,
        loading: false,
      };
      setTabs((prev) => [...prev, newTab]);
      setActiveTabId(newId);
      persistTabToBackend(newTab, tabs.length);
      // NOTE: Auto-execution disabled per user request
    }
  };

  const handleColumnClick = (colName: string) => {
    updateActiveTabSql(`${activeTab.sql.trimEnd()} ${colName}`);
  };

  const toggleTableExpand = (key: string) => {
    setExpandedTables((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const handleExport = async (format: 'parquet' | 'csv') => {
    if (!activeTab.sql.trim()) return;
    setExporting(format);
    const formatUpper = format.toUpperCase();
    showToast(`Generating ${formatUpper} export for "${activeTab.title}"...`, 'info', 10000);
    try {
      const cleanTitle = (activeTab.title || 'query_results')
        .toLowerCase()
        .replace(/\.parquet$/i, '')
        .replace(/[^a-z0-9_-]/g, '_')
        .replace(/_+/g, '_')
        .replace(/^_|_$/g, '') || 'query_results';
      const dateStr = new Date().toISOString().slice(0, 10);
      const downloadFilename = `${cleanTitle}_${dateStr}.${format}`;

      const res = await apiClient.post(
        '/query/export',
        {
          sql: activeTab.sql.trim(),
          format,
          filename: `${cleanTitle}_${dateStr}`,
        },
        { responseType: 'blob' }
      );

      const mimeType = format === 'parquet' ? 'application/octet-stream' : 'text/csv;charset=utf-8;';
      const blob = new Blob([res.data], { type: mimeType });
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', downloadFilename);
      document.body.appendChild(link);
      link.click();
      link.parentNode?.removeChild(link);
      setTimeout(() => window.URL.revokeObjectURL(url), 1000);
      showToast(`Export complete: downloaded ${downloadFilename}`, 'success', 4000);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Export failed';
      showToast(`Export failed: ${msg}`, 'error', 5000);
    } finally {
      setExporting(null);
    }
  };

  const handleHistoryClick = (item: QueryHistoryItem) => {
    const isCurrentTabPristine =
      !activeTab.result &&
      !activeTab.error &&
      (activeTab.sql === FALLBACK_DEFAULT_SQL || !activeTab.sql.trim());

    if (isCurrentTabPristine) {
      updateActiveTabSql(item.sql_text);
      executeQuerySql(item.sql_text, activeTab.id);
    } else {
      const newId = `tab-${Date.now()}`;
      const newTab: QueryTab = {
        id: newId,
        title: item.title || `History ${tabs.length + 1}`,
        sql: item.sql_text,
        result: null,
        error: null,
        loading: false,
      };
      setTabs((prev) => [...prev, newTab]);
      setActiveTabId(newId);
      executeQuerySql(item.sql_text, newId);
    }
  };

  const handleFormatQuery = () => {
    if (!activeTab.sql.trim()) return;
    const formatted = formatSql(activeTab.sql);
    updateActiveTabSql(formatted);
    if (editorRef.current) {
      editorRef.current.setValue(formatted);
    }
  };

  const handlePopOut = () => {
    try {
      localStorage.setItem('obelisk_popout_sql', activeTab.sql);
      localStorage.setItem('obelisk_popout_title', activeTab.title);
    } catch {}
    window.open('/query/popout', '_blank', 'noopener,noreferrer');
  };

  // Same mount-time closure problem as the run action: the formatter must read the
  // current tab, not the one that was open when Monaco registered the key.
  useEffect(() => {
    formatHandlerRef.current = handleFormatQuery;
  });

  // Draggable Schema Explorer Width Handler
  const handleStartSchemaResize = (e: React.MouseEvent) => {
    e.preventDefault();
    const startX = e.clientX;
    const startWidth = schemaWidth;

    const onMouseMove = (moveEvent: MouseEvent) => {
      const delta = moveEvent.clientX - startX;
      const newWidth = Math.max(180, Math.min(650, startWidth + delta));
      setSchemaWidth(newWidth);
    };

    const onMouseUp = () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };

    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
  };

  // Draggable Query Editor vs Results Height Handler
  const handleStartEditorResize = (e: React.MouseEvent) => {
    e.preventDefault();
    const startY = e.clientY;
    const startHeight = editorHeight;

    const onMouseMove = (moveEvent: MouseEvent) => {
      const delta = moveEvent.clientY - startY;
      const newHeight = Math.max(80, Math.min(window.innerHeight - 180, startHeight + delta));
      setEditorHeight(newHeight);
    };

    const onMouseUp = () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };

    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
  };

  const handleEditorDidMount = (editor: any, monaco: any) => {
    editorRef.current = editor;
    monacoRef.current = monaco;

    // Command: Run Query (Ctrl+Enter / Cmd+Enter)
    editor.addAction({
      id: 'obelisk-run-query',
      label: 'Obelisk: Run SQL Query',
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
      contextMenuGroupId: 'navigation',
      contextMenuOrder: 1,
      run: () => {
        runHandlerRef.current();
      },
    });

    // Command: Format Query (Shift+Alt+F)
    editor.addAction({
      id: 'obelisk-format-query',
      label: 'Obelisk: Format SQL',
      keybindings: [monaco.KeyMod.Shift | monaco.KeyMod.Alt | monaco.KeyCode.KeyF],
      contextMenuGroupId: 'modification',
      contextMenuOrder: 1,
      run: () => {
        formatHandlerRef.current();
      },
    });
  };

  // Register Monaco SQL Completion and Formatting Providers dynamically from catalogNodes
  useEffect(() => {
    if (!monacoRef.current) return;
    const monaco = monacoRef.current;

    const tableSuggestions: any[] = [];
    const columnSuggestions: any[] = [];

    catalogNodes.forEach((node) => {
      const isLakehouse = node.type === 'lakehouse' || node.id === 'lakehouse';

      if (isLakehouse) {
        node.directories?.forEach((dir) => {
          dir.tables?.forEach((table) => {
            const cleanName = table.name.replace(/\.parquet$/i, '');
            const folder = dir.path || dir.name || '';
            const qualifiedName = [folder.replace(/\//g, '.'), cleanName].filter(Boolean).join('.');

            tableSuggestions.push({
              label: qualifiedName,
              kind: monaco.languages.CompletionItemKind.Class,
              insertText: qualifiedName,
              detail: `Lakehouse Table${folder ? ` (${folder})` : ''}`,
              documentation: `Path: ${table.path || table.name}\nSize: ${table.size_bytes ? `${Math.round(table.size_bytes / 1024)} KB` : 'N/A'}\nColumns: ${table.columns?.length || 0}`,
              sortText: `0_${qualifiedName}`,
            });

            table.columns?.forEach((col) => {
              columnSuggestions.push({
                label: col.name,
                kind: monaco.languages.CompletionItemKind.Field,
                insertText: col.name,
                detail: `${col.data_type} • ${cleanName}`,
                documentation: `Column from Lakehouse table '${qualifiedName}'`,
                sortText: `1_${col.name}`,
              });
            });
          });
        });
      } else {
        const sourceAlias = (node.name || node.id).toLowerCase().replace(/[^a-z0-9_]/g, '_');
        node.tables?.forEach((table) => {
          const schema = table.schema || 'public';
          let bareTable = table.name;
          if (bareTable.startsWith(`${schema}.`)) {
            bareTable = bareTable.slice(schema.length + 1);
          }
          const fullQualified = `${sourceAlias}.${schema}.${bareTable}`;

          tableSuggestions.push({
            label: fullQualified,
            kind: monaco.languages.CompletionItemKind.Struct,
            insertText: fullQualified,
            detail: `Remote Table (${node.name})`,
            documentation: `Source: ${node.name}\nSchema: ${schema}\nSize: ${tableSizeLabel(table) ?? 'Unknown'}`,
            sortText: `0_${fullQualified}`,
          });

          tableSuggestions.push({
            label: `${schema}.${bareTable}`,
            kind: monaco.languages.CompletionItemKind.Struct,
            insertText: `${schema}.${bareTable}`,
            detail: `Table in ${sourceAlias}`,
            sortText: `0_${schema}_${bareTable}`,
          });

          table.columns?.forEach((col) => {
            columnSuggestions.push({
              label: col.name,
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: col.name,
              detail: `${col.data_type} • ${bareTable}`,
              documentation: `Column from external table '${node.name}.${schema}.${bareTable}'`,
              sortText: `1_${col.name}`,
            });
          });
        });
      }
    });

    const keywordSuggestions = [
      'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET',
      'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN',
      'ON', 'USING', 'UNION', 'UNION ALL', 'EXCEPT', 'INTERSECT', 'WITH', 'AS',
      'DISTINCT', 'ALL', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT',
      'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'ILIKE', 'IS NULL', 'IS NOT NULL',
      'ASC', 'DESC', 'NULLS FIRST', 'NULLS LAST', 'OVER', 'PARTITION BY', 'WINDOW',
      'FILTER', 'QUALIFY', 'CREATE TABLE', 'INSERT INTO', 'SHOW TABLES', 'DESCRIBE'
    ].map((kw) => ({
      label: kw,
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: kw,
      detail: 'SQL Keyword',
      sortText: `3_${kw}`,
    }));

    const functionSuggestions = [
      { label: 'read_parquet', detail: "read_parquet('file:///...')", doc: 'DuckDB native Parquet scanner', snippet: "read_parquet('${1:storage_uri}')" },
      { label: 'read_csv', detail: "read_csv('file:///...')", doc: 'DuckDB native CSV scanner', snippet: "read_csv('${1:file_path}')" },
      { label: 'COUNT(*)', detail: 'COUNT(*) Aggregate', doc: 'Returns the count of all rows', snippet: 'COUNT(*)' },
      { label: 'COUNT', detail: 'COUNT(expression)', doc: 'Count non-null values', snippet: 'COUNT(${1:column})' },
      { label: 'SUM', detail: 'SUM(expression)', doc: 'Sum of numeric column', snippet: 'SUM(${1:column})' },
      { label: 'AVG', detail: 'AVG(expression)', doc: 'Average of numeric values', snippet: 'AVG(${1:column})' },
      { label: 'MIN', detail: 'MIN(expression)', doc: 'Minimum value', snippet: 'MIN(${1:column})' },
      { label: 'MAX', detail: 'MAX(expression)', doc: 'Maximum value', snippet: 'MAX(${1:column})' },
      { label: 'COALESCE', detail: 'COALESCE(val1, val2, ...)', doc: 'Returns first non-null argument', snippet: 'COALESCE(${1:val1}, ${2:val2})' },
      { label: 'DATE_TRUNC', detail: "DATE_TRUNC('day', timestamp)", doc: 'Truncates date to specified precision', snippet: "DATE_TRUNC('${1:day}', ${2:timestamp})" },
      { label: 'ROW_NUMBER', detail: 'ROW_NUMBER() OVER (...)', doc: 'Window function assigning sequential row numbers', snippet: 'ROW_NUMBER() OVER (PARTITION BY ${1:col} ORDER BY ${2:col})' },
      { label: 'CONCAT', detail: 'CONCAT(str1, str2, ...)', doc: 'Concatenates multiple string expressions', snippet: 'CONCAT(${1:str1}, ${2:str2})' },
      { label: 'CURRENT_DATE', detail: 'Current date', doc: 'Returns current system date', snippet: 'CURRENT_DATE' },
      { label: 'NOW', detail: 'NOW() current timestamp', doc: 'Returns current timestamp', snippet: 'NOW()' },
      { label: 'STRFTIME', detail: "STRFTIME(timestamp, '%Y-%m-%d')", doc: 'Formats date to string', snippet: "STRFTIME(${1:timestamp}, '${2:%Y-%m-%d}')" },
    ].map((fn) => ({
      label: fn.label,
      kind: monaco.languages.CompletionItemKind.Function,
      insertText: fn.snippet,
      insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
      detail: fn.detail,
      documentation: fn.doc,
      sortText: `2_${fn.label}`,
    }));

    const snippetSuggestions = [
      {
        label: 'SELECT * FROM table LIMIT 50',
        kind: monaco.languages.CompletionItemKind.Snippet,
        insertText: 'SELECT *\nFROM ${1:table_name}\nLIMIT 50;',
        insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
        detail: 'Quick Select Query',
        sortText: '00_snippet_select',
      },
      {
        label: 'WITH cte AS (...) SELECT ...',
        kind: monaco.languages.CompletionItemKind.Snippet,
        insertText: 'WITH ${1:cte_name} AS (\n  SELECT ${2:*}\n  FROM ${3:source_table}\n)\nSELECT *\nFROM ${1:cte_name}\nLIMIT 50;',
        insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
        detail: 'Common Table Expression (CTE)',
        sortText: '00_snippet_cte',
      },
      {
        label: 'GROUP BY aggregation',
        kind: monaco.languages.CompletionItemKind.Snippet,
        insertText: 'SELECT\n  ${1:category},\n  COUNT(*) AS total_count\nFROM ${2:table_name}\nGROUP BY 1\nORDER BY 2 DESC\nLIMIT 50;',
        insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
        detail: 'Aggregated Group By Template',
        sortText: '00_snippet_groupby',
      },
    ];

    const providerDisposable = monaco.languages.registerCompletionItemProvider('sql', {
      provideCompletionItems: (model: any, position: any) => {
        const word = model.getWordUntilPosition(position);
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        };

        const suggestions = [
          ...tableSuggestions,
          ...columnSuggestions,
          ...functionSuggestions,
          ...snippetSuggestions,
          ...keywordSuggestions,
        ].map((item) => ({ ...item, range }));

        return { suggestions };
      },
    });

    const formatDisposable = monaco.languages.registerDocumentFormattingEditProvider('sql', {
      provideDocumentFormattingEdits: (model: any) => {
        const text = model.getValue();
        const formatted = formatSql(text);
        return [
          {
            range: model.getFullModelRange(),
            text: formatted,
          },
        ];
      },
    });

    return () => {
      providerDisposable.dispose();
      formatDisposable.dispose();
    };
  }, [catalogNodes]);

  return (
    <div className="flex-1 flex flex-col h-full bg-slate-50 overflow-hidden">
      {/* Top Toolbar */}
      <div className="h-14 border-b border-slate-200 bg-white px-5 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-2">
          {/* Run Query Button */}
          <button
            onClick={handleExecute}
            disabled={activeTab.loading}
            className="flex items-center gap-2 px-4 py-2 bg-sky-600 hover:bg-sky-700 text-white rounded-lg text-xs font-semibold shadow-xs transition-colors disabled:opacity-50"
          >
            {activeTab.loading ? (
              <>
                <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                <span>Running...</span>
              </>
            ) : (
              <>
                <Play className="w-3.5 h-3.5 fill-current" />
                <span>Run Query</span>
                <kbd className="text-[10px] bg-sky-700 px-1 rounded text-sky-100 font-mono">
                  ⌘↵
                </kbd>
              </>
            )}
          </button>

          {/* Format SQL Button */}
          <button
            onClick={handleFormatQuery}
            disabled={activeTab.loading || !activeTab.sql.trim()}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-slate-100 hover:bg-slate-200/80 text-slate-700 rounded-lg text-xs font-medium border border-slate-200 transition-colors disabled:opacity-50"
            title="Format & Beautify SQL (Shift+Alt+F)"
          >
            <Sparkles className="w-3.5 h-3.5 text-amber-500" />
            <span>Format</span>
            <kbd className="hidden sm:inline-block text-[10px] bg-slate-200 px-1 rounded text-slate-500 font-mono">
              ⇧⌥F
            </kbd>
          </button>

          {/* Toggle Minimap */}
          <button
            onClick={() => setShowMinimap((prev) => !prev)}
            className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium border transition-colors ${
              showMinimap
                ? 'bg-sky-50 border-sky-300 text-sky-800'
                : 'bg-slate-100 border-slate-200 text-slate-600 hover:bg-slate-200/80'
            }`}
            title="Toggle Monaco Minimap"
          >
            <Map className="w-3.5 h-3.5 text-slate-500" />
            <span>Minimap</span>
          </button>
        </div>

        {/* Right Action Buttons: Pop-out, Schema Helper & History */}
        <div className="flex items-center gap-2">
          {/* Pop-out or Standalone Status */}
          {isPopout ? (
            <div className="flex items-center gap-1.5 px-2.5 py-1 bg-emerald-50 text-emerald-700 border border-emerald-200 rounded-lg text-xs font-semibold">
              <CheckCircle2 className="w-3.5 h-3.5 text-emerald-600" />
              <span>Standalone Studio</span>
            </div>
          ) : (
            <button
              onClick={handlePopOut}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-white border border-slate-200 text-slate-700 hover:bg-slate-100 rounded-lg text-xs font-medium shadow-2xs transition-colors"
              title="Pop out Query Studio into a standalone full-screen window or tab"
            >
              <ExternalLink className="w-3.5 h-3.5 text-slate-500" />
              <span>Pop Out</span>
            </button>
          )}

          <button
            onClick={() => setSchemaOpen(!schemaOpen)}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors ${
              schemaOpen
                ? 'bg-sky-50 border-sky-300 text-sky-800'
                : 'bg-white border-slate-200 text-slate-600 hover:bg-slate-100'
            }`}
          >
            <Table2 className="w-3.5 h-3.5 text-slate-500" />
            <span>Schema Helper</span>
          </button>

          <button
            onClick={() => setHistoryOpen(!historyOpen)}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors ${
              historyOpen
                ? 'bg-sky-50 border-sky-300 text-sky-800'
                : 'bg-white border-slate-200 text-slate-600 hover:bg-slate-100'
            }`}
          >
            <History className="w-3.5 h-3.5 text-slate-400" />
            <span>History ({history.length})</span>
          </button>
        </div>
      </div>

      {/* Editor & Sidebars Split Area */}
      <div className="flex-1 flex overflow-hidden">
        {/* Left Schema Helper Drawer */}
        {schemaOpen && (
          <>
            <aside
              style={{ width: `${schemaWidth}px` }}
              className="border-r border-slate-200 bg-white flex flex-col shrink-0 select-none overflow-hidden"
            >
            <div className="p-3 border-b border-slate-200 font-bold text-xs text-slate-800 flex items-center justify-between">
              <span className="flex items-center gap-1.5">
                <Database className="w-3.5 h-3.5 text-sky-600" />
                Schema Explorer
              </span>
              <button
                onClick={fetchCatalog}
                className="text-slate-400 hover:text-slate-600 p-1 rounded"
                title="Refresh tables"
              >
                <RefreshCw className="w-3 h-3" />
              </button>
            </div>
            <div className="flex-1 overflow-y-auto p-2 space-y-3 text-xs">
              {catalogNodes.map((node) => {
                const isLakehouse = node.type === 'lakehouse' || node.id === 'lakehouse';
                const sourceGroupKey = `source:${node.id}`;
                const isSourceCollapsed = !!collapsedGroups[sourceGroupKey];
                const totalCount = isLakehouse
                  ? (node.directories?.reduce((acc, d) => acc + (d.tables?.length || 0), 0) || node.tables?.length || 0)
                  : (node.tables?.length || 0);

                return (
                  <div key={node.id} className="space-y-1">
                    {/* Node Section Header */}
                    <div
                      onClick={() => toggleGroup(sourceGroupKey)}
                      className="flex items-center justify-between font-semibold text-slate-800 text-[11px] px-2 py-1.5 bg-slate-100/90 hover:bg-slate-200/80 rounded border border-slate-200/80 cursor-pointer select-none transition-colors"
                    >
                      <span className="flex items-center gap-1.5 truncate">
                        {isSourceCollapsed ? (
                          <ChevronRight className="w-3.5 h-3.5 text-slate-500 shrink-0" />
                        ) : (
                          <ChevronDown className="w-3.5 h-3.5 text-slate-500 shrink-0" />
                        )}
                        {isLakehouse ? (
                          <Layers className="w-3.5 h-3.5 text-indigo-600 shrink-0" />
                        ) : (
                          <Database className="w-3.5 h-3.5 text-sky-600 shrink-0" />
                        )}
                        <span className="truncate">{node.name}</span>
                      </span>
                      <span className="text-[9px] font-mono font-medium px-1.5 py-0.5 bg-white rounded text-slate-500 border border-slate-200">
                        {totalCount}
                      </span>
                    </div>

                    {!isSourceCollapsed && (
                      <>
                        {/* Lakehouse: Group by Folders (Uncollapsed by default) */}
                        {isLakehouse && node.directories && node.directories.length > 0 ? (
                      <div className="space-y-1 pl-1">
                        {node.directories.map((dir) => {
                          const groupKey = `lakehouse_dir:${dir.path}`;
                          const isCollapsed = !!collapsedGroups[groupKey];
                          const indentPadding = Math.min((dir.depth || 0) * 10, 24);

                          return (
                            <div
                              key={groupKey}
                              style={{ marginLeft: `${indentPadding}px` }}
                              className="space-y-0.5"
                            >
                              {/* Folder Header */}
                              <div
                                onClick={() => toggleGroup(groupKey)}
                                className="flex items-center justify-between px-1.5 py-1 rounded hover:bg-slate-100 cursor-pointer select-none group text-slate-700"
                              >
                                <div className="flex items-center gap-1.5 truncate flex-1 font-mono text-[11px] font-semibold text-slate-700">
                                  {isCollapsed ? (
                                    <ChevronRight className="w-3 h-3 text-slate-400 shrink-0" />
                                  ) : (
                                    <ChevronDown className="w-3 h-3 text-slate-400 shrink-0" />
                                  )}
                                  <Folder className="w-3.5 h-3.5 text-amber-500 shrink-0" />
                                  <span className="truncate">{dir.name}</span>
                                </div>
                                <span className="text-[9px] font-mono text-slate-400 px-1 rounded bg-slate-50 border border-slate-200">
                                  {dir.tables?.length || 0}
                                </span>
                              </div>

                              {/* Tables under Folder (Uncollapsed by default) */}
                              {!isCollapsed && (
                                <div className="pl-3 space-y-0.5 border-l border-slate-200/70 ml-2">
                                  {dir.tables && dir.tables.length > 0 ? (
                                    dir.tables.map((t) => {
                                      const tableKey = `lakehouse.${dir.path}.${t.name}`;
                                      const isTableExpanded = !!expandedTables[tableKey];

                                      return (
                                        <div key={tableKey} className="space-y-0.5">
                                          <div className="flex items-center justify-between px-1.5 py-0.5 rounded hover:bg-slate-100 group cursor-pointer">
                                            <button
                                              onClick={(e) => {
                                                e.stopPropagation();
                                                toggleTableExpand(tableKey);
                                              }}
                                              className="text-slate-400 hover:text-slate-600 p-0.5"
                                            >
                                              {isTableExpanded ? (
                                                <ChevronDown className="w-3 h-3" />
                                              ) : (
                                                <ChevronRight className="w-3 h-3" />
                                              )}
                                            </button>
                                            <span
                                              onClick={() =>
                                                handleTableClick(t, {
                                                  isLakehouse: true,
                                                  folderPath: dir.path,
                                                  fqn: t.fqn,
                                                })
                                              }
                                              className="flex-1 font-mono text-[11px] text-slate-700 hover:text-indigo-600 truncate ml-1 font-medium"
                                              title={`Click to query ${t.fqn || `${dir.path}.${t.name}`}`}
                                            >
                                              {t.name}
                                            </span>
                                            {tableSizeLabel(t) != null && (
                                              <span className="text-[9px] text-slate-500 font-mono bg-slate-100 px-1 py-0.2 rounded shrink-0">
                                                {tableSizeLabel(t)}
                                              </span>
                                            )}
                                          </div>

                                          {/* Expandable columns */}
                                          {isTableExpanded && t.columns && (
                                            <div className="ml-4 pl-2 border-l border-slate-200 space-y-0.5 py-0.5">
                                              {t.columns.map((c) => (
                                                <div
                                                  key={c.name}
                                                  onClick={() => handleColumnClick(c.name)}
                                                  className="flex items-center justify-between text-[10px] font-mono py-0.5 px-1 rounded hover:bg-indigo-50 text-slate-600 hover:text-indigo-700 cursor-pointer"
                                                  title="Click to insert column name"
                                                >
                                                  <span>{c.name}</span>
                                                  <span className="text-slate-400 text-[9px]">{c.data_type}</span>
                                                </div>
                                              ))}
                                            </div>
                                          )}
                                        </div>
                                      );
                                    })
                                  ) : (
                                    <div className="text-[10px] text-slate-400 italic py-0.5 px-2">No tables in folder</div>
                                  )}
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    ) : isLakehouse && node.tables && node.tables.length > 0 ? (
                      /* Fallback for flat lakehouse tables if directories not populated */
                      <div className="pl-3 space-y-0.5">
                        {node.tables.map((t) => {
                          const tableKey = `${node.id}.${t.name}`;
                          const isTableExpanded = !!expandedTables[tableKey];
                          return (
                            <div key={tableKey} className="space-y-0.5">
                              <div className="flex items-center justify-between px-1.5 py-0.5 rounded hover:bg-slate-100 group cursor-pointer">
                                <button
                                  onClick={() => toggleTableExpand(tableKey)}
                                  className="text-slate-400 hover:text-slate-600 p-0.5"
                                >
                                  {isTableExpanded ? (
                                    <ChevronDown className="w-3 h-3" />
                                  ) : (
                                    <ChevronRight className="w-3 h-3" />
                                  )}
                                </button>
                                <span
                                  onClick={() => handleTableClick(t, { isLakehouse: true, folderPath: '', fqn: t.fqn })}
                                  className="flex-1 font-mono text-[11px] font-medium text-slate-700 hover:text-indigo-600 truncate ml-1"
                                >
                                  {t.name}
                                </span>
                                {tableSizeLabel(t) != null && (
                                  <span className="text-[9px] text-slate-500 font-mono bg-slate-100 px-1.5 py-0.5 rounded shrink-0">
                                    {tableSizeLabel(t)}
                                  </span>
                                )}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    ) : (
                      /* External Databases: Group by Schema (Uncollapsed by default) */
                      <div className="space-y-1 pl-1">
                        {(() => {
                          const schemaGroups = (node.tables || []).reduce<Record<string, CatalogTable[]>>((acc, t) => {
                            const schema = t.schema_name || 'public';
                            if (!acc[schema]) acc[schema] = [];
                            acc[schema].push(t);
                            return acc;
                          }, {});

                          const schemas = Object.keys(schemaGroups).sort();
                          if (schemas.length === 0) {
                            return <div className="text-[11px] text-slate-400 px-2 italic">No tables found</div>;
                          }

                          return schemas.map((schemaName) => {
                            const groupKey = `db_schema:${node.id}:${schemaName}`;
                            const isCollapsed = !!collapsedGroups[groupKey];
                            const tablesInSchema = schemaGroups[schemaName];

                            return (
                              <div key={groupKey} className="space-y-0.5">
                                {/* Schema Header */}
                                <div
                                  onClick={() => toggleGroup(groupKey)}
                                  className="flex items-center justify-between px-1.5 py-1 rounded hover:bg-slate-100 cursor-pointer select-none group text-slate-700"
                                >
                                  <div className="flex items-center gap-1.5 truncate flex-1 font-mono text-[11px] font-semibold text-slate-700">
                                    {isCollapsed ? (
                                      <ChevronRight className="w-3 h-3 text-slate-400 shrink-0" />
                                    ) : (
                                      <ChevronDown className="w-3 h-3 text-slate-400 shrink-0" />
                                    )}
                                    <Table2 className="w-3.5 h-3.5 text-sky-600 shrink-0" />
                                    <span className="truncate">{schemaName}</span>
                                  </div>
                                  <span className="text-[9px] font-mono text-slate-400 px-1 rounded bg-slate-50 border border-slate-200">
                                    {tablesInSchema.length}
                                  </span>
                                </div>

                                {/* Tables under Schema (Uncollapsed by default) */}
                                {!isCollapsed && (
                                  <div className="pl-3 space-y-0.5 border-l border-slate-200/70 ml-2">
                                    {tablesInSchema.map((t) => {
                                      const cleanTableName = t.name.startsWith(`${schemaName}.`)
                                        ? t.name.slice(schemaName.length + 1)
                                        : t.name.includes('.')
                                        ? t.name.split('.').slice(1).join('.')
                                        : t.name;
                                      const tableKey = `${node.id}.${schemaName}.${cleanTableName}`;
                                      const isTableExpanded = !!expandedTables[tableKey];

                                      return (
                                        <div key={tableKey} className="space-y-0.5">
                                          <div className="flex items-center justify-between px-1.5 py-0.5 rounded hover:bg-slate-100 group cursor-pointer">
                                            <button
                                              onClick={(e) => {
                                                e.stopPropagation();
                                                toggleTableExpand(tableKey);
                                              }}
                                              className="text-slate-400 hover:text-slate-600 p-0.5"
                                            >
                                              {isTableExpanded ? (
                                                <ChevronDown className="w-3 h-3" />
                                              ) : (
                                                <ChevronRight className="w-3 h-3" />
                                              )}
                                            </button>
                                            <span
                                              onClick={() =>
                                                handleTableClick(t, {
                                                  isLakehouse: false,
                                                  sourceName: node.name,
                                                  schemaName,
                                                  fqn: t.fqn,
                                                })
                                              }
                                              className="flex-1 font-mono text-[11px] text-slate-700 hover:text-sky-600 truncate ml-1 font-medium"
                                              title={`Click to query ${t.fqn || `${node.name}.${schemaName}.${cleanTableName}`}`}
                                            >
                                              {cleanTableName}
                                            </span>
                                            {tableSizeLabel(t) != null && (
                                              <span className="text-[9px] text-slate-500 font-mono bg-slate-100 px-1 py-0.2 rounded shrink-0">
                                                {tableSizeLabel(t)}
                                              </span>
                                            )}
                                          </div>

                                          {/* Expandable columns */}
                                          {isTableExpanded && t.columns && (
                                            <div className="ml-4 pl-2 border-l border-slate-200 space-y-0.5 py-0.5">
                                              {t.columns.map((c) => (
                                                <div
                                                  key={c.name}
                                                  onClick={() => handleColumnClick(c.name)}
                                                  className="flex items-center justify-between text-[10px] font-mono py-0.5 px-1 rounded hover:bg-sky-50 text-slate-600 hover:text-sky-700 cursor-pointer"
                                                  title="Click to insert column name"
                                                >
                                                  <span>{c.name}</span>
                                                  <span className="text-slate-400 text-[9px]">{c.data_type}</span>
                                                </div>
                                              ))}
                                            </div>
                                          )}
                                        </div>
                                      );
                                    })}
                                  </div>
                                )}
                              </div>
                            );
                          });
                        })()}
                      </div>
                    )}
                      </>
                    )}
                  </div>
                );
              })}
            </div>
          </aside>
          {/* Draggable Schema Explorer Width Handle */}
          <div
            onMouseDown={handleStartSchemaResize}
            className="w-1.5 hover:w-2 bg-slate-200/80 hover:bg-sky-500 cursor-col-resize shrink-0 transition-all flex items-center justify-center group select-none z-20"
            title="Drag to resize Schema Explorer"
          >
            <div className="h-8 w-0.5 bg-slate-400 group-hover:bg-white rounded-full" />
          </div>
        </>
      )}

        <div className="flex-1 flex flex-col overflow-hidden">
          {/* Query Tabs Bar (Option C - Clean Neutral Tabs) */}
          <div className="h-9 bg-slate-100/90 border-b border-slate-200 flex items-center justify-between px-2 select-none shrink-0 overflow-x-auto scrollbar-none">
            <div className="flex items-center gap-1 h-full pt-1 overflow-x-auto">
              {tabs.map((tab) => {
                const isActive = tab.id === activeTabId;
                return (
                  <div
                    key={tab.id}
                    onClick={() => setActiveTabId(tab.id)}
                    className={`group relative h-8 px-3 rounded-t-md flex items-center gap-2 text-xs cursor-pointer border-t-2 transition-all font-mono max-w-[220px] ${
                      isActive
                        ? 'bg-white border-t-sky-600 text-slate-900 font-semibold shadow-2xs border-x border-slate-200 -mb-[1px] z-10'
                        : 'border-t-transparent bg-slate-200/50 hover:bg-slate-200/90 text-slate-500 hover:text-slate-800'
                    }`}
                    title={tab.title}
                  >
                    {/* Clean Neutral File Icon */}
                    <FileCode className="w-3.5 h-3.5 text-slate-400 shrink-0" />

                    {/* Tab Title */}
                    {editingTabId === tab.id ? (
                      <input
                        type="text"
                        autoFocus
                        value={editingTitle}
                        onChange={(e) => setEditingTitle(e.target.value)}
                        onBlur={handleSaveRenameTab}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter') handleSaveRenameTab();
                          if (e.key === 'Escape') setEditingTabId(null);
                        }}
                        onClick={(e) => e.stopPropagation()}
                        className="text-xs font-mono bg-white border border-sky-400 rounded px-1 py-0.5 outline-hidden text-slate-800 w-24"
                      />
                    ) : (
                      <span
                        onDoubleClick={(e) => handleStartRenameTab(tab, e)}
                        className="truncate text-[11px]"
                      >
                        {tab.title}
                      </span>
                    )}

                    {/* Status Dot / Spinner */}
                    {tab.loading ? (
                      <RefreshCw className="w-3 h-3 animate-spin text-sky-600 shrink-0 ml-1" />
                    ) : tab.error ? (
                      <span className="w-1.5 h-1.5 rounded-full bg-rose-500 shrink-0 ml-1" title="Has error" />
                    ) : tab.result ? (
                      <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 shrink-0 ml-1" title="Executed" />
                    ) : null}

                    {/* Close Tab Button */}
                    {tabs.length > 1 && (
                      <button
                        onClick={(e) => handleCloseTab(tab.id, e)}
                        className="opacity-0 group-hover:opacity-100 p-0.5 rounded hover:bg-slate-200 text-slate-400 hover:text-slate-700 transition-opacity ml-1"
                        title="Close Tab"
                      >
                        <X className="w-3 h-3" />
                      </button>
                    )}
                  </div>
                );
              })}

              {/* Add New Tab Button */}
              <button
                onClick={() => handleAddTab()}
                className="h-7 w-7 rounded-md hover:bg-slate-200 text-slate-500 hover:text-slate-800 flex items-center justify-center transition-colors ml-0.5"
                title="New Query Tab (+)"
              >
                <Plus className="w-3.5 h-3.5" />
              </button>
            </div>
          </div>

          {/* Editor Context Ribbon (Option C - Minimalist, Professional Target & Schema Controls) */}
          <div className="h-9 bg-slate-50/90 border-b border-slate-200 px-3 flex items-center justify-between text-xs select-none shrink-0">
            <div className="flex items-center gap-3">
              {/* Target Selector */}
              <div className="flex items-center gap-1.5">
                <span className="text-slate-400 font-medium text-[11px] flex items-center gap-1">
                  <Database className="w-3.5 h-3.5 text-slate-400" />
                  Target:
                </span>
                <div className="relative">
                  <select
                    value={activeTab.targetId || 'duckdb'}
                    onChange={(e) => handleSetTarget(e.target.value)}
                    className="appearance-none bg-white border border-slate-200 hover:border-slate-300 rounded-md pl-2.5 pr-7 py-1 text-xs font-semibold text-slate-700 shadow-2xs focus:outline-sky-500 cursor-pointer"
                  >
                    {targets.length === 0 ? (
                      <option value="duckdb">DuckDB</option>
                    ) : (
                      targets.map((tgt) => (
                        <option key={tgt.id} value={tgt.id}>
                          {tgt.name}
                        </option>
                      ))
                    )}
                  </select>
                  <ChevronDown className="w-3 h-3 text-slate-400 absolute right-2 top-2 pointer-events-none" />
                </div>
              </div>

              {/* Schema Selector (if active target has schemas) */}
              {activeTarget.schemas && activeTarget.schemas.length > 0 && (
                <div className="flex items-center gap-1.5">
                  <span className="text-slate-400 font-medium text-[11px]">Schema:</span>
                  <div className="relative">
                    <select
                      value={activeTab.schema || activeTarget.schemas[0] || 'public'}
                      onChange={(e) => handleSetSchema(e.target.value)}
                      className="appearance-none bg-white border border-slate-200 hover:border-slate-300 rounded-md pl-2.5 pr-6 py-1 text-xs font-medium text-slate-700 shadow-2xs focus:outline-sky-500 cursor-pointer"
                    >
                      {activeTarget.schemas.map((s) => (
                        <option key={s} value={s}>
                          {s}
                        </option>
                      ))}
                    </select>
                    <ChevronDown className="w-3 h-3 text-slate-400 absolute right-2 top-2 pointer-events-none" />
                  </div>
                </div>
              )}
            </div>

            {/* Right Info: Database & Autocommit */}
            <div className="flex items-center gap-3 text-[11px]">
              {activeTarget.database && (
                <span className="text-slate-400 font-mono hidden md:inline">
                  DB: <span className="text-slate-700 font-semibold">{activeTarget.database}</span>
                </span>
              )}
              <span className="text-[10px] text-slate-500 font-mono bg-white px-2 py-0.5 rounded border border-slate-200 shadow-2xs">
                Auto-commit
              </span>
            </div>
          </div>

          {/* Monaco Editor Pane */}
          <div
            style={{ height: `${editorHeight}px` }}
            className="border-b border-slate-200 bg-white shrink-0 overflow-hidden"
          >
            <Editor
              height="100%"
              defaultLanguage="sql"
              theme="light"
              value={activeTab.sql}
              onChange={(val) => updateActiveTabSql(val || '')}
              onMount={handleEditorDidMount}
              options={{
                minimap: { enabled: showMinimap },
                fontSize: 13,
                fontFamily: 'JetBrains Mono, Fira Code, Menlo, monospace',
                lineNumbers: 'on',
                scrollBeyondLastLine: false,
                wordWrap: 'on',
                automaticLayout: true,
                padding: { top: 8, bottom: 8 },
                quickSuggestions: { other: true, comments: false, strings: true },
                suggestOnTriggerCharacters: true,
                acceptSuggestionOnEnter: 'smart',
                tabCompletion: 'on',
                parameterHints: { enabled: true },
                folding: true,
                foldingHighlight: true,
                matchBrackets: 'always',
                autoClosingBrackets: 'always',
                autoClosingQuotes: 'always',
                bracketPairColorization: { enabled: true },
                guides: { bracketPairs: true, indentation: true },
                renderWhitespace: 'selection',
                cursorBlinking: 'smooth',
                smoothScrolling: true,
                contextmenu: true,
                formatOnPaste: true,
              }}
            />
          </div>

          {/* Draggable Query / Results Resizer */}
          <div
            onMouseDown={handleStartEditorResize}
            className="h-1.5 hover:h-2 bg-slate-200/90 hover:bg-sky-500 cursor-row-resize shrink-0 transition-all flex items-center justify-center group select-none z-20"
            title="Drag up/down to resize Query and Results panes"
          >
            <div className="w-10 h-0.5 bg-slate-400 group-hover:bg-white rounded-full" />
          </div>

          {/* Results Telemetry Header */}
          <div className="h-10 border-b border-slate-200 bg-slate-100/70 px-5 flex items-center justify-between shrink-0 text-xs text-slate-600 font-mono">
            <div className="flex items-center gap-4">
              <span className="font-semibold text-slate-800 flex items-center gap-1">
                <FileSpreadsheet className="w-3.5 h-3.5 text-slate-500" /> Results
              </span>
              {activeTab.result && (
                <>
                  <span className="text-slate-300">•</span>
                  <span className="flex items-center gap-1">
                    <Hash className="w-3.5 h-3.5 text-slate-400" />
                    {activeTab.result.row_count} rows
                  </span>
                  <span className="text-slate-300">•</span>
                  <span className="flex items-center gap-1">
                    <Clock className="w-3.5 h-3.5 text-slate-400" />
                    {activeTab.result.duration_ms} ms
                  </span>
                  {activeTab.result.truncated && (
                    <>
                      <span className="text-slate-300">•</span>
                      {/* The engine stops reading at its row cap; without this the grid
                          looks like the whole table when it is a sample of it. */}
                      <span
                        className="flex items-center gap-1 px-1.5 py-0.5 rounded bg-amber-100 text-amber-800 border border-amber-200 font-semibold"
                        title="The result hit the engine's row cap. Export streams the full dataset to disk."
                      >
                        <AlertTriangle className="w-3 h-3" />
                        Showing first {activeTab.result.row_count.toLocaleString()} rows (result truncated —
                        use Export for full dataset)
                      </span>
                    </>
                  )}
                </>
              )}
            </div>

            {/* Right: Download Buttons and Engine Info */}
            <div className="flex items-center gap-3">
              {activeTab.result && activeTab.result.columns.length > 0 && (
                <div className="flex items-center gap-1.5">
                  <button
                    onClick={() => handleExport('csv')}
                    disabled={activeTab.loading || !!exporting}
                    className="flex items-center gap-1 px-2.5 py-1 bg-white hover:bg-slate-200/70 text-slate-700 rounded-md text-[11px] font-medium border border-slate-200 shadow-2xs transition-colors disabled:opacity-50"
                    title="Export Results to CSV"
                  >
                    <Download className="w-3 h-3 text-slate-500" />
                    <span>CSV</span>
                  </button>
                  <button
                    onClick={() => handleExport('parquet')}
                    disabled={activeTab.loading || !!exporting}
                    className="flex items-center gap-1 px-2.5 py-1 bg-white hover:bg-slate-200/70 text-slate-700 rounded-md text-[11px] font-medium border border-slate-200 shadow-2xs transition-colors disabled:opacity-50"
                    title="Export Results to Parquet"
                  >
                    <Download className="w-3 h-3 text-slate-500" />
                    <span>Parquet</span>
                  </button>
                </div>
              )}
              <div className="text-[11px] font-medium text-slate-500 flex items-center gap-1">
                <span>{activeTab.result?.target_name || activeTarget.name}</span>
              </div>
            </div>
          </div>

          {/* Results Area */}
          <div className="flex-1 p-4 overflow-hidden bg-slate-50">
            {activeTab.error ? (() => {
              const lines = activeTab.error.trim().split('\n');
              const headline = lines[0];
              const hasMore = lines.length > 1;
              return (
                <div className="p-4 bg-rose-50/90 border border-rose-200 rounded-xl text-rose-900 text-xs space-y-2">
                  <div className="flex items-center gap-2 font-bold text-rose-800">
                    <AlertCircle className="w-4 h-4 text-rose-600 shrink-0" />
                    <span>SQL Execution Error</span>
                  </div>
                  <div className="font-mono text-xs font-semibold text-rose-800 bg-white/80 p-2.5 rounded-lg border border-rose-200/80 break-words">
                    {headline}
                  </div>
                  {hasMore && (
                    <details className="text-[11px] text-rose-700 font-mono mt-1 group">
                      <summary className="cursor-pointer text-xs font-semibold text-rose-700 hover:text-rose-900 select-none py-1">
                        ▶ View Full Error Log / Traceback ({lines.length} lines)
                      </summary>
                      <pre className="mt-2 p-3 bg-slate-900 text-rose-300 rounded-lg overflow-x-auto max-h-80 whitespace-pre-wrap leading-relaxed shadow-inner">
                        {activeTab.error}
                      </pre>
                    </details>
                  )}
                </div>
              );
            })() : activeTab.result?.command_tag && activeTab.result?.columns.length === 0 ? (
              <div className="h-full flex items-center justify-center p-6">
                <div className="max-w-md w-full bg-white border border-emerald-200 rounded-2xl p-6 shadow-xs flex flex-col items-center text-center space-y-4">
                  <div className="w-12 h-12 rounded-full bg-emerald-50 border border-emerald-200 flex items-center justify-center text-emerald-600 shadow-2xs">
                    <CheckCircle2 className="w-6 h-6" />
                  </div>
                  <div>
                    <h3 className="text-sm font-bold text-slate-800">Command Executed Successfully</h3>
                    <p className="text-xs text-slate-500 mt-1">
                      Executed directly on <span className="font-semibold text-slate-700">{activeTab.result.target_name || 'Target Database'}</span>
                    </p>
                  </div>
                  <div className="w-full bg-slate-50 rounded-xl p-3 border border-slate-200 font-mono text-xs flex justify-around">
                    <div>
                      <div className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">Status Tag</div>
                      <div className="font-bold text-emerald-700 mt-1">{activeTab.result.command_tag}</div>
                    </div>
                    <div className="border-r border-slate-200" />
                    <div>
                      <div className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">Rows Affected</div>
                      <div className="font-bold text-slate-700 mt-1">{activeTab.result.row_count}</div>
                    </div>
                    <div className="border-r border-slate-200" />
                    <div>
                      <div className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">Duration</div>
                      <div className="font-bold text-slate-700 mt-1">{activeTab.result.duration_ms} ms</div>
                    </div>
                  </div>
                </div>
              </div>
            ) : (
              <DataGrid
                columns={activeTab.result?.columns || []}
                rows={activeTab.result?.rows || []}
                loading={activeTab.loading}
              />
            )}
          </div>
        </div>

        {/* Right Query History Drawer */}
        {historyOpen && (
          <aside className="w-72 border-l border-slate-200 bg-white flex flex-col shrink-0">
            <div className="p-3 border-b border-slate-200 font-bold text-xs text-slate-800 flex items-center justify-between">
              <span>Query Execution History</span>
              <button
                onClick={fetchHistory}
                className="text-slate-400 hover:text-slate-600"
              >
                <RefreshCw className="w-3 h-3" />
              </button>
            </div>
            <div className="flex-1 overflow-y-auto p-2 space-y-1.5">
              {history.length === 0 ? (
                <div className="p-6 text-center text-xs text-slate-400 font-mono">
                  No query history yet.
                </div>
              ) : (
                history.map((item) => (
                  <div
                    key={item.id}
                    onClick={() => handleHistoryClick(item)}
                    className="p-2 rounded-lg border border-slate-200 bg-slate-50/50 hover:bg-sky-50/60 hover:border-sky-300 transition-colors cursor-pointer text-left"
                  >
                    <div className="flex items-center justify-between text-[10px] font-mono text-slate-400 mb-1">
                      <span
                        className={`font-semibold ${
                          item.status === 'success'
                            ? 'text-emerald-600'
                            : 'text-rose-600'
                        }`}
                      >
                        {item.status.toUpperCase()}
                      </span>
                      <span>{item.duration_ms ? `${item.duration_ms}ms` : ''}</span>
                    </div>
                    <pre className="text-[11px] font-mono text-slate-700 line-clamp-2">
                      {item.sql_text}
                    </pre>
                  </div>
                ))
              )}
            </div>
          </aside>
        )}
      </div>

      {/* Floating Status Toast for Exports & Queries */}
      {toast && (
        <div
          className={`fixed bottom-5 right-6 z-50 flex items-center gap-2.5 px-4 py-2.5 rounded-xl shadow-lg border text-xs font-medium animate-in fade-in slide-in-from-bottom-2 duration-150 ${
            toast.type === 'success'
              ? 'bg-emerald-50 text-emerald-900 border-emerald-300'
              : toast.type === 'error'
              ? 'bg-rose-50 text-rose-900 border-rose-300'
              : 'bg-sky-50 text-sky-900 border-sky-300'
          }`}
        >
          {toast.type === 'success' ? (
            <CheckCircle2 className="w-4 h-4 text-emerald-600 shrink-0" />
          ) : toast.type === 'error' ? (
            <AlertCircle className="w-4 h-4 text-rose-600 shrink-0" />
          ) : (
            <RefreshCw className="w-4 h-4 text-sky-600 animate-spin shrink-0" />
          )}
          <span>{toast.message}</span>
          <button
            onClick={() => setToast(null)}
            className="ml-2 text-slate-400 hover:text-slate-600"
          >
            <X className="w-3.5 h-3.5" />
          </button>
        </div>
      )}
    </div>
  );
};
