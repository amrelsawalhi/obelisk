import React, { useState, useEffect } from 'react';
import { Sidebar, NavigationTab } from './components/layout/Sidebar';
import { Header } from './components/layout/Header';
import { CommandMenu } from './components/layout/CommandMenu';
import { IngestActivityDock } from './components/layout/IngestActivityDock';

// Views
import { HomeView } from './components/home/HomeView';
import { CatalogTree } from './components/catalog/CatalogTree';
import { TablePreview } from './components/catalog/TablePreview';
import { IngestModal } from './components/catalog/IngestModal';
import { BatchIngestModal } from './components/catalog/BatchIngestModal';
import { AddConnectionModal } from './components/catalog/AddConnectionModal';
import { QueryStudio } from './components/query/QueryStudio';
import { DbtStudio } from './components/dbt/DbtStudio';
import { NotebooksView } from './components/notebooks/NotebooksView';
import { PipelinesView } from './components/pipelines/PipelinesView';
import { DashboardsView } from './components/dashboards/DashboardsView';
import { FileManagerView } from './components/files/FileManagerView';
import { TerminalView } from './components/terminal/TerminalView';
import { ServicesView } from './components/services/ServicesView';
import { SettingsView } from './components/settings/SettingsView';
import { ErrorBoundary } from './components/common/ErrorBoundary';


import { api } from './lib/api';
import type { CatalogNode, CatalogTable, Connection } from './lib/types';
import { Database, Plus } from 'lucide-react';

export const App: React.FC = () => {
  const [activeTab, setActiveTab] = useState<NavigationTab>('home');
  const [isCommandOpen, setIsCommandOpen] = useState(false);
  const [activeQuerySql, setActiveQuerySql] = useState<string | undefined>(undefined);

  // Catalog State
  const [catalogNodes, setCatalogNodes] = useState<CatalogNode[]>([]);
  const [catalogLoading, setCatalogLoading] = useState(true);
  const [selectedTableInfo, setSelectedTableInfo] = useState<{
    sourceId: string;
    sourceName?: string;
    table: CatalogTable;
    isLakehouse: boolean;
  } | null>(null);

  const [activeNotebookTarget, setActiveNotebookTarget] = useState<{
    name: string;
    tableName: string;
    fqn: string;
  } | null>(null);

  const handleOpenInNotebook = (
    tableOrName: CatalogTable | string,
    customFqn?: string
  ) => {
    let tableName: string;
    let fqn: string;

    if (typeof tableOrName === 'string') {
      tableName = tableOrName;
      fqn = customFqn || tableOrName;
    } else {
      tableName = tableOrName.name;
      fqn = customFqn || tableOrName.fqn || tableOrName.name;
    }

    const cleanName = tableName.replace(/\.parquet$/i, '').replace(/[^a-zA-Z0-9_]/g, '_');
    setActiveNotebookTarget({
      name: `Explore_${cleanName}`,
      tableName: cleanName,
      fqn,
    });
    setActiveTab('notebooks');
  };

  // Modals
  const [isIngestOpen, setIsIngestOpen] = useState(false);
  const [batchIngestTarget, setBatchIngestTarget] = useState<{
    connectionId: string;
    connectionName: string;
    schemaName?: string;
    tables: CatalogTable[];
  } | null>(null);
  const [isAddConnectionOpen, setIsAddConnectionOpen] = useState(false);
  const [editingConnection, setEditingConnection] = useState<Connection | null>(null);

  const handleEditConnection = async (connId: string) => {
    try {
      const conns = await api.listConnections();
      const found = conns.find((c) => c.id === connId);
      if (found) {
        setEditingConnection(found);
        setIsAddConnectionOpen(true);
      }
    } catch (e) {
      console.error('Failed to load connection for edit', e);
    }
  };

  const handleDeleteConnection = async (connId: string) => {
    if (window.confirm('Are you sure you want to delete this connection?')) {
      try {
        await api.deleteConnection(connId);
        fetchCatalog();
      } catch (e) {
        console.error('Failed to delete connection', e);
      }
    }
  };

  const fetchCatalog = async () => {
    try {
      setCatalogLoading(true);
      const data = await api.getCatalogTree();
      setCatalogNodes(data);

      // Auto-select first table if nothing selected yet
      if (!selectedTableInfo && data.length > 0) {
        const lakehouse = data.find((n) => n.type === 'lakehouse');
        const firstDir = lakehouse?.directories?.find((d) => d.tables.length > 0) || lakehouse?.directories?.[0];
        if (firstDir && firstDir.tables.length > 0) {
          setSelectedTableInfo({
            sourceId: firstDir.id,
            table: firstDir.tables[0],
            isLakehouse: true,
          });
        } else if (data[0].tables?.length > 0) {
          setSelectedTableInfo({
            sourceId: data[0].id,
            table: data[0].tables[0],
            isLakehouse: data[0].type === 'lakehouse',
          });
        }
      }
    } catch {
      // Background retry
    } finally {
      setCatalogLoading(false);
    }
  };

  useEffect(() => {
    fetchCatalog();
  }, []);

  // Keyboard shortcut listener for Cmd+K / Ctrl+K
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        setIsCommandOpen((prev) => !prev);
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, []);

  const selectedTableKey = selectedTableInfo
    ? `${selectedTableInfo.sourceId}:${selectedTableInfo.table.name}`
    : null;

  const [visitedTabs, setVisitedTabs] = useState<Set<NavigationTab>>(new Set([activeTab]));

  useEffect(() => {
    setVisitedTabs((prev) => {
      if (prev.has(activeTab)) return prev;
      const next = new Set(prev);
      next.add(activeTab);
      return next;
    });
  }, [activeTab]);

  const urlParams = new URLSearchParams(window.location.search);
  const isPopout = urlParams.get('popout') === 'true' || window.location.pathname === '/query/popout';

  if (isPopout) {
    return (
      <div className="h-screen w-screen overflow-hidden flex flex-col bg-white">
        <QueryStudio isPopout={true} />
      </div>
    );
  }

  return (
    <div className="flex h-full w-full overflow-hidden bg-white text-slate-900 font-sans antialiased">
      {/* Primary Left Navigation */}

      <Sidebar activeTab={activeTab} onSelectTab={setActiveTab} />

      {/* Main Content Area */}
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Global Header */}
        <Header activeTab={activeTab} onOpenCommand={() => setIsCommandOpen(true)} />

        {/* View Switcher with Persistent Tab State */}
        <main className="flex-1 flex min-h-0 overflow-hidden relative">
          <ErrorBoundary>
            {visitedTabs.has('home') && (
              <div className={`flex-1 flex min-h-0 overflow-hidden ${activeTab === 'home' ? '' : 'hidden'}`}>
                <HomeView onNavigate={setActiveTab} />
              </div>
            )}

          {visitedTabs.has('catalog') && (
            <div className={`flex-1 flex min-h-0 overflow-hidden ${activeTab === 'catalog' ? '' : 'hidden'}`}>
              {/* Left Catalog Tree */}
              <CatalogTree
                nodes={catalogNodes}
                loading={catalogLoading}
                onRefresh={fetchCatalog}
                selectedTableKey={selectedTableKey}
                onSelectTable={(sourceId, table, isLakehouse, sourceName) => {
                  setSelectedTableInfo({ sourceId, sourceName, table, isLakehouse });
                }}
                onOpenAddConnection={() => {
                  setEditingConnection(null);
                  setIsAddConnectionOpen(true);
                }}
                onEditConnection={handleEditConnection}
                onDeleteConnection={handleDeleteConnection}
                onOpenBatchIngest={(connId, connName, schema, tables) => {
                  setBatchIngestTarget({
                    connectionId: connId,
                    connectionName: connName,
                    schemaName: schema,
                    tables: tables || [],
                  });
                }}
                onOpenInQuery={(query) => {
                  setActiveQuerySql(query);
                  setActiveTab('query');
                }}
                onOpenInNotebook={handleOpenInNotebook}
              />

              {/* Right Table Preview / Schema Viewer */}
              {selectedTableInfo ? (
                <TablePreview
                  sourceId={selectedTableInfo.sourceId}
                  sourceName={selectedTableInfo.sourceName}
                  table={selectedTableInfo.table}
                  isLakehouse={selectedTableInfo.isLakehouse}
                  onOpenIngest={() => setIsIngestOpen(true)}
                  onOpenInQuery={(query) => {
                    setActiveQuerySql(query);
                    setActiveTab('query');
                  }}
                  onOpenInNotebook={handleOpenInNotebook}
                />
              ) : (
                <div className="flex-1 flex flex-col items-center justify-center p-8 bg-slate-50/50 text-center">
                  <Database className="w-12 h-12 text-slate-300 mb-3" />
                  <h3 className="text-sm font-bold text-slate-700">Select a Table to Inspect</h3>
                  <p className="text-xs text-slate-400 max-w-sm mt-1">
                    Select any table in the Lakehouse or connect to an external source to inspect schemas and preview live rows.
                  </p>
                  <button
                    onClick={() => {
                      setEditingConnection(null);
                      setIsAddConnectionOpen(true);
                    }}
                    className="mt-4 flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold shadow-2xs transition-all"
                  >
                    <Plus className="w-3.5 h-3.5" />
                    <span>Add Source Connection</span>
                  </button>
                </div>
              )}

              {/* Single Table Ingest Modal */}
              {selectedTableInfo && (
                <IngestModal
                  isOpen={isIngestOpen}
                  onClose={() => setIsIngestOpen(false)}
                  connectionId={selectedTableInfo.sourceId}
                  table={selectedTableInfo.table}
                  onSuccess={() => {
                    setIsIngestOpen(false);
                    fetchCatalog();
                  }}
                />
              )}

              {/* Batch Database & Schema Ingest Modal */}
              {batchIngestTarget && (
                <BatchIngestModal
                  isOpen={batchIngestTarget !== null}
                  onClose={() => setBatchIngestTarget(null)}
                  connectionId={batchIngestTarget.connectionId}
                  connectionName={batchIngestTarget.connectionName}
                  schemaName={batchIngestTarget.schemaName}
                  availableTables={batchIngestTarget.tables}
                  onSuccess={() => {
                    setBatchIngestTarget(null);
                    fetchCatalog();
                  }}
                />
              )}

              {/* Add / Edit Connection Modal */}
              <AddConnectionModal
                isOpen={isAddConnectionOpen}
                initialConnection={editingConnection}
                onClose={() => {
                  setIsAddConnectionOpen(false);
                  setEditingConnection(null);
                }}
                onSuccess={() => {
                  setIsAddConnectionOpen(false);
                  setEditingConnection(null);
                  fetchCatalog();
                }}
              />
            </div>
          )}

          {visitedTabs.has('query') && (
            <div className={`flex-1 flex min-h-0 overflow-hidden ${activeTab === 'query' ? '' : 'hidden'}`}>
              <QueryStudio initialSql={activeQuerySql} />
            </div>
          )}

          {visitedTabs.has('dbt') && (
            <div className={`flex-1 flex min-h-0 overflow-hidden ${activeTab === 'dbt' ? '' : 'hidden'}`}>
              <DbtStudio />
            </div>
          )}

          {visitedTabs.has('notebooks') && (
            <div className={`flex-1 flex min-h-0 overflow-hidden ${activeTab === 'notebooks' ? '' : 'hidden'}`}>
              <NotebooksView
                target={activeNotebookTarget}
                onClearTarget={() => setActiveNotebookTarget(null)}
              />
            </div>
          )}

          {visitedTabs.has('pipelines') && (
            <div className={`flex-1 flex min-h-0 overflow-hidden ${activeTab === 'pipelines' ? '' : 'hidden'}`}>
              <PipelinesView
                onOpenInQuery={(sql) => {
                  setActiveQuerySql(sql);
                  setActiveTab('query');
                }}
                onNavigateToSettings={() => setActiveTab('settings')}
              />
            </div>
          )}

          {visitedTabs.has('dashboards') && (
            <div className={`flex-1 flex min-h-0 overflow-hidden ${activeTab === 'dashboards' ? '' : 'hidden'}`}>
              <DashboardsView />
            </div>
          )}

          {visitedTabs.has('files') && (
            <div className={`flex-1 flex min-h-0 overflow-hidden ${activeTab === 'files' ? '' : 'hidden'}`}>
              <FileManagerView />
            </div>
          )}

          {visitedTabs.has('terminal') && (
            <div className={`flex-1 flex min-h-0 overflow-hidden ${activeTab === 'terminal' ? '' : 'hidden'}`}>
              <TerminalView />
            </div>
          )}

          {visitedTabs.has('services') && (
            <div className={`flex-1 flex min-h-0 overflow-hidden ${activeTab === 'services' ? '' : 'hidden'}`}>
              <ServicesView />
            </div>
          )}


            {visitedTabs.has('settings') && (
              <div className={`flex-1 flex min-h-0 overflow-hidden ${activeTab === 'settings' ? '' : 'hidden'}`}>
                <SettingsView />
              </div>
            )}
          </ErrorBoundary>
        </main>
      </div>

      {/* Global Cmd+K Command Search Menu */}
      <CommandMenu
        isOpen={isCommandOpen}
        onClose={() => setIsCommandOpen(false)}
        onNavigate={(tab) => {
          setActiveTab(tab);
          setIsCommandOpen(false);
        }}
      />

      {/* Global Minimizable Ingestion Activity Dock */}
      <IngestActivityDock />
    </div>
  );
};
