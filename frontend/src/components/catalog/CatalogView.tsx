import React, { useEffect, useState, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { Database, Plus } from 'lucide-react';
import { api } from '../../lib/api';
import type { CatalogNode, CatalogTable, Connection } from '../../lib/types';
import { CatalogTree } from './CatalogTree';
import { TablePreview } from './TablePreview';
import { IngestModal } from './IngestModal';
import { useIngestTracker } from '../../context/IngestContext';
import { BatchIngestModal } from './BatchIngestModal';
import { AddConnectionModal } from './AddConnectionModal';
import { ConfirmDialog } from '../common/ConfirmDialog';
import type { ConfirmRequest } from '../common/ConfirmDialog';
import { EmptyState } from '../common/ui/EmptyState';
import { toast } from '../../lib/toast';

interface SelectedTable {
  sourceId: string;
  sourceName?: string;
  table: CatalogTable;
  isLakehouse: boolean;
}

interface BatchTarget {
  connectionId: string;
  connectionName: string;
  schemaName?: string;
  tables: CatalogTable[];
}

export const CatalogView: React.FC = () => {
  const navigate = useNavigate();
  const { catalogRevision } = useIngestTracker();

  const [catalogNodes, setCatalogNodes] = useState<CatalogNode[]>([]);
  const [catalogLoading, setCatalogLoading] = useState(true);
  const [selectedTableInfo, setSelectedTableInfo] = useState<SelectedTable | null>(null);
  const [isIngestOpen, setIsIngestOpen] = useState(false);
  const [batchIngestTarget, setBatchIngestTarget] = useState<BatchTarget | null>(null);
  const [isAddConnectionOpen, setIsAddConnectionOpen] = useState(false);
  const [editingConnection, setEditingConnection] = useState<Connection | null>(null);
  const [confirmRequest, setConfirmRequest] = useState<ConfirmRequest | null>(null);

  // Resizable Catalog Tree width with localStorage persistence
  const [treeWidth, setTreeWidth] = useState<number>(() => {
    try {
      const saved = localStorage.getItem('obelisk_catalog_tree_width');
      if (saved) {
        const parsed = parseInt(saved, 10);
        if (!isNaN(parsed) && parsed >= 220 && parsed <= 900) {
          return parsed;
        }
      }
    } catch {}
    return 320;
  });

  const isResizingRef = useRef(false);

  const handleStartTreeResize = (e: React.MouseEvent) => {
    e.preventDefault();
    isResizingRef.current = true;
    const startX = e.clientX;
    const startWidth = treeWidth;

    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    const onMouseMove = (moveEvent: MouseEvent) => {
      if (!isResizingRef.current) return;
      const delta = moveEvent.clientX - startX;
      const maxAllowed = Math.max(380, Math.min(900, window.innerWidth - 380));
      const nextWidth = Math.max(220, Math.min(maxAllowed, startWidth + delta));
      setTreeWidth(nextWidth);
    };

    const onMouseUp = () => {
      if (isResizingRef.current) {
        isResizingRef.current = false;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        setTreeWidth((finalWidth) => {
          try {
            localStorage.setItem('obelisk_catalog_tree_width', String(finalWidth));
          } catch {}
          return finalWidth;
        });
      }
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };

    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
  };

  useEffect(() => {
    return () => {
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
  }, []);

  const fetchCatalog = async (force = false) => {
    try {
      // A forced fetch keeps the tree on screen: it is a background refresh of data the
      // operator is already looking at, not a first load.
      if (!force) setCatalogLoading(true);
      const data = await api.getCatalogTree(force);
      setCatalogNodes(data);

      // Auto-select first table if nothing selected yet
      if (!selectedTableInfo && data.length > 0) {
        const lakehouse = data.find((n) => n.type === 'lakehouse');
        const firstDir =
          lakehouse?.directories?.find((d) => d.tables.length > 0) || lakehouse?.directories?.[0];
        if (firstDir && firstDir.tables.length > 0) {
          setSelectedTableInfo({ sourceId: firstDir.id, table: firstDir.tables[0], isLakehouse: true });
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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // An ingest writes into the catalog behind this view's back; the ingest stream bumps
  // this revision the moment a job ends, which is exactly when the tree goes stale.
  useEffect(() => {
    if (catalogRevision === 0) return;
    void fetchCatalog(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [catalogRevision]);

  const handleOpenInNotebook = (tableOrName: CatalogTable | string, customFqn?: string) => {
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
    navigate('/notebooks', {
      state: { target: { name: `Explore_${cleanName}`, tableName: cleanName, fqn } },
    });
  };

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

  const handleDeleteConnection = (connId: string) => {
    const target = catalogNodes.find((n) => n.id === connId);
    setConfirmRequest({
      title: `Delete ${target?.name ?? 'this connection'}?`,
      body: 'Its credentials are removed from the vault; ingest jobs and dbt models that read it stop working.',
      confirmLabel: 'Delete connection',
      destructive: true,
      onConfirm: async () => {
        try {
          await api.deleteConnection(connId);
          await fetchCatalog(true);
          toast.success(`Deleted ${target?.name ?? 'connection'}.`);
        } catch (e) {
          toast.error(e instanceof Error ? e.message : 'Failed to delete connection');
        }
      },
    });
  };

  const selectedTableKey = selectedTableInfo
    ? `${selectedTableInfo.sourceId}:${selectedTableInfo.table.name}`
    : null;

  const openInQuery = (sql: string) => navigate('/query', { state: { sql } });

  return (
    <>
      {/* Left Catalog Tree */}
      <CatalogTree
        nodes={catalogNodes}
        loading={catalogLoading}
        width={treeWidth}
        onRefresh={() => fetchCatalog(true)}
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
        onOpenInQuery={openInQuery}
        onOpenInNotebook={handleOpenInNotebook}
      />

      {/* Draggable Catalog Tree Splitter Handle */}
      <div
        onMouseDown={handleStartTreeResize}
        onDoubleClick={() => {
          setTreeWidth(320);
          try {
            localStorage.setItem('obelisk_catalog_tree_width', '320');
          } catch {}
        }}
        className="w-1 -ml-0.5 hover:bg-slate-300 active:bg-slate-400 cursor-col-resize shrink-0 transition-colors select-none z-10"
        title="Drag to resize Catalog Tree (double-click to reset)"
      />

      {/* Right Table Preview / Schema Viewer */}
      {selectedTableInfo ? (
        <TablePreview
          sourceId={selectedTableInfo.sourceId}
          sourceName={selectedTableInfo.sourceName}
          table={selectedTableInfo.table}
          isLakehouse={selectedTableInfo.isLakehouse}
          onOpenIngest={() => setIsIngestOpen(true)}
          onOpenInQuery={openInQuery}
          onOpenInNotebook={handleOpenInNotebook}
        />
      ) : (
        <div className="flex-1 flex flex-col items-center justify-center p-8 bg-slate-50/50">
          <EmptyState
            icon={<Database className="w-6 h-6 text-slate-400" />}
            title="Select a Table to Inspect"
            description="Select any table in the Lakehouse or connect to an external source to inspect schemas and preview live rows."
            actionLabel="Add Source Connection"
            actionIcon={<Plus className="w-3.5 h-3.5" />}
            onAction={() => {
              setEditingConnection(null);
              setIsAddConnectionOpen(true);
            }}
          />
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

      <ConfirmDialog request={confirmRequest} onClose={() => setConfirmRequest(null)} />
    </>
  );
};
