import React, { useEffect, useState } from 'react';
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
    </>
  );
};
