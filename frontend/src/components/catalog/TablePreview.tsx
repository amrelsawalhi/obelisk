import React, { useState, useEffect } from 'react';
import { tableSizeLabel } from '../../lib/format';
import {
  FileSpreadsheet,
  Layers,
  ArrowDownToLine,
  Columns,
  RefreshCw,
  Clock,
  Hash,
  Database,
  Terminal,
  BookOpen,
} from 'lucide-react';
import { api } from '../../lib/api';
import type { CatalogTable, TablePreviewResponse } from '../../lib/types';
import { DataGrid } from '../query/DataGrid';

interface TablePreviewProps {
  sourceId: string;
  sourceName?: string;
  table: CatalogTable;
  isLakehouse: boolean;
  directoryName?: string;
  onOpenIngest: () => void;
  onOpenInQuery?: (sql: string) => void;
  onOpenInNotebook?: (table: CatalogTable, fqn?: string) => void;
}

export const TablePreview: React.FC<TablePreviewProps> = ({
  sourceId,
  sourceName,
  table,
  isLakehouse,
  directoryName: propDirectoryName,
  onOpenIngest,
  onOpenInQuery,
  onOpenInNotebook,
}) => {
  const [activeTab, setActiveTab] = useState<'preview' | 'schema'>('preview');
  const [preview, setPreview] = useState<TablePreviewResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const fetchPreview = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await api.getTablePreview(sourceId, table.name);
      setPreview(data);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Failed to fetch table preview';
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPreview();
  }, [sourceId, table.name]);

  const directoryName = propDirectoryName || (isLakehouse
    ? table.schema_name || sourceId.replace(/^lakehouse:/, '')
    : null);

  const canonicalFqn = table.fqn || (isLakehouse
    ? `lakehouse.${directoryName || 'bronze'}.${table.name.replace(/\.parquet$/i, '')}`
    : `${(sourceName || sourceId).toLowerCase().replace(/[^a-z0-9_]/g, '_')}.${table.schema_name || 'public'}.${table.name}`);

  return (
    <div className="flex-1 flex flex-col bg-white overflow-hidden h-full">
      {/* Header Bar */}
      <div className="h-16 border-b border-slate-200 px-6 flex items-center justify-between shrink-0 bg-slate-50/50">
        <div className="flex items-center gap-3">
          <div
            className="w-9 h-9 rounded-lg flex items-center justify-center bg-sky-50 text-sky-700"
          >
            {isLakehouse ? <Layers className="w-5 h-5" /> : <Database className="w-5 h-5" />}
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h2 className="text-base font-bold text-slate-900">{table.name}</h2>
              <span
                className={`text-[11px] font-mono px-2 py-0.5 rounded-md font-medium border inline-flex items-center gap-1.5 ${
                  isLakehouse
                    ? 'bg-slate-100 text-slate-700 border-slate-200'
                    : 'bg-sky-50 text-sky-700 border-sky-200'
                }`}
                title={`Canonical FQN: ${canonicalFqn}`}
              >
                <span className="font-semibold">{canonicalFqn}</span>
              </span>
            </div>
            <div className="text-xs text-slate-500 font-mono">
              {(table.columns || []).length} columns • {table.format || 'Snappy Parquet'}{tableSizeLabel(table) ? ` • ${tableSizeLabel(table)}` : ''}
            </div>
          </div>
        </div>

        {/* Action Controls */}
        <div className="flex items-center gap-2">
          {onOpenInQuery && (
            <button
              onClick={() => {
                const query = `-- Obelisk Query Studio\nSELECT * FROM ${canonicalFqn} LIMIT 50;\n`;
                onOpenInQuery(query);
              }}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold shadow-2xs transition-colors ${
                isLakehouse
                  ? 'bg-sky-600 hover:bg-sky-700 text-white'
                  : 'bg-white hover:bg-slate-50 text-slate-700 border border-slate-300'
              }`}
              title="Open and query table in Monaco Query Studio"
            >
              <Terminal className={`w-3.5 h-3.5 ${isLakehouse ? 'text-white' : 'text-sky-600'}`} />
              <span>Query Table</span>
            </button>
          )}

          {onOpenInNotebook && (
            <button
              onClick={() => {
                onOpenInNotebook(table, canonicalFqn);
              }}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-white hover:bg-slate-50 text-slate-700 border border-slate-300 rounded-lg text-xs font-semibold shadow-2xs transition-colors"
              title="Analyze in reactive Marimo notebook with obelisk Python SDK"
            >
              <BookOpen className="w-3.5 h-3.5 text-amber-500" />
              <span>Analyze in Notebook</span>
            </button>
          )}

          {!isLakehouse && (
            <button
              onClick={onOpenIngest}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-sky-600 hover:bg-sky-700 text-white rounded-lg text-xs font-semibold shadow-2xs transition-colors"
            >
              <ArrowDownToLine className="w-3.5 h-3.5" />
              <span>Ingest to Lakehouse</span>
            </button>
          )}
          <button
            onClick={fetchPreview}
            title="Reload Preview"
            className="p-1.5 border border-slate-300 rounded-lg text-slate-600 hover:text-slate-900 bg-white hover:bg-slate-50 shadow-2xs transition-colors"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="px-6 border-b border-slate-200 flex items-center gap-6 bg-white shrink-0">
        <button
          onClick={() => setActiveTab('preview')}
          className={`py-2.5 text-xs font-semibold flex items-center gap-2 border-b-2 transition-colors ${
            activeTab === 'preview'
              ? 'border-sky-600 text-sky-700'
              : 'border-transparent text-slate-500 hover:text-slate-900'
          }`}
        >
          <FileSpreadsheet className="w-3.5 h-3.5" />
          <span>Data Preview (50 rows)</span>
        </button>
        <button
          onClick={() => setActiveTab('schema')}
          className={`py-2.5 text-xs font-semibold flex items-center gap-2 border-b-2 transition-colors ${
            activeTab === 'schema'
              ? 'border-sky-600 text-sky-700'
              : 'border-transparent text-slate-500 hover:text-slate-900'
          }`}
        >
          <Columns className="w-3.5 h-3.5" />
          <span>Schema & Data Types ({table.columns.length})</span>
        </button>
      </div>

      {/* Content Area */}
      <div className="flex-1 overflow-auto p-6 bg-slate-50/30">
        {loading ? (
          <div className="h-64 flex flex-col items-center justify-center text-slate-400 text-xs font-mono space-y-2">
            <RefreshCw className="w-6 h-6 animate-spin text-sky-500" />
            <p>Querying 50 sample rows...</p>
          </div>
        ) : error ? (
          <div className="p-4 bg-rose-50 border border-rose-200 rounded-xl text-rose-700 text-xs">
            <div className="font-semibold mb-1">Failed to preview table</div>
            <p className="font-mono">{error}</p>
          </div>
        ) : activeTab === 'preview' ? (
          preview && (
            <div className="space-y-3">
              {/* Telemetry metadata banner */}
              <div className="flex items-center gap-4 text-xs font-mono text-slate-500">
                <span className="flex items-center gap-1">
                  <Hash className="w-3.5 h-3.5 text-slate-400" />
                  Returned: {preview.row_count} rows
                </span>
                <span>•</span>
                <span className="flex items-center gap-1">
                  <Clock className="w-3.5 h-3.5 text-slate-400" />
                  Latency: {preview.duration_ms} ms
                </span>
              </div>

              {/* Virtualized DataGrid */}
              <div className="h-[calc(100vh-270px)] min-h-[350px]">
                <DataGrid columns={preview.columns} rows={preview.rows} />
              </div>
            </div>
          )
        ) : (
          /* Schema Details View */
          <div className="border border-slate-200 rounded-lg overflow-hidden bg-white shadow-2xs">
            <table className="w-full text-left text-xs">
              <thead>
                <tr className="bg-slate-100/80 border-b border-slate-200 text-slate-700 font-mono">
                  <th className="py-2.5 px-4 font-semibold">Column Name</th>
                  <th className="py-2.5 px-4 font-semibold">Data Type</th>
                  <th className="py-2.5 px-4 font-semibold">Nullable</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 font-mono text-[11px]">
                {(table.columns || []).map((col) => (
                  <tr key={col.name} className="hover:bg-slate-50 transition-colors">
                    <td className="py-2.5 px-4 font-medium text-slate-900">
                      {col.name}
                      {col.is_primary_key && (
                        <span className="ml-2 rounded border border-amber-200 bg-amber-50 px-1.5 py-0.5 text-[9px] font-semibold text-amber-700">
                          PK
                        </span>
                      )}
                    </td>
                    <td className="py-2.5 px-4 text-sky-700">
                      <span className="bg-sky-50 px-2 py-0.5 rounded border border-sky-200">
                        {col.data_type}
                      </span>
                    </td>
                    <td className="py-2.5 px-4 text-slate-500">
                      {col.is_nullable === undefined ? '—' : col.is_nullable ? 'YES' : 'NO'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};
