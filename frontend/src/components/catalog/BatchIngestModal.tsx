import React, { useState } from 'react';
import { tableSizeLabel } from '../../lib/format';
import {
  X,
  RefreshCw,
  AlertCircle,
  Folder,
  Filter,
  ArrowDownToLine,
  Search,
  CheckSquare,
  Square,
  Layers,
} from 'lucide-react';
import { api } from '../../lib/api';
import type { CatalogTable } from '../../lib/types';
import { useIngestTracker } from '../../context/IngestContext';

interface BatchIngestModalProps {
  isOpen: boolean;
  onClose: () => void;
  connectionId: string;
  connectionName: string;
  schemaName?: string;
  availableTables: CatalogTable[];
  onSuccess: () => void;
}

export const BatchIngestModal: React.FC<BatchIngestModalProps> = ({
  isOpen,
  onClose,
  connectionId,
  connectionName,
  schemaName,
  availableTables,
  onSuccess,
}) => {
  const [selectedTables, setSelectedTables] = useState<string[]>(
    availableTables.map((t) => t.name)
  );
  const [tableSearch, setTableSearch] = useState('');
  const [mode, setMode] = useState<'snapshot' | 'incremental'>('snapshot');
  const [watermarkCol, setWatermarkCol] = useState('updated_at');
  const [writeStrategy, setWriteStrategy] = useState<'append' | 'upsert'>('append');
  const [schemaPolicy, setSchemaPolicy] = useState<'evolve' | 'strict'>('evolve');
  const [primaryKey, setPrimaryKey] = useState('id');

  const defaultFolder = schemaName ? schemaName.replace(/[^a-zA-Z0-9_-]/g, '_') : '';
  // The folder is typed, not chosen: it is a directory in the warehouse, created on
  // write. Blank means the warehouse root (or the source schema, when there is one).
  const [targetFolder, setTargetFolder] = useState<string>(defaultFolder);

  // Sample ingestion option
  const [isSample, setIsSample] = useState(false);
  const [sampleLimit, setSampleLimit] = useState<number>(1000);

  // Scheduling option

  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const { startJob } = useIngestTracker();

  if (!isOpen) return null;

  const filteredTables = availableTables.filter((t) =>
    t.name.toLowerCase().includes(tableSearch.toLowerCase().trim())
  );

  const allFilteredSelected =
    filteredTables.length > 0 &&
    filteredTables.every((t) => selectedTables.includes(t.name));

  const toggleSelectAll = () => {
    if (allFilteredSelected) {
      setSelectedTables((prev) =>
        prev.filter((name) => !filteredTables.some((t) => t.name === name))
      );
    } else {
      const toAdd = filteredTables.map((t) => t.name);
      setSelectedTables((prev) => Array.from(new Set([...prev, ...toAdd])));
    }
  };

  const toggleTable = (tableName: string) => {
    setSelectedTables((prev) =>
      prev.includes(tableName)
        ? prev.filter((t) => t !== tableName)
        : [...prev, tableName]
    );
  };

  const handleBatchTrigger = async (e: React.FormEvent) => {
    e.preventDefault();
    if (selectedTables.length === 0) return;

    setSubmitting(true);
    setError(null);

    try {
      const res = await api.triggerBatchIngestAsync({
        connection_id: connectionId,
        schema_name: schemaName,
        tables: selectedTables,
        mode,
        watermark_column: mode === 'incremental' ? watermarkCol.trim() : undefined,
        write_strategy: mode === 'incremental' ? writeStrategy : undefined,
        schema_policy: mode === 'incremental' ? schemaPolicy : 'evolve',
        primary_key: mode === 'incremental' && writeStrategy === 'upsert' ? primaryKey.trim() : undefined,
        target_folder: targetFolder.trim() || defaultFolder,
        limit: isSample ? Number(sampleLimit) : undefined,
      });

      // Hand off directly to the global activity tray dock in the bottom right corner
      startJob(
        res.job_id,
        `Batch Ingest ${schemaName ? `${schemaName} ` : ''}(${selectedTables.length} tables)`
      );
      onSuccess();
      onClose();
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Batch ingestion failed to trigger');
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 backdrop-blur-xs p-4 animate-in fade-in duration-150">
      <div className="w-full max-w-2xl bg-white rounded-2xl shadow-2xl border border-slate-200 overflow-hidden flex flex-col max-h-[92vh]">
        {/* Header */}
        <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between bg-slate-50 shrink-0">
          <div className="flex items-center gap-3">
            <div className="p-2.5 bg-sky-50 text-sky-700 rounded-lg">
              <Layers className="w-5 h-5" />
            </div>
            <div>
              <h3 className="text-sm font-bold text-slate-900 flex items-center gap-2">
                <span>Batch Lakehouse Ingestion</span>
                <span className="px-2 py-0.5 text-[10px] font-mono bg-sky-100 text-sky-800 rounded-full font-semibold">
                  {selectedTables.length} / {availableTables.length} Tables Selected
                </span>
              </h3>
              <p className="text-xs text-slate-500 font-mono mt-0.5">
                Source: <span className="text-slate-800 font-semibold">{connectionName}</span>
                {schemaName && (
                  <>
                    {' '}• Schema: <span className="text-slate-800 font-semibold">{schemaName}</span>
                  </>
                )}
                {' '}→ <span className="text-sky-600 font-semibold">data/lakehouse/{targetFolder || 'root'}/</span>
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            title="Close"
            className="text-slate-400 hover:text-slate-600 p-1 rounded-md transition-colors"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Configuration Form */}
        <form onSubmit={handleBatchTrigger} className="p-5 space-y-4 overflow-y-auto">
          {/* Table Selection Box */}
          <div className="border border-slate-200 rounded-xl overflow-hidden bg-white">
            <div className="px-3 py-2 bg-slate-50 border-b border-slate-200 flex items-center justify-between gap-3">
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={toggleSelectAll}
                  className="flex items-center gap-1.5 text-xs font-semibold text-slate-700 hover:text-sky-600 transition-colors"
                >
                  {allFilteredSelected ? (
                    <CheckSquare className="w-4 h-4 text-sky-600" />
                  ) : (
                    <Square className="w-4 h-4 text-slate-400" />
                  )}
                  <span>Select All ({filteredTables.length})</span>
                </button>
              </div>

              <div className="relative w-48">
                <Search className="w-3.5 h-3.5 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2" />
                <input
                  type="text"
                  placeholder="Filter tables..."
                  value={tableSearch}
                  onChange={(e) => setTableSearch(e.target.value)}
                  className="w-full text-xs pl-8 pr-2.5 py-1 bg-white border border-slate-200 rounded-lg outline-hidden focus:border-sky-500 focus:ring-1 focus:ring-sky-500"
                />
              </div>
            </div>

            <div className="max-h-48 overflow-y-auto p-2 divide-y divide-slate-100">
              {filteredTables.length === 0 ? (
                <div className="text-xs text-slate-400 text-center py-4">No matching tables found.</div>
              ) : (
                filteredTables.map((t) => {
                  const isChecked = selectedTables.includes(t.name);
                  return (
                    <div
                      key={t.name}
                      onClick={() => toggleTable(t.name)}
                      className={`flex items-center justify-between p-2 rounded-lg cursor-pointer transition-colors ${
                        isChecked ? 'bg-sky-50/70 text-sky-950 font-medium' : 'hover:bg-slate-50 text-slate-700'
                      }`}
                    >
                      <div className="flex items-center gap-2">
                        <input
                          type="checkbox"
                          checked={isChecked}
                          onChange={() => {}}
                          className="w-3.5 h-3.5 text-sky-600 rounded-sm border-slate-300 focus:ring-sky-500 cursor-pointer"
                        />
                        <span className="text-xs font-mono">{t.name}</span>
                      </div>
                      <div className="flex items-center gap-2 text-[10px] text-slate-400 font-mono">
                        <span>{t.columns.length} cols</span>
                        {tableSizeLabel(t) != null && (
                          <span className="bg-slate-100 text-slate-600 px-1.5 py-0.5 rounded">
                            {tableSizeLabel(t)}
                          </span>
                        )}
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </div>

          {/* Mode Selector */}
          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1.5">
              Ingestion Mode
            </label>
            <div className="grid grid-cols-2 gap-3">
              <button
                type="button"
                onClick={() => setMode('snapshot')}
                className={`py-2 px-3 text-xs font-medium rounded-lg border text-left transition-all ${
                  mode === 'snapshot'
                    ? 'border-sky-500 bg-sky-50 text-sky-900 ring-1 ring-sky-500 font-semibold'
                    : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50'
                }`}
              >
                <div className="font-semibold">Full Snapshot</div>
                <div className="text-[10px] text-slate-500 mt-0.5">
                  Overwrites Parquet files with complete snapshot of each table
                </div>
              </button>

              <button
                type="button"
                onClick={() => setMode('incremental')}
                className={`py-2 px-3 text-xs font-medium rounded-lg border text-left transition-all ${
                  mode === 'incremental'
                    ? 'border-sky-500 bg-sky-50 text-sky-900 ring-1 ring-sky-500 font-semibold'
                    : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50'
                }`}
              >
                <div className="font-semibold">Incremental Watermark</div>
                <div className="text-[10px] text-slate-500 mt-0.5">
                  Extracts only newer records per table & merges deltas
                </div>
              </button>
            </div>
          </div>

          {/* Incremental Parameters */}
          {mode === 'incremental' && (
            <div className="space-y-3 p-3 bg-sky-50/40 border border-sky-100 rounded-xl">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-xs font-semibold text-slate-700 mb-1">
                    Watermark Column
                  </label>
                  <input
                    type="text"
                    value={watermarkCol}
                    onChange={(e) => setWatermarkCol(e.target.value)}
                    placeholder="updated_at"
                    className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-2 outline-hidden focus:border-sky-500 focus:ring-1 focus:ring-sky-500 font-mono text-slate-800"
                  />
                  <p className="text-[10px] text-slate-500 mt-0.5">
                    Applied to tables possessing this column
                  </p>
                </div>

                <div>
                  <label className="block text-xs font-semibold text-slate-700 mb-1">
                    Deduplication Strategy
                  </label>
                  <select
                    value={writeStrategy}
                    onChange={(e) => setWriteStrategy(e.target.value as 'append' | 'upsert')}
                    className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-2 outline-hidden focus:border-sky-500 focus:ring-1 focus:ring-sky-500 text-slate-800"
                  >
                    <option value="append">Append Only</option>
                    <option value="upsert">Upsert (Deduplicate on PK)</option>
                  </select>
                </div>
              </div>

              {writeStrategy === 'upsert' && (
                <div>
                  <label className="block text-[11px] font-semibold text-slate-700 mb-1">
                    Primary Key Column
                  </label>
                  <input
                    type="text"
                    value={primaryKey}
                    onChange={(e) => setPrimaryKey(e.target.value)}
                    placeholder="id"
                    className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-1.5 outline-hidden focus:border-sky-500 focus:ring-1 focus:ring-sky-500 font-mono text-slate-800"
                  />
                </div>
              )}

              <div>
                <label className="block text-[11px] font-semibold text-slate-700 mb-1">
                  Schema Drift Policy
                </label>
                <select
                  value={schemaPolicy}
                  onChange={(e) => setSchemaPolicy(e.target.value as 'evolve' | 'strict')}
                  className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-1.5 outline-hidden focus:border-sky-500 focus:ring-1 focus:ring-sky-500 text-slate-800"
                >
                  <option value="evolve">Auto-Evolve Schema (ALTER TABLE ADD COLUMN)</option>
                  <option value="strict">Strict Schema Guard (Fail on Upstream Drift)</option>
                </select>
              </div>
            </div>
          )}

          {/* Target Lakehouse Folder Selection */}
          <div>
            <div className="flex items-center justify-between mb-1">
              <label className="text-xs font-semibold text-slate-700 flex items-center gap-1.5">
                <Folder className="w-3.5 h-3.5 text-sky-600" />
                <span>Target Lakehouse Folder</span>
              </label>
              <span className="text-[10px] text-slate-400 font-mono">data/lakehouse/{targetFolder || 'root'}</span>
            </div>
            <input
              type="text"
              value={targetFolder}
              onChange={(e) => setTargetFolder(e.target.value)}
              placeholder="folder path, e.g. marts/sales (blank = root)"
              className="w-full text-xs bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 font-mono text-slate-800 outline-hidden focus:border-sky-500 focus:ring-1 focus:ring-sky-500 focus:bg-white transition-all"
            />
          </div>

          {/* Sample Ingestion Toggle */}
          <div className="p-3 bg-slate-50 border border-slate-200 rounded-xl space-y-2">
            <div className="flex items-center justify-between">
              <label
                htmlFor="batch-sample-toggle"
                className="text-xs font-semibold text-slate-700 flex items-center gap-1.5 cursor-pointer"
              >
                <Filter className="w-3.5 h-3.5 text-sky-600" />
                <span>Sample Ingestion (Limit rows per table)</span>
              </label>
              <input
                id="batch-sample-toggle"
                type="checkbox"
                checked={isSample}
                onChange={(e) => setIsSample(e.target.checked)}
                className="w-4 h-4 text-sky-600 rounded-sm border-slate-300 focus:ring-sky-500 cursor-pointer"
              />
            </div>
            {isSample && (
              <div className="pt-1 flex items-center gap-2">
                <span className="text-xs text-slate-600">Extract at most</span>
                <input
                  type="number"
                  min="1"
                  max="1000000"
                  value={sampleLimit}
                  onChange={(e) => setSampleLimit(Number(e.target.value))}
                  className="w-24 text-xs bg-white border border-slate-300 rounded-lg px-2.5 py-1 font-mono text-slate-800 outline-hidden focus:border-sky-500 focus:ring-1 focus:ring-sky-500"
                />
                <span className="text-xs text-slate-600">rows per table</span>
              </div>
            )}
          </div>

          {/* Status & Error */}
          {error && (
            <div className="p-3 bg-rose-50 border border-rose-200 rounded-lg flex items-start gap-2 text-rose-700 text-xs">
              <AlertCircle className="w-4 h-4 shrink-0 mt-0.5" />
              <div className="font-mono text-[11px]">{error}</div>
            </div>
          )}

          {/* Footer Actions */}
          <div className="pt-2 flex items-center justify-between border-t border-slate-100">
            <div className="text-xs text-slate-500 font-medium">
              {selectedTables.length} {selectedTables.length === 1 ? 'table' : 'tables'} selected
            </div>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={onClose}
                className="px-3 py-1.5 text-xs text-slate-600 hover:bg-slate-100 rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={submitting || selectedTables.length === 0}
                className="flex items-center gap-1.5 px-4 py-2 bg-sky-600 hover:bg-sky-700 text-white rounded-lg text-xs font-semibold shadow-2xs transition-colors disabled:opacity-50"
              >
                {submitting ? (
                  <>
                    <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                    <span>Triggering Batch Ingestion...</span>
                  </>
                ) : (
                  <>
                    <ArrowDownToLine className="w-3.5 h-3.5" />
                    <span>Ingest {selectedTables.length} Tables to Lakehouse</span>
                  </>
                )}
              </button>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
};
