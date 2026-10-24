import React, { useEffect, useState } from 'react';
import {
  X,
  ArrowDownToLine,
  RefreshCw,
  AlertCircle,
  Folder,
  Filter,
} from 'lucide-react';
import { api } from '../../lib/api';
import type { CatalogTable } from '../../lib/types';
import { useIngestTracker } from '../../context/IngestContext';

interface IngestModalProps {
  isOpen: boolean;
  onClose: () => void;
  connectionId: string;
  table: CatalogTable;
  onSuccess: () => void;
}

export const IngestModal: React.FC<IngestModalProps> = ({
  isOpen,
  onClose,
  connectionId,
  table,
  onSuccess,
}) => {
  const [mode, setMode] = useState<'snapshot' | 'incremental'>('snapshot');
  const [watermarkCol, setWatermarkCol] = useState(
    table.columns.find((c) => c.name.toLowerCase().includes('id') || c.name.toLowerCase().includes('time'))?.name || ''
  );
  const [writeStrategy, setWriteStrategy] = useState<'append' | 'upsert'>('append');
  const [primaryKey, setPrimaryKey] = useState(
    table.columns.find((c) => c.name.toLowerCase() === 'id' || c.name.toLowerCase().endsWith('_id'))?.name || table.columns[0]?.name || ''
  );

  // Clean default target name without database or schema prefix
  const defaultTargetName = table.name.includes('.') ? table.name.split('.').pop()! : table.name;
  const [targetName, setTargetName] = useState(defaultTargetName);

  // Folder selection. Empty means "the workspace's default layer"; the real layer
  // paths are offered as chips once they load.
  const [targetFolder, setTargetFolder] = useState<string>('');
  const [layerPaths, setLayerPaths] = useState<string[]>([]);

  useEffect(() => {
    let active = true;
    api
      .listLayers()
      .then((layers) => {
        if (!active) return;
        setLayerPaths(layers.map((layer) => layer.path));
        const fallback = layers.find((layer) => layer.is_default)?.path ?? layers[0]?.path;
        if (fallback) setTargetFolder((current) => current || fallback);
      })
      .catch(() => {
        if (active) setLayerPaths([]);
      });
    return () => {
      active = false;
    };
  }, []);

  // Sample ingestion option
  const [isSample, setIsSample] = useState(false);
  const [sampleLimit, setSampleLimit] = useState<number>(1000);

  // Scheduling option

  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const { startJob } = useIngestTracker();

  if (!isOpen) return null;

  const handleTrigger = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setError(null);

    try {
      const targetFileName = targetName.trim() || defaultTargetName;
      const res = await api.triggerIngestAsync({
        connection_id: connectionId,
        entity: table.name,
        mode,
        watermark_column: mode === 'incremental' ? watermarkCol : undefined,
        write_strategy: mode === 'incremental' ? writeStrategy : undefined,
        primary_key: mode === 'incremental' && writeStrategy === 'upsert' ? primaryKey : undefined,
        target_name: targetFileName,
        target_folder: targetFolder.trim(),
        limit: isSample ? Number(sampleLimit) : undefined,
      });

      // Hand off directly to global activity tray in the bottom right corner
      startJob(res.job_id, `Ingest ${targetFileName}`);
      onSuccess();
      onClose();
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Ingestion failed to trigger');
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 backdrop-blur-xs p-4 animate-in fade-in duration-150">
      <div className="w-full max-w-lg bg-white rounded-xl shadow-2xl border border-slate-200 overflow-hidden flex flex-col max-h-[92vh]">
        {/* Header */}
        <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between bg-slate-50 shrink-0">
          <div className="flex items-center gap-2.5">
            <div className="p-2 bg-indigo-50 text-indigo-700 rounded-lg">
              <ArrowDownToLine className="w-4 h-4" />
            </div>
            <div>
              <h3 className="text-sm font-bold text-slate-900">
                Ingest to Lakehouse
              </h3>
              <p className="text-xs text-slate-500 font-mono">
                Source: {table.name} → {targetFolder}/{targetName || defaultTargetName}.parquet
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
        <form onSubmit={handleTrigger} className="p-5 space-y-4 overflow-y-auto">
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
                    ? 'border-indigo-600 bg-indigo-50 text-indigo-900 ring-1 ring-indigo-500'
                    : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50'
                }`}
              >
                <div className="font-semibold">Full Snapshot</div>
                <div className="text-[10px] text-slate-500 mt-0.5">
                  Direct C++ streaming snapshot straight to Snappy Parquet
                </div>
              </button>

              <button
                type="button"
                onClick={() => setMode('incremental')}
                className={`py-2 px-3 text-xs font-medium rounded-lg border text-left transition-all ${
                  mode === 'incremental'
                    ? 'border-indigo-600 bg-indigo-50 text-indigo-900 ring-1 ring-indigo-500'
                    : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50'
                }`}
              >
                <div className="font-semibold">Incremental Watermark</div>
                <div className="text-[10px] text-slate-500 mt-0.5">
                  Extracts deltas ($gt watermark) & merges schema
                </div>
              </button>
            </div>
          </div>

          {/* Watermark Column & Strategy (if incremental) */}
          {mode === 'incremental' && (
            <div className="space-y-3 p-3 bg-indigo-50/40 border border-indigo-100 rounded-xl">
              <div>
                <label className="block text-xs font-semibold text-slate-700 mb-1">
                  Watermark Column (Tracking Timestamp or Sequence)
                </label>
                <select
                  value={watermarkCol}
                  onChange={(e) => setWatermarkCol(e.target.value)}
                  required
                  className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-2 outline-hidden focus:border-indigo-500 font-mono text-slate-800"
                >
                  {table.columns.map((c) => (
                    <option key={c.name} value={c.name}>
                      {c.name} ({c.type})
                    </option>
                  ))}
                </select>
                <p className="text-[10px] text-slate-500 mt-1">
                  Obelisk inspects the existing Parquet file to calculate the current maximum watermark scalar.
                </p>
              </div>

              {/* Write Strategy: Append vs Upsert */}
              <div>
                <label className="block text-xs font-semibold text-slate-700 mb-1">
                  Delta Merge Strategy
                </label>
                <div className="grid grid-cols-2 gap-2">
                  <button
                    type="button"
                    onClick={() => setWriteStrategy('append')}
                    className={`py-1.5 px-2.5 text-xs rounded-lg border text-left transition-all ${
                      writeStrategy === 'append'
                        ? 'border-indigo-600 bg-white text-indigo-900 font-semibold shadow-xs'
                        : 'border-slate-200 bg-white/70 text-slate-600 hover:bg-white'
                    }`}
                  >
                    <div className="text-xs">Append Only</div>
                    <div className="text-[9px] text-slate-500 mt-0.5">Fast log-append</div>
                  </button>

                  <button
                    type="button"
                    onClick={() => setWriteStrategy('upsert')}
                    className={`py-1.5 px-2.5 text-xs rounded-lg border text-left transition-all ${
                      writeStrategy === 'upsert'
                        ? 'border-indigo-600 bg-white text-indigo-900 font-semibold shadow-xs'
                        : 'border-slate-200 bg-white/70 text-slate-600 hover:bg-white'
                    }`}
                  >
                    <div className="text-xs">Upsert (Deduplicate)</div>
                    <div className="text-[9px] text-slate-500 mt-0.5">Updates matching PK</div>
                  </button>
                </div>
              </div>

              {/* Primary Key for Upsert */}
              {writeStrategy === 'upsert' && (
                <div className="pt-1">
                  <label className="block text-[11px] font-semibold text-slate-700 mb-1">
                    Primary Key / Deduplication Column
                  </label>
                  <select
                    value={primaryKey}
                    onChange={(e) => setPrimaryKey(e.target.value)}
                    required
                    className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-1.5 outline-hidden focus:border-indigo-500 font-mono text-slate-800"
                  >
                    {table.columns.map((c) => (
                      <option key={c.name} value={c.name}>
                        {c.name} ({c.type})
                      </option>
                    ))}
                  </select>
                </div>
              )}
            </div>
          )}

          {/* Target Lakehouse Folder Selection */}
          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1">
              <span className="flex items-center gap-1.5">
                <Folder className="w-3.5 h-3.5 text-indigo-600" />
                <span>Target Lakehouse Folder</span>
              </span>
            </label>
            <div className="flex items-center gap-2 mb-1.5">
              {layerPaths.map((folder) => (
                <button
                  key={folder}
                  type="button"
                  onClick={() => setTargetFolder(folder)}
                  className={`text-xs px-2.5 py-1 rounded-md border font-mono transition-colors ${
                    targetFolder === folder
                      ? 'bg-indigo-50 border-indigo-400 text-indigo-700 font-bold'
                      : 'bg-white border-slate-200 text-slate-600 hover:bg-slate-50'
                  }`}
                >
                  {folder}
                </button>
              ))}
            </div>
            <input
              type="text"
              value={targetFolder}
              onChange={(e) => setTargetFolder(e.target.value)}
              placeholder="layer path, e.g. marts.sales"
              className="w-full text-xs bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 font-mono text-slate-800 outline-hidden focus:border-indigo-500 focus:bg-white"
            />
          </div>

          {/* Target Parquet Filename */}
          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1">
              Target Parquet Filename
            </label>
            <div className="flex items-center">
              <input
                type="text"
                value={targetName}
                onChange={(e) => setTargetName(e.target.value)}
                placeholder={defaultTargetName}
                className="w-full text-xs bg-slate-50 border border-slate-200 rounded-l-lg px-3 py-2 font-mono text-slate-800 outline-hidden focus:border-indigo-500 focus:bg-white"
              />
              <span className="bg-slate-100 border border-l-0 border-slate-200 text-slate-500 text-xs px-3 py-2 rounded-r-lg font-mono">
                .parquet
              </span>
            </div>
          </div>

          {/* Sample Ingestion Toggle */}
          <div className="p-3 bg-slate-50 border border-slate-200 rounded-xl space-y-2">
            <div className="flex items-center justify-between">
              <label
                htmlFor="sample-toggle"
                className="text-xs font-semibold text-slate-700 flex items-center gap-1.5 cursor-pointer"
              >
                <Filter className="w-3.5 h-3.5 text-indigo-600" />
                <span>Sample Ingestion</span>
              </label>
              <input
                id="sample-toggle"
                type="checkbox"
                checked={isSample}
                onChange={(e) => setIsSample(e.target.checked)}
                className="w-4 h-4 text-indigo-600 rounded-sm border-slate-300 focus:ring-indigo-500 cursor-pointer"
              />
            </div>
            {isSample && (
              <div className="pt-1 flex items-center gap-2">
                <span className="text-xs text-slate-600">Limit to first</span>
                <input
                  type="number"
                  min="1"
                  max="1000000"
                  value={sampleLimit}
                  onChange={(e) => setSampleLimit(Number(e.target.value))}
                  className="w-24 text-xs bg-white border border-slate-300 rounded-lg px-2.5 py-1 font-mono text-slate-800 outline-hidden focus:border-indigo-500"
                />
                <span className="text-xs text-slate-600">rows</span>
              </div>
            )}
          </div>

          {/* Status / Error Banner */}
          {error && (
            <div className="p-3 bg-rose-50 border border-rose-200 rounded-lg flex items-start gap-2 text-rose-700 text-xs">
              <AlertCircle className="w-4 h-4 shrink-0 mt-0.5" />
              <div className="font-mono text-[11px]">{error}</div>
            </div>
          )}

          {/* Footer Actions */}
          <div className="pt-2 flex items-center justify-end gap-2 border-t border-slate-100">
            <button
              type="button"
              onClick={onClose}
              className="px-3 py-1.5 text-xs text-slate-600 hover:bg-slate-100 rounded-lg transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={submitting}
              className="flex items-center gap-1.5 px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-xs font-semibold shadow-xs transition-colors disabled:opacity-50"
            >
              {submitting ? (
                <>
                  <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                  <span>Triggering Ingest...</span>
                </>
              ) : (
                <>
                  <ArrowDownToLine className="w-3.5 h-3.5" />
                  <span>Execute Ingest</span>
                </>
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};
