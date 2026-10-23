import React, { useCallback, useEffect, useState } from 'react';
import {
  AlertCircle,
  Check,
  CornerDownRight,
  Layers,
  Pencil,
  Plus,
  Star,
  Trash2,
  X,
} from 'lucide-react';
import { api } from '../../lib/api';
import type { Layer } from '../../lib/types';

interface LayersModalProps {
  onClose: () => void;
  /** Called after any change, so the catalog tree can pick up new layer paths. */
  onChanged: () => void;
}

const childPath = (parent: string, name: string): string =>
  parent ? `${parent}.${name}` : name;

const errorMessage = (error: unknown): string => {
  const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data
    ?.detail;
  if (typeof detail === 'string') return detail;
  if (error instanceof Error) return error.message;
  return 'Something went wrong.';
};

/**
 * Layer management. A layer is just a name the workspace chooses — there is no fixed
 * set of tiers — so this is the only place that invents them, and every path stays
 * editable afterwards.
 */
export const LayersModal: React.FC<LayersModalProps> = ({ onClose, onChanged }) => {
  const [layers, setLayers] = useState<Layer[]>([]);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [newPath, setNewPath] = useState('');
  const [draftParent, setDraftParent] = useState<string | null>(null);
  const [draftName, setDraftName] = useState('');
  const [renaming, setRenaming] = useState<{ path: string; name: string } | null>(null);
  const [confirmingDelete, setConfirmingDelete] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setLayers(await api.listLayers());
      setError(null);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const run = async (action: () => Promise<unknown>) => {
    setBusy(true);
    setError(null);
    try {
      await action();
      await load();
      onChanged();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  const submitNew = () => {
    const path = newPath.trim();
    if (!path) return;
    run(async () => {
      await api.createLayer({ path });
      setNewPath('');
    });
  };

  const submitChild = (parent: string) => {
    const name = draftName.trim();
    if (!name) return;
    run(async () => {
      await api.createLayer({ path: childPath(parent, name) });
      setDraftName('');
      setDraftParent(null);
    });
  };

  const submitRename = () => {
    if (!renaming) return;
    const name = renaming.name.trim();
    if (!name) return;
    run(async () => {
      await api.updateLayer(renaming.path, { name });
      setRenaming(null);
    });
  };

  const depthPadding = (depth: number) => ({
    paddingLeft: `${Math.max(0, depth - 1) * 16}px`,
  });

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4">
      <div className="w-full max-w-xl max-h-[85vh] flex flex-col rounded-xl bg-white shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-slate-200 shrink-0">
          <div className="flex items-center gap-2">
            <Layers className="w-4 h-4 text-sky-600" />
            <span className="text-sm font-bold text-slate-800">Layers</span>
          </div>
          <button
            onClick={onClose}
            className="p-1 rounded-md text-slate-400 hover:text-slate-700 hover:bg-slate-100"
            title="Close"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* New root layer */}
        <div className="px-4 py-3 border-b border-slate-100 shrink-0">
          <div className="flex items-center gap-2">
            <input
              value={newPath}
              onChange={(e) => setNewPath(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') submitNew();
              }}
              placeholder="new layer path, e.g. marts.sales"
              className="flex-1 bg-white border border-slate-200 rounded-lg px-2.5 py-1.5 text-xs font-mono text-slate-900 placeholder-slate-400 focus:outline-none focus:ring-1 focus:ring-sky-500"
            />
            <button
              onClick={submitNew}
              disabled={busy || !newPath.trim()}
              className="flex items-center gap-1 px-2.5 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-700 disabled:opacity-40 text-white text-xs font-semibold"
            >
              <Plus className="w-3.5 h-3.5" />
              <span>Add</span>
            </button>
          </div>
          <p className="mt-1.5 text-[10px] text-slate-400">
            A child needs its parent first. Hover a layer and use{' '}
            <span className="font-mono">+</span> to nest under it.
          </p>
        </div>

        {/* Error */}
        {error && (
          <div className="mx-4 mt-3 flex items-start gap-2 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-[11px] text-rose-700 shrink-0">
            <AlertCircle className="w-3.5 h-3.5 mt-0.5 shrink-0" />
            <span>{error}</span>
          </div>
        )}

        {/* Tree */}
        <div className="flex-1 overflow-y-auto px-2 py-2">
          {loading && <p className="px-2 py-4 text-xs text-slate-400">Loading layers…</p>}
          {!loading && layers.length === 0 && (
            <p className="px-2 py-4 text-xs text-slate-400">
              No layers yet. Add one above; tables land in the default layer until you do.
            </p>
          )}

          {layers.map((layer) => (
            <div key={layer.path}>
              <div
                style={depthPadding(layer.depth)}
                className="group flex items-center gap-2 rounded-lg px-2 py-1.5 hover:bg-slate-50"
              >
                <Layers className="w-3.5 h-3.5 text-slate-300 shrink-0" />

                {renaming?.path === layer.path ? (
                  <input
                    autoFocus
                    value={renaming.name}
                    onChange={(e) => setRenaming({ path: layer.path, name: e.target.value })}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') submitRename();
                      if (e.key === 'Escape') setRenaming(null);
                    }}
                    onBlur={submitRename}
                    className="flex-1 min-w-0 bg-white border border-sky-300 rounded px-1.5 py-0.5 text-xs font-mono text-slate-900 focus:outline-none"
                  />
                ) : (
                  <span className="font-mono text-xs text-slate-800 truncate">{layer.name}</span>
                )}

                {layer.is_default && (
                  <span title="Default ingest target" className="text-amber-500">
                    <Star className="w-3 h-3 fill-amber-400" />
                  </span>
                )}
                <span className="text-[10px] text-slate-400 whitespace-nowrap">
                  {layer.object_count} obj
                </span>

                <div className="ml-auto flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
                  <button
                    title="Add a child layer"
                    onClick={() => {
                      setDraftParent(layer.path);
                      setDraftName('');
                    }}
                    className="p-1 rounded text-slate-400 hover:text-sky-600 hover:bg-sky-50"
                  >
                    <Plus className="w-3.5 h-3.5" />
                  </button>
                  <button
                    title="Rename this layer"
                    onClick={() => setRenaming({ path: layer.path, name: layer.name })}
                    className="p-1 rounded text-slate-400 hover:text-slate-700 hover:bg-slate-100"
                  >
                    <Pencil className="w-3.5 h-3.5" />
                  </button>
                  <button
                    title="Make this the default ingest target"
                    disabled={layer.is_default || busy}
                    onClick={() => run(() => api.updateLayer(layer.path, { is_default: true }))}
                    className="p-1 rounded text-slate-400 hover:text-amber-600 hover:bg-amber-50 disabled:opacity-30"
                  >
                    <Check className="w-3.5 h-3.5" />
                  </button>
                  <button
                    title="Delete this layer"
                    onClick={() => setConfirmingDelete(layer.path)}
                    className="p-1 rounded text-slate-400 hover:text-rose-600 hover:bg-rose-50"
                  >
                    <Trash2 className="w-3.5 h-3.5" />
                  </button>
                </div>
              </div>
              {/* Delete confirmation stays inline: a destructive action should sit next
                  to its target rather than behind another dialog. */}
              {confirmingDelete === layer.path && (
                <div
                  style={depthPadding(layer.depth + 1)}
                  className="flex flex-wrap items-center gap-2 px-2 py-1 text-[11px] text-rose-700"
                >
                  <span>Delete “{layer.path}” and its child layers?</span>
                  <button
                    onClick={() => {
                      const path = layer.path;
                      setConfirmingDelete(null);
                      run(() => api.deleteLayer(path));
                    }}
                    disabled={busy}
                    className="px-2 py-0.5 rounded bg-rose-600 hover:bg-rose-700 text-white font-semibold"
                  >
                    Delete
                  </button>
                  <button
                    onClick={() => setConfirmingDelete(null)}
                    className="px-2 py-0.5 rounded border border-slate-200 text-slate-600"
                  >
                    Cancel
                  </button>
                </div>
              )}

              {/* Child draft */}
              {draftParent === layer.path && (
                <div
                  style={depthPadding(layer.depth + 1)}
                  className="flex items-center gap-2 px-2 py-1"
                >
                  <CornerDownRight className="w-3.5 h-3.5 text-slate-300 shrink-0" />
                  <input
                    autoFocus
                    value={draftName}
                    onChange={(e) => setDraftName(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') submitChild(layer.path);
                      if (e.key === 'Escape') setDraftParent(null);
                    }}
                    placeholder="child name"
                    className="flex-1 bg-white border border-slate-200 rounded px-1.5 py-0.5 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-sky-500"
                  />
                  <button
                    onClick={() => submitChild(layer.path)}
                    disabled={busy || !draftName.trim()}
                    className="px-2 py-0.5 rounded bg-sky-600 hover:bg-sky-700 disabled:opacity-40 text-white text-[11px] font-semibold"
                  >
                    Add
                  </button>
                  <button
                    onClick={() => setDraftParent(null)}
                    className="px-2 py-0.5 rounded border border-slate-200 text-[11px] text-slate-600"
                  >
                    Cancel
                  </button>
                </div>
              )}
            </div>
          ))}
        </div>

        <div className="px-4 py-2.5 border-t border-slate-100 shrink-0">
          <p className="text-[10px] text-slate-400">
            Renaming is refused while a layer still holds tables, and so is deleting it.
          </p>
        </div>
      </div>
    </div>
  );
};
