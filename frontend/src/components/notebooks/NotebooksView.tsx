import React, { useState, useEffect } from 'react';
import {
  BookOpen,
  Play,
  Square,
  Plus,
  RefreshCw,
  ExternalLink,
  AlertCircle,
  FileCode,
  Trash2,
} from 'lucide-react';
import { api } from '../../lib/api';
import type { Notebook, NotebookServerStatus } from '../../lib/types';

export interface NotebookTarget {
  name: string;
  tableName: string;
  fqn: string;
}

interface NotebooksViewProps {
  target?: NotebookTarget | null;
  onClearTarget?: () => void;
}

export const NotebooksView: React.FC<NotebooksViewProps> = ({ target, onClearTarget }) => {
  const [status, setStatus] = useState<NotebookServerStatus | null>(null);
  const [notebooks, setNotebooks] = useState<Notebook[]>([]);
  const [activeNotebook, setActiveNotebook] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [actionLoading, setActionLoading] = useState<boolean>(false);
  const [newNotebookName, setNewNotebookName] = useState<string>('');
  const [showCreate, setShowCreate] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [iframeReloadKey, setIframeReloadKey] = useState<number>(0);

  const fetchData = async () => {
    try {
      setLoading(true);
      setError(null);
      const [statusRes, nbRes] = await Promise.all([
        api.getNotebookServerStatus(),
        api.listNotebooks(),
      ]);
      setStatus(statusRes);
      setNotebooks(nbRes.notebooks);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to query Marimo status');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  // Handle incoming "Analyze in Notebook" target from Catalog
  useEffect(() => {
    if (!target) return;

    const initializeTargetNotebook = async () => {
      try {
        setActionLoading(true);
        // 1. Check & start Marimo server if offline
        const currentStatus = await api.getNotebookServerStatus();
        if (!currentStatus.is_running) {
          try {
            await api.startNotebookServer(2718);
          } catch {}
        }

        // 2. Build custom Marimo template with obelisk.table(...) cell
        const pythonTemplate = `import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import obelisk
    return (mo, obelisk)


@app.cell
def _(mo):
    mo.md(
        r"""
        # 📊 Table Analysis: \`${target.fqn}\`
        Interactive exploratory workspace powered by **Obelisk Python SDK** and reactive **Marimo**.
        """
    )
    return


@app.cell
def _(obelisk):
    # Load dataset into Polars DataFrame via canonical 3-tier FQN
    df = obelisk.table("${target.fqn}").to_polars()
    df
    return (df,)


@app.cell
def _(df, mo):
    # Live summary
    mo.md(f"Loaded **{'{len(df):,}'}** rows across **{'{len(df.columns)}'}** columns from \`${target.fqn}\`.")
    return


if __name__ == "__main__":
    app.run()
`;

        // 3. Create or update notebook with table loading cell
        await api.createNotebook(target.name, pythonTemplate, true);

        // 4. Refresh notebook list, set active, and force iframe reload
        await fetchData();
        setActiveNotebook(target.name);
        setIframeReloadKey((k) => k + 1);

        // 5. Copy snippet to clipboard and show toast
        const snippet = `df = obelisk.table("${target.fqn}").to_polars()`;
        try {
          await navigator.clipboard.writeText(snippet);
          setToastMessage(`Created notebook "${target.name}.py". Snippet copied to clipboard!`);
        } catch {
          setToastMessage(`Created notebook "${target.name}.py" ready for analysis!`);
        }
        setTimeout(() => setToastMessage(null), 4500);

        if (onClearTarget) onClearTarget();
      } catch (err: unknown) {
        setError(err instanceof Error ? err.message : 'Failed to initialize analysis notebook');
      } finally {
        setActionLoading(false);
      }
    };

    initializeTargetNotebook();
  }, [target]);

  const handleStart = async () => {
    try {
      setActionLoading(true);
      await api.startNotebookServer(2718);
      await fetchData();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to start Marimo server');
    } finally {
      setActionLoading(false);
    }
  };

  const handleStop = async () => {
    try {
      setActionLoading(true);
      await api.stopNotebookServer();
      await fetchData();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to stop Marimo server');
    } finally {
      setActionLoading(false);
    }
  };

  const handleRestart = async () => {
    try {
      setActionLoading(true);
      await api.restartNotebookServer(2718);
      await fetchData();
      setIframeReloadKey((k) => k + 1);
      setToastMessage('Marimo server restarted successfully');
      setTimeout(() => setToastMessage(null), 3000);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to restart Marimo server');
    } finally {
      setActionLoading(false);
    }
  };

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newNotebookName.trim()) return;
    try {
      setActionLoading(true);
      await api.createNotebook(newNotebookName.trim());
      setNewNotebookName('');
      setShowCreate(false);
      await fetchData();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to create notebook');
    } finally {
      setActionLoading(false);
    }
  };

  const handleDeleteNotebook = async (name: string) => {
    if (!window.confirm(`Are you sure you want to delete "${name}.py"?`)) return;
    try {
      setActionLoading(true);
      await api.deleteNotebook(name);
      if (activeNotebook === name) {
        setActiveNotebook(null);
      }
      await fetchData();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to delete notebook');
    } finally {
      setActionLoading(false);
    }
  };

  return (
    <div className="flex-1 flex flex-col h-full bg-slate-50 overflow-hidden">
      {/* Top Controls Bar */}
      <div className="h-14 border-b border-slate-200 bg-white px-6 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <span
              className={`w-2.5 h-2.5 rounded-full ${
                status?.is_running
                  ? 'bg-emerald-500 animate-pulse'
                  : 'bg-slate-300'
              }`}
            />
            <span className="text-xs font-bold text-slate-800">
              {status?.is_running ? 'Marimo Server Active' : 'Server Stopped'}
            </span>
            {status?.is_running && (
              <span className="text-[11px] font-mono text-slate-500 bg-slate-100 px-2 py-0.5 rounded border border-slate-200">
                Port {status.port} • /notebooks
              </span>
            )}
          </div>

          <span className="text-slate-300">|</span>

          {status?.is_running ? (
            <>
              <button
                onClick={handleStop}
                disabled={actionLoading}
                className="flex items-center gap-1.5 px-3 py-1.5 bg-rose-50 hover:bg-rose-100 text-rose-700 border border-rose-200 rounded-lg text-xs font-semibold transition-colors disabled:opacity-50"
              >
                <Square className="w-3.5 h-3.5 fill-current" />
                <span>Stop Server</span>
              </button>
              <button
                onClick={handleRestart}
                disabled={actionLoading}
                className="flex items-center gap-1.5 px-3 py-1.5 bg-amber-50 hover:bg-amber-100 text-amber-700 border border-amber-200 rounded-lg text-xs font-semibold transition-colors disabled:opacity-50"
                title="Restart Marimo server to reload all notebooks from disk"
              >
                <RefreshCw className={`w-3.5 h-3.5 ${actionLoading ? 'animate-spin' : ''}`} />
                <span>Restart Server</span>
              </button>
            </>
          ) : (
            <button
              onClick={handleStart}
              disabled={actionLoading}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-sky-600 hover:bg-sky-700 text-white rounded-lg text-xs font-semibold shadow-xs transition-colors disabled:opacity-50"
            >
              <Play className="w-3.5 h-3.5 fill-current" />
              <span>Start Marimo Server</span>
            </button>
          )}

          <button
            onClick={() => setShowCreate(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-slate-100 hover:bg-slate-200/80 text-slate-700 border border-slate-200 rounded-lg text-xs font-semibold transition-colors"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>New Notebook</span>
          </button>
        </div>

        <div className="flex items-center gap-2">
          {status?.is_running && (
            <a
              href="/notebooks"
              target="_blank"
              rel="noreferrer"
              className="flex items-center gap-1 text-xs text-sky-700 font-medium hover:underline mr-2"
            >
              <span>Open in new window</span>
              <ExternalLink className="w-3 h-3" />
            </a>
          )}
          <button
            onClick={fetchData}
            title="Refresh status"
            className="p-1.5 border border-slate-200 rounded-lg text-slate-500 hover:text-slate-800 hover:bg-slate-100 transition-colors"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Main Workspace */}
      <div className="flex-1 flex overflow-hidden">
        {/* Left Notebooks List Pane */}
        <div className="w-64 border-r border-slate-200 bg-white flex flex-col shrink-0">
          <div className="p-3 border-b border-slate-200 text-xs font-bold text-slate-700 flex items-center justify-between">
            <span>Files ({notebooks.length})</span>
            <span className="text-[10px] font-mono text-slate-400">git_repo/notebooks</span>
          </div>
          <div className="flex-1 overflow-y-auto p-2 space-y-1">
            {notebooks.length === 0 ? (
              <div className="p-6 text-center text-xs text-slate-400 font-mono">
                No notebooks found.
              </div>
            ) : (
              notebooks.map((nb) => {
                const isActive = activeNotebook === nb.name;
                return (
                  <div
                    key={nb.name}
                    onClick={() => setActiveNotebook(nb.name)}
                    className={`p-2 rounded-lg border transition-all cursor-pointer group flex items-center justify-between ${
                      isActive
                        ? 'border-sky-500 bg-sky-50/70 shadow-2xs'
                        : 'border-slate-200 bg-slate-50/50 hover:bg-sky-50/40 hover:border-sky-300'
                    }`}
                  >
                    <div className="flex items-center gap-2 min-w-0">
                      <FileCode className={`w-4 h-4 shrink-0 ${isActive ? 'text-sky-600' : 'text-slate-400'}`} />
                      <div className="truncate">
                        <span className={`font-medium text-xs block truncate ${isActive ? 'text-sky-900 font-semibold' : 'text-slate-800'}`}>
                          {nb.name}
                        </span>
                        <span className="text-[10px] font-mono text-slate-400">
                          {Math.round(nb.size_bytes / 1024)} KB
                        </span>
                      </div>
                    </div>
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleDeleteNotebook(nb.name);
                      }}
                      title="Delete notebook"
                      className="opacity-0 group-hover:opacity-100 p-1 text-slate-400 hover:text-rose-600 hover:bg-rose-50 rounded transition-all shrink-0"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* Embedded Marimo Server Iframe or Standby Screen */}
        <div className="flex-1 bg-slate-100 flex flex-col overflow-hidden relative">
          {error && (
            <div className="m-4 p-3 bg-rose-50 border border-rose-200 rounded-lg text-xs text-rose-700 flex items-center gap-2">
              <AlertCircle className="w-4 h-4 shrink-0" />
              <span>{error}</span>
            </div>
          )}

          {status?.is_running ? (
            <iframe
              key={`${activeNotebook || 'overview'}-${iframeReloadKey}`}
              src={activeNotebook ? `/notebooks?file=${activeNotebook}.py&t=${iframeReloadKey}` : '/notebooks'}
              title="Marimo Notebooks"
              className="w-full h-full border-0 bg-white"
            />
          ) : (
            <div className="flex-1 flex flex-col items-center justify-center p-8 text-center space-y-3">
              <div className="w-14 h-14 rounded-2xl bg-white border border-slate-200 flex items-center justify-center shadow-xs text-slate-400">
                <BookOpen className="w-7 h-7" />
              </div>
              <h3 className="text-sm font-bold text-slate-800">
                Marimo Server is currently Offline
              </h3>
              <p className="text-xs text-slate-500 max-w-sm">
                Click "Start Marimo Server" to boot the headless reactive workspace with the zero-config <code className="text-sky-600 bg-sky-50 px-1 py-0.5 rounded font-mono">obelisk</code> client.
              </p>
              <button
                onClick={handleStart}
                disabled={actionLoading}
                className="flex items-center gap-2 px-4 py-2 bg-sky-600 hover:bg-sky-700 text-white rounded-lg text-xs font-semibold shadow-xs transition-colors"
              >
                <Play className="w-3.5 h-3.5 fill-current" />
                <span>Start Server (:2718)</span>
              </button>
            </div>
          )}

          {toastMessage && (
            <div className="absolute bottom-6 right-6 z-50 flex items-center gap-2.5 px-4 py-2.5 bg-slate-900 text-white rounded-xl shadow-xl text-xs font-medium border border-slate-700 animate-in fade-in slide-in-from-bottom-2 duration-200">
              <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse shrink-0" />
              <span>{toastMessage}</span>
              <button
                onClick={() => setToastMessage(null)}
                className="ml-2 text-slate-400 hover:text-white"
              >
                ✕
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Create Modal */}
      {showCreate && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 backdrop-blur-xs p-4">
          <div className="w-full max-w-sm bg-white rounded-xl shadow-2xl border border-slate-200 p-5 space-y-4">
            <h3 className="text-sm font-bold text-slate-900">Create Reactive Notebook</h3>
            <form onSubmit={handleCreate} className="space-y-3">
              <div>
                <label className="block text-xs font-semibold text-slate-700 mb-1">
                  Notebook Name
                </label>
                <input
                  type="text"
                  placeholder="e.g. Sales_Exploration"
                  value={newNotebookName}
                  onChange={(e) => setNewNotebookName(e.target.value)}
                  required
                  autoFocus
                  className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-2 outline-hidden focus:border-sky-500 text-slate-800"
                />
              </div>
              <div className="flex items-center justify-end gap-2 pt-2">
                <button
                  type="button"
                  onClick={() => setShowCreate(false)}
                  className="px-3 py-1.5 text-xs text-slate-600 hover:bg-slate-100 rounded-lg"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={actionLoading}
                  className="px-4 py-1.5 bg-sky-600 hover:bg-sky-700 text-white rounded-lg text-xs font-semibold shadow-xs disabled:opacity-50"
                >
                  Create File
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};
