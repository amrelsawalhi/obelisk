import React, { useState, useEffect, useRef, useCallback } from 'react';
import Editor from '@monaco-editor/react';
import {
  FileCode2,
  Play,
  Square,
  Save,
  Plus,
  Trash2,
  Search,
  SlidersHorizontal,
  Terminal,
  CheckCircle2,
  AlertTriangle,
  Clock,
  ChevronDown,
  ChevronUp,
  X,
  FileText,
  Loader2,
} from 'lucide-react';
import { api } from '../../lib/api';

interface ScriptItem {
  name: string;
  size_bytes: number;
  updated_at: string;
}

interface ScriptParam {
  key: string;
  value: string;
}

export const ScriptsStudio: React.FC = () => {
  const [scripts, setScripts] = useState<ScriptItem[]>([]);
  const [activeScriptName, setActiveScriptName] = useState<string | null>(null);
  const [content, setContent] = useState<string>('');
  const [originalContent, setOriginalContent] = useState<string>('');
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [isSaving, setIsSaving] = useState<boolean>(false);
  const [searchQuery, setSearchQuery] = useState<string>('');

  // Execution state
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [runStatus, setRunStatus] = useState<'idle' | 'running' | 'success' | 'failed' | 'killed'>('idle');
  const [exitCode, setExitCode] = useState<number | null>(null);
  const [durationSeconds, setDurationSeconds] = useState<number>(0);
  const [logs, setLogs] = useState<string[]>([]);
  const [outputs, setOutputs] = useState<Record<string, any>>({});
  const [isConsoleOpen, setIsConsoleOpen] = useState<boolean>(false);

  // Parameters panel
  const [isParamsOpen, setIsParamsOpen] = useState<boolean>(false);
  const [params, setParams] = useState<ScriptParam[]>([
    { key: '', value: '' },
  ]);

  // Dialogs
  const [isNewModalOpen, setIsNewModalOpen] = useState<boolean>(false);
  const [newScriptName, setNewScriptName] = useState<string>('');
  const [deleteConfirmTarget, setDeleteConfirmTarget] = useState<string | null>(null);

  const logsEndRef = useRef<HTMLDivElement>(null);
  const pollIntervalRef = useRef<number | null>(null);

  const isDirty = content !== originalContent;

  const loadScripts = useCallback(async (selectTarget?: string) => {
    try {
      setIsLoading(true);
      const res = await api.listScripts();
      setScripts(res.scripts);
      if (res.scripts.length > 0) {
        const nextActive = selectTarget || (activeScriptName && res.scripts.some(s => s.name === activeScriptName) ? activeScriptName : res.scripts[0].name);
        setActiveScriptName(nextActive);
        await loadScriptContent(nextActive);
      } else {
        setActiveScriptName(null);
        setContent('');
        setOriginalContent('');
      }
    } catch (err) {
      console.error('Failed to list scripts:', err);
    } finally {
      setIsLoading(false);
    }
  }, [activeScriptName]);

  useEffect(() => {
    loadScripts();
  }, []);

  const loadScriptContent = async (name: string) => {
    try {
      const res = await api.getScriptContent(name);
      setContent(res.content);
      setOriginalContent(res.content);
    } catch (err) {
      console.error('Failed to load script content:', err);
    }
  };

  const handleSelectScript = async (name: string) => {
    if (name === activeScriptName) return;
    if (isDirty) {
      if (!window.confirm('You have unsaved changes. Discard them and switch files?')) {
        return;
      }
    }
    setActiveScriptName(name);
    await loadScriptContent(name);
  };

  const handleSave = async () => {
    if (!activeScriptName || isSaving || !isDirty) return;
    setIsSaving(true);
    try {
      await api.saveScript(activeScriptName, content);
      setOriginalContent(content);
      await loadScripts(activeScriptName);
    } catch (err) {
      console.error('Failed to save script:', err);
      alert('Failed to save script');
    } finally {
      setIsSaving(false);
    }
  };

  // Keyboard shortcut Ctrl+S
  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 's') {
        e.preventDefault();
        handleSave();
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [content, originalContent, activeScriptName, isSaving]);

  // Polling active run status
  useEffect(() => {
    if (!activeRunId) {
      if (pollIntervalRef.current) clearInterval(pollIntervalRef.current);
      return;
    }

    const poll = async () => {
      try {
        const res = await api.getScriptRunStatus(activeRunId);
        setLogs(res.logs || []);
        setDurationSeconds(res.duration_seconds || 0);
        setOutputs(res.outputs || {});
        setExitCode(res.exit_code);
        setRunStatus(res.status);

        if (res.status !== 'running') {
          if (pollIntervalRef.current) {
            clearInterval(pollIntervalRef.current);
            pollIntervalRef.current = null;
          }
          setActiveRunId(null);
        }
      } catch (err) {
        console.error('Poll run error:', err);
        if (pollIntervalRef.current) {
          clearInterval(pollIntervalRef.current);
          pollIntervalRef.current = null;
        }
        setActiveRunId(null);
        setRunStatus('failed');
      }
    };

    poll();
    pollIntervalRef.current = window.setInterval(poll, 700);

    return () => {
      if (pollIntervalRef.current) clearInterval(pollIntervalRef.current);
    };
  }, [activeRunId]);

  // Auto scroll console
  useEffect(() => {
    if (isConsoleOpen && logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [logs, isConsoleOpen]);

  const handleRunScript = async () => {
    if (!activeScriptName || runStatus === 'running') return;
    if (isDirty) {
      await handleSave();
    }

    const paramPayload: Record<string, any> = {};
    for (const p of params) {
      if (p.key.trim()) {
        paramPayload[p.key.trim()] = p.value;
      }
    }

    setIsConsoleOpen(true);
    setRunStatus('running');
    setExitCode(null);
    setDurationSeconds(0);
    setLogs([`$ python git_repo/scripts/${activeScriptName}`]);
    setOutputs({});

    try {
      const res = await api.runScript(activeScriptName, paramPayload);
      setActiveRunId(res.run_id);
    } catch (err: any) {
      console.error('Failed to trigger script run:', err);
      setRunStatus('failed');
      setLogs(prev => [...prev, `Failed to launch script: ${err?.response?.data?.detail || err.message}`]);
    }
  };

  const handleKillRun = async () => {
    if (!activeRunId) return;
    try {
      await api.killScriptRun(activeRunId);
      setRunStatus('killed');
    } catch (err) {
      console.error('Failed to kill script run:', err);
    }
  };

  const handleCreateScript = async (e: React.FormEvent) => {
    e.preventDefault();
    const clean = newScriptName.trim();
    if (!clean) return;
    try {
      const res = await api.createScript(clean);
      setIsNewModalOpen(false);
      setNewScriptName('');
      await loadScripts(res.name);
    } catch (err: any) {
      alert(`Could not create script: ${err?.response?.data?.detail || err.message}`);
    }
  };

  const handleDeleteScript = async () => {
    if (!deleteConfirmTarget) return;
    try {
      await api.deleteScript(deleteConfirmTarget);
      setDeleteConfirmTarget(null);
      await loadScripts();
    } catch (err: any) {
      alert(`Could not delete script: ${err?.response?.data?.detail || err.message}`);
    }
  };

  const filteredScripts = scripts.filter(s =>
    s.name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <div className="flex h-full w-full bg-slate-50 text-slate-800 overflow-hidden font-sans">
      {/* LEFT SIDEBAR: Script Tree & Management */}
      <div className="w-72 flex-shrink-0 border-r border-slate-200 bg-white flex flex-col h-full">
        {/* Header */}
        <div className="p-3 border-b border-slate-200 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="p-1.5 bg-blue-50 text-blue-600 rounded-lg">
              <FileCode2 size={18} />
            </div>
            <div>
              <h2 className="text-sm font-semibold text-slate-900 leading-tight">Scripts Studio</h2>
              <span className="text-[11px] text-slate-500">git_repo/scripts/</span>
            </div>
          </div>
          <button
            onClick={() => setIsNewModalOpen(true)}
            className="p-1.5 text-slate-600 hover:text-blue-600 hover:bg-blue-50 rounded-md transition-colors"
            title="Create New Script"
          >
            <Plus size={16} />
          </button>
        </div>

        {/* Search */}
        <div className="p-2 border-b border-slate-100">
          <div className="relative">
            <Search size={14} className="absolute left-2.5 top-2.5 text-slate-400" />
            <input
              type="text"
              placeholder="Search scripts..."
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
              className="w-full pl-8 pr-3 py-1.5 text-xs bg-slate-50 border border-slate-200 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500 focus:bg-white"
            />
          </div>
        </div>

        {/* Script List */}
        <div className="flex-1 overflow-y-auto p-2 space-y-1">
          {isLoading ? (
            <div className="flex items-center justify-center p-8 text-xs text-slate-400">
              <Loader2 size={16} className="animate-spin mr-2" />
              Loading scripts...
            </div>
          ) : filteredScripts.length === 0 ? (
            <div className="text-center p-8 text-xs text-slate-400">
              No scripts found.
            </div>
          ) : (
            filteredScripts.map(item => {
              const isSelected = item.name === activeScriptName;
              return (
                <div
                  key={item.name}
                  onClick={() => handleSelectScript(item.name)}
                  className={`group flex items-center justify-between px-2.5 py-2 rounded-lg cursor-pointer text-xs transition-colors ${
                    isSelected
                      ? 'bg-blue-50 text-blue-700 font-medium'
                      : 'text-slate-700 hover:bg-slate-100'
                  }`}
                >
                  <div className="flex items-center gap-2 truncate">
                    <FileText size={14} className={isSelected ? 'text-blue-600' : 'text-slate-400'} />
                    <span className="truncate">{item.name}</span>
                  </div>
                  <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                    <button
                      onClick={e => {
                        e.stopPropagation();
                        setDeleteConfirmTarget(item.name);
                      }}
                      className="p-1 text-slate-400 hover:text-red-600 rounded"
                      title="Delete script"
                    >
                      <Trash2 size={13} />
                    </button>
                  </div>
                </div>
              );
            })
          )}
        </div>
      </div>

      {/* RIGHT WORKSPACE: Code Editor & Execution Console */}
      <div className="flex-1 flex flex-col h-full overflow-hidden bg-white">
        {activeScriptName ? (
          <>
            {/* Top Toolbar */}
            <div className="h-12 border-b border-slate-200 px-4 flex items-center justify-between bg-white flex-shrink-0">
              <div className="flex items-center gap-3">
                <div className="flex items-center gap-2 text-sm font-semibold text-slate-800">
                  <span>{activeScriptName}</span>
                  {isDirty && (
                    <span className="w-2 h-2 rounded-full bg-amber-500" title="Unsaved changes" />
                  )}
                </div>
                <span className="text-xs text-slate-400 font-mono">
                  git_repo/scripts/{activeScriptName}
                </span>
              </div>

              {/* Action Buttons */}
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setIsParamsOpen(!isParamsOpen)}
                  className={`flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium rounded-md border transition-colors ${
                    isParamsOpen || params.some(p => p.key.trim())
                      ? 'bg-indigo-50 border-indigo-200 text-indigo-700'
                      : 'border-slate-200 text-slate-700 hover:bg-slate-50'
                  }`}
                  title="Configure environment parameters"
                >
                  <SlidersHorizontal size={14} />
                  <span>Parameters</span>
                  {params.filter(p => p.key.trim()).length > 0 && (
                    <span className="px-1.5 py-0.2 bg-indigo-200 text-indigo-800 rounded-full text-[10px]">
                      {params.filter(p => p.key.trim()).length}
                    </span>
                  )}
                </button>

                <button
                  onClick={handleSave}
                  disabled={!isDirty || isSaving}
                  className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md border transition-colors ${
                    isDirty
                      ? 'bg-blue-600 border-blue-600 text-white hover:bg-blue-700 shadow-sm'
                      : 'border-slate-200 text-slate-400 cursor-not-allowed'
                  }`}
                >
                  <Save size={14} />
                  <span>{isSaving ? 'Saving...' : 'Save'}</span>
                </button>

                {runStatus === 'running' ? (
                  <button
                    onClick={handleKillRun}
                    className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md bg-red-600 text-white hover:bg-red-700 shadow-sm transition-colors"
                  >
                    <Square size={14} fill="currentColor" />
                    <span>Kill Run</span>
                  </button>
                ) : (
                  <button
                    onClick={handleRunScript}
                    className="flex items-center gap-1.5 px-3.5 py-1.5 text-xs font-semibold rounded-md bg-emerald-600 text-white hover:bg-emerald-700 shadow-sm transition-colors"
                  >
                    <Play size={14} fill="currentColor" />
                    <span>Run Script</span>
                  </button>
                )}
              </div>
            </div>

            {/* Optional Parameters Tray */}
            {isParamsOpen && (
              <div className="bg-slate-50 border-b border-slate-200 p-3 flex-shrink-0 animate-fadeIn">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs font-semibold text-slate-700">
                    Execution Parameters (injected as <code className="text-indigo-600 font-mono">OBELISK_PARAM_*</code>)
                  </span>
                  <button
                    onClick={() => setParams([...params, { key: '', value: '' }])}
                    className="text-xs text-blue-600 hover:text-blue-700 font-medium"
                  >
                    + Add Parameter
                  </button>
                </div>
                <div className="space-y-1.5 max-h-36 overflow-y-auto pr-1">
                  {params.map((p, idx) => (
                    <div key={idx} className="flex items-center gap-2">
                      <input
                        type="text"
                        placeholder="KEY (e.g. AS_OF)"
                        value={p.key}
                        onChange={e => {
                          const updated = [...params];
                          updated[idx].key = e.target.value.toUpperCase();
                          setParams(updated);
                        }}
                        className="w-48 px-2 py-1 text-xs border border-slate-300 rounded font-mono uppercase focus:outline-none focus:ring-1 focus:ring-blue-500 bg-white"
                      />
                      <span className="text-slate-400">=</span>
                      <input
                        type="text"
                        placeholder="value (e.g. 2026-10-28)"
                        value={p.value}
                        onChange={e => {
                          const updated = [...params];
                          updated[idx].value = e.target.value;
                          setParams(updated);
                        }}
                        className="flex-1 px-2 py-1 text-xs border border-slate-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 bg-white font-mono"
                      />
                      <button
                        onClick={() => {
                          const updated = params.filter((_, i) => i !== idx);
                          setParams(updated.length ? updated : [{ key: '', value: '' }]);
                        }}
                        className="p-1 text-slate-400 hover:text-red-500"
                      >
                        <X size={14} />
                      </button>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Monaco Code Editor */}
            <div className="flex-1 min-h-0 relative">
              <Editor
                height="100%"
                path={`scripts/${activeScriptName}`}
                language="python"
                value={content}
                onChange={val => setContent(val || '')}
                theme="vs"
                options={{
                  fontSize: 13,
                  fontFamily: "'JetBrains Mono', 'Fira Code', Menlo, Monaco, monospace",
                  lineNumbers: 'on',
                  minimap: { enabled: true },
                  scrollBeyondLastLine: false,
                  wordWrap: 'on',
                  automaticLayout: true,
                  tabSize: 4,
                  insertSpaces: true,
                }}
              />
            </div>

            {/* BOTTOM CONSOLE DRAWER */}
            <div
              className={`border-t border-slate-200 bg-slate-900 text-slate-100 flex flex-col transition-all duration-200 ${
                isConsoleOpen ? 'h-64' : 'h-8'
              }`}
            >
              {/* Console Header Bar */}
              <div
                onClick={() => setIsConsoleOpen(!isConsoleOpen)}
                className="h-8 px-3 flex items-center justify-between cursor-pointer select-none bg-slate-800 text-xs font-mono border-b border-slate-700"
              >
                <div className="flex items-center gap-2">
                  <Terminal size={14} className="text-slate-400" />
                  <span className="font-semibold text-slate-300">Execution Console</span>
                  {runStatus === 'running' && (
                    <span className="flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-blue-900/60 text-blue-300 text-[10px] font-sans font-medium">
                      <Loader2 size={10} className="animate-spin" />
                      Running...
                    </span>
                  )}
                  {runStatus === 'success' && (
                    <span className="flex items-center gap-1 px-2 py-0.5 rounded-full bg-emerald-900/60 text-emerald-300 text-[10px] font-sans font-medium">
                      <CheckCircle2 size={10} />
                      Exit 0 (Success)
                    </span>
                  )}
                  {runStatus === 'failed' && (
                    <span className="flex items-center gap-1 px-2 py-0.5 rounded-full bg-red-900/60 text-red-300 text-[10px] font-sans font-medium">
                      <AlertTriangle size={10} />
                      Exit {exitCode ?? 1} (Failed)
                    </span>
                  )}
                  {runStatus === 'killed' && (
                    <span className="flex items-center gap-1 px-2 py-0.5 rounded-full bg-amber-900/60 text-amber-300 text-[10px] font-sans font-medium">
                      <Square size={10} />
                      Killed
                    </span>
                  )}
                  {durationSeconds > 0 && (
                    <span className="flex items-center gap-1 text-[11px] text-slate-400 font-sans">
                      <Clock size={11} />
                      {durationSeconds}s
                    </span>
                  )}
                </div>

                <div className="flex items-center gap-3">
                  {Object.keys(outputs).length > 0 && (
                    <span className="text-[11px] text-emerald-400 truncate max-w-xs font-mono">
                      Outputs: {JSON.stringify(outputs)}
                    </span>
                  )}
                  {isConsoleOpen ? <ChevronDown size={14} /> : <ChevronUp size={14} />}
                </div>
              </div>

              {/* Console Body */}
              {isConsoleOpen && (
                <div className="flex-1 p-3 overflow-y-auto font-mono text-xs leading-relaxed space-y-0.5 select-text bg-[#0B0F19]">
                  {logs.length === 0 ? (
                    <div className="text-slate-500 italic">No console output yet. Click 'Run Script' to execute.</div>
                  ) : (
                    logs.map((line, idx) => (
                      <div key={idx} className="flex gap-2">
                        <span className="text-slate-600 select-none w-8 text-right flex-shrink-0">{idx + 1}</span>
                        <span className={line.startsWith('FAIL') || line.includes('Error') ? 'text-red-400 font-semibold' : 'text-slate-200'}>
                          {line}
                        </span>
                      </div>
                    ))
                  )}
                  <div ref={logsEndRef} />
                </div>
              )}
            </div>
          </>
        ) : (
          <div className="flex-1 flex flex-col items-center justify-center p-8 text-center text-slate-400">
            <FileCode2 size={48} className="text-slate-300 mb-3" />
            <h3 className="text-base font-semibold text-slate-700">No Script Selected</h3>
            <p className="text-xs text-slate-400 max-w-sm mt-1">
              Select an existing script from the left sidebar or create a new one to start authoring and running pipeline logic.
            </p>
          </div>
        )}
      </div>

      {/* CREATE NEW SCRIPT MODAL */}
      {isNewModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm animate-fadeIn">
          <div className="bg-white rounded-xl shadow-xl border border-slate-200 w-full max-w-md p-5">
            <div className="flex items-center justify-between pb-3 border-b border-slate-100">
              <h3 className="text-sm font-semibold text-slate-900">Create New Python Script</h3>
              <button
                onClick={() => setIsNewModalOpen(false)}
                className="text-slate-400 hover:text-slate-600 p-1"
              >
                <X size={16} />
              </button>
            </div>
            <form onSubmit={handleCreateScript} className="mt-4 space-y-4">
              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">
                  Script File Name
                </label>
                <div className="relative">
                  <input
                    type="text"
                    required
                    placeholder="e.g. customer_churn_alerts"
                    value={newScriptName}
                    onChange={e => setNewScriptName(e.target.value)}
                    className="w-full px-3 py-2 text-xs border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono"
                    autoFocus
                  />
                  <span className="absolute right-3 top-2 text-xs text-slate-400 font-mono">.py</span>
                </div>
                <p className="text-[11px] text-slate-400 mt-1">
                  Will be created under <code className="font-mono text-slate-600">git_repo/scripts/</code>
                </p>
              </div>

              <div className="flex items-center justify-end gap-2 pt-2 border-t border-slate-100">
                <button
                  type="button"
                  onClick={() => setIsNewModalOpen(false)}
                  className="px-3 py-1.5 text-xs font-medium text-slate-600 hover:bg-slate-100 rounded-lg transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="px-4 py-1.5 text-xs font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg shadow-sm transition-colors"
                >
                  Create Script
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* DELETE CONFIRMATION MODAL */}
      {deleteConfirmTarget && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm animate-fadeIn">
          <div className="bg-white rounded-xl shadow-xl border border-slate-200 w-full max-w-sm p-5">
            <h3 className="text-sm font-semibold text-slate-900">Delete Script?</h3>
            <p className="text-xs text-slate-500 mt-2">
              Are you sure you want to delete <code className="font-mono text-slate-700 font-semibold">{deleteConfirmTarget}</code>?
              This action cannot be undone.
            </p>
            <div className="flex items-center justify-end gap-2 mt-5">
              <button
                type="button"
                onClick={() => setDeleteConfirmTarget(null)}
                className="px-3 py-1.5 text-xs font-medium text-slate-600 hover:bg-slate-100 rounded-lg"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleDeleteScript}
                className="px-4 py-1.5 text-xs font-medium text-white bg-red-600 hover:bg-red-700 rounded-lg shadow-sm"
              >
                Delete
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

