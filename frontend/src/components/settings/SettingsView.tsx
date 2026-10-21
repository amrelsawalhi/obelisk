import React, { useState, useEffect } from 'react';
import {
  Shield,
  Key,
  HardDrive,
  Cpu,
  Plus,
  RefreshCw,
  Eye,
  EyeOff,
  Copy,
  Check,
  Trash2,
  Lock,
  Activity,
  CheckCircle2,
  XCircle,
  Edit2,
  AlertTriangle,
  Code2,
} from 'lucide-react';
import { api } from '../../lib/api';
import type {
  VariableItem,
  Connection,
  ConnectionTestResult,
} from '../../lib/types';
import { AddConnectionModal } from '../catalog/AddConnectionModal';
import { SdkDocsView } from './SdkDocsView';

type SettingsSubTab = 'variables' | 'vault' | 'storage' | 'diagnostics' | 'sdk';

export const SettingsView: React.FC = () => {
  const [activeSubTab, setActiveSubTab] = useState<SettingsSubTab>('variables');
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Variables & Secrets State
  const [variables, setVariables] = useState<VariableItem[]>([]);
  const [revealedSecrets, setRevealedSecrets] = useState<Record<string, string>>({});
  const [revealingKey, setRevealingKey] = useState<string | null>(null);
  const [copiedKey, setCopiedKey] = useState<string | null>(null);
  const [isAddVarOpen, setIsAddVarOpen] = useState(false);
  const [newVarKey, setNewVarKey] = useState('');
  const [newVarValue, setNewVarValue] = useState('');
  const [newVarIsSecret, setNewVarIsSecret] = useState(false);
  const [newVarDesc, setNewVarDesc] = useState('');
  const [savingVar, setSavingVar] = useState(false);

  // Connection Vault State
  const [connections, setConnections] = useState<Connection[]>([]);
  const [isAddConnectionOpen, setIsAddConnectionOpen] = useState(false);
  const [editingConnection, setEditingConnection] = useState<Connection | null>(null);
  const [testingConnId, setTestingConnId] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<Record<string, ConnectionTestResult>>({});
  const [deletingConnId, setDeletingConnId] = useState<string | null>(null);

  const loadData = async (silent = false) => {
    try {
      if (!silent) setLoading(true);
      else setRefreshing(true);
      setError(null);

      const [varsData, connsData] = await Promise.all([
        api.getVariables(),
        api.listConnections(),
      ]);

      setVariables(varsData);
      setConnections(connsData);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Failed to fetch settings';
      setError(msg);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    loadData();
  }, []);

  // Variable Actions
  const handleToggleReveal = async (item: VariableItem) => {
    if (!item.is_secret) return;

    if (revealedSecrets[item.key]) {
      // Re-hide
      setRevealedSecrets((prev) => {
        const next = { ...prev };
        delete next[item.key];
        return next;
      });
      return;
    }

    try {
      setRevealingKey(item.key);
      const res = await api.revealVariable(item.key);
      setRevealedSecrets((prev) => ({ ...prev, [item.key]: res.value }));
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Failed to decrypt secret';
      setError(msg);
    } finally {
      setRevealingKey(null);
    }
  };

  const handleCopyValue = async (item: VariableItem) => {
    try {
      let valToCopy = item.value;
      if (item.is_secret) {
        if (revealedSecrets[item.key]) {
          valToCopy = revealedSecrets[item.key];
        } else {
          const res = await api.revealVariable(item.key);
          valToCopy = res.value;
        }
      }
      await navigator.clipboard.writeText(valToCopy);
      setCopiedKey(item.key);
      setTimeout(() => setCopiedKey(null), 2000);
    } catch (e) {
      console.error('Failed to copy', e);
    }
  };

  const handleDeleteVariable = async (item: VariableItem) => {
    if (!window.confirm(`Are you sure you want to delete variable '${item.key}'?`)) {
      return;
    }
    try {
      await api.deleteVariable(item.id);
      await loadData(true);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Failed to delete variable';
      setError(msg);
    }
  };

  const handleSaveVariable = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newVarKey.trim() || !newVarValue.trim()) return;

    setSavingVar(true);
    setError(null);
    try {
      await api.createVariable({
        key: newVarKey.trim().toUpperCase(),
        value: newVarValue,
        is_secret: newVarIsSecret,
        description: newVarDesc.trim(),
      });
      setIsAddVarOpen(false);
      setNewVarKey('');
      setNewVarValue('');
      setNewVarIsSecret(false);
      setNewVarDesc('');
      await loadData(true);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Failed to save variable';
      setError(msg);
    } finally {
      setSavingVar(false);
    }
  };

  // Connection Actions
  const handleTestConnection = async (connId: string) => {
    setTestingConnId(connId);
    try {
      const res = await api.testConnection(connId);
      setTestResults((prev) => ({ ...prev, [connId]: res }));
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Connection test failed';
      setTestResults((prev) => ({
        ...prev,
        [connId]: { status: 'error', latency_ms: 0, message: msg },
      }));
    } finally {
      setTestingConnId(null);
    }
  };

  const handleDeleteConnection = async (connId: string) => {
    if (!window.confirm('Are you sure you want to delete this connection?')) return;
    setDeletingConnId(connId);
    try {
      await api.deleteConnection(connId);
      await loadData(true);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Failed to delete connection';
      setError(msg);
    } finally {
      setDeletingConnId(null);
    }
  };

  if (loading && variables.length === 0 && connections.length === 0) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center p-12 bg-slate-50">
        <RefreshCw className="w-8 h-8 text-sky-600 animate-spin mb-3" />
        <div className="text-sm font-semibold text-slate-700">Loading Obelisk Settings...</div>
        <div className="text-xs text-slate-400 font-mono mt-1">Reading database configuration & AES-256 vault</div>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto bg-slate-50/60 p-6 space-y-6">
      {/* Sub-Tab Navigation & Refresh */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        {/* Sub-Tab Selector Pills */}
        <div className="flex rounded-lg bg-slate-200/70 p-1 text-xs font-medium text-slate-600">
          <button
            onClick={() => setActiveSubTab('variables')}
            className={`px-3 py-1.5 rounded-md flex items-center gap-1.5 transition-all ${
              activeSubTab === 'variables'
                ? 'bg-white text-slate-900 font-semibold shadow-xs'
                : 'hover:text-slate-900'
            }`}
          >
            <Key className="w-3.5 h-3.5 text-sky-600" />
            <span>Variables & Secrets</span>
          </button>
          <button
            onClick={() => setActiveSubTab('vault')}
            className={`px-3 py-1.5 rounded-md flex items-center gap-1.5 transition-all ${
              activeSubTab === 'vault'
                ? 'bg-white text-slate-900 font-semibold shadow-xs'
                : 'hover:text-slate-900'
            }`}
          >
            <Shield className="w-3.5 h-3.5 text-emerald-600" />
            <span>Connection Vault</span>
          </button>
          <button
            onClick={() => setActiveSubTab('storage')}
            className={`px-3 py-1.5 rounded-md flex items-center gap-1.5 transition-all ${
              activeSubTab === 'storage'
                ? 'bg-white text-slate-900 font-semibold shadow-xs'
                : 'hover:text-slate-900'
            }`}
          >
            <HardDrive className="w-3.5 h-3.5 text-purple-600" />
            <span>Lakehouse Storage</span>
          </button>
          <button
            onClick={() => setActiveSubTab('diagnostics')}
            className={`px-3 py-1.5 rounded-md flex items-center gap-1.5 transition-all ${
              activeSubTab === 'diagnostics'
                ? 'bg-white text-slate-900 font-semibold shadow-xs'
                : 'hover:text-slate-900'
            }`}
          >
            <Cpu className="w-3.5 h-3.5 text-amber-600" />
            <span>Diagnostics</span>
          </button>
          <button
            onClick={() => setActiveSubTab('sdk')}
            className={`px-3 py-1.5 rounded-md flex items-center gap-1.5 transition-all ${
              activeSubTab === 'sdk'
                ? 'bg-white text-slate-900 font-semibold shadow-xs'
                : 'hover:text-slate-900'
            }`}
          >
            <Code2 className="w-3.5 h-3.5 text-sky-600" />
            <span>SDK & Platform Guide</span>
          </button>
        </div>

        <button
          onClick={() => loadData(true)}
          disabled={refreshing}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 bg-white hover:bg-slate-50 text-xs font-medium text-slate-700 shadow-2xs transition-all"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin text-sky-600' : 'text-slate-500'}`} />
          <span>Sync</span>
        </button>
      </div>

      {error && (
        <div className="p-3 bg-red-50 border border-red-200 rounded-lg flex items-center gap-2.5 text-xs text-red-700 animate-in fade-in">
          <AlertTriangle className="w-4 h-4 text-red-500 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {/* SUB-TAB 1: VARIABLES & SECRETS */}
      {activeSubTab === 'variables' && (
        <div className="space-y-6">
          {/* Informational Banner */}
          <div className="bg-gradient-to-r from-sky-50 to-indigo-50/60 border border-sky-100 rounded-xl p-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div className="space-y-1">
              <div className="flex items-center gap-2">
                <span className="flex h-2 w-2 rounded-full bg-sky-600" />
                <h4 className="text-xs font-bold text-sky-900 uppercase tracking-wider font-mono">
                  Universal Variables & Zero-Knowledge Vault
                </h4>
              </div>
              <p className="text-xs text-slate-600 max-w-2xl">
                Define plaintext configuration and AES-256 encrypted secrets. Access them effortlessly across Marimo reactive notebooks using <code className="font-mono text-sky-800 bg-sky-100/70 px-1.5 py-0.5 rounded font-semibold">obelisk.var("KEY")</code> or within dbt models and ingestion pipelines.
              </p>
            </div>
            <div className="flex items-center gap-2 shrink-0">
              <button
                onClick={() => setIsAddVarOpen(true)}
                className="flex items-center gap-1.5 px-3.5 py-2 rounded-lg bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold shadow-xs transition-all"
              >
                <Plus className="w-4 h-4" />
                <span>Add Variable or Secret</span>
              </button>
            </div>
          </div>

          {/* Variables Table */}
          <div className="bg-white rounded-xl border border-slate-200 shadow-xs overflow-hidden">
            <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Key className="w-4 h-4 text-sky-600" />
                <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider font-mono">
                  Configured Variables ({variables.length})
                </h3>
              </div>
              <span className="text-xs text-slate-400 font-mono">
                Persisted in data/obelisk.db
              </span>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs">
                <thead className="bg-slate-50 border-b border-slate-200 font-semibold text-slate-600 uppercase text-[10px] tracking-wider font-mono">
                  <tr>
                    <th className="py-3 px-4">Key</th>
                    <th className="py-3 px-4">Type</th>
                    <th className="py-3 px-4">Value</th>
                    <th className="py-3 px-4">Description</th>
                    <th className="py-3 px-4 text-right">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {variables.map((v) => {
                    const isSecret = v.is_secret;
                    const isRevealed = Boolean(revealedSecrets[v.key]);
                    const displayValue = isSecret
                      ? isRevealed
                        ? revealedSecrets[v.key]
                        : '••••••••••••••••'
                      : v.value;

                    return (
                      <tr key={v.id} className="hover:bg-slate-50/70 transition-colors">
                        <td className="py-3 px-4 font-mono font-bold text-slate-900">
                          {v.key}
                        </td>
                        <td className="py-3 px-4">
                          {isSecret ? (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-emerald-50 text-emerald-700 border border-emerald-200 font-mono">
                              <Lock className="w-3 h-3 text-emerald-600" />
                              AES-256 Vault
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-slate-100 text-slate-600 border border-slate-200 font-mono">
                              Plaintext
                            </span>
                          )}
                        </td>
                        <td className="py-3 px-4 font-mono text-slate-700 max-w-xs truncate">
                          <div className="flex items-center gap-2">
                            <span
                              className={`truncate ${
                                isSecret && !isRevealed
                                  ? 'tracking-widest text-slate-400 font-sans'
                                  : 'text-slate-800'
                              }`}
                            >
                              {displayValue}
                            </span>
                            {isSecret && (
                              <button
                                onClick={() => handleToggleReveal(v)}
                                disabled={revealingKey === v.key}
                                className="text-slate-400 hover:text-sky-600 transition-colors p-0.5"
                                title={isRevealed ? 'Hide Secret' : 'Reveal Secret'}
                              >
                                {revealingKey === v.key ? (
                                  <RefreshCw className="w-3.5 h-3.5 animate-spin text-sky-600" />
                                ) : isRevealed ? (
                                  <EyeOff className="w-3.5 h-3.5" />
                                ) : (
                                  <Eye className="w-3.5 h-3.5" />
                                )}
                              </button>
                            )}
                          </div>
                        </td>
                        <td className="py-3 px-4 text-slate-500 text-xs">
                          {v.description || '—'}
                        </td>
                        <td className="py-3 px-4 text-right">
                          <div className="flex items-center justify-end gap-2">
                            <button
                              onClick={() => handleCopyValue(v)}
                              className="p-1 rounded-md border border-slate-200 bg-white hover:bg-slate-50 text-slate-600 hover:text-slate-900 shadow-2xs transition-colors"
                              title="Copy Value"
                            >
                              {copiedKey === v.key ? (
                                <Check className="w-3.5 h-3.5 text-emerald-600" />
                              ) : (
                                <Copy className="w-3.5 h-3.5" />
                              )}
                            </button>
                            <button
                              onClick={() => handleDeleteVariable(v)}
                              className="p-1 rounded-md border border-red-200 bg-red-50 hover:bg-red-100 text-red-600 hover:text-red-800 transition-colors"
                              title="Delete Variable"
                            >
                              <Trash2 className="w-3.5 h-3.5" />
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* SUB-TAB 2: CONNECTION VAULT */}
      {activeSubTab === 'vault' && (
        <section className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs space-y-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Shield className="w-4 h-4 text-emerald-600" />
              <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider font-mono">
                AES-256 Connection Vault (External Data Sources)
              </h3>
            </div>
            <button
              onClick={() => {
                setEditingConnection(null);
                setIsAddConnectionOpen(true);
              }}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold shadow-2xs transition-all"
            >
              <Plus className="w-3.5 h-3.5" />
              <span>Add Source Connection</span>
            </button>
          </div>

          <p className="text-xs text-slate-500 font-normal">
            Enterprise database and API credentials are encrypted at rest with Fernet AES-256 tokens in <code className="font-mono text-slate-700 bg-slate-100 px-1 py-0.5 rounded">data/obelisk.db</code>.
          </p>

          {connections.length === 0 ? (
            <div className="p-8 text-center border border-dashed border-slate-200 rounded-lg">
              <Key className="w-8 h-8 text-slate-300 mx-auto mb-2" />
              <div className="text-xs font-semibold text-slate-600">No external source connections registered yet</div>
              <div className="text-[11px] text-slate-400 mt-1">
                Connect external PostgreSQL, MySQL, Oracle, MongoDB, or REST API endpoints to introspect schemas and stream into Bronze.
              </div>
              <button
                onClick={() => {
                  setEditingConnection(null);
                  setIsAddConnectionOpen(true);
                }}
                className="mt-3 inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-300 bg-white hover:bg-slate-50 text-xs font-medium text-slate-700 shadow-2xs transition-all"
              >
                <Plus className="w-3.5 h-3.5 text-slate-500" />
                <span>Register Connection Now</span>
              </button>
            </div>
          ) : (
            <div className="overflow-x-auto border border-slate-200 rounded-lg">
              <table className="w-full text-left text-xs">
                <thead className="bg-slate-50 border-b border-slate-200 font-semibold text-slate-600 uppercase text-[10px] tracking-wider font-mono">
                  <tr>
                    <th className="py-2.5 px-3.5">Source Name</th>
                    <th className="py-2.5 px-3.5">Type</th>
                    <th className="py-2.5 px-3.5">Host / Endpoint</th>
                    <th className="py-2.5 px-3.5">Database</th>
                    <th className="py-2.5 px-3.5">Username / Auth</th>
                    <th className="py-2.5 px-3.5">Vault Status</th>
                    <th className="py-2.5 px-3.5 text-right">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {connections.map((conn) => {
                    const testRes = testResults[conn.id];
                    const isTesting = testingConnId === conn.id;

                    return (
                      <tr key={conn.id} className="hover:bg-slate-50/60 transition-colors font-mono">
                        <td className="py-2.5 px-3.5 font-bold text-slate-900 font-sans">{conn.name}</td>
                        <td className="py-2.5 px-3.5">
                          <span className="px-2 py-0.5 rounded text-[10px] uppercase font-semibold bg-sky-50 text-sky-700 border border-sky-200">
                            {conn.source_type}
                          </span>
                        </td>
                        <td className="py-2.5 px-3.5 text-slate-600 truncate max-w-[200px]" title={`${conn.host}:${conn.port}`}>
                          {conn.source_type === 'api' ? conn.host : `${conn.host}:${conn.port}`}
                        </td>
                        <td className="py-2.5 px-3.5 text-slate-700 font-semibold">{conn.database || '—'}</td>
                        <td className="py-2.5 px-3.5 text-slate-600">{conn.username || (conn.source_type === 'api' ? 'Header Token' : '—')}</td>
                        <td className="py-2.5 px-3.5">
                          <span className="inline-flex items-center gap-1 text-[11px] text-emerald-600 font-medium">
                            <CheckCircle2 className="w-3.5 h-3.5" />
                            Encrypted
                          </span>
                        </td>
                        <td className="py-2.5 px-3.5 text-right">
                          <div className="flex items-center justify-end gap-1.5 font-sans">
                            {/* Test Connection Button */}
                            <button
                              onClick={() => handleTestConnection(conn.id)}
                              disabled={isTesting}
                              className={`px-2 py-1 rounded-md text-[11px] font-semibold border transition-all flex items-center gap-1 ${
                                testRes?.status === 'success'
                                  ? 'bg-emerald-50 text-emerald-700 border-emerald-300'
                                  : testRes?.status === 'error'
                                  ? 'bg-red-50 text-red-700 border-red-300'
                                  : 'bg-white hover:bg-slate-50 text-slate-700 border-slate-200 shadow-2xs'
                              }`}
                              title={testRes?.message || 'Test Live Connection'}
                            >
                              {isTesting ? (
                                <RefreshCw className="w-3 h-3 animate-spin text-sky-600" />
                              ) : testRes?.status === 'success' ? (
                                <CheckCircle2 className="w-3 h-3 text-emerald-600" />
                              ) : testRes?.status === 'error' ? (
                                <XCircle className="w-3 h-3 text-red-600" />
                              ) : (
                                <Activity className="w-3 h-3 text-slate-500" />
                              )}
                              <span>
                                {isTesting
                                  ? 'Testing...'
                                  : testRes
                                  ? `${testRes.latency_ms}ms`
                                  : 'Test'}
                              </span>
                            </button>

                            {/* Edit Connection */}
                            <button
                              onClick={() => {
                                setEditingConnection(conn);
                                setIsAddConnectionOpen(true);
                              }}
                              className="p-1 rounded-md border border-slate-200 bg-white hover:bg-slate-50 text-slate-600 hover:text-slate-900 shadow-2xs transition-colors"
                              title="Edit Connection"
                            >
                              <Edit2 className="w-3 h-3" />
                            </button>

                            {/* Delete Connection */}
                            <button
                              onClick={() => handleDeleteConnection(conn.id)}
                              disabled={deletingConnId === conn.id}
                              className="p-1 rounded-md border border-red-200 bg-red-50 hover:bg-red-100 text-red-600 hover:text-red-800 transition-colors"
                              title="Delete Connection"
                            >
                              <Trash2 className="w-3 h-3" />
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {/* SUB-TAB 3: LAKEHOUSE STORAGE */}
      {activeSubTab === 'storage' && (
        <div className="space-y-6">
          <section className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs space-y-4">
            <div className="flex items-center gap-2">
              <HardDrive className="w-4 h-4 text-purple-600" />
              <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider font-mono">
                Zero-Cost Storage URI Abstraction (STORAGE_URI)
              </h3>
            </div>
            <p className="text-xs text-slate-600 leading-relaxed">
              Obelisk abstracts the underlying physical storage layer into standard URI semantics. Whether running on a local NVMe drive or an enterprise object store (AWS S3, MinIO, Azure Blob), zero application code or SQL queries need modification.
            </p>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-2">
              <div className="p-4 rounded-lg bg-slate-50 border border-slate-200 space-y-2">
                <div className="text-[11px] font-mono text-slate-500 uppercase">Active STORAGE_URI</div>
                <div className="text-sm font-mono font-bold text-slate-900 bg-white p-2.5 rounded border border-slate-200 break-all">
                  file:///data/lakehouse
                </div>
                <div className="text-[11px] text-slate-500">
                  Target Snappy Parquet destination for raw ingestion streaming.
                </div>
              </div>

              <div className="p-4 rounded-lg bg-slate-50 border border-slate-200 space-y-2">
                <div className="text-[11px] font-mono text-slate-500 uppercase">Parquet Compression</div>
                <div className="text-sm font-mono font-bold text-slate-900 bg-white p-2.5 rounded border border-slate-200">
                  Snappy (Fast analytical scans)
                </div>
                <div className="text-[11px] text-slate-500">
                  High-speed decompression optimized for DuckDB vector scans.
                </div>
              </div>
            </div>

            <div className="p-4 rounded-lg bg-purple-50/60 border border-purple-100 text-xs text-purple-900 space-y-1.5">
              <div className="font-semibold flex items-center gap-1.5">
                <span className="w-1.5 h-1.5 rounded-full bg-purple-600"></span>
                Scale-Out Benefit:
              </div>
              <p className="text-purple-800 leading-relaxed">
                Today on NVMe: <code className="bg-purple-100 px-1 py-0.5 rounded font-mono">STORAGE_URI=file:///data/lakehouse</code> delivers up to 4,000 MB/s native disk throughput.
                Tomorrow on S3/MinIO: simply update the environment variable to <code className="bg-purple-100 px-1 py-0.5 rounded font-mono">STORAGE_URI=s3://my-lakehouse</code>. Every notebook, dbt model, and pipeline executes identically without code changes.
              </p>
            </div>
          </section>
        </div>
      )}

      {/* SUB-TAB 4: SYSTEM DIAGNOSTICS */}
      {activeSubTab === 'diagnostics' && (
        <div className="space-y-6">
          <section className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs space-y-4">
            <div className="flex items-center gap-2">
              <Cpu className="w-4 h-4 text-amber-600" />
              <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider font-mono">
                System Diagnostics & Core Architecture
              </h3>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="p-4 rounded-lg border border-slate-200 bg-slate-50 space-y-2 font-mono">
                <div className="flex items-center justify-between text-xs">
                  <span className="font-bold text-slate-700">Internal Brain (SQLite)</span>
                  <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-50 text-emerald-700 border border-emerald-200">
                    WAL Mode Active
                  </span>
                </div>
                <div className="text-[11px] text-slate-500 font-sans">
                  Located at <code className="text-slate-800 font-mono">data/obelisk.db</code>. Handles asset metadata, pipeline definitions, execution runs, and variables with zero network latency.
                </div>
              </div>

              <div className="p-4 rounded-lg border border-slate-200 bg-slate-50 space-y-2 font-mono">
                <div className="flex items-center justify-between text-xs">
                  <span className="font-bold text-slate-700">Cryptography Vault</span>
                  <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-50 text-emerald-700 border border-emerald-200">
                    Fernet AES-256
                  </span>
                </div>
                <div className="text-[11px] text-slate-500 font-sans">
                  Controlled by <code className="text-slate-800 font-mono">OBELISK_MASTER_KEY</code>. Passwords and secret variables are encrypted with 128-bit CBC AES + HMAC-SHA256 authenticated tokens.
                </div>
              </div>

              <div className="p-4 rounded-lg border border-slate-200 bg-slate-50 space-y-2 font-mono">
                <div className="flex items-center justify-between text-xs">
                  <span className="font-bold text-slate-700">Docker Daemon</span>
                  <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-sky-50 text-sky-700 border border-sky-200">
                    Localhost Socket
                  </span>
                </div>
                <div className="text-[11px] text-slate-500 font-sans">
                  Socket path: <code className="text-slate-800 font-mono">/var/run/docker.sock</code>. Obelisk manages isolated containers strictly on localhost with hardware ceiling enforcement.
                </div>
              </div>

              <div className="p-4 rounded-lg border border-slate-200 bg-slate-50 space-y-2 font-mono">
                <div className="flex items-center justify-between text-xs">
                  <span className="font-bold text-slate-700">Federation Engine</span>
                  <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-sky-50 text-sky-700 border border-sky-200">
                    DuckDB In-Process
                  </span>
                </div>
                <div className="text-[11px] text-slate-500 font-sans">
                  Zero-copy Arrow analytical execution across Snappy Parquet, PostgreSQL, and MySQL without ETL overhead.
                </div>
              </div>
            </div>
          </section>
        </div>
      )}

      {/* SUB-TAB 5: SDK & PLATFORM GUIDE */}
      {activeSubTab === 'sdk' && <SdkDocsView />}

      {/* Modal: Add Variable / Secret */}
      {isAddVarOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 backdrop-blur-xs p-4 animate-in fade-in duration-150">
          <div className="w-full max-w-md bg-white rounded-xl shadow-2xl border border-slate-200 overflow-hidden">
            <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between bg-slate-50/50">
              <div className="flex items-center gap-2">
                <Key className="w-4 h-4 text-sky-600" />
                <h3 className="text-sm font-bold text-slate-900 font-mono">
                  Create Variable or Secret
                </h3>
              </div>
              <button
                onClick={() => setIsAddVarOpen(false)}
                className="text-slate-400 hover:text-slate-600 text-xs font-mono"
              >
                ✕
              </button>
            </div>

            <form onSubmit={handleSaveVariable} className="p-5 space-y-4">
              <div>
                <label className="text-xs font-semibold text-slate-700 block mb-1">
                  Key Name <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  required
                  placeholder="e.g. SLACK_WEBHOOK_URL"
                  value={newVarKey}
                  onChange={(e) => setNewVarKey(e.target.value.toUpperCase().replace(/\s+/g, '_'))}
                  className="w-full px-3 py-2 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500"
                />
                <span className="text-[10px] text-slate-400 font-mono mt-0.5 block">
                  Alphanumeric identifier, uppercase with underscores
                </span>
              </div>

              <div>
                <label className="text-xs font-semibold text-slate-700 block mb-1">
                  Secret Classification
                </label>
                <div className="grid grid-cols-2 gap-2">
                  <button
                    type="button"
                    onClick={() => setNewVarIsSecret(false)}
                    className={`py-2 px-3 rounded-lg border text-xs font-medium flex items-center justify-center gap-1.5 transition-all ${
                      !newVarIsSecret
                        ? 'bg-sky-50 border-sky-300 text-sky-800 font-semibold'
                        : 'bg-white border-slate-200 text-slate-600 hover:bg-slate-50'
                    }`}
                  >
                    <span>Plaintext Variable</span>
                  </button>
                  <button
                    type="button"
                    onClick={() => setNewVarIsSecret(true)}
                    className={`py-2 px-3 rounded-lg border text-xs font-medium flex items-center justify-center gap-1.5 transition-all ${
                      newVarIsSecret
                        ? 'bg-emerald-50 border-emerald-300 text-emerald-800 font-semibold'
                        : 'bg-white border-slate-200 text-slate-600 hover:bg-slate-50'
                    }`}
                  >
                    <Lock className="w-3.5 h-3.5 text-emerald-600" />
                    <span>AES-256 Secret</span>
                  </button>
                </div>
              </div>

              <div>
                <label className="text-xs font-semibold text-slate-700 block mb-1">
                  Value <span className="text-red-500">*</span>
                </label>
                <textarea
                  required
                  rows={3}
                  placeholder={
                    newVarIsSecret
                      ? 'Enter confidential secret (encrypted at rest)...'
                      : 'Enter string or numeric value...'
                  }
                  value={newVarValue}
                  onChange={(e) => setNewVarValue(e.target.value)}
                  className="w-full px-3 py-2 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500"
                />
              </div>

              <div>
                <label className="text-xs font-semibold text-slate-700 block mb-1">
                  Description <span className="text-slate-400 font-normal">(optional)</span>
                </label>
                <input
                  type="text"
                  placeholder="e.g. Incoming webhook for alerting failed pipeline runs"
                  value={newVarDesc}
                  onChange={(e) => setNewVarDesc(e.target.value)}
                  className="w-full px-3 py-2 rounded-lg border border-slate-200 text-xs bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500"
                />
              </div>

              <div className="flex items-center justify-end gap-2 pt-2 border-t border-slate-100">
                <button
                  type="button"
                  onClick={() => setIsAddVarOpen(false)}
                  className="px-3 py-1.5 rounded-lg border border-slate-200 bg-white hover:bg-slate-50 text-xs font-medium text-slate-700"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={savingVar || !newVarKey.trim() || !newVarValue.trim()}
                  className="px-4 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold shadow-xs disabled:opacity-50 transition-all"
                >
                  {savingVar ? 'Saving...' : 'Save Variable'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Modal: Add / Edit Connection */}
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
          loadData(true);
        }}
      />
    </div>
  );
};
