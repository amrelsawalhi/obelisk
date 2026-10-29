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
  Database,
  Wrench,
  SquareTerminal,
} from 'lucide-react';
import { api } from '../../lib/api';
import type {
  VariableItem,
  Connection,
  ConnectionTestResult,
  DbtWarehouseInfo,
  LakehouseDiagnostics,
  TelemetryData,
} from '../../lib/types';
import { formatBytes } from '../../lib/format';
import { toast } from '../../lib/toast';
import { useDevToolsEnabled } from '../../lib/devTools';
import { AddConnectionModal } from '../catalog/AddConnectionModal';
import { ConfirmDialog } from '../common/ConfirmDialog';
import type { ConfirmRequest } from '../common/ConfirmDialog';
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
  // Non-null means the variable modal is editing that row instead of creating one.
  const [editingVar, setEditingVar] = useState<VariableItem | null>(null);
  const [varValueLoading, setVarValueLoading] = useState(false);

  // Lakehouse storage facts. The Storage and Diagnostics tabs read the same payload,
  // so they cannot disagree about what the lakehouse costs.
  const [diagnostics, setDiagnostics] = useState<LakehouseDiagnostics | null>(null);
  const [diagnosticsError, setDiagnosticsError] = useState<string | null>(null);
  const [diagnosticsLoading, setDiagnosticsLoading] = useState(false);
  const [maintaining, setMaintaining] = useState(false);
  const [keepDays, setKeepDays] = useState(7);
  const [deleteFiles, setDeleteFiles] = useState(true);
  // Live container count and RAM come from the services telemetry probe, which is
  // the same payload Home reads; null means the probe failed, and the cards say so.
  const [telemetry, setTelemetry] = useState<TelemetryData | null>(null);

  // Developer Terminal is opt-in (see `lib/devTools.ts`): the sidebar hides it
  // until the operator enables shell access to the workspace container.
  const [devToolsEnabled, setDevToolsEnabled] = useDevToolsEnabled();

  // Destructive actions confirm in-app instead of in a browser dialog.
  const [confirmRequest, setConfirmRequest] = useState<ConfirmRequest | null>(null);

  // Connection Vault State
  const [connections, setConnections] = useState<Connection[]>([]);
  const [isAddConnectionOpen, setIsAddConnectionOpen] = useState(false);
  const [editingConnection, setEditingConnection] = useState<Connection | null>(null);
  const [testingConnId, setTestingConnId] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<Record<string, ConnectionTestResult>>({});
  const [deletingConnId, setDeletingConnId] = useState<string | null>(null);
  // Which connection dbt publishes marts into. A decision, stored server-side — the
  // platform no longer infers it from whichever Postgres row is newest.
  const [warehouse, setWarehouse] = useState<DbtWarehouseInfo | null>(null);
  const [warehouseSaving, setWarehouseSaving] = useState(false);

  const loadData = async (silent = false) => {
    try {
      if (!silent) setLoading(true);
      else setRefreshing(true);
      setError(null);

      const [varsData, connsData, warehouseData] = await Promise.all([
        api.getVariables(),
        api.listConnections(),
        api.getDbtWarehouse().catch(() => null),
      ]);

      setVariables(varsData);
      setConnections(connsData);
      if (warehouseData) setWarehouse(warehouseData.warehouse);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Failed to fetch settings';
      setError(msg);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const chooseWarehouse = async (connectionId: string) => {
    setWarehouseSaving(true);
    try {
      const res = await api.setDbtWarehouse(connectionId || null);
      setWarehouse(res.warehouse);
    } catch (e: unknown) {
      const msg =
        e && typeof e === 'object' && 'response' in e
          ? (e as { response?: { data?: { detail?: string } } }).response?.data?.detail ?? 'Could not set the warehouse.'
          : 'Could not set the warehouse.';
      setError(msg);
    } finally {
      setWarehouseSaving(false);
    }
  };

  const loadDiagnostics = async () => {
    setDiagnosticsLoading(true);
    setDiagnosticsError(null);
    try {
      const [lakehouse, host] = await Promise.all([
        api.getLakehouseDiagnostics(),
        // The container numbers live in the services probe; a failure there must not
        // hide the lakehouse facts, so it degrades to "not reported" per card.
        api.getTelemetry().catch(() => null),
      ]);
      setDiagnostics(lakehouse);
      setTelemetry(host);
    } catch (e: unknown) {
      setDiagnosticsError(
        e instanceof Error ? e.message : 'Could not read the lakehouse diagnostics.',
      );
    } finally {
      setDiagnosticsLoading(false);
    }
  };

  // Only the two tabs that show these numbers pay for reading them.
  useEffect(() => {
    if (activeSubTab !== 'storage' && activeSubTab !== 'diagnostics') return;
    void loadDiagnostics();
  }, [activeSubTab]);

  const runMaintenance = async () => {
    setMaintaining(true);
    try {
      const result = await api.maintainLakehouse({
        keep_days: keepDays,
        delete_files: deleteFiles,
      });
      const reclaimed = Math.max(result.before.data_bytes - result.after.data_bytes, 0);
      toast.success(
        `Compacted ${result.tables_compacted} table(s), expired ${result.snapshots_expired} snapshot(s)` +
          (result.delete_files ? `, reclaimed ${formatBytes(reclaimed) ?? '0 B'}.` : '.'),
      );
      if (result.orphaned_load_tables_dropped > 0 || result.empty_directories_removed > 0) {
        toast.info(
          `Dropped ${result.orphaned_load_tables_dropped} orphaned load table(s) and swept ` +
            `${result.empty_directories_removed} empty director(ies).`,
        );
      }
      await loadDiagnostics();
    } catch (e: unknown) {
      toast.error(
        e instanceof Error ? e.message : 'Maintenance failed. Check the backend log for the DuckLake error.',
      );
    } finally {
      setMaintaining(false);
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

  const openCreateVariable = () => {
    setEditingVar(null);
    setNewVarKey('');
    setNewVarValue('');
    setNewVarIsSecret(false);
    setNewVarDesc('');
    setIsAddVarOpen(true);
  };

  const openEditVariable = async (item: VariableItem) => {
    setEditingVar(item);
    setNewVarKey(item.key);
    setNewVarDesc(item.description ?? '');
    setNewVarIsSecret(item.is_secret);
    setNewVarValue('');
    setIsAddVarOpen(true);
    // The list hands back a mask for secrets, so the current value has to be asked
    // for; otherwise "edit" would quietly mean "replace with whatever I type next".
    setVarValueLoading(true);
    try {
      const res = await api.revealVariable(item.key);
      setNewVarValue(res.value);
    } catch (e: unknown) {
      toast.error(
        e instanceof Error
          ? e.message
          : `Could not read the current value of ${item.key}. Type it to replace it.`,
      );
    } finally {
      setVarValueLoading(false);
    }
  };

  const handleDeleteVariable = (item: VariableItem) => {
    setConfirmRequest({
      title: `Delete ${item.key}?`,
      body: 'Notebooks, dbt models and ingest jobs that read it fail on their next run.',
      confirmLabel: 'Delete variable',
      destructive: true,
      onConfirm: async () => {
        try {
          await api.deleteVariable(item.id);
          await loadData(true);
          toast.success(`Deleted ${item.key}.`);
        } catch (e: unknown) {
          const msg = e instanceof Error ? e.message : 'Failed to delete variable';
          setError(msg);
        }
      },
    });
  };

  const handleSaveVariable = async (e: React.FormEvent) => {
    e.preventDefault();
    const key = newVarKey.trim().toUpperCase();
    if (!key || !newVarValue.trim()) return;

    setSavingVar(true);
    setError(null);
    try {
      // POST upserts on the key, so saving an edit rewrites the row in place instead
      // of adding a second variable under a name nobody noticed was different.
      await api.createVariable({
        key,
        value: newVarValue,
        is_secret: newVarIsSecret,
        description: newVarDesc.trim(),
      });
      setIsAddVarOpen(false);
      setEditingVar(null);
      setNewVarKey('');
      setNewVarValue('');
      setNewVarIsSecret(false);
      setNewVarDesc('');
      await loadData(true);
      toast.success(editingVar ? `Updated ${key}.` : `Saved ${key}.`);
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

  const handleDeleteConnection = (connId: string) => {
    const target = connections.find((c) => c.id === connId);
    setConfirmRequest({
      title: `Delete ${target?.name ?? 'this connection'}?`,
      body: 'The credentials are removed from the vault. Pipelines and dbt models that publish to it stop working.',
      confirmLabel: 'Delete connection',
      destructive: true,
      onConfirm: async () => {
        setDeletingConnId(connId);
        try {
          await api.deleteConnection(connId);
          await loadData(true);
          toast.success(`Deleted ${target?.name ?? 'connection'}.`);
        } catch (e: unknown) {
          const msg = e instanceof Error ? e.message : 'Failed to delete connection';
          setError(msg);
        } finally {
          setDeletingConnId(null);
        }
      },
    });
  };

  if (loading && variables.length === 0 && connections.length === 0) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center p-12 bg-slate-50">
        <RefreshCw className="w-8 h-8 text-sky-600 animate-spin mb-3" />
        <div className="text-sm font-semibold text-slate-700">Loading Obelisk Settings...</div>
        <div className="text-xs text-slate-400 font-mono mt-1">Reading database configuration & AES-128 vault</div>
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
          <div className="bg-gradient-to-r from-sky-50/80 to-slate-50 border border-sky-100 rounded-xl p-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div className="space-y-1">
              <div className="flex items-center gap-2">
                <span className="flex h-2 w-2 rounded-full bg-sky-600" />
                <h4 className="text-xs font-bold text-sky-900 uppercase tracking-wider font-mono">
                  Universal Variables & Zero-Knowledge Vault
                </h4>
              </div>
              <p className="text-xs text-slate-600 max-w-2xl">
                Define plaintext configuration and AES-128 encrypted secrets. Access them effortlessly across Marimo reactive notebooks using <code className="font-mono text-sky-800 bg-sky-100/70 px-1.5 py-0.5 rounded font-semibold">obelisk.var("KEY")</code> or within dbt models and ingestion pipelines.
              </p>
            </div>
            <div className="flex items-center gap-2 shrink-0">
              <button
                onClick={openCreateVariable}
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
                              AES-128 Vault
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
                              onClick={() => void openEditVariable(v)}
                              className="p-1 rounded-md border border-slate-200 bg-white hover:bg-slate-50 text-slate-600 hover:text-sky-700 shadow-2xs transition-colors"
                              title="Edit Value & Description"
                            >
                              <Edit2 className="w-3.5 h-3.5" />
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
                AES-128 Connection Vault (External Data Sources)
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
            Enterprise database and API credentials are encrypted at rest with Fernet AES-128 tokens in <code className="font-mono text-slate-700 bg-slate-100 px-1 py-0.5 rounded">data/obelisk.db</code>.
          </p>

          {/* Where the marts go is a choice, so it is made here instead of inferred */}
          <div className="flex flex-wrap items-center gap-3 p-3 bg-slate-50 border border-slate-200 rounded-lg">
            <div className="flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-wider text-slate-600">
              <Database className="w-3.5 h-3.5 text-slate-400" />
              Publish marts into
            </div>
            <select
              value={warehouse?.id ?? ''}
              onChange={(e) => void chooseWarehouse(e.target.value)}
              disabled={warehouseSaving}
              className="text-[11px] font-mono bg-white border border-slate-300 rounded-md px-2 py-1 outline-hidden focus:border-sky-500"
            >
              <option value="">Not selected — publishing is refused</option>
              {connections
                .filter((c) => ['postgres', 'postgresql'].includes(c.source_type))
                .map((c) => (
                  <option key={c.id} value={c.id} disabled={c.read_only !== false}>
                    {c.name} · {c.host}:{c.port}/{c.database}
                    {c.read_only !== false ? ' (read-only)' : ''}
                  </option>
                ))}
            </select>
            <span className="text-[10px] text-slate-500">
              {warehouse?.message ||
                (warehouse?.read_only
                  ? 'Read-only target: publishing is refused.'
                  : 'dbt builds and obelisk.save() publish here.')}
            </span>
          </div>

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
                          {conn.read_only !== false ? (
                            <span
                              className="ml-1 px-2 py-0.5 rounded text-[10px] uppercase font-semibold bg-slate-100 text-slate-600 border border-slate-200"
                              title="Obelisk reads this source and never writes into it."
                            >
                              Read-only
                            </span>
                          ) : (
                            <span
                              className="ml-1 px-2 py-0.5 rounded text-[10px] uppercase font-semibold bg-sky-50 text-sky-700 border border-sky-200"
                              title="Obelisk may publish dbt models or a saved DataFrame into this database."
                            >
                              Writable
                            </span>
                          )}
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
            <div className="flex items-start justify-between gap-3">
              <div className="flex items-center gap-2">
                <HardDrive className="w-4 h-4 text-purple-600" />
                <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider font-mono">
                  DuckLake Storage & Catalog
                </h3>
              </div>
              <button
                onClick={() => void loadDiagnostics()}
                disabled={diagnosticsLoading}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 bg-white hover:bg-slate-50 text-xs font-medium text-slate-700 shadow-2xs transition-all disabled:opacity-60"
              >
                <RefreshCw
                  className={`w-3.5 h-3.5 ${diagnosticsLoading ? 'animate-spin text-sky-600' : 'text-slate-500'}`}
                />
                <span>Refresh</span>
              </button>
            </div>

            {diagnosticsError && (
              <div className="p-3 bg-red-50 border border-red-200 rounded-lg flex items-center gap-2.5 text-xs text-red-700">
                <AlertTriangle className="w-4 h-4 text-red-500 shrink-0" />
                <span>{diagnosticsError}</span>
              </div>
            )}

            {!diagnostics && diagnosticsLoading && (
              <div className="text-xs text-slate-500 font-mono flex items-center gap-2">
                <RefreshCw className="w-3.5 h-3.5 animate-spin text-sky-600" />
                Reading the catalog…
              </div>
            )}

            {diagnostics && (
              <>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-2">
                  <div className="p-4 rounded-lg bg-slate-50 border border-slate-200 space-y-2">
                    <div className="text-[11px] font-mono text-slate-500 uppercase">
                      Storage Root URI
                    </div>
                    <div className="text-sm font-mono font-bold text-slate-900 bg-white p-2.5 rounded border border-slate-200 break-all">
                      {diagnostics.storage_uri}
                    </div>
                    <div className="text-[11px] text-slate-500 font-mono break-all">
                      {diagnostics.lakehouse_root}
                    </div>
                  </div>

                  <div className="p-4 rounded-lg bg-slate-50 border border-slate-200 space-y-2">
                    <div className="text-[11px] font-mono text-slate-500 uppercase">
                      Catalog Engine
                    </div>
                    <div className="text-sm font-mono font-bold text-slate-900 bg-white p-2.5 rounded border border-slate-200">
                      DuckLake SQLite (.obelisk/catalog.sqlite)
                    </div>
                    <div className="text-[11px] text-slate-500">
                      {formatBytes(diagnostics.catalog_sqlite_bytes) ?? '0 B'} of metadata ·{' '}
                      {diagnostics.snapshot_count.toLocaleString()} snapshots · latest #
                      {diagnostics.current_snapshot_id ?? '—'}
                    </div>
                  </div>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                  <div className="p-3.5 rounded-lg border border-slate-200 bg-white">
                    <div className="text-[10px] font-mono uppercase font-bold text-slate-500">
                      Active Tables
                    </div>
                    <div className="text-xl font-bold font-mono text-slate-900 mt-1">
                      {diagnostics.active_table_count.toLocaleString()}
                    </div>
                  </div>
                  <div className="p-3.5 rounded-lg border border-slate-200 bg-white">
                    <div className="text-[10px] font-mono uppercase font-bold text-slate-500">
                      Parquet Data Files
                    </div>
                    <div className="text-xl font-bold font-mono text-slate-900 mt-1">
                      {diagnostics.active_data_files.toLocaleString()}
                    </div>
                  </div>
                  <div className="p-3.5 rounded-lg border border-slate-200 bg-white">
                    <div className="text-[10px] font-mono uppercase font-bold text-slate-500">
                      Live Data Size
                    </div>
                    <div className="text-xl font-bold font-mono text-slate-900 mt-1">
                      {formatBytes(diagnostics.total_data_bytes) ?? '0 B'}
                    </div>
                  </div>
                </div>

                {diagnostics.orphaned_load_tables > 0 && (
                  <div className="p-3 rounded-lg bg-amber-50 border border-amber-200 text-[11px] text-amber-900 flex items-start gap-2">
                    <AlertTriangle className="w-4 h-4 text-amber-600 shrink-0 mt-0.5" />
                    <span>
                      {diagnostics.orphaned_load_tables} load table(s) were left behind by a killed
                      ingest. They hold bytes nothing reads; Run Maintenance below drops them.
                    </span>
                  </div>
                )}
              </>
            )}

            <div className="p-4 rounded-lg bg-purple-50/60 border border-purple-100 text-xs text-purple-900 space-y-1.5">
              <div className="font-semibold flex items-center gap-1.5">
                <span className="w-1.5 h-1.5 rounded-full bg-purple-600"></span>
                Retention and backup on local NVMe
              </div>
              <p className="text-purple-800 leading-relaxed">
                DuckLake keeps a snapshot per write and never expires one on its own, so the catalog
                grows until maintenance runs. The storage root is a local path — there is no
                object-store backend — so a backup is a copy:{' '}
                <code className="bg-purple-100 px-1 py-0.5 rounded font-mono break-all">
                  python backend/scripts/lakehouse_maintenance.py backup --destination D:\backups
                </code>{' '}
                copies the lakehouse and snapshots the control-plane database. The same script takes{' '}
                <code className="bg-purple-100 px-1 py-0.5 rounded font-mono">
                  maintain --keep-days 14 --delete-files
                </code>{' '}
                for a scheduled run outside the UI.
              </p>
            </div>
          </section>

          {/* The reclaim path for everything the numbers above report */}
          <section className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs space-y-4">
            <div className="flex items-center gap-2">
              <Wrench className="w-4 h-4 text-slate-600" />
              <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider font-mono">
                Compact & Expire Snapshots
              </h3>
            </div>
            <p className="text-xs text-slate-600 leading-relaxed">
              Every write adds a DuckLake snapshot and new Parquet files. Merging adjacent files and
              expiring old snapshots is what turns thousands of small files back into scan-able
              ones. Run it with no ingest or pipeline writing: an interrupted run is what leaves the
              load tables this same run drops.
            </p>

            <div className="flex flex-wrap items-end gap-4 p-3 bg-slate-50 border border-slate-200 rounded-lg">
              <label className="flex flex-col gap-1">
                <span className="text-[10px] font-mono uppercase font-bold text-slate-500">
                  Keep snapshots (days)
                </span>
                <input
                  type="number"
                  min={1}
                  max={365}
                  value={keepDays}
                  onChange={(e) =>
                    setKeepDays(Math.min(365, Math.max(1, Number(e.target.value) || 1)))
                  }
                  className="w-24 text-xs font-mono bg-white border border-slate-300 rounded-md px-2 py-1 outline-hidden focus:border-sky-500"
                />
              </label>
              <label className="flex items-center gap-2 text-[11px] text-slate-700 pb-1.5">
                <input
                  type="checkbox"
                  checked={deleteFiles}
                  onChange={(e) => setDeleteFiles(e.target.checked)}
                  className="rounded border-slate-300 text-sky-600 focus:ring-sky-500/30"
                />
                <span>
                  Also delete files nothing references{' '}
                  <span className="text-rose-600 font-semibold">(irreversible)</span>
                </span>
              </label>
              <button
                onClick={() => void runMaintenance()}
                disabled={maintaining}
                className="ml-auto flex items-center gap-1.5 px-3.5 py-2 rounded-lg bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold shadow-xs transition-all disabled:opacity-60"
              >
                <RefreshCw className={`w-3.5 h-3.5 ${maintaining ? 'animate-spin' : ''}`} />
                <span>{maintaining ? 'Running maintenance…' : 'Run Maintenance'}</span>
              </button>
            </div>

            <p className="text-[11px] text-slate-500">
              Time travel reaches back {keepDays} day(s) once this has run. The response reports what
              each step did — tables compacted, snapshots expired, files removed — in a toast.
            </p>
          </section>
        </div>
      )}

      {/* SUB-TAB 4: SYSTEM DIAGNOSTICS */}
      {activeSubTab === 'diagnostics' && (
        <div className="space-y-6">
          <section className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs space-y-4">
            <div className="flex items-center justify-between gap-3">
              <div className="flex items-center gap-2">
                <Cpu className="w-4 h-4 text-amber-600" />
                <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider font-mono">
                  System Diagnostics & Core Architecture
                </h3>
              </div>
              <button
                onClick={() => void loadDiagnostics()}
                disabled={diagnosticsLoading}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 bg-white hover:bg-slate-50 text-xs font-medium text-slate-700 shadow-2xs transition-all disabled:opacity-60"
              >
                <RefreshCw
                  className={`w-3.5 h-3.5 ${diagnosticsLoading ? 'animate-spin text-sky-600' : 'text-slate-500'}`}
                />
                <span>Refresh</span>
              </button>
            </div>

            {diagnosticsError && (
              <div className="p-3 bg-red-50 border border-red-200 rounded-lg flex items-center gap-2.5 text-xs text-red-700">
                <AlertTriangle className="w-4 h-4 text-red-500 shrink-0" />
                <span>{diagnosticsError}</span>
              </div>
            )}

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="p-4 rounded-lg border border-slate-200 bg-slate-50 space-y-2 font-mono">
                <div className="flex items-center justify-between text-xs">
                  <span className="font-bold text-slate-700">Control Plane DB</span>
                  <span
                    className={`px-2 py-0.5 rounded text-[10px] font-bold border ${
                      diagnostics?.obelisk_db_journal_mode === 'WAL'
                        ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
                        : 'bg-amber-50 text-amber-700 border-amber-200'
                    }`}
                  >
                    {diagnostics ? `${diagnostics.obelisk_db_journal_mode} journal` : 'reading…'}
                  </span>
                </div>
                <div className="text-lg font-bold text-slate-900">
                  {formatBytes(diagnostics?.obelisk_db_bytes) ?? '—'}
                </div>
                <div className="text-[11px] text-slate-500 font-sans">
                  <code className="text-slate-800 font-mono">data/obelisk.db</code> holds assets,
                  pipeline definitions, runs and variables. The journal mode above is what SQLite
                  answers, not what the app asked for.
                </div>
              </div>

              <div className="p-4 rounded-lg border border-slate-200 bg-slate-50 space-y-2 font-mono">
                <div className="flex items-center justify-between text-xs">
                  <span className="font-bold text-slate-700">DuckDB Execution Engine</span>
                  <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-sky-50 text-sky-700 border border-sky-200">
                    In-Process
                  </span>
                </div>
                <div className="grid grid-cols-2 gap-x-3 gap-y-1 text-[11px] text-slate-600">
                  <span>
                    Memory / slot{' '}
                    <b className="text-slate-900">
                      {diagnostics?.duckdb_memory_limit_per_slot ?? '—'}
                    </b>
                  </span>
                  <span>
                    Row cap{' '}
                    <b className="text-slate-900">
                      {diagnostics?.duckdb_max_query_rows.toLocaleString() ?? '—'}
                    </b>
                  </span>
                  <span>
                    Temp spill{' '}
                    <b className="text-slate-900">
                      {formatBytes(diagnostics?.duckdb_tmp_bytes) ?? '—'}
                    </b>
                  </span>
                  <span>
                    Temp limit{' '}
                    <b className="text-slate-900">{diagnostics?.duckdb_tmp_limit ?? '—'}</b>
                  </span>
                </div>
                <div className="text-[11px] text-slate-500 font-sans">
                  Sort and join spill goes to disk under{' '}
                  <code className="text-slate-800 font-mono">duckdb_tmp</code>; the limit above is
                  what stops it from filling the NVMe.
                </div>
              </div>

              <div className="p-4 rounded-lg border border-slate-200 bg-slate-50 space-y-2 font-mono">
                <div className="flex items-center justify-between text-xs">
                  <span className="font-bold text-slate-700">DuckLake Metadata Catalog</span>
                  <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-sky-50 text-sky-700 border border-sky-200">
                    .obelisk/catalog.sqlite
                  </span>
                </div>
                <div className="text-lg font-bold text-slate-900">
                  {formatBytes(diagnostics?.catalog_sqlite_bytes) ?? '—'}
                </div>
                <div className="grid grid-cols-2 gap-x-3 gap-y-1 text-[11px] text-slate-600">
                  <span>
                    Snapshot{' '}
                    <b className="text-slate-900">#{diagnostics?.current_snapshot_id ?? '—'}</b>
                  </span>
                  <span>
                    Snapshots kept{' '}
                    <b className="text-slate-900">
                      {diagnostics?.snapshot_count.toLocaleString() ?? '—'}
                    </b>
                  </span>
                  <span>
                    Active tables{' '}
                    <b className="text-slate-900">
                      {diagnostics?.active_table_count.toLocaleString() ?? '—'}
                    </b>
                  </span>
                  <span>
                    Orphaned loads{' '}
                    <b
                      className={
                        (diagnostics?.orphaned_load_tables ?? 0) > 0
                          ? 'text-amber-700'
                          : 'text-slate-900'
                      }
                    >
                      {diagnostics?.orphaned_load_tables.toLocaleString() ?? '—'}
                    </b>
                  </span>
                </div>
                <div className="text-[11px] text-slate-500 font-sans">
                  The catalog is a SQLite file beside the Parquet it points at. Its snapshot id is
                  the version every new connection reads.
                </div>
              </div>

              <div className="p-4 rounded-lg border border-slate-200 bg-slate-50 space-y-2 font-mono">
                <div className="flex items-center justify-between text-xs">
                  <span className="font-bold text-slate-700">Containers & Secrets Vault</span>
                  <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-50 text-emerald-700 border border-emerald-200">
                    Fernet AES-128
                  </span>
                </div>
                <div className="grid grid-cols-2 gap-x-3 gap-y-1 text-[11px] text-slate-600">
                  <span>
                    Running containers{' '}
                    <b className="text-slate-900">
                      {telemetry ? telemetry.active_containers_count : 'not reported'}
                    </b>
                  </span>
                  <span>
                    Allocated RAM{' '}
                    <b className="text-slate-900">
                      {telemetry ? `${telemetry.allocated_ram_gb.toFixed(1)} GB` : 'not reported'}
                    </b>
                  </span>
                  <span>
                    Source connections <b className="text-slate-900">{connections.length}</b>
                  </span>
                  <span>
                    Encrypted secrets{' '}
                    <b className="text-slate-900">
                      {variables.filter((v) => v.is_secret).length}
                    </b>
                  </span>
                </div>
                <div className="text-[11px] text-slate-500 font-sans">
                  Container numbers come from the services telemetry probe; credentials and secrets
                  are Fernet tokens under <code className="text-slate-800 font-mono">OBELISK_MASTER_KEY</code>.
                </div>
              </div>
            </div>

            {/* Developer Terminal is opt-in for shell access to the workspace container */}
            <div className="flex items-start justify-between gap-4 p-4 rounded-lg border border-slate-200 bg-slate-50">
              <div className="space-y-1">
                <div className="text-xs font-bold text-slate-700 font-mono flex items-center gap-1.5">
                  <SquareTerminal className="w-3.5 h-3.5 text-slate-500" />
                  Developer Terminal
                </div>
                <div className="text-[11px] text-slate-500 leading-relaxed">
                  Adds an embedded <b>Terminal</b> to the sidebar under Platform for shell access to the
                  workspace container and Obelisk CLI. Off by default.
                </div>
              </div>
              <button
                type="button"
                role="switch"
                aria-checked={devToolsEnabled}
                onClick={() => setDevToolsEnabled(!devToolsEnabled)}
                className={`shrink-0 w-11 h-6 rounded-full transition-colors relative ${
                  devToolsEnabled ? 'bg-sky-600' : 'bg-slate-300'
                }`}
                title={devToolsEnabled ? 'Hide Terminal' : 'Show Terminal'}
              >
                <span
                  className={`absolute top-0.5 w-5 h-5 rounded-full bg-white shadow-sm transition-all ${
                    devToolsEnabled ? 'left-[22px]' : 'left-0.5'
                  }`}
                />
              </button>
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
                  {editingVar ? 'Edit Variable or Secret' : 'Create Variable or Secret'}
                </h3>
              </div>
              <button
                onClick={() => {
                  setIsAddVarOpen(false);
                  setEditingVar(null);
                }}
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
                  readOnly={Boolean(editingVar)}
                  placeholder="e.g. SLACK_WEBHOOK_URL"
                  value={newVarKey}
                  onChange={(e) => setNewVarKey(e.target.value.toUpperCase().replace(/\s+/g, '_'))}
                  className={`w-full px-3 py-2 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500 ${
                    editingVar ? 'bg-slate-50 text-slate-500 cursor-not-allowed' : ''
                  }`}
                />
                <span className="text-[10px] text-slate-400 font-mono mt-0.5 block">
                  {editingVar
                    ? 'The key is the row identity — delete and recreate it to rename.'
                    : 'Alphanumeric identifier, uppercase with underscores'}
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
                    <span>AES-128 Secret</span>
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
                  disabled={varValueLoading}
                  placeholder={
                    varValueLoading
                      ? 'Reading the current value…'
                      : newVarIsSecret
                        ? 'Enter confidential secret (encrypted at rest)...'
                        : 'Enter string or numeric value...'
                  }
                  value={newVarValue}
                  onChange={(e) => setNewVarValue(e.target.value)}
                  className="w-full px-3 py-2 rounded-lg border border-slate-200 text-xs font-mono bg-white focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500 disabled:bg-slate-50"
                />
                {varValueLoading && (
                  <span className="text-[10px] text-sky-700 font-mono mt-0.5 flex items-center gap-1">
                    <RefreshCw className="w-3 h-3 animate-spin" /> Reading the current value…
                  </span>
                )}
                {editingVar && !varValueLoading && (
                  <span className="text-[10px] text-slate-400 font-mono mt-0.5 block">
                    Saving replaces the stored value of {editingVar.key}.
                  </span>
                )}
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
                  onClick={() => {
                    setIsAddVarOpen(false);
                    setEditingVar(null);
                  }}
                  className="px-3 py-1.5 rounded-lg border border-slate-300 bg-white hover:bg-slate-50 text-xs font-semibold text-slate-700 shadow-2xs transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={savingVar || varValueLoading || !newVarKey.trim() || !newVarValue.trim()}
                  className="px-4 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold shadow-2xs disabled:opacity-50 transition-colors"
                >
                  {savingVar ? 'Saving...' : editingVar ? 'Save Changes' : 'Save Variable'}
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

      {/* Every destructive action in Settings goes through one dialog */}
      <ConfirmDialog request={confirmRequest} onClose={() => setConfirmRequest(null)} />
    </div>
  );
};
