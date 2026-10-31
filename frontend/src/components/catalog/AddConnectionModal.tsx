import React, { useState, useEffect } from 'react';
import { X, Server, Lock, RefreshCw, AlertCircle, Zap, CheckCircle2 } from 'lucide-react';
import { api } from '../../lib/api';
import type { Connection } from '../../lib/types';

interface AddConnectionModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSuccess: () => void;
  initialConnection?: Connection | null;
}

export type SupportedSourceType = 'postgres' | 'mysql' | 'oracle' | 'mongodb' | 'api';

export const AddConnectionModal: React.FC<AddConnectionModalProps> = ({
  isOpen,
  onClose,
  onSuccess,
  initialConnection = null,
}) => {
  const [sourceType, setSourceType] = useState<SupportedSourceType>('postgres');
  const [name, setName] = useState('');
  const [host, setHost] = useState('localhost');
  const [port, setPort] = useState(5432);
  const [database, setDatabase] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [dataKey, setDataKey] = useState('');
  // REST ingestion options. Paging lives in dedicated fields because it is the one
  // setting every paginated source needs; everything else (auth, retry, budget, POST
  // body) stays in the JSON escape hatch, which is exactly what the backend parses.
  const [apiPresets, setApiPresets] = useState<import('../../lib/types').ApiIngestPreset[]>([]);
  const [presetId, setPresetId] = useState('');
  const [paginationType, setPaginationType] = useState('none');
  const [pageSize, setPageSize] = useState(100);
  const [nextKey, setNextKey] = useState('');
  const [apiAdvanced, setApiAdvanced] = useState('');
  // The operator's own decision, not something inferred at write time: a Postgres source
  // defaults to read-only because it is read live, and everything else defaults to
  // writable. Sending it explicitly is what makes the choice survive the form.
  const [readOnly, setReadOnly] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<{ status: 'ok' | 'error'; message: string; latency_ms?: number } | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (initialConnection) {
      setName(initialConnection.name);
      setSourceType((initialConnection.source_type as SupportedSourceType) || 'postgres');
      setHost(initialConnection.host);
      setPort(initialConnection.port);
      setDatabase(initialConnection.database);
      setUsername(initialConnection.username);
      setPassword('');
      const opts = initialConnection.options as Record<string, any> | undefined;
      setDataKey(opts?.data_key || '');
      const pagination = (opts?.pagination as Record<string, any>) || {};
      setPaginationType(typeof pagination.type === 'string' ? pagination.type : 'none');
      setPageSize(Number(pagination.page_size) || 100);
      setNextKey(typeof pagination.next_key === 'string' ? pagination.next_key : '');
      if (opts) {
        const rest: Record<string, any> = { ...opts };
        delete rest.data_key;
        delete rest.pagination;
        delete rest.endpoint;
        setApiAdvanced(Object.keys(rest).length ? JSON.stringify(rest, null, 2) : '');
      } else {
        setApiAdvanced('');
      }
      setPresetId('');
      setReadOnly(initialConnection.read_only ?? initialConnection.source_type === 'postgres');
    } else {
      setName('');
      setSourceType('postgres');
      setHost('localhost');
      setPort(5432);
      setDatabase('');
      setUsername('');
      setPassword('');
      setDataKey('');
      setPaginationType('none');
      setPageSize(100);
      setNextKey('');
      setApiAdvanced('');
      setPresetId('');
      setReadOnly(true);
    }
    setError(null);
  }, [initialConnection, isOpen]);

  // Presets are fetched the first time the REST source is chosen; a failure leaves the
  // list empty rather than blocking the form (manual configuration always works). This
  // must stay above the `!isOpen` early return: a hook below it would change the hook
  // count between the closed and open renders.
  useEffect(() => {
    if (!isOpen || sourceType !== 'api' || apiPresets.length) return;
    api
      .getApiPresets()
      .then(setApiPresets)
      .catch(() => setApiPresets([]));
  }, [isOpen, sourceType, apiPresets.length]);

  if (!isOpen) return null;

  const handleTypeChange = (type: SupportedSourceType) => {
    setSourceType(type);
    if (type === 'postgres') setPort(5432);
    else if (type === 'mysql') setPort(3306);
    else if (type === 'oracle') setPort(1521);
    else if (type === 'mongodb') setPort(27017);
    else if (type === 'api') setPort(443);
    // A fresh choice follows the safe default for that driver; the operator can still
    // override it below, and once they do we stop moving it for them.
    setReadOnly(type === 'postgres');
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setError(null);

    try {
      let options: Record<string, any>;
      try {
        options = buildOptions();
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Invalid advanced options JSON');
        setSubmitting(false);
        return;
      }

      if (initialConnection) {
        await api.updateConnection(initialConnection.id, {
          name: name.trim(),
          source_type: sourceType,
          host: host.trim(),
          port: Number(port),
          database: database.trim(),
          username: username.trim(),
          ...(password ? { password } : {}),
          options,
          read_only: readOnly,
        });
      } else {
        await api.createConnection({
          name: name.trim(),
          source_type: sourceType,
          host: host.trim(),
          port: Number(port),
          database: database.trim(),
          username: username.trim(),
          password,
          options,
          read_only: readOnly,
        });
      }

      onSuccess();
      onClose();
    } catch (err: unknown) {
      const msg =
        err && typeof err === 'object' && 'response' in err && (err as { response?: { data?: { detail?: string } } }).response?.data?.detail
          ? (err as { response: { data: { detail: string } } }).response.data.detail
          : err instanceof Error
          ? err.message
          : 'Failed to save source connection';
      setError(msg);
    } finally {
      setSubmitting(false);
    }
  };

  const handleTestConnection = async () => {
    setTesting(true);
    setTestResult(null);
    setError(null);
    try {
      let options: Record<string, any>;
      try {
        options = buildOptions();
      } catch (err) {
        setTestResult({
          status: 'error',
          message: err instanceof Error ? err.message : 'Invalid advanced options JSON',
        });
        setTesting(false);
        return;
      }

      const res = await api.testConnectionParams({
        name: name.trim() || 'test_probe',
        source_type: sourceType,
        host: host.trim() || 'localhost',
        port: Number(port),
        database: database.trim(),
        username: username.trim(),
        password,
        options,
      });

      if (res.status === 'success') {
        setTestResult({
          status: 'ok',
          message: res.message || 'Connection verified',
          latency_ms: res.latency_ms,
        });
      } else {
        setTestResult({
          status: 'error',
          message: res.message || 'Connection failed',
          latency_ms: res.latency_ms,
        });
      }
    } catch (e: unknown) {
      const msg =
        e && typeof e === 'object' && 'response' in e && (e as { response?: { data?: { detail?: string } } }).response?.data?.detail
          ? (e as { response: { data: { detail: string } } }).response.data.detail
          : e instanceof Error
          ? e.message
          : 'Connection test failed';
      setTestResult({ status: 'error', message: msg });
    } finally {
      setTesting(false);
    }
  };

  /**
   * The options block this connection will store.
   *
   * Throws on invalid advanced JSON instead of silently sending an empty block: the
   * backend would otherwise ingest one unpaginated page and the operator would blame
   * the source.
   */
  const buildOptions = (): Record<string, any> => {
    const options: Record<string, any> = {};
    if (sourceType === 'postgres') options.sslmode = 'disable';
    if (sourceType === 'mongodb') options.auth_source = 'admin';
    if (sourceType !== 'api') return options;

    let advanced: Record<string, any> = {};
    if (apiAdvanced.trim()) {
      try {
        const parsed = JSON.parse(apiAdvanced);
        if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
          throw new Error('it must be a JSON object');
        }
        advanced = parsed as Record<string, any>;
      } catch (err) {
        throw new Error(
          `Advanced options are not valid JSON: ${err instanceof Error ? err.message : String(err)}`
        );
      }
    }
    Object.assign(options, advanced);
    if (database.trim()) options.endpoint = database.trim();
    if (dataKey.trim()) options.data_key = dataKey.trim();
    if (paginationType !== 'none') {
      options.pagination = {
        ...((advanced.pagination as Record<string, any>) || {}),
        type: paginationType,
        page_size: Number(pageSize) || 100,
        ...(paginationType === 'cursor' && nextKey.trim()
          ? { next_key: nextKey.trim() }
          : {}),
      };
    }
    return options;
  };

  /** Fills the form from one recipe: paging into the fields, the rest into the JSON. */
  const applyPreset = (id: string) => {
    setPresetId(id);
    if (!id) return;
    const preset = apiPresets.find((item) => item.id === id);
    if (!preset) return;
    const incoming: Record<string, any> = { ...(preset.options as Record<string, any>) };
    const pagination = (incoming.pagination as Record<string, any>) || {};
    delete incoming.pagination;
    if (typeof incoming.data_key === 'string') {
      setDataKey(incoming.data_key);
      delete incoming.data_key;
    }
    if (typeof incoming.endpoint === 'string') {
      setDatabase(incoming.endpoint);
      delete incoming.endpoint;
    }
    setPaginationType(typeof pagination.type === 'string' ? pagination.type : 'none');
    setPageSize(Number(pagination.page_size) || 100);
    setNextKey(typeof pagination.next_key === 'string' ? pagination.next_key : '');
    setApiAdvanced(Object.keys(incoming).length ? JSON.stringify(incoming, null, 2) : '');
  };

  const isEditing = !!initialConnection;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 backdrop-blur-xs p-4 animate-in fade-in duration-150">
      <div className="w-full max-w-lg bg-white rounded-xl shadow-2xl border border-slate-200 overflow-hidden flex flex-col">
        {/* Header */}
        <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between bg-slate-50">
          <div className="flex items-center gap-2.5">
            <div className="p-2 bg-sky-100 text-sky-800 rounded-lg">
              <Server className="w-4 h-4" />
            </div>
            <div>
              <h3 className="text-sm font-bold text-slate-900">
                {isEditing ? `Edit Connection: ${initialConnection.name}` : 'Connect External Data Source'}
              </h3>
              <p className="text-xs text-slate-500">
                Encrypted via AES-128 Vault in data/obelisk.db
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-600 p-1 rounded-md"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Form Body */}
        <form onSubmit={handleSubmit} className="p-5 space-y-4 max-h-[80vh] overflow-y-auto">
          {/* Source Type Selector */}
          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1.5">
              Engine / Source Type
            </label>
            <div className="grid grid-cols-3 sm:grid-cols-5 gap-1.5">
              {[
                { id: 'postgres', label: 'PostgreSQL', icon: '🐘' },
                { id: 'mysql', label: 'MySQL', icon: '🐬' },
                { id: 'oracle', label: 'Oracle', icon: '🏛️' },
                { id: 'mongodb', label: 'MongoDB', icon: '🍃' },
                { id: 'api', label: 'REST API', icon: '🌐' },
              ].map((item) => (
                <button
                  key={item.id}
                  type="button"
                  onClick={() => handleTypeChange(item.id as SupportedSourceType)}
                  className={`py-2 px-1 text-xs font-medium rounded-lg border text-center transition-all ${
                    sourceType === item.id
                      ? 'border-sky-500 bg-sky-50 text-sky-900 font-semibold ring-1 ring-sky-500 shadow-2xs'
                      : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50'
                  }`}
                >
                  <div className={`w-7 h-7 mx-auto rounded-md flex items-center justify-center text-sm ${
                    sourceType === item.id ? 'bg-sky-100/80' : 'bg-slate-100'
                  }`}>
                    {item.icon}
                  </div>
                  <div className="text-[11px] font-medium truncate mt-1.5">{item.label}</div>
                </button>
              ))}
            </div>
          </div>

          {/* Connection Name */}
          <div>
            <div className="flex items-center justify-between mb-1">
              <label className="block text-xs font-semibold text-slate-700">
                Connection Name
              </label>
              {name && (
                <span className="text-[10px] text-slate-400 font-mono">
                  SQL identifier: <strong className="text-sky-700">{name.toLowerCase().replace(/[^a-z0-9_]/g, '_')}</strong>
                </span>
              )}
            </div>
            <input
              type="text"
              placeholder="e.g. corp_warehouse, live_crm, or local_postgres_18"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-2 outline-hidden focus:border-sky-500 font-mono text-slate-800"
            />
            <p className="mt-1 text-[11px] text-slate-500">
              lowercase no spaces because it will be used in query (e.g. <span className="font-mono text-slate-700 font-medium">local_postgres_18</span>)
            </p>
          </div>

          {/* Host & Port */}
          <div className="grid grid-cols-3 gap-2">
            <div className="col-span-2">
              <label className="block text-xs font-semibold text-slate-700 mb-1">
                {sourceType === 'api' ? 'Base URL or Host' : 'Host / IP'}
              </label>
              <input
                type="text"
                value={host}
                placeholder={sourceType === 'api' ? 'https://api.corp.com or localhost' : 'localhost or db.corp'}
                onChange={(e) => setHost(e.target.value)}
                required
                className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-2 outline-hidden focus:border-sky-500 font-mono text-slate-800"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold text-slate-700 mb-1">
                Port
              </label>
              <input
                type="number"
                value={port}
                onChange={(e) => setPort(Number(e.target.value))}
                required
                className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-2 outline-hidden focus:border-sky-500 font-mono text-slate-800"
              />
            </div>
          </div>

          {/* Database / Endpoint */}
          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1">
              {sourceType === 'api' ? 'Endpoint Path' : 'Database / Service Name'}
            </label>
            <input
              type="text"
              placeholder={sourceType === 'api' ? 'v1/orders or leave blank for root' : 'e.g. sourcedb or XE'}
              value={database}
              onChange={(e) => setDatabase(e.target.value)}
              className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-2 outline-hidden focus:border-sky-500 font-mono text-slate-800"
            />
          </div>

          {/* REST ingestion: paging in fields, everything else in the JSON escape hatch */}
          {sourceType === 'api' && (
            <div className="space-y-3 rounded-lg border border-slate-200 bg-slate-50/70 p-3">
              <div>
                <label className="block text-xs font-semibold text-slate-700 mb-1">
                  Connector preset
                </label>
                <select
                  value={presetId}
                  onChange={(e) => applyPreset(e.target.value)}
                  className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-2 outline-hidden focus:border-sky-500 text-slate-800"
                >
                  <option value="">None — configure manually</option>
                  {apiPresets.map((preset) => (
                    <option key={preset.id} value={preset.id}>
                      {preset.name}
                    </option>
                  ))}
                </select>
                <p className="mt-1 text-[11px] text-slate-500">
                  {apiPresets.find((item) => item.id === presetId)?.description ||
                    'A preset only fills these options; nothing is hidden or hardcoded.'}
                </p>
              </div>

              <div className="grid grid-cols-3 gap-2">
                <div>
                  <label className="block text-xs font-semibold text-slate-700 mb-1">
                    Pagination
                  </label>
                  <select
                    value={paginationType}
                    onChange={(e) => setPaginationType(e.target.value)}
                    className="w-full text-xs bg-white border border-slate-300 rounded-lg px-2 py-2 outline-hidden focus:border-sky-500 text-slate-800"
                  >
                    <option value="none">Single request</option>
                    <option value="offset">Offset</option>
                    <option value="page">Page number</option>
                    <option value="cursor">Cursor</option>
                    <option value="link">Link header</option>
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-semibold text-slate-700 mb-1">
                    Page size
                  </label>
                  <input
                    type="number"
                    min={1}
                    value={pageSize}
                    onChange={(e) => setPageSize(Number(e.target.value))}
                    className="w-full text-xs bg-white border border-slate-300 rounded-lg px-2 py-2 outline-hidden focus:border-sky-500 font-mono text-slate-800"
                  />
                </div>
                <div>
                  <label className="block text-xs font-semibold text-slate-700 mb-1">
                    Next cursor key
                  </label>
                  <input
                    type="text"
                    value={nextKey}
                    disabled={paginationType !== 'cursor'}
                    placeholder="next_cursor"
                    onChange={(e) => setNextKey(e.target.value)}
                    className="w-full text-xs bg-white border border-slate-300 rounded-lg px-2 py-2 outline-hidden focus:border-sky-500 font-mono text-slate-800 disabled:bg-slate-100 disabled:text-slate-400"
                  />
                </div>
              </div>

              <div>
                <label className="block text-xs font-semibold text-slate-700 mb-1">
                  Advanced options (JSON)
                </label>
                <textarea
                  value={apiAdvanced}
                  onChange={(e) => setApiAdvanced(e.target.value)}
                  rows={4}
                  spellCheck={false}
                  placeholder={'{"auth": {"type": "bearer"}, "retry": {"max_attempts": 4}, "data_key": "results"}'}
                  className="w-full text-[11px] bg-white border border-slate-300 rounded-lg px-3 py-2 outline-hidden focus:border-sky-500 font-mono text-slate-800"
                />
                <p className="mt-1 text-[11px] text-slate-500">
                  Authentication, retries, budgets and POST bodies live here. See{' '}
                  <span className="font-mono text-slate-700">docs/api-ingestion.md</span>.
                </p>
              </div>
            </div>
          )}

          {/* Username & Password/Token */}
          <div className="grid grid-cols-2 gap-2">
            <div>
              <label className="block text-xs font-semibold text-slate-700 mb-1">
                {sourceType === 'api' ? 'Auth User / Header' : 'Username'}
              </label>
              <input
                type="text"
                placeholder={sourceType === 'api' ? 'Authorization' : 'dbuser'}
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-2 outline-hidden focus:border-sky-500 font-mono text-slate-800"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold text-slate-700 mb-1 flex items-center gap-1">
                <Lock className="w-3 h-3 text-slate-400" />
                {sourceType === 'api' ? 'Bearer Token / API Key' : 'Password'}
              </label>
              <input
                type="password"
                placeholder={isEditing ? '•••••••• (leave blank to keep)' : 'Enter password or token'}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required={!isEditing && sourceType !== 'api'}
                className="w-full text-xs bg-white border border-slate-300 rounded-lg px-3 py-2 outline-hidden focus:border-sky-500 font-mono text-slate-800"
              />
            </div>
          </div>

          {/* REST API Options: Data key extraction */}
          {sourceType === 'api' && (
            <div className="p-3 bg-slate-50 border border-slate-200 rounded-lg space-y-2">
              <label className="block text-xs font-semibold text-slate-700">
                JSON Records Key (Optional)
              </label>
              <input
                type="text"
                placeholder="e.g. items, results, or data (leave blank for top-level array)"
                value={dataKey}
                onChange={(e) => setDataKey(e.target.value)}
                className="w-full text-xs bg-white border border-slate-300 rounded px-2.5 py-1.5 outline-hidden focus:border-sky-500 font-mono text-slate-800"
              />
              <p className="text-[10px] text-slate-400">
                Extracts nested records list from response JSON payload.
              </p>
            </div>
          )}

          {/* Access: the operator's decision about writes, not an inference */}
          <div className="p-3 bg-slate-50 border border-slate-200 rounded-lg space-y-2">
            <div className="flex items-center justify-between gap-3">
              <label className="block text-xs font-semibold text-slate-700">Access</label>
              <div className="inline-flex rounded-lg border border-slate-300 overflow-hidden">
                <button
                  type="button"
                  onClick={() => setReadOnly(true)}
                  className={`px-2.5 py-1 text-[11px] font-semibold transition-colors ${
                    readOnly
                      ? 'bg-slate-800 text-white'
                      : 'bg-white text-slate-600 hover:bg-slate-100'
                  }`}
                >
                  Read-only
                </button>
                <button
                  type="button"
                  onClick={() => setReadOnly(false)}
                  className={`px-2.5 py-1 text-[11px] font-semibold transition-colors border-l border-slate-300 ${
                    !readOnly
                      ? 'bg-sky-600 text-white shadow-2xs'
                      : 'bg-white text-slate-600 hover:bg-slate-100'
                  }`}
                >
                  Read &amp; write
                </button>
              </div>
            </div>
            <p className="text-[10px] text-slate-500 leading-snug">
              {readOnly
                ? 'Obelisk reads this source and never writes into it. dbt models and obelisk.save() will not publish here.'
                : 'Obelisk may create schemas and tables in this database when publishing dbt models or a DataFrame mirror.'}
            </p>
          </div>

          {testResult && (
            <div
              className={`p-3 rounded-lg flex items-start gap-2 text-xs border ${
                testResult.status === 'ok'
                  ? 'bg-emerald-50 border-emerald-200 text-emerald-800'
                  : 'bg-rose-50 border-rose-200 text-rose-800'
              }`}
            >
              {testResult.status === 'ok' ? (
                <CheckCircle2 className="w-4 h-4 text-emerald-600 shrink-0 mt-0.5" />
              ) : (
                <AlertCircle className="w-4 h-4 text-rose-600 shrink-0 mt-0.5" />
              )}
              <div className="font-mono text-[11px] flex-1">
                <span className="font-semibold">{testResult.message}</span>
                {testResult.latency_ms != null && (
                  <span className="ml-2 opacity-75">({testResult.latency_ms} ms)</span>
                )}
              </div>
            </div>
          )}

          {error && (
            <div className="p-3 bg-rose-50 border border-rose-200 rounded-lg flex items-start gap-2 text-rose-700 text-xs">
              <AlertCircle className="w-4 h-4 shrink-0 mt-0.5" />
              <div className="font-mono text-[11px]">{error}</div>
            </div>
          )}

          {/* Footer Actions */}
          <div className="pt-2 flex items-center justify-between border-t border-slate-100">
            <button
              type="button"
              onClick={handleTestConnection}
              disabled={testing || submitting}
              className="flex items-center gap-1.5 px-3 py-1.5 border border-slate-300 hover:border-slate-400 text-slate-700 bg-white hover:bg-slate-50 rounded-lg text-xs font-semibold transition-colors disabled:opacity-50"
            >
              {testing ? (
                <>
                  <RefreshCw className="w-3.5 h-3.5 animate-spin text-sky-600" />
                  <span>Testing...</span>
                </>
              ) : (
                <>
                  <Zap className="w-3.5 h-3.5 text-amber-500" />
                  <span>Test Connection</span>
                </>
              )}
            </button>

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
                disabled={submitting}
                className="flex items-center gap-1.5 px-4 py-2 bg-sky-600 hover:bg-sky-700 text-white rounded-lg text-xs font-semibold shadow-xs transition-colors disabled:opacity-50"
              >
                {submitting ? (
                  <>
                    <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                    <span>Saving to Vault...</span>
                  </>
                ) : (
                  <span>{isEditing ? 'Update Connection' : 'Save to Vault'}</span>
                )}
              </button>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
};
