import React, { useState } from 'react';
import { X, FileCode, Check } from 'lucide-react';
import { api } from '../../lib/api';

interface NewModelModalProps {
  isOpen: boolean;
  initialFolder?: 'staging' | 'marts';
  onClose: () => void;
  onSuccess: (relPath: string) => void;
}

export const NewModelModal: React.FC<NewModelModalProps> = ({
  isOpen,
  initialFolder = 'staging',
  onClose,
  onSuccess,
}) => {
  const [modelName, setModelName] = useState('');
  const [folder, setFolder] = useState<'staging' | 'marts'>(initialFolder);
  const [materialization, setMaterialization] = useState<'table' | 'view'>('table');
  const [sourceTable, setSourceTable] = useState('orders');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (!isOpen) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!modelName.trim()) {
      setError('Model name is required');
      return;
    }

    const cleanName = modelName.trim().toLowerCase().replace(/\s+/g, '_').replace(/\.sql$/, '');
    const projectFolder = folder === 'staging' ? 'models/staging' : 'models/marts';
    const filePath = `${projectFolder}/${cleanName}.sql`;

    let template = '';
    if (folder === 'staging') {
      template = `{{ config(materialized='${materialization}') }}\n\n` +
        `with source as (\n` +
        `    select * from {{ source('landing', '${sourceTable || 'orders'}') }}\n` +
        `),\n\n` +
        `cleaned as (\n` +
        `    select\n` +
        `        cast(order_id as integer) as order_id,\n` +
        `        cast(customer_id as integer) as customer_id,\n` +
        `        cast(amount as numeric(10, 2)) as amount,\n` +
        `        cast(created_at as timestamp) as order_timestamp\n` +
        `    from source\n` +
        `    where order_id is not null\n` +
        `)\n\n` +
        `select * from cleaned\n`;
    } else {
      template = `{{ config(materialized='${materialization}') }}\n\n` +
        `with staging as (\n` +
        `    select * from {{ ref('stg_orders') }}\n` +
        `)\n\n` +
        `select\n` +
        `    date_trunc('day', order_timestamp) as order_date,\n` +
        `    count(*) as total_orders,\n` +
        `    sum(amount) as total_volume\n` +
        `from staging\n` +
        `group by 1\n` +
        `order by 1 desc\n`;
    }

    try {
      setLoading(true);
      setError(null);
      await api.saveDbtFile(filePath, template);
      onSuccess(filePath);
      onClose();
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to create dbt model');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 backdrop-blur-xs p-4 animate-in fade-in duration-150">
      <div className="bg-white rounded-2xl shadow-2xl border border-slate-200 w-full max-w-md overflow-hidden select-none">
        {/* Header */}
        <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between bg-slate-50/50">
          <div className="flex items-center gap-2">
            <div className="p-1.5 rounded-lg bg-sky-100 text-sky-700">
              <FileCode className="w-4 h-4" />
            </div>
            <div>
              <h3 className="text-sm font-bold text-slate-900">New dbt Model</h3>
              <p className="text-[11px] text-slate-500">Create a new SQL transformation asset</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1 text-slate-400 hover:text-slate-600 rounded-md hover:bg-slate-100"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="p-5 space-y-4">
          {error && (
            <div className="p-3 rounded-lg bg-rose-50 border border-rose-200 text-rose-700 text-xs">
              {error}
            </div>
          )}

          {/* Model Name */}
          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-slate-700">Model Name</label>
            <div className="relative">
              <input
                type="text"
                autoFocus
                required
                placeholder={folder === 'staging' ? 'stg_payments' : 'fct_monthly_sales'}
                value={modelName}
                onChange={(e) => setModelName(e.target.value)}
                className="w-full bg-white border border-slate-200 rounded-lg px-3 py-2 text-xs font-mono text-slate-900 placeholder-slate-400 focus:outline-sky-500 shadow-2xs"
              />
              <span className="absolute right-3 top-2 text-[11px] font-mono text-slate-400">.sql</span>
            </div>
          </div>

          {/* Target folder */}
          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-slate-700">Model Folder</label>
            <div className="grid grid-cols-2 gap-2">
              <button
                type="button"
                onClick={() => setFolder('staging')}
                className={`p-3 rounded-lg border text-left transition-all ${
                  folder === 'staging'
                    ? 'border-sky-500 bg-sky-50/60 ring-2 ring-sky-500/20'
                    : 'border-slate-200 hover:border-slate-300 bg-white'
                }`}
              >
                <div className="font-semibold text-xs text-slate-900 flex items-center justify-between">
                  <span>Staging</span>
                  {folder === 'staging' && <Check className="w-3.5 h-3.5 text-sky-600" />}
                </div>
                <div className="text-[10px] text-slate-500 mt-1">models/staging/</div>
              </button>

              <button
                type="button"
                onClick={() => setFolder('marts')}
                className={`p-3 rounded-lg border text-left transition-all ${
                  folder === 'marts'
                    ? 'border-amber-500 bg-amber-50/60 ring-2 ring-amber-500/20'
                    : 'border-slate-200 hover:border-slate-300 bg-white'
                }`}
              >
                <div className="font-semibold text-xs text-slate-900 flex items-center justify-between">
                  <span>Marts</span>
                  {folder === 'marts' && <Check className="w-3.5 h-3.5 text-amber-600" />}
                </div>
                <div className="text-[10px] text-slate-500 mt-1">models/marts/</div>
              </button>
            </div>
          </div>

          {/* Staging source table option */}
          {folder === 'staging' && (
            <div className="space-y-1.5">
              <label className="text-xs font-semibold text-slate-700">Landing Table</label>
              <input
                type="text"
                placeholder="orders"
                value={sourceTable}
                onChange={(e) => setSourceTable(e.target.value)}
                className="w-full bg-white border border-slate-200 rounded-lg px-3 py-1.5 text-xs font-mono text-slate-900 placeholder-slate-400 focus:outline-sky-500 shadow-2xs"
              />
              <p className="text-[10px] text-slate-400">
                Resolves to {`{{ source('landing', '${sourceTable || 'orders'}') }}`}
              </p>
            </div>
          )}

          {/* Materialization */}
          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-slate-700">Materialization</label>
            <div className="flex items-center gap-4 text-xs text-slate-700">
              <label className="flex items-center gap-1.5 cursor-pointer">
                <input
                  type="radio"
                  name="mat"
                  checked={materialization === 'table'}
                  onChange={() => setMaterialization('table')}
                  className="text-sky-600"
                />
                <span>Table (Physical Warehouse Table)</span>
              </label>
              <label className="flex items-center gap-1.5 cursor-pointer">
                <input
                  type="radio"
                  name="mat"
                  checked={materialization === 'view'}
                  onChange={() => setMaterialization('view')}
                  className="text-sky-600"
                />
                <span>View (Logical View)</span>
              </label>
            </div>
          </div>

          {/* Buttons */}
          <div className="flex items-center justify-end gap-2 pt-3 border-t border-slate-200">
            <button
              type="button"
              onClick={onClose}
              className="px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-semibold text-slate-600 hover:bg-slate-100 transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={loading}
              className="px-4 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold shadow-2xs transition-colors flex items-center gap-1.5 disabled:opacity-50"
            >
              {loading && <div className="w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin" />}
              <span>Create Model</span>
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};
