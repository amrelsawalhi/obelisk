import React, { useState } from 'react';
import { X, FileText } from 'lucide-react';
import { api } from '../../lib/api';

interface NewYamlModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSuccess: (relPath: string) => void;
}

export const NewYamlModal: React.FC<NewYamlModalProps> = ({
  isOpen,
  onClose,
  onSuccess,
}) => {
  const [filePath, setFilePath] = useState('models/staging/schema.yml');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (!isOpen) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!filePath.trim()) {
      setError('File path is required');
      return;
    }

    const cleanPath = filePath.trim().replace(/\s+/g, '_');
    const finalPath = cleanPath.endsWith('.yml') || cleanPath.endsWith('.yaml') ? cleanPath : `${cleanPath}.yml`;

    const template = `version: 2\n\n` +
      `models:\n` +
      `  - name: new_model\n` +
      `    description: "Model documentation and test assertions"\n` +
      `    columns:\n` +
      `      - name: id\n` +
      `        description: "Primary key"\n` +
      `        tests:\n` +
      `          - unique\n` +
      `          - not_null\n`;

    try {
      setLoading(true);
      setError(null);
      await api.saveDbtFile(finalPath, template);
      onSuccess(finalPath);
      onClose();
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to create YAML file');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 backdrop-blur-xs p-4 animate-in fade-in duration-150">
      <div className="bg-white rounded-xl shadow-2xl border border-slate-200 w-full max-w-md overflow-hidden select-none">
        {/* Header */}
        <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between bg-slate-50/50">
          <div className="flex items-center gap-2">
            <div className="p-1.5 rounded-lg bg-emerald-100 text-emerald-700">
              <FileText className="w-4 h-4" />
            </div>
            <div>
              <h3 className="text-sm font-bold text-slate-900">New Schema YAML</h3>
              <p className="text-[11px] text-slate-500">Add schema documentation and tests</p>
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

          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-slate-700">File Path</label>
            <input
              type="text"
              autoFocus
              required
              placeholder="models/staging/schema.yml"
              value={filePath}
              onChange={(e) => setFilePath(e.target.value)}
              className="w-full bg-white border border-slate-200 rounded-lg px-3 py-2 text-xs font-mono text-slate-900 placeholder-slate-400 focus:outline-sky-500 shadow-2xs"
            />
          </div>

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
              className="px-4 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-700 text-white text-xs font-semibold shadow-2xs transition-colors flex items-center gap-1.5 disabled:opacity-50"
            >
              {loading && <div className="w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin" />}
              <span>Create YAML</span>
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};
