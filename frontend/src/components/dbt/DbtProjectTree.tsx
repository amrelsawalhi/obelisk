import React, { useState } from 'react';
import {
  Folder,
  FolderOpen,
  FileCode,
  FileText,
  ChevronRight,
  ChevronDown,
  Plus,
  Trash2,
  RefreshCw,
  Search,
  Database,
  Boxes,
} from 'lucide-react';
import type { DbtTreeNode } from '../../lib/types';

interface DbtProjectTreeProps {
  root: DbtTreeNode | null;
  loading: boolean;
  selectedPath: string | null;
  onSelectFile: (node: DbtTreeNode) => void;
  onRefresh: () => void;
  onOpenNewModel: (defaultLayer?: 'staging' | 'marts') => void;
  onOpenNewYaml: () => void;
  onDeleteFile: (path: string) => void;
}

export const DbtProjectTree: React.FC<DbtProjectTreeProps> = ({
  root,
  loading,
  selectedPath,
  onSelectFile,
  onRefresh,
  onOpenNewModel,
  onOpenNewYaml,
  onDeleteFile,
}) => {
  const [collapsedPaths, setCollapsedPaths] = useState<Record<string, boolean>>({});
  const [searchTerm, setSearchTerm] = useState('');

  const toggleCollapse = (path: string) => {
    setCollapsedPaths((prev) => ({ ...prev, [path]: !prev[path] }));
  };

  const getFileIcon = (node: DbtTreeNode) => {
    if (node.extension === '.sql') {
      if (node.layer === 'staging') {
        return <FileCode className="w-3.5 h-3.5 text-sky-600 shrink-0" />;
      }
      if (node.layer === 'marts') {
        return <FileCode className="w-3.5 h-3.5 text-amber-600 shrink-0" />;
      }
      return <FileCode className="w-3.5 h-3.5 text-slate-500 shrink-0" />;
    }
    if (node.extension === '.yml' || node.extension === '.yaml') {
      return <FileText className="w-3.5 h-3.5 text-emerald-600 shrink-0" />;
    }
    return <FileText className="w-3.5 h-3.5 text-slate-400 shrink-0" />;
  };

  const renderNode = (node: DbtTreeNode, depth = 0): React.ReactNode => {
    const isDir = node.type === 'directory';
    const isCollapsed = isDir && !!collapsedPaths[node.path];
    const isSelected = selectedPath === node.path;

    // Filter matching
    if (searchTerm.trim()) {
      const match = node.name.toLowerCase().includes(searchTerm.toLowerCase());
      const hasMatchingChild = (n: DbtTreeNode): boolean => {
        if (n.name.toLowerCase().includes(searchTerm.toLowerCase())) return true;
        return (n.children || []).some(hasMatchingChild);
      };
      if (!match && !hasMatchingChild(node)) {
        return null;
      }
    }

    if (isDir) {
      const isStaging = node.path.endsWith('staging');
      const isMarts = node.path.endsWith('marts');
      const isModels = node.name === 'models';

      return (
        <div key={node.path} className="select-none">
          <div
            onClick={() => toggleCollapse(node.path)}
            style={{ paddingLeft: `${depth * 12 + 6}px` }}
            className={`group flex items-center justify-between py-1 px-2 rounded-md hover:bg-slate-100 cursor-pointer text-xs font-medium text-slate-700 transition-colors ${
              isSelected ? 'bg-sky-50 text-sky-900 font-semibold' : ''
            }`}
          >
            <div className="flex items-center gap-1.5 truncate">
              {isCollapsed ? (
                <ChevronRight className="w-3.5 h-3.5 text-slate-400 shrink-0" />
              ) : (
                <ChevronDown className="w-3.5 h-3.5 text-slate-400 shrink-0" />
              )}
              {isCollapsed ? (
                <Folder className="w-3.5 h-3.5 text-amber-500 shrink-0" />
              ) : (
                <FolderOpen className="w-3.5 h-3.5 text-amber-500 shrink-0" />
              )}
              <span className="truncate">{node.name}</span>
            </div>

            {/* Quick add actions for staging & marts folders */}
            <div className="opacity-0 group-hover:opacity-100 flex items-center gap-1 transition-opacity">
              {(isStaging || isMarts || isModels) && (
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    onOpenNewModel(isStaging ? 'staging' : isMarts ? 'marts' : undefined);
                  }}
                  title="Add model in this directory"
                  className="p-0.5 rounded hover:bg-slate-200 text-slate-500 hover:text-sky-700"
                >
                  <Plus className="w-3 h-3" />
                </button>
              )}
            </div>
          </div>

          {!isCollapsed && node.children && (
            <div className="mt-0.5">
              {node.children.map((child) => renderNode(child, depth + 1))}
            </div>
          )}
        </div>
      );
    }

    // File Node
    return (
      <div
        key={node.path}
        onClick={() => onSelectFile(node)}
        style={{ paddingLeft: `${depth * 12 + 18}px` }}
        className={`group flex items-center justify-between py-1 px-2 rounded-md cursor-pointer text-xs transition-colors ${
          isSelected
            ? 'bg-sky-100/70 text-sky-900 font-semibold shadow-2xs'
            : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
        }`}
      >
        <div className="flex items-center gap-2 truncate">
          {getFileIcon(node)}
          <span className="truncate font-mono text-[11px]">{node.name}</span>
        </div>

        {/* Delete file action */}
        <div className="opacity-0 group-hover:opacity-100 flex items-center transition-opacity">
          <button
            onClick={(e) => {
              e.stopPropagation();
              onDeleteFile(node.path);
            }}
            title="Delete file"
            className="p-1 rounded hover:bg-rose-50 text-slate-400 hover:text-rose-600 transition-colors"
          >
            <Trash2 className="w-3 h-3" />
          </button>
        </div>
      </div>
    );
  };

  return (
    <aside className="w-[260px] border-r border-slate-200 bg-white flex flex-col shrink-0 overflow-hidden select-none">
      {/* Header & Quick Action Buttons */}
      <div className="p-3 border-b border-slate-200 flex flex-col gap-2.5 bg-slate-50/60">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-1.5 font-bold text-xs text-slate-900">
            <Boxes className="w-4 h-4 text-sky-600" />
            <span>dbt Explorer</span>
          </div>
          <button
            onClick={onRefresh}
            title="Refresh Explorer"
            className="p-1 rounded hover:bg-slate-200 text-slate-400 hover:text-slate-600 transition-colors"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin text-sky-600' : ''}`} />
          </button>
        </div>

        {/* Action Buttons: + New Model, + New YAML */}
        <div className="grid grid-cols-2 gap-1.5">
          <button
            onClick={() => onOpenNewModel()}
            className="flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-md bg-sky-600 hover:bg-sky-700 text-white text-[11px] font-semibold shadow-2xs transition-colors"
          >
            <Plus className="w-3 h-3" />
            <span>New Model</span>
          </button>
          <button
            onClick={onOpenNewYaml}
            className="flex items-center justify-center gap-1.5 px-2 py-1.5 rounded-md bg-white hover:bg-slate-100 text-slate-700 border border-slate-200 text-[11px] font-semibold shadow-2xs transition-colors"
          >
            <Plus className="w-3 h-3" />
            <span>New YAML</span>
          </button>
        </div>

        {/* Search input */}
        <div className="relative">
          <Search className="w-3.5 h-3.5 text-slate-400 absolute left-2.5 top-2 pointer-events-none" />
          <input
            type="text"
            placeholder="Search models & files..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full bg-white border border-slate-200 rounded-md pl-8 pr-2.5 py-1 text-xs text-slate-800 placeholder-slate-400 outline-hidden focus:border-sky-500 focus:ring-1 focus:ring-sky-500/20 font-mono"
          />
        </div>
      </div>

      {/* Tree Content */}
      <div className="flex-1 overflow-y-auto p-2 space-y-0.5">
        {loading && !root ? (
          <div className="flex items-center justify-center p-6 text-xs text-slate-400">
            <RefreshCw className="w-4 h-4 animate-spin mr-2 text-sky-600" />
            <span>Loading dbt project...</span>
          </div>
        ) : root && root.children && root.children.length > 0 ? (
          root.children.map((child) => renderNode(child))
        ) : (
          <div className="text-center p-6 text-xs text-slate-400">
            No dbt project files found.
          </div>
        )}
      </div>

      {/* Footer Info */}
      <div className="p-2 border-t border-slate-200 bg-slate-50 text-[10px] text-slate-500 font-mono flex items-center justify-between">
        <span className="flex items-center gap-1">
          <Database className="w-3 h-3 text-slate-400" />
          <span>git_repo/dbt_project</span>
        </span>
        <span className="text-slate-400">Gitea Sync</span>
      </div>
    </aside>
  );
};
