import React, { useState } from 'react';
import {
  Folder,
  FolderOpen,
  FileText,
  FileCode,
  FileJson,
  Database,
  Sliders,
  ChevronRight,
  ChevronDown,
  Trash2,
  Edit2,
  RefreshCw,
  FolderPlus,
  FilePlus,
  GitBranch,
} from 'lucide-react';

import type { FileNode } from '../../lib/types';

interface FileTreeProps {
  nodes: FileNode[];
  loading?: boolean;
  selectedPath: string | null;
  onSelectFile: (node: FileNode) => void;
  onRefresh: () => void;
  onCreate: (parentPath: string, type: 'file' | 'directory') => void;
  onRename: (node: FileNode) => void;
  onDelete: (node: FileNode) => void;
}


const getFileIcon = (node: FileNode) => {
  const ext = node.extension?.toLowerCase();
  switch (ext) {
    case '.py':
      return <FileCode className="w-4 h-4 text-sky-600 shrink-0" />;
    case '.sql':
      return <Database className="w-4 h-4 text-emerald-600 shrink-0" />;
    case '.yml':
    case '.yaml':
      return <Sliders className="w-4 h-4 text-amber-600 shrink-0" />;
    case '.json':
      return <FileJson className="w-4 h-4 text-purple-600 shrink-0" />;
    case '.md':
      return <FileText className="w-4 h-4 text-slate-600 shrink-0" />;
    default:
      return <FileText className="w-4 h-4 text-slate-400 shrink-0" />;
  }
};

interface TreeNodeItemProps {
  node: FileNode;
  level: number;
  selectedPath: string | null;
  expandedPaths: Set<string>;
  onToggleExpand: (path: string) => void;
  onSelectFile: (node: FileNode) => void;
  onCreate: (parentPath: string, type: 'file' | 'directory') => void;
  onRename: (node: FileNode) => void;
  onDelete: (node: FileNode) => void;
}

const TreeNodeItem: React.FC<TreeNodeItemProps> = ({
  node,
  level,
  selectedPath,
  expandedPaths,
  onToggleExpand,
  onSelectFile,
  onCreate,
  onRename,
  onDelete,
}) => {
  const isDir = node.type === 'directory';
  const isExpanded = expandedPaths.has(node.path);
  const isSelected = selectedPath === node.path;

  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (isDir) {
      onToggleExpand(node.path);
    } else {
      onSelectFile(node);
    }
  };

  return (
    <div>
      <div
        onClick={handleClick}
        style={{ paddingLeft: `${6 + level * 14}px` }}
        className={`group flex h-7 items-center justify-between pr-2 cursor-pointer text-xs font-medium rounded-md transition-colors select-none ${
          isSelected
            ? 'bg-sky-50 text-sky-950 font-semibold'
            : 'text-slate-700 hover:bg-slate-100/80 hover:text-slate-900'
        }`}
      >
        <div className="flex items-center gap-1.5 min-w-0 flex-1">
          {isDir ? (
            <span
              onClick={(e) => {
                e.stopPropagation();
                onToggleExpand(node.path);
              }}
              className="p-0.5 text-slate-400 hover:text-slate-600 rounded"
            >
              {isExpanded ? (
                <ChevronDown className="w-3.5 h-3.5" />
              ) : (
                <ChevronRight className="w-3.5 h-3.5" />
              )}
            </span>
          ) : (
            <span className="w-3.5" />
          )}

          {isDir ? (
            isExpanded ? (
              <FolderOpen className="w-4 h-4 text-sky-600 shrink-0" />
            ) : (
              <Folder className="w-4 h-4 text-sky-500/80 shrink-0" />
            )
          ) : (
            getFileIcon(node)
          )}

          <span className="truncate">{node.name}</span>
        </div>

        {/* Hover Action Buttons */}
        <div className="opacity-0 group-hover:opacity-100 flex items-center gap-1 ml-2 shrink-0">
          {isDir && (
            <>
              <button
                title="New File inside folder"
                onClick={(e) => {
                  e.stopPropagation();
                  onCreate(node.path, 'file');
                }}
                className="p-1 hover:bg-white text-slate-400 hover:text-sky-600 rounded"
              >
                <FilePlus className="w-3 h-3" />
              </button>
              <button
                title="New Subfolder"
                onClick={(e) => {
                  e.stopPropagation();
                  onCreate(node.path, 'directory');
                }}
                className="p-1 hover:bg-white text-slate-400 hover:text-sky-600 rounded"
              >
                <FolderPlus className="w-3 h-3" />
              </button>
            </>
          )}
          <button
            title="Rename"
            onClick={(e) => {
              e.stopPropagation();
              onRename(node);
            }}
            className="p-1 hover:bg-white text-slate-400 hover:text-amber-600 rounded"
          >
            <Edit2 className="w-3 h-3" />
          </button>
          <button
            title="Delete"
            onClick={(e) => {
              e.stopPropagation();
              onDelete(node);
            }}
            className="p-1 hover:bg-white text-slate-400 hover:text-rose-600 rounded"
          >
            <Trash2 className="w-3 h-3" />
          </button>
        </div>
      </div>

      {/* Render Subdirectory Children */}
      {isDir && isExpanded && node.children && node.children.length > 0 && (
        <div className="flex flex-col">
          {node.children.map((child) => (
            <TreeNodeItem
              key={child.path}
              node={child}
              level={level + 1}
              selectedPath={selectedPath}
              expandedPaths={expandedPaths}
              onToggleExpand={onToggleExpand}
              onSelectFile={onSelectFile}
              onCreate={onCreate}
              onRename={onRename}
              onDelete={onDelete}
            />
          ))}
        </div>
      )}
    </div>
  );
};

export const FileTree: React.FC<FileTreeProps> = ({
  nodes,
  loading = false,
  selectedPath,
  onSelectFile,
  onRefresh,
  onCreate,
  onRename,
  onDelete,
}) => {

  // Initially expand top-level folders
  const [expandedPaths, setExpandedPaths] = useState<Set<string>>(() => {
    const initial = new Set<string>();
    nodes.forEach((n) => {
      if (n.type === 'directory') initial.add(n.path);
    });
    return initial;
  });

  const toggleExpand = (path: string) => {
    setExpandedPaths((prev) => {
      const next = new Set(prev);
      if (next.has(path)) {
        next.delete(path);
      } else {
        next.add(path);
      }
      return next;
    });
  };

  return (
    <div className="flex flex-col h-full bg-slate-50/50 select-none">
      {/* Explorer Top Toolbar */}
      <div className="px-3 py-2.5 border-b border-slate-200 flex items-center justify-between bg-white shrink-0">
        <div className="flex items-center gap-1.5 font-semibold text-xs text-slate-800">
          <GitBranch className="w-3.5 h-3.5 text-sky-600" />
          <span>git_repo</span>
          <span className="text-[10px] font-mono text-slate-400 bg-slate-100 px-1.5 py-0.5 rounded">
            workspace
          </span>
        </div>

        <div className="flex items-center gap-1">
          <button
            onClick={() => onCreate('', 'file')}
            title="New File at Root"
            className="p-1 text-slate-500 hover:text-sky-600 hover:bg-slate-100 rounded transition-colors"
          >
            <FilePlus className="w-3.5 h-3.5" />
          </button>
          <button
            onClick={() => onCreate('', 'directory')}
            title="New Folder at Root"
            className="p-1 text-slate-500 hover:text-sky-600 hover:bg-slate-100 rounded transition-colors"
          >
            <FolderPlus className="w-3.5 h-3.5" />
          </button>
          <button
            onClick={onRefresh}
            title="Refresh Files"
            className="p-1 text-slate-500 hover:text-slate-800 hover:bg-slate-100 rounded transition-colors"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin text-sky-600' : ''}`} />
          </button>

        </div>
      </div>

      {/* Tree Content */}
      <div className="flex-1 overflow-y-auto p-1.5 space-y-0.5">
        {nodes.length === 0 ? (
          <div className="text-center py-8 text-xs text-slate-400">
            No files found in git_repo.
          </div>
        ) : (
          nodes.map((node) => (
            <TreeNodeItem
              key={node.path}
              node={node}
              level={0}
              selectedPath={selectedPath}
              expandedPaths={expandedPaths}
              onToggleExpand={toggleExpand}
              onSelectFile={onSelectFile}
              onCreate={onCreate}
              onRename={onRename}
              onDelete={onDelete}
            />
          ))
        )}
      </div>
    </div>
  );
};
