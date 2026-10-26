import React, { useMemo } from 'react';
import {
  BookOpen,
  Check,
  CornerDownRight,
  Edit2,
  FileSpreadsheet,
  Folder,
  FolderInput,
  FolderOpen,
  FolderPlus,
  RefreshCw,
  Terminal,
  Trash2,
  X,
} from 'lucide-react';
import { tableSizeLabel } from '../../lib/format';
import {
  buildDirectoryTree,
  filterDirectoryTree,
  type DirectoryTreeNode,
} from '../../lib/catalogTree';
import type { CatalogTable, LakehouseDirectory } from '../../lib/types';
import {
  TREE_ICON,
  TreeMessage,
  TreeRow,
  treeActionClass,
  treeIndent,
} from '../common/Tree';

interface DirEditTarget {
  path: string;
  currentName: string;
  newName: string;
}

export interface LakehouseTreeProps {
  directories: LakehouseDirectory[];
  expandedNodes: Record<string, boolean>;
  onToggleNode: (id: string, isCurrentlyExpanded?: boolean) => void;
  searchTerm: string;
  selectedTableKey: string | null;
  onSelectTable: (
    sourceId: string,
    table: CatalogTable,
    isLakehouse: boolean,
    sourceName?: string
  ) => void;
  onOpenInQuery?: (sql: string) => void;
  onOpenInNotebook?: (table: CatalogTable, fqn?: string) => void;
  renamingDir: DirEditTarget | null;
  onRenamingDirChange: (next: DirEditTarget | null) => void;
  renameDirLoading: boolean;
  onRenameDirectory: (event: React.FormEvent) => void;
  creatingSubdirParent: string | null;
  onCreatingSubdirParentChange: (parentPath: string | null) => void;
  subfolderName: string;
  onSubfolderNameChange: (name: string) => void;
  subfolderLoading: boolean;
  onCreateSubdirectory: (event: React.FormEvent, parentPath: string) => void;
  onDeleteFolder: (folder: { path: string; name: string; tableCount: number }) => void;
  onRenameTable: (folder: string, tableName: string) => void;
  onMoveTable: (folder: string, tableName: string) => void;
  onDeleteTable: (folder: string, tableName: string) => void;
}

/**
 * Renders the lakehouse folder hierarchy for the Catalog Tree.
 *
 * Folders are rebuilt into a real tree, so collapsing a folder unmounts its
 * subfolders and every table below them instead of leaving the subfolders
 * behind as siblings.
 */
export const LakehouseTree: React.FC<LakehouseTreeProps> = (props) => {
  const {
    directories,
    expandedNodes,
    onToggleNode,
    searchTerm,
    selectedTableKey,
    onSelectTable,
    onOpenInQuery,
    onOpenInNotebook,
    renamingDir,
    onRenamingDirChange,
    renameDirLoading,
    onRenameDirectory,
    creatingSubdirParent,
    onCreatingSubdirParentChange,
    subfolderName,
    onSubfolderNameChange,
    subfolderLoading,
    onCreateSubdirectory,
    onDeleteFolder,
    onRenameTable,
    onMoveTable,
    onDeleteTable,
  } = props;

  const isSearching = searchTerm.trim().length > 0;
  const tree = useMemo(() => buildDirectoryTree(directories), [directories]);
  const visibleTree = useMemo(() => filterDirectoryTree(tree, searchTerm), [tree, searchTerm]);

  if (tree.length === 0) {
    return <TreeMessage className="px-2">No folders in the lakehouse yet.</TreeMessage>;
  }

  if (visibleTree.length === 0) {
    return (
      <TreeMessage className="px-2">
        No tables matching "{searchTerm.trim()}".
      </TreeMessage>
    );
  }

  const renderTable = (folder: DirectoryTreeNode, table: CatalogTable, depth: number) => {
    const tableKey = `${folder.id}:${table.name}`;
    const cleanName = table.name.replace(/\.parquet$/i, '');
    const lakehouseFqn = table.fqn || `lakehouse.${folder.path.replace(/\//g, '.')}.${cleanName}`;

    return (
      <TreeRow
        key={tableKey}
        depth={depth}
        selected={selectedTableKey === tableKey}
        onClick={() => onSelectTable(folder.id, table, true, folder.name)}
        icon={<FileSpreadsheet className={`h-3.5 w-3.5 ${TREE_ICON.table}`} />}
        label={table.name}
        labelClassName="font-mono text-[11px]"
        meta={tableSizeLabel(table)}
        title={table.name}
        actions={
          <>
            {onOpenInQuery && (
              <button
                type="button"
                onClick={(event) => {
                  event.stopPropagation();
                  onOpenInQuery(`-- Query Lakehouse Table\nSELECT * FROM ${lakehouseFqn} LIMIT 50;\n`);
                }}
                title="Query in Query Studio"
                className={`${treeActionClass} hover:bg-sky-50 hover:text-sky-600`}
              >
                <Terminal className="h-3 w-3" />
              </button>
            )}
            {onOpenInNotebook && (
              <button
                type="button"
                onClick={(event) => {
                  event.stopPropagation();
                  onOpenInNotebook(table, lakehouseFqn);
                }}
                title="Analyze in Notebook"
                className={`${treeActionClass} hover:bg-amber-50 hover:text-amber-600`}
              >
                <BookOpen className="h-3 w-3" />
              </button>
            )}
            <button
              type="button"
              onClick={(event) => {
                event.stopPropagation();
                onRenameTable(folder.path, table.name);
              }}
              title={`Rename table '${table.name}'`}
              className={treeActionClass}
            >
              <Edit2 className="h-3 w-3" />
            </button>
            <button
              type="button"
              onClick={(event) => {
                event.stopPropagation();
                onMoveTable(folder.path, table.name);
              }}
              title={`Move table '${table.name}' to another folder`}
              className={`${treeActionClass} hover:bg-sky-50 hover:text-sky-700`}
            >
              <FolderInput className="h-3 w-3" />
            </button>
            <button
              type="button"
              onClick={(event) => {
                event.stopPropagation();
                onDeleteTable(folder.path, table.name);
              }}
              title={`Delete table '${table.name}'`}
              className={`${treeActionClass} hover:bg-rose-50 hover:text-rose-600`}
            >
              <Trash2 className="h-3 w-3" />
            </button>
          </>
        }
      />
    );
  };

  const renderFolder = (node: DirectoryTreeNode, depth: number): React.ReactNode => {
    const isExpanded = isSearching
      ? true
      : node.id in expandedNodes
      ? !!expandedNodes[node.id]
      : node.tableCount > 0;
    const isRenaming = renamingDir !== null && renamingDir.path === node.path;
    const isAddingSubfolder = creatingSubdirParent === node.path;
    const isEmpty = node.tables.length === 0 && node.children.length === 0;
    const toggle = () => onToggleNode(node.id, isExpanded);

    return (
      <React.Fragment key={node.id}>
        <TreeRow
          depth={depth}
          expandable
          expanded={isExpanded}
          onToggle={toggle}
          onClick={isRenaming ? undefined : toggle}
          icon={
            isExpanded ? (
              <FolderOpen className={`h-3.5 w-3.5 ${TREE_ICON.folder}`} />
            ) : (
              <Folder className={`h-3.5 w-3.5 ${TREE_ICON.folder}`} />
            )
          }
          label={
            isRenaming && renamingDir ? (
              <form
                onSubmit={onRenameDirectory}
                onClick={(event) => event.stopPropagation()}
                className="flex min-w-0 items-center gap-1"
              >
                <input
                  autoFocus
                  value={renamingDir.newName}
                  onChange={(event) =>
                    onRenamingDirChange({ ...renamingDir, newName: event.target.value })
                  }
                  aria-label="Folder name"
                  className="min-w-0 flex-1 rounded border border-sky-400 bg-white px-1.5 py-0.5 font-mono text-[11px] text-slate-900 outline-hidden focus:ring-1 focus:ring-sky-400"
                />
                <button
                  type="submit"
                  disabled={renameDirLoading}
                  title="Save folder name"
                  className="rounded p-0.5 text-emerald-600 hover:bg-emerald-50 disabled:opacity-50"
                >
                  {renameDirLoading ? (
                    <RefreshCw className="h-3 w-3 animate-spin" />
                  ) : (
                    <Check className="h-3 w-3" />
                  )}
                </button>
                <button
                  type="button"
                  onClick={() => onRenamingDirChange(null)}
                  title="Cancel rename"
                  className="rounded p-0.5 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
                >
                  <X className="h-3 w-3" />
                </button>
              </form>
            ) : (
              <span className="font-mono text-[11px] font-semibold">{node.name}</span>
            )
          }
          badge={node.tableCount}
          title={node.path}
          actions={
            !isRenaming && (
              <>
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    onSubfolderNameChange('');
                    onCreatingSubdirParentChange(isAddingSubfolder ? null : node.path);
                  }}
                  title={`Add subfolder inside '${node.path}'`}
                  className={`${treeActionClass} hover:bg-sky-50 hover:text-sky-700`}
                >
                  <FolderPlus className="h-3 w-3" />
                </button>
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    onRenamingDirChange({
                      path: node.path,
                      currentName: node.name,
                      newName: node.name,
                    });
                  }}
                  title={`Rename folder '${node.name}'`}
                  className={treeActionClass}
                >
                  <Edit2 className="h-3 w-3" />
                </button>
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    onDeleteFolder({
                      path: node.path,
                      name: node.name,
                      tableCount: node.tableCount,
                    });
                  }}
                  title={`Delete folder '${node.path}'`}
                  className={`${treeActionClass} hover:bg-rose-50 hover:text-rose-600`}
                >
                  <Trash2 className="h-3 w-3" />
                </button>
              </>
            )
          }
        />

        {isAddingSubfolder && (
          <form
            onSubmit={(event) => onCreateSubdirectory(event, node.path)}
            style={treeIndent(depth + 1)}
            className="flex h-8 items-center gap-1.5 pr-2"
          >
            <CornerDownRight className="h-3.5 w-3.5 shrink-0 text-sky-500" />
            <input
              autoFocus
              placeholder={`New folder in ${node.name}`}
              value={subfolderName}
              onChange={(event) => onSubfolderNameChange(event.target.value)}
              aria-label="Subfolder name"
              className="min-w-0 flex-1 rounded border border-sky-200 bg-white px-2 py-1 font-mono text-[11px] text-slate-800 outline-hidden focus:border-sky-500 focus:ring-1 focus:ring-sky-500"
            />
            <button
              type="submit"
              disabled={subfolderLoading || !subfolderName.trim()}
              title="Create subfolder"
              className="rounded bg-sky-600 p-1 text-white transition-colors hover:bg-sky-700 shadow-2xs disabled:opacity-50"
            >
              {subfolderLoading ? (
                <RefreshCw className="h-3 w-3 animate-spin" />
              ) : (
                <Check className="h-3 w-3" />
              )}
            </button>
            <button
              type="button"
              onClick={() => onCreatingSubdirParentChange(null)}
              title="Cancel"
              className="rounded p-1 text-slate-400 transition-colors hover:bg-slate-100 hover:text-slate-600"
            >
              <X className="h-3 w-3" />
            </button>
          </form>
        )}

        {isExpanded && (
          <>
            {node.children.map((child) => renderFolder(child, depth + 1))}
            {node.tables.map((table) => renderTable(node, table, depth + 1))}
            {isEmpty && (
              <TreeMessage depth={depth}>
                {isSearching ? 'No matching tables' : 'Empty folder'}
              </TreeMessage>
            )}
          </>
        )}
      </React.Fragment>
    );
  };

  return <div role="group">{visibleTree.map((node) => renderFolder(node, 0))}</div>;
};
