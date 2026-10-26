import React, { useMemo } from 'react';
import {
  Database,
  FileSpreadsheet,
  Folder,
  FolderOpen,
  Layers,
  RefreshCw,
  Table2,
} from 'lucide-react';
import { tableSizeLabel } from '../../lib/format';
import { buildDirectoryTree, type DirectoryTreeNode } from '../../lib/catalogTree';
import type { CatalogNode, CatalogTable } from '../../lib/types';
import {
  TREE_ICON,
  TreeGroupLabel,
  TreeMessage,
  TreeRow,
  treeIndent,
} from '../common/Tree';

export interface TableSelectionInfo {
  isLakehouse?: boolean;
  folderPath?: string;
  sourceName?: string;
  schemaName?: string;
  fqn?: string;
}

export interface SchemaExplorerPanelProps {
  nodes: CatalogNode[];
  collapsedGroups: Record<string, boolean>;
  expandedTables: Record<string, boolean>;
  onToggleGroup: (key: string) => void;
  onToggleTable: (key: string) => void;
  onRefresh: () => void;
  onSelectTable: (table: CatalogTable, info: TableSelectionInfo) => void;
  onInsertColumn: (column: string) => void;
}

const isLakehouseNode = (node: CatalogNode) =>
  node.type === 'lakehouse' || node.id === 'lakehouse';

/**
 * Left-hand schema browser for Query Studio.
 *
 * It shares the Catalog Tree's row primitives and directory-tree builder, so
 * the lakehouse hierarchy collapses correctly (a collapsed folder unmounts its
 * subfolders) and both panels browse the catalog with identical geometry.
 */
export const SchemaExplorerPanel: React.FC<SchemaExplorerPanelProps> = ({
  nodes,
  collapsedGroups,
  expandedTables,
  onToggleGroup,
  onToggleTable,
  onRefresh,
  onSelectTable,
  onInsertColumn,
}) => {
  const lakehouseNodes = nodes.filter(isLakehouseNode);
  const externalNodes = nodes.filter((node) => !isLakehouseNode(node));

  const lakehouseTrees = useMemo(() => {
    const trees = new Map<string, DirectoryTreeNode[]>();
    nodes.filter(isLakehouseNode).forEach((node) => {
      trees.set(node.id, buildDirectoryTree(node.directories || []));
    });
    return trees;
  }, [nodes]);

  const renderTableRow = (params: {
    rowKey: string;
    depth: number;
    name: string;
    table: CatalogTable;
    info: TableSelectionInfo;
    title: string;
  }) => {
    const { rowKey, depth, name, table, info, title } = params;
    const isExpanded = !!expandedTables[rowKey];
    const hasColumns = (table.columns?.length || 0) > 0;

    return (
      <React.Fragment key={rowKey}>
        <TreeRow
          depth={depth}
          expandable={hasColumns}
          expanded={isExpanded}
          onToggle={() => onToggleTable(rowKey)}
          onClick={() => onSelectTable(table, info)}
          icon={
            <FileSpreadsheet
              className={`h-3.5 w-3.5 ${
                info.isLakehouse ? TREE_ICON.table : TREE_ICON.externalTable
              }`}
            />
          }
          label={name}
          labelClassName="font-mono text-[11px]"
          meta={tableSizeLabel(table)}
          title={title}
        />
        {isExpanded &&
          (table.columns || []).map((column) => (
            <div
              key={column.name}
              onClick={() => onInsertColumn(column.name)}
              style={treeIndent(depth + 1)}
              className="flex h-6 cursor-pointer items-center pr-2 text-[10px] text-slate-500 transition-colors hover:bg-sky-50 hover:text-sky-700"
              title="Click to insert column name"
            >
              <span className="min-w-0 flex-1 truncate font-mono">{column.name}</span>
              <span className="ml-2 shrink-0 font-mono text-[9px] text-slate-400">
                {column.data_type}
              </span>
            </div>
          ))}
      </React.Fragment>
    );
  };
  const renderLakehouseNode = (node: CatalogNode) => {
    const tree = lakehouseTrees.get(node.id) || [];
    const groupKey = `source:${node.id}`;
    const isCollapsed = !!collapsedGroups[groupKey];
    const totalCount =
      tree.reduce((total, dir) => total + dir.tableCount, 0) || (node.tables?.length || 0);

    const renderDirectory = (dir: DirectoryTreeNode, depth: number): React.ReactNode => {
      const dirKey = `lakehouse_dir:${dir.path}`;
      const isDirCollapsed = !!collapsedGroups[dirKey];

      return (
        <React.Fragment key={dirKey}>
          <TreeRow
            depth={depth}
            expandable
            expanded={!isDirCollapsed}
            onClick={() => onToggleGroup(dirKey)}
            icon={
              isDirCollapsed ? (
                <Folder className={`h-3.5 w-3.5 ${TREE_ICON.folder}`} />
              ) : (
                <FolderOpen className={`h-3.5 w-3.5 ${TREE_ICON.folder}`} />
              )
            }
            label={dir.name}
            labelClassName="font-mono text-[11px] font-semibold"
            badge={dir.tableCount}
            title={dir.path}
          />

          {!isDirCollapsed && (
            <>
              {dir.children.map((child) => renderDirectory(child, depth + 1))}
              {dir.tables.map((table) =>
                renderTableRow({
                  rowKey: `lakehouse.${dir.path}.${table.name}`,
                  depth: depth + 1,
                  name: table.name,
                  table,
                  info: { isLakehouse: true, folderPath: dir.path, fqn: table.fqn },
                  title: `Click to query ${table.fqn || `${dir.path}.${table.name}`}`,
                })
              )}
              {dir.tableCount === 0 && <TreeMessage depth={depth}>Empty folder</TreeMessage>}
            </>
          )}
        </React.Fragment>
      );
    };

    return (
      <div key={node.id}>
        <TreeRow
          variant="section"
          expandable
          expanded={!isCollapsed}
          onClick={() => onToggleGroup(groupKey)}
          icon={<Layers className={`h-4 w-4 ${TREE_ICON.lakehouse}`} />}
          label={node.name}
          badge={totalCount}
        />

        {!isCollapsed && tree.length > 0 && (
          <div className="pb-1">{tree.map((dir) => renderDirectory(dir, 0))}</div>
        )}

        {/* Fallback for a lakehouse payload without folder metadata. */}
        {!isCollapsed && tree.length === 0 && (node.tables?.length || 0) > 0 && (
          <div className="pb-1">
            {node.tables.map((table) =>
              renderTableRow({
                rowKey: `${node.id}.${table.name}`,
                depth: 0,
                name: table.name,
                table,
                info: { isLakehouse: true, folderPath: '', fqn: table.fqn },
                title: table.name,
              })
            )}
          </div>
        )}

        {!isCollapsed && tree.length === 0 && (node.tables?.length || 0) === 0 && (
          <TreeMessage depth={0}>No tables in the lakehouse yet.</TreeMessage>
        )}
      </div>
    );
  };


  const renderExternalNode = (node: CatalogNode) => {
    const groupKey = `source:${node.id}`;
    const isCollapsed = !!collapsedGroups[groupKey];

    const schemaGroups = (node.tables || []).reduce<Record<string, CatalogTable[]>>(
      (acc, table) => {
        const schema = table.schema_name || 'public';
        if (!acc[schema]) acc[schema] = [];
        acc[schema].push(table);
        return acc;
      },
      {}
    );
    const schemaNames = Object.keys(schemaGroups).sort();

    return (
      <div key={node.id}>
        <TreeRow
          variant="section"
          expandable
          expanded={!isCollapsed}
          onClick={() => onToggleGroup(groupKey)}
          icon={<Database className={`h-4 w-4 ${TREE_ICON.database}`} />}
          label={node.name}
          badge={node.tables?.length || 0}
        />

        {!isCollapsed && schemaNames.length === 0 && (
          <TreeMessage depth={0}>No tables found</TreeMessage>
        )}

        {!isCollapsed &&
          schemaNames.map((schemaName) => {
            const schemaKey = `db_schema:${node.id}:${schemaName}`;
            const isSchemaCollapsed = !!collapsedGroups[schemaKey];
            const tablesInSchema = schemaGroups[schemaName];

            return (
              <React.Fragment key={schemaKey}>
                <TreeRow
                  depth={0}
                  expandable
                  expanded={!isSchemaCollapsed}
                  onClick={() => onToggleGroup(schemaKey)}
                  icon={<Table2 className={`h-3.5 w-3.5 ${TREE_ICON.schema}`} />}
                  label={schemaName}
                  labelClassName="font-mono text-[11px] font-semibold"
                  badge={tablesInSchema.length}
                />

                {!isSchemaCollapsed &&
                  tablesInSchema.map((table) => {
                    const cleanTableName = table.name.startsWith(`${schemaName}.`)
                      ? table.name.slice(schemaName.length + 1)
                      : table.name.includes('.')
                      ? table.name.split('.').slice(1).join('.')
                      : table.name;

                    return renderTableRow({
                      rowKey: `${node.id}.${schemaName}.${cleanTableName}`,
                      depth: 1,
                      name: cleanTableName,
                      table,
                      info: {
                        isLakehouse: false,
                        sourceName: node.name,
                        schemaName,
                        fqn: table.fqn,
                      },
                      title: `Click to query ${table.fqn || table.name}`,
                    });
                  })}
              </React.Fragment>
            );
          })}
      </div>
    );
  };

  return (
    <>
      <div className="flex items-center justify-between border-b border-slate-200 px-3 py-2">
        <span className="flex items-center gap-1.5 text-xs font-bold text-slate-800">
          <Database className="h-3.5 w-3.5 text-sky-600" />
          Schema Explorer
        </span>
        <button
          type="button"
          onClick={onRefresh}
          className="rounded p-1 text-slate-400 transition-colors hover:bg-slate-100 hover:text-slate-600"
          title="Refresh tables"
        >
          <RefreshCw className="h-3.5 w-3.5" />
        </button>
      </div>

      <div role="tree" className="flex-1 overflow-y-auto py-1.5 custom-scrollbar">
        {nodes.length === 0 && <TreeMessage className="px-3">Loading catalog...</TreeMessage>}

        {lakehouseNodes.map(renderLakehouseNode)}

        {externalNodes.length > 0 && (
          <>
            <TreeGroupLabel meta={`${externalNodes.length} connected`}>
              External Sources
            </TreeGroupLabel>
            {externalNodes.map(renderExternalNode)}
          </>
        )}
      </div>
    </>
  );
};

