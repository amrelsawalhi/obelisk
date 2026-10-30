import type { CatalogNode, CatalogTable, CatalogTableColumn } from './types';

/** A table the SQL editor can suggest, including every address it may be queried by. */
export interface SqlCatalogTable {
  names: string[];
  columns: CatalogTableColumn[];
  source: string;
  detail: string;
}

/**
 * Flattens the catalog tree into the names DuckDB accepts in a pipeline SQL step.
 *
 * Lakehouse folders become dotted schemas and external sources use their normalized
 * connection alias. Both the short and canonical forms are included because the SQL
 * rewriter resolves either one.
 */
export function collectSqlCatalog(nodes: CatalogNode[]): SqlCatalogTable[] {
  const tables: SqlCatalogTable[] = [];

  const addTable = (
    names: Array<string | undefined>,
    table: CatalogTable,
    source: string,
    detail: string,
  ) => {
    const cleanNames = Array.from(
      new Set(
        names
          .map((name) => (name ?? '').replace(/\.parquet$/i, '').replace(/^\.+|\.+$/g, ''))
          .filter(Boolean),
      ),
    );
    if (!cleanNames.length) return;
    tables.push({
      names: cleanNames,
      columns: table.columns ?? [],
      source,
      detail,
    });
  };

  for (const node of nodes ?? []) {
    if (node.type === 'lakehouse' || node.id === 'lakehouse') {
      for (const dir of node.directories ?? []) {
        const folder = (dir.path || dir.name || '').replace(/\//g, '.').replace(/^\.+|\.+$/g, '');
        for (const table of dir.tables ?? []) {
          const clean = table.name.replace(/\.parquet$/i, '');
          const qualified = [folder, clean].filter(Boolean).join('.');
          addTable(
            [qualified, `lakehouse.${qualified}`, table.fqn, clean],
            table,
            'Lakehouse',
            `Lakehouse table${folder ? ` in ${dir.path || folder}` : ''}`,
          );
        }
      }
      for (const table of node.tables ?? []) {
        const clean = table.name.replace(/\.parquet$/i, '');
        addTable(
          [clean, table.fqn && `lakehouse.${table.fqn}`],
          table,
          'Lakehouse',
          'Lakehouse table',
        );
      }
      continue;
    }

    const alias = (node.name || node.id).toLowerCase().replace(/[^a-z0-9_]/g, '_');
    for (const table of node.tables ?? []) {
      const schema = table.schema || table.schema_name || 'public';
      const bare = table.name.startsWith(`${schema}.`)
        ? table.name.slice(schema.length + 1)
        : table.name;
      addTable(
        [`${alias}.${schema}.${bare}`, `${schema}.${bare}`, bare, table.fqn],
        table,
        node.name || node.id,
        `External table from ${node.name || node.id}`,
      );
    }
  }

  return tables;
}

/** Addresses that occur more than once are not safe bare-name completions. */
export function uniqueTableNames(tables: SqlCatalogTable[]): string[] {
  const counts = new Map<string, number>();
  tables.forEach((table) => {
    table.names.forEach((name) => counts.set(name, (counts.get(name) ?? 0) + 1));
  });
  const names: string[] = [];
  const seen = new Set<string>();
  tables.forEach((table) => {
    table.names.forEach((name) => {
      if (seen.has(name)) return;
      const bare = !name.includes('.');
      if (bare && (counts.get(name) ?? 0) > 1) return;
      seen.add(name);
      names.push(name);
    });
  });
  return names.sort((left, right) => left.localeCompare(right));
}

export interface SqlTableAlias {
  alias: string;
  table: SqlCatalogTable;
}

const SQL_ALIAS_STOP_WORDS = new Set([
  'where', 'join', 'left', 'right', 'inner', 'outer', 'full', 'cross', 'on', 'using',
  'group', 'order', 'limit', 'union', 'except', 'intersect', 'qualify', 'having',
  'window', 'as', 'select', 'from', 'and', 'or', 'not', 'lateral', 'unnest',
]);

/**
 * Best-effort alias map for completion after ``alias.`` in ordinary FROM/JOIN clauses.
 * It is deliberately not a SQL parser: a missed alias falls back to the full column list,
 * while a guessed one never changes the query text.
 */
export function collectSqlAliases(sql: string, tables: SqlCatalogTable[]): Map<string, SqlCatalogTable> {
  const byName = new Map<string, SqlCatalogTable>();
  tables.forEach((table) => table.names.forEach((name) => byName.set(name.toLowerCase(), table)));

  const aliases = new Map<string, SqlCatalogTable>();
  const pattern =
    /\b(?:from|join)\s+([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)*)(?:\s+(?:as\s+)?([A-Za-z_][\w]*))?/gi;
  for (const match of sql.matchAll(pattern)) {
    const relation = match[1]?.replace(/^["`]|["`]$/g, '') ?? '';
    const table = byName.get(relation.toLowerCase());
    if (!table) continue;
    const explicit = match[2]?.toLowerCase();
    const alias =
      explicit && !SQL_ALIAS_STOP_WORDS.has(explicit)
        ? explicit
        : relation.split('.').pop()?.toLowerCase();
    if (alias) aliases.set(alias, table);
    aliases.set(relation.split('.').pop()?.toLowerCase() ?? relation.toLowerCase(), table);
  }
  return aliases;
}
