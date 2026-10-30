import type { StepOutputDescriptor } from './pipelineOutputs';
import { outputToken } from './pipelineOutputs';

/** One parent output offered inside the SQL editor. */
export interface SqlEditorToken {
  stepName: string;
  output: string;
  type: 'table' | 'scalar';
  token: string;
}

export interface PipelineSqlSnippet {
  id: string;
  label: string;
  description: string;
  sql: string;
}

export function buildParentTokens(
  parentOutputs: Array<{ stepName: string; outputs: StepOutputDescriptor[] }>,
): SqlEditorToken[] {
  return parentOutputs.flatMap((parent) =>
    parent.outputs.map((output) => ({
      stepName: parent.stepName,
      output: output.name,
      type: output.type,
      token: outputToken(parent.stepName, output.name),
    })),
  );
}

/**
 * Common pipeline SQL patterns, resolved against direct-parent outputs where possible.
 *
 * Monaco placeholders use ``${n:value}`` syntax; when a parent token exists we insert
 * the real token instead so the snippet is immediately runnable.
 */
export function buildPipelineSqlSnippets(tokens: SqlEditorToken[]): PipelineSqlSnippet[] {
  const tableToken = tokens.find((item) => item.type === 'table')?.token;
  const rowsToken = tokens.find((item) => item.output === 'rows')?.token;

  const table = tableToken ?? '${1:lakehouse.raw.table_name}';
  const reportedRows = rowsToken ?? '${1:reported_rows}';
  const column = '${1:column}';
  const key = '${2:key_column}';

  return [
    {
      id: 'preview-parent',
      label: 'Preview parent table',
      description: 'Read the table a parent step produced.',
      sql: `SELECT *\nFROM ${table}\nLIMIT 50;`,
    },
    {
      id: 'row-count-check',
      label: 'Reconcile step row count',
      description: 'Compare the parent rows output with the actual table count.',
      sql: `WITH actual AS (\n    SELECT COUNT(*)::BIGINT AS actual_rows\n    FROM ${table}\n)\nSELECT\n    ${reportedRows}::BIGINT AS reported_rows,\n    actual_rows,\n    actual_rows - ${reportedRows}::BIGINT AS difference,\n    CASE\n        WHEN actual_rows = ${reportedRows}::BIGINT THEN 'PASS'\n        ELSE 'FAIL'\n    END AS status\nFROM actual;`,
    },
    {
      id: 'quality-check',
      label: 'Null and key quality check',
      description: 'Count rows, nulls, and duplicate keys in one pass.',
      sql: `SELECT\n    COUNT(*) AS total_rows,\n    COUNT(*) FILTER (WHERE ${column} IS NULL) AS null_rows,\n    COUNT(*) - COUNT(DISTINCT ${key}) AS duplicate_key_rows\nFROM ${table};`,
    },
    {
      id: 'group-profile',
      label: 'Group and count',
      description: 'Profile the distribution of a column.',
      sql: `SELECT\n    ${column},\n    COUNT(*) AS row_count\nFROM ${table}\nGROUP BY 1\nORDER BY 2 DESC\nLIMIT 50;`,
    },
    {
      id: 'cte-skeleton',
      label: 'CTE skeleton',
      description: 'Start a staged transformation with a named source CTE.',
      sql: `WITH source AS (\n    SELECT *\n    FROM ${table}\n)\nSELECT *\nFROM source\nLIMIT 50;`,
    },
  ];
}
