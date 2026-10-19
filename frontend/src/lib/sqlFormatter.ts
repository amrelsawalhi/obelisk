/**
 * Obelisk SQL Formatter Utility
 * Preserves string literals and comments, capitalizes major SQL keywords,
 * and organizes clauses into clean, readable indentation.
 */
export function formatSql(sql: string): string {
  if (!sql || !sql.trim()) return sql;

  // Protect string literals and comments during formatting
  const tokens: string[] = [];
  let tokenized = sql.replace(
    /('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"|--[^\r\n]*|\/\*[\s\S]*?\*\/)/g,
    (match) => {
      tokens.push(match);
      return `__SQL_TOKEN_${tokens.length - 1}__`;
    }
  );

  // SQL Keywords to uppercase
  const keywords = [
    'SELECT', 'DISTINCT', 'ALL', 'AS', 'FROM', 'WHERE', 'AND', 'OR', 'NOT',
    'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET',
    'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN',
    'ON', 'USING', 'UNION ALL', 'UNION', 'EXCEPT', 'INTERSECT',
    'WITH', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
    'IN', 'NOT IN', 'BETWEEN', 'LIKE', 'ILIKE', 'IS NULL', 'IS NOT NULL',
    'EXISTS', 'ASC', 'DESC', 'NULLS FIRST', 'NULLS LAST',
    'OVER', 'PARTITION BY', 'WINDOW', 'FILTER', 'QUALIFY',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COALESCE', 'DATE_TRUNC'
  ];

  keywords.forEach((kw) => {
    const pattern = new RegExp(`\\b${kw.replace(/\s+/g, '\\s+')}\\b`, 'gi');
    tokenized = tokenized.replace(pattern, kw);
  });

  // Major clauses that should start on their own line
  const clauses = [
    'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET',
    'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN', 'JOIN',
    'UNION ALL', 'UNION', 'WITH'
  ];

  clauses.forEach((cl) => {
    const regex = new RegExp(`(?<!^)[ \\t]+(${cl.replace(/\s+/g, '\\s+')})\\b`, 'gi');
    tokenized = tokenized.replace(regex, '\n$1');
  });

  // Indent AND / OR within WHERE
  tokenized = tokenized.replace(/[ \t]+(AND|OR)[ \t]+/g, '\n  $1 ');

  // Indent ON for JOIN
  tokenized = tokenized.replace(/[ \t]+ON[ \t]+/g, '\n  ON ');

  // Clean trailing semicolons
  tokenized = tokenized.trim();
  if (tokenized.endsWith(';')) {
    tokenized = tokenized.slice(0, -1).trimEnd() + ';';
  }

  // Restore protected tokens
  tokens.forEach((tok, idx) => {
    tokenized = tokenized.replace(`__SQL_TOKEN_${idx}__`, tok);
  });

  // Clean excessive blank lines
  tokenized = tokenized.replace(/\n{3,}/g, '\n\n');

  return tokenized;
}
