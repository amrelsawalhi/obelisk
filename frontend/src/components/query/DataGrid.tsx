import React, { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import { Copy, Check, Loader2 } from 'lucide-react';

interface DataGridProps {
  columns: string[];
  rows: Record<string, unknown>[];
  columnTypes?: Record<string, string>;
  loading?: boolean;
}

function inferColumnType(col: string, rows: Record<string, unknown>[], explicitType?: string): string {
  if (explicitType) {
    const cleaned = explicitType.toUpperCase().replace(/\(.*?\)/, '').trim();
    return cleaned.length > 12 ? cleaned.slice(0, 10) : cleaned;
  }
  for (let i = 0; i < Math.min(rows.length, 15); i++) {
    const val = rows[i]?.[col];
    if (val === null || val === undefined) continue;
    if (typeof val === 'number') {
      return Number.isInteger(val) ? 'INT' : 'DECIMAL';
    }
    if (typeof val === 'boolean') return 'BOOL';
    if (typeof val === 'string') {
      if (/^\d{4}-\d{2}-\d{2}(T|\s)\d{2}:\d{2}:\d{2}/.test(val)) return 'TIMESTAMP';
      if (/^\d{4}-\d{2}-\d{2}$/.test(val)) return 'DATE';
      if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(val)) return 'UUID';
      return 'VARCHAR';
    }
    if (typeof val === 'object') return 'JSON';
  }
  return 'VARCHAR';
}

function getTypeBadgeStyle(type: string): string {
  const t = type.toUpperCase();
  if (['INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE'].some((k) => t.includes(k))) {
    return 'bg-blue-50 text-blue-700 border-blue-200/60';
  }
  if (['VARCHAR', 'TEXT', 'STRING', 'CHAR'].some((k) => t.includes(k))) {
    return 'bg-slate-100 text-slate-700 border-slate-200/80';
  }
  if (['TIMESTAMP', 'DATE', 'TIME'].some((k) => t.includes(k))) {
    return 'bg-purple-50 text-purple-700 border-purple-200/60';
  }
  if (['BOOL', 'BOOLEAN'].some((k) => t.includes(k))) {
    return 'bg-amber-50 text-amber-700 border-amber-200/60';
  }
  if (['JSON', 'STRUCT', 'LIST', 'ARRAY', 'MAP'].some((k) => t.includes(k))) {
    return 'bg-emerald-50 text-emerald-700 border-emerald-200/60';
  }
  return 'bg-slate-100 text-slate-600 border-slate-200';
}

const ROW_HEIGHT = 33; // Fixed row height in px
const BUFFER_ROWS = 25; // Pre-render buffer above and below viewport

// Memoised: the dbt studio and query view re-render as the user types, and the
// grid only needs to repaint when the result set changes.
export const DataGrid = React.memo(function DataGrid({
  columns,
  rows,
  columnTypes,
  loading,
}: DataGridProps) {
  const [copiedCell, setCopiedCell] = useState<string | null>(null);
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});

  // Virtual scrolling state
  const containerRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportHeight, setViewportHeight] = useState(600);

  // ResizeObserver to track container viewport height
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const updateHeight = () => {
      if (el) setViewportHeight(el.clientHeight || 600);
    };
    updateHeight();
    const observer = new ResizeObserver(updateHeight);
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  // Optimized throttled scroll listener using requestAnimationFrame
  const scrollRafRef = useRef<number | null>(null);
  const handleScroll = useCallback((e: React.UIEvent<HTMLDivElement>) => {
    const target = e.currentTarget;
    if (scrollRafRef.current !== null) return;
    scrollRafRef.current = requestAnimationFrame(() => {
      setScrollTop(target.scrollTop);
      scrollRafRef.current = null;
    });
  }, []);

  // When rows change (new query run), reset scroll position to top
  useEffect(() => {
    if (containerRef.current) {
      containerRef.current.scrollTop = 0;
      setScrollTop(0);
    }
  }, [rows]);

  const copyToClipboard = (val: string, key: string) => {
    navigator.clipboard.writeText(val);
    setCopiedCell(key);
    setTimeout(() => setCopiedCell(null), 1500);
  };

  // Compute or retrieve width for a given column
  const getColWidth = (col: string): number => {
    if (columnWidths[col]) return columnWidths[col];
    // Default width based on column name length and type badge (min: 130px, max: 340px)
    return Math.max(130, Math.min(340, col.length * 9 + 60));
  };

  // Draggable Column Width Resizer
  const handleColResizeStart = (col: string, e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    const startX = e.clientX;
    const startWidth = getColWidth(col);

    const onMouseMove = (moveEvent: MouseEvent) => {
      const delta = moveEvent.clientX - startX;
      const newWidth = Math.max(65, Math.min(1200, startWidth + delta));
      setColumnWidths((prev) => ({ ...prev, [col]: newWidth }));
    };

    const onMouseUp = () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };

    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
  };

  // Double-Click to Auto-Fit Column Width to Content
  const handleColAutoFit = (col: string, e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();

    // Measure maximum length of column header and sampled row values
    let maxChars = col.length;
    const sampleRows = rows.slice(0, 100);
    for (const row of sampleRows) {
      const val = row[col];
      const str =
        val === null || val === undefined
          ? 'null'
          : typeof val === 'object'
          ? JSON.stringify(val)
          : String(val);
      if (str.length > maxChars) {
        maxChars = str.length;
      }
    }

    // Character width estimate: ~8.2px + cell padding (24px) + copy button icon (20px)
    const idealWidth = Math.max(85, Math.min(900, Math.round(maxChars * 8.2 + 44)));
    setColumnWidths((prev) => ({ ...prev, [col]: idealWidth }));
  };

  if (loading) {
    return (
      <div className="h-64 flex flex-col items-center justify-center text-xs text-slate-400 font-mono gap-2">
        <Loader2 className="w-5 h-5 animate-spin text-sky-600" />
        <span>Executing query...</span>
      </div>
    );
  }

  if (columns.length === 0) {
    return (
      <div className="h-64 flex items-center justify-center text-xs text-slate-400 font-mono">
        Write a SQL query above and click "Run Query" (⌘↵).
      </div>
    );
  }

  // Virtual index calculations
  const totalRows = rows.length;
  const isVirtual = totalRows > 60;
  
  const startIndex = isVirtual 
    ? Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - BUFFER_ROWS)
    : 0;
  const endIndex = isVirtual
    ? Math.min(totalRows, Math.ceil((scrollTop + viewportHeight) / ROW_HEIGHT) + BUFFER_ROWS)
    : totalRows;

  const topSpacerHeight = isVirtual ? startIndex * ROW_HEIGHT : 0;
  const bottomSpacerHeight = isVirtual ? Math.max(0, (totalRows - endIndex) * ROW_HEIGHT) : 0;
  const visibleRows = isVirtual ? rows.slice(startIndex, endIndex) : rows;

  return (
    <div className="w-full h-full flex flex-col bg-white overflow-hidden border border-slate-200 rounded-lg shadow-2xs">
      {/* Scrollable Container with Virtual Windowing */}
      <div ref={containerRef} onScroll={handleScroll} className="flex-1 overflow-auto">
        <table className="w-max min-w-full text-left text-xs border-collapse">
          {/* Header Row */}
          <thead className="sticky top-0 z-10 bg-slate-100 shadow-2xs select-none">
            <tr className="border-b border-slate-200 text-slate-700 font-mono text-[11px]">
              {/* Row Index Column Header */}
              <th className="py-2.5 px-3 w-12 min-w-[48px] max-w-[48px] text-slate-400 text-center font-normal border-r border-slate-200 select-none bg-slate-100">
                #
              </th>

              {/* Data Column Headers */}
              {columns.map((col) => {
                const width = getColWidth(col);
                const colType = inferColumnType(col, rows, columnTypes?.[col]);
                const badgeStyle = getTypeBadgeStyle(colType);

                return (
                  <th
                    key={col}
                    style={{ width: `${width}px`, minWidth: `${width}px`, maxWidth: `${width}px` }}
                    className="relative py-2 px-3 font-semibold text-slate-800 whitespace-nowrap border-r border-slate-200 last:border-r-0 select-none group bg-slate-100"
                  >
                    <div className="flex items-center justify-between gap-1.5 pr-2">
                      <span className="truncate block font-semibold text-slate-800" title={`${col} (double-click divider to auto-fit)`}>
                        {col}
                      </span>
                      <span
                        className={`shrink-0 text-[9px] font-mono font-medium px-1.5 py-0.5 rounded border uppercase tracking-wider ${badgeStyle}`}
                        title={`Data type: ${colType}`}
                      >
                        {colType}
                      </span>
                    </div>

                    {/* Column Resizer Drag Handle */}
                    <div
                      onMouseDown={(e) => handleColResizeStart(col, e)}
                      onDoubleClick={(e) => handleColAutoFit(col, e)}
                      className="absolute right-0 top-0 bottom-0 w-2 cursor-col-resize hover:bg-sky-500/80 group-hover:bg-slate-300 transition-colors z-20"
                      title="Drag to resize • Double-click to auto-fit"
                    />
                  </th>
                );
              })}
            </tr>
          </thead>

          {/* Table Body */}
          <tbody className="divide-y divide-slate-100 font-mono text-[11px]">
            {totalRows === 0 ? (
              <tr>
                <td
                  colSpan={columns.length + 1}
                  className="py-12 text-center text-slate-400 font-mono"
                >
                  Query returned 0 rows.
                </td>
              </tr>
            ) : (
              <>
                {topSpacerHeight > 0 && (
                  <tr style={{ height: `${topSpacerHeight}px` }}>
                    <td
                      colSpan={columns.length + 1}
                      style={{ height: `${topSpacerHeight}px`, padding: 0, border: 'none' }}
                    />
                  </tr>
                )}
                {visibleRows.map((row, idx) => {
                  const rIdx = startIndex + idx;
                  return (
                    <tr key={rIdx} style={{ height: `${ROW_HEIGHT}px` }} className="hover:bg-sky-50/40 transition-colors">
                      {/* Row Index Cell */}
                      <td className="py-2 px-3 w-12 min-w-[48px] max-w-[48px] text-slate-400 text-center border-r border-slate-200 select-none">
                        {rIdx + 1}
                      </td>

                      {/* Data Cells */}
                      {columns.map((col) => {
                        const rawVal = row[col];
                        const strVal =
                          rawVal === null || rawVal === undefined
                            ? 'null'
                            : typeof rawVal === 'object'
                            ? JSON.stringify(rawVal)
                            : String(rawVal);
                        const cellKey = `${rIdx}:${col}`;
                        const isCopied = copiedCell === cellKey;
                        const width = getColWidth(col);

                        return (
                          <td
                            key={col}
                            style={{
                              width: `${width}px`,
                              minWidth: `${width}px`,
                              maxWidth: `${width}px`,
                              height: `${ROW_HEIGHT}px`,
                            }}
                            onClick={() => copyToClipboard(strVal, cellKey)}
                            className="py-2 px-3 text-slate-700 whitespace-nowrap border-r border-slate-200 last:border-r-0 truncate cursor-pointer hover:bg-slate-100/80 group"
                            title="Click to copy"
                          >
                            <div className="flex items-center justify-between gap-2 overflow-hidden">
                              <span
                                className={`truncate ${
                                  rawVal === null || rawVal === undefined
                                    ? 'text-slate-300 italic'
                                    : 'text-slate-800'
                                }`}
                              >
                                {strVal}
                              </span>
                              <span className="opacity-0 group-hover:opacity-100 text-slate-400 shrink-0">
                                {isCopied ? (
                                  <Check className="w-3 h-3 text-emerald-600" />
                                ) : (
                                  <Copy className="w-3 h-3" />
                                )}
                              </span>
                            </div>
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
                {bottomSpacerHeight > 0 && (
                  <tr style={{ height: `${bottomSpacerHeight}px` }}>
                    <td
                      colSpan={columns.length + 1}
                      style={{ height: `${bottomSpacerHeight}px`, padding: 0, border: 'none' }}
                    />
                  </tr>
                )}
              </>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
});
