/**
 * Formats a byte count the way pgAdmin does: `512 B`, `1.4 KB`, `23.8 MB`,
 * `2.1 GB`, `1.4 TB`.
 */
export function formatBytes(bytes?: number | null): string | null {
  if (bytes == null || !Number.isFinite(bytes) || bytes < 0) return null;
  if (bytes < 1024) return `${Math.round(bytes)} B`;

  const units = ['KB', 'MB', 'GB', 'TB', 'PB'];
  let value = bytes / 1024;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return `${value.toFixed(1)} ${units[unitIndex]}`;
}

/**
 * Preferred metric for a catalog row.
 *
 * Physical size is the default everywhere because it comes from database
 * metadata (`pg_total_relation_size`, `data_length`, `storageSize`, segment
 * bytes) and never requires the backend to scan the table. Row counts are
 * planner estimates and are deliberately not shown as the primary metric.
 */
export function tableSizeLabel(table: {
  size_human?: string | null;
  size_bytes?: number | null;
}): string | null {
  return table.size_human ?? formatBytes(table.size_bytes) ?? null;
}
