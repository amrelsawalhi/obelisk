import {
  Database,
  GitBranch,
  BarChart3,
  Radio,
  Zap,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';

// Presentational icons for catalog services (backend blueprints are the
// single source of truth for identity metadata; icons are UI-only).
export const SERVICE_ICONS: Record<string, LucideIcon> = {
  postgres: Database,
  dagster: GitBranch,
  superset: BarChart3,
  clickhouse: Zap,
  redpanda: Radio,
  gitea: GitBranch,
};
