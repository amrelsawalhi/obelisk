// Strict TypeScript interfaces matching FastAPI Pydantic v2 schemas 1:1

export interface HostSpecs {
  total_ram_bytes: number;
  total_ram_gb: number;
  available_ram_gb: number;
  ram_percent_used: number;
  cpu_count: number;
  cpu_percent: number;
  max_safe_allocation_bytes: number;
  max_safe_allocation_gb: number;
}

export interface TelemetryData {
  cpu_percent: number;
  cpu_count: number;
  ram_percent_used: number;
  total_ram_gb: number;
  available_ram_gb: number;
  ram_used_gb: number;
  max_safe_allocation_gb: number;
  active_containers_count: number;
  allocated_ram_gb: number;
  timestamp?: number;
}

export interface ServiceRecommendation {
  service_name: string;
  recommended_ram_mb: number;
  recommended_ram_bytes: number;
  recommended_cpus: number;
  total_host_ram_gb: number;
  remaining_safe_gb: number;
}

export interface ServiceStatus {
  service_name: string;
  deployment_type: 'local' | 'remote';
  status: string; // 'running' | 'stopped' | 'connected' | 'error'
  host: string;
  port: number;
  allocated_memory_mb: number;
  container_id?: string | null;
  live_docker_status?: string | null;
  host_port?: number;
}

export interface DeployedCredentials {
  username?: string | null;
  password?: string | null;
  database?: string | null;
  host?: string | null;
  port?: number | null;
}

export interface DeployLocalResponse extends ServiceStatus {
  ready: boolean;
  message?: string | null;
  credentials?: DeployedCredentials | null;
  registered_connection?: string | null;
}

export interface ServiceCatalogEntry {
  name: string;
  display_name: string;
  description: string;
  category: string;
  image: string;
  deployable: boolean;
  reason_not_deployable?: string | null;
  default_host_port: number;
  port_map: Record<string, number>;
  volume_name?: string | null;
  probe_timeout_seconds: number;
  recommended_min_disk_gb: number;
  needs_credentials: boolean;
  default_username?: string | null;
  default_database?: string | null;
  register_connection: boolean;
  web_ui: boolean;
  web_ui_path?: string | null;
  run_mode: string;
}

export interface ServiceEngineStatus {
  docker: { available: boolean; error?: string | null };
  disk: { free_gb: number; total_gb: number; percent_used: number };
}

export interface DeployProgressResponse {
  service_name: string;
  phase: string; // idle | pre-flight | pulling | creating | initializing | probing | ready | starting | failed
  detail: string;
  pull_percent: number;
  logs: string[];
  active: boolean;
  error?: string | null;
}

export interface DeployLocalRequest {
  service_name: string;
  memory_mb?: number;
  image?: string;
  host_port?: number;
  public_url?: string;
  username?: string;
  password?: string;
  database?: string;
  env_vars?: Record<string, string>;
}

export interface ConnectRemoteRequest {
  service_name: string;
  host: string;
  port: number;
  config_json?: Record<string, unknown>;
  username?: string;
  password?: string;
  database?: string;
  sslmode?: string;
}

export interface CatalogTableColumn {
  name: string;
  type: string;
  nullable?: boolean;
}

export interface CatalogTable {
  name: string;
  fqn?: string;
  columns: CatalogTableColumn[];
  row_count?: number;
  size_bytes?: number;
  size_human?: string | null;
  size_is_estimate?: boolean;
  format?: string;
  path?: string;
  schema_name?: string;
  schema?: string;
}

export interface LakehouseDirectory {
  id: string;
  name: string;
  tier: string;
  path: string;
  parent_path?: string | null;
  depth?: number;
  tables: CatalogTable[];
}

export interface CatalogNode {
  id: string;
  name: string;
  type: string;
  storage_uri?: string;
  host?: string;
  port?: number;
  database?: string;
  tables: CatalogTable[];
  directories?: LakehouseDirectory[];
}

export interface Connection {
  id: string;
  name: string;
  source_type: string;
  host: string;
  port: number;
  database: string;
  username: string;
  options?: Record<string, unknown> | null;
  created_at: string;
  updated_at?: string;
}

export interface CreateConnectionRequest {
  name: string;
  source_type: string;
  host: string;
  port: number;
  database: string;
  username: string;
  password?: string;
  options?: Record<string, unknown>;
}

export interface UpdateConnectionRequest {
  name?: string;
  source_type?: string;
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  password?: string;
  options?: Record<string, unknown>;
}

export interface ConnectionTestResult {
  status: 'success' | 'error';
  latency_ms: number;
  message: string;
}

export interface TablePreviewResponse {
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
  duration_ms: number;
}

export interface IngestTriggerRequest {
  connection_id: string;
  entity: string;
  mode: 'snapshot' | 'incremental';
  watermark_column?: string;
  target_name?: string;
  target_folder?: string;
  limit?: number;
  write_strategy?: 'append' | 'upsert';
  primary_key?: string;
}

export interface IngestTriggerResponse {
  success: boolean;
  run_id: string;
  asset_name: string;
  metadata: Record<string, unknown>;
  logs: string[];
}

export interface BatchIngestRequest {
  connection_id: string;
  schema_name?: string;
  tables: string[];
  mode: 'snapshot' | 'incremental';
  watermark_column?: string;
  target_folder?: string;
  limit?: number;
  write_strategy?: 'append' | 'upsert';
  primary_key?: string;
}

export interface BatchIngestResultItem {
  status: string;
  entity: string;
  target: string;
  target_folder: string;
  num_rows: number;
  new_rows_extracted: number;
  file_path: string;
  mode: string;
  write_strategy?: string;
}

export interface BatchIngestErrorItem {
  table: string;
  error: string;
}

export interface BatchIngestResponse {
  success: boolean;
  total_tables: number;
  succeeded: number;
  failed: number;
  results: BatchIngestResultItem[];
  errors: BatchIngestErrorItem[];
  logs: string[];
}

export interface IngestJobInfo {
  id: string;
  title: string;
  job_type: 'single' | 'batch';
  connection_id: string;
  source_name: string;
  source_type: string;
  status: 'running' | 'completed' | 'failed' | 'cancelled';
  mode: string;
  started_at: string;
  completed_at?: string | null;
  duration_ms?: number | null;
  total_tables: number;
  completed_tables: number;
  failed_tables: number;
  current_table?: string | null;
  current_phase?: string | null;
  percent: number;
  total_rows: number;
  logs: string[];
  results: BatchIngestResultItem[];
  errors: BatchIngestErrorItem[];
  is_cancelled: boolean;
}

export interface AsyncIngestTriggerResponse {
  job_id: string;
  status: string;
  title: string;
  source_name: string;
  total_tables?: number;
}

export interface QueryTarget {
  id: string;
  name: string;
  engine: string;
  is_default?: boolean;
  description?: string;
  capabilities: string[];
  status: string;
  schemas: string[];
  database?: string;
  host?: string;
  port?: number;
}

export interface QueryExecuteRequest {
  sql: string;
  title?: string;
  timeout_seconds?: number;
  target?: string;
  schema_name?: string;
  postgres_attachments?: Record<string, unknown>[];
}

export interface QueryExecuteResponse {
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
  duration_ms: number;
  status: string;
  command_tag?: string | null;
  target_engine?: string;
  target_name?: string;
}

export interface QueryHistoryItem {
  id: string;
  title?: string | null;
  sql_text: string;
  status: string;
  duration_ms?: number | null;
  row_count?: number | null;
  executed_at: string;
}

export interface Notebook {
  name: string;
  filename: string;
  path: string;
  size_bytes: number;
  modified_at: number;
}

export interface NotebookServerStatus {
  is_running: boolean;
  port: number;
  base_url: string;
  url: string;
  pid?: number | null;
  uptime_seconds: number;
  notebook_count: number;
  notebooks_dir: string;
}

export interface AssetGraphNode {
  id: string;
  name: string;
  /** Schema the node's output lands in, e.g. "raw" or "marts". */
  schema: string;
  /** Project folder it came from: landing, staging or marts. */
  folder?: string;
  type: string;
  path?: string;
  sql_code?: string;
  is_in_pipeline?: boolean;
  filename?: string;
  group?: string;
  deps?: string[];
  description?: string;
}

export interface SaveModelRequest {
  folder: string;
  name: string;
  sql: string;
}

export interface AssetGraphEdge {
  source: string;
  target: string;
}

export interface AssetGraphResponse {
  nodes: AssetGraphNode[];
  edges: AssetGraphEdge[];
  schemas: string[];
  active_pipeline_selector?: string;
  active_node_count?: number;
}

export interface PipelineRun {
  run_id: string;
  pipeline_name?: string;
  selector?: string;
  status: string;
  started_at: string;
  completed_at?: string;
  dbt_run_duration?: number;
  dbt_test_duration?: number;
  logs: string;
}

export interface TriggerPipelineRequest {
  postgres_params?: Record<string, unknown>;
  table_names?: string[];
  selector?: string;
  pipeline_name?: string;
}

export interface VariableItem {
  id: string;
  key: string;
  value: string;
  is_secret: boolean;
  description: string;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface CreateVariableRequest {
  key: string;
  value: string;
  is_secret: boolean;
  description?: string;
}

export interface FileNode {
  name: string;
  path: string;
  type: 'file' | 'directory';
  extension?: string;
  size?: number;
  modified_at?: string | null;
  children?: FileNode[];
}

export interface FileTreeResponse {
  root: string;
  absolute_path: string;
  children: FileNode[];
}

export interface FileContentResponse {
  path: string;
  name: string;
  extension: string;
  size: number;
  modified_at: string;
  content: string;
}

export interface SaveFileRequest {
  path: string;
  content: string;
}

export interface CreateFileRequest {
  path: string;
  type: 'file' | 'directory';
}

export interface RenameFileRequest {
  old_path: string;
  new_path: string;
}

export interface DeleteFileRequest {
  path: string;
}

export interface DbtTreeNode {
  name: string;
  path: string;
  type: 'file' | 'directory';
  extension?: string;
  folder?: 'staging' | 'marts' | null;
  size_bytes?: number;
  modified_at?: string;
  children?: DbtTreeNode[];
}

export interface DbtFileResponse {
  path: string;
  name: string;
  extension: string;
  content: string;
  size_bytes: number;
  modified_at: string;
}

export interface DbtCompileResponse {
  model_name: string;
  target: string;
  compiled_sql: string;
  duration_ms: number;
  logs: string;
  success: boolean;
}

export interface DbtPreviewResponse {
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
  duration_ms: number;
  status: string;
  error?: string;
  command_tag?: string;
  model_name?: string;
  target?: string;
}

export interface DbtExecutionResponse {
  command: string;
  selector: string;
  target: string;
  success: boolean;
  exit_code: number;
  duration_seconds: number;
  duration_ms: number;
  output: string;
  logs: string;
}

export interface DbtLineageNode {
  id: string;
  name: string;
  type: 'model' | 'source' | 'test';
  /** Schema the model lands in, or "test" for a dbt test node. */
  schema: string;
  folder?: string;
  is_current?: boolean;
  materialized?: string;
  path?: string;
}

export interface DbtLineageEdge {
  source: string;
  target: string;
}

export interface DbtLineageResponse {
  model_name: string;
  nodes: DbtLineageNode[];
  edges: DbtLineageEdge[];
  total_nodes: number;
  total_edges: number;
}

export interface OpenQueryTabItem {
  id: string;
  title: string;
  sql: string;
  target_id: string;
  schema_name?: string | null;
  tab_order?: number;
  updated_at?: string;
}

export interface SaveQueryTabPayload {
  id: string;
  title: string;
  sql: string;
  target_id?: string;
  schema_name?: string | null;
  tab_order?: number;
}


// ==========================================
// Pipeline Orchestrator
// ==========================================

export type StepType =
  | 'dbt_run'
  | 'dbt_test'
  | 'script'
  | 'notebook'
  | 'webhook'
  | 'raw_sql'
  | 'pipeline';
export type EdgeCondition = 'success' | 'failure' | 'completion' | 'skipped';
export type OverlapPolicy = 'skip' | 'queue' | 'kill';
export type ExecutionStatus =
  | 'pending'
  | 'waiting'
  | 'running'
  | 'success'
  | 'failed'
  | 'cancelled'
  | 'interrupted'
  | 'skipped';
export type StepRunStatus =
  | 'pending'
  | 'waiting_deps'
  | 'running'
  | 'success'
  | 'failed'
  | 'skipped'
  | 'cancelled'
  | 'interrupted';

export interface PipelineParamDef {
  name: string;
  type: 'string' | 'int' | 'float' | 'bool';
  default_value?: string | null;
  required: boolean;
}

export interface PipelineStepDef {
  id: string;
  key: string;
  name: string;
  step_type: StepType;
  target: string;
  args: Record<string, unknown>;
  timeout_seconds: number | null;
  max_retries: number | null;
  retry_delay_seconds: number | null;
  position_x: number;
  position_y: number;
}

export interface PipelineEdgeDef {
  id: string;
  from_step: string;
  to_step: string;
  from_key: string;
  to_key: string;
  condition: EdgeCondition;
}

export interface PipelineSummary {
  id: string;
  name: string;
  description: string;
  cron_expression: string;
  timezone: string;
  is_enabled: boolean;
  overlap_policy: OverlapPolicy;
  max_parallel: number;
  keep_runs: number;
  timeout_seconds: number;
  max_retries: number;
  retry_delay_seconds: number;
  auto_rerun_interrupted: boolean;
  next_run_at: string | null;
  last_status: string;
  last_run_at: string | null;
  /** Newest first; the rail paints one dot per run. */
  recent_statuses?: string[];
  step_count: number;
  is_running: boolean;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface PipelineExecution {
  id: string;
  pipeline_id: string;
  pipeline_name?: string | null;
  kind: 'scheduled' | 'manual' | 'rerun';
  triggered_by?: string | null;
  status: ExecutionStatus;
  params: Record<string, unknown>;
  scheduled_for: string | null;
  started_at: string | null;
  ended_at: string | null;
  duration_seconds: number | null;
  error: string | null;
  step_counts: Record<string, number>;
  /** Set when a pipeline step in another pipeline started this run. */
  parent_run_id?: string | null;
  parent_step_run_id?: string | null;
  parent?: PipelineParent | null;
}

export interface PipelineParent {
  run_id: string;
  pipeline_id?: string;
  pipeline_name: string | null;
  step_name: string | null;
}

export interface StepOutputValue {
  type: 'scalar' | 'table';
  value: unknown;
}

export interface StepRunItem {
  id: string;
  run_id: string;
  step_id: string;
  step_name: string;
  attempt: number;
  status: StepRunStatus;
  claimed_at: string | null;
  started_at: string | null;
  ended_at: string | null;
  duration_seconds: number | null;
  next_retry_at: string | null;
  error: string | null;
  logs_size: number;
  logs?: string;
  /** Declared outputs captured from this attempt; never rows, only names and scalars. */
  outputs?: Record<string, StepOutputValue>;
}

export interface PipelineExecutionDetail extends PipelineExecution {
  step_runs: StepRunItem[];
}

export interface PipelineDetail extends PipelineSummary {
  steps: PipelineStepDef[];
  edges: PipelineEdgeDef[];
  params: PipelineParamDef[];
  last_run: PipelineExecution | null;
}

/** A target defined in the dbt project's profiles.yml. */
export interface DbtTarget {
  name: string;
  adapter: string;
  available: boolean;
  detail: {
    host?: string;
    port?: number;
    database?: string;
    path?: string;
    source?: string;
  };
}

export interface DbtTargets {
  project: string;
  default: string;
  targets: DbtTarget[];
}

export interface PipelineTargets {
  scripts: string[];
  notebooks: string[];
  dbt_models: string[];
  dbt_directories: string[];
  step_types: StepType[];
  edge_conditions: EdgeCondition[];
}

export interface StepPayload {
  key: string;
  name: string;
  step_type: StepType;
  target: string;
  args?: Record<string, unknown> | null;
  timeout_seconds?: number | null;
  max_retries?: number | null;
  retry_delay_seconds?: number | null;
  position_x: number;
  position_y: number;
}

export interface EdgePayload {
  from_key: string;
  to_key: string;
  condition: EdgeCondition;
}

export interface GraphPayload {
  steps: StepPayload[];
  edges: EdgePayload[];
  params: PipelineParamDef[];
}

export interface CreatePipelinePayload {
  name: string;
  description?: string;
  cron_expression: string;
  timezone: string;
  is_enabled: boolean;
  overlap_policy: OverlapPolicy;
  max_parallel: number;
  keep_runs: number;
  timeout_seconds: number;
  max_retries: number;
  retry_delay_seconds: number;
  auto_rerun_interrupted: boolean;
  graph?: GraphPayload;
}

