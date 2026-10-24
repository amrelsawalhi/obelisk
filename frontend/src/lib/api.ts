import axios from 'axios';
import type {
  HostSpecs,
  ServiceRecommendation,
  ServiceStatus,
  DeployLocalRequest,
  ConnectRemoteRequest,
  CatalogNode,
  Connection,
  CreateConnectionRequest,
  TablePreviewResponse,
  IngestTriggerRequest,
  IngestTriggerResponse,
  QueryExecuteRequest,
  QueryExecuteResponse,
  QueryHistoryItem,
  QueryTarget,
  Notebook,
  NotebookServerStatus,
  AssetGraphResponse,
  PipelineRun,
  TriggerPipelineRequest,
  VariableItem,
  CreateVariableRequest,
  DbtTreeNode,
  DbtFileResponse,
  DbtCompileResponse,
  DbtPreviewResponse,
  DbtExecutionResponse,
  DbtLineageResponse,
  DeployLocalResponse,
  ServiceCatalogEntry,
  ServiceEngineStatus,
  DeployProgressResponse,
  PipelineSummary,
  PipelineDetail,
  PipelineExecution,
  PipelineExecutionDetail,
  PipelineTargets,
  CreatePipelinePayload,
  GraphPayload,
  ExecutionStatus,
} from './types';

export const apiClient = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
  },
  withCredentials: true,
});

export const api = {
  // Services & Resource Advisor
  getHardwareSpecs: async (): Promise<HostSpecs> => {
    const res = await apiClient.get<HostSpecs>('/services/advisor/specs');
    return res.data;
  },
  getTelemetry: async (): Promise<import('./types').TelemetryData> => {
    const res = await apiClient.get<import('./types').TelemetryData>('/services/telemetry');
    return res.data;
  },
  getRecommendation: async (serviceName: string): Promise<ServiceRecommendation> => {
    const res = await apiClient.get<ServiceRecommendation>(`/services/advisor/recommend/${serviceName}`);
    return res.data;
  },
  listServices: async (): Promise<ServiceStatus[]> => {
    const res = await apiClient.get<ServiceStatus[]>('/services/status');
    return res.data;
  },
  deployLocalService: async (req: DeployLocalRequest): Promise<DeployLocalResponse> => {
    const res = await apiClient.post<DeployLocalResponse>('/services/deploy-local', req);
    return res.data;
  },
  getServicesEngine: async (): Promise<ServiceEngineStatus> => {
    const res = await apiClient.get<ServiceEngineStatus>('/services/engine');
    return res.data;
  },
  getServiceDeployProgress: async (serviceName: string): Promise<DeployProgressResponse> => {
    const res = await apiClient.get<DeployProgressResponse>(`/services/${serviceName}/deploy-progress`);
    return res.data;
  },
  getServiceCatalog: async (): Promise<ServiceCatalogEntry[]> => {
    const res = await apiClient.get<ServiceCatalogEntry[]>('/services/catalog');
    return res.data;
  },
  getServiceLogs: async (serviceName: string, tail: number = 200): Promise<{ service_name: string; logs: string }> => {
    const res = await apiClient.get<{ service_name: string; logs: string }>(`/services/${serviceName}/logs`, {
      params: { tail },
    });
    return res.data;
  },
  destroyService: async (serviceName: string): Promise<{ service_name: string; status: string; data_removed: boolean }> => {
    const res = await apiClient.post<{ service_name: string; status: string; data_removed: boolean }>(`/services/${serviceName}/destroy`);
    return res.data;
  },
  connectRemoteService: async (req: ConnectRemoteRequest): Promise<ServiceStatus> => {
    const res = await apiClient.post<ServiceStatus>('/services/connect', req);
    return res.data;
  },
  stopService: async (serviceName: string): Promise<{ service_name: string; status: string }> => {
    const res = await apiClient.post<{ service_name: string; status: string }>(`/services/${serviceName}/stop`);
    return res.data;
  },

  // Catalog & Connections
  getCatalogTree: async (): Promise<CatalogNode[]> => {
    const res = await apiClient.get<CatalogNode[]>('/catalog/tree');
    return res.data;
  },
  getTablePreview: async (sourceId: string, tableName: string): Promise<TablePreviewResponse> => {
    const res = await apiClient.get<TablePreviewResponse>(`/catalog/preview/${sourceId}/${tableName}`);
    return res.data;
  },
  createLakehouseDirectory: async (path: string): Promise<{ status: string; name: string; path: string }> => {
    const res = await apiClient.post<{ status: string; name: string; path: string }>('/catalog/lakehouse/directories', { path });
    return res.data;
  },
  renameLakehouseDirectory: async (path: string, newName: string): Promise<{ status: string; old_path: string; new_path: string }> => {
    const res = await apiClient.put<{ status: string; old_path: string; new_path: string }>('/catalog/lakehouse/directories/rename', { path, new_name: newName });
    return res.data;
  },
  deleteLakehouseDirectory: async (path: string, cascade: boolean = false): Promise<{ status: string; path: string }> => {
    const res = await apiClient.delete<{ status: string; path: string }>('/catalog/lakehouse/directories', {
      params: { path, cascade }
    });
    return res.data;
  },
  renameLakehouseTable: async (folder: string, oldName: string, newName: string): Promise<{ status: string; old_name: string; new_name: string }> => {
    const res = await apiClient.put<{ status: string; old_name: string; new_name: string }>('/catalog/lakehouse/tables/rename', { folder, old_name: oldName, new_name: newName });
    return res.data;
  },
  deleteLakehouseTable: async (folder: string, name: string): Promise<{ status: string; folder: string; name: string }> => {
    const res = await apiClient.delete<{ status: string; folder: string; name: string }>('/catalog/lakehouse/tables', {
      params: { folder, name }
    });
    return res.data;
  },
  moveLakehouseTable: async (sourceFolder: string, targetFolder: string, tableName: string): Promise<{ status: string; source: string; target: string; table: string }> => {
    const res = await apiClient.post<{ status: string; source: string; target: string; table: string }>('/catalog/lakehouse/tables/move', {
      source_folder: sourceFolder,
      target_folder: targetFolder,
      table_name: tableName,
    });
    return res.data;
  },
  listConnections: async (): Promise<Connection[]> => {
    const res = await apiClient.get<Connection[]>('/catalog/connections');
    return res.data;
  },
  listLayers: async (): Promise<import('./types').Layer[]> => {
    const res = await apiClient.get<import('./types').Layer[]>('/layers');
    return res.data;
  },
  createLayer: async (req: {
    path: string;
    description?: string;
  }): Promise<import('./types').Layer> => {
    const res = await apiClient.post<import('./types').Layer>('/layers', req);
    return res.data;
  },
  updateLayer: async (
    path: string,
    req: { name?: string; description?: string; position?: number; is_default?: boolean },
  ): Promise<import('./types').Layer> => {
    const res = await apiClient.patch<import('./types').Layer>(
      `/layers/${encodeURIComponent(path)}`,
      req,
    );
    return res.data;
  },
  deleteLayer: async (
    path: string,
  ): Promise<{ deleted: string; default_layer: string | null }> => {
    const res = await apiClient.delete<{ deleted: string; default_layer: string | null }>(
      `/layers/${encodeURIComponent(path)}`,
    );
    return res.data;
  },
  resolveLayer: async (path: string): Promise<import('./types').ResolvedLayer> => {
    const res = await apiClient.get<import('./types').ResolvedLayer>('/layers/resolve', {
      params: { path },
    });
    return res.data;
  },
  createConnection: async (req: CreateConnectionRequest): Promise<Connection> => {
    const res = await apiClient.post<Connection>('/catalog/connections', req);
    return res.data;
  },
  updateConnection: async (connId: string, req: import('./types').UpdateConnectionRequest): Promise<Connection> => {
    const res = await apiClient.put<Connection>(`/catalog/connections/${connId}`, req);
    return res.data;
  },
  deleteConnection: async (connId: string): Promise<{ status: string; id: string }> => {
    const res = await apiClient.delete<{ status: string; id: string }>(`/catalog/connections/${connId}`);
    return res.data;
  },
  testConnection: async (connId: string): Promise<import('./types').ConnectionTestResult> => {
    const res = await apiClient.post<import('./types').ConnectionTestResult>(`/catalog/connections/${connId}/test`);
    return res.data;
  },
  testConnectionParams: async (req: CreateConnectionRequest): Promise<import('./types').ConnectionTestResult> => {
    const res = await apiClient.post<import('./types').ConnectionTestResult>('/catalog/connections/test', req);
    return res.data;
  },

  // Ingestion
  triggerIngest: async (req: IngestTriggerRequest): Promise<IngestTriggerResponse> => {
    const res = await apiClient.post<IngestTriggerResponse>('/ingest/trigger', req);
    return res.data;
  },
  triggerBatchIngest: async (req: import('./types').BatchIngestRequest): Promise<import('./types').BatchIngestResponse> => {
    const res = await apiClient.post<import('./types').BatchIngestResponse>('/ingest/batch', req);
    return res.data;
  },
  triggerIngestAsync: async (req: IngestTriggerRequest): Promise<import('./types').AsyncIngestTriggerResponse> => {
    const res = await apiClient.post<import('./types').AsyncIngestTriggerResponse>('/ingest/trigger/async', req);
    return res.data;
  },
  triggerBatchIngestAsync: async (req: import('./types').BatchIngestRequest): Promise<import('./types').AsyncIngestTriggerResponse> => {
    const res = await apiClient.post<import('./types').AsyncIngestTriggerResponse>('/ingest/batch/async', req);
    return res.data;
  },
  getIngestJob: async (jobId: string): Promise<import('./types').IngestJobInfo> => {
    const res = await apiClient.get<import('./types').IngestJobInfo>(`/ingest/jobs/${jobId}`);
    return res.data;
  },
  getActiveIngestJobs: async (): Promise<import('./types').IngestJobInfo[]> => {
    const res = await apiClient.get<import('./types').IngestJobInfo[]>('/ingest/jobs/active');
    return res.data;
  },
  cancelIngestJob: async (jobId: string): Promise<{ status: string; job_id: string }> => {
    const res = await apiClient.post<{ status: string; job_id: string }>(`/ingest/jobs/${jobId}/cancel`);
    return res.data;
  },

  // Query Studio
  executeQuery: async (req: QueryExecuteRequest): Promise<QueryExecuteResponse> => {
    const res = await apiClient.post<QueryExecuteResponse>('/query/execute', req);
    return res.data;
  },
  getQueryHistory: async (): Promise<QueryHistoryItem[]> => {
    const res = await apiClient.get<QueryHistoryItem[]>('/query/history');
    return res.data;
  },
  getQueryTargets: async (): Promise<QueryTarget[]> => {
    const res = await apiClient.get<QueryTarget[]>('/query/targets');
    return res.data;
  },
  getOpenQueryTabs: async (): Promise<import('./types').OpenQueryTabItem[]> => {
    const res = await apiClient.get<import('./types').OpenQueryTabItem[]>('/query/tabs');
    return res.data;
  },
  saveOpenQueryTab: async (tab: import('./types').SaveQueryTabPayload): Promise<import('./types').OpenQueryTabItem> => {
    const res = await apiClient.post<import('./types').OpenQueryTabItem>('/query/tabs', tab);
    return res.data;
  },
  deleteOpenQueryTab: async (tabId: string): Promise<{ status: string; id: string }> => {
    const res = await apiClient.delete<{ status: string; id: string }>(`/query/tabs/${tabId}`);
    return res.data;
  },

  // Notebooks
  listNotebooks: async (): Promise<{ notebooks: Notebook[]; total: number }> => {
    const res = await apiClient.get<{ notebooks: Notebook[]; total: number }>('/notebooks');
    return res.data;
  },
  createNotebook: async (name: string, content?: string, overwrite?: boolean): Promise<{ status: string; name: string; filename: string; path: string }> => {
    const res = await apiClient.post<{ status: string; name: string; filename: string; path: string }>('/notebooks', { name, content, overwrite });
    return res.data;
  },
  deleteNotebook: async (name: string): Promise<{ status: string; name: string }> => {
    const res = await apiClient.delete<{ status: string; name: string }>(`/notebooks/${name}`);
    return res.data;
  },
  getNotebookServerStatus: async (): Promise<NotebookServerStatus> => {
    const res = await apiClient.get<NotebookServerStatus>('/notebooks/server/status');
    return res.data;
  },
  startNotebookServer: async (port?: number): Promise<Record<string, unknown>> => {
    const res = await apiClient.post<Record<string, unknown>>('/notebooks/server/start', port ? { port } : {});
    return res.data;
  },
  stopNotebookServer: async (): Promise<{ status: string }> => {
    const res = await apiClient.post<{ status: string }>('/notebooks/server/stop');
    return res.data;
  },
  restartNotebookServer: async (port?: number): Promise<Record<string, unknown>> => {
    const res = await apiClient.post<Record<string, unknown>>('/notebooks/server/restart', port ? { port } : {});
    return res.data;
  },

  // Pipelines (orchestrator)
  listPipelines: async (): Promise<PipelineSummary[]> => {
    const res = await apiClient.get<PipelineSummary[]>('/pipelines');
    return res.data;
  },
  getPipeline: async (id: string): Promise<PipelineDetail> => {
    const res = await apiClient.get<PipelineDetail>(`/pipelines/${id}`);
    return res.data;
  },
  createPipeline: async (payload: CreatePipelinePayload): Promise<PipelineSummary> => {
    const res = await apiClient.post<PipelineSummary>('/pipelines', payload);
    return res.data;
  },
  updatePipeline: async (
    id: string,
    payload: Partial<CreatePipelinePayload>,
  ): Promise<PipelineSummary> => {
    const res = await apiClient.patch<PipelineSummary>(`/pipelines/${id}`, payload);
    return res.data;
  },
  replacePipelineGraph: async (
    id: string,
    graph: GraphPayload,
  ): Promise<{ status: string; pipeline_id: string; step_count: number }> => {
    const res = await apiClient.put<{ status: string; pipeline_id: string; step_count: number }>(
      `/pipelines/${id}/graph`,
      graph,
    );
    return res.data;
  },
  // Layout is presentation: it saves on its own, without the graph's validation.
  savePipelineLayout: async (
    id: string,
    positions: Array<{ key: string; x: number; y: number }>,
  ): Promise<{ status: string; updated: number }> => {
    const res = await apiClient.put<{ status: string; updated: number }>(
      `/pipelines/${id}/layout`,
      { positions },
    );
    return res.data;
  },

  deletePipeline: async (id: string): Promise<{ status: string; id: string }> => {
    const res = await apiClient.delete<{ status: string; id: string }>(`/pipelines/${id}`);
    return res.data;
  },
  togglePipeline: async (
    id: string,
  ): Promise<{ id: string; is_enabled: boolean; next_run_at: string | null }> => {
    const res = await apiClient.post<{ id: string; is_enabled: boolean; next_run_at: string | null }>(
      `/pipelines/${id}/toggle`,
    );
    return res.data;
  },
  runPipelineNow: async (
    id: string,
    params?: Record<string, unknown>,
  ): Promise<{ run_id: string }> => {
    const res = await apiClient.post<{ run_id: string }>(`/pipelines/${id}/run`, {
      params: params ?? null,
    });
    return res.data;
  },
  previewSchedule: async (
    cronExpression: string,
    timezone: string,
    count = 5,
  ): Promise<{ next_runs: string[] }> => {
    const res = await apiClient.get<{ next_runs: string[] }>('/pipelines/preview', {
      params: { cron_expression: cronExpression, timezone, count },
    });
    return res.data;
  },
  getPipelineTargets: async (): Promise<PipelineTargets> => {
    const res = await apiClient.get<PipelineTargets>('/pipelines/targets');
    return res.data;
  },
  listPipelineExecutions: async (params?: {
    pipeline_id?: string;
    status?: ExecutionStatus;
    limit?: number;
  }): Promise<PipelineExecution[]> => {
    const res = await apiClient.get<PipelineExecution[]>('/pipeline-runs', { params });
    return res.data;
  },
  getPipelineExecution: async (runId: string): Promise<PipelineExecutionDetail> => {
    const res = await apiClient.get<PipelineExecutionDetail>(`/pipeline-runs/${runId}`);
    return res.data;
  },
  cancelPipelineExecution: async (runId: string): Promise<{ status: string }> => {
    const res = await apiClient.post<{ status: string }>(`/pipeline-runs/${runId}/cancel`);
    return res.data;
  },
  rerunFromStep: async (runId: string, stepId: string): Promise<{ run_id: string }> => {
    const res = await apiClient.post<{ run_id: string }>(
      `/pipeline-runs/${runId}/rerun-from/${stepId}`,
    );
    return res.data;
  },
  getStepRunLogs: async (
    stepRunId: string,
    after = 0,
  ): Promise<{ id: string; status: string; size: number; offset: number; logs: string }> => {
    const res = await apiClient.get<{
      id: string;
      status: string;
      size: number;
      offset: number;
      logs: string;
    }>(`/step-runs/${stepRunId}/logs`, { params: { after } });
    return res.data;
  },
  getRunLogs: async (
    runId: string,
  ): Promise<{ run_id: string; status: string; size: number; logs: string }> => {
    const res = await apiClient.get<{
      run_id: string;
      status: string;
      size: number;
      logs: string;
    }>(`/pipeline-runs/${runId}/logs`);
    return res.data;
  },
  getAssetGraph: async (selector?: string): Promise<AssetGraphResponse> => {
    const res = await apiClient.get<AssetGraphResponse>('/pipelines/assets', {
      params: selector ? { selector } : {},
    });
    return res.data;
  },
  triggerPipelineRun: async (payload?: TriggerPipelineRequest): Promise<PipelineRun> => {
    const res = await apiClient.post<PipelineRun>('/pipelines/run', payload || {});
    return res.data;
  },
  getPipelineRuns: async (): Promise<{ runs: PipelineRun[]; total: number }> => {
    const res = await apiClient.get<{ runs: PipelineRun[]; total: number }>('/pipelines/runs');
    return res.data;
  },
  getModelCode: async (layer: string, modelName: string): Promise<{ name: string; layer: string; sql: string }> => {
    const res = await apiClient.get<{ name: string; layer: string; sql: string }>(`/pipelines/model/${layer}/${modelName}`);
    return res.data;
  },
  saveModelCode: async (payload: { layer: string; name: string; sql: string }): Promise<Record<string, unknown>> => {
    const res = await apiClient.post<Record<string, unknown>>('/pipelines/model', payload);
    return res.data;
  },

  // Variables & Secrets
  getVariables: async (): Promise<VariableItem[]> => {
    const res = await apiClient.get<VariableItem[]>('/variables');
    return res.data;
  },
  createVariable: async (payload: CreateVariableRequest): Promise<VariableItem> => {
    const res = await apiClient.post<VariableItem>('/variables', payload);
    return res.data;
  },
  revealVariable: async (key: string): Promise<{ key: string; value: string; is_secret: boolean }> => {
    const res = await apiClient.get<{ key: string; value: string; is_secret: boolean }>(`/variables/${key}/reveal`);
    return res.data;
  },
  deleteVariable: async (id: string): Promise<{ status: string; id: string }> => {
    const res = await apiClient.delete<{ status: string; id: string }>(`/variables/${id}`);
    return res.data;
  },



  // File Manager (git_repo/)
  getFileTree: async (): Promise<import('./types').FileTreeResponse> => {
    const res = await apiClient.get<import('./types').FileTreeResponse>('/files/tree');
    return res.data;
  },
  getFileContent: async (path: string): Promise<import('./types').FileContentResponse> => {
    const res = await apiClient.get<import('./types').FileContentResponse>('/files/content', {
      params: { path },
    });
    return res.data;
  },
  saveFile: async (path: string, content: string): Promise<{ status: string; path: string; size: number; modified_at?: string }> => {
    const res = await apiClient.post<{ status: string; path: string; size: number; modified_at?: string }>('/files/save', {
      path,
      content,
    });
    return res.data;
  },

  createFile: async (path: string, type: 'file' | 'directory'): Promise<{ status: string; path: string }> => {
    const res = await apiClient.post<{ status: string; path: string }>('/files/create', {
      path,
      type,
    });
    return res.data;
  },
  renameFile: async (old_path: string, new_path: string): Promise<{ status: string; old_path: string; new_path: string }> => {
    const res = await apiClient.post<{ status: string; old_path: string; new_path: string }>('/files/rename', {
      old_path,
      new_path,
    });
    return res.data;
  },
  deleteFile: async (path: string): Promise<{ status: string; path: string }> => {
    const res = await apiClient.post<{ status: string; path: string }>('/files/delete', {
      path,
    });
    return res.data;
  },

  // dbt Studio API
  getDbtTree: async (): Promise<DbtTreeNode> => {
    const res = await apiClient.get<DbtTreeNode>('/dbt/tree');
    return res.data;
  },

  getDbtFile: async (path: string): Promise<DbtFileResponse> => {
    const res = await apiClient.get<DbtFileResponse>('/dbt/file', {
      params: { path },
    });
    return res.data;
  },

  saveDbtFile: async (path: string, content: string): Promise<{ status: string; path: string; size_bytes: number; modified_at: string }> => {
    const res = await apiClient.post<{ status: string; path: string; size_bytes: number; modified_at: string }>('/dbt/file', {
      path,
      content,
    });
    return res.data;
  },

  deleteDbtFile: async (path: string): Promise<{ status: string; path: string }> => {
    const res = await apiClient.delete<{ status: string; path: string }>('/dbt/file', {
      params: { path },
    });
    return res.data;
  },

  compileDbtModel: async (model_name: string, target: string = 'postgres'): Promise<DbtCompileResponse> => {
    const res = await apiClient.post<DbtCompileResponse>('/dbt/compile', {
      model_name,
      target,
    });
    return res.data;
  },

  previewDbtModel: async (model_name: string, target: string = 'postgres', limit: number = 50): Promise<DbtPreviewResponse> => {
    const res = await apiClient.post<DbtPreviewResponse>('/dbt/preview', {
      model_name,
      target,
      limit,
    });
    return res.data;
  },

  runDbtModels: async (selector: string = '*', target: string = 'postgres'): Promise<DbtExecutionResponse> => {
    const res = await apiClient.post<DbtExecutionResponse>('/dbt/run', {
      selector,
      target,
    });
    return res.data;
  },

  testDbtModels: async (selector: string = '*', target: string = 'postgres'): Promise<DbtExecutionResponse> => {
    const res = await apiClient.post<DbtExecutionResponse>('/dbt/test', {
      selector,
      target,
    });
    return res.data;
  },

  getDbtLineage: async (model: string): Promise<DbtLineageResponse> => {
    const res = await apiClient.get<DbtLineageResponse>('/dbt/lineage', {
      params: { model },
    });
    return res.data;
  },

};

