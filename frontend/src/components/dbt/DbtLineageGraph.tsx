import React from 'react';
import {
  Database,
  FileCode,
  CheckCircle2,
  Layers,
  Sparkles,
  Split,
} from 'lucide-react';
import type { DbtLineageResponse, DbtLineageNode } from '../../lib/types';

interface DbtLineageGraphProps {
  lineage: DbtLineageResponse | null;
  loading: boolean;
  onSelectModel?: (modelName: string) => void;
}

export const DbtLineageGraph: React.FC<DbtLineageGraphProps> = ({
  lineage,
  loading,
  onSelectModel,
}) => {
  if (loading) {
    return (
      <div className="h-full flex items-center justify-center p-8 text-xs text-slate-400">
        <div className="w-5 h-5 border-2 border-sky-600 border-t-transparent rounded-full animate-spin mr-2" />
        <span>Computing model lineage from dbt manifest...</span>
      </div>
    );
  }

  if (!lineage || lineage.nodes.length === 0) {
    return (
      <div className="h-full flex flex-col items-center justify-center p-8 text-slate-400 text-xs">
        <Split className="w-8 h-8 text-slate-300 mb-2" />
        <p>No lineage dependencies found for this model.</p>
        <p className="text-[11px] text-slate-400 mt-0.5">
          Compile or run the model to generate the dependency manifest.
        </p>
      </div>
    );
  }

  const currentNode = lineage.nodes.find((n) => n.is_current) || {
    id: `model.${lineage.model_name}`,
    name: lineage.model_name,
    type: 'model' as const,
    layer: 'silver',
    is_current: true,
  };

  const upstreamNodes = lineage.nodes.filter(
    (n) => !n.is_current && lineage.edges.some((e) => e.source === n.id && e.target === currentNode.id)
  );

  const downstreamNodes = lineage.nodes.filter(
    (n) => !n.is_current && lineage.edges.some((e) => e.source === currentNode.id && e.target === n.id)
  );

  const getNodeColor = (node: DbtLineageNode) => {
    if (node.is_current) {
      return 'border-sky-500 bg-sky-50/70 text-sky-950 ring-2 ring-sky-500/20';
    }
    if (node.type === 'source') {
      return 'border-amber-300 bg-amber-50/60 text-amber-950';
    }
    if (node.type === 'test') {
      return 'border-emerald-300 bg-emerald-50/60 text-emerald-950';
    }
    if (node.layer === 'gold') {
      return 'border-indigo-300 bg-indigo-50/60 text-indigo-950';
    }
    return 'border-slate-300 bg-white text-slate-900';
  };

  const getNodeIcon = (node: DbtLineageNode) => {
    if (node.type === 'source') {
      return <Database className="w-3.5 h-3.5 text-amber-600 shrink-0" />;
    }
    if (node.type === 'test') {
      return <CheckCircle2 className="w-3.5 h-3.5 text-emerald-600 shrink-0" />;
    }
    if (node.layer === 'gold') {
      return <Layers className="w-3.5 h-3.5 text-indigo-600 shrink-0" />;
    }
    return <FileCode className="w-3.5 h-3.5 text-sky-600 shrink-0" />;
  };

  return (
    <div className="h-full flex flex-col overflow-hidden bg-slate-50/40">
      {/* Subheader Toolbar */}
      <div className="px-4 py-2 border-b border-slate-200 bg-white flex items-center justify-between text-xs">
        <div className="flex items-center gap-2">
          <span className="font-semibold text-slate-800">Model Lineage DAG</span>
          <span className="text-slate-400 font-mono text-[11px] truncate">
            {lineage.model_name} ({upstreamNodes.length} Upstream • {downstreamNodes.length} Downstream)
          </span>
        </div>
        <div className="flex items-center gap-3 text-[10px] text-slate-500">
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 rounded-full bg-amber-400" />
            <span>Bronze Source</span>
          </span>
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 rounded-full bg-sky-500" />
            <span>Silver Staging</span>
          </span>
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 rounded-full bg-indigo-500" />
            <span>Gold Mart</span>
          </span>
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 rounded-full bg-emerald-500" />
            <span>Test Assertion</span>
          </span>
        </div>
      </div>

      {/* Visual DAG Columns Canvas */}
      <div className="flex-1 overflow-auto p-6 flex items-center justify-center min-w-[700px]">
        <div className="grid grid-cols-3 gap-12 w-full max-w-4xl items-center relative">
          {/* Column 1: Upstream Dependencies */}
          <div className="flex flex-col gap-3">
            <div className="text-[11px] font-bold text-slate-400 uppercase tracking-wider mb-1 flex items-center gap-1">
              <span>Upstream Sources & Models</span>
              <span className="text-[10px] font-mono px-1.5 py-0.2 bg-slate-200/70 rounded text-slate-600">
                {upstreamNodes.length}
              </span>
            </div>
            {upstreamNodes.length === 0 ? (
              <div className="p-3 rounded-lg border border-dashed border-slate-200 text-slate-400 text-center text-xs bg-white">
                No direct upstream parents
              </div>
            ) : (
              upstreamNodes.map((node) => (
                <div
                  key={node.id}
                  onClick={() => node.type === 'model' && onSelectModel?.(node.name)}
                  className={`p-2.5 rounded-lg border shadow-2xs transition-all flex items-center justify-between gap-2 ${
                    node.type === 'model' ? 'cursor-pointer hover:shadow-xs hover:border-slate-400' : ''
                  } ${getNodeColor(node)}`}
                >
                  <div className="flex items-center gap-2 truncate">
                    {getNodeIcon(node)}
                    <span className="font-mono text-xs font-medium truncate">{node.name}</span>
                  </div>
                  <span className="text-[9px] font-mono uppercase px-1.5 py-0.5 rounded bg-white/80 border border-slate-200 text-slate-500 shrink-0">
                    {node.type === 'source' ? 'source' : node.layer}
                  </span>
                </div>
              ))
            )}
          </div>

          {/* Column 2: Current Center Model */}
          <div className="flex flex-col items-center">
            <div className="text-[11px] font-bold text-sky-700 uppercase tracking-wider mb-2 flex items-center gap-1">
              <Sparkles className="w-3.5 h-3.5 text-sky-600" />
              <span>Current Model</span>
            </div>
            <div
              className={`w-full p-4 rounded-xl border-2 shadow-sm flex flex-col gap-2 ${getNodeColor(
                currentNode
              )}`}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <FileCode className="w-4 h-4 text-sky-600" />
                  <span className="font-mono font-bold text-sm text-slate-900 truncate">
                    {currentNode.name}
                  </span>
                </div>
                <span className="text-[10px] font-mono font-semibold uppercase px-2 py-0.5 rounded bg-sky-100 text-sky-800 border border-sky-300">
                  {currentNode.layer || 'model'}
                </span>
              </div>
              <div className="text-[11px] text-slate-500 font-mono flex items-center gap-2">
                <span>Materialization: {currentNode.materialized || 'table'}</span>
              </div>
            </div>
          </div>

          {/* Column 3: Downstream Models & Tests */}
          <div className="flex flex-col gap-3">
            <div className="text-[11px] font-bold text-slate-400 uppercase tracking-wider mb-1 flex items-center gap-1">
              <span>Downstream Marts & Tests</span>
              <span className="text-[10px] font-mono px-1.5 py-0.2 bg-slate-200/70 rounded text-slate-600">
                {downstreamNodes.length}
              </span>
            </div>
            {downstreamNodes.length === 0 ? (
              <div className="p-3 rounded-lg border border-dashed border-slate-200 text-slate-400 text-center text-xs bg-white">
                No downstream consumers
              </div>
            ) : (
              downstreamNodes.map((node) => (
                <div
                  key={node.id}
                  onClick={() => node.type === 'model' && onSelectModel?.(node.name)}
                  className={`p-2.5 rounded-lg border shadow-2xs transition-all flex items-center justify-between gap-2 ${
                    node.type === 'model' ? 'cursor-pointer hover:shadow-xs hover:border-slate-400' : ''
                  } ${getNodeColor(node)}`}
                >
                  <div className="flex items-center gap-2 truncate">
                    {getNodeIcon(node)}
                    <span className="font-mono text-xs font-medium truncate">{node.name}</span>
                  </div>
                  <span className="text-[9px] font-mono uppercase px-1.5 py-0.5 rounded bg-white/80 border border-slate-200 text-slate-500 shrink-0">
                    {node.type === 'test' ? 'test' : node.layer}
                  </span>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
};
