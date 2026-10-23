import React, { useState } from 'react';
import {
  Copy,
  Check,
  BookOpen,
  Search,
  Sparkles,
  Database,
  Key,
  Layers,
  Cpu,
  Zap,
} from 'lucide-react';

interface CodeSnippetProps {
  title: string;
  badge?: string;
  code: string;
}

const CodeSnippet: React.FC<CodeSnippetProps> = ({ title, badge = 'obelisk Python SDK', code }) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 1800);
  };

  return (
    <div className="bg-[#0b1120] border border-slate-800/90 rounded-xl overflow-hidden shadow-xl text-xs font-mono group">
      {/* Terminal Title Bar */}
      <div className="px-4 py-3 bg-[#070b14] border-b border-slate-800/80 flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 text-slate-300 font-semibold tracking-wide">
          <span className="text-sky-400 font-bold">&gt;_</span>
          <span className="text-[11px] uppercase tracking-wider text-slate-200">{title}</span>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-[10px] text-sky-400/80 bg-sky-950/60 border border-sky-800/60 px-2 py-0.5 rounded font-mono">
            {badge}
          </span>
          <button
            onClick={handleCopy}
            className="flex items-center gap-1.5 px-2.5 py-1 rounded bg-slate-800/80 hover:bg-slate-700/80 text-slate-300 hover:text-white transition-colors border border-slate-700/60 text-[11px]"
            title="Copy snippet"
          >
            {copied ? (
              <>
                <Check className="w-3.5 h-3.5 text-emerald-400" />
                <span className="text-emerald-400">Copied</span>
              </>
            ) : (
              <>
                <Copy className="w-3.5 h-3.5" />
                <span>Copy</span>
              </>
            )}
          </button>
        </div>
      </div>

      {/* Code Area */}
      <div className="p-4 overflow-x-auto text-slate-200 leading-relaxed font-mono selection:bg-sky-500/30">
        <pre>{code}</pre>
      </div>
    </div>
  );
};

export const SdkDocsView: React.FC = () => {
  const [activeCategory, setActiveCategory] = useState<
    'all' | 'table' | 'query' | 'save' | 'var' | 'spark' | 'guide'
  >('all');
  const [searchQuery, setSearchQuery] = useState('');

  const sections = [
    {
      id: 'var',
      title: 'obelisk.var(key, default=None)',
      category: 'var',
      icon: Key,
      badge: 'Zero-Knowledge Vault',
      desc: 'Retrieves plaintext configuration or automatically decrypts AES-128 secrets in-memory from the centralized SQLite vault.',
      snippetTitle: 'DEVELOPER QUICKSTART: CONSUMING VARIABLES IN NOTEBOOKS & PIPELINES',
      code: `import obelisk

# Retrieve standard plaintext configuration
env_mode = obelisk.var("ENV", default="production")
batch_chunk = int(obelisk.var("BATCH_SIZE", default="1000"))

# Retrieve zero-knowledge AES-128 secrets (auto-decrypted in memory)
slack_webhook = obelisk.var("SLACK_WEBHOOK_URL")
api_key = obelisk.var("OPENAI_API_KEY")

print(f"Running in {env_mode} with batch size {batch_chunk}")`,
    },
    {
      id: 'table',
      title: 'obelisk.table(identifier)',
      category: 'table',
      icon: Database,
      badge: 'Lakehouse & Remote Tables',
      desc: 'Resolves any Lakehouse Parquet dataset or registered external database table (PostgreSQL, Supabase, MySQL). Returns a TableWrapper with zero-copy conversion methods.',
      snippetTitle: 'DATA ACCESS: LOAD LAKEHOUSE & EXTERNAL TABLES',
      code: `import obelisk

# 1. Access by Canonical 3-tier FQN: lakehouse.<folder>.<table>
tbl_venues = obelisk.table("lakehouse.bronze.venues")

# 2. Access external connected database: <catalog>.<schema>.<table>
tbl_remote = obelisk.table("supabase.public.audit_log_entries")

# 3. Access flat or bare 1-tier table (auto-discovered across lakehouse root & subfolders):
tbl_orders = obelisk.table("orders")

# Zero-Copy Engine Conversions:
df_polars = tbl_venues.to_polars()   # Lightning-fast Polars DataFrame
df_arrow  = tbl_venues.to_arrow()    # Apache Arrow Table (zero-copy memory)
df_pandas = tbl_venues.to_df()       # Standard Pandas DataFrame
duck_rel  = tbl_venues.to_duckdb()   # DuckDB PyRelation for chaining queries

print(f"Loaded {len(df_polars):,} rows across {len(df_polars.columns)} columns.")`,
    },
    {
      id: 'query',
      title: 'obelisk.query(sql) & obelisk.query_arrow(sql)',
      category: 'query',
      icon: Zap,
      badge: 'DuckDB In-Process Engine',
      desc: 'Executes analytical SQL directly against local Snappy Parquet files and external database sources. DuckDB handles joins, filtering, and aggregations at native NVMe memory speed.',
      snippetTitle: 'FEDERATED SQL: CROSS-SOURCE ANALYSIS IN NOTEBOOKS',
      code: `import obelisk

# Execute cross-source federated queries combining Lakehouse Parquet with remote databases:
sql = """
SELECT 
    o.order_id,
    o.customer_id,
    o.amount,
    u.full_name,
    u.email
FROM lakehouse.bronze.orders o
JOIN supabase.public.users u ON o.customer_id = u.id
WHERE o.amount > 250.0
ORDER BY o.amount DESC
LIMIT 50;
"""

# Returns a Polars DataFrame
df_result = obelisk.query(sql)

# Or return Apache Arrow table for streaming pipelines
arrow_result = obelisk.query_arrow("SELECT * FROM lakehouse.bronze.fact_news_articles LIMIT 500")

display(df_result)`,
    },
    {
      id: 'save',
      title: 'obelisk.save(df, target, sync_postgres=True)',
      category: 'save',
      icon: Layers,
      badge: 'Atomic Lakehouse Writer',
      desc: 'Atomically serializes Polars, Pandas, Arrow, or PySpark DataFrames into Snappy-compressed Parquet. Synchronizes metadata in data/obelisk.db and optionally syncs to PostgreSQL.',
      snippetTitle: 'PERSISTENCE: ATOMIC PARQUET WRITING & CATALOG SYNC',
      code: `import obelisk
import polars as pl

# 1. Transform or aggregate your analytical dataset:
df_summary = obelisk.table("orders").to_polars().group_by("region").agg([
    pl.col("amount").sum().alias("total_revenue"),
    pl.col("order_id").count().alias("order_count")
])

# 2. Persist to a custom lakehouse folder:
# Saves atomically to: data/lakehouse/analytics/daily_revenue.parquet
res = obelisk.save(df_summary, "analytics.daily_revenue")

# 3. Or save flat in the lakehouse root:
# Saves atomically to: data/lakehouse/quick_export.parquet
res_flat = obelisk.save(df_summary, "quick_export")

print(f"Status: {res['status']} • Wrote {res['num_rows']} rows to {res['file_path']}")`,
    },
    {
      id: 'spark',
      title: 'obelisk.spark()',
      category: 'spark',
      icon: Cpu,
      badge: 'Dynamic Hardware-Aware PySpark',
      desc: 'Initializes a PySpark session configured dynamically with ResourceAdvisor CPU and RAM caps to prevent host OOM crashes. Seamlessly connects to remote Spark clusters via SPARK_REMOTE_URL.',
      snippetTitle: 'BIG DATA ENGINE: DYNAMIC HARDWARE-AWARE PYSPARK',
      code: `import obelisk

# Initializes PySpark locally with memory caps computed by Resource Advisor (e.g. 4GB cap)
# Or connects automatically to remote Spark cluster if SPARK_REMOTE_URL is set:
spark = obelisk.spark()

# Read from Zero-Cost STORAGE_URI
df_spark = spark.read.parquet("data/lakehouse/bronze/events.parquet")

# Distributed aggregation
df_aggregated = df_spark.groupBy("event_type").count()

# Persist output back into Obelisk Lakehouse
obelisk.save(df_aggregated, "gold.aggregated_events")`,
    },
  ];

  const filteredSections = sections.filter((s) => {
    const matchesCat = activeCategory === 'all' || s.category === activeCategory;
    const matchesSearch =
      searchQuery === '' ||
      s.title.toLowerCase().includes(searchQuery.toLowerCase()) ||
      s.desc.toLowerCase().includes(searchQuery.toLowerCase()) ||
      s.code.toLowerCase().includes(searchQuery.toLowerCase());
    return matchesCat && matchesSearch;
  });

  return (
    <div className="space-y-8 animate-in fade-in duration-200">
      {/* Hero Banner */}
      <div className="relative overflow-hidden rounded-2xl bg-gradient-to-br from-slate-900 via-indigo-950 to-slate-900 text-white p-6 md:p-8 border border-slate-800 shadow-xl">
        <div className="absolute top-0 right-0 w-96 h-96 bg-sky-500/10 rounded-full blur-3xl pointer-events-none" />
        <div className="relative z-10 max-w-3xl space-y-3">
          <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-sky-500/10 border border-sky-400/20 text-sky-300 text-xs font-mono">
            <Sparkles className="w-3.5 h-3.5" />
            <span>Developer Reference & Platform Architecture</span>
          </div>
          <h2 className="text-2xl md:text-3xl font-bold tracking-tight text-white font-mono">
            Obelisk SDK &amp; Platform Guide
          </h2>
          <p className="text-xs md:text-sm text-slate-300 leading-relaxed">
            The <code className="text-sky-300 font-bold">obelisk</code> Python client is a zero-configuration data plane SDK built directly into every Marimo notebook, dbt pipeline, and Python environment in the platform. Query, transform, and persist data at native NVMe speed.
          </p>
        </div>
      </div>

      {/* Filter and Search Navigation Bar */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-3 bg-white p-3 rounded-xl border border-slate-200 shadow-2xs">
        {/* Category Pills */}
        <div className="flex flex-wrap items-center gap-1.5 text-xs">
          {[
            { id: 'all', label: 'All Functions' },
            { id: 'table', label: 'obelisk.table' },
            { id: 'query', label: 'obelisk.query' },
            { id: 'save', label: 'obelisk.save' },
            { id: 'var', label: 'obelisk.var' },
            { id: 'spark', label: 'obelisk.spark' },
            { id: 'guide', label: 'Platform Guide' },
          ].map((cat) => (
            <button
              key={cat.id}
              onClick={() => setActiveCategory(cat.id as any)}
              className={`px-3 py-1.5 rounded-lg font-mono transition-all ${
                activeCategory === cat.id
                  ? 'bg-slate-900 text-white font-semibold shadow-xs'
                  : 'bg-slate-50 text-slate-600 hover:bg-slate-100 hover:text-slate-900'
              }`}
            >
              {cat.label}
            </button>
          ))}
        </div>

        {/* Search Input */}
        <div className="relative min-w-[240px]">
          <Search className="w-3.5 h-3.5 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Search SDK functions or keywords..."
            className="w-full pl-9 pr-3 py-1.5 rounded-lg border border-slate-200 bg-slate-50 text-xs text-slate-800 placeholder-slate-400 focus:outline-hidden focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500 font-mono transition-all"
          />
        </div>
      </div>

      {/* SDK Function Documentation Cards */}
      {activeCategory !== 'guide' && (
        <div className="space-y-6">
          {filteredSections.map((sec) => {
            const Icon = sec.icon;
            return (
              <div
                key={sec.id}
                className="bg-white rounded-xl border border-slate-200 p-6 shadow-xs space-y-4 hover:border-slate-300 transition-all"
              >
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-100 pb-3">
                  <div className="flex items-center gap-2.5">
                    <div className="p-2 rounded-lg bg-sky-50 border border-sky-100 text-sky-700">
                      <Icon className="w-4 h-4" />
                    </div>
                    <div>
                      <h3 className="text-sm font-bold text-slate-900 font-mono">{sec.title}</h3>
                      <p className="text-xs text-slate-500 mt-0.5">{sec.desc}</p>
                    </div>
                  </div>
                  <span className="self-start sm:self-center px-2.5 py-1 rounded-md bg-slate-100 text-slate-700 font-mono text-[11px] font-semibold border border-slate-200">
                    {sec.badge}
                  </span>
                </div>

                {/* Dark Code Block */}
                <CodeSnippet
                  title={sec.snippetTitle}
                  badge="obelisk Python SDK"
                  code={sec.code}
                />
              </div>
            );
          })}
        </div>
      )}

      {/* Platform Architecture & How-To Guide */}
      {(activeCategory === 'all' || activeCategory === 'guide') && (
        <div className="space-y-6 pt-4">
          <div className="flex items-center gap-2 border-b border-slate-200 pb-3">
            <BookOpen className="w-5 h-5 text-indigo-600" />
            <h3 className="text-base font-bold text-slate-900 font-mono uppercase tracking-wider">
              How to Use the Obelisk Platform: End-to-End Workflow
            </h3>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
            {/* Step 1: Ingestion */}
            <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs space-y-3 hover:border-indigo-300 transition-all flex flex-col justify-between">
              <div className="space-y-2">
                <div className="w-8 h-8 rounded-lg bg-indigo-50 border border-indigo-100 text-indigo-700 flex items-center justify-center font-mono font-bold text-xs">
                  01
                </div>
                <h4 className="text-sm font-bold text-slate-900 font-mono">1-Click Ingest to Lakehouse</h4>
                <p className="text-xs text-slate-600 leading-relaxed">
                  In <strong>Data Catalog</strong>, expand any connected PostgreSQL, MySQL, MongoDB, or Oracle source. Click <strong>[ Ingest ]</strong> on any table to stream Snappy Parquet directly into <code className="font-mono text-indigo-700 bg-indigo-50 px-1 py-0.5 rounded">data/lakehouse/</code>.
                </p>
              </div>
              <div className="pt-2 text-[11px] font-mono text-indigo-600 font-semibold flex items-center gap-1">
                <span>Supports snapshot &amp; incremental watermarks</span>
              </div>
            </div>

            {/* Step 2: Query Studio */}
            <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs space-y-3 hover:border-sky-300 transition-all flex flex-col justify-between">
              <div className="space-y-2">
                <div className="w-8 h-8 rounded-lg bg-sky-50 border border-sky-100 text-sky-700 flex items-center justify-center font-mono font-bold text-xs">
                  02
                </div>
                <h4 className="text-sm font-bold text-slate-900 font-mono">Monaco Query Studio</h4>
                <p className="text-xs text-slate-600 leading-relaxed">
                  Run ad-hoc analytical queries using in-process DuckDB. Join Lakehouse Parquet files directly with external databases. Features SQL autocomplete, virtualized 60 FPS DataGrid, and binary CSV / Parquet exports.
                </p>
              </div>
              <div className="pt-2 text-[11px] font-mono text-sky-600 font-semibold flex items-center gap-1">
                <span>Shortcut: ⌘↵ (Cmd + Enter) to execute</span>
              </div>
            </div>

            {/* Step 3: Notebooks */}
            <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs space-y-3 hover:border-purple-300 transition-all flex flex-col justify-between">
              <div className="space-y-2">
                <div className="w-8 h-8 rounded-lg bg-purple-50 border border-purple-100 text-purple-700 flex items-center justify-center font-mono font-bold text-xs">
                  03
                </div>
                <h4 className="text-sm font-bold text-slate-900 font-mono">Marimo Reactive Notebooks</h4>
                <p className="text-xs text-slate-600 leading-relaxed">
                  Pure Git-controlled <code className="font-mono text-purple-700 bg-purple-50 px-1 py-0.5 rounded">.py</code> reactive notebooks. Click <strong>[ Analyze in Notebook ]</strong> from any table preview to launch an interactive session with <code className="font-mono text-purple-700 bg-purple-50 px-1 py-0.5 rounded">import obelisk</code> pre-loaded.
                </p>
              </div>
              <div className="pt-2 text-[11px] font-mono text-purple-600 font-semibold flex items-center gap-1">
                <span>Runs headlessly on :2718 in /notebooks</span>
              </div>
            </div>

            {/* Step 4: dbt Studio */}
            <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs space-y-3 hover:border-amber-300 transition-all flex flex-col justify-between">
              <div className="space-y-2">
                <div className="w-8 h-8 rounded-lg bg-amber-50 border border-amber-100 text-amber-700 flex items-center justify-center font-mono font-bold text-xs">
                  04
                </div>
                <h4 className="text-sm font-bold text-slate-900 font-mono">dbt Studio &amp; Lineage</h4>
                <p className="text-xs text-slate-600 leading-relaxed">
                  Transform raw files into clean relational staging and analytical data marts. Inspect the visual DAG lineage graph, run <code className="font-mono text-amber-700 bg-amber-50 px-1 py-0.5 rounded">dbt run</code>, compile SQL, and test data quality assertions in real time.
                </p>
              </div>
              <div className="pt-2 text-[11px] font-mono text-amber-600 font-semibold flex items-center gap-1">
                <span>Models stored in git_repo/dbt_project</span>
              </div>
            </div>

            {/* Step 5: Orchestration */}
            <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs space-y-3 hover:border-emerald-300 transition-all flex flex-col justify-between">
              <div className="space-y-2">
                <div className="w-8 h-8 rounded-lg bg-emerald-50 border border-emerald-100 text-emerald-700 flex items-center justify-center font-mono font-bold text-xs">
                  05
                </div>
                <h4 className="text-sm font-bold text-slate-900 font-mono">Pipelines &amp; Cron Engine</h4>
                <p className="text-xs text-slate-600 leading-relaxed">
                  Define automated recurring ingestion and transformation schedules using standard 5-part cron expressions. Or discover Python DAGs in <code className="font-mono text-emerald-700 bg-emerald-50 px-1 py-0.5 rounded">git_repo/dags/</code> with live streaming run logs.
                </p>
              </div>
              <div className="pt-2 text-[11px] font-mono text-emerald-600 font-semibold flex items-center gap-1">
                <span>Integrated Dagster daemon &amp; Cron runner</span>
              </div>
            </div>

            {/* Step 6: Vault & Variables */}
            <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs space-y-3 hover:border-rose-300 transition-all flex flex-col justify-between">
              <div className="space-y-2">
                <div className="w-8 h-8 rounded-lg bg-rose-50 border border-rose-100 text-rose-700 flex items-center justify-center font-mono font-bold text-xs">
                  06
                </div>
                <h4 className="text-sm font-bold text-slate-900 font-mono">Security &amp; Connection Vault</h4>
                <p className="text-xs text-slate-600 leading-relaxed">
                  Enterprise credentials and API keys are protected by an in-memory AES-128 Fernet crypto vault keyed by <code className="font-mono text-rose-700 bg-rose-50 px-1 py-0.5 rounded">OBELISK_MASTER_KEY</code>. Secrets are never exposed in plaintext logs or serialized into git commits.
                </p>
              </div>
              <div className="pt-2 text-[11px] font-mono text-rose-600 font-semibold flex items-center gap-1">
                <span>Zero-Knowledge Decryption in Memory</span>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
