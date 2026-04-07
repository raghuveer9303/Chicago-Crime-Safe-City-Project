import TopNav from "../components/TopNav";

// ──────────────────────────────────────────────────────────────────────────────
// Icon primitives – tiny inline SVGs so no extra deps are needed
// ──────────────────────────────────────────────────────────────────────────────
function Icon({ d, size = 14, className = "" }: { d: string; size?: number; className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      width={size}
      height={size}
      fill="none"
      stroke="currentColor"
      strokeWidth={1.8}
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
    >
      <path d={d} />
    </svg>
  );
}

const ICONS = {
  db: "M4 7c0-1.657 3.582-3 8-3s8 1.343 8 3v10c0 1.657-3.582 3-8 3S4 18.657 4 17V7z M4 12c0 1.657 3.582 3 8 3s8-1.343 8-3 M4 7c0 1.657 3.582 3 8 3s8-1.343 8-3",
  ml: "M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5",
  graph: "M12 3a2 2 0 1 0 0 4 2 2 0 0 0 0-4zm-5 9a2 2 0 1 0 0 4 2 2 0 0 0 0-4zm10 0a2 2 0 1 0 0 4 2 2 0 0 0 0-4zM7 14l5-2m5 0l-5-2",
  spark: "M13 10V3L4 14h7v7l9-11h-7z",
  airflow: "M9 12l2 2 4-4M7.835 4.697a3.42 3.42 0 001.946-.806 3.42 3.42 0 014.438 0 3.42 3.42 0 001.946.806 3.42 3.42 0 013.138 3.138 3.42 3.42 0 00.806 1.946 3.42 3.42 0 010 4.438 3.42 3.42 0 00-.806 1.946 3.42 3.42 0 01-3.138 3.138 3.42 3.42 0 00-1.946.806 3.42 3.42 0 01-4.438 0 3.42 3.42 0 00-1.946-.806 3.42 3.42 0 01-3.138-3.138 3.42 3.42 0 00-.806-1.946 3.42 3.42 0 010-4.438 3.42 3.42 0 00.806-1.946 3.42 3.42 0 013.138-3.138z",
  bq: "M3 3h18v18H3V3zm4 4v10m4-10v10m4-10v10M3 9h18M3 15h18",
  api: "M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z",
  chat: "M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z",
  arrow: "M17 8l4 4-4 4M21 12H3",
  arrowDown: "M12 5v14M19 12l-7 7-7-7",
  minio: "M4 4h16v16H4V4zm4 4h3v3H8V8zm5 0h3v3h-3V8zm-5 5h3v3H8v-3zm5 0h3v3h-3v-3z",
  neo4j: "M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 3a7 7 0 110 14A7 7 0 0112 5zm2 3a2 2 0 11-4 0 2 2 0 014 0zm-5 5a2 2 0 11-4 0 2 2 0 014 0zm8 0a2 2 0 11-4 0 2 2 0 014 0z",
  xgboost: "M12 3v18M3 12h18M6.343 6.343l11.314 11.314M17.657 6.343L6.343 17.657",
  check: "M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z",
};

// ──────────────────────────────────────────────────────────────────────────────
// Small reusable components
// ──────────────────────────────────────────────────────────────────────────────

function Badge({ label, color = "primary" }: { label: string; color?: "primary" | "accent" | "blue" | "green" | "purple" | "orange" }) {
  const map: Record<string, string> = {
    primary: "bg-[hsl(23_84%_62%/0.15)] text-[hsl(23_84%_72%)] border-[hsl(23_84%_62%/0.3)]",
    accent:  "bg-[hsl(43_72%_63%/0.15)] text-[hsl(43_72%_73%)] border-[hsl(43_72%_63%/0.3)]",
    blue:    "bg-[hsl(210_80%_60%/0.15)] text-[hsl(210_80%_72%)] border-[hsl(210_80%_60%/0.3)]",
    green:   "bg-[hsl(148_52%_57%/0.15)] text-[hsl(148_52%_67%)] border-[hsl(148_52%_57%/0.3)]",
    purple:  "bg-[hsl(270_60%_65%/0.15)] text-[hsl(270_60%_75%)] border-[hsl(270_60%_65%/0.3)]",
    orange:  "bg-[hsl(30_90%_60%/0.15)] text-[hsl(30_90%_72%)] border-[hsl(30_90%_60%/0.3)]",
  };
  return (
    <span className={`inline-flex items-center rounded-md border px-2 py-0.5 text-[10px] font-semibold tracking-wide ${map[color]}`}>
      {label}
    </span>
  );
}

function NodeBox({
  icon,
  title,
  subtitle,
  tags = [],
  accent = false,
}: {
  icon: string;
  title: string;
  subtitle: string;
  tags?: { label: string; color?: "primary" | "accent" | "blue" | "green" | "purple" | "orange" }[];
  accent?: boolean;
}) {
  return (
    <div
      className={`rounded-xl border p-3 flex flex-col gap-1.5 transition-all hover:scale-[1.02] hover:shadow-lg ${
        accent
          ? "border-[hsl(23_84%_62%/0.5)] bg-[hsl(23_84%_62%/0.08)] shadow-[0_0_20px_hsl(23_84%_62%/0.1)]"
          : "border-[hsl(217_16%_28%/0.7)] bg-[hsl(216_18%_13%/0.7)]"
      }`}
    >
      <div className="flex items-center gap-2">
        <span
          className={`inline-flex h-7 w-7 shrink-0 items-center justify-center rounded-lg ${
            accent ? "bg-[hsl(23_84%_62%/0.2)] text-[hsl(23_84%_72%)]" : "bg-[hsl(218_18%_19%)] text-[hsl(34_30%_75%)]"
          }`}
        >
          <Icon d={icon} size={13} />
        </span>
        <span className="text-xs font-semibold text-[hsl(34_30%_94%)] leading-tight">{title}</span>
      </div>
      <p className="text-[11px] text-[hsl(34_13%_66%)] leading-snug">{subtitle}</p>
      {tags.length > 0 && (
        <div className="flex flex-wrap gap-1 mt-0.5">
          {tags.map((t) => (
            <Badge key={t.label} label={t.label} color={t.color} />
          ))}
        </div>
      )}
    </div>
  );
}

function ArrowV() {
  return (
    <div className="flex justify-center py-1 text-[hsl(23_84%_62%/0.5)]">
      <Icon d={ICONS.arrowDown} size={16} />
    </div>
  );
}

function ArrowH() {
  return (
    <div className="flex items-center justify-center px-1 text-[hsl(23_84%_62%/0.5)]">
      <Icon d={ICONS.arrow} size={16} />
    </div>
  );
}

function SectionTitle({ label, icon, color = "primary" }: { label: string; icon: string; color?: "primary" | "blue" | "green" | "purple" }) {
  const map: Record<string, string> = {
    primary: "text-[hsl(23_84%_72%)] bg-[hsl(23_84%_62%/0.15)] border-[hsl(23_84%_62%/0.3)]",
    blue:    "text-[hsl(210_80%_72%)] bg-[hsl(210_80%_60%/0.15)] border-[hsl(210_80%_60%/0.3)]",
    green:   "text-[hsl(148_52%_67%)] bg-[hsl(148_52%_57%/0.15)] border-[hsl(148_52%_57%/0.3)]",
    purple:  "text-[hsl(270_60%_75%)] bg-[hsl(270_60%_65%/0.15)] border-[hsl(270_60%_65%/0.3)]",
  };
  return (
    <div className={`inline-flex items-center gap-2 rounded-lg border px-3 py-1.5 text-xs font-bold tracking-widest uppercase ${map[color]}`}>
      <Icon d={icon} size={12} />
      {label}
    </div>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// Pipeline column
// ──────────────────────────────────────────────────────────────────────────────

function PillarCard({
  children,
  glow = "orange",
}: {
  children: React.ReactNode;
  glow?: "orange" | "blue" | "green";
}) {
  const glowMap: Record<string, string> = {
    orange: "hover:shadow-[0_0_40px_hsl(23_84%_62%/0.12)]",
    blue:   "hover:shadow-[0_0_40px_hsl(210_80%_60%/0.12)]",
    green:  "hover:shadow-[0_0_40px_hsl(148_52%_57%/0.12)]",
  };
  return (
    <div
      className={`flex flex-col gap-2 rounded-2xl border border-[hsl(217_16%_28%/0.6)] bg-[hsl(216_18%_13%/0.5)] p-4 backdrop-blur transition-shadow ${glowMap[glow]}`}
      style={{ backdropFilter: "blur(16px)", WebkitBackdropFilter: "blur(16px)" }}
    >
      {children}
    </div>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// Pipeline DAG step strip
// ──────────────────────────────────────────────────────────────────────────────

function DagStep({ label, sub, active }: { label: string; sub: string; active?: boolean }) {
  return (
    <div
      className={`flex flex-col items-center rounded-lg border px-2 py-2 min-w-[80px] text-center ${
        active
          ? "border-[hsl(23_84%_62%/0.6)] bg-[hsl(23_84%_62%/0.12)] text-[hsl(23_84%_72%)]"
          : "border-[hsl(217_16%_28%/0.6)] bg-[hsl(218_18%_16%/0.8)] text-[hsl(34_13%_66%)]"
      }`}
    >
      <span className="text-[10px] font-bold">{label}</span>
      <span className="text-[9px] opacity-70 leading-tight mt-0.5">{sub}</span>
    </div>
  );
}

function DagArrow() {
  return <div className="text-[hsl(23_84%_62%/0.45)] text-xs font-bold self-center">→</div>;
}

// ──────────────────────────────────────────────────────────────────────────────
// Main page
// ──────────────────────────────────────────────────────────────────────────────

export default function ArchitecturePage() {
  return (
    <div className="flex min-h-dvh flex-col">
      <TopNav />

      <main className="flex-1 px-4 sm:px-6 pb-10 overflow-x-hidden">
        <div className="max-w-7xl mx-auto">

          {/* ── Page header ─────────────────────────────────────────────── */}
          <div className="mt-6 mb-8">
            <div className="flex flex-wrap items-center gap-3 mb-3">
              <h1 className="text-xl sm:text-2xl font-semibold tracking-tight">
                System Architecture
              </h1>
              <Badge label="Chicago Crime Analytics" color="primary" />
              <Badge label="v2.0" color="accent" />
            </div>
            <p className="text-sm text-[hsl(34_13%_66%)] max-w-3xl leading-relaxed">
              End-to-end data engineering stack ingesting daily Chicago crime data from the Socrata API
              through a bronze→silver→gold Lakehouse, powering three distinct consumer surfaces:
              an <strong className="text-[hsl(34_30%_80%)]">XGBoost safety-score ML pipeline</strong>,
              a <strong className="text-[hsl(34_30%_80%)]">Graph RAG chat system</strong>, and
              a <strong className="text-[hsl(34_30%_80%)]">BigQuery dimensional dashboard</strong>.
            </p>
          </div>

          {/* ── Top-level data flow: Ingestion → Lakehouse ─────────────── */}
          <section className="mb-6">
            <div
              className="rounded-2xl border border-[hsl(217_16%_28%/0.6)] bg-[hsl(216_18%_13%/0.4)] p-5"
              style={{ backdropFilter: "blur(16px)" }}
            >
              <div className="mb-4 flex items-center gap-2">
                <SectionTitle label="Data Ingestion & Lakehouse" icon={ICONS.airflow} color="blue" />
                <span className="text-[10px] text-[hsl(34_13%_56%)]">Daily @ 00:30 UTC · 10-day lag window</span>
              </div>

              {/* Horizontal flow */}
              <div className="flex flex-wrap items-center gap-1 overflow-x-auto pb-1 no-scrollbar">
                <DagStep label="Socrata API" sub="data.cityofchicago.org" />
                <DagArrow />
                <DagStep label="Airflow DAG" sub="chicago_crime_raw_to_minio" active />
                <DagArrow />
                <DagStep label="MinIO" sub="Bronze Parquet" />
                <DagArrow />
                <DagStep label="Great Expectations" sub="Bronze QC" />
                <DagArrow />
                <DagStep label="Spark" sub="Bronze → Silver" active />
                <DagArrow />
                <DagStep label="Iceberg" sub="Silver Table (S3A)" />
                <DagArrow />
                <DagStep label="Silver → Gold" sub="Fan-out 3 paths" active />
              </div>

              {/* Metadata strip */}
              <div className="mt-4 grid grid-cols-2 sm:grid-cols-4 gap-3">
                {[
                  { k: "Source Dataset", v: "ijzp-q8t2 (CPD)" },
                  { k: "Ingest Lag", v: "10 days (API)" },
                  { k: "Storage", v: "MinIO / S3A" },
                  { k: "Table Format", v: "Apache Iceberg 1.4.3" },
                ].map(({ k, v }) => (
                  <div key={k} className="rounded-lg border border-[hsl(217_16%_28%/0.5)] bg-[hsl(218_18%_16%/0.5)] px-3 py-2">
                    <div className="text-[10px] text-[hsl(34_13%_56%)] uppercase tracking-wider">{k}</div>
                    <div className="text-xs font-semibold text-[hsl(34_30%_88%)] mt-0.5">{v}</div>
                  </div>
                ))}
              </div>
            </div>
          </section>

          {/* ── Three Pillars ────────────────────────────────────────────── */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-6">

            {/* ── PILLAR 1: ML Pipeline ─────────────────────────────────── */}
            <PillarCard glow="orange">
              <SectionTitle label="ML Pipeline" icon={ICONS.ml} color="primary" />

              <p className="text-[11px] text-[hsl(34_13%_60%)] leading-snug">
                Horizon-aware XGBoost model trained weekly and scored daily via Airflow &amp; Spark.
                MLflow tracks experiments and gates promotion to Production.
              </p>

              {/* Daily scoring path */}
              <div className="mt-1 text-[10px] text-[hsl(23_84%_62%)] font-semibold tracking-wider uppercase">
                Daily DAG — chicago_crime_ml_daily_dag
              </div>

              <div className="flex flex-col gap-1.5">
                <NodeBox
                  icon={ICONS.airflow}
                  title="build_area_risk_features"
                  subtitle="SparkSubmit: reads Iceberg silver, computes rolling crime-rate features (7d / 28d / 90d) per community area."
                  tags={[{ label: "SparkSubmit", color: "blue" }, { label: "Iceberg Gold", color: "accent" }]}
                />
                <ArrowV />
                <NodeBox
                  icon={ICONS.check}
                  title="production_model_guard"
                  subtitle="ShortCircuit: queries MLflow registry — skips score & load tasks until a Production model exists."
                  tags={[{ label: "ShortCircuit", color: "purple" }, { label: "MLflow", color: "orange" }]}
                  accent
                />
                <ArrowV />
                <NodeBox
                  icon={ICONS.xgboost}
                  title="score_area_risk"
                  subtitle="Loads models:/chicago_crime_area_risk/Production. Forward-fills latest features across ds→ds+10 with explicit horizon feature (h=0..10)."
                  tags={[{ label: "XGBoost", color: "primary" }, { label: "Poisson CI", color: "green" }]}
                />
                <ArrowV />
                <NodeBox
                  icon={ICONS.db}
                  title="load_studio_postgres"
                  subtitle="Writes safety_score, predicted_count_low/high, top-3 crime categories to studio_area_snapshot for the FastAPI map endpoint."
                  tags={[{ label: "PostgreSQL", color: "blue" }, { label: "FastAPI", color: "accent" }]}
                />
              </div>

              <div className="mt-2 text-[10px] text-[hsl(148_52%_57%)] font-semibold tracking-wider uppercase">
                Weekly DAG — chicago_crime_ml_weekly_dag
              </div>
              <div className="flex flex-col gap-1.5">
                <div className="flex flex-wrap items-center gap-1">
                  <DagStep label="evaluate" sub="RMSE / MAE" active />
                  <DagArrow />
                  <DagStep label="drift" sub="PSI detect" />
                  <DagArrow />
                  <DagStep label="train" sub="392d window" active />
                  <DagArrow />
                  <DagStep label="promote" sub="if RMSE ↓" />
                </div>
                <p className="text-[10px] text-[hsl(34_13%_56%)] leading-snug">
                  Train window: -392d to -92d. Val: -91d to -61d. Test: -60d to -30d.
                  Promotion guarded by sparkml artifact completeness check on MinIO.
                </p>
              </div>

              {/* MLflow callout */}
              <div className="mt-1 rounded-xl border border-[hsl(23_84%_62%/0.3)] bg-[hsl(23_84%_62%/0.07)] p-3">
                <div className="text-[10px] font-bold text-[hsl(23_84%_72%)] uppercase tracking-wider mb-1.5">MLflow Registry</div>
                <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[10px]">
                  {[
                    ["Experiment", "crime_risk_area"],
                    ["Model", "chicago_crime_area_risk"],
                    ["Stage gate", "Production alias"],
                    ["Metric", "test_rmse (lower→promote)"],
                    ["Artifact", "spark_xgboost_pipeline"],
                    ["DFS tmpdir", "s3a://…/_mlflow_spark_tmp"],
                  ].map(([k, v]) => (
                    <div key={k}>
                      <span className="text-[hsl(34_13%_56%)]">{k}: </span>
                      <span className="text-[hsl(34_30%_82%)] font-medium">{v}</span>
                    </div>
                  ))}
                </div>
              </div>
            </PillarCard>

            {/* ── PILLAR 2: Graph RAG ───────────────────────────────────── */}
            <PillarCard glow="blue">
              <SectionTitle label="Graph RAG" icon={ICONS.graph} color="purple" />

              <p className="text-[11px] text-[hsl(34_13%_60%)] leading-snug">
                Daily crime data is loaded into Neo4j as a property graph. LangGraph orchestrates
                a Mistral-powered RAG agent that converts natural language questions into Cypher
                queries against the knowledge graph.
              </p>

              <div className="mt-1 text-[10px] text-[hsl(270_60%_75%)] font-semibold tracking-wider uppercase">
                Ingestion DAG — chicago_crime_neo4j_maintenance_and_load
              </div>

              <div className="flex flex-col gap-1.5">
                <NodeBox
                  icon={ICONS.check}
                  title="ensure_neo4j_indexes"
                  subtitle="BashOperator: idempotent Cypher DDL — unique constraints on CrimeIncident.id, Date.value, Location.beat, CrimeType.primary. Runs before any load."
                  tags={[{ label: "BashOp", color: "blue" }, { label: "Cypher DDL", color: "purple" }]}
                />
                <ArrowV />
                <NodeBox
                  icon={ICONS.spark}
                  title="load_neo4j_nodes_and_edges"
                  subtitle="SparkSubmit reads Iceberg silver via chicago_crime.chicago_crime_silver and writes nodes (CrimeIncident, Date, Location, CrimeType, Beat) and edges via neo4j-connector-apache-spark."
                  tags={[{ label: "SparkSubmit", color: "blue" }, { label: "Neo4j Connector", color: "purple" }]}
                  accent
                />
                <ArrowV />
                <NodeBox
                  icon={ICONS.db}
                  title="prune_old_neo4j_data"
                  subtitle="Deletes nodes with date older than 365 days — maintains a rolling 1-year knowledge window to keep graph traversal performant."
                  tags={[{ label: "BashOp", color: "accent" }, { label: "365d window", color: "green" }]}
                />
              </div>

              {/* Graph schema */}
              <div className="mt-2 rounded-xl border border-[hsl(270_60%_65%/0.3)] bg-[hsl(270_60%_65%/0.07)] p-3">
                <div className="text-[10px] font-bold text-[hsl(270_60%_75%)] uppercase tracking-wider mb-2">Neo4j Property Graph Schema</div>
                <div className="grid grid-cols-2 gap-1.5 text-[10px]">
                  {[
                    { label: "CrimeIncident", sub: "id, date, arrest, domestic" },
                    { label: "Date", sub: "value, year, month, day_of_week" },
                    { label: "Location", sub: "beat, district, community_area" },
                    { label: "CrimeType", sub: "primary_type, description" },
                  ].map(({ label, sub }) => (
                    <div key={label} className="rounded-lg border border-[hsl(270_60%_65%/0.25)] bg-[hsl(270_60%_65%/0.08)] px-2 py-1.5">
                      <div className="font-bold text-[hsl(270_60%_80%)]">{label}</div>
                      <div className="text-[hsl(34_13%_56%)] leading-tight">{sub}</div>
                    </div>
                  ))}
                </div>
                <div className="mt-2 text-[10px] text-[hsl(34_13%_56%)] space-y-0.5">
                  <div><span className="text-[hsl(270_60%_75%)]">OCCURRED_ON</span> · <span className="text-[hsl(270_60%_75%)]">AT_LOCATION</span> · <span className="text-[hsl(270_60%_75%)]">OF_TYPE</span></div>
                  <div><span className="text-[hsl(270_60%_75%)]">IN_BEAT</span> · <span className="text-[hsl(270_60%_75%)]">IN_DISTRICT</span></div>
                </div>
              </div>

              {/* LangGraph / RAG API */}
              <div className="mt-2 text-[10px] text-[hsl(270_60%_75%)] font-semibold tracking-wider uppercase">
                RAG Chat API — FastAPI + LangGraph
              </div>
              <div className="flex flex-col gap-1.5">
                <NodeBox
                  icon={ICONS.chat}
                  title="LangGraph Agent"
                  subtitle="3-node graph: question_router → cypher_generator → answer_synthesiser. Mistral LLM generates Cypher that is executed against Neo4j bolt://neo4j:7687."
                  tags={[{ label: "LangGraph", color: "purple" }, { label: "Mistral", color: "accent" }]}
                />
                <NodeBox
                  icon={ICONS.api}
                  title="RAG Chatbot API  :8080"
                  subtitle="FastAPI service exposes /chat endpoint consumed by the React frontend. Streams answer chunks with source Cypher for transparency."
                  tags={[{ label: "FastAPI", color: "blue" }, { label: "Streaming", color: "green" }]}
                />
              </div>
            </PillarCard>

            {/* ── PILLAR 3: BigQuery Dashboard ─────────────────────────── */}
            <PillarCard glow="green">
              <SectionTitle label="BQ Dashboard" icon={ICONS.bq} color="green" />

              <p className="text-[11px] text-[hsl(34_13%_60%)] leading-snug">
                Spark builds a star-schema dimensional model in BigQuery (chicago_crime_gold dataset),
                enabling Looker / BI tool analytics across time, location, and crime type dimensions.
              </p>

              <div className="mt-1 text-[10px] text-[hsl(148_52%_67%)] font-semibold tracking-wider uppercase">
                Dimensional DAG — chicago_crime_silver_to_gold_dag
              </div>

              <div className="flex flex-col gap-1.5">
                <NodeBox
                  icon={ICONS.spark}
                  title="build_dim_date / location / crime_type / time_of_day"
                  subtitle="4 parallel SparkSubmit tasks write dimension slices to chicago_crime_gold_temp (staging). Iceberg Silver → BQ via spark-bigquery connector."
                  tags={[{ label: "SparkSubmit", color: "blue" }, { label: "BigQuery", color: "green" }]}
                />
                <ArrowV />
                <NodeBox
                  icon={ICONS.db}
                  title="wait_for_bq_visibility (60s)"
                  subtitle="BashOperator sleep gate prevents read-after-write race between dimension writes and fact reads on BigQuery's eventual-consistency window."
                  tags={[{ label: "BashOp", color: "accent" }]}
                />
                <ArrowV />
                <NodeBox
                  icon={ICONS.bq}
                  title="build_fact_crime_incidents + build_fact_crime_summary"
                  subtitle="Fact tables join Iceberg silver against staging dimensions, writing incident-level and daily-summary facts to BQ staging."
                  tags={[{ label: "Fact Table", color: "primary" }, { label: "Star Schema", color: "green" }]}
                  accent
                />
                <ArrowV />
                <NodeBox
                  icon={ICONS.check}
                  title="merge_gold_temp_to_final"
                  subtitle="Idempotent merge script promotes staging → chicago_crime_gold (final). Deduplicates by process_date to support safe daily re-runs."
                  tags={[{ label: "Idempotent", color: "accent" }, { label: "Gold Layer", color: "green" }]}
                />
                <ArrowV />
                <NodeBox
                  icon={ICONS.check}
                  title="gold_bi_quality_check"
                  subtitle="Great Expectations SparkSubmit validates fact_crime_incidents row counts, referential integrity across all 4 dimensions, and null rates."
                  tags={[{ label: "Great Expectations", color: "purple" }, { label: "trigger: all_done", color: "accent" }]}
                />
              </div>

              {/* Star schema callout */}
              <div className="mt-2 rounded-xl border border-[hsl(148_52%_57%/0.3)] bg-[hsl(148_52%_57%/0.07)] p-3">
                <div className="text-[10px] font-bold text-[hsl(148_52%_67%)] uppercase tracking-wider mb-2">Star Schema — BigQuery</div>
                <div className="grid grid-cols-2 gap-1.5 text-[10px]">
                  {[
                    { label: "dim_date", sub: "year · month · dow · quarter", type: "DIM" },
                    { label: "dim_location", sub: "beat · district · community_area", type: "DIM" },
                    { label: "dim_crime_type", sub: "primary_type · description", type: "DIM" },
                    { label: "dim_time_of_day", sub: "hour · shift · time_bucket", type: "DIM" },
                    { label: "fact_crime_incidents", sub: "FK to all dims · arrest · domestic", type: "FACT" },
                    { label: "fact_crime_summary", sub: "daily rollup · count · arrest_rate", type: "FACT" },
                  ].map(({ label, sub, type }) => (
                    <div key={label} className="rounded-lg border border-[hsl(148_52%_57%/0.2)] bg-[hsl(148_52%_57%/0.06)] px-2 py-1.5">
                      <div className="flex items-center gap-1">
                        <span className={`inline-block rounded px-1 text-[8px] font-bold ${type === "FACT" ? "bg-[hsl(23_84%_62%/0.2)] text-[hsl(23_84%_72%)]" : "bg-[hsl(148_52%_57%/0.2)] text-[hsl(148_52%_67%)]"}`}>{type}</span>
                        <span className="font-bold text-[hsl(34_30%_88%)] truncate">{label}</span>
                      </div>
                      <div className="text-[hsl(34_13%_56%)] mt-0.5 leading-tight">{sub}</div>
                    </div>
                  ))}
                </div>
                <div className="mt-2 grid grid-cols-2 gap-1 text-[10px]">
                  {[
                    { k: "Project", v: "GCP_PROJECT_ID" },
                    { k: "Dataset", v: "chicago_crime_gold" },
                    { k: "Staging", v: "chicago_crime_gold_temp" },
                    { k: "Connector", v: "spark-bigquery 0.36.1" },
                  ].map(({ k, v }) => (
                    <div key={k}>
                      <span className="text-[hsl(34_13%_56%)]">{k}: </span>
                      <span className="text-[hsl(34_30%_82%)] font-medium">{v}</span>
                    </div>
                  ))}
                </div>
              </div>
            </PillarCard>
          </div>

          {/* ── Infrastructure strip ─────────────────────────────────────── */}
          <section>
            <div
              className="rounded-2xl border border-[hsl(217_16%_28%/0.6)] bg-[hsl(216_18%_13%/0.4)] p-5"
              style={{ backdropFilter: "blur(16px)" }}
            >
              <div className="mb-4">
                <SectionTitle label="Shared Infrastructure" icon={ICONS.minio} color="blue" />
              </div>
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
                {[
                  { name: "Apache Airflow", role: "Orchestration", badge: "DAG scheduler", color: "blue" as const },
                  { name: "Apache Spark 3.5", role: "Compute engine", badge: "SparkSubmit", color: "primary" as const },
                  { name: "MinIO", role: "Object storage", badge: "S3A compatible", color: "accent" as const },
                  { name: "Apache Iceberg 1.4", role: "Table format", badge: "ACID lakehouse", color: "green" as const },
                  { name: "MLflow", role: "Experiment tracking", badge: "Model registry", color: "orange" as const },
                  { name: "Neo4j", role: "Graph database", badge: "bolt://7687", color: "purple" as const },
                ].map(({ name, role, badge, color }) => (
                  <div
                    key={name}
                    className="flex flex-col rounded-xl border border-[hsl(217_16%_28%/0.5)] bg-[hsl(218_18%_16%/0.6)] p-3 gap-1"
                  >
                    <div className="text-xs font-bold text-[hsl(34_30%_90%)]">{name}</div>
                    <div className="text-[10px] text-[hsl(34_13%_60%)]">{role}</div>
                    <div className="mt-auto pt-1">
                      <Badge label={badge} color={color} />
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </section>

        </div>
      </main>
    </div>
  );
}
