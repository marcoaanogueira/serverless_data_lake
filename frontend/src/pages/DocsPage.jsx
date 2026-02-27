import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  BookOpen, Zap, Code2, Layers, Bot,
  Database, Search, GitBranch, FileSearch,
  Brain, BarChart3, RefreshCw, Lock,
  ChevronRight, ArrowLeft, Server, Check,
} from 'lucide-react';
import { SketchyBadge, SketchyCard, FloatingDecorations } from '../components/ui/sketchy';

// ─── Sidebar nav structure ─────────────────────────────────────────────────
const NAV = [
  {
    id: 'overview',
    label: 'Overview',
    icon: BookOpen,
    color: 'mint',
  },
  {
    id: 'getting-started',
    label: 'Getting Started',
    icon: Zap,
    color: 'peach',
  },
  {
    id: 'api',
    label: 'API Reference',
    icon: Code2,
    color: 'lilac',
    children: [
      { id: 'api-auth',        label: 'Authentication' },
      { id: 'api-endpoints',   label: 'Schema Endpoints' },
      { id: 'api-ingestion',   label: 'Ingestion' },
      { id: 'api-plans',       label: 'Ingestion Plans' },
      { id: 'api-transform',   label: 'Transform Jobs' },
      { id: 'api-query',       label: 'Query' },
      { id: 'api-agents',      label: 'AI Agents' },
      { id: 'api-chat',        label: 'Chat' },
    ],
  },
  {
    id: 'architecture',
    label: 'Architecture',
    icon: Layers,
    color: 'lilac',
  },
];

// ─── Code block ────────────────────────────────────────────────────────────
function Code({ children, lang }) {
  return (
    <div className="bg-[#111827] rounded-2xl border-2 border-[#1F2937] overflow-x-auto my-4"
         style={{ boxShadow: '4px 4px 0 rgba(0,0,0,0.2)' }}>
      {lang && (
        <div className="px-4 pt-3 pb-1 flex items-center gap-2 border-b border-[#1F2937]">
          <div className="w-2.5 h-2.5 rounded-full bg-[#FECACA]" />
          <div className="w-2.5 h-2.5 rounded-full bg-[#FDE68A]" />
          <div className="w-2.5 h-2.5 rounded-full bg-[#A8E6CF]" />
          <span className="text-[#4B5563] text-xs font-mono ml-1">{lang}</span>
        </div>
      )}
      <pre className="p-4 text-sm font-mono text-[#E5E7EB] leading-relaxed overflow-x-auto">
        <code>{children}</code>
      </pre>
    </div>
  );
}

// ─── Badge inline ──────────────────────────────────────────────────────────
function Method({ m }) {
  const colors = {
    GET:    'bg-[#D4F5E6] text-[#065F46] border-[#A8E6CF]',
    POST:   'bg-[#DDD6FE] text-[#5B21B6] border-[#C4B5FD]',
    PUT:    'bg-[#FEF9C3] text-[#92400E] border-[#FDE68A]',
    DELETE: 'bg-[#FEE2E2] text-[#991B1B] border-[#FECACA]',
  };
  return (
    <span className={`inline-block text-xs font-black px-2 py-0.5 rounded-lg border-2 mr-2 ${colors[m]}`}>
      {m}
    </span>
  );
}

function Endpoint({ method, path, desc }) {
  return (
    <div className="bg-white rounded-2xl border-2 border-gray-100 px-4 py-3 my-3"
         style={{ boxShadow: '3px 3px 0 rgba(0,0,0,0.05)' }}>
      <div className="flex items-baseline gap-2 flex-wrap">
        <Method m={method} />
        <code className="text-sm font-mono font-bold text-gray-800">{path}</code>
      </div>
      {desc && <p className="text-sm text-gray-500 mt-1">{desc}</p>}
    </div>
  );
}

// ─── Section heading helpers ───────────────────────────────────────────────
function H2({ children }) {
  return <h2 className="text-2xl font-black text-gray-900 mt-10 mb-4">{children}</h2>;
}
function H3({ children }) {
  return <h3 className="text-lg font-black text-gray-800 mt-7 mb-3">{children}</h3>;
}
function P({ children }) {
  return <p className="text-gray-600 leading-relaxed mb-4">{children}</p>;
}
function Li({ children }) {
  return (
    <li className="flex items-start gap-2 text-gray-600 text-sm mb-1.5">
      <Check className="w-4 h-4 mt-0.5 shrink-0 text-[#065F46]" />
      <span>{children}</span>
    </li>
  );
}

// ─── Doc sections ──────────────────────────────────────────────────────────
function SectionOverview() {
  const stack = [
    ['Infrastructure', 'AWS CDK (Python)'],
    ['APIs', 'FastAPI + Mangum + API Gateway'],
    ['Compute', 'AWS Lambda + ECS Fargate'],
    ['Storage', 'S3 (data, schemas, configs)'],
    ['Table format', 'Apache Iceberg (Silver)'],
    ['Transformations', 'dbt'],
    ['Query engine', 'DuckDB'],
    ['AI', 'Amazon Bedrock (Claude) via Strands'],
    ['Schema catalog', 'AWS Glue'],
    ['Streaming', 'Kinesis Data Firehose'],
    ['Auth', 'Secrets Manager + OIDC-ready'],
    ['Frontend', 'React + Vite + Tailwind CSS'],
  ];

  return (
    <div>
      <SketchyBadge variant="mint" className="mb-4">Overview</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">Tadpole</h1>
      <P>
        Tadpole is a serverless data lake platform built on AWS using the medallion architecture
        (Bronze → Silver → Gold). It combines AI agents for automated ingestion and transformation
        with a fully serverless infrastructure — no servers to provision, no clusters to manage.
      </P>

      <H2>Data flow</H2>
      <div className="flex items-center gap-2 flex-wrap my-4 p-5 bg-gray-50 rounded-2xl border-2 border-gray-200">
        {['REST API', 'Bronze (S3)', 'Silver (Iceberg)', 'Gold (dbt)', 'Query (DuckDB)'].map((s, i, arr) => (
          <React.Fragment key={s}>
            <span className="text-sm font-bold text-gray-700 bg-white border-2 border-gray-200 px-3 py-1.5 rounded-xl">
              {s}
            </span>
            {i < arr.length - 1 && <ChevronRight className="w-4 h-4 text-gray-400 shrink-0" />}
          </React.Fragment>
        ))}
      </div>

      <H2>Core modules</H2>

      <H3>AI Agents</H3>
      <ul className="mb-4 space-y-1">
        <Li><strong>Ingestion Agent</strong> — reads OpenAPI/Swagger specs, matches endpoints semantically, samples data to detect primary keys, enriches fields with AI descriptions.</Li>
        <Li><strong>Transform Agent</strong> — uses ingestion metadata to auto-generate dbt YAML models for the Gold layer.</Li>
        <Li><strong>Analyze Agent</strong> — ChatBI-style text-to-SQL. Ask a question, get SQL executed against your tables.</Li>
      </ul>

      <H3>Ingestion</H3>
      <ul className="mb-4 space-y-1">
        <Li><strong>Active</strong> — DLT pipelines in Lambda, pull from any REST API on a schedule. Auto-upsert into Silver.</Li>
        <Li><strong>Passive</strong> — push to a REST endpoint. Pydantic validates the payload against the registered schema. PK detection handles dedup automatically.</Li>
      </ul>

      <H3>Transformation</H3>
      <P>
        Transformation jobs are generated dynamically and run on ECS Fargate.
        Jobs can be scheduled (hourly/daily/monthly) or dependency-driven.
      </P>

      <H3>Query</H3>
      <P>
        SQL editor organized by Bronze / Silver / Gold. Powered by DuckDB on Lambda.
        Click any table to see its schema catalog.
      </P>

      <H2>Tech stack</H2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm border-2 border-gray-200 rounded-2xl overflow-hidden">
          <tbody>
            {stack.map(([k, v], i) => (
              <tr key={k} className={i % 2 === 0 ? 'bg-gray-50' : 'bg-white'}>
                <td className="px-4 py-2.5 font-bold text-gray-700 w-1/3">{k}</td>
                <td className="px-4 py-2.5 text-gray-600">{v}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function SectionGettingStarted() {
  return (
    <div>
      <SketchyBadge variant="peach" className="mb-4">Setup</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">Getting Started</h1>

      <H2>Prerequisites</H2>
      <ul className="mb-4 space-y-1">
        <Li>Python 3.11+</Li>
        <Li>Node.js 18+</Li>
        <Li>AWS CLI configured (<code className="bg-gray-100 px-1 rounded text-sm">aws configure</code>)</Li>
        <Li>AWS CDK CLI (<code className="bg-gray-100 px-1 rounded text-sm">npm install -g aws-cdk</code>)</Li>
        <Li>Docker (for building Docker-based Lambdas)</Li>
      </ul>

      <H2>1. Install dependencies</H2>
      <Code lang="bash">{`git clone https://github.com/marcoaanogueira/serverless_data_lake
cd serverless_data_lake

pip install -r requirements.txt
pip install -r requirements-dev.txt

cd frontend && npm install && cd ..`}</Code>

      <H2>2. Configure your tenant</H2>
      <P>Edit <code className="bg-gray-100 px-1 rounded text-sm">artifacts/tables.yaml</code> to define your tenant and initial tables:</P>
      <Code lang="yaml">{`tenants:
  - tenant_name: my_company
    tables:
      - table_name: orders
        primary_keys: [id]
      - table_name: customers
        primary_keys: [customer_id]`}</Code>
      <P>Tables with <code className="bg-gray-100 px-1 rounded text-sm">primary_keys</code> use upsert mode in Silver. Tables without them use append-only.</P>

      <H2>3. Deploy</H2>
      <Code lang="bash">{`cdk bootstrap   # first time only
cdk synth       # validate without deploying
cdk deploy      # deploy all stacks`}</Code>

      <H2>4. Register your first schema endpoint</H2>
      <Code lang="bash">{`curl -X POST https://<api-gateway>/endpoints \\
  -H "x-api-key: <your-api-key>" \\
  -H "Content-Type: application/json" \\
  -d '{
    "name": "orders",
    "domain": "ecommerce",
    "mode": "MANUAL",
    "columns": [
      {"name": "id",         "type": "INTEGER",   "required": true, "primary_key": true},
      {"name": "total",      "type": "FLOAT",     "required": false, "primary_key": false},
      {"name": "created_at", "type": "TIMESTAMP", "required": false, "primary_key": false}
    ]
  }'`}</Code>

      <H2>5. Ingest data</H2>
      <Code lang="bash">{`curl -X POST https://<api-gateway>/ingest/ecommerce/orders \\
  -H "x-api-key: <your-api-key>" \\
  -H "Content-Type: application/json" \\
  -d '{"data": {"id": 1, "total": 99.90, "created_at": "2024-01-15T10:00:00"}}'`}</Code>

      <H2>6. Run tests</H2>
      <Code lang="bash">{`pytest tests/          # Python
ruff check .           # linting

cd frontend
npm run test:run       # Vitest
npm run lint           # ESLint`}</Code>
    </div>
  );
}

function SectionAPIAuth() {
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">API Reference</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">Authentication</h1>
      <P>All endpoints require the <code className="bg-gray-100 px-1 rounded text-sm">x-api-key</code> header, except <code className="bg-gray-100 px-1 rounded text-sm">POST /auth/login</code>.</P>

      <Endpoint method="POST" path="/auth/login" desc="Authenticate with email and password." />
      <H3>Request</H3>
      <Code lang="json">{`{
  "email": "admin@mycompany.com",
  "password": "your_password"
}`}</Code>
      <H3>Response 200</H3>
      <Code lang="json">{`{ "token": "abc123..." }`}</Code>
      <P>Use the returned token as the <code className="bg-gray-100 px-1 rounded text-sm">x-api-key</code> header on all subsequent requests.</P>
    </div>
  );
}

function SectionAPIEndpoints() {
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">API Reference</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">Schema Endpoints</h1>
      <P>Endpoints define the schema for a data source. Creating an endpoint automatically provisions a Kinesis Firehose stream.</P>

      <Endpoint method="GET"    path="/endpoints"                          desc="List all endpoints. Filter by ?domain=..." />
      <Endpoint method="POST"   path="/endpoints"                          desc="Create a new endpoint and provision Firehose." />
      <Endpoint method="GET"    path="/endpoints/{domain}/{name}"          desc="Get a specific endpoint schema." />
      <Endpoint method="PUT"    path="/endpoints/{domain}/{name}"          desc="Update endpoint — creates a new version." />
      <Endpoint method="DELETE" path="/endpoints/{domain}/{name}"          desc="Delete endpoint and all versions." />
      <Endpoint method="GET"    path="/endpoints/{domain}/{name}/versions" desc="List all version numbers." />
      <Endpoint method="GET"    path="/endpoints/{domain}/{name}/yaml"     desc="Get raw YAML schema." />
      <Endpoint method="GET"    path="/endpoints/{domain}/{name}/download" desc="Get presigned S3 download URL." />
      <Endpoint method="POST"   path="/endpoints/infer"                    desc="Infer schema columns from a JSON payload." />

      <H3>POST /endpoints — request body</H3>
      <Code lang="json">{`{
  "name": "orders",
  "domain": "ecommerce",
  "description": "Order records",
  "mode": "MANUAL",
  "columns": [
    {"name": "id",         "type": "INTEGER",   "required": true,  "primary_key": true},
    {"name": "total",      "type": "FLOAT",     "required": false, "primary_key": false},
    {"name": "created_at", "type": "TIMESTAMP", "required": false, "primary_key": false}
  ]
}`}</Code>
      <P>Supported types: <code className="bg-gray-100 px-1 rounded text-xs">STRING VARCHAR INTEGER BIGINT FLOAT DOUBLE BOOLEAN TIMESTAMP DATE JSON ARRAY DECIMAL</code></P>
      <P>Modes: <code className="bg-gray-100 px-1 rounded text-xs">MANUAL</code> · <code className="bg-gray-100 px-1 rounded text-xs">AUTO_INFERENCE</code> · <code className="bg-gray-100 px-1 rounded text-xs">SINGLE_COLUMN</code></P>

      <H3>POST /endpoints/infer — example</H3>
      <Code lang="json">{`// request
{ "payload": {"id": 1, "name": "John", "active": true, "score": 9.5} }

// response
{
  "columns": [
    {"name": "id",     "type": "INTEGER"},
    {"name": "name",   "type": "STRING"},
    {"name": "active", "type": "BOOLEAN"},
    {"name": "score",  "type": "FLOAT"}
  ]
}`}</Code>
    </div>
  );
}

function SectionAPIIngestion() {
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">API Reference</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">Ingestion</h1>
      <P>Records are validated (optionally) and forwarded to Kinesis Firehose → S3 Bronze → Silver (Iceberg).</P>

      <Endpoint method="POST" path="/ingest/{domain}/{endpoint_name}"       desc="Ingest a single record." />
      <Endpoint method="POST" path="/ingest/{domain}/{endpoint_name}/batch" desc="Ingest multiple records." />

      <H3>Query params</H3>
      <div className="overflow-x-auto my-3">
        <table className="w-full text-sm border-2 border-gray-200 rounded-2xl overflow-hidden">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-2 text-left font-black text-gray-700">Param</th>
              <th className="px-4 py-2 text-left font-black text-gray-700">Default</th>
              <th className="px-4 py-2 text-left font-black text-gray-700">Description</th>
            </tr>
          </thead>
          <tbody>
            <tr className="bg-white"><td className="px-4 py-2 font-mono text-xs">validate</td><td className="px-4 py-2">false</td><td className="px-4 py-2 text-gray-600">Validate against schema before sending</td></tr>
            <tr className="bg-gray-50"><td className="px-4 py-2 font-mono text-xs">strict</td><td className="px-4 py-2">false</td><td className="px-4 py-2 text-gray-600">Reject entire batch on any validation failure</td></tr>
          </tbody>
        </table>
      </div>

      <H3>Single record</H3>
      <Code lang="json">{`// POST /ingest/ecommerce/orders
{ "data": {"id": 1, "total": 99.90, "created_at": "2024-01-15T10:00:00"} }

// response
{"status": "ok", "endpoint": "ecommerce/orders", "records_sent": 1, "validated": false}`}</Code>

      <H3>Batch</H3>
      <Code lang="json">{`// POST /ingest/ecommerce/orders/batch
{
  "records": [
    {"id": 1, "total": 99.90},
    {"id": 2, "total": 150.00}
  ]
}

// response
{"status": "ok", "total_records": 2, "sent_count": 2, "failed_count": 0}`}</Code>
    </div>
  );
}

function SectionAPIPlans() {
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">API Reference</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">Ingestion Plans</h1>
      <P>Ingestion plans are AI-generated configurations that describe which API endpoints to pull and how. OAuth2 credentials are stored in Secrets Manager, never in S3.</P>

      <Endpoint method="GET"    path="/ingestion/plans"               desc="List all plans." />
      <Endpoint method="POST"   path="/ingestion/plans"               desc="Create or update a plan." />
      <Endpoint method="GET"    path="/ingestion/plans/{plan_name}"   desc="Get a plan (credentials redacted)." />
      <Endpoint method="DELETE" path="/ingestion/plans/{plan_name}"   desc="Delete plan and associated OAuth2 secret." />
      <Endpoint method="POST"   path="/ingestion/plans/{plan_name}/run" desc="Trigger execution via Step Functions." />

      <H3>Create plan body</H3>
      <Code lang="json">{`{
  "plan_name": "ecommerce_sync",
  "domain": "ecommerce",
  "tags": ["hourly"],
  "plan": {
    "endpoints": [
      {"path": "/orders", "method": "GET", "params": {"page_size": 100}}
    ]
  },
  "oauth2": {
    "token_url": "https://api.myservice.com/oauth/token",
    "client_id": "client_id",
    "client_secret": "client_secret"
  }
}`}</Code>
    </div>
  );
}

function SectionAPITransform() {
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">API Reference</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">Transform Jobs</h1>
      <P>Transform jobs run dbt models on ECS Fargate, writing results to the Gold Iceberg layer.</P>

      <Endpoint method="GET"    path="/transform/jobs"                         desc="List all jobs. Filter by ?domain=..." />
      <Endpoint method="POST"   path="/transform/jobs"                         desc="Create a new transform job." />
      <Endpoint method="GET"    path="/transform/jobs/{domain}/{job_name}"     desc="Get job config." />
      <Endpoint method="PUT"    path="/transform/jobs/{domain}/{job_name}"     desc="Update job." />
      <Endpoint method="DELETE" path="/transform/jobs/{domain}/{job_name}"     desc="Delete job." />
      <Endpoint method="POST"   path="/transform/jobs/{domain}/{job_name}/run" desc="Trigger execution via Step Functions." />
      <Endpoint method="GET"    path="/transform/executions/{execution_id}"    desc="Poll execution status." />

      <H3>Create job body</H3>
      <Code lang="json">{`{
  "domain": "ecommerce",
  "job_name": "daily_revenue",
  "query": "SELECT DATE(created_at) as day, SUM(total) as revenue FROM ecommerce.silver.orders GROUP BY 1",
  "write_mode": "overwrite",
  "unique_key": "day",
  "schedule_type": "cron",
  "cron_schedule": "0 6 * * *",
  "status": "active"
}`}</Code>
      <P><code className="bg-gray-100 px-1 rounded text-xs">schedule_type</code>: <code className="bg-gray-100 px-1 rounded text-xs">cron</code> (requires <code className="bg-gray-100 px-1 rounded text-xs">cron_schedule</code>) or <code className="bg-gray-100 px-1 rounded text-xs">dependency</code> (requires <code className="bg-gray-100 px-1 rounded text-xs">dependencies</code> list).</P>
    </div>
  );
}

function SectionAPIQuery() {
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">API Reference</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">Query</h1>
      <P>Execute SQL against Bronze, Silver, or Gold tables via DuckDB. Only SELECT and WITH statements are allowed. Results are capped at 10,000 rows.</P>

      <Endpoint method="GET" path="/consumption/query"  desc="Execute a SQL SELECT. Pass sql= as query param." />
      <Endpoint method="GET" path="/consumption/tables" desc="List all available tables across layers." />

      <H3>Table naming</H3>
      <div className="overflow-x-auto my-3">
        <table className="w-full text-sm border-2 border-gray-200 rounded-2xl overflow-hidden">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-2 text-left font-black text-gray-700">Reference in SQL</th>
              <th className="px-4 py-2 text-left font-black text-gray-700">Maps to</th>
            </tr>
          </thead>
          <tbody>
            <tr className="bg-white"><td className="px-4 py-2 font-mono text-xs">domain.bronze.table</td><td className="px-4 py-2 text-gray-600">Raw JSONL files in S3</td></tr>
            <tr className="bg-gray-50"><td className="px-4 py-2 font-mono text-xs">domain.silver.table</td><td className="px-4 py-2 text-gray-600">Iceberg table in Glue Catalog</td></tr>
            <tr className="bg-white"><td className="px-4 py-2 font-mono text-xs">domain.gold.table</td><td className="px-4 py-2 text-gray-600">Iceberg table in Glue Catalog</td></tr>
          </tbody>
        </table>
      </div>

      <H3>Example</H3>
      <Code lang="bash">{`GET /consumption/query?sql=SELECT * FROM ecommerce.silver.orders LIMIT 10`}</Code>
      <Code lang="json">{`{
  "data": [{"id": 1, "total": 99.90, "created_at": "2024-01-15T10:00:00"}],
  "row_count": 1,
  "truncated": false
}`}</Code>
    </div>
  );
}

function SectionAPIAgents() {
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">API Reference</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">AI Agents</h1>

      <H2>Ingestion Agent</H2>
      <P>Reads an OpenAPI/Swagger spec, filters endpoints by semantic matching, samples data to detect primary keys, and enriches fields with AI descriptions.</P>

      <Endpoint method="POST" path="/agent/ingestion/plan"          desc="Generate plan synchronously. Does not save or execute." />
      <Endpoint method="POST" path="/agent/ingestion/run"           desc="Generate, save, and trigger execution. Returns a job ID." />
      <Endpoint method="GET"  path="/agent/ingestion/jobs/{job_id}" desc="Poll async job status." />

      <H3>Request body</H3>
      <Code lang="json">{`{
  "openapi_url": "https://api.myservice.com/openapi.json",
  "interests": ["orders", "customers"],
  "token": "Bearer eyJ...",
  "domain": "ecommerce",
  "plan_name": "ecommerce_sync",
  "tags": ["hourly"]
}`}</Code>
      <P><code className="bg-gray-100 px-1 rounded text-xs">interests</code> supports semantic matching — listing "customers" will find <code className="bg-gray-100 px-1 rounded text-xs">GET /persons</code> in the spec.</P>

      <H3>Async response</H3>
      <Code lang="json">{`{
  "job_id": "abc123",
  "status": "running",
  "poll_url": "/agent/ingestion/jobs/abc123"
}`}</Code>

      <H2>Transform Agent</H2>
      <P>Reads Silver table metadata and auto-generates dbt YAML model definitions for the Gold layer.</P>

      <Endpoint method="POST" path="/agent/transformation/plan"          desc="Generate job definitions synchronously." />
      <Endpoint method="POST" path="/agent/transformation/run"           desc="Generate, save, and optionally execute. Returns a job ID." />
      <Endpoint method="GET"  path="/agent/transformation/jobs/{job_id}" desc="Poll async job status." />

      <H3>Request body</H3>
      <Code lang="json">{`{
  "domain": "ecommerce",
  "tables": ["orders", "customers"],
  "trigger_execution": true
}`}</Code>
    </div>
  );
}

function SectionAPIChat() {
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">API Reference</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">Chat (Analyze Agent)</h1>
      <P>ChatBI-style text-to-SQL agent with session memory. Translates natural language into SQL, executes it against your tables, and returns the result.</P>

      <Endpoint method="POST"   path="/chat/message"               desc="Send a message. Creates a session if none provided." />
      <Endpoint method="GET"    path="/chat/sessions"              desc="List all sessions." />
      <Endpoint method="GET"    path="/chat/sessions/{session_id}" desc="Get session with full message history." />
      <Endpoint method="DELETE" path="/chat/sessions/{session_id}" desc="Delete a session." />

      <H3>Send message</H3>
      <Code lang="json">{`// request
{
  "session_id": "optional-existing-id",
  "message": "What was the total revenue per day last week?"
}

// response
{
  "session_id": "sess_abc123",
  "message_id": "msg_xyz",
  "content": [
    {"type": "text", "text": "Here are the daily revenues:"},
    {"type": "tool_result", "data": [
      {"day": "2024-01-08", "revenue": 12450.00},
      {"day": "2024-01-09", "revenue": 9800.00}
    ]}
  ]
}`}</Code>
    </div>
  );
}

function SectionArchitecture() {
  const lambdas = [
    ['auth',                         'Non-Docker', '128MB',  '30s',   'API Gateway'],
    ['authorizer',                   'Non-Docker', '128MB',  '10s',   'API Gateway (authorizer)'],
    ['endpoints',                    'Non-Docker (Layers)', '256MB', '30s', 'API Gateway'],
    ['serverless_ingestion',         'Non-Docker (Layers)', '256MB', '30s', 'API Gateway'],
    ['query_api',                    'Docker',     '5GB',    '900s',  'API Gateway'],
    ['transform_jobs',               'Docker',     '512MB',  '30s',   'API Gateway'],
    ['ingestion_plans',              'Docker',     '512MB',  '30s',   'API Gateway'],
    ['ingestion_agent',              'Docker',     '1GB',    '900s',  'API Gateway'],
    ['transformation_agent',         'Docker',     '512MB',  '900s',  'API Gateway'],
    ['chat_api',                     'Docker',     '512MB',  '120s',  'API Gateway'],
    ['serverless_processing_iceberg','Docker',     '5GB',    '900s',  'S3 event'],
    ['serverless_analytics',         'Docker',     '5GB',    '900s',  'EventBridge'],
  ];

  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">Architecture</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">Architecture</h1>

      <H2>Data flow</H2>
      <Code lang="text">{`REST API / Push
     │
     ▼
POST /ingest/{domain}/{table}
     │
     ▼
Kinesis Data Firehose
     │
     ▼
S3 Bronze  ──────────────── raw JSONL, partitioned by domain/table
     │
     │  (S3 event)
     ▼
Lambda: serverless_processing_iceberg
     │
     ▼
S3 Silver (Apache Iceberg) ─── Glue Catalog, domain_silver namespace
     │
     │  (Step Functions + ECS Fargate)
     ▼
dbt transform jobs
     │
     ▼
S3 Gold (Apache Iceberg) ────  Glue Catalog, domain_gold namespace
     │
     ▼
DuckDB / Lambda  ◄──────────── GET /consumption/query`}</Code>

      <H2>Why ECS Fargate for transforms?</H2>
      <P>Transformation jobs are generated dynamically and run on ECS Fargate, keeping the compute layer fully serverless with no persistent infrastructure to manage.</P>

      <H2>Schema registry</H2>
      <P>All schema metadata is stored in S3, not a database. This makes schemas version-controlled and keeps the system fully serverless.</P>
      <Code lang="text">{`s3://{schema_bucket}/
├── schemas/{domain}/
│   ├── bronze/{table}/v1.yaml, latest.yaml
│   ├── silver/{table}/latest.yaml
│   └── gold/{job}/config.yaml
└── {tenant}/ingestion_plans/{plan}/config.yaml`}</Code>

      <H2>Lambda inventory</H2>
      <div className="overflow-x-auto">
        <table className="w-full text-xs border-2 border-gray-200 rounded-2xl overflow-hidden">
          <thead className="bg-gray-50">
            <tr>
              {['Service', 'Type', 'Memory', 'Timeout', 'Trigger'].map(h => (
                <th key={h} className="px-3 py-2 text-left font-black text-gray-700">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {lambdas.map(([name, type, mem, timeout, trigger], i) => (
              <tr key={name} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                <td className="px-3 py-2 font-mono font-bold text-gray-800">{name}</td>
                <td className="px-3 py-2 text-gray-600">{type}</td>
                <td className="px-3 py-2 text-gray-600">{mem}</td>
                <td className="px-3 py-2 text-gray-600">{timeout}</td>
                <td className="px-3 py-2 text-gray-600">{trigger}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <H2>Authentication</H2>
      <ul className="mb-4 space-y-1">
        <Li>PBKDF2-HMAC-SHA256 (260k iterations) for password hashing</Li>
        <Li>API key validated by Lambda Authorizer on every request</Li>
        <Li>OAuth2 credentials stored in Secrets Manager per ingestion plan</Li>
        <Li>OIDC-ready interface for future SSO (Supabase / Cognito)</Li>
      </ul>
    </div>
  );
}

const SECTIONS = {
  'overview':       SectionOverview,
  'getting-started': SectionGettingStarted,
  'api-auth':       SectionAPIAuth,
  'api-endpoints':  SectionAPIEndpoints,
  'api-ingestion':  SectionAPIIngestion,
  'api-plans':      SectionAPIPlans,
  'api-transform':  SectionAPITransform,
  'api-query':      SectionAPIQuery,
  'api-agents':     SectionAPIAgents,
  'api-chat':       SectionAPIChat,
  'architecture':   SectionArchitecture,
};

// ─── Main ──────────────────────────────────────────────────────────────────
export default function DocsPage({ onBack }) {
  const [active, setActive] = useState('overview');
  const [apiOpen, setApiOpen] = useState(false);

  const SectionComponent = SECTIONS[active] || SectionOverview;

  return (
    <div className="min-h-screen bg-white">
      <FloatingDecorations />

      {/* Top gradient bar */}
      <div className="fixed top-0 left-0 right-0 h-0.5 bg-gradient-to-r from-[#A8E6CF] via-[#C4B5FD] to-[#FECACA] z-50" />

      {/* Navbar */}
      <nav className="fixed top-0 left-0 right-0 z-40 bg-white/90 backdrop-blur-sm border-b-2 border-gray-100 mt-0.5">
        <div className="max-w-7xl mx-auto px-6 h-14 flex items-center gap-4">
          <button
            onClick={onBack}
            className="flex items-center gap-1.5 text-sm font-semibold text-gray-500 hover:text-gray-900 transition-colors"
          >
            <ArrowLeft className="w-4 h-4" /> Back
          </button>
          <div className="w-px h-5 bg-gray-200" />
          <div className="flex items-center gap-2">
            <span className="text-lg">🐸</span>
            <span className="font-black text-gray-900 text-sm">
              Tadpole<span className="text-[#A8E6CF]">.</span>
            </span>
          </div>
          <span className="text-gray-400 text-sm font-medium">/ docs</span>
        </div>
      </nav>

      <div className="flex pt-14 max-w-7xl mx-auto">
        {/* Sidebar */}
        <aside className="w-60 shrink-0 sticky top-14 h-[calc(100vh-3.5rem)] overflow-y-auto border-r-2 border-gray-100 py-6 px-4">
          <nav className="space-y-1">
            {NAV.map((item) => {
              const isActive = active === item.id || (item.children && item.children.some(c => c.id === active));
              const isOpen = item.children && (apiOpen || item.children.some(c => c.id === active));

              return (
                <div key={item.id}>
                  <button
                    onClick={() => {
                      if (item.children) {
                        setApiOpen(o => !o);
                        if (!active.startsWith('api')) setActive('api-auth');
                      } else {
                        setActive(item.id);
                      }
                    }}
                    className={`w-full flex items-center gap-2.5 px-3 py-2 rounded-xl text-sm font-bold transition-all ${
                      isActive && !item.children
                        ? 'bg-[#A8E6CF] text-[#065F46]'
                        : 'text-gray-600 hover:bg-gray-100'
                    }`}
                    style={isActive && !item.children ? { boxShadow: '3px 3px 0 rgba(0,0,0,0.08)' } : {}}
                  >
                    <item.icon className="w-4 h-4 shrink-0" />
                    {item.label}
                    {item.children && (
                      <ChevronRight className={`w-3.5 h-3.5 ml-auto transition-transform ${isOpen ? 'rotate-90' : ''}`} />
                    )}
                  </button>

                  {item.children && isOpen && (
                    <div className="ml-6 mt-1 space-y-0.5">
                      {item.children.map(child => (
                        <button
                          key={child.id}
                          onClick={() => setActive(child.id)}
                          className={`w-full text-left px-3 py-1.5 rounded-xl text-xs font-semibold transition-all ${
                            active === child.id
                              ? 'bg-[#DDD6FE] text-[#5B21B6]'
                              : 'text-gray-500 hover:bg-gray-100'
                          }`}
                        >
                          {child.label}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
          </nav>
        </aside>

        {/* Content */}
        <main className="flex-1 min-w-0 px-10 py-10 max-w-3xl">
          <AnimatePresence mode="wait">
            <motion.div
              key={active}
              initial={{ opacity: 0, y: 16 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -8 }}
              transition={{ duration: 0.2 }}
            >
              <SectionComponent />
            </motion.div>
          </AnimatePresence>
        </main>
      </div>

      {/* Bottom gradient bar */}
      <div className="fixed bottom-0 left-0 right-0 h-1.5 bg-gradient-to-r from-[#A8E6CF] via-[#C4B5FD] to-[#FECACA]" />
    </div>
  );
}
