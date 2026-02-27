import React, { useState, useContext, createContext } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  BookOpen, Zap, Code2, Layers,
  ChevronRight, ArrowLeft, Check,
} from 'lucide-react';
import { SketchyBadge, FloatingDecorations } from '../components/ui/sketchy';

// ─── i18n ──────────────────────────────────────────────────────────────────
const LangCtx = createContext('en');
const useT = () => TRANSLATIONS[useContext(LangCtx)];

const TRANSLATIONS = {
  en: {
    nav: {
      back: 'Back', docs: '/ docs',
      overview: 'Overview', gettingStarted: 'Getting Started',
      apiReference: 'API Reference', architecture: 'Architecture',
      api: {
        auth: 'Authentication', endpoints: 'Schema Endpoints',
        ingestion: 'Ingestion', plans: 'Ingestion Plans',
        transform: 'Transform Jobs', query: 'Query',
        agents: 'AI Agents', chat: 'Chat',
      },
    },
    overview: {
      badge: 'Overview', title: 'Tadpole',
      intro: 'Tadpole is a serverless data lake platform built on AWS using the medallion architecture (Bronze → Silver → Gold). It combines AI agents for automated ingestion and transformation with a fully serverless infrastructure — no servers to provision, no clusters to manage.',
      h2DataFlow: 'Data flow', h2Modules: 'Core modules',
      h3Agents: 'AI Agents',
      agentItems: [
        <><strong>Ingestion Agent</strong> — reads OpenAPI/Swagger specs, matches endpoints semantically, samples data to detect primary keys, enriches fields with AI descriptions.</>,
        <><strong>Transform Agent</strong> — uses ingestion metadata to auto-generate dbt YAML models for the Gold layer.</>,
        <><strong>Analyze Agent</strong> — ChatBI-style text-to-SQL. Ask a question, get SQL executed against your tables.</>,
      ],
      h3Ingestion: 'Ingestion',
      ingestionItems: [
        <><strong>Active</strong> — DLT pipelines in Lambda, pull from any REST API on a schedule. Auto-upsert into Silver.</>,
        <><strong>Passive</strong> — push to a REST endpoint. Pydantic validates the payload against the registered schema. PK detection handles dedup automatically.</>,
      ],
      h3Transform: 'Transformation',
      transformText: 'Transformation jobs are generated dynamically and run on ECS Fargate. Jobs can be scheduled (hourly/daily/monthly) or dependency-driven.',
      h3Query: 'Query',
      queryText: 'SQL editor organized by Bronze / Silver / Gold. Powered by DuckDB on Lambda. Click any table to see its schema catalog.',
      h2TechStack: 'Tech stack',
    },
    gs: {
      badge: 'Setup', title: 'Getting Started',
      h2Prereqs: 'Prerequisites',
      h2S1: '1. Install dependencies',
      h2S2: '2. Configure your tenant',
      s2Desc: <>Set the tenant name in <code className="bg-gray-100 px-1 rounded text-sm">cdk.json</code> under the <code className="bg-gray-100 px-1 rounded text-sm">context</code> key:</>,
      s2Note: <>This becomes the <code className="bg-gray-100 px-1 rounded text-sm">TENANT</code> env variable in all Lambdas and is used to name S3 buckets (<code className="bg-gray-100 px-1 rounded text-sm">my_company-bronze</code>, <code className="bg-gray-100 px-1 rounded text-sm">my_company-silver</code>, etc.). Tables and schemas are managed at runtime via the Schema Registry API.</>,
      h2S3: '3. Deploy',
      h2S4: '4. Access the frontend',
      s4Desc: <>After deploy, the CDK prints the stack outputs in the terminal. Copy the <code className="bg-gray-100 px-1 rounded text-sm">CloudFrontURL</code> and open it in your browser.</>,
      h2S5: '5. Create the first user',
      s5Desc: 'Run the helper script — it prompts for email and password, generates the hash, and prints the AWS CLI command ready to copy and run:',
      s5Note: <>The script outputs an <code className="bg-gray-100 px-1 rounded text-sm">aws secretsmanager put-secret-value</code> command. Run it to store the credentials in Secrets Manager. The login page will be active immediately.</>,
      h2S6: '6. Register your first schema endpoint',
      h2S7: '7. Ingest data',
      h2S8: '8. Run tests',
    },
    auth: {
      badge: 'API Reference', title: 'Authentication',
      intro: <>All endpoints require the <code className="bg-gray-100 px-1 rounded text-sm">x-api-key</code> header, except <code className="bg-gray-100 px-1 rounded text-sm">POST /auth/login</code>.</>,
      epDesc: 'Authenticate with email and password.',
      h3Req: 'Request', h3Res: 'Response 200',
      note: <>Use the returned token as the <code className="bg-gray-100 px-1 rounded text-sm">x-api-key</code> header on all subsequent requests.</>,
    },
    endpoints: {
      badge: 'API Reference', title: 'Schema Endpoints',
      intro: 'Endpoints define the schema for a data source. Creating an endpoint automatically provisions a Kinesis Firehose stream.',
      list: 'List all endpoints. Filter by ?domain=...',
      create: 'Create a new endpoint and provision Firehose.',
      get: 'Get a specific endpoint schema.',
      update: 'Update endpoint — creates a new version.',
      delete: 'Delete endpoint and all versions.',
      versions: 'List all version numbers.',
      yaml: 'Get raw YAML schema.',
      download: 'Get presigned S3 download URL.',
      infer: 'Infer schema columns from a JSON payload.',
      h3Body: 'POST /endpoints — request body',
      types: 'Supported types:',
      modes: 'Modes:',
      h3Infer: 'POST /endpoints/infer — example',
    },
    ingestion: {
      badge: 'API Reference', title: 'Ingestion',
      intro: 'Records are validated (optionally) and forwarded to Kinesis Firehose → S3 Bronze → Silver (Iceberg).',
      single: 'Ingest a single record.',
      batch: 'Ingest multiple records.',
      h3Params: 'Query params',
      thParam: 'Param', thDefault: 'Default', thDesc: 'Description',
      paramValidate: 'Validate against schema before sending',
      paramStrict: 'Reject entire batch on any validation failure',
      h3Single: 'Single record', h3Batch: 'Batch',
    },
    plans: {
      badge: 'API Reference', title: 'Ingestion Plans',
      intro: 'Ingestion plans are AI-generated configurations that describe which API endpoints to pull and how. OAuth2 credentials are stored in Secrets Manager, never in S3.',
      list: 'List all plans.',
      create: 'Create or update a plan.',
      get: 'Get a plan (credentials redacted).',
      delete: 'Delete plan and associated OAuth2 secret.',
      run: 'Trigger execution via Step Functions.',
      h3Body: 'Create plan body',
    },
    transform: {
      badge: 'API Reference', title: 'Transform Jobs',
      intro: 'Transform jobs run dbt models on ECS Fargate, writing results to the Gold Iceberg layer.',
      list: 'List all jobs. Filter by ?domain=...',
      create: 'Create a new transform job.',
      get: 'Get job config.',
      update: 'Update job.',
      delete: 'Delete job.',
      run: 'Trigger execution via Step Functions.',
      poll: 'Poll execution status.',
      h3Body: 'Create job body',
      scheduleNote: <><code className="bg-gray-100 px-1 rounded text-xs">schedule_type</code>: <code className="bg-gray-100 px-1 rounded text-xs">cron</code> (requires <code className="bg-gray-100 px-1 rounded text-xs">cron_schedule</code>) or <code className="bg-gray-100 px-1 rounded text-xs">dependency</code> (requires <code className="bg-gray-100 px-1 rounded text-xs">dependencies</code> list).</>,
    },
    query: {
      badge: 'API Reference', title: 'Query',
      intro: 'Execute SQL against Bronze, Silver, or Gold tables via DuckDB. Only SELECT and WITH statements are allowed. Results are capped at 10,000 rows.',
      queryDesc: 'Execute a SQL SELECT. Pass sql= as query param.',
      tablesDesc: 'List all available tables across layers.',
      h3Naming: 'Table naming',
      thRef: 'Reference in SQL', thMaps: 'Maps to',
      bronze: 'Raw JSONL files in S3',
      silver: 'Iceberg table in Glue Catalog',
      gold: 'Iceberg table in Glue Catalog',
      h3Example: 'Example',
    },
    agents: {
      badge: 'API Reference', title: 'AI Agents',
      h2Ingestion: 'Ingestion Agent',
      ingestionIntro: 'Reads an OpenAPI/Swagger spec, filters endpoints by semantic matching, samples data to detect primary keys, and enriches fields with AI descriptions.',
      planDesc: 'Generate plan synchronously. Does not save or execute.',
      runDesc: 'Generate, save, and trigger execution. Returns a job ID.',
      pollDesc: 'Poll async job status.',
      h3Req: 'Request body',
      semanticNote: <><code className="bg-gray-100 px-1 rounded text-xs">interests</code> supports semantic matching — listing "customers" will find <code className="bg-gray-100 px-1 rounded text-xs">GET /persons</code> in the spec.</>,
      h3Async: 'Async response',
      h2Transform: 'Transform Agent',
      transformIntro: 'Reads Silver table metadata and auto-generates dbt YAML model definitions for the Gold layer.',
      transformPlan: 'Generate job definitions synchronously.',
      transformRun: 'Generate, save, and optionally execute. Returns a job ID.',
      transformPoll: 'Poll async job status.',
    },
    chat: {
      badge: 'API Reference', title: 'Chat (Analyze Agent)',
      intro: 'ChatBI-style text-to-SQL agent with session memory. Translates natural language into SQL, executes it against your tables, and returns the result.',
      messageDesc: 'Send a message. Creates a session if none provided.',
      listDesc: 'List all sessions.',
      getDesc: 'Get session with full message history.',
      deleteDesc: 'Delete a session.',
      h3Send: 'Send message',
    },
    arch: {
      badge: 'Architecture', title: 'Architecture',
      h2DataFlow: 'Data flow',
      h2Fargate: 'Why ECS Fargate for transforms?',
      fargateText: 'Transformation jobs are generated dynamically and run on ECS Fargate, keeping the compute layer fully serverless with no persistent infrastructure to manage.',
      h2Schema: 'Schema registry',
      schemaText: 'All schema metadata is stored in S3, not a database. This makes schemas version-controlled and keeps the system fully serverless.',
      h2Lambda: 'Lambda inventory',
      thService: 'Service', thType: 'Type', thMem: 'Memory', thTimeout: 'Timeout', thTrigger: 'Trigger',
      h2Auth: 'Authentication',
      authItems: [
        'PBKDF2-HMAC-SHA256 (260k iterations) for password hashing',
        'API key validated by Lambda Authorizer on every request',
        'OAuth2 credentials stored in Secrets Manager per ingestion plan',
        'OIDC-ready interface for future SSO (Supabase / Cognito)',
      ],
    },
  },

  pt_br: {
    nav: {
      back: 'Voltar', docs: '/ docs',
      overview: 'Visão Geral', gettingStarted: 'Primeiros Passos',
      apiReference: 'Referência da API', architecture: 'Arquitetura',
      api: {
        auth: 'Autenticação', endpoints: 'Endpoints de Schema',
        ingestion: 'Ingestão', plans: 'Planos de Ingestão',
        transform: 'Jobs de Transformação', query: 'Consulta',
        agents: 'Agentes de IA', chat: 'Chat',
      },
    },
    overview: {
      badge: 'Visão Geral', title: 'Tadpole',
      intro: 'Tadpole é uma plataforma de data lake serverless na AWS com arquitetura medallion (Bronze → Silver → Gold). Combina agentes de IA para ingestão e transformação automatizadas com infraestrutura totalmente serverless — sem servidores para provisionar, sem clusters para gerenciar.',
      h2DataFlow: 'Fluxo de dados', h2Modules: 'Módulos principais',
      h3Agents: 'Agentes de IA',
      agentItems: [
        <><strong>Agente de Ingestão</strong> — lê specs OpenAPI/Swagger, filtra endpoints semanticamente, amostra dados para detectar chaves primárias e enriquece campos com descrições de IA.</>,
        <><strong>Agente de Transformação</strong> — usa metadados de ingestão para gerar automaticamente modelos dbt YAML para a camada Gold.</>,
        <><strong>Agente de Análise</strong> — texto para SQL estilo ChatBI. Faça uma pergunta e obtenha o SQL executado nas suas tabelas.</>,
      ],
      h3Ingestion: 'Ingestão',
      ingestionItems: [
        <><strong>Ativa</strong> — pipelines DLT no Lambda, puxa de qualquer API REST em um agendamento. Auto-upsert no Silver.</>,
        <><strong>Passiva</strong> — envie para um endpoint REST. O Pydantic valida o payload contra o schema registrado. Detecção de PK trata deduplicação automaticamente.</>,
      ],
      h3Transform: 'Transformação',
      transformText: 'Jobs de transformação são gerados dinamicamente e executados no ECS Fargate. Jobs podem ser agendados (horário/diário/mensal) ou orientados por dependências.',
      h3Query: 'Consulta',
      queryText: 'Editor SQL organizado por Bronze / Silver / Gold. Powered by DuckDB no Lambda. Clique em qualquer tabela para ver o catálogo de schema.',
      h2TechStack: 'Stack tecnológica',
    },
    gs: {
      badge: 'Configuração', title: 'Primeiros Passos',
      h2Prereqs: 'Pré-requisitos',
      h2S1: '1. Instalar dependências',
      h2S2: '2. Configurar o tenant',
      s2Desc: <>Defina o nome do tenant no <code className="bg-gray-100 px-1 rounded text-sm">cdk.json</code> dentro da chave <code className="bg-gray-100 px-1 rounded text-sm">context</code>:</>,
      s2Note: <>Esse valor vira a variável de ambiente <code className="bg-gray-100 px-1 rounded text-sm">TENANT</code> em todas as Lambdas e é usado para nomear os buckets S3 (<code className="bg-gray-100 px-1 rounded text-sm">my_company-bronze</code>, <code className="bg-gray-100 px-1 rounded text-sm">my_company-silver</code>, etc.). Tabelas e schemas são gerenciados em runtime via Schema Registry API.</>,
      h2S3: '3. Deploy',
      h2S4: '4. Acessar o frontend',
      s4Desc: <>Após o deploy, o CDK exibe os outputs no terminal. Copie o <code className="bg-gray-100 px-1 rounded text-sm">CloudFrontURL</code> e abra no navegador.</>,
      h2S5: '5. Criar o primeiro usuário',
      s5Desc: 'Execute o script auxiliar — ele solicita email e senha, gera o hash e imprime o comando AWS CLI pronto para copiar e executar:',
      s5Note: <>O script gera um comando <code className="bg-gray-100 px-1 rounded text-sm">aws secretsmanager put-secret-value</code>. Execute-o para armazenar as credenciais no Secrets Manager. A página de login estará ativa imediatamente.</>,
      h2S6: '6. Registrar o primeiro endpoint de schema',
      h2S7: '7. Ingerir dados',
      h2S8: '8. Executar testes',
    },
    auth: {
      badge: 'Referência da API', title: 'Autenticação',
      intro: <>Todos os endpoints exigem o header <code className="bg-gray-100 px-1 rounded text-sm">x-api-key</code>, exceto <code className="bg-gray-100 px-1 rounded text-sm">POST /auth/login</code>.</>,
      epDesc: 'Autenticar com email e senha.',
      h3Req: 'Requisição', h3Res: 'Resposta 200',
      note: <>Use o token retornado como header <code className="bg-gray-100 px-1 rounded text-sm">x-api-key</code> em todas as requisições subsequentes.</>,
    },
    endpoints: {
      badge: 'Referência da API', title: 'Endpoints de Schema',
      intro: 'Endpoints definem o schema para uma fonte de dados. Criar um endpoint provisiona automaticamente um stream do Kinesis Firehose.',
      list: 'Lista todos os endpoints. Filtre por ?domain=...',
      create: 'Cria um novo endpoint e provisiona o Firehose.',
      get: 'Obtém o schema de um endpoint específico.',
      update: 'Atualiza o endpoint — cria uma nova versão.',
      delete: 'Exclui o endpoint e todas as versões.',
      versions: 'Lista todos os números de versão.',
      yaml: 'Obtém o schema YAML bruto.',
      download: 'Obtém URL de download pré-assinada do S3.',
      infer: 'Infere colunas do schema a partir de um payload JSON.',
      h3Body: 'POST /endpoints — corpo da requisição',
      types: 'Tipos suportados:',
      modes: 'Modos:',
      h3Infer: 'POST /endpoints/infer — exemplo',
    },
    ingestion: {
      badge: 'Referência da API', title: 'Ingestão',
      intro: 'Registros são validados (opcionalmente) e encaminhados para Kinesis Firehose → S3 Bronze → Silver (Iceberg).',
      single: 'Ingere um único registro.',
      batch: 'Ingere múltiplos registros.',
      h3Params: 'Query params',
      thParam: 'Param', thDefault: 'Padrão', thDesc: 'Descrição',
      paramValidate: 'Valida contra o schema antes de enviar',
      paramStrict: 'Rejeita o batch inteiro em qualquer falha de validação',
      h3Single: 'Registro único', h3Batch: 'Batch',
    },
    plans: {
      badge: 'Referência da API', title: 'Planos de Ingestão',
      intro: 'Planos de ingestão são configurações geradas por IA que descrevem quais endpoints de API consumir. Credenciais OAuth2 são armazenadas no Secrets Manager, nunca no S3.',
      list: 'Lista todos os planos.',
      create: 'Cria ou atualiza um plano.',
      get: 'Obtém um plano (credenciais ocultadas).',
      delete: 'Exclui o plano e o secret OAuth2 associado.',
      run: 'Dispara execução via Step Functions.',
      h3Body: 'Corpo para criação de plano',
    },
    transform: {
      badge: 'Referência da API', title: 'Jobs de Transformação',
      intro: 'Jobs de transformação executam modelos dbt no ECS Fargate, gravando resultados na camada Gold Iceberg.',
      list: 'Lista todos os jobs. Filtre por ?domain=...',
      create: 'Cria um novo job de transformação.',
      get: 'Obtém a configuração do job.',
      update: 'Atualiza o job.',
      delete: 'Exclui o job.',
      run: 'Dispara execução via Step Functions.',
      poll: 'Consulta status da execução.',
      h3Body: 'Corpo para criação de job',
      scheduleNote: <><code className="bg-gray-100 px-1 rounded text-xs">schedule_type</code>: <code className="bg-gray-100 px-1 rounded text-xs">cron</code> (exige <code className="bg-gray-100 px-1 rounded text-xs">cron_schedule</code>) ou <code className="bg-gray-100 px-1 rounded text-xs">dependency</code> (exige lista de <code className="bg-gray-100 px-1 rounded text-xs">dependencies</code>).</>,
    },
    query: {
      badge: 'Referência da API', title: 'Consulta',
      intro: 'Execute SQL nas tabelas Bronze, Silver ou Gold via DuckDB. Apenas statements SELECT e WITH são permitidos. Resultados limitados a 10.000 linhas.',
      queryDesc: 'Executa um SELECT SQL. Passe sql= como query param.',
      tablesDesc: 'Lista todas as tabelas disponíveis entre as camadas.',
      h3Naming: 'Nomenclatura das tabelas',
      thRef: 'Referência no SQL', thMaps: 'Mapeia para',
      bronze: 'Arquivos JSONL brutos no S3',
      silver: 'Tabela Iceberg no Glue Catalog',
      gold: 'Tabela Iceberg no Glue Catalog',
      h3Example: 'Exemplo',
    },
    agents: {
      badge: 'Referência da API', title: 'Agentes de IA',
      h2Ingestion: 'Agente de Ingestão',
      ingestionIntro: 'Lê uma spec OpenAPI/Swagger, filtra endpoints por correspondência semântica, amostra dados para detectar chaves primárias e enriquece campos com descrições de IA.',
      planDesc: 'Gera plano de forma síncrona. Não salva nem executa.',
      runDesc: 'Gera, salva e dispara execução. Retorna um job ID.',
      pollDesc: 'Consulta status do job assíncrono.',
      h3Req: 'Corpo da requisição',
      semanticNote: <><code className="bg-gray-100 px-1 rounded text-xs">interests</code> suporta correspondência semântica — listar "customers" encontrará <code className="bg-gray-100 px-1 rounded text-xs">GET /persons</code> na spec.</>,
      h3Async: 'Resposta assíncrona',
      h2Transform: 'Agente de Transformação',
      transformIntro: 'Lê metadados das tabelas Silver e gera automaticamente definições de modelos dbt YAML para a camada Gold.',
      transformPlan: 'Gera definições de jobs de forma síncrona.',
      transformRun: 'Gera, salva e opcionalmente executa. Retorna um job ID.',
      transformPoll: 'Consulta status do job assíncrono.',
    },
    chat: {
      badge: 'Referência da API', title: 'Chat (Agente de Análise)',
      intro: 'Agente texto-para-SQL estilo ChatBI com memória de sessão. Traduz linguagem natural em SQL, executa nas suas tabelas e retorna o resultado.',
      messageDesc: 'Envia uma mensagem. Cria uma sessão se nenhuma for fornecida.',
      listDesc: 'Lista todas as sessões.',
      getDesc: 'Obtém a sessão com histórico completo de mensagens.',
      deleteDesc: 'Exclui uma sessão.',
      h3Send: 'Enviar mensagem',
    },
    arch: {
      badge: 'Arquitetura', title: 'Arquitetura',
      h2DataFlow: 'Fluxo de dados',
      h2Fargate: 'Por que ECS Fargate para transformações?',
      fargateText: 'Jobs de transformação são gerados dinamicamente e executados no ECS Fargate, mantendo a camada de computação totalmente serverless sem infraestrutura persistente para gerenciar.',
      h2Schema: 'Schema registry',
      schemaText: 'Todos os metadados de schema são armazenados no S3, não em um banco de dados. Isso torna os schemas versionados e mantém o sistema totalmente serverless.',
      h2Lambda: 'Inventário de Lambdas',
      thService: 'Serviço', thType: 'Tipo', thMem: 'Memória', thTimeout: 'Timeout', thTrigger: 'Trigger',
      h2Auth: 'Autenticação',
      authItems: [
        'PBKDF2-HMAC-SHA256 (260k iterações) para hash de senha',
        'API key validada por Lambda Authorizer em cada requisição',
        'Credenciais OAuth2 armazenadas no Secrets Manager por plano de ingestão',
        'Interface OIDC-ready para SSO futuro (Supabase / Cognito)',
      ],
    },
  },
};

// ─── Sidebar nav ───────────────────────────────────────────────────────────
const makeNav = (t) => [
  { id: 'overview',        label: t.nav.overview,      icon: BookOpen, color: 'mint' },
  { id: 'getting-started', label: t.nav.gettingStarted, icon: Zap,      color: 'peach' },
  {
    id: 'api', label: t.nav.apiReference, icon: Code2, color: 'lilac',
    children: [
      { id: 'api-auth',      label: t.nav.api.auth },
      { id: 'api-endpoints', label: t.nav.api.endpoints },
      { id: 'api-ingestion', label: t.nav.api.ingestion },
      { id: 'api-plans',     label: t.nav.api.plans },
      { id: 'api-transform', label: t.nav.api.transform },
      { id: 'api-query',     label: t.nav.api.query },
      { id: 'api-agents',    label: t.nav.api.agents },
      { id: 'api-chat',      label: t.nav.api.chat },
    ],
  },
  { id: 'architecture', label: t.nav.architecture, icon: Layers, color: 'lilac' },
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

// ─── Sections ──────────────────────────────────────────────────────────────
function SectionOverview() {
  const t = useT().overview;
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
      <SketchyBadge variant="mint" className="mb-4">{t.badge}</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">{t.title}</h1>
      <P>{t.intro}</P>

      <H2>{t.h2DataFlow}</H2>
      <div className="flex items-center gap-2 flex-wrap my-4 p-5 bg-gray-50 rounded-2xl border-2 border-gray-200">
        {['REST API', 'Bronze (S3)', 'Silver (Iceberg)', 'Gold (dbt)', 'Query (DuckDB)'].map((s, i, arr) => (
          <React.Fragment key={s}>
            <span className="text-sm font-bold text-gray-700 bg-white border-2 border-gray-200 px-3 py-1.5 rounded-xl">{s}</span>
            {i < arr.length - 1 && <ChevronRight className="w-4 h-4 text-gray-400 shrink-0" />}
          </React.Fragment>
        ))}
      </div>

      <H2>{t.h2Modules}</H2>
      <H3>{t.h3Agents}</H3>
      <ul className="mb-4 space-y-1">
        {t.agentItems.map((item, i) => <Li key={i}>{item}</Li>)}
      </ul>

      <H3>{t.h3Ingestion}</H3>
      <ul className="mb-4 space-y-1">
        {t.ingestionItems.map((item, i) => <Li key={i}>{item}</Li>)}
      </ul>

      <H3>{t.h3Transform}</H3>
      <P>{t.transformText}</P>

      <H3>{t.h3Query}</H3>
      <P>{t.queryText}</P>

      <H2>{t.h2TechStack}</H2>
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
  const t = useT().gs;
  return (
    <div>
      <SketchyBadge variant="peach" className="mb-4">{t.badge}</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">{t.title}</h1>

      <H2>{t.h2Prereqs}</H2>
      <ul className="mb-4 space-y-1">
        <Li>Python 3.11+</Li>
        <Li>Node.js 18+</Li>
        <Li>AWS CLI configured (<code className="bg-gray-100 px-1 rounded text-sm">aws configure</code>)</Li>
        <Li>AWS CDK CLI (<code className="bg-gray-100 px-1 rounded text-sm">npm install -g aws-cdk</code>)</Li>
        <Li>Docker (for building Docker-based Lambdas)</Li>
      </ul>

      <H2>{t.h2S1}</H2>
      <Code lang="bash">{`git clone https://github.com/marcoaanogueira/serverless_data_lake
cd serverless_data_lake

pip install -r requirements.txt
pip install -r requirements-dev.txt

cd frontend && npm install && cd ..`}</Code>

      <H2>{t.h2S2}</H2>
      <P>{t.s2Desc}</P>
      <Code lang="json">{`{
  "context": {
    "tenant": "my_company"
  }
}`}</Code>
      <P>{t.s2Note}</P>

      <H2>{t.h2S3}</H2>
      <Code lang="bash">{`cdk bootstrap   # first time only
cdk synth       # validate without deploying
cdk deploy      # deploy all stacks`}</Code>

      <H2>{t.h2S4}</H2>
      <P>{t.s4Desc}</P>

      <H2>{t.h2S5}</H2>
      <P>{t.s5Desc}</P>
      <Code lang="bash">{`python scripts/hash_password.py`}</Code>
      <P>{t.s5Note}</P>

      <H2>{t.h2S6}</H2>
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

      <H2>{t.h2S7}</H2>
      <Code lang="bash">{`curl -X POST https://<api-gateway>/ingest/ecommerce/orders \\
  -H "x-api-key: <your-api-key>" \\
  -H "Content-Type: application/json" \\
  -d '{"data": {"id": 1, "total": 99.90, "created_at": "2024-01-15T10:00:00"}}'`}</Code>

      <H2>{t.h2S8}</H2>
      <Code lang="bash">{`pytest tests/          # Python
ruff check .           # linting

cd frontend
npm run test:run       # Vitest
npm run lint           # ESLint`}</Code>
    </div>
  );
}

function SectionAPIAuth() {
  const t = useT().auth;
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">{t.badge}</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">{t.title}</h1>
      <P>{t.intro}</P>
      <Endpoint method="POST" path="/auth/login" desc={t.epDesc} />
      <H3>{t.h3Req}</H3>
      <Code lang="json">{`{
  "email": "admin@mycompany.com",
  "password": "your_password"
}`}</Code>
      <H3>{t.h3Res}</H3>
      <Code lang="json">{`{ "token": "abc123..." }`}</Code>
      <P>{t.note}</P>
    </div>
  );
}

function SectionAPIEndpoints() {
  const t = useT().endpoints;
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">{t.badge}</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">{t.title}</h1>
      <P>{t.intro}</P>

      <Endpoint method="GET"    path="/endpoints"                          desc={t.list} />
      <Endpoint method="POST"   path="/endpoints"                          desc={t.create} />
      <Endpoint method="GET"    path="/endpoints/{domain}/{name}"          desc={t.get} />
      <Endpoint method="PUT"    path="/endpoints/{domain}/{name}"          desc={t.update} />
      <Endpoint method="DELETE" path="/endpoints/{domain}/{name}"          desc={t.delete} />
      <Endpoint method="GET"    path="/endpoints/{domain}/{name}/versions" desc={t.versions} />
      <Endpoint method="GET"    path="/endpoints/{domain}/{name}/yaml"     desc={t.yaml} />
      <Endpoint method="GET"    path="/endpoints/{domain}/{name}/download" desc={t.download} />
      <Endpoint method="POST"   path="/endpoints/infer"                    desc={t.infer} />

      <H3>{t.h3Body}</H3>
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
      <P>{t.types} <code className="bg-gray-100 px-1 rounded text-xs">STRING VARCHAR INTEGER BIGINT FLOAT DOUBLE BOOLEAN TIMESTAMP DATE JSON ARRAY DECIMAL</code></P>
      <P>{t.modes} <code className="bg-gray-100 px-1 rounded text-xs">MANUAL</code> · <code className="bg-gray-100 px-1 rounded text-xs">AUTO_INFERENCE</code> · <code className="bg-gray-100 px-1 rounded text-xs">SINGLE_COLUMN</code></P>

      <H3>{t.h3Infer}</H3>
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
  const t = useT().ingestion;
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">{t.badge}</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">{t.title}</h1>
      <P>{t.intro}</P>

      <Endpoint method="POST" path="/ingest/{domain}/{endpoint_name}"       desc={t.single} />
      <Endpoint method="POST" path="/ingest/{domain}/{endpoint_name}/batch" desc={t.batch} />

      <H3>{t.h3Params}</H3>
      <div className="overflow-x-auto my-3">
        <table className="w-full text-sm border-2 border-gray-200 rounded-2xl overflow-hidden">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-2 text-left font-black text-gray-700">{t.thParam}</th>
              <th className="px-4 py-2 text-left font-black text-gray-700">{t.thDefault}</th>
              <th className="px-4 py-2 text-left font-black text-gray-700">{t.thDesc}</th>
            </tr>
          </thead>
          <tbody>
            <tr className="bg-white"><td className="px-4 py-2 font-mono text-xs">validate</td><td className="px-4 py-2">false</td><td className="px-4 py-2 text-gray-600">{t.paramValidate}</td></tr>
            <tr className="bg-gray-50"><td className="px-4 py-2 font-mono text-xs">strict</td><td className="px-4 py-2">false</td><td className="px-4 py-2 text-gray-600">{t.paramStrict}</td></tr>
          </tbody>
        </table>
      </div>

      <H3>{t.h3Single}</H3>
      <Code lang="json">{`// POST /ingest/ecommerce/orders
{ "data": {"id": 1, "total": 99.90, "created_at": "2024-01-15T10:00:00"} }

// response
{"status": "ok", "endpoint": "ecommerce/orders", "records_sent": 1, "validated": false}`}</Code>

      <H3>{t.h3Batch}</H3>
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
  const t = useT().plans;
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">{t.badge}</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">{t.title}</h1>
      <P>{t.intro}</P>

      <Endpoint method="GET"    path="/ingestion/plans"                 desc={t.list} />
      <Endpoint method="POST"   path="/ingestion/plans"                 desc={t.create} />
      <Endpoint method="GET"    path="/ingestion/plans/{plan_name}"     desc={t.get} />
      <Endpoint method="DELETE" path="/ingestion/plans/{plan_name}"     desc={t.delete} />
      <Endpoint method="POST"   path="/ingestion/plans/{plan_name}/run" desc={t.run} />

      <H3>{t.h3Body}</H3>
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
  const t = useT().transform;
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">{t.badge}</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">{t.title}</h1>
      <P>{t.intro}</P>

      <Endpoint method="GET"    path="/transform/jobs"                         desc={t.list} />
      <Endpoint method="POST"   path="/transform/jobs"                         desc={t.create} />
      <Endpoint method="GET"    path="/transform/jobs/{domain}/{job_name}"     desc={t.get} />
      <Endpoint method="PUT"    path="/transform/jobs/{domain}/{job_name}"     desc={t.update} />
      <Endpoint method="DELETE" path="/transform/jobs/{domain}/{job_name}"     desc={t.delete} />
      <Endpoint method="POST"   path="/transform/jobs/{domain}/{job_name}/run" desc={t.run} />
      <Endpoint method="GET"    path="/transform/executions/{execution_id}"    desc={t.poll} />

      <H3>{t.h3Body}</H3>
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
      <P>{t.scheduleNote}</P>
    </div>
  );
}

function SectionAPIQuery() {
  const t = useT().query;
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">{t.badge}</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">{t.title}</h1>
      <P>{t.intro}</P>

      <Endpoint method="GET" path="/consumption/query"  desc={t.queryDesc} />
      <Endpoint method="GET" path="/consumption/tables" desc={t.tablesDesc} />

      <H3>{t.h3Naming}</H3>
      <div className="overflow-x-auto my-3">
        <table className="w-full text-sm border-2 border-gray-200 rounded-2xl overflow-hidden">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-2 text-left font-black text-gray-700">{t.thRef}</th>
              <th className="px-4 py-2 text-left font-black text-gray-700">{t.thMaps}</th>
            </tr>
          </thead>
          <tbody>
            <tr className="bg-white"><td className="px-4 py-2 font-mono text-xs">domain.bronze.table</td><td className="px-4 py-2 text-gray-600">{t.bronze}</td></tr>
            <tr className="bg-gray-50"><td className="px-4 py-2 font-mono text-xs">domain.silver.table</td><td className="px-4 py-2 text-gray-600">{t.silver}</td></tr>
            <tr className="bg-white"><td className="px-4 py-2 font-mono text-xs">domain.gold.table</td><td className="px-4 py-2 text-gray-600">{t.gold}</td></tr>
          </tbody>
        </table>
      </div>

      <H3>{t.h3Example}</H3>
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
  const t = useT().agents;
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">{t.badge}</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">{t.title}</h1>

      <H2>{t.h2Ingestion}</H2>
      <P>{t.ingestionIntro}</P>

      <Endpoint method="POST" path="/agent/ingestion/plan"          desc={t.planDesc} />
      <Endpoint method="POST" path="/agent/ingestion/run"           desc={t.runDesc} />
      <Endpoint method="GET"  path="/agent/ingestion/jobs/{job_id}" desc={t.pollDesc} />

      <H3>{t.h3Req}</H3>
      <Code lang="json">{`{
  "openapi_url": "https://api.myservice.com/openapi.json",
  "interests": ["orders", "customers"],
  "token": "Bearer eyJ...",
  "domain": "ecommerce",
  "plan_name": "ecommerce_sync",
  "tags": ["hourly"]
}`}</Code>
      <P>{t.semanticNote}</P>

      <H3>{t.h3Async}</H3>
      <Code lang="json">{`{
  "job_id": "abc123",
  "status": "running",
  "poll_url": "/agent/ingestion/jobs/abc123"
}`}</Code>

      <H2>{t.h2Transform}</H2>
      <P>{t.transformIntro}</P>

      <Endpoint method="POST" path="/agent/transformation/plan"          desc={t.transformPlan} />
      <Endpoint method="POST" path="/agent/transformation/run"           desc={t.transformRun} />
      <Endpoint method="GET"  path="/agent/transformation/jobs/{job_id}" desc={t.transformPoll} />

      <H3>{t.h3Req}</H3>
      <Code lang="json">{`{
  "domain": "ecommerce",
  "tables": ["orders", "customers"],
  "trigger_execution": true
}`}</Code>
    </div>
  );
}

function SectionAPIChat() {
  const t = useT().chat;
  return (
    <div>
      <SketchyBadge variant="lilac" className="mb-4">{t.badge}</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">{t.title}</h1>
      <P>{t.intro}</P>

      <Endpoint method="POST"   path="/chat/message"               desc={t.messageDesc} />
      <Endpoint method="GET"    path="/chat/sessions"              desc={t.listDesc} />
      <Endpoint method="GET"    path="/chat/sessions/{session_id}" desc={t.getDesc} />
      <Endpoint method="DELETE" path="/chat/sessions/{session_id}" desc={t.deleteDesc} />

      <H3>{t.h3Send}</H3>
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
  const t = useT().arch;
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
      <SketchyBadge variant="lilac" className="mb-4">{t.badge}</SketchyBadge>
      <h1 className="text-4xl font-black text-gray-900 mb-4">{t.title}</h1>

      <H2>{t.h2DataFlow}</H2>
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

      <H2>{t.h2Fargate}</H2>
      <P>{t.fargateText}</P>

      <H2>{t.h2Schema}</H2>
      <P>{t.schemaText}</P>
      <Code lang="text">{`s3://{schema_bucket}/
├── schemas/{domain}/
│   ├── bronze/{table}/v1.yaml, latest.yaml
│   ├── silver/{table}/latest.yaml
│   └── gold/{job}/config.yaml
└── {tenant}/ingestion_plans/{plan}/config.yaml`}</Code>

      <H2>{t.h2Lambda}</H2>
      <div className="overflow-x-auto">
        <table className="w-full text-xs border-2 border-gray-200 rounded-2xl overflow-hidden">
          <thead className="bg-gray-50">
            <tr>
              {[t.thService, t.thType, t.thMem, t.thTimeout, t.thTrigger].map(h => (
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

      <H2>{t.h2Auth}</H2>
      <ul className="mb-4 space-y-1">
        {t.authItems.map((item, i) => <Li key={i}>{item}</Li>)}
      </ul>
    </div>
  );
}

const SECTIONS = {
  'overview':        SectionOverview,
  'getting-started': SectionGettingStarted,
  'api-auth':        SectionAPIAuth,
  'api-endpoints':   SectionAPIEndpoints,
  'api-ingestion':   SectionAPIIngestion,
  'api-plans':       SectionAPIPlans,
  'api-transform':   SectionAPITransform,
  'api-query':       SectionAPIQuery,
  'api-agents':      SectionAPIAgents,
  'api-chat':        SectionAPIChat,
  'architecture':    SectionArchitecture,
};

// ─── Main ──────────────────────────────────────────────────────────────────
export default function DocsPage({ onBack }) {
  const [active, setActive] = useState('overview');
  const [apiOpen, setApiOpen] = useState(false);
  const [lang, setLang] = useState('en');

  const t = TRANSLATIONS[lang];
  const nav = makeNav(t);
  const SectionComponent = SECTIONS[active] || SectionOverview;

  return (
    <LangCtx.Provider value={lang}>
      <div className="min-h-screen bg-white">
        <FloatingDecorations />

        <div className="fixed top-0 left-0 right-0 h-0.5 bg-gradient-to-r from-[#A8E6CF] via-[#C4B5FD] to-[#FECACA] z-50" />

        <nav className="fixed top-0 left-0 right-0 z-40 bg-white/90 backdrop-blur-sm border-b-2 border-gray-100 mt-0.5">
          <div className="max-w-7xl mx-auto px-6 h-14 flex items-center gap-4">
            <button
              onClick={onBack}
              className="flex items-center gap-1.5 text-sm font-semibold text-gray-500 hover:text-gray-900 transition-colors"
            >
              <ArrowLeft className="w-4 h-4" /> {t.nav.back}
            </button>
            <div className="w-px h-5 bg-gray-200" />
            <div className="flex items-center gap-2">
              <span className="font-black text-gray-900 text-sm">
                Tadpole<span className="text-[#A8E6CF]">.</span>
              </span>
            </div>
            <span className="text-gray-400 text-sm font-medium">{t.nav.docs}</span>
            <div className="ml-auto">
              <button
                onClick={() => setLang(l => l === 'en' ? 'pt_br' : 'en')}
                className="text-xs font-black px-3 py-1.5 rounded-xl border-2 border-gray-200 text-gray-600 hover:border-gray-400 hover:text-gray-900 transition-all"
                style={{ boxShadow: '2px 2px 0 rgba(0,0,0,0.06)' }}
              >
                {lang === 'en' ? 'PT-BR' : 'EN'}
              </button>
            </div>
          </div>
        </nav>

        <div className="flex pt-14 max-w-7xl mx-auto">
          <aside className="w-60 shrink-0 sticky top-14 h-[calc(100vh-3.5rem)] overflow-y-auto border-r-2 border-gray-100 py-6 px-4">
            <nav className="space-y-1">
              {nav.map((item) => {
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

          <main className="flex-1 min-w-0 px-10 py-10 max-w-3xl">
            <AnimatePresence mode="wait">
              <motion.div
                key={active + lang}
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

        <div className="fixed bottom-0 left-0 right-0 h-1.5 bg-gradient-to-r from-[#A8E6CF] via-[#C4B5FD] to-[#FECACA]" />
      </div>
    </LangCtx.Provider>
  );
}
