# Plano de Integração: Nao + Serverless Data Lake

## Análise da Arquitetura do Nao

### O que o Nao realmente é
O Nao é um **framework de analytics agent** com arquitetura polyglot:

| Componente | Tecnologia | Função |
|---|---|---|
| **Backend principal** | TypeScript (Fastify + tRPC + Drizzle ORM) | Orquestra o agent loop, gerencia chats, memória, skills |
| **Agent core** | Vercel AI SDK (`ai` v6) + tool loop | Chama LLMs (Anthropic/OpenAI/Google/Mistral) com ferramentas |
| **FastAPI microservice** | Python | Apenas executa SQL já gerado contra databases configurados |
| **CLI (`nao-core`)** | Python | Gera contexto (metadata de tabelas) via `nao sync` |
| **Frontend** | React + Shadcn + tRPC client | Chat interface |
| **Banco de estado** | PostgreSQL ou SQLite (via Drizzle) | Persiste chats, users, memórias, feedback |

### Como o Text-to-SQL funciona no Nao
O fluxo é:
1. **Contexto**: O CLI Python (`nao sync`) extrai metadata dos databases e gera arquivos markdown (`columns.md`, `preview.md`, `description.md`) organizados em: `databases/type={type}/database={name}/schema={name}/table={name}/`
2. **System Prompt**: O backend TypeScript monta um system prompt com as memórias do user + regras + contexto das tabelas (markdown files)
3. **Agent Loop**: O Vercel AI SDK faz o loop agentic — o LLM recebe o prompt + pergunta do user → decide gerar SQL
4. **Tool `execute_sql`**: O agent chama a tool que faz POST para o FastAPI Python (`/execute_sql`)
5. **FastAPI**: Recebe o SQL pronto e executa no database configurado (DuckDB, Postgres, etc.)
6. **Resultado**: Volta pro agent que pode interpretar, gerar charts, ou iterar se houve erro

### Dependências críticas do backend
- `ai` (Vercel AI SDK v6) — core do agent loop
- `@ai-sdk/anthropic`, `@ai-sdk/openai`, etc. — providers
- `@duckdb/node-api` — DuckDB nativo em Node.js
- `better-auth` — autenticação
- `drizzle-orm` + `pg`/`better-sqlite3` — banco de estado
- `fastify` — server HTTP
- `@trpc/server` — API tipada
- `mcporter` — MCP (Model Context Protocol)

---

## Análise de Viabilidade: Nao no seu projeto

### O Problema Central
O backend do Nao é um **servidor stateful TypeScript** que:
- Roda como processo persistente (Fastify server)
- Mantém estado em PostgreSQL/SQLite (chats, memórias, users)
- Usa Vercel AI SDK (TypeScript-only) para o agent loop
- Gerencia sessões de chat com streaming
- Espera rodar em Docker com `supervisor` gerenciando Node + Python

**Seu stack**: Lambda (Python/FastAPI) + DuckDB + Iceberg — **serverless, stateless, Python**

### Incompatibilidades

| Aspecto | Nao | Seu Projeto | Compatível? |
|---|---|---|---|
| Runtime | Node.js 24 + Python 3.12 (Docker) | Python Lambda | ❌ |
| State | PostgreSQL/SQLite persistente | Stateless Lambda | ❌ |
| Agent framework | Vercel AI SDK (TypeScript) | N/A | ❌ |
| Server model | Long-running Fastify | Invocation-based Lambda | ❌ |
| DuckDB | `@duckdb/node-api` (Node binding) | DuckDB Python | ⚠️ diferente |
| SQL execution | FastAPI `/execute_sql` | Já tem seu query_api | ✅ equivalente |
| Context format | Markdown files em filesystem | Schema Registry YAML no S3 | ⚠️ adaptável |

---

## Opções de Integração

### Opção 1: Usar o Nao como serviço separado (Docker no ECS)
**Esforço**: Médio | **Reuso do Nao**: ~90%

Rodar o Nao como container no ECS Fargate, conectado ao seu DuckDB/Iceberg.

```
Frontend React → API Gateway → Lambda (proxy) → ECS Nao Container → DuckDB/Iceberg
```

**Prós**:
- Usa o Nao praticamente intacto
- Aproveita todo o agent loop, UI de chat, memória, feedback
- O Nao já suporta DuckDB nativamente

**Contras**:
- Novo serviço para gerenciar (ECS task, networking, etc.)
- Custo: container always-on ou cold start no Fargate
- O DuckDB do Nao usa binding Node.js, não Python — precisa apontar para seus arquivos Iceberg/Parquet
- Frontend do Nao é separado do seu React — precisa integrar ou usar iframe
- Duplicação: você já tem query_api no Lambda

### Opção 2: Extrair apenas o padrão Text-to-SQL e reimplementar em Python
**Esforço**: Médio-Alto | **Reuso do Nao**: ~20% (conceitos)

Pegar a estratégia de prompting do Nao e reimplementar com ferramentas Python.

```
Frontend React → API Gateway → Lambda (FastAPI + LangChain/Claude SDK) → DuckDB/Iceberg
```

**O que você reusa do Nao**:
- Estrutura do system prompt (formato de contexto, regras para o agent)
- Formato de contexto das tabelas (columns.md, preview.md)
- Padrão de ferramentas (execute_sql, grep, read)
- Lógica de iteração em caso de erro SQL

**O que você implementa**:
- Agent loop em Python (usando `anthropic` SDK ou LangChain)
- Adaptador do Schema Registry → contexto markdown
- Tool de execute_sql apontando para seu DuckDB existente
- Gerenciamento de chat/memória (DynamoDB ou S3)

**Prós**:
- Tudo serverless, tudo Python, tudo no seu stack atual
- Sem novo serviço para gerenciar
- Controle total sobre o comportamento
- Aproveita seu DuckDB + Iceberg que já funciona

**Contras**:
- Mais trabalho de implementação
- Precisa manter o agent loop você mesmo
- Não ganha updates do Nao automaticamente

### Opção 3: Híbrida — Nao no ECS para chat + seu Lambda para execução SQL
**Esforço**: Médio | **Reuso do Nao**: ~70%

Rodar o backend do Nao no ECS mas configurar o `execute_sql` para chamar seu Lambda query_api existente em vez do FastAPI interno.

```
Frontend → Nao (ECS) → [agent decide SQL] → chama seu Lambda query_api → DuckDB/Iceberg
```

**Prós**:
- Reusa agent loop e chat UI do Nao
- Sua camada de dados (DuckDB + Iceberg) continua no Lambda
- Separação de concerns clara

**Contras**:
- Precisa modificar o Nao (fork do execute-sql tool)
- Latência adicional (ECS → Lambda)
- Ainda precisa gerenciar ECS

---

## Recomendação

**Opção 2** é a mais alinhada com seu projeto por estas razões:

1. **Stack consistente**: Tudo continua Python + Lambda + serverless
2. **Você já tem 80% da infraestrutura**: query_api (DuckDB), Schema Registry (metadata das tabelas), Processing Lambda
3. **O "segredo" do Nao é o prompting, não o código**: O system prompt do Nao (como formata contexto, como instrui o LLM a gerar SQL) é o que realmente importa — e isso é portável para qualquer linguagem
4. **Implementação Python é simples**: O Anthropic SDK em Python suporta tool use nativo, sem precisar do Vercel AI SDK

### O que extrair do Nao para seu projeto:
1. **System prompt structure** — como apresentar metadata de tabelas ao LLM
2. **Context format** — `columns.md` + `preview.md` por tabela (adaptar do seu Schema Registry)
3. **Tool definitions** — `execute_sql`, `display_chart`, `suggest_follow_ups`
4. **Error handling pattern** — "loop until you fix the error"
5. **Memory/rules system** — regras do usuário para guiar queries

### Arquitetura proposta (Opção 2 — com AWS Strands Agents SDK):

```
React Frontend (Recharts)
    ↓ POST /chat/message
API Gateway
    ↓
Lambda "chat_api" (FastAPI + Strands Agents SDK)
    ├── Monta system prompt com contexto das tabelas (do Schema Registry)
    ├── Agent loop via Strands SDK com tools [execute_sql, display_chart]
    ├── Tool execute_sql → chama seu query_api Lambda internamente
    ├── Claude interpreta resultado → responde em linguagem natural
    ├── Tool display_chart → retorna spec de chart (type, data, config)
    └── Salva histórico do chat no DynamoDB/S3
```

---

## Detalhamento Técnico: Strands Agents SDK

### Por que Strands em vez do Anthropic SDK direto?

| Aspecto | Anthropic SDK direto | Strands Agents SDK |
|---|---|---|
| Agent loop | Implementar manualmente (while tool_use...) | Built-in — `agent("pergunta")` faz o loop todo |
| Tool definition | JSON schema manual | Decorador `@tool` simples |
| Multi-provider | Só Claude | Claude (Bedrock), OpenAI, LLama, etc. |
| Streaming | Implementar manualmente | Built-in |
| Observabilidade | Manual | OpenTelemetry integrado |
| Manutenção AWS | Você mantém | AWS mantém (open-source, Apache 2.0) |

### Estrutura do Agent com Strands

```python
# lambdas/chat_api/agent.py
from strands import Agent
from strands.models import BedrockModel
from tools import execute_sql, display_chart

model = BedrockModel(
    model_id="us.anthropic.claude-sonnet-4-20250514",
    region_name="us-east-1"
)

def create_agent(system_prompt: str):
    return Agent(
        model=model,
        system_prompt=system_prompt,
        tools=[execute_sql, display_chart]
    )
```

### Tools com Strands

```python
# lambdas/chat_api/tools.py
from strands.types.tools import tool

@tool
def execute_sql(query: str) -> dict:
    """Execute uma query SQL no data lake DuckDB/Iceberg.

    Args:
        query: A SQL query to execute against the data lake tables.

    Returns:
        Query results as a dict with columns and rows.
    """
    # Chama o query_api existente (invoke Lambda ou HTTP)
    ...

@tool
def display_chart(
    chart_type: str,
    title: str,
    data: list[dict],
    x_key: str,
    y_keys: list[str],
    config: dict | None = None
) -> dict:
    """Gera uma especificação de chart para o frontend renderizar.

    Args:
        chart_type: Type of chart (bar, line, area, pie, scatter).
        title: Chart title.
        data: Array of data points.
        x_key: Key in data to use for X axis.
        y_keys: Keys in data to use for Y axis values.
        config: Optional chart configuration (colors, labels, stacked, etc).

    Returns:
        Chart specification for frontend rendering.
    """
    return {
        "type": "chart",
        "chart_type": chart_type,
        "title": title,
        "data": data,
        "x_key": x_key,
        "y_keys": y_keys,
        "config": config or {}
    }
```

### System Prompt Builder

```python
# lambdas/chat_api/prompt.py
def build_system_prompt(tables_metadata: list[dict]) -> str:
    """
    Monta o system prompt no estilo Nao:
    - Regras gerais de SQL
    - Contexto de cada tabela (colunas, tipos, preview)
    - Instruções para usar display_chart quando fizer sentido
    """
    sections = [RULES_SECTION]

    for table in tables_metadata:
        sections.append(format_table_context(table))

    sections.append(CHART_INSTRUCTIONS)
    return "\n\n".join(sections)
```

---

## Detalhamento Técnico: Frontend Charts com Recharts

### Por que Recharts?

| Aspecto | Recharts | Chart.js | D3 |
|---|---|---|---|
| Integração React | Nativo (componentes React) | Wrapper necessário | Baixo nível |
| Bundle size | ~150KB | ~200KB | ~250KB |
| Curva de aprendizado | Baixa | Média | Alta |
| Declarativo | ✅ JSX puro | ❌ Imperativo | ❌ Imperativo |
| Composable | ✅ Componentes modulares | ❌ Config object | ❌ Manual |
| Já no ecossistema | Shadcn/Tailwind friendly | Sim | Sim |

### Componente ChartRenderer

O agent retorna uma `chart_spec` no response. O frontend detecta e renderiza:

```jsx
// src/components/chat/ChartRenderer.jsx
import {
  BarChart, Bar, LineChart, Line, AreaChart, Area,
  PieChart, Pie, ScatterChart, Scatter,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer
} from 'recharts';

const CHART_COMPONENTS = {
  bar: BarChart,
  line: LineChart,
  area: AreaChart,
  pie: PieChart,
  scatter: ScatterChart,
};

const SERIES_COMPONENTS = {
  bar: Bar,
  line: Line,
  area: Area,
  pie: Pie,
  scatter: Scatter,
};

const COLORS = ['#8884d8', '#82ca9d', '#ffc658', '#ff7c7c', '#8dd1e1'];

export function ChartRenderer({ spec }) {
  const { chart_type, title, data, x_key, y_keys, config } = spec;
  const ChartComponent = CHART_COMPONENTS[chart_type];
  const SeriesComponent = SERIES_COMPONENTS[chart_type];

  if (!ChartComponent) return null;

  return (
    <div className="my-4 p-4 bg-white rounded-xl border shadow-sm">
      {title && <h4 className="text-sm font-medium mb-2">{title}</h4>}
      <ResponsiveContainer width="100%" height={300}>
        <ChartComponent data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey={x_key} />
          <YAxis />
          <Tooltip />
          <Legend />
          {y_keys.map((key, i) => (
            <SeriesComponent
              key={key}
              dataKey={key}
              fill={COLORS[i % COLORS.length]}
              stroke={COLORS[i % COLORS.length]}
            />
          ))}
        </ChartComponent>
      </ResponsiveContainer>
    </div>
  );
}
```

### Formato da resposta do Agent

O agent retorna mensagens que podem conter texto e/ou charts:

```json
{
  "message_id": "abc123",
  "role": "assistant",
  "content": [
    {
      "type": "text",
      "text": "As vendas cresceram 23% no último trimestre. Aqui está o gráfico:"
    },
    {
      "type": "chart",
      "chart_type": "bar",
      "title": "Vendas por Mês",
      "data": [
        {"mes": "Jan", "vendas": 1200},
        {"mes": "Fev", "vendas": 1450},
        {"mes": "Mar", "vendas": 1780}
      ],
      "x_key": "mes",
      "y_keys": ["vendas"],
      "config": {"colors": ["#8884d8"]}
    }
  ]
}
```

### Componente ChatMessage atualizado

```jsx
// src/components/chat/ChatMessage.jsx
import { ChartRenderer } from './ChartRenderer';

export function ChatMessage({ message }) {
  return (
    <div className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'}`}>
      <div className="max-w-[80%] rounded-lg p-3">
        {message.content.map((block, i) => {
          if (block.type === 'text') {
            return <p key={i} className="text-sm">{block.text}</p>;
          }
          if (block.type === 'chart') {
            return <ChartRenderer key={i} spec={block} />;
          }
          return null;
        })}
      </div>
    </div>
  );
}
```

---

## Novo Lambda: `chat_api`

### Estrutura de arquivos
```
lambdas/chat_api/
├── Dockerfile
├── requirements.txt
├── main.py          # FastAPI + Mangum (padrão do projeto)
├── agent.py         # Strands Agent setup
├── tools.py         # execute_sql, display_chart
├── prompt.py        # System prompt builder
└── chat_store.py    # Persistência de chat (DynamoDB)
```

### Dependências (`requirements.txt`)
```
strands-agents
strands-agents-bedrock
fastapi
mangum
boto3
```

### Endpoints da API
```
POST /chat/message          → Envia mensagem, retorna resposta do agent
GET  /chat/sessions         → Lista sessões de chat do user
GET  /chat/sessions/{id}    → Histórico de uma sessão
DELETE /chat/sessions/{id}  → Deleta sessão
```

### CDK Config (em `API_SERVICES`)
```python
"chat_api": ApiServiceConfig(
    source_path="lambdas/chat_api",
    handler="main.handler",
    use_docker=True,  # Strands SDK precisa de Docker
    timeout=Duration.seconds(120),  # Agent loop pode demorar
    memory_size=512,
    api_routes=[
        ApiRoute(method="POST", path="/chat/message"),
        ApiRoute(method="GET", path="/chat/sessions"),
        ApiRoute(method="GET", path="/chat/sessions/{id}"),
        ApiRoute(method="DELETE", path="/chat/sessions/{id}"),
    ],
    environment={
        "BEDROCK_MODEL_ID": "us.anthropic.claude-sonnet-4-20250514",
        "CHAT_TABLE_NAME": "",  # DynamoDB table ref
    },
    grant_read=["schema_bucket"],
    grant_write=["chat_table"],
)
```

---

## Frontend: Novo módulo "Analyze"

### Integração no DataPlatform

O chat de analytics será um novo módulo no `DataPlatform.jsx`:

```
Extract → Transform → Load → **Analyze** (novo)
```

### Estrutura de componentes
```
src/components/analyze/
├── ChatInterface.jsx    # Container principal do chat
├── ChatMessage.jsx      # Renderiza mensagem (texto + charts)
├── ChartRenderer.jsx    # Renderiza charts com Recharts
├── ChatInput.jsx        # Input de mensagem com sugestões
└── SessionSidebar.jsx   # Lista de sessões anteriores
```

### API Client (`dataLakeClient.js`)
```javascript
// Novo namespace no dataLakeApi
chat: {
  sendMessage: (sessionId, message) =>
    api.post('/chat/message', { session_id: sessionId, message }),
  getSessions: () =>
    api.get('/chat/sessions'),
  getSession: (id) =>
    api.get(`/chat/sessions/${id}`),
  deleteSession: (id) =>
    api.delete(`/chat/sessions/${id}`),
}
```

---

## Plano de Implementação (ordem)

### Fase 1 — Backend Agent (Lambda `chat_api`)
1. Criar `lambdas/chat_api/` com Dockerfile e requirements
2. Implementar `prompt.py` — system prompt builder usando Schema Registry
3. Implementar `tools.py` — `execute_sql` (chamando query_api) + `display_chart`
4. Implementar `agent.py` — Strands Agent com Bedrock
5. Implementar `main.py` — FastAPI endpoints
6. Implementar `chat_store.py` — persistência DynamoDB
7. Adicionar ao CDK stack (`API_SERVICES` + DynamoDB table)

### Fase 2 — Frontend Chat + Charts
1. Instalar `recharts` no frontend
2. Criar `ChartRenderer.jsx` com suporte a bar/line/area/pie/scatter
3. Criar `ChatMessage.jsx` com renderização de texto + charts
4. Criar `ChatInput.jsx` com sugestões de perguntas
5. Criar `ChatInterface.jsx` como container principal
6. Adicionar módulo "Analyze" no `DataPlatform.jsx`
7. Adicionar namespace `chat` no `dataLakeClient.js`

### Fase 3 — Refinamentos
1. Streaming de respostas (SSE ou WebSocket via API Gateway)
2. Sugestões de follow-up do agent
3. Export de charts (PNG/SVG)
4. Memória do agent (regras do usuário persistidas)
5. Cache de queries frequentes
