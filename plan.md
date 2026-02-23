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

### Arquitetura proposta (Opção 2):
```
React Frontend
    ↓ POST /chat/message
API Gateway
    ↓
Lambda "chat_api" (FastAPI + Anthropic SDK)
    ├── Monta system prompt com contexto das tabelas (do Schema Registry)
    ├── Chama Claude com tools [execute_sql, display_chart]
    ├── Tool execute_sql → chama seu query_api Lambda internamente
    ├── Claude interpreta resultado → responde em linguagem natural
    └── Salva histórico do chat no DynamoDB/S3
```

### Novo Lambda necessário:
- `lambdas/chat_api/main.py` — FastAPI + Mangum (mesmo padrão do projeto)
- Dependências: `anthropic` SDK, seu `shared` layer
- Tools: reusa seu `query_api` existente para executar SQL

### Estimativa de componentes:
1. **System prompt builder** — lê Schema Registry, formata como markdown (similar ao Nao)
2. **Agent loop** — Anthropic SDK `messages.create()` com `tools` param
3. **Tool: execute_sql** — wrapper que chama seu DuckDB query existente
4. **Tool: display_chart** — retorna dados formatados pro frontend renderizar
5. **Chat persistence** — DynamoDB table para histórico
6. **Frontend chat component** — componente React de chat (pode se inspirar no Nao)
