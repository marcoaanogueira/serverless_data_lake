# Tadpole — Config-Driven dbt via S3

## O que é

Padrão central do Tadpole: em vez de commitar arquivos `.sql` e `.yml` estáticos no
repo do dbt, os modelos são gerados dinamicamente no startup do container ECS a partir
de configs YAML armazenados no S3.

Tema da talk técnica — implementação do pattern que a comunidade dbt pede como feature
nativa (issues #5101 e #11478 no dbt-core), rodando hoje com ECS + S3.

---

## Fluxo completo

```
S3 (YAML configs)
│   schemas/{domain}/gold/{job_name}/config.yaml
▼
ECS Task (Fargate) sobe
│
▼
entrypoint.py executa:
  1. fetch_all_job_configs()       →  lista todos os configs via S3 paginator
  2. compute_effective_tags()      →  calcula tag dbt por job (hourly/daily/monthly)
  3. generate_multi_model_project() → escreve .sql em /tmp/dbt_project/models/
  4. dbt run --select tag:{filter}
  5. write_all_to_iceberg()        →  PyIceberg lê Parquet e persiste no Glue Catalog
│
▼
Tabelas Iceberg materializadas no S3 Gold + Glue Catalog registrado
```

---

## Estrutura do YAML de configuração no S3

Caminho: `schemas/{domain}/gold/{job_name}/config.yaml`

```yaml
domain: starwars
job_name: people_per_planet
query: >
  SELECT p.homeworld, COUNT(*) AS people_count
  FROM starwars.silver.people p
  GROUP BY p.homeworld
write_mode: append          # overwrite | append
unique_key: homeworld       # coluna de dedup para append/upsert
schedule_type: cron         # cron | dependency
cron_schedule: day          # hour | day | month
dependencies: []            # job_names upstream (para schedule_type=dependency)
status: active
created_at: "2026-01-01T00:00:00"
updated_at: "2026-01-01T00:00:00"
```

Referências no `query` usam sintaxe amigável que o entrypoint reescreve:

- `domain.silver.table` → `catalog.domain_silver.table` (via `rewrite_query()`)
- `domain.gold.job_name` → `{{ ref('job_name') }}` (via `process_query_for_dbt()`, apenas para `schedule_type=dependency`)

---

## entrypoint.py — lógica detalhada

**Arquivo:** `containers/dbt_runner/entrypoint.py`

### Dois modos de execução

| Modo | Trigger | Comportamento |
|------|---------|---------------|
| `single` | API → Step Functions one-shot | Roda 1 job pelo nome |
| `scheduled` | EventBridge (cron) | Busca todos os jobs do S3, gera projeto multi-model, filtra por tag |

### Pipeline em dois estágios

**Stage 1 — dbt:** DuckDB in-memory conectado ao Glue Catalog via `ATTACH`. Executa a
query SQL e exporta resultado para Parquet via `post_hook: COPY ... TO ... (FORMAT PARQUET)`.

**Stage 2 — Iceberg:** `write_to_iceberg()` lê o Parquet com PyArrow, carrega (ou cria)
a tabela Iceberg no Glue Catalog, persiste no S3 Gold com modo `overwrite`, `append`, ou
upsert (DELETE + INSERT por `unique_key`).

### Funções-chave

```
rewrite_query()                 # domain.layer.table → catalog.domain_layer.table
process_query_for_dbt()         # substitui gold refs por {{ ref('job_name') }}
compute_effective_tags()        # propaga frequência de consumers p/ dependency jobs
fetch_all_job_configs()         # paginator S3 em schemas/*/gold/*/config.yaml
generate_dbt_project()          # single-mode: 1 modelo
generate_multi_model_project()  # scheduled-mode: N modelos com tags
run_dbt()                       # subprocess: dbt run --select tag:{filter}
write_to_iceberg()              # PyIceberg + Glue Catalog → S3 Iceberg
update_execution_status()       # persiste last_execution.yaml no S3
```

### Como o Glue é anexado

Macro `attach_glue_catalog()` injetada no `on-run-start` do `dbt_project.yml` gerado
dinamicamente:

```sql
ATTACH '{{ account_id }}' AS {{ catalog_name }} (
    TYPE iceberg,
    ENDPOINT 'glue.{{ region }}.amazonaws.com/iceberg',
    AUTHORIZATION_TYPE 'sigv4'
)
```

`profiles.yml` gerado com DuckDB `:memory:`, extensões `httpfs + aws + iceberg`, e
credentials via `credential_chain`.

### Tag computation para scheduled mode

Cron jobs: `cron_schedule` (hour/day/month) mapeia direto para tag (hourly/daily/monthly).

Dependency jobs: herdam a maior frequência entre todos os consumers downstream.
Resolvido iterativamente até estabilizar. Default: `daily`.

```python
SCHEDULE_TO_TAG = {"hour": "hourly", "day": "daily", "month": "monthly"}
FREQUENCY_ORDER = {"hourly": 0, "daily": 1, "monthly": 2}
```

---

## Materializações customizadas Iceberg

**Caminho:** `containers/dbt_runner/macros/materializations/`

O dbt-duckdb não tem suporte nativo a tabelas Iceberg em catálogos ATTACHed, então
foram criadas duas materializações customizadas em Jinja/SQL.

### `iceberg_table.sql` — full overwrite

```
Alvo: catalog.schema.table via {{ this.database }}.{{ this.schema }}.{{ this.identifier }}
Verifica existência via information_schema.tables
Primeira execução: CREATE TABLE AS SELECT (CTAS)
Execuções seguintes: DELETE FROM + INSERT INTO
```

- Comportamento determinístico: sempre substitui toda a tabela
- DuckDB gerencia o contexto de transação automaticamente
- Retorna um `Relation` para o dbt rastrear a tabela

### `iceberg_incremental.sql` — incremental com duas estratégias

```
Config: incremental_strategy ('append' | 'upsert'), unique_key (obrigatório p/ upsert)
Primeira execução: CTAS (igual ao iceberg_table)
append:  INSERT INTO direto
upsert:  DELETE WHERE (key_cols) IN (subquery) + INSERT INTO
         Suporta chave composta (unique_key como lista)
```

Validações em compile-time via `raise_compiler_error`:
- `upsert` sem `unique_key` → erro
- estratégia inválida → erro

---

## Infra (CDK)

**Método:** `create_transform_pipeline()` em `stack/serverless_data_lake_stack.py`

### Task Role

- `s3:*` em todos os buckets via `grant_read_write`
- Glue: `GetDatabase`, `GetDatabases`, `CreateDatabase`, `GetTable`, `GetTables`,
  `CreateTable`, `UpdateTable`, `DeleteTable`, `GetCatalog`, `GetCatalogs`

### Fargate Task Definition

```
family:   {tenant}-dbt-runner
memory:   2048 MiB
cpu:      1024 (1 vCPU)
arch:     X86_64 / LINUX
image:    containers/dbt_runner (CDK from_asset)
logs:     CloudWatch /ecs/{tenant}/dbt-runner
```

### Variáveis de ambiente (baked no container)

```
SCHEMA_BUCKET      → buckets["Artifacts"].bucket_name
SILVER_BUCKET      → buckets["Silver"].bucket_name
GOLD_BUCKET        → buckets["Gold"].bucket_name
AWS_REGION         → self.region
AWS_ACCOUNT_ID     → self.account
GLUE_CATALOG_NAME  → "tadpole"
TENANT             → tenant
```

### Mecanismo de trigger — Step Functions

Dois estados ECS com `IntegrationPattern.RUN_JOB`:

**`RunDbtSingle`** — triggered por `POST /transform/jobs/{domain}/{job_name}/run`:
recebe `domain`, `job_name`, `query`, `write_mode`, `unique_key` via `container_overrides`.

**`RunDbtScheduled`** — triggered por EventBridge schedules (hourly/daily/monthly):
recebe apenas `tag_filter` via `container_overrides`.

Ambos usam `assign_public_ip=True` (sem NAT Gateway) e `FargatePlatformVersion.LATEST`.
