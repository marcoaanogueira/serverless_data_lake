# Security Remediation Plan — Serverless Data Lake

Audit realizado em todas as 15 lambdas + shared layers.
Nada deve ser aplicado sem aprovação — este documento é apenas o plano.

---

## Resumo

| Severidade | Qtd | Status |
|------------|-----|--------|
| CRITICAL   | 2   | query_api já corrigido, chat_api pendente |
| HIGH       | 5   | Todos pendentes |
| MEDIUM     | 4   | Todos pendentes |
| LOW        | 3   | Todos pendentes |

---

## CRITICAL

### 1. ~~SQL Injection — `query_api`~~ (JÁ CORRIGIDO)
Corrigido no commit anterior: `validate_query()`, blocklist de DDL/DML/funções de arquivo, limit de rows, sanitização de erros.

### 2. SSRF — `chat_api`
**Arquivo:** `lambdas/chat_api/main.py:60,74-76`

```python
url = f"{API_GATEWAY_ENDPOINT}/consumption/tables"
req = urllib.request.Request(url, headers=headers, method="GET")
with urllib.request.urlopen(req, timeout=30) as resp:
```

`API_GATEWAY_ENDPOINT` vem de env var (controlada pelo CDK), mas o `urlopen` segue redirects por padrão. Se o endpoint for comprometido ou mal configurado, é possível SSRF para `169.254.169.254` (metadata do EC2/Lambda) ou serviços internos.

**Plano:**
- Validar `API_GATEWAY_ENDPOINT` no startup (pattern match de URL esperado)
- Desabilitar redirects no urllib (`urllib.request.HTTPRedirectHandler` vazio)
- Ou trocar para `boto3` invocando o Lambda diretamente (sem HTTP)

---

## HIGH

### 3. Timing Attack na Comparação de API Key — `authorizer`
**Arquivo:** `lambdas/authorizer/main.py:55`

```python
is_authorized = api_key == expected_key
```

Comparação de string com `==` é vulnerável a timing attack. Um atacante pode descobrir a API key caractere por caractere medindo tempo de resposta.

**Plano:**
- Trocar para `hmac.compare_digest(api_key, expected_key)`
- Verificar que `lambdas/auth/main.py` já usa `hmac.compare_digest` (usa sim, linha 85) — só o authorizer está errado

### 4. Session Access sem Ownership Check — `chat_api`
**Arquivo:** `lambdas/chat_api/main.py:123-149`

```python
@app.get("/chat/sessions/{session_id}")
async def get_session(session_id: str):
    session = chat_store.get_session(session_id)  # qualquer um pode acessar
```

Qualquer usuário autenticado pode ler/deletar a sessão de outro, bastando saber o `session_id`.

**Plano:**
- Associar `user_id` (do token ou API key) à sessão no momento da criação
- Validar ownership em `get_session`, `delete_session`, `list_sessions`
- Filtrar `list_sessions` por `user_id`

### 5. CORS `allow_origins=["*"]` em TODAS as lambdas
**Arquivos:** `chat_api`, `endpoints`, `ingestion_agent`, `ingestion_plans`, `query_api`, `transform_jobs` — todos com:

```python
allow_origins=["*"],
allow_methods=["*"],
allow_headers=["*"],
```

**Plano:**
- Centralizar config de CORS em variável de ambiente `ALLOWED_ORIGINS`
- CDK injeta o domínio correto do frontend (CloudFront ou custom domain)
- Restringir `allow_methods` ao necessário por lambda (GET/POST/DELETE)

### 6. Unsafe YAML Dump com User Input — `ingestion_plans`
**Arquivo:** `lambdas/ingestion_plans/main.py:62,118`

```python
Body=yaml.dump(cfg, allow_unicode=True),
```

O `cfg` contém dados do request do usuário. Embora `yaml.dump()` seja seguro para escrita, se o downstream fizer `yaml.load()` (sem safe_load) os dados do user podem explorar desserialização.

**Plano:**
- Verificar que TODO downstream usa `yaml.safe_load()` (layers/shared já usa — OK)
- Adicionar validação Pydantic no `cfg` antes de serializar
- Considerar trocar para JSON em vez de YAML para configs gerados

### 7. Validação de Path Params — `endpoints`, `transform_jobs`
**Arquivo:** `lambdas/endpoints/main.py:226-237`, `lambdas/transform_jobs/main.py:162-168`

```python
@app.get("/endpoints/{domain}/{name}")
def get_endpoint(domain: str, name: str, version: Optional[int] = Query(None)):
```

`domain` e `name` são usados para construir S3 keys. Pydantic valida no modelo, mas os path params chegam antes da validação do modelo.

**Plano:**
- Adicionar `Path(pattern=r'^[a-z][a-z0-9_]*$')` nos parâmetros de rota
- Validar `version >= 1` quando não é `None`
- Aplicar o mesmo pattern em `transform_jobs` para `domain` e `job_name`

---

## MEDIUM

### 8. `execution_id` sem Validação — `transform_jobs`
**Arquivo:** `lambdas/transform_jobs/main.py:234-267`

```python
if arn.endswith(execution_id):  # match fraco
```

Usar `endswith` permite que strings curtas dêem match em múltiplas execuções.

**Plano:**
- Validar formato de `execution_id` (UUID ou formato do Step Functions)
- Usar match exato no ARN inteiro em vez de `endswith`

### 9. Batch Ingestion sem Limite de Tamanho — `serverless_ingestion`
**Arquivo:** `lambdas/serverless_ingestion/main.py:237-243`

```python
records: list[dict[str, Any]],  # sem limite de items
```

**Plano:**
- Adicionar `Field(max_length=1000)` ou validar `len(records) <= MAX_BATCH_SIZE`
- Retornar 400 se exceder

### 10. S3 Key Parsing sem Re-validação — `serverless_processing`, `serverless_processing_iceberg`
**Arquivo:** `lambdas/serverless_processing/main.py:152`, `lambdas/serverless_processing_iceberg/main.py:77-92`

```python
table_name = re.search(r"firehose-data/([^/]+)/", s3_object).group(1)
```

`[^/]+` pode incluir caracteres especiais. Valores extraídos não são revalidados.

**Plano:**
- Re-validar domain/table extraídos contra pattern `^[a-z][a-z0-9_]*$`
- Adicionar `try/except` no regex para evitar `AttributeError` em paths inesperados

### 11. Error Leakage em Job Status — `transformation_agent`
**Arquivo:** `lambdas/transformation_agent/main.py:411-424`

```python
"error": str(exc),  # pode conter credenciais ou paths internos
```

**Plano:**
- Sanitizar mensagem de erro antes de salvar no S3
- Reutilizar o mesmo pattern de `_friendly_error()` do query_api

---

## LOW

### 12. Rate Limiting Ausente — Todas as lambdas
Nenhum endpoint tem rate limiting. Vulnerável a brute force (auth) e DoS.

**Plano:**
- Adicionar throttling no API Gateway (CDK)
- Considerar AWS WAF para proteção adicional
- Não precisa mudar código das lambdas

### 13. Env Vars no DuckDB — `query_api`, `serverless_analytics`, `serverless_processing`

```python
con.execute(f"SET home_directory='{HOME_DIR}';")
```

Env vars controladas pelo CDK mas usam f-string sem validação.

**Plano:**
- Adicionar validação de formato nas env vars no startup de cada lambda
- Pattern: `^[a-zA-Z0-9/_-]+$` para paths

### 14. Auth Lambda Desusada — `auth`
Existe mas parece candidate para migração (comentários no código).

**Plano:**
- Decidir se mantém ou remove
- Se mantém: adicionar rate limiting e account lockout
- Se remove: limpar da stack CDK

---

## Ordem de Execução Sugerida

1. **Imediato (CRITICAL):** SSRF no chat_api (#2)
2. **Urgente (HIGH):** Timing attack no authorizer (#3) — fix de 1 linha
3. **Urgente (HIGH):** Session ownership no chat_api (#4)
4. **Próximo sprint (HIGH):** CORS (#5), YAML validation (#6), path params (#7)
5. **Backlog (MEDIUM):** execution_id (#8), batch limit (#9), S3 parsing (#10), error leakage (#11)
6. **Backlog (LOW):** Rate limiting (#12), env var validation (#13), auth cleanup (#14)
