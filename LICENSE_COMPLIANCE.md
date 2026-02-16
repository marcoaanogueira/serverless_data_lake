# License Compliance Analysis

## Summary

All dependencies used in this project are under **permissive open-source licenses** (MIT or Apache 2.0).
This is fully compatible with:
- Releasing the project as **open source**
- Building a **commercial SaaS** product on top of it

---

## Dependency License Matrix

### Core Data Processing

| Dependency | Version | License | SaaS OK? | Notes |
|---|---|---|---|---|
| **DuckDB** | 1.0.0 / 1.4.1 | MIT | Yes | Trademark guidelines apply to branding |
| **Polars** | 1.7.1 | MIT | Yes | |
| **PyArrow** | (transitive) | Apache 2.0 | Yes | |
| **deltalake** (delta-rs) | 0.20.0 | Apache 2.0 | Yes | |
| **PyIceberg** | 0.9.0 | Apache 2.0 | Yes | ASF project |

### Transformation (dbt)

| Dependency | Version | License | SaaS OK? | Notes |
|---|---|---|---|---|
| **dbt-core** | (via dbt-duckdb) | Apache 2.0 | Yes | |
| **dbt-duckdb** | 1.9.1 | Apache 2.0 | Yes | |
| **dbt Fusion** | N/A | **ELv2** | **CUIDADO** | **NAO usado**, mas evitar no futuro se for SaaS |

### Web Framework & API

| Dependency | Version | License | SaaS OK? | Notes |
|---|---|---|---|---|
| **FastAPI** | 0.114.0 | MIT | Yes | |
| **Mangum** | 0.17.0 | MIT | Yes | |
| **Pydantic** | 2.9.0 | MIT | Yes | |

### Infrastructure

| Dependency | Version | License | SaaS OK? | Notes |
|---|---|---|---|---|
| **AWS CDK** | 2.156.0 | Apache 2.0 | Yes | |
| **boto3** | 1.35.14 | Apache 2.0 | Yes | |
| **AWS Lambda Powertools** | 2.36.0 | MIT-0 | Yes | |

### Table Format Conversion

| Dependency | Version | License | SaaS OK? | Notes |
|---|---|---|---|---|
| **Apache XTable** | (via JAR) | Apache 2.0 | Yes | ASF incubating project |
| **JPype1** | 1.5.0 | Apache 2.0 | Yes | |

### Frontend

| Dependency | License | SaaS OK? |
|---|---|---|
| **React** | MIT | Yes |
| **Radix UI** (all packages) | MIT | Yes |
| **TanStack React Query** | MIT | Yes |
| **Tailwind CSS** | MIT | Yes |
| **Framer Motion** | MIT | Yes |
| **Recharts** | MIT | Yes |
| **Vite** | MIT | Yes |
| **Lucide React** | ISC | Yes |

### Utilities

| Dependency | License | SaaS OK? |
|---|---|---|
| **PyYAML** | MIT | Yes |
| **s3fs** | BSD-3-Clause | Yes |
| **SQLAlchemy** | MIT | Yes |

---

## Perguntas Específicas

### 1. Posso colocar um frontend na frente do dlt (dlthub)?

**Sim.** O dlt é licenciado sob **Apache 2.0**, que permite uso comercial, modificação e distribuição.
Você pode criar uma UI/frontend que use o dlt como biblioteca sem problemas.

> **Nota:** O dlt **não está sendo usado** no codebase atualmente. Se for adicioná-lo, a licença Apache 2.0 é compatível.

A dlthub oferece produtos comerciais separados (dlt+), mas a biblioteca open source é totalmente livre.

### 2. Usar dbt na parte de transform é de boa?

**Sim, totalmente.** O `dbt-core` e o `dbt-duckdb` são ambos **Apache 2.0**.
Você pode:
- Usá-los em produto open source
- Usá-los em SaaS pago
- Modificá-los
- Distribuí-los

**ALERTA IMPORTANTE:** O **dbt Fusion** (engine nova em Rust) usa **Elastic License v2 (ELv2)**, que **proíbe oferecer como serviço gerenciado/hosted** que compete com o dbt Cloud. Seu projeto usa `dbt-core` (Apache 2.0), então está seguro. **Não migre para dbt Fusion se pretende fazer SaaS.**

### 3. Posso usar DuckDB no query editor?

**Sim.** O DuckDB usa licença **MIT**, que é uma das mais permissivas que existe.
Empresas como **MotherDuck** (que levantou $100M+) construíram SaaS inteiro em cima do DuckDB.

**Cuidados com trademark:**
- Você pode usar o DuckDB no seu produto sem restrições de código
- Mas **não pode usar o nome "DuckDB" ou o logo** de forma que sugira endosso ou parceria oficial
- Se for mencionar DuckDB no marketing, deixe claro que é uma tecnologia de terceiros

---

## Recomendações para Lançamento

### Para Open Source

1. **Adicionar arquivo LICENSE** na raiz do projeto (atualmente não existe)
   - Recomendado: Apache 2.0 (compatível com todas as dependências)
   - Alternativa: MIT (mais simples, também compatível)
2. **Adicionar NOTICE file** listando atribuições das dependências Apache 2.0
3. Incluir copyright headers nos arquivos fonte

### Para SaaS Futuro

1. **Evitar dbt Fusion** - Continuar com dbt-core (Apache 2.0)
2. **Respeitar trademarks** - DuckDB, Apache Iceberg, Delta Lake têm guidelines de uso de marca
3. **Dual licensing** é uma opção - Open source com Apache 2.0, features premium com licença proprietária
4. Considerar **Business Source License (BSL)** ou **Server Side Public License (SSPL)** se quiser proteger contra concorrentes que hospedam seu código como serviço
5. Alternativa mais moderna: **Functional Source License (FSL)** - que converte para Apache/MIT após 2 anos

### Licenças que NÃO Estão no Projeto (Bom Sinal)

- Nenhuma dependência **GPL** ou **AGPL** (que forçariam copyleft)
- Nenhuma dependência **SSPL** (que restringiria SaaS)
- Nenhuma dependência **ELv2** ou **BSL** (que restringiria uso comercial)

---

## Conclusão

**O projeto está 100% limpo em termos de licenciamento.** Todas as dependências usam licenças permissivas (MIT ou Apache 2.0) que permitem tanto uso open source quanto comercial/SaaS sem restrições significativas. A única ação necessária é adicionar um arquivo LICENSE ao próprio projeto.
