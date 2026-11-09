# 🚀 Autonomous Enterprise Data Platform Modernization Suite (Release v2.1.0)
### AI Agent Skills & Deterministic CLI Tools: Databricks (Spark / Delta) ➔ GCP BigQuery + Dataform + Airflow

[![Release](https://img.shields.io/badge/Release-v2.1.0-blue.svg)](#-release-v210-highlights)
[![Architecture](https://img.shields.io/badge/Architecture-.py--First%20%2B%20Surgical%20LLM-blue.svg)](#-solution-architecture)
[![GCP Native](https://img.shields.io/badge/GCP-BigQuery%20%7C%20Dataform%20%7C%20Airflow-orange.svg)](#)
[![Cycle Time Speedup](https://img.shields.io/badge/Speedup-95x%20to%20280x%20(-99.6%25)-green.svg)](#-1-performance-and-finops-efficiency-audited-benchmark)
[![LLM Token Savings](https://img.shields.io/badge/Token%20Savings-98%25%20to%20100%25-brightgreen.svg)](#-1-performance-and-finops-efficiency-audited-benchmark)
[![Quality Gate](https://img.shields.io/badge/Gate%20Approval-100%25%20(BL__00--BL__15%20%2B%2037%20Rules%20%2B%20S1--S6)-red.svg)](#-2-architectural-quality-semantic-equivalence--zero-rework-guardrails)
[![Enterprise Scale](https://img.shields.io/badge/Scale-%2B2%2C400%20Backlog%20Items-purple.svg)](#-3-flow-governance-autonomous-unblocking--critical-path-analysis-2400-items)
[![Compliance](https://img.shields.io/badge/Sanitization-100%25%20Agnostic%20%26%20Compliant-success.svg)](#)

---

## 📌 Executive Summary & Architectural Motivation

Modernizing thousands of enterprise data pipelines from a legacy **Databricks (Spark / Delta Lakehouse)** platform to a cloud-native, serverless architecture on **Google Cloud Platform (BigQuery + Dataform + Cloud Composer / Airflow)** is one of the most critical engineering initiatives in enterprise data engineering.

### The Genesis: Replacing Fragile Prompt-Chains with `.py`-First Engineering
This suite was engineered to overcome the direct failure modes of an initial **multi-agent automation chain composed of ~10 chained subagent skills relying purely on LLM prompts**.

In real production workloads, delegating catalog discovery, cloud preflight checks, upstream lineage resolution, and Dataform code generation entirely to LLMs produced four severe bottlenecks:
1. **Exponential Token Costs & Runtime Latency:** Passing massive enterprise catalogs and multi-thousand-line Spark scripts into LLM context windows consumed **~2.8 million tokens** and took **12 hours and 20 minutes** for a cohort of just 14 cards (~52.9 min/card).
2. **Coordinate & Source Hallucinations:** Free-form generation altered GCP project IDs, hallucinated dataset names, generated single-argument `${ref("table")}` calls that failed compilation, and omitted critical upstream datasets from ticket documentation.
3. **False Blockers from IAM Blindness:** Prompt-based agents could not distinguish between a missing cloud asset (`404 Not Found`) and restricted local runner permissions (`403 Forbidden`), unnecessarily paralyzing backlog refinement squads.
4. **Hidden Backlog Dependencies & Broken Waves:** Predecessor dependencies were inverted, unstarted predecessor cards were overlooked, and generic orchestrator tickets were falsely assumed to have created physical tables.

To solve this at the root, the entire modernization factory was rebuilt around the **`.py`-First + Surgical LLM + Chained Micro-Batch** paradigm:
- **0 LLM Tokens for 95% of the Pipeline:** Central catalog discovery (`origin/main` lock), 6-pillar cloud readiness audit with Service Account Impersonation, 6-layer upstream reverse lineage, multi-target Dataform scaffolding, structural auto-healing, and PBI consistency assembly run **100% deterministically in Python**.
- **Surgical LLM Translation (~3k–5k tokens/card):** The LLM is invoked strictly for the semantic translation of the query body (`SELECT ...`) and for structured root-cause classification in impediment diagnosis.
- **Micro-Batch Queue Control (<=4 cards) & Translation Pause (`Exit Code 10`):** Enforces a strict pause whenever SQL translation is pending, guaranteeing zero incomplete boilerplate or placeholder generation in enterprise backlogs.

---

## 📊 1. Performance and FinOps Efficiency (Audited Benchmark)

The metrics below were audited in production during the continuous migration of an enterprise data warehouse:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              AUDITED PRODUCTION BENCHMARK                              │
├──────────────────────────┬─────────────────────────────┬───────────────────────────────┤
│ Metric                   │ Legacy Prompt-Only Chain    │ Modern .py-First + LLM Suite  │
├──────────────────────────┼─────────────────────────────┼───────────────────────────────┤
│ Cycle Time / Card        │ ~52.9 min (12h 20m / 14 cds)│ 11.3s (det.) / 30-90s (w/ LLM)│
│ Speedup Factor           │ Baseline (1x)               │ 95x to 280x faster (-99.6%)   │
│ Token Consumption        │ ~2,800,000 tokens / 14 cards│ 0 tokens (det.) / ~48k (total)│
│ Direct Token Savings     │ 0%                          │ 98.3% to 100% reduction       │
│ Quality Gate Pass Rate   │ 71.4% (frequent rework)     │ 100.0% (BL_00-BL_15 + 37 Rules)│
│ Cloud Blocker Precision  │ Frequent 403 False Blockers │ 0 False Positives / 0 Negatives│
│ Scalability Limit        │ ~1-2 concurrent cards       │ +2,400 Work Items via DAG     │
└──────────────────────────┴─────────────────────────────┴───────────────────────────────┘
```

### Audited Benchmark Highlights:
- **Redução de Tempo de 99,6% (Speedup de 95x a 280x):** Tempo de refinamento + auditoria reduzido de **~52,9 min/card** (12h 20min para 14 cards no modelo legado de 10 skills multi-LLM) para **~11,3s/card** no passe determinístico com cache de sessão ativo (`fcntl.flock`, TTL 15–30 min), ou **~30s a 90s/card** incluindo tradução cirúrgica de SQL do zero pelo LLM em micro-lotes controlados de até 4 cards.
- **Economia de 98% a 100% em Tokens de LLM:** Queda de **~2,8 milhões de tokens** (em lotes de 14 cards no fluxo legado) para **0 tokens** nas etapas determinísticas de descoberta, governança de 6 pilares, linhagem multi-hop de 6 camadas, scaffolding multi-target, auto-healing estrutural, validação semântica BL_00–BL_15 e montagem de PBI, acionando o LLM exclusivamente para tradução cirúrgica da query SQL (~3k–5k tokens/card) e extração estruturada de insumos de impedimento via modelo Flash (`gemini-2.5-flash`).

---

## 🛡️ 2. Architectural Quality, Semantic Equivalence & Zero-Rework Guardrails

- **100% de Aprovação no Quality Gate (37 Regras Arquiteturais + Validador Semântico BL_00–BL_15 + Suites S1–S6 + Compilação Real):**
  Evolução de **71,4%** (fluxo legado com retrabalho frequente) para **100.0% First-Time-Right**:
  * **Expurgo Automático de Artefatos Legados Proibidos (`_purge_forbidden_legacy_dataform_files` / `[FORBIDDEN_LEGACY_DATAFORM_ARTIFACTS_GUARD]`):** Remove e bloqueia scripts de backfill manual (`05_data_migration/backfill_*.sqlx`), DDLs avulsos (`00_ddl/create_*.sqlx`) e subdiretórios aninhados duplicados dentro do repositório Dataform.
  * **Auto-Healing Estrutural Pré-Auditoria (`_auto_heal_sqlx_before_audit`):** Utiliza um parser determinístico de chaves balanceadas para curar automaticamente desvios comuns antes da auditoria (remove comentários antes do `config {}`, funde blocos duplicados, converte `database:` hardcoded para `utils.projectId()`, restaura coordenadas contra `context_locked.json`, expande `${ref()}` de 1 argumento para 2 argumentos, corrige `GROUP BY ... AS` e limpa aliases redundantes).
  * **Suporte Nativo a Multi-Target & Consolidação:** Suporta pipelines multi-tabela, múltiplos DataFrames auxiliares, consolidação de planilhas/abas e inlining automático de bibliotecas filhas importadas via `%run`.
  * **Partition Pruning Escopado por Alias com Folga de Fuso Horário (`[PARTITION_PRUNING_UTC_OFFSET_GUARD]`):** Exige filtro direto sobre a coluna de partição física sem envolver a coluna em funções (`SAFE_CAST()`, `DATE()`) e obriga margem de segurança **D - 1** (`INTERVAL 1 DAY`) quando a partição estiver em UTC e o filtro de negócio em fuso local.
  * **Conversão Explícita de Timezone & Serialização Segura de Arrays/JSON:** Exige tradução de funções Spark legadas via `DATETIME(..., '<Timezone_Local>')` e protege representações vazias (`['[]']`) antes de serializações `TO_JSON_STRING(...)`.
  * **Equivalência Semântica de Cargas Incrementais:** Valida tradução de upsert (`type: "incremental"` + `uniqueKey`), insert-new-only (`NOT EXISTS (SELECT 1 FROM ${self()} ...)`) e delete-from-target (`DELETE FROM ${self()}` em pre/post_operations).
  * **Paridade Bidirecional de Tipos e Schemas:** Prioriza introspecção via Dataform CLI (`dataform compile --json`), views de contrato legado retrocompatíveis (`06_legacy_contracts`) e templates canônicos Airflow DAG Factory com proteção cross-project (`utils.projectId` + `vars.project_id`).

### Matriz dos Validadores Semânticos BL_00 a BL_15 (`business_logic_validator.py`):
```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        BUSINESS LOGIC & SEMANTIC PARITY MATRIX (BL_00–BL_15)           │
├───────┬──────────────────────────────────────────┬─────────────────────────────────────┤
│ Regra │ Nome do Validador Semântico              │ Garantia de Equivalência            │
├───────┼──────────────────────────────────────────┼─────────────────────────────────────┤
│ BL_00 │ BL_0_MISSING_TARGET_SQLX                 │ 100% dos destinos multi-target com  │
│       │                                          │ arquivo .sqlx correspondente        │
│ BL_01 │ BL_01_INCREMENTAL_DOWNGRADE_TO_TABLE     │ Impede downgrade acidental de       │
│       │                                          │ incremental para type: "table"      │
│ BL_02 │ BL_02_UPSERT_INCREMENTAL_FRAMEWORK       │ Valida type: "incremental" com      │
│       │                                          │ chave primária única (uniqueKey)    │
│ BL_03 │ BL_03_INSERT_NEW_ONLY_FRAMEWORK          │ Valida insert-new com self-check    │
│       │                                          │ NOT EXISTS (SELECT 1 FROM ${self()})│
│ BL_04 │ BL_04_DELETE_FROM_TARGET_FRAMEWORK       │ Valida DELETE FROM ${self()} em     │
│       │                                          │ pre_operations/post_operations      │
│ BL_05 │ PARTITION_PRUNING_UTC_OFFSET_GUARD       │ Filtro direto na partição com folga │
│       │                                          │ D - 1 (INTERVAL 1 DAY) vs UTC       │
│ BL_06 │ FROM_UTC_TIMESTAMP_GUARD                 │ Tradução Spark -> DATETIME(..., tz) │
│ BL_07 │ ARRAY_JSON_EMPTY_GUARD                   │ Proteção de arrays vazios (['[]'])  │
│       │                                          │ antes de TO_JSON_STRING             │
│ BL_08 │ ANTI_BOILERPLATE_GUARD                   │ Rejeita PENDING_LLM, 1=1, SELECT *  │
│ BL_09 │ CONTRACT_TEST_GUARD                      │ Integridade de arquivos e hashes    │
│ BL_10 │ ARCHITECTURAL_RULES_GUARD                │ 37 regras arquiteturais estruturais │
│ BL_11 │ DATAFORM_COMPILE_ERROR                   │ dataform compile --json real        │
│ BL_12 │ PARITY_4X4_GUARD                         │ SSOT x Spec JSON x Artefatos x PBI  │
│ BL_13 │ LEGACY_CONTRACT_PROJECTION               │ Aliases snake_col AS LegacyCol      │
│ BL_14 │ BIDIRECTIONAL_SCHEMA_TYPE_PARITY         │ Paridade Spark Schema vs BigQuery   │
│ BL_15 │ CROSS_SHEET_UNIFIED_JOIN                 │ Joins unificados em multi-abas      │
└───────┴──────────────────────────────────────────┴─────────────────────────────────────┘
```

---

## 🚦 3. Flow Governance, Autonomous Unblocking & Critical Path Analysis (+2,400 Items)

- **Mapeamento em Larga Escala do Caminho Crítico:**
  O motor de linhagem multi-hop em 6 camadas combinado ao classificador de **8 Raias de Cards Pais** (`critical_path_and_wave_analyzer.py`) e ao **Investigador Híbrido Vertex AI Gemini + Sondas Python** permitiram auditar o efeito cascata sobre mais de **2.400 itens de backlog**:
  * **97% dos Bloqueios em Cascata Concentrados em 8 Cards Pais:** Mais de 600 cards bloqueados dependiam de apenas 8 itens raiz. Ao priorizar esses 8 cards nas raias corretas, destravou-se a quase totalidade do backlog.
  * **38% Quick Wins Identificados:** Centenas de cards retidos aguardavam tabelas upstream que já estavam **100% materializadas e populadas no BigQuery (`num_rows > 0`)**, permitindo liberação imediata para desenvolvimento.
  * **Detecção de Armadilhas de Orquestradores Genéricos & Duplicatas:** Identificou cards marcados como `Done` que apenas migraram scripts agendadores mestres sem criar tabelas físicas filhas no DW, além de unificar cards duplicados via normalização canônica de caminhos.
  * **Correção de Inversões Topológicas de Waves:** Reordenou dependências onde cartões pais estavam alocados em waves posteriores aos seus cards filhos, eliminando bloqueios estruturais sem disparar conflitos de regras de transição de estado no Kanban.

---

## 🏛️ System Architecture Diagrams

### 1. The `.py`-First Architecture with Surgical Pause (`Exit Code 10`)

```mermaid
flowchart TD
    subgraph INGRESS["1. Ingestion & Live origin/main SSOT Lock (0 Tokens)"]
        GIT["git fetch origin main"] --> SSOT["load_latest_ssot_json()"]
        SSOT --> D32["Rule D-32: Filter Egress Endpoints (EMAIL/SFTP) & discarded_by"]
        D32 --> CTX[("context_locked.json (Immutable Contract)")]
    end

    subgraph PREFLIGHT["2. Shift-Left Preflight Checks & SA Impersonation (0 Tokens)"]
        CTX --> SKILL1["cloud-preflight-infra-auditor\n6 Pillars: Alias Normalization & SA Impersonation"]
        CTX --> SKILL2["data-lineage-upstream-validator\n6 Layers: Ghost Views & 0-Rows Lock"]
        
        SKILL1 -->|404 Not Found| BLK1["🔴 Hard Blocker: Generates Terraform IaC"]
        SKILL1 -->|403 with SA Impersonation| WRN1["🟡 Technical Note: Restricted Local Runner"]
        SKILL1 -->|200 OK| PASS1["🟢 Infra Ready"]
        SKILL2 -->|Upstream Missing| LIN6["6-Layer Cascaded Lineage Resolver"]
        SKILL2 -->|100% Populated| PASS2["🟢 Lineage Ready"]
    end

    subgraph SCAFFOLD["3. Deterministic Scaffolding & Multi-Target Artifacts (0 Tokens)"]
        PASS1 & PASS2 --> SKILL3_STAGE2["stage2_scaffold_dataform_and_review.py"]
        SKILL3_STAGE2 --> A1["1. definitions/01_sources/*.sqlx (utils.projectId)"]
        SKILL3_STAGE2 --> A2["2. definitions/03_silver/<table>.sqlx (${ref(2-arg)})"]
        SKILL3_STAGE2 --> A3["3. definitions/05_assertions/assert_<table>.sqlx (dependencies)"]
        SKILL3_STAGE2 --> A4["4. definitions/06_legacy_contracts/vw_<table>.sqlx"]
        SKILL3_STAGE2 --> A5["5. dags/<region>/<dag_id>.yaml (Airflow DAG Factory)"]
        SKILL3_STAGE2 --> DETECT{"Contains [PENDING_LLM_TRANSLATION]\nor SELECT *?"}
    end

    subgraph SURGICAL_PAUSE["4. Mandatory Surgical Pause (Anti-Skip Guard)"]
        DETECT -->|Yes| PROMPT["Generate *_surgical_llm_prompt.md\n(Full Legacy Code + %run + Partition Pruning D-1 Rules)"]
        PROMPT --> PAUSE["🛑 PAUSE: Exit Code 10 (AWAITING_SURGICAL_LLM)\nBlocks new cards until SQL translation completes"]
        PAUSE --> LLM["Surgical LLM: SELECT Body ONLY (~3k-5k tokens)"]
    end

    subgraph GATE["5. Fail-Closed Gate, BL_00–BL_15 & Unit Test Report (0 Tokens)"]
        LLM & DETECT -->|No / Ready| PURGE["_purge_forbidden_legacy_dataform_files\n(Backfills & DDLs Purge)"]
        PURGE --> HEAL["_auto_heal_sqlx_before_audit (Balanced-Brace Parser)"]
        HEAL --> GT["stage3_validate_refinement_gate.py"]
        GT --> BL["business_logic_validator.py (BL_00 to BL_15)"]
        GT --> V1["test_architect_consistency.py (37 Rules)"]
        GT --> V2["test_pbi_consistency.py (Suites S1–S6)"]
        GT --> V3["contract_test.py (4x4 Parity & Real dataform compile)"]
        BL & V1 & V2 & V3 -->|All Passed| UTR[("unit_test_report_*.md (Audited Report)")]
        UTR --> PUB["stage4_build_and_publish_pbi.py"]
        PUB --> ADO["Bidirectional Linker: Predecessor/Successor + Tag 'Refined'"]
        PUB --> TLM[("telemetry_run.jsonl with flock & os.replace")]
    end
```

---

### 2. Chained Micro-Batch Queue Flow (`refine_card_orchestrator.py`)

```mermaid
flowchart LR
    subgraph INPUT["Queue Ingestion & Auto-Reset"]
        CMD["--enqueue CARD-1 ... CARD-100"] --> RESET["Auto-Reset Session Cache"]
        RESET --> PERSIST[("refinement_batch_queue.json\nPersistent State File")]
    end

    subgraph WINDOW["Controlled Micro-Batch Execution (max 4 active)"]
        PERSIST --> BATCH["Active Window: Cards [1..4]"]
        BATCH --> P1["--phase pre-llm\nStage 1 & Stage 2 Scaffolding"]
        P1 --> CHECK{"SQL translation required?"}
        CHECK -->|Yes| EXIT10["🛑 Exit Code 10: Pause & Prompt\nWait for Surgical LLM Translation"]
        CHECK -->|No| P2["--phase post-llm\nPurge + Auto-Heal + BL_00–BL_15 + Gate 3"]
        EXIT10 --> LLM_DONE["LLM translates SELECT body"]
        LLM_DONE --> P2
        P2 --> UPSTREAM{"--upstream-checker?"}
        UPSTREAM -->|Yes| LIN_RUN["validate_lineage.py"]
        UPSTREAM -->|No| GCS["Publish Artifacts to GCS"]
        LIN_RUN --> GCS
        GCS --> PR_OPT{"--open-pr?"}
        PR_OPT -->|Yes| CLEAN_PR["Isolated Git Branch & PR"]
        PR_OPT -->|No| DONE_BATCH["Batch Completed"]
        CLEAN_PR --> DONE_BATCH
    end

    subgraph ADVANCE["Queue Advance & Safe Session Cache"]
        DONE_BATCH --> FLOCK[("Session Cache: fcntl.flock\nTTL 15-30m for READY results")]
        FLOCK --> NEXT["--next: Slide Window to Cards [5..8]"]
        NEXT --> BATCH
    end
```

---

### 3. Semantic Validator BL_00–BL_15 & Quality Gate Engine

```mermaid
flowchart TD
    ARTIFACTS["Modernized Artifacts (.sqlx / .yaml / PBI)"] --> GATE_RUN["stage3_validate_refinement_gate.py"]
    
    subgraph STAGE_3A["Pre-Audit Disinfection & Structural Auto-Healing"]
        GATE_RUN --> PURGE["_purge_forbidden_legacy_dataform_files\nExpurga 05_data_migration & 00_ddl"]
        PURGE --> HEAL["_auto_heal_sqlx_before_audit\nParser balanceado: config, utils.projectId, 2-arg ref, GROUP BY"]
    end

    subgraph STAGE_3B["Semantic Equivalence Engine (BL_00 to BL_15)"]
        HEAL --> BL_ENG["business_logic_validator.py"]
        BL_ENG --> BL1["BL_00: 100% Multi-Target Scaffolding Coverage"]
        BL_ENG --> BL2["BL_01-BL_04: Incremental Load Equivalence (upsert/insert-only/delete)"]
        BL_ENG --> BL3["BL_05-BL_07: Partition Pruning D-1 Margin, Timezone & Array JSON"]
        BL_ENG --> BL4["BL_08-BL_13: Anti-Boilerplate, Contracts & Legacy Views"]
        BL_ENG --> BL5["BL_14-BL_15: Introspecção CLI, Bidirecional & Cross-Sheet Joins"]
    end

    subgraph STAGE_3C["Compilation & 4x4 Parity"]
        BL_ENG --> ARCH["test_architect_consistency.py (37 Rules)"]
        ARCH --> DF["dataform compile --json real ([DATAFORM_COMPILE_ERROR])"]
        DF --> P4X4["contract_test.py (Paridade 4x4: SSOT x Spec x Code x PBI)"]
        P4X4 --> S_SUITES["test_pbi_consistency.py (Suites S1–S6)"]
    end

    S_SUITES -->|100% Passed| REPORT[("unit_test_report_*.md")]
```

---

### 4. Hybrid Impediment Investigator (Vertex AI Gemini + Deterministic Python Probes)

```mermaid
flowchart TD
    subgraph S1["Stage 1: LLM Semantic Extraction (Vertex AI Gemini 2.5 Flash)"]
        IMP["Card Trapped in IMP Column\n(System.History, Tags, Links, Spec)"] --> GEMINI["Vertex AI Gemini 2.5 Flash\nresponseSchema: application/json"]
        GEMINI --> TAX["Classification: IMP_C01 to IMP_C40"]
        GEMINI --> INP["Actionable Probe Inputs:\n• upstream_card_ids\n• required_bq_tables & datasets\n• required_service_accounts\n• required_secrets & buckets\n• github_prs_to_watch\n• awaiting_human_approval"]
        GEMINI --> HYP["suspicions_to_investigate (Hypotheses)"]
    end

    subgraph S2["Stage 2: Deterministic Python Probes Runner (python_imp_probe_runner.py)"]
        INP & HYP --> P1["ADO_PREDECESSOR_STATUS & ADO_CLONE_DONE_CHECK"]
        INP & HYP --> P2["BQ_DATASET_EXISTS & BQ_TABLE_EXISTS_AND_POPULATED"]
        INP & HYP --> P3["SECRET_MANAGER_EXISTS & GITHUB_PR_MERGED"]
        INP & HYP --> P4["ADO_HUMAN_APPROVAL_REPLY (Discussion Scan)"]
    end

    subgraph OUT["Executive Verdict & Board Action"]
        P1 & P2 & P3 & P4 --> VERDICT{"Consolidated Verdict"}
        VERDICT -->|All Cleared| UNBLOCK["🟢 READY_TO_UNBLOCK\nAuto-Promote to Ready & In-Place PATCH"]
        VERDICT -->|Partial| PARTIAL["🟡 PARTIALLY_UNBLOCKED\nHighlight Remaining Items in Comment"]
        VERDICT -->|Still Blocked| STILL["🔴 STILL_IMPEDED\nRetain Blocker & Generate SRE Task"]
    end
```

---

### 5. Autonomous Refinement Worker (Continuous Zero-PR Architecture for Airflow / Cloud Run)

```mermaid
flowchart TD
    START["Trigger: Scheduled Airflow DAG / Cloud Run Job\n(Lock: USE_SECRET_CREDENTIALS)"] --> SCAN1["Scan 'Refinement' Column for New Cards"]
    START --> SCAN2["Scan 'Impediments' Column for Blocked Cards"]

    subgraph REFINE_LOOP["Continuous Refinement Cycle"]
        SCAN1 --> PF{"cloud-preflight-infra-auditor (6 Pillars)"}
        PF -->|Blocked| TAG_IMP["Tag 'Infra-Blocked' & Move to Impediments via WEF"]
        PF -->|Cleared| REF["migration-spec-refiner (Micro-Batches <=4 Cards)"]
        REF --> HEAL_GATE["Expurgo + Auto-Healing + BL_00–BL_15 + 37 Rules + dataform compile"]
        HEAL_GATE --> PUB_GCS["Publish to gs://<artifacts-bucket>/cards/<id>/v1"]
        PUB_GCS --> OPT_PR{"Flag --open-pr active?"}
        OPT_PR -->|Yes| OPEN_PR["Create Clean Branch from main & Open PR"]
        OPT_PR -->|No| LIN_CHECK{"data-lineage-upstream-validator"}
        OPEN_PR --> LIN_CHECK
        LIN_CHECK -->|Approved| PROMOTE["Promote to 'Ready' (Zero-PR Required)"]
    end

    subgraph UNBLOCK_LOOP["Autonomous Impediment Resolution"]
        SCAN2 --> HYBRID["hybrid-impediment-investigator (Gemini + Probes)"]
        HYBRID -->|🟢 READY_TO_UNBLOCK| PROMOTE2["Promote to 'Ready' & In-Place PATCH"]
        HYBRID -->|🔴 STILL_IMPEDED| KEEP["Keep in IMP with SRE Playbook Details"]
    end
```

---

## 🚦 Enterprise Backlog Governance: The 8 Official Unblocking Lanes

Using `tools/critical_path_and_wave_analyzer.py` combined with the 6-layer reverse lineage engine, the platform modernizes large-scale backlogs with deep topological intelligence:

| Lane | Name & Strategic Focus | Operational Action |
| :---: | :--- | :--- |
| **Raia 1** | **Pai já Concluído (`Done`)** | Validar tabela física no BigQuery e destravar card filho imediatamente. |
| **Raia 2** | **Pai em Fase Final (`Validation` / `Backfill`)** | **Maior Quick Win:** Priorizar homologação de negócio para destravar lote. |
| **Raia 3A** | **Pai em Execução Ativa (`Refining` / `Ready` / `Test`)** | Acompanhar execução regular do squad produtor. |
| **Raia 3B** | **Pai Parado em `Pre-Wave`** | Puxar card pai para refinamento técnico imediatamente e atribuir dev. |
| **Raia 3C** | **Pai Também Bloqueado (`IMP` / `Task Dependency`)** | Atacar bloqueio raiz na infraestrutura para eliminar bloqueio em cadeia. |
| **Raia 4** | **Inversão Topológica de Wave** | Reallocar card pai para Wave <= filho ou postergar filho no planejamento. |
| **Raia 5** | **Armadilha de Orquestrador Concluído & Pai Cancelado** | Card `Done` migrou apenas script sem criar tabelas físicas no DW. |
| **Raia 6** | **Tabela Órfã sem Card Pai (`SEM_PBI`)** | Cadastrar novo PBI de ingestão/transformação no backlog imediatamente. |

---

## 🛠️ Complete Suite Guide: 4 Core Skills & 5 Complementary Tools

### Core Skills Directory (`skills/`)

| Skill | Directory | Core Purpose & Capabilities |
| :--- | :--- | :--- |
| **`cloud-preflight-infra-auditor`** | `skills/cloud-preflight-infra-auditor/` | 6-Pillar cloud & IaC readiness audit, alias normalization, strict non-prod protection, SA Impersonation cascade, live BigQuery smoke query (`SELECT 1`), cross-project IAM ACLs, AI/LLM Gateway remote functions, Secret Manager archetype governance, GCS tiering, Google Sheets live probes, Git repo verification, and copyable Terraform generation. |
| **`data-lineage-upstream-validator`** | `skills/data-lineage-upstream-validator/` | 6-Layer reverse lineage resolver, Ghost Views mapping, Non-AS-IS architectural consolidation, DW Live coordinate checks with SA Impersonation, 0-rows empty table lock, Dataform draft workspace guardrail (`is_draft_workspace`), sibling project scanner, and detection of 4 backlog governance anomalies. |
| **`migration-spec-refiner`** | `skills/migration-spec-refiner/` | .py-First technical refinement orchestrator with chained micro-batches (<=4 cards), translation pause (`Exit Code 10`), `%run` library extraction, Partition Pruning enforcement, multi-target scaffolding, forbidden files purge, pre-audit structural auto-healing, **BL_00–BL_15 business logic validator**, 37 architectural rules gate, real Dataform compile, and PBI publishing with suites S1–S6. |
| **`hybrid-impediment-investigator`** | `skills/hybrid-impediment-investigator/` | Deep two-stage impediment diagnosis: Stage 1 structured semantic extraction via Vertex AI Gemini 2.5 Flash (`responseSchema: application/json`, taxonomy `IMP_C01`–`IMP_C40`), and Stage 2 deterministic Python probes (`ADO_PREDECESSOR_STATUS`, `BQ_DATASET_EXISTS`, `BQ_TABLE_POPULATED`, `SECRET_MANAGER_EXISTS`, `GITHUB_PR_MERGED`, `HUMAN_APPROVAL`). |

### Complementary CLI Tools (`tools/`)

| Tool | CLI Script | Capabilities & Outputs |
| :--- | :--- | :--- |
| **Tool 1** | `tools/critical_path_and_wave_analyzer.py` | Standalone & comparative DAG analyzer for +2,400 items, 8 Official Unblocking Lanes, Opportunity Cost / Cascade reach, Quick Wins detection, and formatted **8-sheet Excel workbook (`.xlsx`)**, Markdown, JSON, and PowerPoint (`.pptx`). |
| **Tool 2** | `tools/dataform_cross_project_guard.py` | Cross-project boundary auditor (`-a-` vs `-p-`/`-i-`), forbidden files purge (`05_data_migration`, `00_ddl`), `utils.projectId(...)` injection, 2-arg ref enforcement, DAG YAML `vars.project_id`, assertion dependencies, `snake_case` / `GROUP BY` healing, Dataform CLI compile parity, and PR scope isolation. Formats: Markdown, JSON, Excel, PPTX. |
| **Tool 3** | `tools/board_workflow_governor.py` | Safe Kanban state transitions resolving dynamic WEF board extension fields (`System.BoardColumn`, `WEF_*_Kanban.Column.Done`), movement governance lock, atomic tag management (`Refined`, `Infra-Blocked`), batch bidirectional links, in-place `PATCH` comment updates, and synthesis of SRE infrastructure orders grouped by resource type. Formats: Markdown, JSON, Excel, PPTX. |
| **Tool 4** | `tools/executive_telemetry_exporter.py` | FinOps benchmark consolidator (Speedup 95x–280x, 0 tokens deterministic, 100% Quality Gate pass) generating **PowerPoint Widescreen 16:9 (`.pptx`)** dark-theme presentations and Excel spreadsheets (`.xlsx`). |
| **Tool 5** | `tools/autonomous_refinement_worker.py` | Continuous refinement worker for Airflow DAGs and Cloud Run Jobs with `USE_SECRET_CREDENTIALS` authentication lock: monitors Refinement & IMP columns, runs preflight, scaffolds with auto-healing and BL_00–BL_15 gates, publishes to `gs://`, optionally opens PR (`--open-pr`), and unblocks via Hybrid Investigator. |

---

## 💻 CLI Quickstart & Usage Examples

### 1. Run Autonomous Refinement Worker (Airflow / Cloud Run Mode)
```bash
python3 tools/autonomous_refinement_worker.py \
  --artifacts-bucket gs://acme-modernization-artifacts-prod \
  --max-cards 4 \
  --open-pr \
  --dry-run
```

### 2. Analyze Critical Path, 8 Unblocking Lanes & Export 8-Sheet Excel & PPTX
```bash
# Export Formatted 8-Sheet Excel Workbook
python3 tools/critical_path_and_wave_analyzer.py \
  --format excel \
  --detailed \
  --output /tmp/critical_path_8_lanes.xlsx \
  --dry-run

# Export Executive PowerPoint
python3 tools/critical_path_and_wave_analyzer.py \
  --format pptx \
  --output /tmp/critical_path_executive.pptx \
  --dry-run
```

### 3. Audit, Purge Forbidden Files & Auto-Heal Cross-Project Dataform Definitions
```bash
python3 tools/dataform_cross_project_guard.py \
  --artifacts-dir /path/to/dataform \
  --target-project acme-dwh-prod \
  --source-project acme-lakehouse-raw \
  --fix
```

### 4. Run Chained Micro-Batch Queue Refinement (.py-First with Exit Code 10)
```bash
# 1. Enqueue Batch of Cards
python3 skills/migration-spec-refiner/scripts/refine_card_orchestrator.py \
  --enqueue MIG-101 MIG-102 MIG-103 MIG-104 \
  --max-active 4 \
  --phase pre-llm

# 2. Resume After Surgical LLM SQL Translation
python3 skills/migration-spec-refiner/scripts/refine_card_orchestrator.py \
  --next \
  --upstream-checker \
  --open-pr
```

### 5. Investigate Trapped Cards via Hybrid Investigator (Gemini + Probes)
```bash
python3 skills/hybrid-impediment-investigator/scripts/hybrid_investigator_orchestrator.py \
  --history-json skills/hybrid-impediment-investigator/examples/sample_impediment_history.json \
  --output-md /tmp/impediment_verdict.md \
  --dry-run
```

### 6. Export FinOps Executive Presentation (16:9 Dark Theme .pptx / .xlsx)
```bash
python3 tools/executive_telemetry_exporter.py \
  --format pptx \
  --output /tmp/finops_executive_presentation.pptx \
  --dry-run
```

---

## 🔒 Compliance, Sanitization & Portability

- **Enterprise Neutrality:** All proprietary enterprise naming conventions, internal project acronyms, credentials, and tenant identifiers are 100% sanitized and abstracted via dynamic parameterization (`projectId(acronym)`).
- **Zero Hallucination Guarantee:** Architectural consistency, schema definitions, and cloud IAM configurations are verified deterministically by Python code before code review.
- **Fail-Closed Engineering:** If any cloud prerequisite, partition pruning guideline, or contract assertion fails, the pipeline halts immediately, preventing broken pipelines from entering enterprise production environments.
