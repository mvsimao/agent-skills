---
name: data-lineage-upstream-validator
description: >-
  Audits upstream data lineage across 6 verification layers before modernization pipelines advance
  to development. Features multi-target extraction, Non-AS-IS consolidation awareness, Ghost Views resolution,
  DW Live coordinate verification with SA Impersonation, 0-rows lock, Dataform draft workspace guardrail,
  sibling project scanner, 6-layer legacy lineage resolver, generic orchestrator trap detection,
  and 4 backlog governance anomalies detection.
---

# 🔗 Data Lineage Upstream Validator

Skill de pré-voo e auditoria profunda de linhagem de dados upstream em **6 camadas** para impedir o desenvolvimento fantasma (*phantom development*) — garantindo que tabelas de origem realmente existam, estejam populadas, contratos estejam alinhados e dependências de cards predecessores estejam mapeadas sem ambiguidades.

```mermaid
flowchart TD
    LEG["Código Legado: Spark / SQL / Ingestão"] --> EXTRACT["Extração Estruturada Multi-Target\nSeparação Leitura vs Escrita (02_bronze / 03_silver / 04_gold)"]
    EXTRACT --> GHOST["Resolução de Ghost Views & Promoção Canônica\n(Mapeamento para tabelas físicas & consolidações Non-AS-IS)"]

    GHOST --> C1{"Camada 1: BigQuery DW Live + SA Impersonation"}
    C1 -->|Existe & num_rows > 0| LIVE_OK["🟢 Verificado no DW Live"]
    C1 -->|Existe fisicamente mas .sqlx diverge| ALRT["🟢 Aprovado com Alerta Cirúrgico de Ajuste de .sqlx"]
    C1 -->|Existe com 0 rows & Predecessor não iniciado| B_UNPOP["🔴 Bloqueio: UNPOPULATED_UPSTREAM"]
    C1 -->|Não existe no DW| C2{"Camada 2: Fallback Dataform Repos (Primary + Sibling)"}

    C2 -->|Contrato mergeado na main| DF_OK["🟢 Aprovado: DATAFORM_VALIDATED (Esteira por Waves)"]
    C2 -->|Apenas em Workspace de Rascunho| B_DRAFT["🔴 Bloqueio: UNSTARTED_PREDECESSOR (is_draft_workspace)"]
    C2 -->|Divergência entre Projetos Irmãos| W_CROSS["🟡 Alerta: CROSS_PROJECT_MISMATCH"]
    C2 -->|Ausente em Repositórios| C3["Camada 3: Resolução de Linhagem Legada em 6 Camadas\n(resolve_legacy_lineage.py)"]

    subgraph L6["As 6 Camadas de Resolução Legada"]
        C3 --> L1["1. Expansão recursiva de Views"]
        C3 --> L2["2. Grafo soberano de jobs + Overrides de arquitetura"]
        C3 --> L3["3. Templates parametrizados de ingestão"]
        C3 --> L4["4. Match exato & radical (clean_stem) com score recorrente"]
        C3 --> L5["5. Reverse scan em metadados estáticos (normalização de paths)"]
        C3 --> L6A["6. Scanner AST / Regex de escrita (.saveAsTable, INSERT, MERGE)"]
    end

    L6 --> DISCOVERED["Produtor Legado Mapeado"]
    DISCOVERED --> C4{"Camada 4: Auditoria do Grafo de Tarefas & Detecção de Armadilhas"}
    C4 --> DETECT_GEN["Detecção de Armadilhas de Orquestradores Genéricos\n(Card Done que migrou apenas script sem materializar tabelas no DW)"]
    C4 --> DETECT_ANOM["Auditoria das 4 Anomalias de Governança:\n• INVERTED_LINK\n• UNLINKED_PREDECESSOR\n• PREDECESSOR_WITHOUT_PBI\n• OMITTED_SOURCES"]

    DETECT_GEN & DETECT_ANOM --> SEMAPHORE["Semáforo Executivo & Diagnóstico HTML em 3 Blocos\n(🟢 READY | 🟡 TASK DEPENDENCY | 🔴 BLOCKED)"]
```

---

## 🏛️ Os Pilares e Camadas de Verificação de Linhagem

### Extração Estruturada, Resolução de Ghost Views e Filtro Non-AS-IS
- **Separação Estrita de Fluxos:** Separa rigorosamente tabelas de leitura (*upstream sources*) das tabelas de escrita (*target tables* em `02_bronze`, `03_silver`, `04_gold`), suportando pipelines multi-target e fluxos de ingestão externa pura (JDBC, bancos relacionais, APIs REST sem fontes intermediárias no DW).
- **Resolução de Ghost Views e Promoção Canônica:** Mapeia automaticamente views lógicas legadas (que não existem mais como tabela física na nuvem) para suas tabelas físicas canônicas de streaming ou batch, promove schemas legados brutos para suas camadas prata correspondentes e reconhece consolidações arquiteturais (*Non-AS-IS*), impedindo falsos bloqueios ou criação de alvos fantasmas.

---

### Camada 1: Data Warehouse Live + SA Impersonation + Cruzamento de Coordenadas
- **Validação Física no BigQuery:** Consulta a API do BigQuery inspecionando existência física, `num_rows` e data de última modificação da partição, com fallback automático de *Service Account Impersonation*.
- **Alerta Cirúrgico de Ajuste de `.sqlx`:** Se a tabela existir fisicamente na coordenada mapeada na arquitetura, mas o arquivo `01_sources/*.sqlx` apontar para um projeto/dataset divergente, aprova o card emitindo alerta cirúrgico para ajuste imediato da coordenada no `.sqlx`.
- **Trava de Tabela Vazia (`UNPOPULATED_UPSTREAM`):** Se a tabela existir no BigQuery porém contiver `0 rows` (casca vazia) e o card produtor ainda estiver não iniciado (`Pre-Wave` ou `Refining`), bloqueia o avanço para desenvolvimento até que a carga seja completada.

---

### Camada 2: Fallback no Repositório Dataform + Guardrail de Rascunho + Projetos Irmãos
- **Aprovação por Waves de Modernização:** Em migrações faseadas, se a tabela física ainda não estiver materializada em produção mas seu contrato `.sqlx` já estiver mergeado no repositório Dataform principal, aprova o card (`DATAFORM_VALIDATED`).
- **Guardrail de Rascunho (`is_draft_workspace`):** Rejeita contratos que residem exclusivamente em workspaces locais ou branches de rascunho de cards predecessores não iniciados, bloqueando como `UNSTARTED_PREDECESSOR`.
- **Varredura de Projetos Irmãos:** Varre repositórios irmãos de ingestão e domínio para capturar inconsistências entre projetos analíticos e de processamento (`CROSS_PROJECT_MISMATCH`).

---

### Camada 3: Resolução de Linhagem Legada em 6 Camadas (`resolve_legacy_lineage.py`)
Quando uma tabela de origem está ausente no BigQuery, o motor descobre deterministicamente qual rotina legada a gera:
1. **Expansão Recursiva de Views:** Expande recursivamente views Hive, Spark SQL e Trino até encontrar as tabelas físicas base.
2. **Grafo Soberano de Jobs e Overrides Manuais:** Cruza o grafo de dependências da arquitetura corporativa respeitando overrides manuais para pipelines consolidados.
3. **Templates Parametrizados de Ingestão:** Analisa templates genéricos de ingestão (CDC, Debezium, Sqoop, Dataflow).
4. **Match Exato e Radical (`clean_stem`) com Score Recorrente:** Aplica análise morfológica de radicais de nomes de tabelas combinada a scores de execução frequente.
5. **Reverse Scan em Metadados Estáticos:** Varre arquivos de configuração YAML/JSON suportando dicionários ou listas e normalizando caminhos com barras, espaços e caracteres especiais.
6. **Scanner AST e Regex de Escrita:** Analisa o código-fonte Spark/Python por comandos explícitos de escrita (`.write`, `.saveAsTable`, `INSERT INTO`, `MERGE INTO`, `CREATE TABLE`), além de agendadores legados e tabelas dimensionais estáticas.

---

### Camada 4: Auditoria do Grafo de Tarefas, Detecção de Armadilhas & Semáforo Executivo
- **Detecção de Armadilhas de Orquestradores Genéricos:** Detecta e bloqueia vínculos com cards marcados como `Done` que apenas migraram scripts orquestradores maestros sem ter materializado as tabelas físicas filhas no Data Warehouse.
- **Auditoria das 4 Anomalias de Governança:**
  1. `INVERTED_LINK`: Predecessor erroneamente apontado como sucessor (dependência invertida/circular).
  2. `UNLINKED_PREDECESSOR`: Dependência real de código existente no backlog mas sem vínculo no board Kanban.
  3. `PREDECESSOR_WITHOUT_PBI`: Tabela de origem órfã sem nenhum card mapeado no backlog.
  4. `OMITTED_SOURCES`: Fontes lidas no código legado que foram omitidas na documentação do ticket.
- **Diagnóstico HTML em 3 Blocos:**
  - `📌 Resumo`: Visão executiva com Semáforo de Prontidão.
  - `🎯 Ao time de refinamento`: Ações exatas de correção de vínculos e priorização.
  - `🔬 Análise complementar`: Rastreabilidade detalhada de todas as fontes auditadas.

---

## 🚦 Semáforo Executivo de Governança

| Status | Semáforo | Descrição | Ação no Pipeline |
| :--- | :---: | :--- | :--- |
| **`READY`** | 🟢 | Todas as tabelas de origem existem fisicamente e estão populadas no DW. | Liberado para desenvolvimento imediato. |
| **`TASK DEPENDENCY`** | 🟡 | Dependência legítima mapeada para card predecessor em andamento. | Aguarda conclusão do predecessor. |
| **`CROSS-PROJECT MISMATCH`** | 🟡 | Tabela existe no projeto irmão mas declaração local diverge. | Ajustar coordenadas do `.sqlx`. |
| **`BLOCKED / GOVERNANCE GAP`** | 🔴 | Tabela com 0 rows, card predecessor inexistente (`SEM_PBI`) ou link invertido. | Bloqueado: escalonamento imediato. |

---

## 🛠️ CLI Execution & Usage

### 1. Execução Padrão de Linhagem Upstream
```bash
python3 skills/data-lineage-upstream-validator/scripts/validate_lineage.py \
  --config skills/data-lineage-upstream-validator/examples/sample_lineage_graph.json \
  --output-md /tmp/lineage_report.md \
  --fail-on-missing
```

### 2. Com SA Impersonation e Verificação de 0 Rows
```bash
python3 skills/data-lineage-upstream-validator/scripts/validate_lineage.py \
  --config /path/to/lineage_config.json \
  --impersonate-sa sa-dataform-executor@acme-dwh-prod.iam.gserviceaccount.com \
  --output-md /tmp/lineage_audit.md
```

### 3. Modo Dry-Run Simulado
```bash
python3 skills/data-lineage-upstream-validator/scripts/validate_lineage.py \
  --config skills/data-lineage-upstream-validator/examples/sample_lineage_graph.json \
  --dry-run \
  --output-md /tmp/lineage_dryrun.md
```
