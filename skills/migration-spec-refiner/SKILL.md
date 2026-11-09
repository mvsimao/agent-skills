---
name: migration-spec-refiner
description: >-
  Orchestrates modern data platform migration refinement using a .py-First + Surgical LLM architecture
  with chained micro-batch queue control (<=4 cards) and Exit Code 10 translation pause.
  Fetches live origin/main SSOT, supports multi-target scaffolding, resolves %run imported libraries,
  enforces BigQuery Partition Pruning, applies pre-audit auto-healing, validates 37 architectural rules
  plus dataform compile, and publishes verified PBIs across suites S1–S6 with atomic telemetry and clean PR staging.
---

# ⚙️ Migration Spec Refiner (.py-First + Surgical LLM + Micro-Batch Queue)

End-to-end technical refinement orchestrator skill founded on the **`.py`-First + Surgical LLM + Chained Micro-Batch** architecture, replacing 10 legacy multi-agent prompt chains with **4 deterministic Python engines** + self-contained architectural and semantic validator suites (`scripts/validators/`).

```mermaid
flowchart TD
    subgraph QUEUE["Persistent Chained Batch Queue Engine (refine_card_orchestrator.py)"]
        ENQ["--enqueue <ids...> (100+ cards)"] --> PERSIST[("refinement_batch_queue.json\nSession Cache: flock + TTL 15-30m")]
        PERSIST --> WIN["Controlled Window: --max-active 1 to 4 cards"]
    end

    subgraph STAGE1["Stage 1: Live origin/main SSOT Lock, %run & Context Resolution"]
        WIN --> FETCH["git fetch origin main"]
        FETCH --> S1["resolve_refinement_context.py\n• Regional repositories (sam/nam/eur) & hmg -> dev\n• Universal view canonization (vw*)\n• %run extraction (imported_libraries_code)\n• schedule_cron & partition_field (Partition Pruning)\n• AST legacy_columns dictionary\n• Rule D-32 filter (Egress & discarded_by)"]
        S1 --> CTX[("context_locked.json (Immutable Contract)")]
    end

    subgraph STAGE2["Stage 2: Multi-Target Scaffolding & snake_case Auto-Healing"]
        CTX --> S2["scaffold_dataform_and_review.py\n• Helper projectId(acronym) in includes/utils.js\n• 01_sources/*.sqlx with utils.projectId()\n• 03_silver/04_gold with partitionBy, columns, ${ref(2-args)}\n• 05_assertions with dependencies\n• 06_legacy_contracts: backward-compatible views\n• dags/<region>/<dag_id>.yaml (Airflow DAG Factory)"]
        S2 --> DETECT{"Contains [PENDING_LLM_TRANSLATION]\nor SELECT *?"}
    end

    subgraph SURGICAL["Mandatory Surgical Translation Pause (Anti-Skip Guard)"]
        DETECT -->|Yes| PROMPT["Generate *_surgical_llm_prompt.md\n(Full Legacy Code + %run + Partition Pruning D-1 Margin)"]
        PROMPT --> PAUSE["🛑 MANDATORY PAUSE: Exit Code 10 (AWAITING_SURGICAL_LLM)\nAnti-skip guard: blocks new cards until LLM finishes SQL translation"]
        PAUSE --> LLM["Surgical LLM: SELECT Body ONLY (~3k-5k tokens)"]
    end

    subgraph STAGE3["Stage 3: Pre-Audit Auto-Healing + BL_00–BL_15 + Fail-Closed Gate"]
        LLM & DETECT -->|No / Ready| S3["validate_refinement_gate.py"]
        S3 --> PURGE["_purge_forbidden_legacy_dataform_files\n(Removes 05_data_migration & 00_ddl)"]
        PURGE --> HEAL["_auto_heal_sqlx_before_audit (Balanced-Brace Parser)\n• Merges duplicate configs\n• Converts database: hardcoded -> utils.projectId()\n• Expands ref 1-arg -> 2-args\n• Corrects GROUP BY AS & cleans redundant aliases"]
        HEAL --> BL["validators/business_logic_validator.py (BL_00 to BL_15)"]
        BL --> G1["[ANTI_BOILERPLATE_GUARD] & [CONTRACT_TEST_GUARD]"]
        G1 --> G2["test_architect_consistency.py (37 Architectural Rules)"]
        G2 --> G3["dataform compile --json real ([DATAFORM_COMPILE_ERROR])"]
        G3 --> G4["4x4 Parity Check (SSOT, Spec JSON, Disk, PBI)"]
    end

    subgraph STAGE4["Stage 4: Parity by Construction, Suites S1–S6 & Publishing"]
        G4 --> S4["build_and_publish_pbi.py\n• Section 1 & 3.3 inlined from identical in-memory canonical list\n• test_pbi_consistency.py (Suites S1 to S6)\n• Clickable links in Section 4.1\n• Predecessor -> Successor topological linking\n• Tag 'Refined' on Board\n• Atomic telemetry (flock + os.replace)"]
        S4 --> PR_CHECK{"Flag --open-pr active?"}
        PR_CHECK -->|Yes| GIT_PR["Creates clean branch from main, stages artifacts strictly & opens PR"]
        PR_CHECK -->|No| NEXT_CARD["Advances to next cards in the chained micro-batch queue"]
    end
```

---

## 🏛️ The 4 Deterministic Stages & Queue Architecture

### Persistent Chained Batch Queue Engine (`refine_card_orchestrator.py`)
- **Two-Phase Execution Modes:** Supports `--phase pre-llm` (Stages 1 and 2 up to surgical pause) and `--phase post-llm` (Purge, Auto-healing, Gate 3, BL_00–BL_19, Dry-Run, Compilation, and Publishing).
- **High-Throughput Enqueueing:** CLI commands `--enqueue <ids...>`, `--next`, `--queue-status`, `--clear-queue` with configurable concurrency windows (`--max-active 1` to `4`).
- **Closed Self-Healing Loop via Vertex AI Gemini (`--auto-llm` / `--self-heal-llm` — Wave 3):** When enabled, instead of pausing on Exit Code 10, invokes `gemini-3.8-flash` on Vertex AI (global endpoint, `temperature=0.1`) in a closed loop of up to 3 retries, injecting compiler tracebacks, live BigQuery dry-run diagnostics, and BL_00–BL_19 violation traces for surgical self-healing, while deterministically reapplying the SSOT coordinate lock after each generation.
- **Anti-Skip Translation Lock (`Exit Code 10 - AWAITING_SURGICAL_LLM`):** Prevents progressing new cards while any active card awaits surgical SQL translation from the LLM when `--auto-llm` is not passed, ensuring zero boilerplate or placeholder leaks in production.
- **Session Cache Auto-Reset:** Automatically resets temporary caches upon initializing a new queue (`--enqueue`/`--clear-queue`), maintaining a **Secure Session Cache (`fcntl.flock`, TTL 15–30 min for positive `READY` outcomes)** between pipeline phases.
- **Continuous Integration Pipeline:** Integrated flags `--upstream-checker` (triggers lineage audit immediately following Stage 4) and `--open-pr` (branches cleanly from `origin/main`, stages strictly batch artifacts, validates Gate 3, and opens/updates the Pull Request).

---

### Stage 1: Live `origin/main` SSOT Lock, Upstream BQ Inspection & AST Reconciliation (`resolve_refinement_context.py`)
- **Authoritative `origin/main` Synchronization:** Runs `git fetch origin main` and loads canonical catalogs directly from the default branch, resolving regional repositories (`sam`, `nam`, `eur`), non-production redirect safeguards (`hmg` ➔ `dev`), universal view canonization (`vw*`), and anti-self-dependency guards.
- **Live Physical BigQuery Metadata & Column Drift Detection (Wave 1):** In addition to static definitions, live-inspects upstream BigQuery physical schemas (`live_bq_schema`), overlays outdated static partitioning by physical attributes (`live_partition_field`, `live_partition_type`, `require_partition_filter`, `clustering_fields`), and automatically computes column drift (`schema_drift_missing_in_bq`) between legacy dictionary references and physical GCP tables.
- **AST x Catalog Reconciliation & BigQuery Auto-Discovery (Wave 2):** Scans the AST/regex of legacy code and `%run` libraries to detect uncataloged auxiliary join tables (`ast_reconciliation`), auto-discovering their live GCP physical coordinates across candidate projects and injecting them into `ctx["canonical_sources"]` (or flagging `unmapped_blocking` if missing).
- **Multi-CSV Destination Derivation (`BL_19`):** When legacy scripts export multiple CSV files collapsed under one catalog entry, derives BigQuery target table names from actual CSV file stems with reverse mapping to canonical Dataform tables.
- **Discarded Targets & Egress Filter (Rule D-32):** Filters out targets marked `discarded_by` and non-table egress destinations (`EMAIL`, `SFTP`, `WEBHOOK`, `DATA_APPLICATION_API`).
- **Immutable Contract:** Persists `context_locked.json` recording the verified state prior to any scaffolding.

---

### Stage 2: Multi-Target Scaffolding, Canonical DAG YAML & Physical Schema Prompt (`scaffold_dataform_and_review.py`)
Deterministically scaffolds the complete suite of artifacts for **all card destinations (`ctx["targets"]` - native Multi-Target support)**:
1. `definitions/01_sources/*.sqlx`: Reuses existing definitions from `main` without duplication; injects `database: utils.projectId("<acronym>")` on cross-project sources (`p-*` / `i-*` read by `a-*`).
2. `definitions/03_silver/<domain>/<table>.sqlx` (or `04_gold`):
   - Injects `config { type: "incremental", bigquery: { partitionBy: "..." }, columns: { ... } }`.
   - References upstream tables strictly via `${ref("schema", "table")}` (2 arguments).
   - Normalizes column identifiers to `snake_case` without breaking `GROUP BY` syntax.
3. `definitions/05_assertions/<domain>/assert_unique_<table>.sqlx`:
   - Injects mandatory `dependencies: ["<target_table>"]` in the `config {}` block.
4. `definitions/06_legacy_contracts/<domain>/vw_<table>.sqlx`:
   - Backward-compatible contract view with reverse projection (`snake_col AS LegacyCol`).
5. `dags/<region>/<dag_id>.yaml`:
   - Canonical **Airflow DAG Factory** YAML with `"vars": {"project_id": "<canonical_source_project>"}` in cross-project setups.
- **Injection of Physical BigQuery Upstream Metadata (Wave 1):** Injects physical upstream column lists, `require_partition_filter` requirements, and column drift warnings into `*_surgical_llm_prompt.md`, eliminating `Unrecognized name` errors on first compilation.
- **Mandatory Surgical Pause & Partition Pruning Rules:** If any generated file contains `[PENDING_LLM_TRANSLATION]` or `SELECT *`, emits `*_surgical_llm_prompt.md` and pauses execution with **`Exit Code 10`** unless `--auto-llm` is enabled.

---

### Stage 3: Forbidden Artifact Purge + Pre-Audit Auto-Healing + BL_00–BL_19 Gate + BigQuery Live Dry-Run + 5/5 Confidence Scorecard (`validate_refinement_gate.py`)
- **Automated Legacy Artifact Purge (`_purge_forbidden_legacy_dataform_files` / `[FORBIDDEN_LEGACY_DATAFORM_ARTIFACTS_GUARD]`):**
  Removes and blocks manual backfill scripts (`05_data_migration/backfill_*.sqlx`), standalone DDLs (`00_ddl/create_*.sqlx`), and nested duplicate subdirectories.
- **Pre-Audit Structural Auto-Healing (`_auto_heal_sqlx_before_audit`):**
  Deterministic balanced-brace parser curing syntax deviations prior to audit.
- **Business Logic & Semantic Parity Validator (`validators/business_logic_validator.py` — Rules BL_00 to BL_19):**
  * `BL_00_MISSING_TARGET_SQLX`: Guarantees 100% of targets from multi-target cards possess `.sqlx` definitions;
  * `BL_01_INCREMENTAL_DOWNGRADE_TO_TABLE` & `BL_01_ROLLING_WINDOW_TABLE_WIPES_HISTORY`: Blocks accidental downgrade of incremental models to `type: "table"` and blocks rolling window table wipes on historical pipelines;
  * `BL_02_UPSERT_INCREMENTAL_FRAMEWORK`: Validates upsert semantics (`type: "incremental"` + `uniqueKey`);
  * `BL_03_INSERT_NEW_ONLY_FRAMEWORK`: Validates insert-new-only logic (`NOT EXISTS (SELECT 1 FROM ${self()} ...)`);
  * `BL_04_DELETE_FROM_TARGET_FRAMEWORK`: Validates delete-from-target (`DELETE FROM ${self()}` in `pre_operations`);
  * `BL_05_PARTITION_PRUNING_UTC_OFFSET_GUARD`: Audits alias-scoped partition pruning on large tables, guarding against missing D - 1 margins between UTC partitions and local filters;
  * `BL_06_FROM_UTC_TIMESTAMP_GUARD`: Blocks Spark `from_utc_timestamp`, enforcing `DATETIME(..., '<Timezone_Local>')`;
  * `BL_07_ARRAY_JSON_EMPTY_GUARD`: Guards empty array serializations (`['[]']`) before `TO_JSON_STRING`;
  * `BL_08_ANTI_BOILERPLATE_GUARD`: Rejects `[PENDING_LLM_TRANSLATION]`, `TODO`, `PLACEHOLDER`, `1=1`, and `SELECT * FROM`;
  * `BL_09_CONTRACT_TEST_GUARD`: Executes `validators/contract_test.py` to inspect on-disk file integrity;
  * `BL_10_ARCHITECTURAL_RULES_GUARD`: Evaluates the **37 architectural consistency rules**;
  * `BL_11_DATAFORM_COMPILE_ERROR`: Executes real `dataform compile --json` compilation;
  * `BL_12_PARITY_4X4_GUARD`: Asserts 4x4 parity across SSOT Catalog, Spec JSON, Code, and PBI Documentation;
  * `BL_13_LEGACY_CONTRACT_PROJECTION`: Validates backward-compatible projection (`snake_col AS LegacyCol`) in contract views;
  * `BL_14_BIDIRECTIONAL_SCHEMA_TYPE_PARITY`: Asserts bidirectional schema and type parity between legacy Spark and BigQuery;
  * `BL_15_CROSS_SHEET_UNIFIED_JOIN`: Validates unified joins/unions in multi-sheet ingestion consolidation;
  * `BL_16_DUPLICATE_UPSTREAM_UNION_ALL`: Blocks accidental UNION ALL/DISTINCT between homonymous upstream variants;
  * `BL_17_UNPRUNED_MASSIVE_TABLE_SCAN` & `BL_17_RAID_D37_EVENTTRACKS_WHITESPACE_NORM`: Prevents unpruned scans on 5B+ row tables and enforces whitespace/newline normalization before JSON parsing on silver redirects;
  * `BL_18_GROUP_BY_PARSED_ALIAS_SHADOWING`: Prevents Spark SQL vs BigQuery alias shadowing in `GROUP BY`;
  * `BL_19_MULTI_CSV_EXPORT_FILENAME_MISMATCH`: Enforces destination naming parity for multi-CSV exports.
- **BigQuery Live Dry-Run over Compiled Graph (`run_bigquery_live_dry_run` — Wave 1):** Submits compiled SQL with `dryRun: True` (0 bytes processed) using a **Smart Virtual Target Subquery Wrapper** to validate `${self()}` circular models, auditing `snake_case` output columns (`[DRY_RUN_OUTPUT_SNAKE_CASE_GUARD]`) and contract column parity (`[DRY_RUN_OUTPUT_COLUMNS_PARITY_GUARD]`).
- **AST Unmapped Upstream Guard (`[AST_UNMAPPED_UPSTREAM_GUARD]` — Wave 2):** Fails the gate if uncataloged joins remain unmapped in BigQuery.
- **5/5 Confidence Scorecard (`compute_ready_confidence_gate` — Wave 3):** Evaluates 5 criteria simultaneously: (1) Clean Dataform compilation, (2) 100% BigQuery Live Dry-Run pass, (3) Dry-run output parity and snake_case, (4) Zero BL_00–BL_19 or architectural violations, and (5) 100% AST upstreams reconciled.

---

### Stage 4: Parity by Construction, 5/5 Confidence Report & Governance Lock (`build_and_publish_pbi.py`)
- **Parity by Construction:** Generates Section 1 (Target Identification) and Section 3.3 (Column Dictionary) from the **identical in-memory canonical list**, eliminating documentation drift.
- **Suites S1 through S6 Validation:** Executes `validators/test_pbi_consistency.py`.
- **Wave 3 Ready Confidence Report (`output/refinement_context/<CARD_ID>_ready_confidence_report.json`):** Persists diagnostic report and embeds 5/5 Scorecard into `unit_test_report_*.md`.
- **Strict Board Governance Lock:** Even upon achieving 5/5 Confidence score, newly refined cards are kept in **`Refining`** with tag **`Refined`** for human review and final approval, strictly preventing unauthorized automatic promotion to `Ready`.

---

## 🛠️ CLI Execution & Usage

### 1. Enqueue and Run Pre-LLM Phase (Micro-Batch <=4 cards)
```bash
python3 skills/migration-spec-refiner/scripts/refine_card_orchestrator.py \
  --enqueue MIG-101 MIG-102 MIG-103 MIG-104 \
  --max-active 4 \
  --phase pre-llm
```

### 2. Resume Post-LLM Phase After Surgical Translation (Gate 3 + Compilation + PBI)
```bash
python3 skills/migration-spec-refiner/scripts/refine_card_orchestrator.py \
  --phase post-llm \
  --upstream-checker \
  --open-pr
```

### 3. Check Queue Status
```bash
python3 skills/migration-spec-refiner/scripts/refine_card_orchestrator.py --queue-status
```

### 4. Standalone Single-Card Refinement
```bash
python3 skills/migration-spec-refiner/scripts/refiner_pipeline.py \
  --ticket-id MIG-502 \
  --output-dir /tmp/refined_mig_502
```
