# [SymphonyAI Promo Planner] Technical Documentation — `sai_data_sync_main`

| | |
|---|---|
| **Component** | `sai_data_sync_main.py` — Main orchestrator DAG (SymphonyAI Promo Planner data sync) |
| **Project** | `project-hkwe-symphonyai` (Hong Kong / Wellcome) |
| **Composer environment** | `wh0uwt1100` |
| **Path** | `composer/dags/pipelines/symphonyai_promo_planner/sai_data_sync_main.py` |
| **Last updated** | 18 August 2026 |

---

## Contents

1. [Introduction](#1-introduction)
   - 1.1 [Document Overview](#11-document-overview)
   - 1.2 [Pipeline Objectives](#12-pipeline-objectives)
   - 1.3 [Related Resources](#13-related-resources)
2. [Architecture Overview](#2-architecture-overview)
   - 2.1 [End-to-End Data Flow](#21-end-to-end-data-flow)
   - 2.2 [Component Overview](#22-component-overview)
   - 2.3 [Configuration & Naming Conventions](#23-configuration--naming-conventions)
3. [Dynamic DAG Generation](#3-dynamic-dag-generation)
   - 3.1 [Generation Loop](#31-generation-loop)
   - 3.2 [`DagConfig` Utility](#32-dagconfig-utility)
   - 3.3 [DAG Parameters](#33-dag-parameters)
4. [Task Reference](#4-task-reference)
   - 4.1 [Task Inventory](#41-task-inventory)
   - 4.2 [Dependency Graph](#42-dependency-graph)
   - 4.3 [`validate_params`](#43-validate_params)
   - 4.4 [`bq_export_gcs_and_send_to_abs`](#44-bq_export_gcs_and_send_to_abs)
   - 4.5 [`archive_gcs_files`](#45-archive_gcs_files)
   - 4.6 [Validation Failure Path](#46-validation-failure-path)
5. [Parameter Validation Logic](#5-parameter-validation-logic)
6. [Configuration Reference](#6-configuration-reference)
   - 6.1 [Trigger Config (DAG level)](#61-trigger-config-dag-level)
   - 6.2 [Pipeline Config (task level)](#62-pipeline-config-task-level)
   - 6.3 [Archive Config](#63-archive-config)
7. [Maintenance & Runbook](#7-maintenance--runbook)
   - 7.1 [Triggering & Monitoring](#71-triggering--monitoring)
   - 7.2 [Common Failure Scenarios](#72-common-failure-scenarios)
   - 7.3 [Adding a New Interface](#73-adding-a-new-interface)
8. [Technical Specification](#8-technical-specification)
   - 8.1 [Runtime Environment & Dependencies](#81-runtime-environment--dependencies)
   - 8.2 [DAG Specification](#82-dag-specification)
   - 8.3 [Interface Contracts](#83-interface-contracts)
   - 8.4 [Non-Functional Characteristics](#84-non-functional-characteristics)
9. [Unit Tests](#9-unit-tests)
   - 9.1 [Scope & Setup](#91-scope--setup)
   - 9.2 [Test Matrix](#92-test-matrix)
   - 9.3 [Test Implementation](#93-test-implementation)
   - 9.4 [Running the Tests](#94-running-the-tests)
10. [Operation Guide](#10-operation-guide)
    - 10.1 [Normal Scheduled Run](#101-normal-scheduled-run)
    - 10.2 [Manual Run (Current Window)](#102-manual-run-current-window)
    - 10.3 [Re-run Scenarios](#103-re-run-scenarios)
    - 10.4 [Verification Checklist](#104-verification-checklist)

---

## 1. Introduction

### 1.1 Document Overview

This document describes the technical implementation of `sai_data_sync_main.py`, the **main orchestrator DAG** for the SymphonyAI Promo Planner outbound data sync. The module does not define a single DAG; instead it acts as a **DAG factory** — at parse time it iterates over a directory of trigger configuration files and generates one scheduled DAG per file.

Each generated DAG validates its runtime parameters, triggers the shared **L1 trigger framework** (`L1_trigger_framework_v6`) to export data from BigQuery to Google Cloud Storage (GCS) and forward it to SymphonyAI's Azure Blob Storage (ABS), and finally archives the processed GCS files. On invalid parameters, it short-circuits to an email alert and fails the run.

The document walks through the dynamic generation mechanism, the task graph and control flow (including the branching / archiving trigger rules), the parameter validation rules, the two-tier JSON configuration model, and a maintenance runbook.

### 1.2 Pipeline Objectives

This pipeline delivers curated promotion-planning data from the DFI datalake to SymphonyAI on a scheduled basis. Specifically, the module is designed to:

1. **Generate config-driven DAGs** — one DAG per trigger config file, so that new schedules/interfaces can be onboarded by adding JSON, without code changes.
2. **Guard against bad inputs** — validate `filter_start_date`, `filter_end_date`, `max_workers`, and `replace` before any downstream export runs, and alert stakeholders by email if validation fails.
3. **Orchestrate the export-and-ship flow** — delegate the actual BigQuery → GCS export and GCS → Azure Blob Storage upload to the shared L1 framework via `MyTriggerDagRunOperator`.
4. **Archive processed output** — copy/relocate the exported GCS files to an archive location after a successful send, keeping the "hot" outbound folder clean.

### 1.3 Related Resources

| Resource | Location |
|---|---|
| Main orchestrator DAG (this doc) | `composer/dags/pipelines/symphonyai_promo_planner/sai_data_sync_main.py` |
| Ad-hoc manual send DAG | `composer/dags/pipelines/symphonyai_promo_planner/sai_data_sync_manual.py` |
| Ad-hoc reverse pull DAG (ABS → GCS) | `composer/dags/pipelines/symphonyai_promo_planner/pull_data_from_sai.py` |
| `DagConfig` utility | `composer/dags/utils/dag_config.py` |
| Custom trigger operator | `composer/dags/custom_operator/my_trigger_dagrun_operator.py` |
| Email utility | `composer/dags/utils/email_utils.py` |
| Trigger configs (DAG level) | `composer/dag_config/pipeline_trigger/symphonyai_promo_planner/sai_data_sync_main/` |
| Pipeline configs (task level) | `composer/pipeline_config/symphonyai_promo_planner/sai_data_sync_main/` |
| Shared L1 framework (triggered) | `L1_trigger_framework_v6` |

---

## 2. Architecture Overview

### 2.1 End-to-End Data Flow

The main DAG is an orchestration layer. It performs no data movement itself — it validates inputs and triggers the shared L1 framework, which reads the task-level pipeline configs and executes the underlying utilities (stored-procedure export, GCS↔ABS transfer, GCS file logistics).

```
                         ┌──────────────────────────────────────────────┐
   dag_config/*.json ──▶ │  sai_data_sync_main.py  (DAG factory)          │
   (one DAG each)        │                                                │
                         │  start ─▶ validate_params ─┬─▶ bq_export_...   │
                         │                            │        │          │
                         │                            │        ▼          │
                         │                            │   archive_gcs ─▶ end
                         │                            │                   │
                         │                            └─▶ send_alert_email │
                         │                                    │           │
                         │                                    ▼           │
                         │                              raise_exception   │
                         └───────────────┬────────────────────────────────┘
                                         │ MyTriggerDagRunOperator
                                         ▼
                            ┌────────────────────────────┐
                            │  L1_trigger_framework_v6    │
                            │  (reads pipeline_config)    │
                            └────────────┬────────────────┘
                                         ▼
   BigQuery  ──stored proc──▶  GCS (hot)  ──GCS→ABS parallel──▶  SymphonyAI Azure Blob Storage
                                   │
                                   └── archive step: GCS (hot) ──▶ GCS (bak/archive)
```

### 2.2 Component Overview

| Component | Role |
|---|---|
| `sai_data_sync_main.py` | DAG factory; validation + orchestration only |
| `DagConfig` | Parses a trigger JSON into DAG-level attributes (`dag_id`, `tags`, `schedule_interval`, `default_args`, email settings, `parameters`) |
| `MyTriggerDagRunOperator` | Subclass of `TriggerDagRunOperator` that sets a Hong Kong `logical_date` and logs the triggered DAG's Airflow URL |
| `L1_trigger_framework_v6` | Shared, generic framework that reads a folder of pipeline configs and runs the declared utilities |
| `_validate_params` | Branch callable — validates params and routes the flow |
| `_send_alert_email_notification` | Sends the templated failure alert email |

### 2.3 Configuration & Naming Conventions

The pipeline is driven by **two tiers** of JSON configuration:

| Tier | Directory | Consumed by | Purpose |
|---|---|---|---|
| **Trigger config** (DAG level) | `dag_config/pipeline_trigger/.../sai_data_sync_main/` | `sai_data_sync_main.py` | Defines each DAG — schedule, tags, email, and pointers to the pipeline-config folder |
| **Pipeline config** (task level) | `pipeline_config/.../sai_data_sync_main/<interval>/` | `L1_trigger_framework_v6` | Defines the actual export/transfer utilities per interface (one file per interface) |

Naming conventions observed across the project:

| Rule | Example |
|---|---|
| Trigger config filename encodes banner + schedule | `symphonyai_hkwe_promo_planner_main_daily_0900.json` |
| Pipeline config prefixed `config_sai_` | `config_sai_hk_we_basket_transaction.json` |
| Archive pipeline config prefixed `config_archive_sai_` | `config_archive_sai_hk_we_store.json` |
| Schedule folders named `<frequency>_<HHMM>` | `daily_0900`, `weekly_sunday_0900` |
| `dag_id` matches the trigger config filename | `symphonyai_hkwe_promo_planner_main_daily_0900` |

---

## 3. Dynamic DAG Generation

### 3.1 Generation Loop

At parse time, the module lists every file under the trigger-config base directory and builds one DAG per file. Each DAG is registered into the module globals so Airflow can discover it.

```73:76:DFI/data_stitching/airflow/bitbucket/dfg-data-datalake-airflow/composer/dags/pipelines/symphonyai_promo_planner/sai_data_sync_main.py
base_dir = "/home/airflow/gcs/data/dag_config/pipeline_trigger/symphonyai_promo_planner/sai_data_sync_main"
for filename in os.listdir(base_dir):
    config_path = f"{base_dir}/{filename}"
    dag_config = DagConfig(config_path)
```

```209:209:DFI/data_stitching/airflow/bitbucket/dfg-data-datalake-airflow/composer/dags/pipelines/symphonyai_promo_planner/sai_data_sync_main.py
    globals()[dag.dag_id] = dag
```

> **Note:** Because generation happens during DAG parsing, every file in `base_dir` **must** be a valid trigger JSON. A malformed or non-JSON file will raise during parse and can break the whole module (all DAGs defined by it). Keep the folder clean — only trigger configs belong here.

### 3.2 `DagConfig` Utility

`DagConfig` (`utils/dag_config.py`) is a small dataclass wrapper that loads a trigger JSON and exposes its values as attributes. Key behaviours:

| Attribute | Source key | Notes |
|---|---|---|
| `dag_id`, `tags`, `catchup`, `schedule_interval` | direct keys | Passed straight to the `DAG(...)` constructor |
| `parameters` | `parameters` | Dict of pass-through/config values used to seed DAG `params` |
| `default_args` | `generate_default_dag_args()` | Builds `owner`, `start_date` (parsed from `start_date` via `%Y-%m-%d`), `depends_on_past`, `retries`, `email`, `email_on_failure`, `email_on_retry`, plus `lookup_key`/`flow`/`country_region`/`banner`/`cost_center` when `parameters` present |
| `email`, `email_cc`, `is_email_active` | `email`, `email_cc`, `is_email_active` | `is_email_active` defaults to `True` |

> **Note:** `generate_default_dag_args()` calls `datetime.strptime(start_date, "%Y-%m-%d")`, so `start_date` is **mandatory** in every trigger config and must use the `YYYY-MM-DD` format.

### 3.3 DAG Parameters

Each generated DAG exposes runtime `params`. Most are pass-through values copied from the trigger config's `parameters` block; four are exposed as typed, UI-editable Airflow `Param` objects for manual/backfill runs.

| Param | Type | Default source | Description |
|---|---|---|---|
| `lookup_key` | value | `parameters.lookup_key` | Routing key forwarded to L1 (e.g. `project-hkwe-symphonyai`) |
| `flow` | value | `parameters.flow` | Flow classification (e.g. `2-downstream`) |
| `country_region` | value | `parameters.country_region` | e.g. `hong-kong` |
| `banner` | value | `parameters.banner` | e.g. `wellcome` |
| `cost_center` | value | `parameters.cost_center` | e.g. `wh0uwt1100` |
| `file_directory` | value | `parameters.file_directory` | Folder of **main** pipeline configs read by L1 |
| `file_prefix` / `file_suffix` | value | `parameters.*` | Config filename filter (e.g. `config_sai` / `.json`) |
| `file_directory_archive` | value | `parameters.file_directory_archive` | Folder of **archive** pipeline configs |
| `file_prefix_archive` / `file_suffix_archive` | value | `parameters.*` | Archive config filename filter |
| `filter_start_date` | `["string","null"]` | `parameters.filter_start_date` | Export window start — format `YYYY-MM-DD` |
| `filter_end_date` | `["string","null"]` | `parameters.filter_end_date` | Export window end — format `YYYY-MM-DD` |
| `max_workers` | `["integer","null"]` | `parameters.max_workers` | Parallel upload workers to ABS |
| `replace` | `boolean` | `parameters.replace` | Overwrite existing ABS files matching the filename |
| `email`, `email_cc`, `email_active` | value | `DagConfig` | Alert recipients / toggle |

> **Note:** When `filter_start_date` / `filter_end_date` are `null`, the export window is instead derived inside the pipeline config via `UtilTime` expressions (see [§6.2](#62-pipeline-config-task-level)). The DAG params act as an optional manual override.

---

## 4. Task Reference

### 4.1 Task Inventory

| Task ID | Operator | Purpose |
|---|---|---|
| `start` | `EmptyOperator` | Entry marker |
| `validate_params` | `BranchPythonOperator` | Validate params; route to export **or** alert |
| `bq_export_gcs_and_send_to_abs` | `MyTriggerDagRunOperator` | Trigger L1 to export BigQuery → GCS and send GCS → ABS |
| `archive_gcs_files` | `MyTriggerDagRunOperator` | Trigger L1 to archive processed GCS files |
| `send_alert_email_validation_check` | `PythonOperator` | Send failure-alert email with the validation error message |
| `raise_exception_task` | `PythonOperator` | Force the DAG run into a failed state |
| `end` | `EmptyOperator` | Success marker |

### 4.2 Dependency Graph

```205:207:DFI/data_stitching/airflow/bitbucket/dfg-data-datalake-airflow/composer/dags/pipelines/symphonyai_promo_planner/sai_data_sync_main.py
        start >> validate_params >> [bq_export_gcs_and_send_to_abs, send_alert_email_validation_check] 
        bq_export_gcs_and_send_to_abs >> archive_gcs_files >> end
        send_alert_email_validation_check >> raise_exception_task
```

- **Happy path:** `start → validate_params → bq_export_gcs_and_send_to_abs → archive_gcs_files → end`
- **Failure path:** `start → validate_params → send_alert_email_validation_check → raise_exception_task`

### 4.3 `validate_params`

A `BranchPythonOperator` whose callable is `_validate_params`. It receives the four validated params via templated `op_kwargs` and returns the `task_id` of the branch to follow:

- returns `"bq_export_gcs_and_send_to_abs"` when validation passes;
- returns `"send_alert_email_validation_check"` when validation fails (after pushing the error message to XCom under key `email_error_msg`).

See [§5](#5-parameter-validation-logic) for the full rule set.

### 4.4 `bq_export_gcs_and_send_to_abs`

Triggers the shared framework DAG `L1_trigger_framework_v6` and **waits for completion** (`wait_for_completion=True`, `poke_interval=15`). The `conf` payload forwards routing keys and the **main** config-folder pointers, plus a `custom_param` block carrying the runtime overrides:

```136:159:DFI/data_stitching/airflow/bitbucket/dfg-data-datalake-airflow/composer/dags/pipelines/symphonyai_promo_planner/sai_data_sync_main.py
        bq_export_gcs_and_send_to_abs = MyTriggerDagRunOperator(
            task_id="bq_export_gcs_and_send_to_abs",
            trigger_dag_id="L1_trigger_framework_v6",
            wait_for_completion=True,
            poke_interval=15,
            conf={
                "root_dag_id": dag_config.dag_id,
                "lookup_key": "{{ params.lookup_key }}",
                "flow": "{{ params.flow }}",
                "country_region": "{{ params.country_region }}",
                "banner": "{{ params.banner }}",
                "cost_center": "{{ params.cost_center }}",
                "file_directory": "{{ params.file_directory }}",
                "file_prefix": "{{ params.file_prefix }}",
                "file_suffix": "{{ params.file_suffix }}",
                "email": "{{ params.email }}",
                "email_cc": "{{ params.email_cc }}",
                "custom_param": {
                    "filter_start_date": "{{ params.filter_start_date }}",
                    "filter_end_date": "{{ params.filter_end_date }}",
                    "max_workers": "{{ params.max_workers }}",
                    "replace": "{{ params.replace }}",
                },
            },
        )
```

L1 uses `file_directory` + `file_prefix` + `file_suffix` to locate the pipeline configs (one per interface) and executes their declared utilities (stored-procedure export → GCS, then GCS → ABS parallel transfer).

### 4.5 `archive_gcs_files`

Also triggers `L1_trigger_framework_v6`, but points at the **archive** config folder (`file_directory_archive` / `file_prefix_archive` / `file_suffix_archive`). It carries the distinguishing trigger rule:

```162:167:DFI/data_stitching/airflow/bitbucket/dfg-data-datalake-airflow/composer/dags/pipelines/symphonyai_promo_planner/sai_data_sync_main.py
        archive_gcs_files = MyTriggerDagRunOperator(
            task_id="archive_gcs_files",
            trigger_dag_id="L1_trigger_framework_v6",
            trigger_rule=TriggerRule.NONE_SKIPPED,
            wait_for_completion=True,
            poke_interval=15,
```

> **Note:** `trigger_rule=TriggerRule.NONE_SKIPPED` means archive runs as long as no direct upstream was skipped. Since `bq_export_gcs_and_send_to_abs` is only skipped when the branch chooses the alert path, archiving effectively runs only on the successful export branch and is bypassed on validation failure.

### 4.6 Validation Failure Path

When validation fails, the branch selects `send_alert_email_validation_check`:

```183:201:DFI/data_stitching/airflow/bitbucket/dfg-data-datalake-airflow/composer/dags/pipelines/symphonyai_promo_planner/sai_data_sync_main.py
        send_alert_email_validation_check = PythonOperator(
            task_id="send_alert_email_validation_check",
            python_callable=_send_alert_email_notification,
            op_kwargs={
                "dag_id": dag_config.dag_id,
                "is_email_active": True,
                "email": "{{ params.email }}",
                "email_cc": "{{ params.email_cc }}",
                "email_subject": f"[Datalake GCP] {dag_config.dag_id} Failed - Invalid DAG parameters",
                "email_message": "{{ ti.xcom_pull('validate_params', key='email_error_msg') }}",
                "sla_priority": "P3",
                "support_analyst": "DFIT APPS DEV SUPPORT ANALYTICS",
            },
        )

        raise_exception_task = PythonOperator(
            task_id="raise_exception_task",
            python_callable=raise_exception,
        )
```

- `_send_alert_email_notification` renders the standard L1 HTML email template and sends it to the configured recipients (falling back to `DEFAULT_EMAIL` / `DEFAULT_EMAIL_CC` if none are provided). The body is the validation error message pulled from XCom.
- `raise_exception` then raises `AirflowFailException("Invalid DAG parameters")`, marking the run as **failed** so it surfaces in monitoring.

---

## 5. Parameter Validation Logic

`_validate_params` enforces the following rules before any export is triggered. All failures are collected, logged, pushed to XCom (`email_error_msg`), and the branch routes to the alert path.

| Parameter | Rule | Failure message (example) |
|---|---|---|
| `filter_start_date` | If provided (not `None`/empty), must parse as `%Y-%m-%d` | `filter_start_date must follow the format %Y-%m-%d (e.g. 2026-07-01), got: ...` |
| `filter_end_date` | If provided (not `None`/empty), must parse as `%Y-%m-%d` | `filter_end_date must follow the format %Y-%m-%d ...` |
| `max_workers` | If provided, must be a non-boolean `int` **and** `> 0` | `max_workers must be an integer, got: ...` / `must be greater than 0, got: ...` |
| `replace` | If provided, must be a `bool` | `replace must be a boolean, got: ...` |

```54:67:DFI/data_stitching/airflow/bitbucket/dfg-data-datalake-airflow/composer/dags/pipelines/symphonyai_promo_planner/sai_data_sync_main.py
    if errors:
        error_msg = "Invalid DAG parameters:\n- " + "\n- ".join(errors)
        logger.error(error_msg)
        context["task_instance"].xcom_push(key="email_error_msg", value=error_msg)
        return "send_alert_email_validation_check"

    logger.info(
        "Parameter validation passed: filter_start_date=%s, filter_end_date=%s, max_workers=%s, replace=%s",
        filter_start_date if filter_start_date is not None else "None",
        filter_end_date if filter_end_date is not None else "None",
        max_workers if max_workers is not None else "None",
        replace if replace is not None else "None",
    )
    return "bq_export_gcs_and_send_to_abs"
```

> **Note:** With `render_template_as_native_obj=True`, the templated `op_kwargs` are rendered to native Python types (e.g. `null` → `None`, `5` → `int`, `true` → `bool`) rather than strings — which is what allows the `isinstance` checks to work as intended.

---

## 6. Configuration Reference

### 6.1 Trigger Config (DAG level)

One file per DAG under `dag_config/pipeline_trigger/symphonyai_promo_planner/sai_data_sync_main/`. Example (`symphonyai_hkwe_promo_planner_main_daily_0900.json`):

| Key | Example | Description |
|---|---|---|
| `dag_id` | `symphonyai_hkwe_promo_planner_main_daily_0900` | Unique DAG id (matches filename) |
| `tags` | `["project-hkwe-symphonyai", "2-downstream", ...]` | Airflow UI tags |
| `schedule_interval` | `0 9 * * *` | Cron schedule (09:00 daily) |
| `start_date` | `2026-07-01` | Required, `YYYY-MM-DD` |
| `is_email_active` / `email` / `email_cc` | `true` / `[...]` | Alert config |
| `parameters.file_directory` | `.../sai_data_sync_main/daily_0900` | Main pipeline-config folder |
| `parameters.file_directory_archive` | `.../daily_0900/archive` | Archive pipeline-config folder |
| `parameters.filter_start_date` / `filter_end_date` | `null` | Optional manual export-window override |
| `parameters.max_workers` | `null` | Optional worker override |
| `parameters.replace` | `true` | Overwrite existing ABS files |

Two intervals are configured for HK/WE: `daily_0900` (`0 9 * * *`) and `weekly_sunday_0900` (`0 9 * * 0`).

### 6.2 Pipeline Config (task level)

One file per interface under `pipeline_config/.../sai_data_sync_main/<interval>/`, read by L1. Example (`config_sai_hk_we_basket_transaction.json`) structure:

- **`metadata`** — `operation_id`, `operation_desc`, `config_version`, `operation_unit` (`HK/WE`).
- **`custom_param`** — typed values (`string` / `variable` / `expression`) resolved by L1. Notable fields:

| Field | Type | Example / Value |
|---|---|---|
| `gcs_bucket` | variable | `gcs_outbound_symphonyai_promo_planner` |
| `export_date` | expression | `f"{UtilTime.get_current_time(fmt='%Y%m%d',offset_d=0)}/HKWE_Baskettransaction"` |
| `filename` | expression | `f"HKWE_Baskettransaction_{...%Y%m%d...}_*.csv.gz"` |
| `abs_conn_id` | string | `symphonyai_pd_wasbsastoken` |
| `abs_container_dest` | string | `incoming` |
| `abs_prefix_dest` | string | `dfiincoming/.../basket_transaction` |
| `filter_start_date` | expression | `UtilTime.get_current_time(fmt='%Y-%m-%d',offset_d=-7)` |
| `filter_end_date` | expression | `UtilTime.get_current_time(fmt='%Y-%m-%d',offset_d=0)` |
| `stored_procedure` | string | `export_hkwe_merchplan_basket_transaction` |

- **`list_of_utility`** — ordered utilities L1 executes:

| Utility | `utility_domain` / `utility_group` | Purpose |
|---|---|---|
| Run stored procedure | `Stored_Procedure` / `Run_Dataform_with_Params` | Export BigQuery data to GCS via the named Dataform stored procedure |
| Upload to ABS | `File_Logistics` / `GCS_to_ABS_Parallel` | Parallel-upload the exported GCS files to SymphonyAI Azure Blob Storage |

### 6.3 Archive Config

Archive pipeline configs (prefixed `config_archive_sai_`, under the `archive/` subfolder) are consumed by the `archive_gcs_files` task. Example (`config_archive_sai_hk_we_store.json`) utilities:

| Utility | `utility_domain` / `utility_group` | Purpose |
|---|---|---|
| Delete existing archive | `File_Logistics` / `GCS_Delete_File` | Clear the target `bak/<timestamp>` folder |
| Copy to archive | `File_Logistics` / `GCS_Copy_File` | Copy processed files from `hot` → `bak/<timestamp>`, with `delete_source=True` |

---

## 7. Maintenance & Runbook

### 7.1 Triggering & Monitoring

- **Scheduled runs:** driven by each trigger config's `schedule_interval` (HK/WE: daily 09:00, weekly Sunday 09:00, Asia/Hong_Kong logical date via `MyTriggerDagRunOperator`).
- **Manual / backfill:** trigger the DAG with config, overriding `filter_start_date` / `filter_end_date` (both `YYYY-MM-DD`), `max_workers`, and/or `replace` as needed.
- **Watch the trigger tasks:** `bq_export_gcs_and_send_to_abs` and `archive_gcs_files` wait for the triggered `L1_trigger_framework_v6` run (`poke_interval=15s`). Open the triggered run — the operator logs its Airflow URL — to inspect the actual export/transfer.

### 7.2 Common Failure Scenarios

| Symptom | Likely cause | Resolution |
|---|---|---|
| Run fails at `raise_exception_task`; alert email received | Invalid DAG params | Read the emailed message / `validate_params` XCom (`email_error_msg`); correct the offending param and re-run |
| All DAGs from this module disappear / import error | Non-JSON or malformed file in the trigger `base_dir` | Remove/fix the stray file; only valid trigger configs belong there |
| `strptime` error while parsing config | Missing or badly formatted `start_date` in trigger config | Ensure every trigger config has `start_date` as `YYYY-MM-DD` |
| Export/upload fails inside the triggered run | Issue in the L1 stored procedure or GCS↔ABS transfer | Debug in the `L1_trigger_framework_v6` run; verify `abs_conn_id`, buckets, and stored-procedure availability |
| `archive_gcs_files` skipped | Validation failed (upstream export skipped) | Expected behaviour; archiving only runs on the successful export branch |

### 7.3 Adding a New Interface

1. **Add a pipeline config** in the appropriate interval folder, e.g. `pipeline_config/.../sai_data_sync_main/daily_0900/config_sai_hk_we_<interface>.json`. Set `stored_procedure`, `export_date`, `filename`, and ABS destination fields.
2. **Add the matching archive config** in `.../daily_0900/archive/config_archive_sai_hk_we_<interface>.json` (delete + copy-to-`bak` utilities).
3. No change to `sai_data_sync_main.py` is required — L1 discovers the new config via `file_prefix` / `file_suffix`.
4. **To add a whole new schedule**, create a new trigger config in `dag_config/pipeline_trigger/.../sai_data_sync_main/` (unique `dag_id`, `schedule_interval`, `start_date`, and `file_directory*` pointing at a new interval folder). A new DAG will be generated automatically on the next parse.

---

## 8. Technical Specification

### 8.1 Runtime Environment & Dependencies

| Item | Specification |
|---|---|
| Orchestrator | Google Cloud Composer (Apache Airflow 2.x) |
| Language | Python 3.8+ |
| Composer environment | `wh0uwt1100` |
| Timezone (logical date) | `Asia/Hong_Kong` (set by `MyTriggerDagRunOperator`) |
| GCS DAGs/data mount | `/home/airflow/gcs/data/...` |
| Triggered framework | `L1_trigger_framework_v6` (must be deployed in the same environment) |

**Import dependencies** (from the module header):

| Import | Purpose |
|---|---|
| `os`, `logging`, `datetime` | Config discovery, logging, date parsing |
| `airflow.DAG`, `airflow.models.param.Param` | DAG + typed parameter definitions |
| `airflow.exceptions.AirflowFailException` | Hard-fail the run on invalid params |
| `airflow.operators.empty.EmptyOperator` | `start` / `end` markers |
| `airflow.operators.python.PythonOperator`, `BranchPythonOperator` | Validation branch + alert/raise tasks |
| `airflow.utils.trigger_rule.TriggerRule` | `NONE_SKIPPED` rule on the archive task |
| `my_trigger_dagrun_operator.MyTriggerDagRunOperator` | Custom cross-DAG trigger |
| `utils.dag_config.DagConfig` | Trigger-config parsing |
| `utils.email_utils._send_alert_email_notification` | Failure-alert email |

**Airflow connections / variables required by the triggered L1 run** (not by this DAG directly):

| Type | Name | Used for |
|---|---|---|
| Connection | `symphonyai_pd_wasbsastoken` (`abs_conn_id`) | Azure Blob Storage (SAS token) |
| Variable | `gcs_outbound_symphonyai_promo_planner` | Outbound GCS bucket |
| Variable | `composer_name` | Composer name shown in alert emails |

### 8.2 DAG Specification

| Attribute | Value |
|---|---|
| DAG id | `dag_config.dag_id` (per trigger config, e.g. `symphonyai_hkwe_promo_planner_main_daily_0900`) |
| Number of DAGs | 1 per file in the trigger `base_dir` |
| Schedule | `dag_config.schedule_interval` (e.g. `0 9 * * *`, `0 9 * * 0`) |
| `catchup` | `dag_config.catchup` (configured `false`) |
| `render_template_as_native_obj` | `True` (critical for type-correct validation) |
| Task count | 7 (`start`, `validate_params`, `bq_export_gcs_and_send_to_abs`, `archive_gcs_files`, `send_alert_email_validation_check`, `raise_exception_task`, `end`) |
| Default retries | `dag_config.default_args["retries"]` (configured `0`) |
| Idempotency | Depends on `replace` and the GCS/ABS filenames (date-stamped); re-running the same logical date overwrites when `replace=true` |

### 8.3 Interface Contracts

**Upstream input — trigger config JSON** (see [§6.1](#61-trigger-config-dag-level)). Required keys: `dag_id`, `tags`, `schedule_interval`, `start_date` (`YYYY-MM-DD`), `catchup`, `parameters.*`, `email`, `email_cc`, `is_email_active`.

**Downstream output — `conf` payload to `L1_trigger_framework_v6`.** The contract the framework relies on:

| Field | Type | Meaning |
|---|---|---|
| `root_dag_id` | string | Originating DAG id (used in alert emails) |
| `lookup_key`, `flow`, `country_region`, `banner`, `cost_center` | string | Routing / classification keys |
| `file_directory`, `file_prefix`, `file_suffix` | string | Locates the pipeline-config files to execute |
| `email`, `email_cc` | list | Alert recipients |
| `custom_param.filter_start_date` / `filter_end_date` | string / null | Export window override |
| `custom_param.max_workers` | int / null | ABS upload parallelism |
| `custom_param.replace` | bool | Overwrite behaviour |

**XCom contract (intra-DAG):** `validate_params` pushes `email_error_msg` (string) → consumed by `send_alert_email_validation_check` via `ti.xcom_pull('validate_params', key='email_error_msg')`.

### 8.4 Non-Functional Characteristics

| Aspect | Detail |
|---|---|
| Fail-fast | Invalid params abort before any export; hard failure via `AirflowFailException` |
| Blocking behaviour | Trigger tasks block (`wait_for_completion=True`) and poll every 15s |
| Observability | Validation result logged; triggered-run Airflow URL logged; failure email with SLA `P3` |
| Coupling | Loosely coupled to L1 via `trigger_dag_id` + `conf`; no shared DB state |
| Blast radius | A parse-time error in one trigger config affects all DAGs generated by this module |

---

## 9. Unit Tests

### 9.1 Scope & Setup

The module exposes two directly unit-testable callables — `_validate_params` (branch routing) and `raise_exception` (hard fail) — plus the generated DAG structure (integrity tests). Because the module executes `os.listdir(base_dir)` and `DagConfig(...)` **at import time**, tests must isolate that side effect before importing the module.

**Suggested layout** (place under the repo test tree, e.g. `composer/tests/pipelines/symphonyai_promo_planner/test_sai_data_sync_main.py`):

```python
import os
import sys
import types
from datetime import datetime
from unittest import mock

import pytest


# ---------------------------------------------------------------------------
# Fixtures: isolate import-time side effects (os.listdir + DagConfig + custom
# operator / util imports that may not resolve in a bare test environment).
# ---------------------------------------------------------------------------
@pytest.fixture
def sai_module(monkeypatch, tmp_path):
    """Import sai_data_sync_main with all import-time I/O stubbed out."""
    # 1. Fake a single trigger config file in a temp base_dir.
    fake_cfg = tmp_path / "symphonyai_hkwe_promo_planner_main_daily_0900.json"
    fake_cfg.write_text("{}")
    monkeypatch.setattr(os, "listdir", lambda _p: [fake_cfg.name])

    # 2. Stub DagConfig so no real JSON/GCS is read.
    class FakeDagConfig:
        def __init__(self, path):
            self.dag_id = "symphonyai_hkwe_promo_planner_main_daily_0900"
            self.tags = ["project-hkwe-symphonyai"]
            self.catchup = False
            self.schedule_interval = "0 9 * * *"
            self.default_args = {"owner": "test", "start_date": datetime(2026, 7, 1),
                                 "retries": 0}
            self.email = ["a@b.com"]
            self.email_cc = ["a@b.com"]
            self.is_email_active = True
            self.parameters = {"lookup_key": "project-hkwe-symphonyai", "flow": "2-downstream",
                               "country_region": "hong-kong", "banner": "wellcome",
                               "cost_center": "wh0uwt1100", "file_directory": "/d",
                               "file_prefix": "config_sai", "file_suffix": ".json",
                               "file_directory_archive": "/d/archive",
                               "file_prefix_archive": "config_archive_sai",
                               "file_suffix_archive": ".json", "filter_start_date": None,
                               "filter_end_date": None, "max_workers": None, "replace": True}

    # 3. Register stub modules for imports that aren't on the test path.
    fake_dc = types.ModuleType("utils.dag_config"); fake_dc.DagConfig = FakeDagConfig
    monkeypatch.setitem(sys.modules, "utils.dag_config", fake_dc)
    # ... similarly stub `my_trigger_dagrun_operator` and `utils.email_utils`
    #     with lightweight fakes / mocks as needed for your environment.

    import importlib
    module = importlib.import_module(
        "composer.dags.pipelines.symphonyai_promo_planner.sai_data_sync_main"
    )
    importlib.reload(module)
    return module


@pytest.fixture
def fake_context():
    """A minimal Airflow context exposing an xcom-capable task_instance."""
    ti = mock.MagicMock()
    return {"task_instance": ti, "ti": ti}
```

> **Note:** The exact set of `sys.modules` stubs depends on how the test environment resolves the `my_trigger_dagrun_operator` and `utils.*` imports. The pattern above (temp `base_dir` + stubbed `DagConfig`) is the key trick that makes the factory module importable in isolation.

### 9.2 Test Matrix

| # | Step / Feature | Scenario | Expected result |
|---|---|---|---|
| T1 | `_validate_params` | All params valid (`2026-07-01`, `2026-07-08`, `5`, `True`) | returns `"bq_export_gcs_and_send_to_abs"`; no XCom push |
| T2 | `_validate_params` | All params `None`/empty | returns `"bq_export_gcs_and_send_to_abs"` (optional params skipped) |
| T3 | `_validate_params` | Bad `filter_start_date` (`2026/07/01`) | returns `"send_alert_email_validation_check"`; XCom `email_error_msg` mentions `filter_start_date` |
| T4 | `_validate_params` | Bad `filter_end_date` (`07-01-2026`) | routes to alert; error mentions `filter_end_date` |
| T5 | `_validate_params` | `max_workers = 0` | routes to alert; error "must be greater than 0" |
| T6 | `_validate_params` | `max_workers = "5"` (str) / `True` (bool) | routes to alert; error "must be an integer" |
| T7 | `_validate_params` | `replace = "yes"` (non-bool) | routes to alert; error "must be a boolean" |
| T8 | `_validate_params` | Multiple invalid params | single XCom message lists **all** offending params |
| T9 | `raise_exception` | Called | raises `AirflowFailException` |
| T10 | DAG integrity | Module import | at least 1 DAG registered in `globals()`; no import error |
| T11 | DAG integrity | Task set | task ids exactly match the 7 expected ids |
| T12 | DAG integrity | Dependencies | `validate_params` downstream = {export, alert}; `archive_gcs_files` downstream = {end}; `raise_exception_task` downstream of alert |
| T13 | DAG integrity | Trigger rule | `archive_gcs_files.trigger_rule == NONE_SKIPPED` |

### 9.3 Test Implementation

```python
# --- _validate_params branch routing -----------------------------------------

def test_valid_params_route_to_export(sai_module, fake_context):
    result = sai_module._validate_params(
        filter_start_date="2026-07-01", filter_end_date="2026-07-08",
        max_workers=5, replace=True, **fake_context,
    )
    assert result == "bq_export_gcs_and_send_to_abs"
    fake_context["task_instance"].xcom_push.assert_not_called()


def test_all_none_is_valid(sai_module, fake_context):
    result = sai_module._validate_params(
        filter_start_date=None, filter_end_date=None,
        max_workers=None, replace=None, **fake_context,
    )
    assert result == "bq_export_gcs_and_send_to_abs"


@pytest.mark.parametrize("bad_start", ["2026/07/01", "07-01-2026", "not-a-date"])
def test_bad_start_date_routes_to_alert(sai_module, fake_context, bad_start):
    result = sai_module._validate_params(
        filter_start_date=bad_start, filter_end_date=None,
        max_workers=None, replace=None, **fake_context,
    )
    assert result == "send_alert_email_validation_check"
    _, kwargs = fake_context["task_instance"].xcom_push.call_args
    assert kwargs["key"] == "email_error_msg"
    assert "filter_start_date" in kwargs["value"]


@pytest.mark.parametrize("bad_workers,fragment", [
    (0, "greater than 0"),
    (-3, "greater than 0"),
    ("5", "must be an integer"),
    (True, "must be an integer"),   # bool is explicitly rejected
])
def test_bad_max_workers(sai_module, fake_context, bad_workers, fragment):
    result = sai_module._validate_params(
        filter_start_date=None, filter_end_date=None,
        max_workers=bad_workers, replace=None, **fake_context,
    )
    assert result == "send_alert_email_validation_check"
    assert fragment in fake_context["task_instance"].xcom_push.call_args.kwargs["value"]


def test_bad_replace_routes_to_alert(sai_module, fake_context):
    result = sai_module._validate_params(
        filter_start_date=None, filter_end_date=None,
        max_workers=None, replace="yes", **fake_context,
    )
    assert result == "send_alert_email_validation_check"
    assert "replace must be a boolean" in \
        fake_context["task_instance"].xcom_push.call_args.kwargs["value"]


def test_multiple_errors_collected(sai_module, fake_context):
    sai_module._validate_params(
        filter_start_date="bad", filter_end_date="bad",
        max_workers=0, replace="no", **fake_context,
    )
    msg = fake_context["task_instance"].xcom_push.call_args.kwargs["value"]
    for token in ("filter_start_date", "filter_end_date", "max_workers", "replace"):
        assert token in msg


# --- raise_exception ----------------------------------------------------------

def test_raise_exception(sai_module):
    from airflow.exceptions import AirflowFailException
    with pytest.raises(AirflowFailException):
        sai_module.raise_exception()


# --- DAG integrity ------------------------------------------------------------

def _get_dag(sai_module):
    dags = [v for v in vars(sai_module).values() if v.__class__.__name__ == "DAG"]
    assert dags, "no DAG registered by the factory"
    return dags[0]


def test_expected_tasks(sai_module):
    dag = _get_dag(sai_module)
    assert set(dag.task_ids) == {
        "start", "validate_params", "bq_export_gcs_and_send_to_abs",
        "archive_gcs_files", "send_alert_email_validation_check",
        "raise_exception_task", "end",
    }


def test_dependencies(sai_module):
    dag = _get_dag(sai_module)
    assert dag.get_task("validate_params").downstream_task_ids == {
        "bq_export_gcs_and_send_to_abs", "send_alert_email_validation_check"}
    assert dag.get_task("archive_gcs_files").downstream_task_ids == {"end"}
    assert dag.get_task("send_alert_email_validation_check").downstream_task_ids == {
        "raise_exception_task"}


def test_archive_trigger_rule(sai_module):
    from airflow.utils.trigger_rule import TriggerRule
    dag = _get_dag(sai_module)
    assert dag.get_task("archive_gcs_files").trigger_rule == TriggerRule.NONE_SKIPPED
```

### 9.4 Running the Tests

```bash
# From the Composer/dags repo root, with Airflow installed in the venv:
pip install pytest
pytest composer/tests/pipelines/symphonyai_promo_planner/test_sai_data_sync_main.py -v
```

> **Note:** These are pure unit tests — they never touch BigQuery, GCS, or Azure. The trigger tasks are only asserted structurally (existence, wiring, trigger rule); the real export/transfer is the responsibility of `L1_trigger_framework_v6` and should be covered by that framework's own tests / an integration run.

---

## 10. Operation Guide

### 10.1 Normal Scheduled Run

No action required — Airflow triggers each DAG on its `schedule_interval`.

| Interface set | DAG id | Schedule (Asia/Hong_Kong) |
|---|---|---|
| Daily | `symphonyai_hkwe_promo_planner_main_daily_0900` | 09:00 every day (`0 9 * * *`) |
| Weekly | `symphonyai_hkwe_promo_planner_main_weekly_sunday_0900` | 09:00 every Sunday (`0 9 * * 0`) |

On a scheduled run the export window comes from the **pipeline config** `UtilTime` expressions (e.g. daily basket transaction = last 7 days: `offset_d=-7` → `offset_d=0`); the DAG-level `filter_*` params stay `null`.

**What to expect:** `start → validate_params → bq_export_gcs_and_send_to_abs → archive_gcs_files → end`, all green. Files land under the ABS `incoming` container.

### 10.2 Manual Run (Current Window)

To ship today's data outside the schedule, using the config defaults:

1. Airflow UI → open the DAG → **Trigger DAG** (not "w/ config").
2. Leave all params at their defaults (`filter_start_date` / `filter_end_date` = `null`).
3. Monitor `bq_export_gcs_and_send_to_abs`; open the triggered `L1_trigger_framework_v6` run via the logged URL.

CLI equivalent:

```bash
airflow dags trigger symphonyai_hkwe_promo_planner_main_daily_0900
```

### 10.3 Re-run Scenarios

| Scenario | How to run | Key parameters |
|---|---|---|
| **A. Re-run a failed scheduled run as-is** | Clear the failed task(s) in the UI (Grid view → select task → **Clear**), or re-trigger the DAG | none (uses config defaults) |
| **B. Backfill a specific date range** | **Trigger DAG w/ config** | `filter_start_date` + `filter_end_date` (both `YYYY-MM-DD`), e.g. `2026-06-01` → `2026-06-30` |
| **C. Re-send without overwriting existing ABS files** | Trigger w/ config | `replace = false` (skips blobs already present) |
| **D. Force overwrite of existing ABS files** | Trigger w/ config | `replace = true` |
| **E. Speed up / throttle the ABS upload** | Trigger w/ config | `max_workers` = integer `> 0` (e.g. `8` faster, `2` gentler) |
| **F. Re-run only the archive step** | Grid view → **Clear** just `archive_gcs_files` | none |
| **G. Re-run one interface only** | Temporarily point the pipeline-config folder at only that interface, or re-run via the ad-hoc `sai_data_sync_manual.py` DAG | per that DAG's params |

**Trigger-w/-config JSON example (Scenario B — 30-day backfill, overwrite):**

```json
{
  "filter_start_date": "2026-06-01",
  "filter_end_date": "2026-06-30",
  "max_workers": 8,
  "replace": true
}
```

CLI equivalent:

```bash
airflow dags trigger symphonyai_hkwe_promo_planner_main_daily_0900 \
  --conf '{"filter_start_date":"2026-06-01","filter_end_date":"2026-06-30","max_workers":8,"replace":true}'
```

> **Note:** Parameter formats are enforced by `validate_params`. A bad value (e.g. `filter_start_date` not `YYYY-MM-DD`, `max_workers <= 0`, non-boolean `replace`) will short-circuit the run to `send_alert_email_validation_check` → `raise_exception_task` and email the reason — no data is exported. Correct the value and re-trigger.

### 10.4 Verification Checklist

After any run (normal or re-run), confirm:

1. **DAG state** — the run is `success`; `bq_export_gcs_and_send_to_abs` and `archive_gcs_files` both green (or `archive_gcs_files` skipped only on the validation-failure path).
2. **Triggered run** — the `L1_trigger_framework_v6` run linked from the task log completed successfully.
3. **ABS delivery** — expected files exist in the `incoming` container under the interface's `abs_prefix_dest` (verify with the ad-hoc `pull_data_from_sai.py` listing, or the SAI side).
4. **Archive** — processed files moved from the `hot` folder to `bak/<timestamp>` in the outbound GCS bucket.
5. **No alert email** — absence of the `[Datalake GCP] ... Failed - Invalid DAG parameters` email confirms validation passed.
