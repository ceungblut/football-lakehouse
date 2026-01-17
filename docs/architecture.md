
# Architecture

## Overview
This project implements a governed football analytics lakehouse on Azure Databricks,
designed to support Fantasy Premier League (FPL) transfer optimisation and advanced
football analytics.

The architecture prioritises:
- clarity of data contracts
- reproducibility
- cost-aware execution
- explainable optimisation logic

The system is intentionally modular so additional datasets or execution patterns
can be added without refactoring existing pipelines.

---

## Architectural principles

### Governance first
- All data assets are managed through **Unity Catalog**
- Catalogs, schemas, and tables are explicitly defined
- Managed identity is used for all storage access
- No embedded secrets or storage keys

### Cost-aware execution
- All pipelines run as **Databricks Jobs**
- No always-on clusters
- Single-node clusters only
- Short auto-termination windows
- Streaming jobs are finite and checkpointed

### Explicit logic over managed abstractions
- Transformations are implemented using Spark SQL and PySpark
- Orchestration is handled via Jobs and Workflows
- Managed abstractions (e.g. Delta Live Tables) are deliberately avoided to retain:
  - visibility into execution semantics
  - explicit control over cost
  - easier reasoning about correctness

---

## Medallion architecture

The lakehouse follows a classic Bronze / Silver / Gold pattern.

### Bronze — Raw ingestion
**Purpose:** Preserve source data exactly as received.

Characteristics:
- Append-only
- Replayable
- Minimal transformation
- Partitioned by snapshot date where applicable

Examples:
- FPL bootstrap snapshots
- Polled live match event payloads
- Stream-ingested JSON files (Auto Loader output)

Bronze tables act as the immutable system of record.

---

### Silver — Conformed layer
**Purpose:** Provide clean, typed, and deduplicated datasets with stable grains.

Characteristics:
- Explicit schemas
- Deterministic primary keys
- Idempotent upserts using `MERGE`
- Late-arriving data handled explicitly

Responsibilities:
- Normalise nested JSON into relational structures
- Deduplicate event-level data
- Conform entities across sources (players, teams, fixtures)

Silver tables are suitable for analytics and downstream feature generation.

---

### Gold — Optimisation-ready layer
**Purpose:** Serve analytics, modelling, and optimisation workloads.

Characteristics:
- Business-meaningful grains
- Pre-joined, feature-rich tables
- Explicit assumptions and derivations

Examples:
- Player-gameweek fact tables
- Daily price snapshot facts
- Live-form and minutes-risk features
- Transfer recommendation outputs

Gold tables are designed to support **decision-making**, not raw analysis.

---

## Batch vs streaming

Batch and streaming are treated as **execution modes**, not data layers.

### Batch ingestion
Used for:
- Daily FPL snapshots
- Periodic enrichment datasets
- Backfills and reprocessing

Batch jobs are deterministic and replayable.

---

### Structured Streaming
Used where data arrives incrementally or continuously.

Characteristics:
- Implemented via **Auto Loader**
- File-based ingestion into Bronze tables
- Finite execution using `availableNow`
- Explicit checkpoint management

Streaming jobs are designed to:
- process only newly arrived data
- terminate automatically
- avoid long-running cluster usage

---

## Orchestration

Pipelines are orchestrated using **Databricks Workflows**.

Design choices:
- One workflow per logical slice
- Clear task dependencies
- Parameterised runs (`run_id`, `snapshot_ts`, `mode`)
- Job clusters scoped to individual workflows

This approach keeps execution transparent and debuggable.

---

## Optimisation approach

Transfer optimisation is treated as a two-stage problem:

1. **Prediction**
   - Estimate expected points over short horizons
   - Use interpretable features derived from Gold tables
   - Track model artefacts and metrics explicitly

2. **Deterministic optimisation**
   - Apply FPL constraints (budget, squad composition)
   - Evaluate candidate transfers based on expected gain
   - Surface rationale and risk indicators alongside recommendations

The goal is **decision quality and explainability**, not black-box prediction.

---

## Extensibility

The architecture is designed to support future extensions such as:
- betting market odds
- richer live event feeds
- additional leagues or competitions
- alternative optimisation strategies

New data sources are expected to:
- land in Bronze
- conform in Silver
- surface as features or facts in Gold

Existing pipelines should not require modification to accommodate new sources.

---

## Summary

This architecture emphasises:
- explicit data contracts
- controlled execution
- clear separation of concerns
- explainable outputs

It is suitable for:
- advanced football analytics
- optimisation use cases
- experimentation with additional datasets
- extension into more operational or monetised systems

