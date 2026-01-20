# Football Lakehouse (Azure Databricks)

### Purpose
This repository implements a governed football analytics lakehouse on Azure Databricks,
designed to support Fantasy Premier League (FPL) transfer optimisation and advanced
football analytics.

The project focuses on **clean data engineering**, **reproducible pipelines**, and
**explainable optimisation**, rather than building a consumer-facing application.

### Scope
This project is intended to:
- Build a robust Bronze / Silver / Gold lakehouse
- Handle both batch and streaming-style ingestion
- Produce optimisation-ready datasets for squad and transfer decisions
- Demonstrate cost-aware, production-grade data engineering patterns

It explicitly does **not** aim to:
- Provide a hosted website or UI
- Compete directly with existing fantasy football tools
- Optimise purely for prediction accuracy over explainability
- Use always-on or cost-heavy infrastructure

### Design principles
- Unity Catalog from day one
- Managed identity only (no storage keys or embedded secrets)
- Jobs-first compute (no always-on clusters)
- Single-node clusters only
- Auto-termination everywhere
- Cost control > convenience
- Explicit logic over managed abstractions (e.g. Jobs over DLT)

### Architecture
The system follows a Medallion architecture:

- **Bronze**: raw, append-only, replayable snapshots
- **Silver**: typed, deduplicated, conformed tables
- **Gold**: optimisation-ready dimensions, facts, and recommendations

Structured Streaming is used as an **execution pattern**, not a data layer.
Streaming pipelines write to Bronze tables and are finite, checkpointed jobs
(e.g. Auto Loader with `availableNow` semantics).

### Data sources
- **FPL API**  
  Authoritative source for prices, positions, squads, and historical points.

- **Live Match Pulse (optional, incremental)**  
  REST-based polling of live match events during match windows to enhance:
  - short-term form
  - minutes-risk estimation
  - team momentum features

The architecture is intentionally extensible to support additional datasets
(e.g. betting odds, advanced match events) without refactoring core pipelines.

### Repository structure
notebooks/
- 00_admin        # environment and Unity Catalog smoke tests
- 01_bronze       # batch ingestion (raw snapshots)
- 02_streaming    # structured streaming mechanics (Auto Loader)
- 03_silver       # normalisation, MERGE patterns, idempotency
- 04_gold         # analytics / optimisation-ready tables
- 05_ml           # feature engineering and modelling
- 06_ops          # maintenance, schema evolution, time travel

resources/
- Unity Catalog objects and grants

workflows/         # Databricks job/workflow definitions

src/
  football_lakehouse/
  - fpl           # FPL API clients and schemas
  - live          # live match pulse ingestion logic
  - transforms    # shared transformation logic
  - ml            # modelling and optimisation helpers


### Operational model
- All pipelines run as **Databricks Jobs**
- No always-on clusters
- Streaming jobs are finite and checkpointed
- Costs are controlled via:
  - single-node clusters
  - short auto-termination windows
  - explicit scheduling

### Optimisation philosophy
Transfer recommendations are built using:
- predictive modelling (expected points over short horizons)
- deterministic optimisation under FPL constraints
- explicit, explainable drivers (price, form, fixtures, minutes risk)

The goal is **decision quality and transparency**, not black-box prediction.

### Status
This repository is under active development and is designed to be forked,
extended, or adapted for other football analytics or optimisation use cases.


