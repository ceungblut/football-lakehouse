# Execution Backlog

This backlog is ordered to deliver a working, governed lakehouse quickly,
then progressively add incremental ingestion, streaming mechanics, Delta patterns,
and transfer optimisation outputs.

## Phase 1 — Repo + Environment (foundation)
- [ ] Step 1B: Hook GitHub repo into Databricks Repos
- [ ] Run environment smoke test notebook:
  - [ ] Verify Unity Catalog access (SHOW CATALOGS / SCHEMAS)
  - [ ] Verify external location access (write/read a small Delta table)
  - [ ] Confirm default catalog/schema behaviour in Jobs context
- [ ] Confirm cost safety defaults:
  - [ ] Single-node job clusters only
  - [ ] Auto-termination 10–15 minutes everywhere
  - [ ] No always-on clusters

## Phase 2 — Slice v1: FPL foundation (Batch, daily)
- [ ] Implement Bronze daily snapshot ingestion:
  - [ ] `bronze.fpl_bootstrap_raw` (append-only, partitioned by snapshot_date)
  - [ ] Add run metadata: snapshot_ts, run_id, source_url, http_status
- [ ] Build Silver dims from latest snapshot:
  - [ ] `silver.player`
  - [ ] `silver.team`
  - [ ] `silver.gameweek`
- [ ] Add Silver DQ checks (fail fast):
  - [ ] Null primary keys
  - [ ] Duplicate key checks
- [ ] Publish Gold dims:
  - [ ] `gold.dim_player`
  - [ ] `gold.dim_team`
  - [ ] `gold.dim_gameweek`
- [ ] Create Workflow: Slice v1 (Bronze → Silver → Gold)
  - [ ] Task dependencies wired
  - [ ] Parameters: run_id, snapshot_ts, mode=daily|backfill
  - [ ] Retries/backoff for ingestion task

## Phase 3 — Fixtures + Price snapshots (Optimisation prerequisites)
- [ ] Add fixtures ingestion:
  - [ ] Bronze raw fixtures snapshots
  - [ ] Silver `silver.fixture`
  - [ ] Gold `gold.dim_fixture`
- [ ] Build `gold.fact_price_snapshot` (daily)
  - [ ] Uses FPL elements fields: now_cost, selected_by_percent, form
  - [ ] Partition by snapshot_date
  - [ ] Track price deltas (optional derived view/table later)

## Phase 4 — Delta patterns (must-have reliability)
- [ ] MERGE pattern (idempotent upsert)
  - [ ] Implement MERGE into `silver.live_event` (canonical event timeline)
  - [ ] Ensure deterministic event_key and rerun safety
- [ ] Schema evolution handling
  - [ ] Bronze: permissive ingestion / rescue column strategy for drift
  - [ ] Silver: explicit typing and controlled column additions
- [ ] Delta time travel demo
  - [ ] `DESCRIBE HISTORY` on key tables
  - [ ] Query `VERSION AS OF` / `TIMESTAMP AS OF`
  - [ ] Document debugging approach in `docs/runbook.md`

## Phase 5 — Slice v2: Live Match Pulse (match windows)
Goal: enhance short-horizon features (minutes risk, live form, team momentum) with cost control.

- [ ] Implement REST polling during match windows:
  - [ ] Append raw payload snapshots to `bronze.live_events_raw`
  - [ ] Include snapshot_ts/run_id/http_status/source
  - [ ] Backoff + retry strategy
- [ ] Land polled payloads as JSON files into ADLS landing folder:
  - [ ] Partition by snapshot_date and (if available) fixture_id
  - [ ] Ensure idempotent naming (run_id + sequence)
- [ ] Structured Streaming ingestion (Auto Loader):
  - [ ] Read landing folder using `cloudFiles`
  - [ ] Write to `bronze.live_events_stream_raw`
  - [ ] Use finite trigger (`availableNow`) + checkpoints
  - [ ] Enable schema evolution mode (addNewColumns) + rescue column
- [ ] Silver conformance:
  - [ ] Parse streamed payloads into canonical `silver.live_event`
  - [ ] MERGE-based dedupe/upsert (late-arriving updates)
  - [ ] Build `silver.live_fixture_state` (score/state timeline)
- [ ] Gold features:
  - [ ] Build `gold.fact_live_form` (rolling features: form_score, minutes_risk)
  - [ ] Validate feature stability and explainability

## Phase 6 — Slice v3: Modelling + Transfer recommendation (product output)
- [ ] Define modelling target:
  - [ ] predicted_points_next_3_gws (default)
- [ ] Feature pipeline:
  - [ ] Join dims + price_snapshot + fixture difficulty + live_form features
  - [ ] Materialise training set
- [ ] Train baseline model:
  - [ ] Log metrics and model artefacts (MLflow)
  - [ ] Start with interpretable baseline (e.g. GBT regressor)
- [ ] Score players for next N gameweeks:
  - [ ] Persist predictions (optional table/view)
- [ ] Deterministic optimiser:
  - [ ] Input: current squad, budget, constraints
  - [ ] Output: best single transfer (out → in)
  - [ ] Enforce constraints (budget, position, max 3 per team)
- [ ] Persist results:
  - [ ] Create `gold.fact_transfer_recommendation`
  - [ ] Store rationale + constraint flags

## Phase 7 — Ops hardening (cheap but professional)
- [ ] Maintenance notebook:
  - [ ] Small-file controls via table properties (Auto Optimize)
  - [ ] Optional OPTIMIZE decisions for Gold only (sparingly)
  - [ ] Safe VACUUM guidance (retention policy awareness)
- [ ] Documentation tightening:
  - [ ] Keep README high-level and stable
  - [ ] Update architecture/runbook as features land
- [ ] Optional tests:
  - [ ] Minimal schema tests for parsing logic
  - [ ] Recommendation constraint tests

