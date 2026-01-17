-- Unity Catalog objects (idempotent)
-- This file documents and creates core tables used by the lakehouse.
-- Keep schemas stable and evolve intentionally.

USE CATALOG football;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- BRONZE: Raw snapshots (append-only, replayable)
-- ============================================================

-- FPL bootstrap-static raw snapshot (daily)
CREATE TABLE IF NOT EXISTS bronze.fpl_bootstrap_raw (
  snapshot_ts   TIMESTAMP,
  snapshot_date DATE,
  run_id        STRING,
  source_url    STRING,
  http_status   INT,
  payload_json  STRING
)
USING DELTA
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
);

-- Live Match Pulse: raw poll snapshots (micro-batch polling, match windows)
CREATE TABLE IF NOT EXISTS bronze.live_events_raw (
  snapshot_ts   TIMESTAMP,
  snapshot_date DATE,
  run_id        STRING,
  source        STRING,
  http_status   INT,
  payload_json  STRING
)
USING DELTA
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
);

-- Auto Loader output: streamed file ingest into Bronze Delta
-- payload is stored as a string to keep Bronze permissive.
CREATE TABLE IF NOT EXISTS bronze.live_events_stream_raw (
  ingest_ts     TIMESTAMP,
  snapshot_date DATE,
  source_file   STRING,
  payload       STRING
)
USING DELTA
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
);

-- (Optional later) Fixtures raw snapshots
CREATE TABLE IF NOT EXISTS bronze.fpl_fixtures_raw (
  snapshot_ts   TIMESTAMP,
  snapshot_date DATE,
  run_id        STRING,
  source_url    STRING,
  http_status   INT,
  payload_json  STRING
)
USING DELTA
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
);

-- ============================================================
-- SILVER: Typed, conformed tables
-- Keep schemas minimal now; evolve with explicit typing decisions.
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.team (
  snapshot_date DATE,
  run_id        STRING,
  team_id       INT,
  team_name     STRING,
  team_short_name STRING,
  strength      INT,
  updated_at    TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver.gameweek (
  snapshot_date   DATE,
  run_id          STRING,
  gameweek_id     INT,
  gameweek_name   STRING,
  deadline_time_utc STRING,
  finished        BOOLEAN,
  is_current      BOOLEAN,
  is_next         BOOLEAN,
  updated_at      TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver.player (
  snapshot_date DATE,
  run_id        STRING,
  player_id     INT,
  first_name    STRING,
  second_name   STRING,
  web_name      STRING,
  team_id       INT,
  position_id   INT,
  now_cost_tenths_million INT,
  status        STRING,
  minutes       INT,
  total_points  INT,
  selected_by_percent DOUBLE,
  form          DOUBLE,
  updated_at    TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver.fixture (
  snapshot_date DATE,
  run_id        STRING,
  fixture_id    INT,
  gameweek_id   INT,
  kickoff_time_utc STRING,
  home_team_id  INT,
  away_team_id  INT,
  home_difficulty INT,
  away_difficulty INT,
  finished      BOOLEAN,
  updated_at    TIMESTAMP
)
USING DELTA;

-- Canonical live event timeline (MERGE target)
-- event_key should be deterministic (fixture + timestamp + event type + involved entity ids).
CREATE TABLE IF NOT EXISTS silver.live_event (
  event_key   STRING,
  fixture_id  STRING,
  event_ts    TIMESTAMP,
  team_id     STRING,
  player_id   STRING,
  event_type  STRING,
  payload     STRING,
  updated_at  TIMESTAMP
)
USING DELTA;

-- Optional: fixture state timeline derived from events (score, status, etc.)
CREATE TABLE IF NOT EXISTS silver.live_fixture_state (
  fixture_id  STRING,
  state_ts    TIMESTAMP,
  home_score  INT,
  away_score  INT,
  status      STRING,
  payload     STRING,
  updated_at  TIMESTAMP
)
USING DELTA;

-- ============================================================
-- GOLD: Optimisation-ready tables
-- ============================================================

-- Dimensions (published from Silver)
CREATE TABLE IF NOT EXISTS gold.dim_team (
  team_id INT,
  team_name STRING,
  team_short_name STRING,
  strength INT,
  updated_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_gameweek (
  gameweek_id INT,
  gameweek_name STRING,
  deadline_time_utc STRING,
  finished BOOLEAN,
  is_current BOOLEAN,
  is_next BOOLEAN,
  updated_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_player (
  player_id INT,
  first_name STRING,
  second_name STRING,
  web_name STRING,
  team_id INT,
  position_id INT,
  now_cost_tenths_million INT,
  status STRING,
  updated_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_fixture (
  fixture_id INT,
  gameweek_id INT,
  kickoff_time_utc STRING,
  home_team_id INT,
  away_team_id INT,
  home_difficulty INT,
  away_difficulty INT,
  finished BOOLEAN,
  updated_at TIMESTAMP
)
USING DELTA;

-- Daily snapshots for optimisation constraints and dynamics
CREATE TABLE IF NOT EXISTS gold.fact_price_snapshot (
  snapshot_date DATE,
  player_id INT,
  now_cost_tenths_million INT,
  selected_by_percent DOUBLE,
  form DOUBLE,
  status STRING,
  updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (snapshot_date);

-- Core modelling / optimisation grain (player x gameweek)
-- This will be built once player history ingestion exists.
CREATE TABLE IF NOT EXISTS gold.fact_player_gameweek (
  gameweek_id INT,
  player_id INT,
  team_id INT,
  minutes INT,
  points INT,
  updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (gameweek_id);

-- Live-derived rolling features (short-horizon form + minutes risk)
CREATE TABLE IF NOT EXISTS gold.fact_live_form (
  snapshot_date DATE,
  player_id STRING,
  fixture_window STRING,
  form_score DOUBLE,
  minutes_risk DOUBLE,
  updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (snapshot_date);

-- Recommendation output (the "product" table)
CREATE TABLE IF NOT EXISTS gold.fact_transfer_recommendation (
  run_id STRING,
  snapshot_date DATE,
  out_player_id INT,
  in_player_id INT,
  expected_points_delta DOUBLE,
  cost_delta_tenths_million INT,
  constraint_flags STRING,
  rationale STRING,
  created_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (snapshot_date);

