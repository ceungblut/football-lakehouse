# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW CATALOGS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW STORAGE CREDENTIALS;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW EXTERNAL LOCATIONS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG football;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE CATALOG EXTENDED football;

# COMMAND ----------

# DBTITLE 1,Create medallion schema
# MAGIC %sql
# MAGIC USE CATALOG football;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# DBTITLE 1,Create bronze layer tables
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- BRONZE: Raw snapshots (append-only, replayable)
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- FPL bootstrap-static raw snapshot (daily)
# MAGIC CREATE TABLE IF NOT EXISTS bronze.fpl_bootstrap_raw (
# MAGIC   snapshot_ts   TIMESTAMP,
# MAGIC   snapshot_date DATE,
# MAGIC   ingest_ts   TIMESTAMP,
# MAGIC   run_id        STRING,
# MAGIC   source_url    STRING,
# MAGIC   http_status   INT,
# MAGIC   payload_json  STRING,
# MAGIC   payload_size_bytes BIGINT,
# MAGIC   payload_sha256 STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact   = true
# MAGIC );
# MAGIC
# MAGIC -- Live Match Pulse: raw poll snapshots (micro-batch polling, match windows)
# MAGIC CREATE TABLE IF NOT EXISTS bronze.live_events_raw (
# MAGIC   snapshot_ts   TIMESTAMP,
# MAGIC   snapshot_date DATE,
# MAGIC   run_id        STRING,
# MAGIC   source        STRING,
# MAGIC   http_status   INT,
# MAGIC   payload_json  STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact   = true
# MAGIC );
# MAGIC
# MAGIC -- Auto Loader output: streamed file ingest into Bronze Delta
# MAGIC -- payload is stored as a string to keep Bronze permissive.
# MAGIC CREATE TABLE IF NOT EXISTS bronze.live_events_stream_raw (
# MAGIC   ingest_ts     TIMESTAMP,
# MAGIC   snapshot_date DATE,
# MAGIC   source_file   STRING,
# MAGIC   payload       STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact   = true
# MAGIC );
# MAGIC
# MAGIC -- (Optional later) Fixtures raw snapshots
# MAGIC CREATE TABLE IF NOT EXISTS bronze.fpl_fixtures_raw (
# MAGIC   snapshot_ts   TIMESTAMP,
# MAGIC   snapshot_date DATE,
# MAGIC   run_id        STRING,
# MAGIC   source_url    STRING,
# MAGIC   http_status   INT,
# MAGIC   payload_json  STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact   = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SILVER: Typed, conformed tables
# MAGIC -- Keep schemas minimal now; evolve with explicit typing decisions.
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.team (
# MAGIC   snapshot_date DATE,
# MAGIC   run_id        STRING,
# MAGIC   team_id       INT,
# MAGIC   team_name     STRING,
# MAGIC   team_short_name STRING,
# MAGIC   strength      INT,
# MAGIC   updated_at    TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact   = true
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.gameweek (
# MAGIC   snapshot_date   DATE,
# MAGIC   run_id          STRING,
# MAGIC   gameweek_id     INT,
# MAGIC   gameweek_name   STRING,
# MAGIC   deadline_time_utc STRING,
# MAGIC   finished        BOOLEAN,
# MAGIC   is_current      BOOLEAN,
# MAGIC   is_next         BOOLEAN,
# MAGIC   updated_at      TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact   = true
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.player (
# MAGIC   snapshot_date DATE,
# MAGIC   run_id        STRING,
# MAGIC   player_id     INT,
# MAGIC   first_name    STRING,
# MAGIC   second_name   STRING,
# MAGIC   web_name      STRING,
# MAGIC   team_id       INT,
# MAGIC   position_id   INT,
# MAGIC   now_cost_tenths_million INT,
# MAGIC   status        STRING,
# MAGIC   minutes       INT,
# MAGIC   total_points  INT,
# MAGIC   selected_by_percent DOUBLE,
# MAGIC   form          DOUBLE,
# MAGIC   updated_at    TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact   = true
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.fixture (
# MAGIC   snapshot_date DATE,
# MAGIC   run_id        STRING,
# MAGIC   fixture_id    INT,
# MAGIC   gameweek_id   INT,
# MAGIC   kickoff_time_utc STRING,
# MAGIC   home_team_id  INT,
# MAGIC   away_team_id  INT,
# MAGIC   home_difficulty INT,
# MAGIC   away_difficulty INT,
# MAGIC   finished      BOOLEAN,
# MAGIC   updated_at    TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact   = true
# MAGIC );
# MAGIC
# MAGIC -- Canonical live event timeline (MERGE target)
# MAGIC -- event_key should be deterministic (fixture + timestamp + event type + involved entity ids).
# MAGIC CREATE TABLE IF NOT EXISTS silver.live_event (
# MAGIC   event_key   STRING,
# MAGIC   fixture_id  STRING,
# MAGIC   event_ts    TIMESTAMP,
# MAGIC   team_id     STRING,
# MAGIC   player_id   STRING,
# MAGIC   event_type  STRING,
# MAGIC   payload     STRING,
# MAGIC   updated_at  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact   = true
# MAGIC );
# MAGIC
# MAGIC -- Optional: fixture state timeline derived from events (score, status, etc.)
# MAGIC CREATE TABLE IF NOT EXISTS silver.live_fixture_state (
# MAGIC   fixture_id  STRING,
# MAGIC   state_ts    TIMESTAMP,
# MAGIC   home_score  INT,
# MAGIC   away_score  INT,
# MAGIC   status      STRING,
# MAGIC   payload     STRING,
# MAGIC   updated_at  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact   = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- GOLD: Optimisation-ready tables
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- Dimensions (published from Silver)
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_team (
# MAGIC   team_id INT,
# MAGIC   team_name STRING,
# MAGIC   team_short_name STRING,
# MAGIC   strength INT,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_gameweek (
# MAGIC   gameweek_id INT,
# MAGIC   gameweek_name STRING,
# MAGIC   deadline_time_utc STRING,
# MAGIC   finished BOOLEAN,
# MAGIC   is_current BOOLEAN,
# MAGIC   is_next BOOLEAN,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_player (
# MAGIC   player_id INT,
# MAGIC   first_name STRING,
# MAGIC   second_name STRING,
# MAGIC   web_name STRING,
# MAGIC   team_id INT,
# MAGIC   position_id INT,
# MAGIC   now_cost_tenths_million INT,
# MAGIC   status STRING,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_fixture (
# MAGIC   fixture_id INT,
# MAGIC   gameweek_id INT,
# MAGIC   kickoff_time_utc STRING,
# MAGIC   home_team_id INT,
# MAGIC   away_team_id INT,
# MAGIC   home_difficulty INT,
# MAGIC   away_difficulty INT,
# MAGIC   finished BOOLEAN,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Daily snapshots for optimisation constraints and dynamics
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_price_snapshot (
# MAGIC   snapshot_date DATE,
# MAGIC   player_id INT,
# MAGIC   now_cost_tenths_million INT,
# MAGIC   selected_by_percent DOUBLE,
# MAGIC   form DOUBLE,
# MAGIC   status STRING,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Core modelling / optimisation grain (player x gameweek)
# MAGIC -- This will be built once player history ingestion exists.
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_player_gameweek (
# MAGIC   gameweek_id INT,
# MAGIC   player_id INT,
# MAGIC   team_id INT,
# MAGIC   minutes INT,
# MAGIC   points INT,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Live-derived rolling features (short-horizon form + minutes risk)
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_live_form (
# MAGIC   snapshot_date DATE,
# MAGIC   player_id STRING,
# MAGIC   fixture_window STRING,
# MAGIC   form_score DOUBLE,
# MAGIC   minutes_risk DOUBLE,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Recommendation output (the "product" table)
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_transfer_recommendation (
# MAGIC   run_id STRING,
# MAGIC   snapshot_date DATE,
# MAGIC   out_player_id INT,
# MAGIC   in_player_id INT,
# MAGIC   expected_points_delta DOUBLE,
# MAGIC   cost_delta_tenths_million INT,
# MAGIC   constraint_flags STRING,
# MAGIC   rationale STRING,
# MAGIC   created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
