# Databricks notebook source
# DBTITLE 1,Context
from pyspark.sql import functions as F

spark.sql("USE CATALOG football")
spark.sql("USE SCHEMA gold")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# DBTITLE 1,gold.dim_team
dim_team = (
    spark.table("football.silver.team")
      .select(
          F.col("team_id").cast("int"),
          F.col("team_name"),
          F.col("team_short_name"),
          F.col("strength").cast("int"),
          F.col("pulse_id").cast("int"),
          F.col("source_snapshot_ts"),
          F.col("source_snapshot_date"),
          F.col("source_run_id"),
      )
)

(
  dim_team.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("football.gold.dim_team")
)

print("Wrote football.gold.dim_team")

# COMMAND ----------

# DBTITLE 1,gold.dim_player
dim_player = (
    spark.table("football.silver.player")
      .select(
          F.col("player_id").cast("int"),
          F.col("team_id").cast("int"),
          F.col("position_id").cast("int"),
          F.concat_ws(" ", F.col("first_name"), F.col("second_name")).alias("player_name"),
          F.col("web_name"),
          F.col("status"),
          F.col("now_cost").cast("int"),            # tenths of £m
          F.col("now_cost_million").cast("double"), # £m
          F.col("minutes").cast("int"),
          F.col("selected_by_percent").cast("double"),
          F.col("form").cast("double"),
          F.col("points_per_game").cast("double"),
          F.col("total_points").cast("int"),
          F.col("ep_next").cast("double"),
          F.col("ep_this").cast("double"),
          F.col("chance_next_round").cast("int"),
          F.col("chance_this_round").cast("int"),
          F.col("news"),
          F.col("news_added_ts"),
          F.col("source_snapshot_ts"),
          F.col("source_snapshot_date"),
          F.col("source_run_id"),
      )
)

(
  dim_player.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("football.gold.dim_player")
)

print("Wrote football.gold.dim_player")


# COMMAND ----------

# DBTITLE 1,gold.dim_gameweek
dim_gameweek = (
    spark.table("football.silver.gameweek")
      .select(
          F.col("gameweek_id").cast("int"),
          F.col("gameweek_name"),
          F.col("deadline_time_ts"),
          F.col("finished").cast("boolean"),
          F.col("is_previous").cast("boolean"),
          F.col("is_current").cast("boolean"),
          F.col("is_next").cast("boolean"),
          F.col("data_checked").cast("boolean"),
          F.col("highest_score").cast("int"),
          F.col("average_entry_score").cast("int"),
          F.col("most_selected_player_id").cast("int"),
          F.col("most_transferred_in_player_id").cast("int"),
          F.col("top_element_player_id").cast("int"),
          F.col("source_snapshot_ts"),
          F.col("source_snapshot_date"),
          F.col("source_run_id"),
      )
)

(
  dim_gameweek.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("football.gold.dim_gameweek")
)

print("Wrote football.gold.dim_gameweek")


# COMMAND ----------

# DBTITLE 1,gold.dim_fixture
dim_fixture = (
    spark.table("football.silver.fixture")
      .select(
          F.col("fixture_id").cast("int"),
          F.col("gameweek_id").cast("int"),
          F.col("kickoff_ts"),
          F.col("team_h_id").cast("int"),
          F.col("team_a_id").cast("int"),
          F.col("team_h_difficulty").cast("int"),
          F.col("team_a_difficulty").cast("int"),
          F.col("finished").cast("boolean"),
          F.col("started").cast("boolean"),
          F.col("provisional_start_time").cast("boolean"),
          F.col("minutes").cast("int"),
          F.col("fixture_code").cast("bigint"),
          F.col("source_snapshot_ts"),
          F.col("source_snapshot_date"),
          F.col("source_run_id"),
      )
)

(
  dim_fixture.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("football.gold.dim_fixture")
)

print("Wrote football.gold.dim_fixture")


# COMMAND ----------

# DBTITLE 1,Gold DQ
def assert_pk(df, key, name):
    if df.where(F.col(key).isNull()).limit(1).count() > 0:
        raise Exception(f"{name}: NULL {key}")
    if df.groupBy(key).count().where("count > 1").limit(1).count() > 0:
        raise Exception(f"{name}: duplicate {key}")

assert_pk(dim_team, "team_id", "gold.dim_team")
assert_pk(dim_player, "player_id", "gold.dim_player")
assert_pk(dim_gameweek, "gameweek_id", "gold.dim_gameweek")
assert_pk(dim_fixture, "fixture_id", "gold.dim_fixture")

print("Gold DQ checks passed.")

# COMMAND ----------

# DBTITLE 1,Gold Validation
display(spark.sql("""
    SELECT 'dim_team' AS tbl, count(*) AS rows, max(source_snapshot_ts) AS snapshot_ts
    FROM football.gold.dim_team
    UNION ALL
    SELECT 'dim_player', count(*), max(source_snapshot_ts)
    FROM football.gold.dim_player
    UNION ALL
    SELECT 'dim_gameweek', count(*), max(source_snapshot_ts)
    FROM football.gold.dim_gameweek
    UNION ALL
    SELECT 'dim_fixture', count(*), max(source_snapshot_ts)
    FROM football.gold.dim_fixture
"""))
