# Databricks notebook source
# DBTITLE 1,Context
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark.conf.set("spark.sql.session.timeZone", "UTC")

spark.sql("USE CATALOG football")
spark.sql("USE SCHEMA silver")
bronze_tbl = "football.bronze.fpl_bootstrap_raw"

# COMMAND ----------

# DBTITLE 1,Read latest snapshot
latest = (
    spark.table(bronze_tbl)
      .where(F.col("http_status") == 200)
      .orderBy(F.col("snapshot_ts").desc())
      .select("snapshot_ts", "snapshot_date", "run_id", "source_url", "payload_json")
      .limit(1).collect()
)

if not latest:
    raise Exception("No successful (http_status=200) rows found in bronze.fpl_bootstrap_raw")

latest_row = latest[0]
source_snapshot_ts = latest_row["snapshot_ts"]
source_snapshot_date = latest_row["snapshot_date"]
source_run_id = latest_row["run_id"]
source_url = latest_row["source_url"]

print("Using snapshot_ts:", source_snapshot_ts)
print("Using snapshot_date:", source_snapshot_date)
print("Using run_id:", source_run_id)
print("Source URL:", source_url)

# COMMAND ----------

# DBTITLE 1,Parse JSON
element_schema = T.StructType([
    T.StructField("id", T.IntegerType(), True),
    T.StructField("first_name", T.StringType(), True),
    T.StructField("second_name", T.StringType(), True),
    T.StructField("web_name", T.StringType(), True),
    T.StructField("team", T.IntegerType(), True),
    T.StructField("element_type", T.IntegerType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("now_cost", T.IntegerType(), True),  # tenths of £m
    T.StructField("chance_of_playing_next_round", T.IntegerType(), True),
    T.StructField("chance_of_playing_this_round", T.IntegerType(), True),
    T.StructField("minutes", T.IntegerType(), True),
    T.StructField("selected_by_percent", T.StringType(), True),
    T.StructField("form", T.StringType(), True),
    T.StructField("points_per_game", T.StringType(), True),
    T.StructField("total_points", T.IntegerType(), True),
    T.StructField("transfers_in_event", T.IntegerType(), True),
    T.StructField("transfers_out_event", T.IntegerType(), True),
    T.StructField("ep_next", T.StringType(), True),
    T.StructField("ep_this", T.StringType(), True),
    T.StructField("news", T.StringType(), True),
    T.StructField("news_added", T.StringType(), True),
])

team_schema = T.StructType([
    T.StructField("id", T.IntegerType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("short_name", T.StringType(), True),
    T.StructField("strength", T.IntegerType(), True),
    T.StructField("pulse_id", T.IntegerType(), True),
])

event_schema = T.StructType([
    T.StructField("id", T.IntegerType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("deadline_time", T.StringType(), True),
    T.StructField("finished", T.BooleanType(), True),
    T.StructField("is_previous", T.BooleanType(), True),
    T.StructField("is_current", T.BooleanType(), True),
    T.StructField("is_next", T.BooleanType(), True),
    T.StructField("data_checked", T.BooleanType(), True),
    T.StructField("highest_score", T.IntegerType(), True),
    T.StructField("average_entry_score", T.IntegerType(), True),
    T.StructField("most_selected", T.IntegerType(), True),
    T.StructField("most_transferred_in", T.IntegerType(), True),
    T.StructField("top_element", T.IntegerType(), True),
])

bootstrap_schema = T.StructType([
    T.StructField("elements", T.ArrayType(element_schema), True),
    T.StructField("teams", T.ArrayType(team_schema), True),
    T.StructField("events", T.ArrayType(event_schema), True),
])

parsed = (
    spark.createDataFrame([latest_row])
      .select(
          "snapshot_ts", "snapshot_date", "run_id", "source_url",
          F.from_json(F.col("payload_json"), bootstrap_schema).alias("j")
      )
)

# Safety check: parsed JSON should not be null
bad_parse = parsed.where(F.col("j").isNull()).count()
if bad_parse > 0:
    raise Exception("JSON parse failed: from_json returned null struct. Check that payload_json is valid JSON string.")

parsed.display()

# COMMAND ----------

# DBTITLE 1,Build Silver: team
teams = (
    parsed
      .select("snapshot_ts", "snapshot_date", "run_id", F.explode("j.teams").alias("t"))
      .select(
          F.col("t.id").cast("int").alias("team_id"),
          F.col("t.name").alias("team_name"),
          F.col("t.short_name").alias("team_short_name"),
          F.col("t.strength").cast("int").alias("strength"),
          F.col("t.pulse_id").cast("int").alias("pulse_id"),
          F.col("snapshot_ts").alias("source_snapshot_ts"),
          F.col("snapshot_date").alias("source_snapshot_date"),
          F.col("run_id").alias("source_run_id"),
      )
)

print("teams rows:", teams.count())


# COMMAND ----------

# DBTITLE 1,Build Silver: player
def to_double_or_null(c):
    # handles "", None, "0.0" etc
    return F.when(F.trim(c).isin("", "null", "None"), F.lit(None).cast("double")).otherwise(c.cast("double"))

players = (
    parsed
      .select("snapshot_ts", "snapshot_date", "run_id", F.explode("j.elements").alias("e"))
      .select(
          F.col("e.id").cast("int").alias("player_id"),
          F.col("e.first_name").alias("first_name"),
          F.col("e.second_name").alias("second_name"),
          F.col("e.web_name").alias("web_name"),
          F.col("e.team").cast("int").alias("team_id"),
          F.col("e.element_type").cast("int").alias("position_id"),
          F.col("e.status").alias("status"),
          F.col("e.now_cost").cast("int").alias("now_cost"),  # tenths of £m
          (F.col("e.now_cost").cast("double") / F.lit(10.0)).alias("now_cost_million"),
          F.col("e.chance_of_playing_next_round").cast("int").alias("chance_next_round"),
          F.col("e.chance_of_playing_this_round").cast("int").alias("chance_this_round"),
          F.col("e.minutes").cast("int").alias("minutes"),
          to_double_or_null(F.col("e.selected_by_percent")).alias("selected_by_percent"),
          to_double_or_null(F.col("e.form")).alias("form"),
          to_double_or_null(F.col("e.points_per_game")).alias("points_per_game"),
          F.col("e.total_points").cast("int").alias("total_points"),
          F.col("e.transfers_in_event").cast("int").alias("transfers_in_event"),
          F.col("e.transfers_out_event").cast("int").alias("transfers_out_event"),
          to_double_or_null(F.col("e.ep_next")).alias("ep_next"),
          to_double_or_null(F.col("e.ep_this")).alias("ep_this"),
          F.col("e.news").alias("news"),
          F.to_timestamp("e.news_added").alias("news_added_ts"),
          F.col("snapshot_ts").alias("source_snapshot_ts"),
          F.col("snapshot_date").alias("source_snapshot_date"),
          F.col("run_id").alias("source_run_id"),
      )
)

print("players rows:", players.count())

# COMMAND ----------

# DBTITLE 1,Build Silver: gameweek
gameweeks = (
    parsed
      .select("snapshot_ts", "snapshot_date", "run_id", F.explode("j.events").alias("g"))
      .select(
          F.col("g.id").cast("int").alias("gameweek_id"),
          F.col("g.name").alias("gameweek_name"),
          F.to_timestamp("g.deadline_time").alias("deadline_time_ts"),
          F.col("g.finished").cast("boolean").alias("finished"),
          F.col("g.is_previous").cast("boolean").alias("is_previous"),
          F.col("g.is_current").cast("boolean").alias("is_current"),
          F.col("g.is_next").cast("boolean").alias("is_next"),
          F.col("g.data_checked").cast("boolean").alias("data_checked"),
          F.col("g.highest_score").cast("int").alias("highest_score"),
          F.col("g.average_entry_score").cast("int").alias("average_entry_score"),
          F.col("g.most_selected").cast("int").alias("most_selected_player_id"),
          F.col("g.most_transferred_in").cast("int").alias("most_transferred_in_player_id"),
          F.col("g.top_element").cast("int").alias("top_element_player_id"),
          F.col("snapshot_ts").alias("source_snapshot_ts"),
          F.col("snapshot_date").alias("source_snapshot_date"),
          F.col("run_id").alias("source_run_id"),
      )
)

print("gameweeks rows:", gameweeks.count())

# COMMAND ----------

# DBTITLE 1,Data quality check
def assert_no_null_keys(df, key_col, name):
    n = df.where(F.col(key_col).isNull()).count()
    if n > 0:
        raise Exception(f"DQ fail: {name} has {n} rows with NULL {key_col}")

def assert_no_duplicate_keys(df, key_col, name):
    dup = (
        df.groupBy(key_col)
          .count()
          .where(F.col("count") > 1)
          .count()
    )
    if dup > 0:
        raise Exception(f"DQ fail: {name} has {dup} duplicate {key_col} values")

assert_no_null_keys(teams, "team_id", "silver.team")
assert_no_duplicate_keys(teams, "team_id", "silver.team")

assert_no_null_keys(players, "player_id", "silver.player")
assert_no_duplicate_keys(players, "player_id", "silver.player")

assert_no_null_keys(gameweeks, "gameweek_id", "silver.gameweek")
assert_no_duplicate_keys(gameweeks, "gameweek_id", "silver.gameweek")

print("DQ checks passed.")

# COMMAND ----------

# DBTITLE 1,Write Silver tables (overwrite)
(
  teams.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("football.silver.team")
)

(
  players.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("football.silver.player")
)

(
  gameweeks.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("football.silver.gameweek")
)

print("Wrote silver.team, silver.player, silver.gameweek")

# COMMAND ----------

# DBTITLE 1,Verify data
display(spark.sql("""
    SELECT 'team' AS tbl, count(*) AS rows, max(source_snapshot_ts) AS snapshot_ts 
    FROM football.silver.team
    UNION ALL
    SELECT 'player' AS tbl, count(*) AS rows, max(source_snapshot_ts) AS snapshot_ts 
    FROM football.silver.player
    UNION ALL
    SELECT 'gameweek' AS tbl, count(*) AS rows, max(source_snapshot_ts) AS snapshot_ts 
    FROM football.silver.gameweek
"""))

# COMMAND ----------


