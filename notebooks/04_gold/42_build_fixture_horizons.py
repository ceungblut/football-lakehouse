# Databricks notebook source
# DBTITLE 1,Context
from datetime import datetime, timezone
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")
updated_at = datetime.now(timezone.utc)
snapshot_date = updated_at.date().isoformat()

# COMMAND ----------

# Anchor gameweek: prefer is_next, fallback is_current
gw = spark.table("football.gold.dim_gameweek")

asof = gw.filter(F.col("is_next") == True).select("gameweek_id").limit(1).collect()
if not asof:
    asof = gw.filter(F.col("is_current") == True).select("gameweek_id").limit(1).collect()
if not asof:
    raise Exception("No current/next gameweek found in football.gold.dim_gameweek")

asof_gw_id = int(asof[0]["gameweek_id"])
print("As-of gameweek:", asof_gw_id, "Snapshot date: ", snapshot_date)


# COMMAND ----------

fx = spark.table("football.gold.dim_fixture")

# Fixture difficulty rows
home = fx.select(
    F.col("team_h_id").alias("team_id"),
    F.col("gameweek_id"),
    F.col("team_h_difficulty").alias("difficulty"),
    "source_snapshot_ts",
    "source_snapshot_date",
    "source_run_id",
)
away = fx.select(
    F.col("team_a_id").alias("team_id"),
    F.col("gameweek_id"),
    F.col("team_a_difficulty").alias("difficulty"),
    "source_snapshot_ts",
    "source_snapshot_date",
    "source_run_id",
)

team_fx = home.unionByName(away).filter(F.col("gameweek_id").isNotNull())
future_fx = team_fx.filter(F.col("gameweek_id") >= F.lit(asof_gw_id))

# COMMAND ----------

def horizon(df, n):
    return (
        df.filter(F.col("gameweek_id") < F.lit(asof_gw_id + n))
          .groupBy("team_id")
          .agg(
              F.avg("difficulty").alias(f"h{n}_avg_fdr"),
              F.count("*").alias(f"h{n}_fixture_count"),
              F.max("source_snapshot_ts").alias("source_snapshot_ts"),
              F.max("source_snapshot_date").alias("source_snapshot_date"),
              F.max("source_run_id").alias("source_run_id"),
          )
    )

h3 = horizon(future_fx, 3)
h5 = horizon(future_fx, 5)
h8 = horizon(future_fx, 8)

# COMMAND ----------

final = (
      h3.join(h5, "team_id", "left")
            .join(h8, "team_id", "left")
            .withColumn("snapshot_date", F.to_date(F.lit(snapshot_date)))
            .withColumn("asof_gameweek_id", F.lit(asof_gw_id).cast("int"))
            .withColumn("updated_at", F.lit(updated_at.isoformat()).cast("timestamp"))
            .select(
                  "snapshot_date", "asof_gameweek_id", "team_id",
                  "h3_avg_fdr", "h3_fixture_count",
                  "h5_avg_fdr", "h5_fixture_count",
                  "h8_avg_fdr", "h8_fixture_count",
                  "updated_at",
            )
)

final.display()

# COMMAND ----------

deduped = (
    final.dropDuplicates(["snapshot_date", "asof_gameweek_id", "team_id"])
)

# COMMAND ----------

# DBTITLE 1,Cell 5
(deduped.write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable("football.gold.fact_team_fixture_horizon_snapshot"))

# COMMAND ----------

# DBTITLE 1,Gold DQ
def assert_pk(df, key, name):
    if df.where(F.col(key).isNull()).limit(1).count() > 0:
        raise Exception(f"{name}: NULL {key}")
    if df.groupBy(key).count().where("count > 1").limit(1).count() > 0:
        raise Exception(f"{name}: duplicate {key}")

assert_pk(deduped, "team_id", "gold.fact_team_fixture_horizon_snapshot")
# assert_pk(dim_player, "player_id", "gold.dim_player")
# assert_pk(dim_gameweek, "gameweek_id", "gold.dim_gameweek")
# assert_pk(dim_fixture, "fixture_id", "gold.dim_fixture")

print("Gold DQ checks passed.")

# COMMAND ----------

# DBTITLE 1,Gold Validation
display(spark.sql("""
    SELECT 'fact_team_fixture_horizon_snapshot' AS tbl, count(*) AS rows, max(snapshot_date) AS snapshot_ts
    FROM football.gold.fact_team_fixture_horizon_snapshot
"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT snapshot_date, asof_gameweek_id, COUNT(*) AS teams
# MAGIC FROM football.gold.fact_team_fixture_horizon_snapshot
# MAGIC GROUP BY snapshot_date, asof_gameweek_id
# MAGIC ORDER BY snapshot_date DESC;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM football.gold.vw_player_fixture_horizon
# MAGIC ORDER BY h5_avg_fdr
# MAGIC LIMIT 20;

# COMMAND ----------


