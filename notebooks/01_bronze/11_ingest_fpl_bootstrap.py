# Databricks notebook source
# DBTITLE 1,Context
from datetime import datetime, timezone
import uuid

spark.conf.set("spark.sql.session.timeZone", "UTC")

spark.sql("USE CATALOG football")
spark.sql("USE SCHEMA bronze")

dbutils.widgets.text("source_url", "https://fantasy.premierleague.com/api/bootstrap-static/", "source_url")
dbutils.widgets.text("run_id", "", "run_id (optional)")
dbutils.widgets.text("write_mode", "append", "write_mode (append only)")
dbutils.widgets.text("timeout_seconds", "30", "timeout_seconds")

# COMMAND ----------

# DBTITLE 1,Params
source_url = dbutils.widgets.get("source_url").strip()
run_id_in = dbutils.widgets.get("run_id").strip()
write_mode = dbutils.widgets.get("write_mode").strip().lower()
timeout_seconds = dbutils.widgets.get("timeout_seconds").strip().lower()

assert write_mode in ("append",), f"write_mode must be append; got: {write_mode}"

run_id = run_id_in if run_id_in else str(uuid.uuid4())
snapshot_ts = datetime.now(timezone.utc)
snapshot_date = snapshot_ts.date().isoformat()  # YYYY-MM-DD

print("source_url:", source_url)
print("run_id:", run_id)
print("snapshot_ts(utc):", snapshot_ts.isoformat())
print("snapshot_date:", snapshot_date)

# COMMAND ----------

# DBTITLE 1,Params
import requests
import json
import hashlib

headers = {
    "User-Agent": "football-lakehouse/1.0 (+Databricks; UC; snapshot ingestion)"
}

resp = requests.get(
    source_url,
    headers=headers,
    timeout=(float(timeout_seconds), float(timeout_seconds))
)
http_status = int(resp.status_code)
payload_json = None
payload_size_bytes = None
payload_sha256 = None
error_text = None

if http_status == 200:
    try:
        parsed = resp.json()  
        payload_json = json.dumps(parsed, separators=(",", ":"), sort_keys=True)
    except ValueError:
        payload_json = resp.text
        error_text = "JSON parse failed on 200 response"
else:
    payload_json = resp.text
    error_text = resp.text[:2000]

# Hash + size of payload
if payload_json is not None:
    payload_bytes = payload_json.encode("utf-8")
    payload_size_bytes = len(payload_bytes)
    payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()

print("http_status:", http_status)
print("payload_size_bytes:", payload_size_bytes)
print("payload_sha256:", payload_sha256)
if error_text:
    print("error_text (truncated):", error_text[:500])

# COMMAND ----------

# DBTITLE 1,Raw schema
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, TimestampType, DateType, StringType, IntegerType, LongType
)
from pyspark.sql.functions import col, to_date    

table_name = "football.bronze.fpl_bootstrap_raw"

schema = StructType([
    StructField("snapshot_ts", TimestampType(), False),
    StructField("ingest_ts", TimestampType(), False),
    StructField("snapshot_date", DateType(), False),
    StructField("run_id", StringType(), False),
    StructField("source_url", StringType(), False),
    StructField("http_status", IntegerType(), False),
    StructField("payload_size_bytes", LongType(), True),
    StructField("payload_sha256", StringType(), True),
    StructField("payload_json", StringType(), True),
])

# Define ingest_ts as current UTC time
ingest_ts = datetime.now(timezone.utc)

rows = [Row(
    snapshot_ts=snapshot_ts.replace(tzinfo=None),   
    ingest_ts=ingest_ts.replace(tzinfo=None),
    snapshot_date=datetime.fromisoformat(snapshot_date).date(),
    run_id=run_id,
    source_url=source_url,
    http_status=http_status,
    payload_size_bytes=payload_size_bytes,
    payload_sha256=payload_sha256,
    payload_json=payload_json
)]

df = spark.createDataFrame(rows, schema=schema) # echeck partition date 

# Ensure table exists (idempotent). If objects.sql already created it, this is a no-op.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    snapshot_ts TIMESTAMP NOT NULL,
    snapshot_date DATE NOT NULL,
    run_id STRING NOT NULL,
    source_url STRING NOT NULL,
    http_status INT NOT NULL,
    payload_size_bytes BIGINT,
    payload_sha256 STRING,
    payload_json STRING
    )
    USING DELTA
    """)

# COMMAND ----------

# DBTITLE 1,Write to Delta
exists = spark.sql(f"""
    SELECT 1
    FROM football.bronze.fpl_bootstrap_raw
    WHERE 
        snapshot_date = date('{snapshot_date}')
        AND payload_sha256 = '{payload_sha256}'
    LIMIT 1
    """).count() > 0

if exists:
    print("Duplicate payload detected; skipping write.")

else:
    df.write.format("delta").mode("append").saveAsTable(table_name)

# COMMAND ----------

display(spark.sql("""
SELECT snapshot_ts, run_id, http_status, length(payload_json) AS payload_len, source_url
FROM football.bronze.fpl_bootstrap_raw
ORDER BY snapshot_ts DESC
LIMIT 5
"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   snapshot_date,
# MAGIC   count(*) AS rows,
# MAGIC   max(snapshot_ts) AS latest_ts,
# MAGIC   sum(CASE WHEN http_status = 200 THEN 1 ELSE 0 END) AS ok_rows
# MAGIC FROM football.bronze.fpl_bootstrap_raw
# MAGIC GROUP BY snapshot_date
# MAGIC ORDER BY snapshot_date DESC;

# COMMAND ----------


