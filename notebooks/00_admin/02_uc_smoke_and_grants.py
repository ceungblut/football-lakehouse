# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW GRANTS ON METASTORE;

# COMMAND ----------

# ===============================
# 01_uc_smoke_and_grants
# Purpose: validate Unity Catalog objects, external location, grants, and managed table write
# ===============================

from pyspark.sql import functions as F
from datetime import datetime

# Force correct catalog/schema
spark.sql("USE CATALOG football")
spark.sql("USE SCHEMA bronze")

print("Current catalogue:", spark.catalog.currentCatalog())
print("Current schema:", spark.catalog.currentDatabase())

# COMMAND ----------

# DBTITLE 1,Cell 2
# ===============================
# 1. Confirm catalog and schemas exist
# ===============================
spark.sql("SHOW CATALOGS").display()
spark.sql("DESCRIBE CATALOG EXTENDED football").display()
spark.sql("SHOW DATABASES").display()

# COMMAND ----------

# ===============================
# 2. List tables in bronze/silver/gold
# ===============================
spark.sql("SHOW TABLES IN football.bronze;").display()
spark.sql("SHOW TABLES IN football.silver;").display()
spark.sql("SHOW TABLES IN football.gold;").display()

# COMMAND ----------


