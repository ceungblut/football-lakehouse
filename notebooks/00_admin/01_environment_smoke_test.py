# Databricks notebook source
# ===============================
# 00_environment_smoke_test
# Purpose: validate cluster, Spark/Python, DBFS, repos, UC, and managed table permissions
# ===============================

import sys
import os
from pyspark.sql import SparkSession
from datetime import datetime

print("=== Python & Spark Versions ===")
print("Python version:", sys.version)
print("Spark version:", spark.version)


# COMMAND ----------

# ===============================
# Basic Spark DataFrame test
# ===============================
print("=== Spark DataFrame Test ===")
df = spark.range(5)
df.show()

# COMMAND ----------

# ===============================
# DBFS and repo visibility
# ===============================
print("=== DBFS Root Listing ===")
display(dbutils.fs.ls("dbfs:/"))

print("=== Repo Listing (football-lakehouse) ===")
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
dir_path = "/Workspace" + os.path.dirname(os.path.dirname(notebook_path))
display(dbutils.fs.ls(dir_path))

# COMMAND ----------

# ===============================
# Unity Catalog basic checks
# ===============================

print("Initial catalogue:", spark.catalog.currentCatalog())
print("Initial schema:", spark.catalog.currentDatabase())

spark.sql("USE CATALOG football")
spark.sql("USE SCHEMA bronze")

print("Current catalogue:", spark.catalog.currentCatalog())
print("Current schema:", spark.catalog.currentDatabase())

print("=== Show Schemas in football ===")
spark.sql("SHOW SCHEMAS IN football").show()

print("=== Show Tables in bronze ===")
spark.sql("SHOW TABLES IN bronze").show()

print("=== Describe External Location ext-football-root ===")
spark.sql("DESCRIBE EXTERNAL LOCATION `ext-football-root`").display()

print("=== Show Grants on External Location ===")
spark.sql("SHOW GRANTS ON EXTERNAL LOCATION `ext-football-root`").display()

# COMMAND ----------

# ===============================
# Managed table creation test (dummy)
# ===============================
print("=== Dummy Managed Table Test ===")

# Create a tiny test table
spark.sql("""
CREATE OR REPLACE TABLE test_smoke (
    id INT,
    ts TIMESTAMP
)
USING DELTA
""")

# Insert a couple of rows
spark.sql("""
INSERT INTO test_smoke VALUES
(1, current_timestamp()),
(2, current_timestamp())
""")

# Read it back
spark.sql("SELECT * FROM test_smoke").show()

# Drop table to keep bronze clean
spark.sql("DROP TABLE test_smoke")

print("=== Dummy table creation test PASSED ===")


# COMMAND ----------


