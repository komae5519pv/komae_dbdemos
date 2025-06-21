# Databricks notebook source
MY_CATALOG = "komae_cleanroom"
MY_SCHEMA = "default"

# COMMAND ----------

# カタログ、スキーマ、ボリューム作成
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.raw_data")

# ボリュームのサブディレクトリ作成
# dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/raw_data/customers")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/raw_data/orders")

print(f"MY_CATALOG: {MY_CATALOG}")
print(f"MY_SCHEMA: {MY_CATALOG}.{MY_SCHEMA}")
print(f"MY_VOLUME ディレクトリ")
# print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/raw_data/customers")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/raw_data/orders")
