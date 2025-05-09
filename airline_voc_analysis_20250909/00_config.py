# Databricks notebook source
# カタログ、スキーマ、ボリューム
MY_CATALOG = "komae_demo_v3"             # 使用したいカタログ名に変更してください
MY_SCHEMA = "airline_voc"
MY_VOLUME = "raw_data"

# COMMAND ----------

# カタログ、スキーマ、ボリューム作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME}")

spark.sql(f"USE CATALOG {MY_CATALOG}")
spark.sql(f"USE SCHEMA {MY_SCHEMA}")

print(f"MY_CATALOG: {MY_CATALOG}")
print(f"MY_SCHEMA: {MY_SCHEMA}")
print(f"MY_VOLUME: {MY_VOLUME}")
