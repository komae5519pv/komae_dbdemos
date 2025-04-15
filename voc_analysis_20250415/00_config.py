# Databricks notebook source
# カタログ、スキーマ、ボリューム
MY_CATALOG = "komae_demo_v3"             # 使用したいカタログ名に変更してください
MY_SCHEMA = "airline_reviews"
MY_VOLUME = "raw_data"
MY_VOLUME_IMAGE = "images"

# ボリュームディレクトリ
VOLUME_EN = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/EN"
VOLUME_JA = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/JP"

# COMMAND ----------

# カタログ、スキーマ、ボリューム作成
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_IMAGE}")

# ボリュームのサブディレクトリ作成
dbutils.fs.mkdirs(VOLUME_EN)
dbutils.fs.mkdirs(VOLUME_JA)

spark.sql(f"USE CATALOG {MY_CATALOG}")
spark.sql(f"USE SCHEMA {MY_SCHEMA}")

print(f"MY_CATALOG: {MY_CATALOG}")
print(f"MY_SCHEMA: {MY_SCHEMA}")
print(f"MY_VOLUME: {MY_VOLUME}")
print(f"MY_VOLUME_IMAGE: {MY_VOLUME_IMAGE}")
print(f"ボリュームパス")
print(VOLUME_EN)
print(VOLUME_JA)
