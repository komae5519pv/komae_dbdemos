# Databricks notebook source
# カタログ
MY_CATALOG = "komae_demo_v2" # 使用したいカタログ名に変更してください

# スキーマ
MY_SCHEMA = "demand_forecast"

# ボリューム
MY_VOLUME_IMPORT = "raw_data"
# MY_VOLUME_EXPORT = "export_data"

# モデル名
MODEL_NAME = "demand_forecast"
MODEL_NAME_AUTOML = "demand_forecast_automl"

# COMMAND ----------

# カタログ、スキーマ、ボリューム作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_IMPORT};")
# spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_EXPORT};")

# USE設定
spark.sql(f"USE CATALOG {MY_CATALOG};")
spark.sql(f"USE SCHEMA {MY_SCHEMA};")

# ボリュームのサブディレクトリ作成
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/sales")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/items")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/vending_machine_location")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/date_master")

# SQLで使う変数設定
spark.conf.set("c.catalog", MY_CATALOG)
spark.conf.set("c.schema", MY_SCHEMA)
spark.conf.set("c.volume", MY_VOLUME_IMPORT)

print(f"カタログ: {MY_CATALOG}")
print(f"スキーマ: {MY_CATALOG}.{MY_SCHEMA}")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/sales")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/items")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/vending_machine_location")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/date_master")
# print(f"モデル名: {MODEL_NAME}")
print(f"モデル名（AutoML）: {MODEL_NAME_AUTOML}")
