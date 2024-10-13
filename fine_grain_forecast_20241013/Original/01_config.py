# Databricks notebook source
# カタログ
MY_CATALOG = "komae_demo" # 使用したいカタログ名に変更してください

# スキーマ
MY_SCHEMA = "fine_grain_forecast"

# ボリューム
MY_VOLUME_IMPORT = "raw_data"
MY_VOLUME_EXPORT = "export_data"

# モデル名
MODEL_NAME = "fine_grain_forecast"

# COMMAND ----------

# カタログ、スキーマ、ボリューム作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_IMPORT};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_EXPORT};")

# USE設定
spark.sql(f"USE CATALOG {MY_CATALOG};")
spark.sql(f"USE SCHEMA {MY_SCHEMA};")

# ボリュームのサブディレクトリ作成
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/train")

# SQLで使う変数設定
spark.conf.set("c.catalog", MY_CATALOG)
spark.conf.set("c.schema", MY_SCHEMA)
# dbutils.widgets.removeAll()


print(f"カタログ: {MY_CATALOG}")
print(f"スキーマ: {MY_CATALOG}.{MY_SCHEMA}")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/train")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_EXPORT}")
print(f"モデル名: {MODEL_NAME}")
