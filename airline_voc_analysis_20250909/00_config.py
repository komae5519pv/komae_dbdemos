# Databricks notebook source
# カタログ、スキーマ、ボリューム
MY_CATALOG = "komae_demo_v3"             # 使用したいカタログ名に変更してください
MY_SCHEMA = "airline_voc"
MY_VOLUME = "raw_data"
MY_VOLUME_TMP = "by_topics"
MY_VOLUME_IMG = "img"

# ワークフロー名
WORKFLOW_NAME = "komae_airline_voc_wf"   # 使用したいワークフロー名に変更してください

# ワークスペースのホストURL
# 補足
# ・Azureの場合は、`HOST_URL`https://adb-<ワークスペースID含む文字列>.azuredatabricks.net`の形式で記載
# ・AWSの場合は、`HOST_URL`https://<任意のワークスペース名>.cloud.databricks.com`の形式で記載
HOST_URL = "https://adb-984752964297111.11.azuredatabricks.net" # 本ワークスペースのURL

# COMMAND ----------

# カタログ、スキーマ、ボリューム作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_TMP}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_IMG}")

spark.sql(f"USE CATALOG {MY_CATALOG}")
spark.sql(f"USE SCHEMA {MY_SCHEMA}")

print(f"MY_CATALOG: {MY_CATALOG}")
print(f"MY_SCHEMA: {MY_SCHEMA}")
print(f"MY_VOLUME: {MY_VOLUME}")
print(f"MY_VOLUME_TMP: {MY_VOLUME_TMP}")
print(f"MY_VOLUME_IMG: {MY_VOLUME_IMG}")
print(f"WORKFLOW_NAME: {WORKFLOW_NAME}")
print(f"HOST_URL: {HOST_URL}")
