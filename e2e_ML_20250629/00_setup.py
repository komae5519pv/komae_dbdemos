# Databricks notebook source
MY_CATALOG = "komae_demo_v1"      # 任意のカタログ名に変更してください
MY_SCHEMA = "ml_handson"
MY_VOLUME = "raw_data"
MODEL_NAME = "churn_model"
SERVING_ENDPOINT = "komae_churn_model"

# COMMAND ----------

# DBTITLE 1,Unity Catalog設定(管理者はあらかじめ作成し、権限を付与しておいてください）
# カタログ、スキーマ、ボリューム作成
spark.sql(f'CREATE CATALOG IF NOT EXISTS {MY_CATALOG}')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}')
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME}")

# アクセス許可
spark.sql(f'GRANT CREATE, USAGE ON CATALOG {MY_CATALOG} TO `account users`')

spark.sql(f'USE CATALOG {MY_CATALOG}')

# ボリュームのサブディレクトリ作成
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/customer")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/ml_sample")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/data")

print(f'MY_CATALOG: {MY_CATALOG}')
print(f'MY_SCHEMA: {MY_SCHEMA}')
print(f'MY_VOLUME: {MY_VOLUME}')
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/customer")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/ml_sample")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/data")
print(f'MODEL_NAME: {MODEL_NAME}')
print(f'SERVING_ENDPOINT: {SERVING_ENDPOINT}')
