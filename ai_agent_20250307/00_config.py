# Databricks notebook source
# カタログ情報
MY_CATALOG = 'komae_demo' # ご自分のカタログ名に変更してください
MY_SCHEMA = 'retail_cdp'

# COMMAND ----------

# カタログ、スキーマ、ボリューム作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA};")

# 使うカタログ、スキーマを指定
spark.sql(f"USE CATALOG {MY_CATALOG};")
spark.sql(f"USE SCHEMA {MY_SCHEMA};")

print(f"MY_CATALOG: {MY_CATALOG}")
print(f"MY_SCHEMA: {MY_CATALOG}.{MY_SCHEMA}")
