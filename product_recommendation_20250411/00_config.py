# Databricks notebook source
# カタログ、スキーマ、ボリューム名
MY_CATALOG = "komae_demo_v3"        # 使用したいカタログ名に変更してください
MY_SCHEMA = "product_recommendation_stadium"
MY_VOLUME = "data"
MY_VOLUME_TMP = "tmp"

# モデル名
MODEL_NAME = "als_recommender_model"

# ワークフロー名
WORKFLOW_NAME = "komae_item_recommend_wf_2"

# COMMAND ----------

# DBTITLE 1,カタログ設定
# カタログ、スキーマ、ボリューム作成
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_TMP};")

# USE設定
spark.sql(f"USE CATALOG {MY_CATALOG};")
spark.sql(f"USE SCHEMA {MY_SCHEMA};")

# ボリュームのサブディレクトリ作成
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/vendors")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/games")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/purchase_history")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_TMP}/gd_final_recommendations")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_TMP}/gd_recommendations")

print(f"MY_CATALOG: {MY_CATALOG}")
print(f"MY_SCHEMA: {MY_CATALOG}.{MY_SCHEMA}")
print(f"MY_VOLUME")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/vendors")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/games")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/purchase_history")
print(f"MY_VOLUME_TMP")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_TMP}/gd_final_recommendations")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_TMP}/gd_recommendations")
print(f"MODEL_NAME: {MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}")
print(f"WORKFLOW_NAME: {WORKFLOW_NAME}")
