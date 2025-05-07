# Databricks notebook source
# カタログ、スキーマ、ボリューム名
MY_CATALOG = "komae_demo_v3"        # 使用したいカタログ名に変更してください
MY_SCHEMA = "airline_recommends"
MY_VOLUME = "raw_data"
MY_VOLUME_QR = "qr_code_img"
MY_VOLUME_CONTNETS = "contents_img"
MY_VOLUME_MLFLOW = "mlflow_artifacts"

# モデル名
MODEL_NAME = "als_recommender_model"                        # ALSモデル
MODEL_NAME_GET_RECOMMENDS = "get-airline-recommendations"   # Feature Serving Endpoint

# SQLウェアハウスID
WAREHOUSE_ID = "148ccb90800933a1"   # 使用したいSQLウェアハウスのIDに変更してください

# ワークフロー名
WORKFLOW_NAME = "komae_airline_recommends_wf"   # 使用したいワークフロー名に変更してください

# COMMAND ----------

# DBTITLE 1,スキーマ配下完全削除
# spark.sql(f"DROP SCHEMA IF EXISTS {MY_CATALOG}.{MY_SCHEMA} CASCADE")

# COMMAND ----------

# DBTITLE 1,カタログ設定
# カタログ、スキーマ、ボリューム作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_QR};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_CONTNETS};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_MLFLOW};")

# USE設定
spark.sql(f"USE CATALOG {MY_CATALOG};")
spark.sql(f"USE SCHEMA {MY_SCHEMA};")

# ボリュームのサブディレクトリ作成
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/flight_booking")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/pre_survey")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/ife_play_logs")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/ife_contents")

print(f"MY_CATALOG: {MY_CATALOG}")
print(f"MY_SCHEMA: {MY_CATALOG}.{MY_SCHEMA}")
print(f"MY_VOLUME")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/flight_booking")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/pre_survey")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/ife_play_logs")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/ife_contents")
print(f"MY_VOLUME_QR")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_QR}")
print(f"MY_VOLUME_CONTNETS")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_CONTNETS}")
print(f"MY_VOLUME_MLFLOW")
print(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_MLFLOW}")
print(f"MODEL_NAME: {MODEL_NAME}")
print(f"MODEL_NAME_GET_RECOMMENDS: {MODEL_NAME_GET_RECOMMENDS}")
print(f"WAREHOUSE_ID: {WAREHOUSE_ID}")
print(f"WORKFLOW_NAME: {WORKFLOW_NAME}")
