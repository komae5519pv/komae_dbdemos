# Databricks notebook source
# MAGIC %md
# MAGIC # レコメンドモデルの構築・推論
# MAGIC - ここではレコメンドモデルを構築し、Unity Catalogモデルレジストリに登録します。
# MAGIC - DBR 16.0ML以降

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. レコメンデーション・モデルの構築
# MAGIC
# MAGIC Mlflowは、モデル自身を含むすべての実験メトリクスの追跡に使用されています。  
# MAGIC さらにMLFlowを使って、Unity Catalogモデルレジストリにモデルをデプロイして、prodエイリアスを付与します。  
# MAGIC デプロイができたら、このモデルを再利用して、最終的なレコメンデーションを取得し、パーソナルなオファーをプッシュ通知で送ることができるようになります。

# COMMAND ----------

# DBTITLE 1,Experimentをセット
import mlflow
from datetime import datetime

# ノートブックのパス取得
full_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
parent_dir = "/".join(full_path.split("/")[:-1])

# _experimentsディレクトリをセット
experiments_dir = f"{parent_dir}/_experiments"

# タイムスタンプ付きエクスペリメント名
now = datetime.now()
timestamp_str = now.strftime("%Y%m%d_%H%M%S")
experiment_name = f"{timestamp_str}_experiment"

# 完成形のExperimentパス
experiment_path = f"{experiments_dir}/{experiment_name}"

# Artifact保存先（ボリュームを使う）
artifact_location = f"dbfs:/Volumes/{MY_CATALOG}/{MY_SCHEMA}/mlflow_artifacts/{timestamp_str}"

# Experiment作成
mlflow.set_registry_uri("databricks-uc")

if mlflow.get_experiment_by_name(experiment_path) is None:
    mlflow.create_experiment(
        name=experiment_path,
        artifact_location=artifact_location
    )

mlflow.set_experiment(experiment_path)
print(f"MLflow Experimentをセットしました: {experiment_path}")

# COMMAND ----------

import mlflow
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.recommendation import ALS

# --- 0) MLflow Autolog を無効化（競合防止） ----------------------
mlflow.pyspark.ml.autolog(disable=True)

# --- 1) 学習データを集計 ---------------------------------------
# user_id は 既に 数値型！
# ratingColには play_sec/duration_sec の合算を使用
train_df = (
    spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.sv_ife_play_logs")
         .withColumn("ratio", F.least(F.col("play_sec") / F.col("duration_sec"), F.lit(1.0)))
         .groupBy("user_id", "content_id")
         .agg(F.sum("ratio").alias("rating"))
         .filter("rating > 0")
)

# --- 2) ALSモデル ---------------------------------------------
als = ALS(
    rank=8,
    maxIter=10,
    regParam=0.1,
    userCol="user_id",
    itemCol="content_id",
    ratingCol="rating",
    implicitPrefs=True,
    coldStartStrategy="drop",
    seed=42
)

# --- 3) パイプライン構築 ---------------------------------------
pipeline = Pipeline(stages=[als])

# --- 4) モデル学習 & MLflow ログ -------------------------------
with mlflow.start_run() as run:
    model = pipeline.fit(train_df)

    # 推論用サンプル抽出
    sample_input = train_df.select("user_id", "content_id").limit(1000)
    predictions  = model.transform(sample_input)

    input_pd  = sample_input.toPandas()
    output_pd = predictions.select("prediction").toPandas()

    mlflow.spark.log_model(
        model,
        "als_ife_model",
        signature=mlflow.models.infer_signature(input_pd, output_pd),
        input_example=input_pd.head(10)
    )

    run_id = run.info.run_id
    print(f"モデルをMLflowにログしました。run_id = {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. モデルをUnity Catalogに登録します

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

# run_id = run.info.run_id

# MLflow Registry URI をUCにセット
mlflow.set_registry_uri("databricks-uc")

# UCにモデルを登録します
registered_model = mlflow.register_model(
    model_uri=f"runs:/{run_id}/als_ife_model",
    name=f"{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}"
)

# UC登録したモデルにエイリアスを付与
client = MlflowClient()
client.set_registered_model_alias(
    name=registered_model.name,
    alias="prod",
    version=registered_model.version
)

# 全ユーザー(グループ名: account users)にモデルの権限を設定します
sdk_client = WorkspaceClient()
sdk_client.grants.update(c.SecurableType.FUNCTION, registered_model.name, 
                         changes=[c.PermissionsChange(add=[c.Privilege["ALL_PRIVILEGES"]], principal="account users")])
