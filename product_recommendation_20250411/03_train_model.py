# Databricks notebook source
# MAGIC %md
# MAGIC # レコメンドモデルの構築・UC登録
# MAGIC - ここではレコメンドモデルを構築し、Unity Catalogに登録します
# MAGIC - DBR 16.0ML以降

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. レコメンデーション・モデルの構築
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-3.png" width="1000px">
# MAGIC
# MAGIC <img src= https://databricks.com/wp-content/uploads/2020/04/databricks-adds-access-control-to-mlflow-model-registry_01.jpg width='500px' style='float: right'>
# MAGIC
# MAGIC ### MLFlowを用いたALSレコメンダーの構築
# MAGIC  
# MAGIC Mlflowは、モデル自身を含むすべての実験メトリクスの追跡に使用されています。  
# MAGIC さらにMLFlowを使って、Unity Catalogモデルレジストリにモデルをデプロイして、prodエイリアスを付与します。  
# MAGIC デプロイができたら、このモデルを再利用して、最終的なレコメンデーションを取得し、パーソナルなオファーをプッシュ通知で送ることができるようになります。

# COMMAND ----------

import mlflow
from pyspark.ml.recommendation import ALS
from mlflow.models import infer_signature

with mlflow.start_run() as run:
  #MLFlowで実行のパラメータなどを自動的にログに記録します。
  mlflow.pyspark.ml.autolog()
  
  df = spark.sql(f'''
    SELECT customer_id, item_id, COUNT(item_id) AS item_purchases
    FROM {MY_CATALOG}.{MY_SCHEMA}.sv_sales
    GROUP BY customer_id, item_id
  ''')

  # ALSを使用してトレーニングデータからレコメンデーションモデルを構築する。
  # コールドスタート戦略を'drop'に設定して、NaN評価メトリクスにならないようにすることに注意してください。
  # 評価マトリックスは別の情報源から導出されます (つまり、他の情報から推測されます)。より良い結果を得るために、implicitPrefs を true に設定します。
  als = ALS(
    rank=3,
    userCol="customer_id",
    itemCol="item_id",
    ratingCol="item_purchases",
    implicitPrefs=True,
    seed=0,
    # coldStartStrategy="nan"
    coldStartStrategy="drop"  # NaN評価メトリクスを避ける
  )
  
  num_cores = sc.defaultParallelism
  als.setNumBlocks(num_cores)
  
  # モデルのトレーニング
  model = als.fit(df)

  # モデルをログ
  mlflow.spark.log_model(model, "spark-model")

  # サンプルデータでシグネチャーを推測
  sample_input = df.select("customer_id", "item_id").limit(10)
  predictions = model.transform(sample_input)

  # シグネチャーを取得するために、Pandasに変換する場合はデータ量を制限
  sample_input_pd = sample_input.toPandas()
  predictions_pd = predictions.select("prediction").toPandas()

  signature = infer_signature(sample_input_pd, predictions_pd)

  # 別のセルからこの実行の他の数値を追加する必要があるため、実行IDを取得しておく
  run_id = run.info.run_id

  # モデルをログします
  mlflow.spark.log_model(
    model,
    f"spark-model-{run_id}",   # 実行IDを使って一意のパスを生成
    signature=signature,
    input_example=sample_input.limit(10).toPandas()
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. モデルをUnity Catalogに登録します

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

# UCにモデルを登録します
mlflow.set_registry_uri("databricks-uc")
registered_model = mlflow.register_model(
    f"runs:/{run_id}/spark-model-{run_id}",
    f"{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}"
)

# UC登録したモデルにエイリアスを設定します
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
