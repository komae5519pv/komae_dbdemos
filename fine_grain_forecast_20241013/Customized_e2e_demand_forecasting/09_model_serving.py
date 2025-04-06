# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalogに登録されたモデルをサービングする
# MAGIC
# MAGIC 事前に、ノートブック`./06_AutoML`で、AutoMLでトレーニングしたベストモデルをUnity Catalogに登録しました。  
# MAGIC さらにここでは、Databricks Model Serving を使用してカスタム モデルを提供するモデル サービング エンドポイントを作成します。
# MAGIC
# MAGIC Model Serving には、提供エンドポイントの作成に関する次のオプションが用意されています。
# MAGIC - 提供 UI
# MAGIC - REST API
# MAGIC - MLflow デプロイ SDK
# MAGIC
# MAGIC ここでは、MLflow デプロイ SDK を使用します。
# MAGIC
# MAGIC [参考サイト](https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/model-serving/custom-models)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/model_serving.png?raw=true' width='1200'/>

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import mlflow
from mlflow.deployments import get_deploy_client
from mlflow.exceptions import MlflowException
from requests.exceptions import HTTPError

# Unity Catalogをレジストリとして設定
mlflow.set_registry_uri("databricks-uc")

# Databricksデプロイメントクライアントを取得
client = get_deploy_client("databricks")

# MLflowクライアントを取得
mlflow_client = mlflow.tracking.MlflowClient()

# @prodエイリアスが指すバージョンを取得
model_version = mlflow_client.get_model_version_by_alias(f"{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME_AUTOML}", "prod")

# エンドポイントの設定
endpoint_name = f"{MODEL_NAME_AUTOML}"

# エンドポイントの設定
config = {
    "served_entities": [
        {
            "entity_name": f"{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME_AUTOML}",
            "entity_version": model_version.version,
            "workload_size": "Small",
            "scale_to_zero_enabled": True
        }
    ],
    "traffic_config": {
        "routes": [
            {
                "served_model_name": f"{MODEL_NAME_AUTOML}-{model_version.version}",
                "traffic_percentage": 100
            }
        ]
    }
}

try:
    existing_endpoint = client.get_endpoint(endpoint_name)
    print(f"既存のエンドポイント '{endpoint_name}' が見つかりました。更新は行いません。")
except HTTPError as e:
    if e.response.status_code == 404:
        endpoint = client.create_endpoint(name=endpoint_name, config=config)
        print(f"エンドポイント '{endpoint_name}' を新規作成しました。")
    else:
        raise
except MlflowException as e:
    print(f"エラーが発生しました: {e}")
    raise

print(f"エンドポイント '{endpoint_name}' の処理が完了しました。")

# COMMAND ----------

# MAGIC %md
# MAGIC ### サービングしたモデルをSQL `ai_query()`でクエリしてみましょう！
# MAGIC クエリエディタで下記クエリを実行してみてください

# COMMAND ----------

print(f'''
SELECT 
  ds,
  vm,
  item,
  y,
  ai_query(
    '{MODEL_NAME_AUTOML}',
    named_struct(
      "ds", ds,
      "vm", vm,
      "item", item
    )
  ).yhat AS prediction
FROM {MY_CATALOG}.{MY_SCHEMA}.silver_inference_input
''')

# COMMAND ----------

# MAGIC %md
# MAGIC サービングエンドポイントがREADYになったら、下記SQLを、上記セルの出力結果の文字列に書き換えて実行してください

# COMMAND ----------

# %sql

# SELECT 
#   ds,
#   vm,
#   item,
#   y,
#   ai_query(
#     'komae_demand_forecast_automl',
#     named_struct(
#       "ds", ds,
#       "vm", vm,
#       "item", item
#     )
#   ).yhat AS prediction
# FROM komae_demo_v2.demand_forecast.silver_inference_input
