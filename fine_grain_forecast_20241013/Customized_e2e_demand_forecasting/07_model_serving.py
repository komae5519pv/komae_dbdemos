# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalogに登録されたモデルをサービングする
# MAGIC
# MAGIC ここでは、Databricks Model Serving を使用してカスタム モデルを提供するモデル サービング エンドポイントを作成します。
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

# MAGIC %run ./01_config

# COMMAND ----------

# エンドポイントを更新フラグ
# UPDATE_ENDPOINT_FLG = True # 更新する
UPDATE_ENDPOINT_FLG = False # 更新しない

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
endpoint_name = f"komae_{MODEL_NAME_AUTOML}"

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

# DBTITLE 1,create dummy table for ai_query
result = dbutils.notebook.run("./_helper/create_table_for_inference", timeout_seconds=60)
print(f"{MY_CATALOG}.{MY_SCHEMA}.silver_inference_input を作成しました！")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE komae_demo_v2.demand_forecast.silver_inference_input

# COMMAND ----------

# MAGIC %md
# MAGIC ### サービングしたモデルをSQL `ai_query()`でクエリしてみましょう！

# COMMAND ----------

print(f'''
クエリエディタで下記実行してみてください

SELECT 
  ds,
  vm,
  item,
  y,
  ai_query(
    'komae_demand_forecast_automl',
    named_struct(
      "ds", ds,
      "vm", vm,
      "item", item
    )
  ).yhat AS prediction
FROM komae_demo_v2.demand_forecast.silver_inference_input
''')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   ds,
# MAGIC   vm,
# MAGIC   item,
# MAGIC   y,
# MAGIC   ai_query(
# MAGIC     'komae_demand_forecast_automl',
# MAGIC     named_struct(
# MAGIC       "ds", ds,
# MAGIC       "vm", vm,
# MAGIC       "item", item
# MAGIC     )
# MAGIC   ).yhat AS prediction
# MAGIC FROM komae_demo_v2.demand_forecast.silver_inference_input
