# Databricks notebook source
# MAGIC %md
# MAGIC # DeepSeek R1 on Databricks 試してみる
# MAGIC [DeepSeek R1 on Databricks](https://www.databricks.com/blog/deepseek-r1-databricks)

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. カタログ・スキーマ作成
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. モデルとトーカナイザーをロード
# MAGIC Llama 8Bの場合、32GB分のモデルの重みをダウンロードする必要があるため、このプロセスには数分かかります。

# COMMAND ----------

from transformers import AutoTokenizer, AutoModelForCausalLM

tokenizer = AutoTokenizer.from_pretrained("deepseek-ai/DeepSeek-R1-Distill-Llama-8B")
model = AutoModelForCausalLM.from_pretrained("deepseek-ai/DeepSeek-R1-Distill-Llama-8B")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. モデルとトークナイザーをtransformersモデルとして登録
# MAGIC mlflow.transformersを使用すると、Unity Catalogにモデルを登録するのが簡単になります。モデルサイズ（この場合は8B）とモデル名を設定するだけです。

# COMMAND ----------

import mlflow

transformers_model = {"model": model, "tokenizer": tokenizer}
task = "llm/v1/chat"

# UCモデル名
UC_MODEL_NAME = 'deepseek_r1_distilled_llama_8b'

# ロードしたモデルをUnity Catalogに登録
with mlflow.start_run():
   model_info = mlflow.transformers.log_model(
       transformers_model=transformers_model,
       artifact_path="model",
       task=task,
       registered_model_name=f"{MY_CATALOG}.{MY_SCHEMA}.{UC_MODEL_NAME}"
,
       metadata={
        "pretrained_model_name": "meta-llama/Llama-3.1-8B-Instruct",
           "databricks_model_family": "LlamaForCausalLM",
           "databricks_model_size_parameters": "8b",
       },
   )

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Model Servingにデプロイ
# MAGIC 画面UIで「Serving」に移動し、UnityCatalogに登録したモデルをModel Servingエンドポイントにデプロイします。
# MAGIC
# MAGIC **ServingEndPoint**
# MAGIC [komae_deepseek_r1_distilled_llama_8b](https://adb-984752964297111.11.azuredatabricks.net/ml/endpoints/komae_deepseek_r1_distilled_llama_8b?o=984752964297111)
# MAGIC
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/deep_seek/model_serving.png' width='1200'/>

# COMMAND ----------

# MAGIC %md
# MAGIC もちろん、プログラムからもUnityCatalogに登録したモデルをModel Servingエンドポイントにデプロイできます。

# COMMAND ----------

import mlflow.deployments

# Databricks用のMLflowデプロイクライアントを取得
client = mlflow.deployments.get_deploy_client("databricks")

# モデルサービングエンドポイント名
ENDPOINT_NAME = "komae_deepseek_r1_distilled_llama_8b"

# エンドポイント作成とUCモデルのデプロイ
client.create_endpoint(
    name=ENDPOINT_NAME,
    config={
        "served_models": [
            {
                "model_name": f"{MY_CATALOG}.{MY_SCHEMA}.{UC_MODEL_NAME}",    # UCに登録されたモデル名
                "model_version": "1",                                         # デプロイするバージョン（例: v1）
                "workload_size": "Small",                                     # ワークロードサイズ（Small/Medium/Large）
                "scale_to_zero_enabled": True                                 # スケールダウンオプション
            }
        ]
    }
)
