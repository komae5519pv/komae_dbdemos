# Databricks notebook source
# MAGIC %md
# MAGIC # 外部モデルとしてGPT-4.5を利用したい場合はこちら
# MAGIC * [外部モデルとしてサービングエンドポイントにデプロイ](https://qiita.com/taka_yayoi/items/352bff5f19789f299341)
# MAGIC * DBR 16.0 MLで実行してください
# MAGIC * 前提
# MAGIC   * ここでは個別契約しているOpenAI APIを外部モデルとしてModel Serving Endpointに登録します
# MAGIC   * よって事前に下記が必要です
# MAGIC     * OpenAI APIの契約（[公式サイト](https://platform.openai.com/settings/organization/general)）
# MAGIC     * OpenAIのAPI_KEYの発行
# MAGIC     * AI_KEYの[Databricksシークレット](https://docs.databricks.com/aws/ja/security/secrets)への登録（シークレットスコープ作成&シークレット作成）

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("secret_scope_name", "")
dbutils.widgets.text("openai_api_key", "")

# COMMAND ----------

# DBTITLE 1,Deploy Model Serving
import mlflow.deployments

# APIを格納しているシークレットスコープ
SECRET_SCOPE_NAME = dbutils.widgets.get("secret_scope_name")
# APIを格納しているシークレット
SECRET_KEY_NAME = dbutils.widgets.get("openai_api_key")
# モデルサービングエンドポイント名
ENDPOINT_NAME = "komae-openai-gpt-4-5"

client = mlflow.deployments.get_deploy_client("databricks")

client.create_endpoint(
    name=ENDPOINT_NAME,
    config={
        "served_entities": [
            {
                "name": "test",
                "external_model": {
                    "name": "gpt-4.5-preview",
                    "provider": "openai",
                    "task": "llm/v1/chat",
                    "openai_config": {
                        "openai_api_key": f"{{{{secrets/{SECRET_SCOPE_NAME}/{SECRET_KEY_NAME}}}}}"
                    }
                }
            }
        ]
    }
)
