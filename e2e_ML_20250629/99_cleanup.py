# Databricks notebook source
# MAGIC %md # Clean up script
# MAGIC
# MAGIC ハンズオンの最後にこちらのノートブックを実行して、環境をクリーンアップしてください。

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Feature Tableは現在UIから削除する必要があります。<br>
# MAGIC <img src='https://docs.databricks.com/_images/feature-store-deletion.png' width='800' />

# COMMAND ----------

# Drop Database & Table
#spark.sql(f'drop database {dbName} cascade')

# Delete Delta Path
#dbutils.fs.rm(bronze_path, True)
#dbutils.fs.rm(result_path, True)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Model Serving を有効にしている場合は Stopしてください。
# MAGIC
# MAGIC <img src='https://docs.databricks.com/_images/serving-tab.png' width='800' />

# COMMAND ----------

# MAGIC %md
# MAGIC ### UCに登録されたモデルの削除

# COMMAND ----------

# DBTITLE 1,エイリアスを削除
from mlflow import MlflowClient

client = MlflowClient()

# エイリアス自体を削除する場合
client.delete_registered_model_alias(
  name=f"{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}", 
  alias="prod"
)

# COMMAND ----------

# DBTITLE 1,特定バージョンのモデルを削除
from mlflow import MlflowClient

client = MlflowClient()

# エイリアスからモデルバージョンを取得
model_version = client.get_model_version_by_alias(
  name=f"{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}", 
  alias="prod"  # 削除したいエイリアス名を指定
)

# 特定バージョンのモデルを削除
client.delete_model_version(
    name=f"{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}",
    version=model_version.version
)

# COMMAND ----------

# MAGIC %md ## その他
# MAGIC
# MAGIC ダッシュボードやアラートは個別で削除してください。<br>
# MAGIC 管理者は ml_handsonというカタログを削除すると一括して全体のデータを削除できます。<br>
# MAGIC ```
# MAGIC DROP CATALOG ml_handson CASCADE 
# MAGIC ```
