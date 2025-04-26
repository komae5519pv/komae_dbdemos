# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalogに登録したProphetモデルをロードして、将来日付を含むデータセットに適用して推論します
# MAGIC
# MAGIC **要件**
# MAGIC クラスタ Runtime 15.4 ML LTS以上を使用してください。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/batch_scoring.png?raw=true' width='1200'/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. MLflowを用いてモデルをロード
# MAGIC
# MAGIC Unity Catalog Model Registry に登録されたモデルをロードする際、2つのアプローチがあります。
# MAGIC - **アプローチ1: mlflow.pyfunc.load_model()**
# MAGIC   - 単一のマシンでの推論: 単一のマシンやノートブック環境で推論する場合
# MAGIC   - 小規模なデータセット: 処理するデータ量が比較的少ない場合
# MAGIC   - 柔軟な前処理/後処理: モデルの入力や出力に対して柔軟な操作が必要な場合
# MAGIC
# MAGIC - **アプローチ2: mlflow.pyfunc.spark_udf()**
# MAGIC   - 大規模データの処理: Sparkを使用した大量データの分散処理
# MAGIC   - バッチ処理: 大規模なバッチ処理ジョブ
# MAGIC   - リアルタイムストリーミング: Spark Streamingを使用したリアルタイム推論
# MAGIC
# MAGIC ここでは、大規模データ処理を見越してアプローチ2を採用します。
# MAGIC
# MAGIC
# MAGIC [参考サイト](https://learn.microsoft.com/ja-jp/azure/databricks/mlflow/models)
# MAGIC

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Unity Catalogに登録したモデルをロードします

# COMMAND ----------

# DBTITLE 1,Load Model
import mlflow
from pyspark.sql.functions import struct, col

# Get Model Path
model_path = f"models:/{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME_AUTOML}@prod"
print(model_path)

# Load Model as Apache Spark UDF
model_udf = mlflow.pyfunc.spark_udf(spark, model_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. ロードしたモデルで推論を実行します

# COMMAND ----------

# DBTITLE 1,Inference
# Load Dummy Dataset for Inference / already filtered by vm=1, item=1
input_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.silver_inference_input")

# Sort by Date
input_df = input_df.orderBy("ds")

# Execute Inference with Loaded Model
df = (input_df
      .withColumn("prediction", model_udf(struct(*input_df.columns)))
      .withColumn("forecast_sales_quantity", col("prediction"))
      .drop("prediction")
     )
display(df)
