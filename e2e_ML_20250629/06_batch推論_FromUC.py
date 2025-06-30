# Databricks notebook source
# MAGIC %md # 06 Bacth/Streaming による推論
# MAGIC
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/6_batch_infer.png' width='800' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/6_batch_infer.png?raw=true' width='1200' />

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC
# MAGIC <!-- <img style="float: right" src='https://sajpstorage.blob.core.windows.net/demo-asset-workshop2021/20210222_deployment_pattern.png' width='800' /> -->
# MAGIC <img style="float: right" src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/20210222_deployment_pattern.png?raw=true' width='1200' />
# MAGIC
# MAGIC Databricks上でのモデルのデプロイは以下の3通りに分けられます。
# MAGIC
# MAGIC 1. バッチ推論: Databricks上のnotebookでDataframeを入力し、スコアリングするコードを定期実行する
# MAGIC 1. ストリーミング推論: Databricks上のnotebookでストリーミングDataframeを入力し、スコアリングを逐次実行する
# MAGIC 1. Model Serving: REST Server上にモデルをデプロイし、HTTPリクエストでスコアリングデータを読み込み、レスポンスで推定結果を返す
# MAGIC
# MAGIC Databricks上ではバッチ処理、ストリーミング処理がDataframe的に同等に扱えるため、上記のバッチ処理、ストリーミング処理はほぼ同じデプロイ方法になります。
# MAGIC Rest Servingについては、MLflowのレジストリUIからデプロイ可能です。

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md ## モデルのロード
# MAGIC
# MAGIC mlflow には`pyfunc`というライブラリがあり、モデルをロードしてUDF化してくれる関数も用意してあります。

# COMMAND ----------

import mlflow
import pyspark.sql.functions as F

mlflow.set_registry_uri("databricks-uc")

# UCモデルレジストリからモデルロード
predict_udf = mlflow.pyfunc.spark_udf(spark, f"models:/{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}@prod")

# COMMAND ----------

# MAGIC %md ## Spark DataFrameによる推論
# MAGIC
# MAGIC Spark DataFrameの場合、バッチとストリーミングを両方とも扱える点と、SparkAPIを使った分散処理が出来るため大量のデータを非常に高速に処理実行することができます。<br>
# MAGIC ただしPandasにも対応しているためどちらも利用することが可能です。

# COMMAND ----------

from pyspark.sql import functions as F

# 推定を実施する(スコアリングを実施する)対象のデータを読み込む
df = spark.read.table('churn_features')

# 推定実行
pred_df = df.withColumn('prediction', predict_udf(F.struct(*df.columns)))

print(pred_df.count())
print(pred_df.columns)
display(pred_df)

# COMMAND ----------

# MAGIC %md ## Delta Lakeへ保存
# MAGIC
# MAGIC 最後に元のデータ（Bronze Data)に予測結果を追加してDeltaLakeに保存しておきます。この後Databricks SQLによる可視化等で利用します。

# COMMAND ----------

#01のノートブックでソースデータをDeltaLakeに保存したパスを指定します。

#ソースデータを読み込みます。
bronze_df = spark.read.table('bronze_table')

#予測結果をjoinします。
final_df = bronze_df.join(pred_df.select('customerID','prediction','churn'), bronze_df.customerID == pred_df.customerID, "left").drop(pred_df.customerID)

# DeltaTable ('churn_prediction')として保存
final_df.write.mode('overwrite').saveAsTable('churn_prediction')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from churn_prediction
