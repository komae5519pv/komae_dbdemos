# Databricks notebook source
# MAGIC %md 
# MAGIC **要件**  DBR 15.4 ML 以降をお使いください

# COMMAND ----------

# MAGIC %md # Machine Learning End to End Demo 概要
# MAGIC
# MAGIC 実際にRawデータから加工してモデル学習＆デプロイまで構築するデモになります。以下のようなパイプラインを想定しております。
# MAGIC
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/overall.png' width='1200'/> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/overall.png?raw=true' width='1200'/>

# COMMAND ----------

# MAGIC %md # 01. Create Delta Lake
# MAGIC Azure Blob Storage上のcsvデータを読み込み、必要なETL処理を実施した上でデルタレイクに保存するまでのノートブックになります。
# MAGIC
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/1_createDelta.png' width='800' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/1_createDelta.png?raw=true' width='800' />

# COMMAND ----------

# DBTITLE 1,Setup Script for hands-on
# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md ## Data Load
# MAGIC
# MAGIC DataLake上のデータをロードして、データプロファイルをチェックしてみよう

# COMMAND ----------

# MAGIC %run ./_helper/load_data

# COMMAND ----------

# MAGIC %md ### Spark Dataframe による取り込みと加工処理
# MAGIC
# MAGIC [Databricks(Delta lake)のデータ入出力の実装パターン - cheatsheet](https://qiita.com/ktmrmshk/items/54ce2d6f274a67b2e54c)

# COMMAND ----------

# Volumeから学習データを取得します
sourcePath=f'/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/customer/customer.csv'

df_customer_csv = spark.read.format('csv').option("header","true").option("inferSchema", "true").load(sourcePath)
display(df_customer_csv)

# COMMAND ----------

# MAGIC %md ## Delta Lakeに保存
# MAGIC
# MAGIC データサイズが大きくなると処理速度に影響が出るため一度Raw Dataを Delta Lakeに保存することでパフォーマンスを向上させることができます。性能に課題がある場合は、このやり方をお勧めします。

# COMMAND ----------

# save to delta lake
df_customer_csv.write.mode('overwrite').saveAsTable('bronze_table')

# load data as delta
df_bronze = spark.read.table('bronze_table')

# COMMAND ----------

# MAGIC %md ## PySpark Pandas API を使って前処理を実施
# MAGIC
# MAGIC 多くの Data Scientist は、pandasの扱いになれており、Spark Dataframeには不慣れです。Spark 3.2より Pandas APIを一部サポートしました。<br>
# MAGIC これにより、Pandasの関数を使いながら、Sparkの分散機能も使うことが可能になります。 <br>
# MAGIC **Requirement** Spark3.2以降の機能なため、**DBR 10.2以上**のクラスター上で実行する必要があります

# COMMAND ----------

import pyspark.pandas as ps

# Spark DataFrame から Pandas-on-Spark DataFrame に変換
data = df_bronze.to_pandas_on_spark()

# "null" という文字列を None（=NaN）に変換し、float に変換
data['totalCharges'] = data['totalCharges'].replace("null", None)
data['totalCharges'] = ps.to_numeric(data['totalCharges'], errors='coerce')

# ワンホットエンコーディング
data = ps.get_dummies(data, 
                        columns=['gender', 'partner', 'dependents',
                                 'phoneService', 'multipleLines', 'internetService',
                                 'onlineSecurity', 'onlineBackup', 'deviceProtection',
                                 'techSupport', 'streamingTV', 'streamingMovies',
                                 'contract', 'paperlessBilling', 'paymentMethod'],dtype = 'int64')

# ラベルをintに変換し、列名を変更
data['churnString'] = data['churnString'].map({'Yes': 1, 'No': 0})
data = data.astype({'churnString': 'int32'})
# data = data.astype({'churnString': 'int16'})
data = data.rename(columns = {'churnString': 'churn'})
  
# 列名をクリーンアップ
data.columns = data.columns.str.replace(' ', '')
data.columns = data.columns.str.replace('(', '-')
data.columns = data.columns.str.replace(')', '')
  
# 欠損値を削除
data = data.dropna()
  
# data.head(10)
print(data.count())
print(data.columns)
display(data.head(10))

# COMMAND ----------

# MAGIC %md ## Delta Tableとして保存
# MAGIC 現在、UnityCatalogでは Feature Storeをサポートしていないため、通常のDeltaTableを作成します。

# COMMAND ----------

# Pandas-on-Spark DataFrame から Spark DataFrameに変換してからテーブル保存
data.to_spark().write.mode('overwrite').saveAsTable('churn_features')
