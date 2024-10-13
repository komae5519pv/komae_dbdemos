# Databricks notebook source
# MAGIC %md
# MAGIC ### Azure Blob（公開）から各データをダウンロードします
# MAGIC これはデモ専用にサンプルデータを準備する処理です
# MAGIC
# MAGIC ローデータを該当ボリュームに配置します  
# MAGIC ```
# MAGIC /<catalog_name>
# MAGIC ├── fine_grain_forecast.         <- スキーマ
# MAGIC │   ├── raw_data                 <- ボリューム(Import用)
# MAGIC │       ├── raw_data/train       <- RAWファイルを配置
# MAGIC │   ├── export_data              <- ボリューム(Export用)
# MAGIC ```
# MAGIC
# MAGIC Kaggle Challenge で公開されているデータを使います。このデモでは、データは、Azure Blob（公開）からインポートします。:   
# MAGIC [Store Item Demand Forecasting Challenge](https://www.kaggle.com/competitions/demand-forecasting-kernels-only/data)

# COMMAND ----------

# MAGIC %run ./01_config

# COMMAND ----------

# MAGIC %md
# MAGIC #### トレーニングデータをロード
# MAGIC
# MAGIC トレーニングデータをロードしてvolumeに保存します

# COMMAND ----------

# WASBプロトコル定義
container = "komae"                                       # Azure コンテナ名
storage_account = "sajpstorage"                           # Azure ストレージアカウント名
file_path_train = "fine_grain_forecast/train"             # Azure ファイルパス（トレーニング）

# COMMAND ----------

# 公開Azure Storage Blobから学習データを取得します (WASBプロトコル)
df = spark.read.format('csv') \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(f'wasbs://{container}@{storage_account}.blob.core.windows.net/{file_path_train}/train.csv')

output_path = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/train/train.csv"
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
print(f"ファイル出力完了: {output_path}")
