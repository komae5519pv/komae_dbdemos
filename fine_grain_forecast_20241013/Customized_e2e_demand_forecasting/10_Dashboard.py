# Databricks notebook source
# MAGIC %md
# MAGIC # 自動販売機の需要予測・分析ダッシュボードを作成
# MAGIC ## やること
# MAGIC - 分析用マートを作成してダッシュボードに活用します、本ノートブックを上から下まで流してください
# MAGIC - クラスタはDBR15.4 LTS or DBR15.4 LTS ML以降で実行してください
# MAGIC - [テーブル定義書](https://docs.google.com/spreadsheets/d/10wdoTxlAGcD5gHjY4_upPYKd1gt4rEupKLSgP5q4uWI/edit?gid=1392031218#gid=1392031218)に基づくテーブルを作ります
# MAGIC
# MAGIC 想定のディレクトリ構成
# MAGIC
# MAGIC ```
# MAGIC /<catalog_name>
# MAGIC ├── demand_forecast                           <- スキーマ
# MAGIC │   ├── bronze_xxx                            <- ローデータテーブル
# MAGIC │       ├── bronze_sales                      <- 自動販売機売上
# MAGIC │       ├── bronze_vending_machine_location   <- 自販機設定場所マスタ
# MAGIC │       ├── bronze_date_master                <- 日付マスタ
# MAGIC │       ├── bronze_items                      <- 商品マスタ
# MAGIC │       ├── bronze_train                      <- トレーニングデータ
# MAGIC │   ├── silver_xxx                            <- bronze_xxxをクレンジングしたテーブル
# MAGIC │       ├── silver_analysis                   <- 分析マート
# MAGIC │       ├── silver_demand_forecasting         <- 需要予測結果データ
# MAGIC │   ├── gold_xxx                              <- silver_xxxを使いやすく加工したテーブル
# MAGIC │       ├── gold_analysis_with_prediction     <- 需要予測結果付き分析マート
# MAGIC │   ├── raw_data                              <- ボリューム(Import用)
# MAGIC │       ├── sales.csv                         <- RAWファイルを配置：自動販売機売上
# MAGIC │       ├── vending_machine_location.csv      <- RAWファイルを配置：自販機設定場所マスタ
# MAGIC │       ├── date_master.csv                   <- RAWファイルを配置：日付マスタ
# MAGIC │       ├── items.csv                         <- RAWファイルを配置：商品マスタ
# MAGIC │   ├── export_data                           <- ボリューム(Export用)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備

# COMMAND ----------

# MAGIC %run ./01_config
