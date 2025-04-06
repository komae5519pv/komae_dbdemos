# Databricks notebook source
# MAGIC %md
# MAGIC # ai_query()を介して需要予測モデルを適用する未来日付きデータを作成
# MAGIC ## やること
# MAGIC - `silver_train`をロードして、`silver_inference_input`テーブルを作ります、本ノートブックを上から下まで流してください
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
# MAGIC │   ├── silver_xxx                            <- bronze_xxxをクレンジングしたテーブル
# MAGIC │       ├── silver_sales                      <- 自動販売機売上
# MAGIC │       ├── silver_vending_machine_location   <- 自販機設定場所マスタ
# MAGIC │       ├── silver_date_master                <- 日付マスタ
# MAGIC │       ├── silver_items                      <- 商品マスタ
# MAGIC │       ├── silver_train                      <- トレーニングデータ
# MAGIC │       ├── silver_analysis                   <- 分析マート（需要予測結果なし）
# MAGIC │       ├── silver_inference_input            <- ai_query()専用の未来日付きsalesデータ   ★ココ!
# MAGIC │       ├── silver_forecasts                  <- 需要予測結果データ
# MAGIC │       ├── silver_forecast_evals             <- 需要予測評価メトリクス
# MAGIC │   ├── gold_xxx                              <- silver_xxxを使いやすく加工したテーブル
# MAGIC │       ├── gold_analysis                     <- 分析マート（需要予測結果あり）
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

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. silverテーブルを作る

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-1. silver_inference_input / 推論用インプットデータ

# COMMAND ----------

# 入力データテーブルを読み込み、vm=1, item=1に絞り込む
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW input_view AS
    SELECT 
        DATE_FORMAT(ds, 'yyyy-MM-dd') AS ds,
        vm,
        item,
        CAST(y as INT) as y
    FROM {MY_CATALOG}.{MY_SCHEMA}.silver_train
    WHERE vm = 1 AND item = 1
""")

# 最終日を取得
max_date = spark.sql("SELECT MAX(ds) as max_date FROM input_view").collect()[0]['max_date']

# 最終日から90日間の日付シーケンスを生成
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW future_dates AS
    SELECT 
        DATE_FORMAT(date_add('{max_date}', seq), 'yyyy-MM-dd') as ds,
        1 as vm,        -- vm=1固定
        1 as item       -- item=1固定
    FROM (SELECT explode(sequence(1, 90)) as seq)
""")

# 既存のデータと新しい日付範囲を結合
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.silver_inference_input AS
    SELECT ds, vm, item, y FROM input_view
    UNION ALL
    SELECT ds, vm, item, CAST(NULL AS DOUBLE) as y FROM future_dates
    ORDER BY ds
""")

inference_input_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.silver_inference_input")

display(inference_input_df)

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_inference_input'

# テーブルコメント
table_comment = """
`silver_inference_input`テーブルは、自動販売機の需要予測モデルの推論用データセットです。
このテーブルには、過去の販売データと最終日の翌日から90日間の将来データが含まれています。
将来データの販売数は初期値としてnullが設定されています。
"""

# テーブルにコメントを追加
spark.sql(f'COMMENT ON TABLE {table_name} IS "{table_comment}"')

# カラムコメント
column_comments = {
    "ds": "日付（YYYY-MM-DDフォーマット）",
    "vm": "自動販売機ID、整数",
    "item": "商品ID、整数",
    "y": "販売数、整数、例 13"
}

# 各カラムにコメントを追加
for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)
