# Databricks notebook source
# MAGIC %md
# MAGIC # 自動販売機の需要予測・分析デモデータを作成
# MAGIC ## やること
# MAGIC - csvを読み込んで、bronze、silverテーブルを作ります、本ノートブックを上から下まで流してください
# MAGIC - クラスタはDBR15.4 LTS or DBR15.4 LTS ML以降で実行してください
# MAGIC - [テーブル定義書](https://docs.google.com/spreadsheets/d/10wdoTxlAGcD5gHjY4_upPYKd1gt4rEupKLSgP5q4uWI/edit?gid=1392031218#gid=1392031218)に基づくテーブルを作ります
# MAGIC
# MAGIC 想定のディレクトリ構成
# MAGIC
# MAGIC ```
# MAGIC /<catalog_name>
# MAGIC ├── demand_forecast                           <- スキーマ
# MAGIC │   ├── bronze_xxx                            <- ローデータテーブル
# MAGIC │       ├── bronze_sales                      <- 自動販売機売上                       ★ココ!
# MAGIC │       ├── bronze_vending_machine_location   <- 自販機設定場所マスタ                  ★ココ!
# MAGIC │       ├── bronze_date_master                <- 日付マスタ                          ★ココ!
# MAGIC │       ├── bronze_items                      <- 商品マスタ                          ★ココ!
# MAGIC │   ├── silver_xxx                            <- bronze_xxxをクレンジングしたテーブル
# MAGIC │       ├── silver_sales                      <- 自動販売機売上                       ※本ノートブックでは実装しません(CDC考慮なし)
# MAGIC │       ├── silver_vending_machine_location   <- 自販機設定場所マスタ                  ※本ノートブックでは実装しません(CDC考慮なし)
# MAGIC │       ├── silver_date_master                <- 日付マスタ                          ※本ノートブックでは実装しません(CDC考慮なし)
# MAGIC │       ├── silver_items                      <- 商品マスタ                          ※本ノートブックでは実装しません(CDC考慮なし)
# MAGIC │       ├── silver_train                      <- トレーニングデータ                   ★ココ!
# MAGIC │       ├── silver_analysis                   <- 分析マート（需要予測結果なし）         ★ココ!
# MAGIC │       ├── silver_inference_input            <- ai_query()専用の未来日付きsalesデータ
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

# MAGIC %run ./01_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. bronzeテーブルを作る
# MAGIC
# MAGIC ここはDLTに差し替えます。ここではパイプラインのロジックを確認するためだけに加工処理を行なう

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-1. bronze_sales / 自動販売機売上

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DateType, LongType

# 自動販売機売上のスキーマを定義
schema_sales = StructType([
    StructField("order_date", DateType(), False),              # 受注日
    StructField("vending_machine_id", LongType(), False),      # 自動販売機ID
    StructField("item_id", LongType(), False),                 # 商品ID
    StructField("sales_quantity", LongType(), False),          # 販売数
    StructField("stock_quantity", LongType(), False)           # 在庫数
])

# CSVファイルを読み込む
sales_df = spark.read.csv(
    f'/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/sales/sales.csv',
    header=True,
    schema=schema_sales
)

# Deltaテーブルとして保存
sales_df.write.mode("overwrite").format("delta").saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_sales')

# データの確認
print(f"Total records: {sales_df.count()}")
display(sales_df)

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.bronze_sales'

# テーブルコメント
comment = """
`bronze_sales`テーブルは、自動販売機での販売実績を管理します。各自動販売機ごとの売上状況や商品ごとの販売パフォーマンスを分析するために活用されます。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
    "order_date": "受注日（主キー）、YYYY-MM-DDフォーマット",
    "vending_machine_id": "自動販売機ID、例: 10",
    "item_id": "商品ID、例: 10",
    "sales_quantity": "販売数、例: 50",
    "stock_quantity": "在庫数、例: 60"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-2. bronze_vending_machine_location / 自販機設定場所マスタ

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType

# 自販機設定場所マスタのスキーマを定義
schema_vending_machine_locations = StructType([
    StructField("vending_machine_id", LongType(), False),  # 自動販売機ID
    StructField("location_type", StringType(), False),     # 設置場所タイプ
    StructField("postal_code", StringType(), False),       # 郵便番号
    StructField("address", StringType(), False),           # 住所
    StructField("pref", StringType(), False),              # 都道府県
    StructField("city", StringType(), False),              # 市区町村
    StructField("latitude", FloatType(), False),           # 緯度
    StructField("longitude", FloatType(), False)           # 経度
])

# CSVファイルを読み込む
vending_machine_locations_df = spark.read.csv(
    f'/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/vending_machine_location/vending_machine_location.csv',
    header=True,
    schema=schema_vending_machine_locations
)

# Deltaテーブルとして保存
vending_machine_locations_df.write.mode("overwrite").format("delta").saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_vending_machine_location')

# データの確認
print(f"Total records: {vending_machine_locations_df.count()}")
display(vending_machine_locations_df.limit(10))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.bronze_vending_machine_location'

# テーブルコメント
comment = """
`bronze_vending_machine_location`テーブルは、自動販売機の設置場所に関する情報（設置場所タイプ、住所、緯度経度など）を管理します。このデータは、自動販売機の配置戦略や販売分析に活用されます。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
    "vending_machine_id": "自動販売機ID、ユニーク（主キー）",
    "location_type": "設置場所タイプ、例: Office, Station",
    "postal_code": "郵便番号、例: 100-0005",
    "address": "住所、例: 東京都千代田区丸の内1丁目",
    "pref": "都道府県、例: 東京都",
    "city": "市区町村、例: 千代田区丸の内",
    "latitude": "緯度、小数点以下7桁まで",
    "longitude": "経度、小数点以下7桁まで"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-3. bronze_date_master / 日付マスタ

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DateType, StringType, LongType

# 日付マスタのスキーマを定義
schema_date_master = StructType([
    StructField("date", DateType(), False),           # 日付
    StructField("day_of_week", StringType(), False),  # 曜日
    StructField("month", StringType(), False),        # 月
    StructField("quarter", StringType(), False),      # 四半期
    StructField("year", StringType(), False),         # 年
    StructField("is_holiday", LongType(), False)      # 祝日フラグ
])

# CSVファイルを読み込む
date_master_df = spark.read.csv(
    f'/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/date_master/date_master.csv',
    header=True,
    schema=schema_date_master
)

# Deltaテーブルとして保存
date_master_df.write.mode("overwrite").format("delta").saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_date_master')

# データの確認
print(f"Total records: {date_master_df.count()}")
display(date_master_df.limit(10))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.bronze_date_master'

# テーブルコメント
comment = """
`bronze_date_master`テーブルは、日付に関する基本情報（曜日、月、四半期、年、祝日フラグなど）を管理します。このデータは、時系列分析や季節性の把握、営業日の判定などに活用されます。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
    "date": "日付、YYYY-MM-DDフォーマット（主キー）",
    "day_of_week": "曜日、例: Friday",
    "month": "月、例: November",
    "quarter": "四半期、例: Q4",
    "year": "年、例: 2024",
    "is_holiday": "祝日フラグ、1=祝日、0=祝日以外"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-4. bronze_items / 商品マスタ

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType

# 商品マスタのスキーマを定義
schema_items = StructType([
    StructField("item_id", LongType(), False),          # 商品ID
    StructField("item_name", StringType(), False),      # 商品名
    StructField("category_name", StringType(), False),  # カテゴリー
    StructField("unit_price", LongType(), False),       # 商品単価
    StructField("unit_cost", LongType(), False)         # 商品原価
])

# CSVファイルを読み込む
items_df = spark.read.csv(
    f'/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/items/items.csv',
    header=True,
    schema=schema_items
)

# Deltaテーブルとして保存
items_df.write.mode("overwrite").format("delta").saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_items')

# データの確認
print(f"Total records: {items_df.count()}")
display(items_df)

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.bronze_items'

# テーブルコメント
comment = """
`bronze_items`テーブルは、自動販売機で取り扱う商品の基本情報（商品名、カテゴリー、価格など）を管理します。このデータは、商品ラインナップの分析、価格戦略の立案、収益性の評価などに活用されます。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
    "item_id": "商品ID、ユニーク（主キー）",
    "item_name": "商品名、例: Cola",
    "category_name": "カテゴリー名、例: Soft Drink",
    "unit_price": "商品単価（円）、例: 100",
    "unit_cost": "商品原価（円）、例: 60"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. silverテーブルを作る

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-1. silver_train / トレーニングデータ

# COMMAND ----------

from pyspark.sql.types import *

# structure of the training data set
schema_sales = StructType([
    StructField("vm", LongType(), False),      # 自動販売機ID
    StructField("item", LongType(), False),    # 商品ID
    StructField("ds", DateType(), False),      # 受注日
    StructField("y", LongType(), False)        # 販売数
])

train = spark.sql(f'''
SELECT
  vending_machine_id as vm,
  item_id as item,
  CAST(order_date as date) as ds,
  SUM(sales_quantity) as y
FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_sales
WHERE
  vending_machine_id IS NOT NULL AND item_id IS NOT NULL AND order_date IS NOT NULL
GROUP BY vm, item, ds
ORDER BY vm, item, ds
''')

# create delta table
train.write.format('delta').mode('overwrite').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_train')

display(train.count())
display(train)

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_train'

# テーブルコメント
comment = """
`silver_train`テーブルは、自動販売機の需要予測モデルのトレーニングデータとして活用されます。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
    "ds": "受注日（主キー）、YYYY-MM-DDフォーマット",
    "vm": "自動販売機ID、例: 10",
    "item": "商品ID、例: 10",
    "y": "販売数、例: 50"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2. silver_analysis / 分析用マート

# COMMAND ----------

# ----------------------------------------------
# Step1 一時ビューを作成
# ----------------------------------------------
create_view_query = f"""
CREATE OR REPLACE TABLE silver_analysis AS
  WITH sales AS (
      SELECT order_date, vending_machine_id, item_id, sales_quantity, stock_quantity
      FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_sales
      WHERE
        order_date IS NOT NULL AND
        vending_machine_id IS NOT NULL AND
        item_id IS NOT NULL
    )
  SELECT
      s.order_date,                                                     -- 受注日
      s.vending_machine_id,                                             -- 自動販売機ID
      s.item_id,                                                        -- 商品ID
      i.item_name,                                                      -- 商品名
      i.category_name,                                                  -- カテゴリ名
      CAST(s.sales_quantity * i.unit_price AS BIGINT) AS actual_sales,  -- 実績売上金額
      i.unit_price,                                                     -- 商品単価
      s.sales_quantity AS actual_sales_quantity,                        -- 実績販売数
      s.stock_quantity,                                                 -- 在庫数
      v.location_type,                                                  -- 設置場所タイプ
      v.postal_code,                                                    -- 郵便番号
      v.address,                                                        -- 住所
      v.pref,                                                           -- 都道府県
      v.city,                                                           -- 市区町村
      v.latitude,                                                       -- 緯度
      v.longitude,                                                      -- 経度
      d.day_of_week,                                                    -- 曜日
      d.month,                                                          -- 月
      d.quarter,                                                        -- 四半期
      d.year,                                                           -- 年
      d.is_holiday                                                      -- 祝日フラグ
  FROM
      sales s
  LEFT OUTER JOIN
      {MY_CATALOG}.{MY_SCHEMA}.bronze_items i ON s.item_id = i.item_id
  LEFT OUTER JOIN
      {MY_CATALOG}.{MY_SCHEMA}.bronze_vending_machine_location v ON s.vending_machine_id = v.vending_machine_id
  LEFT OUTER JOIN
      {MY_CATALOG}.{MY_SCHEMA}.bronze_date_master d ON s.order_date = d.date
"""

# SQL実行
spark.sql(create_view_query)

# 作成されたテーブルの確認
result_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.silver_analysis')

print(f"Total records: {result_df.count()}")
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3. silver_inference_input / 推論用インプットデータ

# COMMAND ----------

# 入力データテーブルを読み込み、vm=1, item=1に絞り込む
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW input_view AS
    SELECT 
        DATE_FORMAT(ds, 'yyyy-MM-dd') AS ds,
        vm,
        item,
        CAST(y as INT) as y
    FROM {MY_CATALOG}kono.{MY_SCHEMA}.silver_train
    WHERE vm = 1 AND item = 1
""")

# 最終日を取得
max_date = spark.sql("SELECT MAX(ds) as max_date FROM input_view").collect()[0]['max_date']

# 最終日から90日間の日付シーケンスを生成
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW future_dates AS
    SELECT 
        DATE_FORMAT(date_add('{max_date}', seq), 'yyyy-MM-dd') as ds,
        1 as vm,
        1 as item
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
