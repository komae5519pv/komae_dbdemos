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
# MAGIC │       ├── silver_analysis                   <- 分析用マート ★ココ!
# MAGIC │       ├── silver_demand_forecasting         <- 需要予測結果付き分析マート
# MAGIC │       ├── silver_forecasts                  <- 需要予測結果データ
# MAGIC │       ├── silver_forecast_evals             <- 需要予測評価メトリクス
# MAGIC │   ├── gold_xxx                              <- silver_xxxを使いやすく加工したテーブル
# MAGIC │       ├── gold_analysis                     <- 需要予測結果付き分析マート ★ココ!
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
# MAGIC ## 1. goldテーブルを作る

# COMMAND ----------

# # スキーマ内のすべてのテーブル名を取得する
# tables_df = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}")

# # テーブル名が "gold_" で始まるテーブルのみ削除する
# for table in tables_df.collect():
#     table_name = table["tableName"]
#     if table_name.startswith("gold_"):
#         spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{table_name}")
#         print(f"削除されたテーブル: {table_name}")

# print("全ての gold_ で始まるテーブルが削除されました。")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-1. gold_analysis / 分析用マート

# COMMAND ----------

# DBTITLE 1,Step1 Temp View
# ----------------------------------------------
# Step1 一時ビューを作成
# ----------------------------------------------
create_view_query = f"""
CREATE OR REPLACE TEMPORARY VIEW tmp_view_analysis AS
SELECT
    f.order_date,                                                     -- 受注日
    f.vending_machine_id,                                             -- 自動販売機ID
    f.item_id,                                                        -- 商品ID
    i.item_name,                                                      -- 商品名
    i.category_name,                                                  -- カテゴリ名
    CAST(s.sales_quantity * i.unit_price AS BIGINT) AS actual_sales,  -- 実績売上金額
    i.unit_price,                                                     -- 商品単価
    s.sales_quantity AS actual_sales_quantity,                        -- 実績販売数
    f.forecast_sales_quantity,                                        -- 予測販売数
    f.forecast_sales_quantity_upper,                                  -- 予測販売数（上限）
    f.forecast_sales_quantity_lower,                                  -- 予測販売数（下限）
    f.sales_inference_date,                                           -- 販売数予測日
    s.stock_quantity,                                                 -- 在庫数
    CASE
        WHEN s.stock_quantity < f.forecast_sales_quantity THEN 1 
        ELSE 0
    END AS restock_flag,                                             -- 補充フラグ
    CASE 
        WHEN s.stock_quantity < f.forecast_sales_quantity THEN f.forecast_sales_quantity - s.stock_quantity 
        ELSE 0 
    END AS recommended_restock_quantity,                              -- 推奨補充数量
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
    d.is_holiday,                                                     -- 祝日フラグ
    AVG(s.sales_quantity) OVER (
        PARTITION BY f.vending_machine_id, f.item_id 
        ORDER BY f.order_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_daily_demand,                                            -- 平均日次需要（過去7日）
    MAX(s.sales_quantity) OVER (
        PARTITION BY f.vending_machine_id, f.item_id 
        ORDER BY f.order_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS max_daily_demand,                                            -- 最大日次需要（過去7日）
    ROUND(
        (MAX(s.sales_quantity) OVER (
            PARTITION BY f.vending_machine_id, f.item_id 
            ORDER BY f.order_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) * 2.2) - (AVG(s.sales_quantity) OVER (
            PARTITION BY f.vending_machine_id, f.item_id 
            ORDER BY f.order_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) * 1.5), 
    2) AS safety_stock,                                               -- 安全在庫（Average-Max方式）
    CASE
        WHEN s.sales_quantity / NULLIF(f.forecast_sales_quantity, 0) < 1 THEN 1
        ELSE 0
    END AS stock_warn,                                                -- 在庫警告フラグ
    CASE 
        WHEN s.stock_quantity > (f.forecast_sales_quantity + safety_stock) 
        THEN s.stock_quantity - (f.forecast_sales_quantity + safety_stock)
        ELSE 0 
    END AS excess_inventory,                                          -- 過剰在庫数
    CASE 
        WHEN s.stock_quantity > (f.forecast_sales_quantity + safety_stock) 
        THEN (s.stock_quantity - (f.forecast_sales_quantity + safety_stock)) * i.unit_price
        ELSE 0 
    END AS excess_inventory_value,                                    -- 過剰在庫金額
    -- 機会損失数の計算
    CASE 
        WHEN s.stock_quantity < f.forecast_sales_quantity AND s.sales_quantity < f.forecast_sales_quantity
        THEN f.forecast_sales_quantity - s.sales_quantity
        ELSE 0 
    END AS lost_sales_quantity,                                       -- 機会損失数
    -- 機会損失金額の計算
    CASE 
        WHEN s.stock_quantity < f.forecast_sales_quantity AND s.sales_quantity < f.forecast_sales_quantity
        THEN (f.forecast_sales_quantity - s.sales_quantity) * i.unit_price
        ELSE 0 
    END AS lost_sales_value,                                         -- 機会損失金額
    -- 需要充足率の計算
    CASE 
        WHEN f.forecast_sales_quantity > 0 
        THEN ROUND(s.sales_quantity / f.forecast_sales_quantity * 100, 1)
        ELSE 100 
    END AS demand_fulfillment_rate                                   -- 需要充足率（%）
FROM
    {MY_CATALOG}.{MY_SCHEMA}.silver_forecasts f 
LEFT JOIN
    {MY_CATALOG}.{MY_SCHEMA}.bronze_sales s ON f.order_date = s.order_date
                                            AND f.vending_machine_id = s.vending_machine_id
                                            AND f.item_id = s.item_id
LEFT JOIN
    {MY_CATALOG}.{MY_SCHEMA}.bronze_items i ON f.item_id = i.item_id
LEFT JOIN
    {MY_CATALOG}.{MY_SCHEMA}.bronze_vending_machine_location v ON f.vending_machine_id = v.vending_machine_id
LEFT JOIN
    {MY_CATALOG}.{MY_SCHEMA}.bronze_date_master d ON f.order_date = d.date
"""

# 一時ビューを作成
spark.sql(create_view_query)

display(
    spark.table("tmp_view_analysis")
)

# COMMAND ----------

# DBTITLE 1,Step1 Temp View
# # ----------------------------------------------
# # Step1 一時ビューを作成
# # ----------------------------------------------
# create_view_query = f"""
# CREATE OR REPLACE TEMPORARY VIEW tmp_view_analysis AS
# SELECT
#     f.order_date,                                                     -- 受注日
#     f.vending_machine_id,                                             -- 自動販売機ID
#     f.item_id,                                                        -- 商品ID
#     i.item_name,                                                      -- 商品名
#     i.category_name,                                                  -- カテゴリ名
#     CAST(s.sales_quantity * i.unit_price AS BIGINT) AS actual_sales,  -- 実績売上金額
#     i.unit_price,                                                     -- 商品単価
#     s.sales_quantity AS actual_sales_quantity,                        -- 実績販売数
#     f.forecast_sales_quantity,                                        -- 予測販売数
#     f.forecast_sales_quantity_upper,                                  -- 予測販売数（上限）
#     f.forecast_sales_quantity_lower,                                  -- 予測販売数（下限）
#     f.sales_inference_date,                                           -- 販売数予測日
#     s.stock_quantity,                                                 -- 在庫数
#     CASE
#         WHEN s.stock_quantity < f.forecast_sales_quantity THEN 1 
#         ELSE 0
#     END AS restock_flag,
#     CASE 
#         WHEN s.stock_quantity < f.forecast_sales_quantity THEN f.forecast_sales_quantity - s.stock_quantity 
#         ELSE 0 
#     END AS recommended_restock_quantity,                              -- 推奨補充数量
#     v.location_type,                                                  -- 設置場所タイプ
#     v.postal_code,                                                    -- 郵便番号
#     v.address,                                                        -- 住所
#     v.pref,                                                           -- 都道府県
#     v.city,                                                           -- 市区町村
#     v.latitude,                                                       -- 緯度
#     v.longitude,                                                      -- 経度
#     d.day_of_week,                                                    -- 曜日
#     d.month,                                                          -- 月
#     d.quarter,                                                        -- 四半期
#     d.year,                                                           -- 年
#     d.is_holiday,                                                     -- 祝日フラグ
#     AVG(s.sales_quantity) OVER (
#         PARTITION BY f.vending_machine_id, f.item_id 
#         ORDER BY f.order_date 
#         ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
#     ) AS avg_daily_demand,                                            -- 平均日次需要（過去7日）
#     MAX(s.sales_quantity) OVER (
#         PARTITION BY f.vending_machine_id, f.item_id 
#         ORDER BY f.order_date 
#         ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
#     ) AS max_daily_demand,                                            -- 最大日次需要（過去7日）
#     ROUND(
#         (MAX(s.sales_quantity) OVER (
#             PARTITION BY f.vending_machine_id, f.item_id 
#             ORDER BY f.order_date 
#             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
#         ) * 2.2) - (AVG(s.sales_quantity) OVER (
#             PARTITION BY f.vending_machine_id, f.item_id 
#             ORDER BY f.order_date 
#             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
#         ) * 1.5), 
#     2) AS safety_stock,                                               -- 安全在庫（Average-Max方式）
#     CASE
#         WHEN s.stock_quantity < f.forecast_sales_quantity THEN 1      -- 在庫が予測販売数を下回る場合に警告
#         ELSE 0
#     END AS stock_warn,                                                -- 安全在庫警告フラグ(安全在庫が在庫実績より多い場合のフラグ)

#     CASE 
#         WHEN s.stock_quantity > (f.forecast_sales_quantity + safety_stock) 
#         THEN s.stock_quantity - (f.forecast_sales_quantity + safety_stock)
#         ELSE 0 
#     END AS excess_inventory,                                          -- 過剰在庫数
#     CASE 
#         WHEN s.stock_quantity > (f.forecast_sales_quantity + safety_stock) 
#         THEN (s.stock_quantity - (f.forecast_sales_quantity + safety_stock)) * i.unit_price
#         ELSE 0 
#     END AS excess_inventory_value,                                    -- 過剰在庫金額

#     -- 機会損失数の計算
#     CASE 
#         WHEN s.stock_quantity < f.forecast_sales_quantity 
#         THEN f.forecast_sales_quantity - s.sales_quantity
#         ELSE 0 
#     END AS lost_sales_quantity,                                       -- 機会損失数

#     -- 機会損失金額の計算
#     CASE 
#         WHEN s.sales_quantity >= s.stock_quantity 
#         THEN (f.forecast_sales_quantity - s.stock_quantity) * i.unit_price
#         ELSE 0 
#     END AS lost_sales_value,                                         -- 機会損失金額

#     -- 需要充足率の計算（実績販売数/予測販売数）
#     CASE 
#         WHEN s.stock_quantity < f.forecast_sales_quantity 
#         THEN ROUND(s.sales_quantity / f.forecast_sales_quantity * 100, 2)
#         ELSE 100 
#     END AS demand_fulfillment_rate                                   -- 需要充足率（%）
# FROM
#     {MY_CATALOG}.{MY_SCHEMA}.silver_forecasts f 
# LEFT JOIN
#     {MY_CATALOG}.{MY_SCHEMA}.bronze_sales s ON f.order_date = s.order_date
#                                             AND f.vending_machine_id = s.vending_machine_id
#                                             AND f.item_id = s.item_id
# LEFT JOIN
#     {MY_CATALOG}.{MY_SCHEMA}.bronze_items i ON f.item_id = i.item_id
# LEFT JOIN
#     {MY_CATALOG}.{MY_SCHEMA}.bronze_vending_machine_location v ON f.vending_machine_id = v.vending_machine_id
# LEFT JOIN
#     {MY_CATALOG}.{MY_SCHEMA}.bronze_date_master d ON f.order_date = d.date
# """

# # 一時ビューを作成
# spark.sql(create_view_query)

# display(
#     spark.table("tmp_view_analysis")
# )

# COMMAND ----------

# DBTITLE 1,Step2 Create Gold Table
# ----------------------------------------------
# Step2 gold_analysis テーブル作成
# ----------------------------------------------
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.gold_analysis (
  order_date DATE,                      -- 日付
  vending_machine_id BIGINT,            -- 自動販売機ID
  item_id BIGINT,                       -- 商品ID
  item_name STRING,                     -- 商品名
  category_name STRING,                 -- カテゴリー
  actual_sales BIGINT,                  -- 実績売上金額
  unit_price BIGINT,                    -- 商品単価
  actual_sales_quantity BIGINT,         -- 実績販売数
  forecast_sales_quantity BIGINT,       -- 予測販売数
  forecast_sales_quantity_upper BIGINT, -- 予測販売数（上限）
  forecast_sales_quantity_lower BIGINT, -- 予測販売数（下限）
  sales_inference_date DATE,            -- 販売数予測日
  stock_quantity BIGINT,                -- 在庫数
  safety_stock FLOAT,                   -- 安全在庫
  stock_warn BIGINT,                    -- 安全在庫警告フラグ
  excess_inventory FLOAT,               -- 過剰在庫数
  excess_inventory_value FLOAT,         -- 過剰在庫金額
  lost_sales_quantity FLOAT,            -- 機会損失数
  demand_fulfillment_rate FLOAT,        -- 需要充足率（%）
  restock_flag BIGINT,                  -- 補充フラグ
  recommended_restock_quantity BIGINT,  -- 推奨補充数量
  location_type STRING,                 -- 設置場所タイプ
  postal_code STRING,                   -- 郵便番号
  address STRING,                       -- 住所
  pref STRING,                          -- 都道府県
  city STRING,                          -- 市区町村
  latitude FLOAT,                       -- 緯度
  longitude FLOAT,                      -- 経度
  day_of_week STRING,                   -- 曜日
  month STRING,                         -- 月
  quarter STRING,                       -- 四半期
  year STRING,                          -- 年
  is_holiday BOOLEAN                    -- 祝日フラグ
)
USING DELTA
PARTITIONED BY (order_date)
"""

# テーブル作成クエリを実行
spark.sql(create_table_query)

# ----------------------------------------------
# Step3 MERGEステートメントを実行
# ----------------------------------------------
merge_query = f"""
MERGE INTO {MY_CATALOG}.{MY_SCHEMA}.gold_analysis AS target
USING tmp_view_analysis AS source
ON target.order_date = source.order_date
   AND target.vending_machine_id = source.vending_machine_id
   AND target.item_id = source.item_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
"""

# MERGEクエリを実行
spark.sql(merge_query)

# 作成されたテーブルの確認
result_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.gold_analysis')

print(f"Total records: {result_df.count()}")
display(result_df)

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.gold_analysis'

# テーブルコメント
comment = """
`gold_analysis`テーブルは、自動販売機の売上データと関連する各種マスタ情報を統合した分析用マートです。需要予測結果は含まれません。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
    "order_date": "日付、YYYY-MM-DDフォーマット（主キー、外部キー）",
    "vending_machine_id": "自動販売機ID（主キー）",
    "item_id": "商品ID（外部キー）",
    "item_name": "商品名、例: Cola",
    "category_name": "カテゴリー名、例: Soft Drink",
    "actual_sales": "売上実績",
    "unit_price": "商品単価",
    "actual_sales_quantity": "実績販売数、整数",
    "forecast_sales_quantity": "予測販売数、整数",
    "forecast_sales_quantity_upper": "予測販売数（上限）、整数",
    "forecast_sales_quantity_lower": "予測販売数（下限）、整数",
    "sales_inference_date": "販売数予測日、YYYY-MM-DDフォーマット",
    "stock_quantity": "在庫数、整数",
    "safety_stock": "安全在庫、浮動小数店、販売機会損失を防ぐために維持すべき在庫量",
    "stock_warn": "安全在庫警告フラグ、例 1: 在庫不足を示唆、0: 正常",
    "excess_inventory": "過剰在庫数、浮動小数点",
    "excess_inventory_value": "過剰在庫金額、浮動小数点",
    "lost_sales_quantity": "機会損失数、浮動小数点",
    "demand_fulfillment_rate": "需要充足率（%）",
    "restock_flag": "補充フラグ、補充が必要か否か(1:必要, 0:不要)",
    "recommended_restock_quantity": "推奨補充数量、整数",
    "location_type": "設置場所タイプ、例 'Office', 'Station', 'Shopping_Mall'",
    "postal_code": "郵便番号、例: 100-0005",
    "address": "住所、例: 東京都千代田区丸の内1丁目",
    "pref": "都道府県、例: 東京都",
    "city": "市区町村、例: 千代田区丸の内",
    "latitude": "緯度、小数点以下7桁まで",
    "longitude": "経度、小数点以下7桁まで",
    "day_of_week": "曜日、例: Friday",
    "month": "月、例: November",
    "quarter": "四半期、例: Q4",
    "year": "年、例: 2024",
    "is_holiday": "祝日フラグ、true=祝日、false=平日"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)
