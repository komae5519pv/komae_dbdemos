-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 自動販売機の需要予測・分析デモデータを作成
-- MAGIC ## やること
-- MAGIC - csvを読み込んで、bronzeテーブルおよび需要予測モデルのトレーニングデータセットを作ります、本ノートブックを上から下まで流してください
-- MAGIC - クラスタはDBR15.4 LTS or DBR15.4 LTS ML以降で実行してください
-- MAGIC - [テーブル定義書](https://docs.google.com/spreadsheets/d/10wdoTxlAGcD5gHjY4_upPYKd1gt4rEupKLSgP5q4uWI/edit?gid=1392031218#gid=1392031218)に基づくテーブルを作ります
-- MAGIC
-- MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/01.%20DLT_Pipeline.png?raw=true' width='1200'/>
-- MAGIC
-- MAGIC 想定のディレクトリ構成
-- MAGIC
-- MAGIC ```
-- MAGIC /<catalog_name>
-- MAGIC ├── demand_forecast                           <- スキーマ
-- MAGIC │   ├── bronze_xxx                            <- ローデータテーブル
-- MAGIC │       ├── bronze_sales                      <- 自動販売機売上                       ★ココ!
-- MAGIC │       ├── bronze_vending_machine_location   <- 自販機設定場所マスタ                  ★ココ!
-- MAGIC │       ├── bronze_date_master                <- 日付マスタ                          ★ココ!
-- MAGIC │       ├── bronze_items                      <- 商品マスタ                          ★ココ!
-- MAGIC │   ├── silver_xxx                            <- bronze_xxxをクレンジングしたテーブル
-- MAGIC │       ├── silver_sales                      <- 自動販売機売上                       ★ココ!
-- MAGIC │       ├── silver_vending_machine_location   <- 自販機設定場所マスタ                  ★ココ!
-- MAGIC │       ├── silver_date_master                <- 日付マスタ                          ★ココ!
-- MAGIC │       ├── silver_items                      <- 商品マスタ                          ★ココ!
-- MAGIC │       ├── silver_train                      <- トレーニングデータ                   ★ココ!
-- MAGIC │       ├── silver_analysis                   <- 分析マート（需要予測結果なし）         ★ココ!
-- MAGIC │       ├── silver_inference_input            <- ai_query()専用の未来日付きsalesデータ
-- MAGIC │       ├── silver_forecasts                  <- 需要予測結果データ
-- MAGIC │       ├── silver_forecast_evals             <- 需要予測評価メトリクス
-- MAGIC │   ├── gold_xxx                              <- silver_xxxを使いやすく加工したテーブル
-- MAGIC │       ├── gold_analysis                     <- 分析マート（需要予測結果あり）
-- MAGIC │   ├── raw_data                              <- ボリューム(Import用)
-- MAGIC │       ├── sales.csv                         <- RAWファイルを配置：自動販売機売上
-- MAGIC │       ├── vending_machine_location.csv      <- RAWファイルを配置：自販機設定場所マスタ
-- MAGIC │       ├── date_master.csv                   <- RAWファイルを配置：日付マスタ
-- MAGIC │       ├── items.csv                         <- RAWファイルを配置：商品マスタ
-- MAGIC │   ├── export_data                           <- ボリューム(Export用)
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. パイプライン手動設定
-- MAGIC パイプライン > パイプライン設定
-- MAGIC 一般
-- MAGIC ```
-- MAGIC パイプライン名: <お名前>_demand_forecasting_dlt
-- MAGIC パイプラインモード: トリガー（バッチ処理）
-- MAGIC ```
-- MAGIC
-- MAGIC Advanced
-- MAGIC ```
-- MAGIC catalog: <使用中のカタログ名を指定してください>
-- MAGIC schema : demand_forecast
-- MAGIC volume : raw_data
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. bronzeテーブルを作る

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1-1. bronze_sales / 自動販売機売上

-- COMMAND ----------

-- Step1：CDCデータの読み込み
CREATE OR REFRESH STREAMING LIVE TABLE bronze_sales
AS SELECT *,
          current_timestamp() AS cdc_timestamp
   FROM cloud_files(
     '/Volumes/${catalog}/${schema}/${volume}/sales', 
     'csv',
     map(
       'header', 'true', 
       'inferSchema', 'true', 
       'cloudFiles.inferColumnTypes', 'true',
       'cloudFiles.partitionColumns', ''
     )
   );

-- Step2：CDC処理と重複排除を適用
CREATE OR REFRESH STREAMING LIVE TABLE silver_sales (
  order_date DATE COMMENT "受注日（主キー）、YYYY-MM-DDフォーマット",
  vending_machine_id DOUBLE COMMENT "自動販売機ID、例: 10",
  item_id DOUBLE COMMENT "商品ID、例: 10",
  sales_quantity INT COMMENT "販売数、例: 50",
  stock_quantity INT COMMENT "在庫数、例: 60",
  _rescued_data STRING COMMENT "",
  CONSTRAINT unique_order EXPECT (order_date IS NOT NULL AND vending_machine_id IS NOT NULL AND item_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "`silver_sales`テーブルは、自動販売機x商品ごとの日別売上を管理します。";

APPLY CHANGES INTO LIVE.silver_sales
FROM STREAM(LIVE.bronze_sales)
KEYS (order_date, vending_machine_id, item_id)
SEQUENCE BY cdc_timestamp
COLUMNS * EXCEPT (_rescued_data, cdc_timestamp)
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1-2. bronze_vending_machine_location / 自販機設定場所マスタ

-- COMMAND ----------

-- Step1：CDCデータの読み込み
CREATE OR REFRESH STREAMING LIVE TABLE bronze_vending_machine_location
AS SELECT *,
          current_timestamp() AS cdc_timestamp
   FROM cloud_files(
     '/Volumes/${catalog}/${schema}/${volume}/vending_machine_location', 
     'csv',
     map(
       'header', 'true', 
       'inferSchema', 'true', 
       'cloudFiles.inferColumnTypes', 'true',
       'cloudFiles.partitionColumns', ''
     )
   );

-- Step2：CDC処理と重複排除を適用
CREATE OR REFRESH STREAMING LIVE TABLE silver_vending_machine_location (
  vending_machine_id INT COMMENT "自動販売機ID、ユニーク（主キー）",
  location_type STRING COMMENT "設置場所タイプ、例: Office, Station",
  postal_code STRING COMMENT "郵便番号、例: 100-0005",
  address STRING COMMENT "住所、例: 東京都千代田区丸の内1丁目",
  pref STRING COMMENT "都道府県、例: 東京都",
  city STRING COMMENT "市区町村、例: 千代田区丸の内",
  latitude DOUBLE COMMENT "緯度、小数点以下7桁まで",
  longitude DOUBLE COMMENT "経度、小数点以下7桁まで",
  _rescued_data STRING COMMENT ""
)
COMMENT "`silver_vending_machine_location`テーブルは、自動販売機の設置場所に関する情報（設置場所タイプ、住所、緯度経度など）を管理します。このデータは、自動販売機の配置戦略や販売分析に活用されます。";

APPLY CHANGES INTO LIVE.silver_vending_machine_location
FROM STREAM(LIVE.bronze_vending_machine_location)
KEYS (vending_machine_id)
SEQUENCE BY cdc_timestamp
COLUMNS * EXCEPT (_rescued_data, cdc_timestamp)
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1-3. bronze_date_master / 日付マスタ

-- COMMAND ----------

-- Step1：CDCデータの読み込み
CREATE OR REFRESH STREAMING LIVE TABLE bronze_date_master
AS SELECT *,
          current_timestamp() AS cdc_timestamp
   FROM cloud_files(
     '/Volumes/${catalog}/${schema}/${volume}/date_master', 
     'csv',
     map(
       'header', 'true', 
       'inferSchema', 'true', 
       'cloudFiles.inferColumnTypes', 'true',
       'cloudFiles.partitionColumns', ''
     )
   );

-- Step2：CDC処理と重複排除を適用
CREATE OR REFRESH STREAMING LIVE TABLE silver_date_master (
  date DATE COMMENT "日付、YYYY-MM-DDフォーマット（主キー）",
  day_of_week STRING COMMENT "曜日、例: Friday",
  month STRING COMMENT "月、例: November",
  year INT COMMENT "年、例: 2024",
  quarter STRING COMMENT "四半期、例: Q4",
  is_holiday INT COMMENT "祝日フラグ、1=祝日、0=祝日以外",
  _rescued_data STRING COMMENT ""
)
COMMENT "`silver_date_master`テーブルは、日付に関する基本情報（曜日、月、四半期、年、祝日フラグなど）を管理します。このデータは、時系列分析や季節性の把握、営業日の判定などに活用されます。";

APPLY CHANGES INTO LIVE.silver_date_master
FROM STREAM(LIVE.bronze_date_master)
KEYS (date)
SEQUENCE BY cdc_timestamp
COLUMNS * EXCEPT (_rescued_data, cdc_timestamp)
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1-4. bronze_items / 商品マスタ

-- COMMAND ----------

-- Step1：CDCデータの読み込み
CREATE OR REFRESH STREAMING LIVE TABLE bronze_items
AS SELECT *,
          current_timestamp() AS cdc_timestamp
   FROM cloud_files(
     '/Volumes/${catalog}/${schema}/${volume}/items', 
     'csv',
     map(
       'header', 'true', 
       'inferSchema', 'true', 
       'cloudFiles.inferColumnTypes', 'true',
       'cloudFiles.partitionColumns', ''
     )
   );

-- Step2：CDC処理と重複排除を適用
CREATE OR REFRESH STREAMING LIVE TABLE silver_items (
  item_id INT COMMENT "商品ID、ユニーク（主キー）",
  item_name STRING COMMENT "商品名、例: Coffee",
  category_name STRING COMMENT "カテゴリー名、例: Soft Drink",
  unit_price INT COMMENT "商品単価（円）、例: 100",
  unit_cost INT COMMENT "商品原価（円）、例: 60",
  _rescued_data STRING COMMENT ""
)
COMMENT "`silver_items`テーブルは、自動販売機で取り扱う商品の基本情報（商品名、カテゴリー、価格など）を管理します。このデータは、商品ラインナップの分析、価格戦略の立案、収益性の評価などに活用されます。";

APPLY CHANGES INTO LIVE.silver_items
FROM STREAM(LIVE.bronze_items)
KEYS (item_id)
SEQUENCE BY cdc_timestamp
COLUMNS * EXCEPT (_rescued_data, cdc_timestamp)
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. silverテーブルを作る

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2-1. silver_train / トレーニングデータ

-- COMMAND ----------

-- 機械学習で使うシルバーテーブルを作成
-- 自販機ID、商品ID、日付がNULLでないことを確認し、違反した場合は行を削除する制約を追加
CREATE OR REFRESH LIVE TABLE silver_train (
  ds DATE COMMENT "受注日（主キー）、YYYY-MM-DDフォーマット",
  vm DOUBLE COMMENT "自動販売機ID、例: 10",
  item DOUBLE COMMENT "商品ID、例: 10",
  y LONG COMMENT "販売数、例: 50",
  CONSTRAINT vm_not_null EXPECT (vm IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT item_not_null EXPECT (item IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT ds_not_null EXPECT (ds IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "`silver_train`テーブルは、自動販売機の需要予測モデルのトレーニングデータとして活用されます。"
AS
  SELECT
    CAST(order_date as date) as ds,
    vending_machine_id as vm,
    item_id as item,
    CAST(SUM(sales_quantity) AS BIGINT) as y
  FROM
    live.silver_sales
  GROUP BY ds, vm, item
  ORDER BY ds, vm, item

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2-2. silver_analysis / 分析用マート

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_analysis (
    order_date DATE COMMENT "日付、YYYY-MM-DDフォーマット（主キー、外部キー）",
    vending_machine_id DOUBLE COMMENT "自動販売機ID（主キー）",
    item_id DOUBLE COMMENT "商品ID（外部キー）",
    item_name STRING COMMENT "商品名、例: Cofee",
    category_name STRING COMMENT "カテゴリー名、例: Soft Drink",
    actual_sales LONG COMMENT "売上実績",
    unit_price INT COMMENT "商品単価",
    actual_sales_quantity INT COMMENT "実績販売数、整数",
    stock_quantity INT COMMENT "在庫数、整数",
    location_type STRING COMMENT "設置場所タイプ、例 'Office', 'Station', 'Shopping_Mall'",
    postal_code STRING COMMENT "郵便番号、例: 100-0005",
    address STRING COMMENT "住所、例: 東京都千代田区丸の内1丁目",
    pref STRING COMMENT "都道府県、例: 東京都",
    city STRING COMMENT "市区町村、例: 千代田区丸の内",
    latitude DOUBLE COMMENT "緯度、小数点以下7桁まで",
    longitude DOUBLE COMMENT "経度、小数点以下7桁まで",
    day_of_week STRING COMMENT "曜日、例: Friday",
    month STRING COMMENT "月、例: November",
    quarter STRING COMMENT "四半期、例: Q4",
    year INT COMMENT "年、例: 2024",
    is_holiday INT COMMENT "祝日フラグ、true=祝日、false=平日",
    CONSTRAINT vm_not_null EXPECT (vending_machine_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT item_not_null EXPECT (item_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT ds_not_null EXPECT (order_date IS NOT NULL) ON VIOLATION DROP ROW)
AS
  WITH sales AS (
      SELECT order_date, vending_machine_id, item_id, sales_quantity, stock_quantity
      FROM live.silver_sales
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
      live.silver_items i ON s.item_id = i.item_id
  LEFT OUTER JOIN
      live.silver_vending_machine_location v ON s.vending_machine_id = v.vending_machine_id
  LEFT OUTER JOIN
      live.silver_date_master d ON s.order_date = d.date
