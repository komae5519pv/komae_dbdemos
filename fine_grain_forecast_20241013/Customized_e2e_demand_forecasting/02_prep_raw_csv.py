# Databricks notebook source
# MAGIC %md
# MAGIC # 自動販売機の需要予測・分析デモデータを作成
# MAGIC ## やること
# MAGIC - デモ用にローデータ（csv）を出力します、本ノートブックを上から下まで流してください
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
# MAGIC │       ├── silver_inference_input            <- ai_query()専用の未来日付きsalesデータ
# MAGIC │       ├── silver_forecasts                  <- 需要予測結果データ
# MAGIC │       ├── silver_forecast_evals             <- 需要予測評価メトリクス
# MAGIC │   ├── gold_xxx                              <- silver_xxxを使いやすく加工したテーブル
# MAGIC │       ├── gold_analysis                     <- 分析マート（需要予測結果あり）
# MAGIC │   ├── raw_data                              <- ボリューム(Import用)
# MAGIC │       ├── sales.csv                         <- RAWファイルを配置：自動販売機売上          ★ココ!
# MAGIC │       ├── vending_machine_location.csv      <- RAWファイルを配置：自販機設定場所マスタ     ★ココ!
# MAGIC │       ├── date_master.csv                   <- RAWファイルを配置：日付マスタ             ★ココ!
# MAGIC │       ├── items.csv                         <- RAWファイルを配置：商品マスタ             ★ココ!
# MAGIC │   ├── export_data                           <- ボリューム(Export用)
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC Kaggle Challenge で公開されているデータを使います。このデモでは、データは、Azure Blob（公開）からインポートします。:   
# MAGIC [Store Item Demand Forecasting Challenge](https://www.kaggle.com/competitions/demand-forecasting-kernels-only/data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備

# COMMAND ----------

# MAGIC %run ./01_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. RAWデータを生成してCSV出力する

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-1. sales.csv / 自動販売機売上

# COMMAND ----------

# DBTITLE 1,Azureストレージからcsvデータをロード
# WASBプロトコル定義
container = "komae"                                       # Azure コンテナ名
storage_account = "sajpstorage"                           # Azure ストレージアカウント名
file_path_train = "fine_grain_forecast/train"             # Azure ファイルパス（トレーニング）

# 公開Azure Storage Blobから学習データを取得します (WASBプロトコル)
train = spark.read.format('csv') \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(f'wasbs://{container}@{storage_account}.blob.core.windows.net/{file_path_train}/train.csv')

# COMMAND ----------

# from pyspark.sql.functions import col, row_number, expr, when, rand, lit
# from pyspark.sql.window import Window
# from pyspark.sql.types import StructType, StructField, DateType, IntegerType, LongType
# from pyspark.sql.functions import abs, ceil, sin, datediff

# # 既存のCSVファイルのスキーマ定義
# train_schema = StructType([
#     StructField('date', DateType()),      # 日付
#     StructField('store', IntegerType()),  # 店舗ID
#     StructField('item', IntegerType()),   # 商品ID
#     StructField('sales', IntegerType())   # 売上
# ])

# # ウィンドウ関数の定義（受注IDの生成用）
# window_spec = Window.orderBy("date", "store", "item")

# # データフレームの変換
# sales_df = (train
#     .select(
#         col("date").alias("order_date"),                            # 受注日
#         col("store").alias("vending_machine_id").cast(LongType()),  # 自動販売機ID
#         col("item").alias("item_id").cast(LongType()),              # 商品ID
#         col("sales").alias("sales_quantity").cast(LongType())       # 販売数
#     )
# )

# '''
# 概要：在庫数
# 詳細：季節性と商品カテゴリーを考慮した在庫計算
# 　　　・在庫切れの確率10%
# 　　　・商品IDを使用してABC分析に基づく分類を行い、それぞれ異なる在庫計算ロジックを適用
# 　　　　└ Aランク商品には季節性を導入し、sin関数を使用して年間の需要変動を模倣
# 　　　　└ BランクとCランク商品には、異なる乱数範囲を適用して変動性を持たせる
# '''
# sales_df = sales_df.withColumn(
#     "stock_quantity",
#     when(rand() < 0.1, 0)  # 10%の確率で在庫切れ
#     .otherwise(
#         when(col("item_id") % 3 == 0,  # Aランク商品
#             abs((col("sales_quantity") * (0.8 + sin(datediff(col("order_date"), lit("2024-01-01")) / 365 * 2 * 3.14159) * 0.5) + (rand() - 0.5) * 50)).cast(LongType())
#         ).when(col("item_id") % 3 == 1,  # Bランク商品
#             abs((col("sales_quantity") * (1.0 + rand() * 0.8) + (rand() - 0.5) * 30)).cast(LongType())
#         ).otherwise(  # Cランク商品
#             abs((col("sales_quantity") * (1.2 + rand() * 1.5) - ceil(rand() * 20))).cast(LongType())
#         )
#     )
# )

# # NULLレコードを追加する関数（修正版）
# def add_null_records(df, column_name, num_records=10):
#     # 元のデータフレームから10件のレコードを選択し、特定のカラムのみNULLにする
#     null_df = df.limit(num_records).withColumn(column_name, lit(None))
    
#     # 元のデータフレームと、NULLレコードを結合
#     return df.union(null_df)

# # NULLレコードを追加（それぞれ10件のみ）
# sales_df = add_null_records(sales_df, "order_date")
# sales_df = add_null_records(sales_df, "vending_machine_id")
# sales_df = add_null_records(sales_df, "item_id")

# # CSVファイルとして出力
# sales_df.coalesce(1).toPandas().to_csv(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/sales/sales.csv", index=False)

# # データの確認
# print(f"Total records: {sales_df.count()}")
# display(sales_df)

# COMMAND ----------

from pyspark.sql.functions import col, row_number, expr, when, rand
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, LongType
from pyspark.sql.functions import col, when, rand, sin, datediff, lit
from pyspark.sql.functions import abs, ceil

# 既存のCSVファイルのスキーマ定義
train_schema = StructType([
  StructField('date', DateType()),      # 日付
  StructField('store', IntegerType()),  # 店舗ID
  StructField('item', IntegerType()),   # 商品ID
  StructField('sales', IntegerType())   # 売上
])

# ウィンドウ関数の定義（受注IDの生成用）
window_spec = Window.orderBy("date", "store", "item")

# データフレームの変換
sales_df = (train
    .select(
        col("date").alias("order_date"),                            # 受注日
        col("store").alias("vending_machine_id").cast(LongType()),  # 自動販売機ID
        col("item").alias("item_id").cast(LongType()),              # 商品ID
        col("sales").alias("sales_quantity").cast(LongType())       # 販売数
    )
)

'''
概要：在庫数
詳細：季節性と商品カテゴリーを考慮した在庫計算
　　　・在庫切れの確率10%
　　　・商品IDを使用してABC分析に基づく分類を行い、それぞれ異なる在庫計算ロジックを適用
　　　　└ Aランク商品には季節性を導入し、sin関数を使用して年間の需要変動を模倣
　　　　└ BランクとCランク商品には、異なる乱数範囲を適用して変動性を持たせる
'''
sales_df = sales_df.withColumn(
    "stock_quantity",
    when(rand() < 0.1, 0)  # 10%の確率で在庫切れ
    .otherwise(
        when(col("item_id") % 3 == 0,  # Aランク商品
            abs((col("sales_quantity") * (0.8 + sin(datediff(col("order_date"), lit("2024-01-01")) / 365 * 2 * 3.14159) * 0.5) + (rand() - 0.5) * 50)).cast(LongType())
        ).when(col("item_id") % 3 == 1,  # Bランク商品
            abs((col("sales_quantity") * (1.0 + rand() * 0.8) + (rand() - 0.5) * 30)).cast(LongType())
        ).otherwise(  # Cランク商品
            abs((col("sales_quantity") * (1.2 + rand() * 1.5) - ceil(rand() * 20))).cast(LongType())
        )
    )
)

# CSVファイルとして出力
sales_df.coalesce(1).toPandas().to_csv(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/sales/sales.csv", index=False)

# データの確認
print(f"Total records: {sales_df.count()}")
# display(sales_df.limit(10))
display(sales_df)

# COMMAND ----------

from pyspark.sql.functions import lit, rand, when, date_add
from pyspark.sql.types import StructType, StructField, DateType, LongType, IntegerType

# NULLを含むデータのスキーマ定義
null_schema = StructType([
    StructField("order_date", DateType()),
    StructField("vending_machine_id", LongType()),
    StructField("item_id", LongType()),
    StructField("sales_quantity", LongType()),
    StructField("stock_quantity", LongType())
])

# NULLを含むデータの生成
null_data = spark.createDataFrame([], schema=null_schema)

# order_date NULL 10件
null_data = null_data.union(
    spark.range(10).select(
        lit(None).cast(DateType()).alias("order_date"),
        lit(11).cast(LongType()).alias("vending_machine_id"),
        (rand() * 50 + 51).cast(LongType()).alias("item_id"),
        lit(0).cast(LongType()).alias("sales_quantity"),
        lit(0).cast(LongType()).alias("stock_quantity")
    )
)

# item_id NULL 10件
null_data = null_data.union(
    spark.range(10).select(
        date_add(lit("2012-12-31"), -(rand() * 365 * 10).cast("int")).alias("order_date"),
        lit(11).cast(LongType()).alias("vending_machine_id"),
        lit(None).cast(LongType()).alias("item_id"),
        lit(0).cast(LongType()).alias("sales_quantity"),
        lit(0).cast(LongType()).alias("stock_quantity")
    )
)

# vending_machine_id NULL 10件
null_data = null_data.union(
    spark.range(10).select(
        date_add(lit("2012-12-31"), -(rand() * 365 * 10).cast("int")).alias("order_date"),
        lit(None).cast(LongType()).alias("vending_machine_id"),
        (rand() * 50 + 51).cast(LongType()).alias("item_id"),
        lit(0).cast(LongType()).alias("sales_quantity"),
        lit(0).cast(LongType()).alias("stock_quantity")
    )
)

# データをCSVファイルとして出力
null_data.coalesce(1).toPandas().to_csv(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/sales/sales_with_null.csv", header=True)

# データの確認
print(f"Total records with NULL: {null_data.count()}")
display(null_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-2. vending_machine_location.csv / 自販機設定場所マスタ

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, rand, concat, row_number
from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType
from pyspark.sql.window import Window

# ウィンドウ関数を定義
window_spec = Window.orderBy(lit(1))

# 自販機設定場所マスタのスキーマを定義
schema_vending_machine_locations = StructType([
    StructField("vending_machine_id", LongType(), False),
    StructField("location_type", StringType(), False),
    StructField("postal_code", StringType(), False),
    StructField("address", StringType(), False),
    StructField("pref", StringType(), False),
    StructField("city", StringType(), False),
    StructField("latitude", FloatType(), False),
    StructField("longitude", FloatType(), False)
])

# 設置場所データ
locations = [
    ("東京駅", "東京都千代田区丸の内1丁目", "100-0005", "東京都", "千代田区", 35.6809591, 139.7673068),
    ("渋谷駅", "東京都渋谷区道玄坂2丁目", "150-0043", "東京都", "渋谷区", 35.658034, 139.701635),
    ("新宿駅", "東京都新宿区新宿3丁目", "160-0022", "東京都", "新宿区", 35.689634, 139.700566),
    ("池袋駅", "東京都豊島区南池袋1丁目", "171-0022", "東京都", "豊島区", 35.733333, 139.710000),
    ("上野アメ横", "東京都台東区上野6丁目", "110-0005", "東京都", "台東区", 35.710622, 139.774200),
    ("原宿（ラフォーレ原宿）", "東京都渋谷区神宮前1丁目11-6", "150-0001", "東京都", "渋谷区", 35.670167, 139.702708),
    ("有明ガーデン", "東京都江東区有明2丁目1-8", "135-0063", "東京都", "江東区", 35.634167, 139.786667),
    ("六本木ヒルズ", "東京都港区六本木6丁目10-1", "106-6108", "東京都", "港区", 35.658581, 139.730779),
    ("秋葉原駅", "東京都千代田区外神田1丁目", "101-0021", "東京都", "千代田区", 35.698342, 139.774703),
    ("中目黒（東急ストア前）", "東京都目黒区上目黒1丁目21-12", "153-0051", "東京都", "目黒区", 35.646167, 139.694167)
]

# 設置タイプ
location_types = ["Office", "Station", "Shopping_Mall"]

# 自販機設定場所マスタのデータを生成
vending_machine_locations_df = (
    spark.createDataFrame(locations, ["location_name", "address", "postal_code", "pref", "city", "latitude", "longitude"])
    .withColumn("vending_machine_id", row_number().over(window_spec))
    .withColumn("location_type", when(col("location_name").isin("東京駅", "渋谷駅", "新宿駅", "池袋駅", "秋葉原駅"), "Station")
                                .when(col("location_name").isin("上野アメ横", "原宿（ラフォーレ原宿）", "有明ガーデン", "六本木ヒルズ", "中目黒（東急ストア前）"), "Shopping_Mall")
                                .otherwise("Office"))
    .select("vending_machine_id", "location_type", "postal_code", "address", "pref", "city", "latitude", "longitude")
)

# CSVファイルとして出力
vending_machine_locations_df.coalesce(1).toPandas().to_csv(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/vending_machine_location/vending_machine_location.csv", index=False)

# レコード数とサンプルデータの表示
print(f"Total records: {vending_machine_locations_df.count()}")
display(vending_machine_locations_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-3. date_master.csv / 日付マスタ

# COMMAND ----------

from pyspark.sql.functions import col, date_format, year, month, dayofweek, when, lit
from pyspark.sql.types import StructType, StructField, DateType, StringType, LongType
from datetime import date, timedelta

# 日付マスタのスキーマを定義
schema_date_master = StructType([
    StructField("date", DateType(), False),           # 日付
    StructField("day_of_week", StringType(), False),  # 曜日
    StructField("month", StringType(), False),        # 月
    StructField("quarter", StringType(), False),      # 四半期
    StructField("year", StringType(), False),         # 年
    StructField("is_holiday", LongType(), False)      # 祝日フラグ
])

# 日付範囲を生成
start_date = date(2013, 1, 1)
end_date = date(2018, 12, 31)
date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

# 日本の祝日リスト（2013年から2018年まで）
holidays = [
    # 2013年の祝日
    "2013-01-01", "2013-01-14", "2013-02-11", "2013-03-20", "2013-04-29", "2013-05-03", "2013-05-04", "2013-05-05", "2013-07-15", "2013-09-16", "2013-09-23", "2013-10-14", "2013-11-03", "2013-11-23", "2013-12-23",
    # 2014年の祝日
    "2014-01-01", "2014-01-13", "2014-02-11", "2014-03-21", "2014-04-29", "2014-05-03", "2014-05-04", "2014-05-05", "2014-07-21", "2014-09-15", "2014-09-23", "2014-10-13", "2014-11-03", "2014-11-23", "2014-12-23",
    # 2015年の祝日
    "2015-01-01", "2015-01-12", "2015-02-11", "2015-03-21", "2015-04-29", "2015-05-03", "2015-05-04", "2015-05-05", "2015-07-20", "2015-09-21", "2015-09-22", "2015-09-23", "2015-10-12", "2015-11-03", "2015-11-23", "2015-12-23",
    # 2016年の祝日
    "2016-01-01", "2016-01-11", "2016-02-11", "2016-03-20", "2016-04-29", "2016-05-03", "2016-05-04", "2016-05-05", "2016-07-18", "2016-08-11", "2016-09-19", "2016-09-22", "2016-10-10", "2016-11-03", "2016-11-23", "2016-12-23",
    # 2017年の祝日
    "2017-01-01", "2017-01-02", "2017-01-09", "2017-02-11", "2017-03-20", "2017-04-29", "2017-05-03", "2017-05-04", "2017-05-05", "2017-07-17", "2017-08-11", "2017-09-18", "2017-09-23", "2017-10-09", "2017-11-03", "2017-11-23", "2017-12-23",
    # 2018年の祝日
    "2018-01-01", "2018-01-08", "2018-02-11", "2018-02-12", "2018-03-21", "2018-04-29", "2018-04-30", "2018-05-03", "2018-05-04", "2018-05-05", "2018-07-16", "2018-08-11", "2018-09-17", "2018-09-23", "2018-09-24", "2018-10-08", "2018-11-03", "2018-11-23", "2018-12-23", "2018-12-24"
]

# 日付マスタのデータを生成
date_master_df = (
    spark.createDataFrame([(d,) for d in date_range], ["date"])
    .withColumn("day_of_week", date_format("date", "EEEE"))
    .withColumn("month", date_format("date", "MMMM"))
    .withColumn("year", year("date").cast("string"))
    .withColumn("quarter", when((month("date") >= 1) & (month("date") <= 3), "Q1")
                           .when((month("date") >= 4) & (month("date") <= 6), "Q2")
                           .when((month("date") >= 7) & (month("date") <= 9), "Q3")
                           .otherwise("Q4"))
    .withColumn("is_holiday", when(col("date").cast("string").isin(holidays), 1).otherwise(0))
)

# CSVファイルとして出力
date_master_df.coalesce(1).toPandas().to_csv(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/date_master/date_master.csv", index=False)

# レコード数とサンプルデータの表示
print(f"Total records: {date_master_df.count()}")
display(date_master_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-4. items.csv / 商品マスタ

# COMMAND ----------

import pandas as pd
import numpy as np

# 商品データの定義
items = [
    ("コカ・コーラ", "炭酸飲料", 160),
    ("コカ・コーラ ゼロ", "炭酸飲料", 160),
    ("ファンタ オレンジ", "炭酸飲料", 160),
    ("ファンタ グレープ", "炭酸飲料", 160),
    ("スプライト", "炭酸飲料", 160),
    ("ジョージア エメラルドマウンテンブレンド", "コーヒー", 130),
    ("ジョージア ブラック", "コーヒー", 130),
    ("ジョージア カフェ・オ・レ", "コーヒー", 130),
    ("ジョージア ザ・プレミアム", "コーヒー", 150),
    ("ボス ブラック", "コーヒー", 130),
    ("ボス カフェオレ", "コーヒー", 130),
    ("綾鷹", "お茶", 150),
    ("綾鷹 茶葉のあまみ", "お茶", 150),
    ("爽健美茶", "お茶", 150),
    ("からだすこやか茶W", "お茶", 150),
    ("い・ろ・は・す", "水", 110),
    ("い・ろ・は・す スパークリング", "水", 130),
    ("アクエリアス", "スポーツドリンク", 150),
    ("アクエリアス ゼロ", "スポーツドリンク", 150),
    ("ポカリスエット", "スポーツドリンク", 150),
    ("リアルゴールド", "エナジードリンク", 180),
    ("リアルゴールド フォーカスショット", "エナジードリンク", 180),
    ("ジョージア ミルクコーヒー", "コーヒー", 130),
    ("ジョージア マックスコーヒー", "コーヒー", 130),
    ("ジョージア ラテニスタ", "コーヒー", 150),
    ("ミニッツメイド オレンジブレンド", "果汁飲料", 160),
    ("ミニッツメイド アップル100%", "果汁飲料", 160),
    ("爽健美茶 健康素材の麦茶", "お茶", 150),
    ("太陽のマテ茶", "お茶", 150),
    ("ザ・タンサン ストロング", "炭酸水", 130),
    ("ザ・タンサン レモン", "炭酸水", 130),
    ("ジョージア ジャパン クラフトマン", "コーヒー", 150),
    ("ジョージア ショット&ブレイク", "コーヒー", 130),
    ("綾鷹 特選茶", "お茶", 160),
    ("綾鷹 カフェ 抹茶ラテ", "お茶", 160),
    ("からだすこやか茶W 黒豆ブレンド", "お茶", 150),
    ("い・ろ・は・す 天然水", "水", 110),
    ("い・ろ・は・す みかん", "水", 130),
    ("アクエリアス エスボディ", "スポーツドリンク", 150),
    ("コカ・コーラ ゼロカフェイン", "炭酸飲料", 160),
    ("ファンタ レモン", "炭酸飲料", 160),
    ("ジョージア グラン 微糖", "コーヒー", 150),
    ("ジョージア ヨーロピアン", "コーヒー", 150),
    ("綾鷹 濃い緑茶", "お茶", 160),
    ("爽健美茶 すっきりブレンド茶", "お茶", 150),
    ("い・ろ・は・す なし", "水", 130),
    ("アクエリアス ビタミンガード", "スポーツドリンク", 150),
    ("リアルゴールド ウルトラチャージ", "エナジードリンク", 180),
    ("ミニッツメイド 朝バナナ", "果汁飲料", 160),
    ("ザ・タンサン 強炭酸", "炭酸水", 130)
]

# DataFrameの作成
df = pd.DataFrame(items, columns=['item_name', 'category_name', 'unit_price'])
df['item_id'] = range(1, len(df) + 1)
df['unit_cost'] = (df['unit_price'] * 0.6).astype(int)  # 原価を売価の60%と仮定

# 列の順序を調整
df = df[['item_id', 'item_name', 'category_name', 'unit_price', 'unit_cost']]

# SparkDataFrameに変換
items_df = spark.createDataFrame(df)

# CSVファイルとして出力
items_df.coalesce(1).toPandas().to_csv(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/items/items.csv", index=False)

# データの確認
print(f"Total records: {items_df.count()}")
display(items_df)
