# Databricks notebook source
# MAGIC %md
# MAGIC # モデルでベストレコメンドを取得
# MAGIC - Unity Catalogに登録されたモデルを使ってお客様ごとのレコメンドリストTOP10を取得します。さらにお客様の座席に最も近い店舗のアイテムをベストレコメンドとしてGoldテーブルを作ります。
# MAGIC - DBR 16.0ML以降

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC 該当スキーマ配下の既存goldテーブルを全て削除

# COMMAND ----------

# # スキーマ内のすべてのテーブル名を取得する
# tables_df = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}")

# # テーブル名が "bronze_" で始まるテーブルのみ削除する
# for table in tables_df.collect():
#     table_name = table["tableName"]
#     if table_name.startswith("gd_"):
#         spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{table_name}")
#         print(f"削除されたテーブル: {table_name}")

# print("全ての gd_で始まるテーブルが削除されました。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 推論 - モデルでレコメンデーションリスト作成
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-4.png" width="1000px">
# MAGIC
# MAGIC これでUnity Catalogモデルレジストリにモデルが登録されたので、本番のパイプラインで使い始めることができます。
# MAGIC
# MAGIC あとはUnity Catalogからロードして推論を適用するだけです。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-1. Unity Catalogに登録したモデルをロードします

# COMMAND ----------

#                                                               　           Alias for production
#                                        Model name                              |
#                                             |                                  |
model = mlflow.spark.load_model(f"models:/{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}@prod")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-2. モデルを使って顧客ごとにレコメンデーションアイテムTOP10を推論・取得

# COMMAND ----------

# 顧客毎の上位10件のレコメンデーションアイテムを取得する
recommendations = model.stages[0].recommendForAllUsers(10)

# create table
recommendations.createOrReplaceTempView("sv_recommendations_v")

display(recommendations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 顧客毎のベストアイテムを取得 - Goldテーブル作成
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-5.png" width="1000px">

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-1. 顧客毎のレコメンドTOP10を展開し、アイテムや顧客の詳細を取得
# MAGIC

# COMMAND ----------

gold_recommendations_df = spark.sql(f'''
WITH exploded_recommendations AS (
  SELECT 
    customer_id, 
    explode(recommendations) AS items
  FROM 
    sv_recommendations_v
),
customer_items AS (
  SELECT 
    customer_id,
    items.*  -- item_id, predictionなどのカラムを展開
  FROM 
    exploded_recommendations
)
SELECT 
  ci.customer_id,
  ts.customer_name,
  ts.phone_number,
  ci.item_id,
  ci.rating,
  v.vendor_id,
  v.vendor_location_number,
  v.vendor_name,
  v.vendor_scope,
  v.section,
  v.item_type,
  v.item,
  v.price,
  v.error,
  v.item_img_url
FROM {MY_CATALOG}.{MY_SCHEMA}.bz_stadium_vendors AS v
INNER JOIN customer_items AS ci ON v.item_id = ci.item_id
INNER JOIN {MY_CATALOG}.{MY_SCHEMA}.bz_ticket_sales AS ts ON ci.customer_id = ts.customer_id
''')

# create table
gold_recommendations_df.createOrReplaceTempView("gd_recommendations_v")

print(gold_recommendations_df.count())
print(gold_recommendations_df.columns)
display(gold_recommendations_df.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2. 顧客毎のレコメンドアイテムTOP10を取得
# MAGIC
# MAGIC アイテムのパーソナライゼドレコメンドを行う時に、近い距離で購入できるアイテムも探したいです。そのために、お客様の席からお店の場所までの距離を計算します。

# COMMAND ----------

sections_recommendations_df = spark.sql(f"""
SELECT
  r.customer_id,
  r.customer_name,
  r.phone_number,
  r.vendor_name,
  item_id,
  item,
  r.item_img_url,
  rating,
  section,
  section_number,
  abs(section-section_number) AS distance
FROM gd_recommendations_v r
JOIN {MY_CATALOG}.{MY_SCHEMA}.bz_ticket_sales s ON s.customer_id = r.customer_id
""")

# create table
spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.gd_sections_recommendations")
sections_recommendations_df.write.saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.gd_sections_recommendations")

print(sections_recommendations_df.count())
print(sections_recommendations_df.columns)
display(sections_recommendations_df.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ↓デモ用に`03_train_model`ノートブックがうまくいかない場合のチート

# COMMAND ----------

# DBTITLE 1,gd_recommendations
from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType

schema_items = StructType([
    StructField("customer_id", LongType(), False),            # 会員ID
    StructField("customer_name", StringType(), False),        # 会員氏名
    StructField("phone_number", StringType(), False),         # 会員電話番号
    StructField("item_id", LongType(), False),                # 商品ID
    StructField("rating", FloatType(), False),                # ALSレコメンドレート
    StructField("vendor_id", LongType(), False),              # ベンダーID
    StructField("vendor_location_number", LongType(), False), # ベンダーロケーション番号
    StructField("vendor_name", StringType(), False),          # ベンダー名
    StructField("vendor_scope", StringType(), False),         # ベンダースコープ
    StructField("section", LongType(), False),                # セクション
    StructField("item_type", StringType(), False),            # 商品タイプ
    StructField("item", StringType(), False),                 # 商品名
    StructField("price", FloatType(), False),                 # 価格
    StructField("error", StringType(), True),                 # エラー
    StructField("item_img_url", StringType(), True),          # 画像URL
])

gd_recommend_df = spark.read.csv(
    f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_TMP}/gd_recommendations/gd_recommendations.csv",
    header=True,
    schema=schema_items)

# create table
# spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.gd_recommendations")
# gd_recommend_df.write.saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.gd_recommendations")
gd_recommend_df.write.saveAsTable(f"komae_demo_v2.{MY_SCHEMA}.gd_recommendations")

print(gd_recommend_df.count())
print(gd_recommend_df.columns)
display(gd_recommend_df.limit(100))

# COMMAND ----------

# DBTITLE 1,gd_final_recommendations
from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType

schema_items = StructType([
    StructField("customer_id", LongType(), False),            # 会員ID
    StructField("customer_name", StringType(), False),        # 会員氏名
    StructField("phone_number", StringType(), False),         # 会員電話番号
    StructField("vendor_name", StringType(), False),          # ベンダー名
    StructField("item_id", LongType(), False),                # 商品ID
    StructField("item", StringType(), False),                 # 商品名
    StructField("item_img_url", StringType(), True),          # 画像URL
    StructField("rating", FloatType(), False),                # ALSレコメンドレート
    StructField("section", LongType(), False),                # セクション
    StructField("section_number", LongType(), False),         # セクション番号
    StructField("distance", LongType(), False),               # 距離
    StructField("rnk", StringType(), True),                   # ランク
])

gd_final_df = spark.read.csv(
    f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_TMP}/gd_final_recommendations/gd_final_recommendations.csv",
    header=True,
    schema=schema_items)

# create table
# spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.gd_final_recommendations")
# gd_final_df.write.saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.gd_final_recommendations")
gd_final_df.write.saveAsTable(f"komae_demo_v2.{MY_SCHEMA}.gd_final_recommendations")

print(gd_final_df.count())
print(gd_final_df.columns)
display(gd_final_df.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3. 顧客毎の距離の最も近いベストアイテムを特定
# MAGIC レコメンドアイテムTOP10のうち、お客様の席から最も近いお店のアイテムをベストアイテムとして抽出します。

# COMMAND ----------

final_recommendations_df = spark.sql(f"""
SELECT * FROM (
    SELECT
      -- *,
      customer_id,
      customer_name,
      phone_number,
      vendor_name,
      item_id,
      item,
      item_img_url,
      rating,
      section,
      section_number,
      distance,
      RANK() OVER (PARTITION BY customer_id ORDER BY distance ASC) AS rnk
    FROM {MY_CATALOG}.{MY_SCHEMA}.gd_sections_recommendations
  ) WHERE rnk = 1
""")

# create table
spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.gd_final_recommendations")
final_recommendations_df.write.saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.gd_final_recommendations")

print(final_recommendations_df.count())
print(final_recommendations_df.columns)
display(final_recommendations_df.limit(10))
