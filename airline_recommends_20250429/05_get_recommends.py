# Databricks notebook source
# MAGIC %md
# MAGIC # モデルでベストレコメンドを取得
# MAGIC - Unity Catalogに登録されたモデルを使ってお客様ごとのレコメンドリストTOP30を取得します。
# MAGIC - さらにお客様の座席に最も近い店舗のアイテムをベストレコメンドとしてGoldテーブルを作ります。
# MAGIC - DBR 16.0ML以降

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC 該当スキーマ配下の既存goldテーブルを全て削除

# COMMAND ----------

# # スキーマ内のすべてのテーブル名を取得する
# tables_df = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}")

# # テーブル名が "gd_" で始まるテーブルのみ削除する
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
# MAGIC Unity Catalogモデルレジストリにモデルが登録されたので、本番のパイプラインで使い始めることができます。  
# MAGIC あとはUnity Catalogからロードして推論を適用するだけです。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-1. Unity Catalogに登録したモデルをロードします

# COMMAND ----------

# import mlflow
# from mlflow import MlflowClient

# # Unity Catalogを参照するように設定（必須）
# mlflow.set_registry_uri("databricks-uc")

# client = MlflowClient()
# alias_info = client.get_model_version_by_alias(
#     name=f"{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}",
#     alias="prod"
# )
# print(alias_info)

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")

#                                                               　           Alias for production
#                                        Model name                              |
#                                             |                                  |
model = mlflow.spark.load_model(f"models:/{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}@prod")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-2. モデルを使って顧客ごとにレコメンデーションアイテムTOP30を推論・取得

# COMMAND ----------

from pyspark.ml.recommendation import ALSModel

# PipelineModelからALSModelを抽出
als_model = None
for stage in model.stages:
    if isinstance(stage, ALSModel):
        als_model = stage
        break

if als_model is None:
    raise ValueError("ALSモデルが見つかりませんでした")

# COMMAND ----------

# 顧客毎の上位20件のレコメンデーションアイテムを取得する
recommendations = als_model.recommendForAllUsers(30)

# create table
recommendations.createOrReplaceTempView("v_recom_top30")

display(recommendations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 顧客毎のベストアイテムを取得 - Goldテーブル作成

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-1. レコメンドTOP30を展開し、渡航アンケートで答えたコンテンツカテゴリを優先してTOP6を抽出
# MAGIC - 渡航アンケートの回答が「映画、ドラマ」: レコメンドアイテムTOP30から「映画、ドラマ」に絞ってレコメンド6つ抽出します
# MAGIC - 渡航アンケートの回答が「映画、ドラマ」以外: レコメンドアイテムTOP30から「映画、ドラマ」以外に絞ってレコメンド6つ抽出します
# MAGIC

# COMMAND ----------

df = spark.sql(f'''
/* ============================================================
   最新日の予約者に対し
   ① 渡航前アンケート回答あり
   ② ALS レコメンド結果あり
   のユーザーへ TOP-6 レコメンドを返す
============================================================ */

WITH
-- ---------- 最新日フライト予約者 ----------
latest_flights AS (
  /* sv_flight_booking で最も新しい flight_date に運航する便一覧 */
  SELECT DISTINCT
         flight_id,
         flight_date
  FROM   {MY_CATALOG}.{MY_SCHEMA}.sv_flight_booking
  WHERE  flight_date = (
           SELECT MAX(flight_date)
           FROM   {MY_CATALOG}.{MY_SCHEMA}.sv_flight_booking
         )
),

flight_booking AS (
  /* 最新日に搭乗する会員だけを 1 行に */
  SELECT
    user_id,
    booking_id,
    flight_id,
    route_id,
    flight_date
  FROM (
    SELECT
      fb.*,
      ROW_NUMBER() OVER (PARTITION BY fb.user_id
                         ORDER BY fb.flight_date DESC, fb.booking_id) AS rn
    FROM   {MY_CATALOG}.{MY_SCHEMA}.sv_flight_booking fb
    INNER  JOIN latest_flights lf
           ON fb.flight_id  = lf.flight_id
          AND fb.flight_date = lf.flight_date
  )
  WHERE rn = 1
),

-- ---------- 最新アンケート（必須） ----------
latest_pre_survey AS (
  SELECT user_id,
         trip_purpose,
         content_category,
         answer_date
  FROM (
    SELECT ps.*,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY answer_date DESC) AS rn
    FROM   {MY_CATALOG}.{MY_SCHEMA}.sv_pre_survey ps
  )
  WHERE rn = 1
),

-- ---------- ALS レコメンド 30 件（必須） ----------
user_items AS (
  SELECT
    user_id,
    items.content_id,
    items.rating
  FROM (
    SELECT
      user_id,
      explode(recommendations) AS items     -- 行展開
    FROM   v_recom_top30
  )
),

-- ---------- IFE 視聴ログ（画像 URL 用／重複排除） ----------
ife_logs_dedup AS (
  SELECT
    pl.user_id,
    pl.content_id,
    pl.content_img_url,
    ROW_NUMBER() OVER (
      PARTITION BY pl.user_id, pl.content_id
      ORDER BY pl.play_end_at DESC
    ) AS rn
  FROM   {MY_CATALOG}.{MY_SCHEMA}.sv_ife_play_logs pl
  INNER  JOIN flight_booking fb
         ON pl.user_id  = fb.user_id
        AND pl.flight_id = fb.flight_id
),

-- ---------- コンテンツマスタ ----------
content_info AS (
  SELECT
    content_id,
    content_category,
    content_img_url
  FROM   {MY_CATALOG}.{MY_SCHEMA}.sv_ife_contents
),

-- ---------- 映画・ドラマカテゴリのレコメンド ----------
movie_drama_recommendations AS (
  SELECT
    ui.user_id,
    fb.booking_id,
    fb.flight_id,
    fb.route_id,
    fb.flight_date,
    ui.content_id,
    ui.rating,
    ci.content_category,
    COALESCE(ild.content_img_url, ci.content_img_url) AS content_img_url,
    ROW_NUMBER() OVER (PARTITION BY ui.user_id ORDER BY ui.rating DESC) AS rn
  FROM   flight_booking      fb
  INNER  JOIN latest_pre_survey lps ON fb.user_id = lps.user_id          -- 回答必須
  INNER  JOIN user_items         ui ON fb.user_id = ui.user_id           -- ALS 必須
  LEFT   JOIN content_info       ci ON ui.content_id = ci.content_id
  LEFT   JOIN (
           SELECT user_id, content_id, content_img_url
           FROM   ife_logs_dedup
           WHERE  rn = 1
         ) ild
         ON ui.user_id    = ild.user_id
        AND ui.content_id = ild.content_id
  WHERE  lps.content_category = '映画、ドラマ'
    AND  ci.content_category  = '映画、ドラマ'
),

-- ---------- 上記以外のカテゴリ ----------
non_movie_drama_recommendations AS (
  SELECT
    ui.user_id,
    fb.booking_id,
    fb.flight_id,
    fb.route_id,
    fb.flight_date,
    ui.content_id,
    ui.rating,
    ci.content_category,
    COALESCE(ild.content_img_url, ci.content_img_url) AS content_img_url,
    ROW_NUMBER() OVER (PARTITION BY ui.user_id ORDER BY ui.rating DESC) AS rn
  FROM   flight_booking      fb
  INNER  JOIN latest_pre_survey lps ON fb.user_id = lps.user_id
  INNER  JOIN user_items         ui ON fb.user_id = ui.user_id
  LEFT   JOIN content_info       ci ON ui.content_id = ci.content_id
  LEFT   JOIN (
           SELECT user_id, content_id, content_img_url
           FROM   ife_logs_dedup
           WHERE  rn = 1
         ) ild
         ON ui.user_id    = ild.user_id
        AND ui.content_id = ild.content_id
  WHERE  lps.content_category != '映画、ドラマ'
    AND  ci.content_category  != '映画、ドラマ'
)

-- ---------- ユーザーごとに最大 6 件返却 ----------
SELECT
  user_id,
  booking_id,
  flight_id,
  route_id,
  flight_date,
  content_id,
  rating,
  content_category,
  content_img_url
FROM   movie_drama_recommendations
WHERE  rn <= 6

UNION ALL

SELECT
  user_id,
  booking_id,
  flight_id,
  route_id,
  flight_date,
  content_id,
  rating,
  content_category,
  content_img_url
FROM   non_movie_drama_recommendations
WHERE  rn <= 6      -- TOP6に絞る
''')

# 一時ビューとして登録（必要ならテーブル化）
df.createOrReplaceTempView("gd_recom_top6_v")

# 結果を確認
print(df.count())      # 搭乗者数 × 6 付近になるはず
print(df.columns)
display(df.limit(100))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2. レコメンドTOP6を配列にしてGoldテーブル作成

# COMMAND ----------

df = spark.sql(f'''
-- `gd_recom_top6_v` のレコメンドデータをJSON配列に加工
WITH processed_recommendations AS (
  SELECT
    user_id,
    booking_id,
    flight_id,
    route_id,
    flight_date,
    -- 映画、ドラマカテゴリとその画像URLをJSON形式に
    STRUCT(
      collect_list(content_category) AS content_category,
      collect_list(content_img_url) AS content_img_url
    ) AS contents_list
  FROM
    gd_recom_top6_v
  GROUP BY
    user_id, booking_id, flight_id, route_id, flight_date
)

-- 結果を格納
SELECT
  user_id,
  booking_id,
  flight_id,
  route_id,
  flight_date,
  contents_list
FROM
  processed_recommendations
''')

# create table
df.write.format("delta")\
  .option("comment", "レコメンド結果 TOP-6")\
  .mode("overwrite")\
  .saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6")

# NOT NULL制約の追加
columns_to_set_not_null = ['user_id', 'flight_id']
for column in columns_to_set_not_null:
  spark.sql(f"""
  ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6
  ALTER COLUMN {column} SET NOT NULL;
""")

# 主キーの設定
spark.sql(f"""
ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6
ADD CONSTRAINT gd_recom_top6_pk PRIMARY KEY(user_id, flight_id);
""")

# CDFの有効化
spark.sql(f"""
ALTER TABLE {MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6 
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
""")

# OPTIMIZE(推奨)：大規模テーブルでOPTIMIZEを実行しない場合、オンラインテーブルとの初回同期に時間がかかる可能性があるため
spark.sql(f"""
OPTIMIZE {MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6;
""")


# 結果を表示
print("レコード数:", df.count())
print("カラム名:", df.columns)
display(df.limit(100))

