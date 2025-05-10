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

# 顧客毎の上位30件のレコメンデーションアイテムを取得する
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
  INNER  JOIN latest_pre_survey lps ON fb.user_id = lps.user_id          -- 渡航前アンケート回答必須
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
  INNER  JOIN latest_pre_survey lps ON fb.user_id = lps.user_id          -- 渡航前アンケート回答必須
  INNER  JOIN user_items         ui ON fb.user_id = ui.user_id           -- ALS 必須
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
df.createOrReplaceTempView("v_recom_top6")

# 結果を確認
print(df.count())      # 搭乗者数 × 6 付近になるはず
print(df.columns)
display(df.limit(100))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2. Volumeにある画像をBase64エンコード -> 圧縮後 -> Goldテーブル作成
# MAGIC Volumeの画像データを読み込み、エンコードしてからApps表示するのは時間がかかる。  
# MAGIC Appsから画像読み込みを高速化させるために、Volume → Base64 変換 → gd_recom_top6 へテーブル保存。

# COMMAND ----------

from pyspark.sql import functions as F, types as T
from PIL import Image
import base64, io

# 1) v_recom_top6 を取得
df_top6 = spark.table("v_recom_top6")          # /Volumes/... 形式

# 2) 重複のないパスをリスト化し、binaryFile で読める形に "dbfs:" を付ける
paths = [r.content_img_url for r in df_top6.select("content_img_url").distinct().collect()]
paths_dbfs = [p if p.startswith("dbfs:/") else f"dbfs:{p}" for p in paths]

# 3) UDF: 300px リサイズ → Base64
@F.udf(returnType=T.StringType())
def to_b64(binary):
    img = Image.open(io.BytesIO(binary))
    ratio = 300 / img.width
    img = img.resize((300, int(img.height * ratio)), Image.Resampling.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True, quality=70)
    return base64.b64encode(buf.getvalue()).decode("utf8")

# 4) Volume から読み込み & パスを /Volumes/... 形式に戻しておく
df_b64 = (
    spark.read.format("binaryFile").load(paths_dbfs)
         .select(
             F.regexp_replace("path", r"^dbfs:", "").alias("content_img_url"),  # ← キーをそろえる
             to_b64("content").alias("content_img_b64")
         )
)

# 5) JOIN で置き換え
df_final = (
    df_top6.alias("t")
      .join(df_b64.alias("b"), on="content_img_url", how="left")
      .drop("content_img_url")
)

# 6) 一時ビューとして登録（必要ならテーブル化）
df_final.createOrReplaceTempView("v_recom_top6_bs64")

# # 6) Delta テーブルに保存（上書き）
# TABLE = f"{MY_CATALOG}.{MY_SCHEMA}.sv_recom_top6_bs64"
# (df_final.write.format("delta")
#         .mode("overwrite")
#         .option("comment", "TOP-6 (画像はBase64, 行レイアウト)")
#         .saveAsTable(TABLE))

print(df_final.columns)
print(df_final.count())
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3. レコメンドTOP6を配列に変換

# COMMAND ----------

df = spark.sql(f'''
-- `v_recom_top6_bs64` のレコメンドデータをJSON配列に加工
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
      collect_list(content_img_b64) AS content_img_b64
    ) AS contents_list
  FROM
    v_recom_top6_bs64
  GROUP BY ALL
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
  .saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6_bs64")

# 結果を表示
print("レコード数:", df.count())
print("カラム名:", df.columns)
display(df.limit(10))


# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6_bs64'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_gd_recom_top6_bs64'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'user_id',
    'flight_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH}
DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (user_id, flight_id);
''')

# CDFの有効化
spark.sql(f"""
ALTER TABLE {TABLE_PATH}
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
""")

# # OPTIMIZE(推奨)
# # 大規模テーブルでOPTIMIZEを実行しない場合、オンラインテーブルとの初回同期に時間がかかる可能性があるため
# spark.sql(f"OPTIMIZE {TABLE_PATH}")

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,認定済みタグの追加
certified_tag = 'system.Certified'

try:
    spark.sql(f"ALTER TABLE {TABLE_PATH} SET TAGS ('{certified_tag}')")
    print(f"認定済みタグ '{certified_tag}' の追加が完了しました。")

except Exception as e:
    print(f"認定済みタグ '{certified_tag}' の追加中にエラーが発生しました: {str(e)}")
    print("このエラーはタグ機能に対応していないワークスペースで実行した場合に発生する可能性があります。")

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブルコメント
comment = """
テーブル名：`gd_recom_top6_bs64 / 機内レコメンドTOP6（施策用マート）`  
説明：航空サービスの機内エンタメコンテンツのレコメンドコンテンツTOP6です。  
過去視聴ログを学習したALSモデルで会員ごとに機内コンテンツレコメンドを予測。さらに渡航前アンケートの回答に一致するコンテンツに絞った上で、TOP6コンテンツをレコメンドリストとして登録。プッシュ配信や機内ディスプレイに表示するコンテンツ一覧の元データとして利用します。
"""
spark.sql(f'COMMENT ON TABLE {TABLE_PATH} IS "{comment}"')

# カラムコメント
column_comments = {
    "user_id": "会員ID、主キー、例）1",
    "booking_id": "航空券予約番号、例）B9345721",
    "flight_id": "便名（機材＋日付で一意）、主キー、例）JL006",
    "route_id":"区間、例）NYC-NRT",
    "flight_date":"出発日、YYYY-MM-DDフォーマット",
    "contents_list":"機内コンテンツリスト、会員ごとにおすすめのコンテンツカテゴリと画像URL(今回デモなのでVolumeパス)を6件ずつ格納します"
}

for column, comment in column_comments.items():
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {TABLE_PATH} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)
