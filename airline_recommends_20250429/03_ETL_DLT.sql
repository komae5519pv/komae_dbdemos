-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 自動販売機の需要予測・分析デモデータを作成
-- MAGIC ## やること
-- MAGIC - csvを読み込んで、bronzeテーブルおよび需要予測モデルのトレーニングデータセットを作ります、本ノートブックを上から下まで流してください
-- MAGIC - クラスタはDBR15.4 LTS or DBR15.4 LTS ML以降で実行してください
-- MAGIC - [テーブル定義書](https://docs.google.com/spreadsheets/d/1_h4mpPROH2VJQja3oHIIfhFWIGfDYoSoMsY31HKsOTA/edit?gid=1392031218#gid=1392031218)に基づくテーブルを作ります
-- MAGIC
-- MAGIC <!-- <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/demand_forcast_20250426/_image_for_notebook/01.%20DLT_Pipeline.png?raw=true' width='90%'/> -->

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. パイプライン手動設定
-- MAGIC パイプライン > パイプライン設定
-- MAGIC 一般
-- MAGIC ```
-- MAGIC パイプライン名: <お名前>_airline_recomments_dlt
-- MAGIC パイプラインモード: トリガー（バッチ処理）
-- MAGIC ```
-- MAGIC
-- MAGIC Advanced
-- MAGIC ```
-- MAGIC catalog: <使用中のカタログ名を指定してください>
-- MAGIC schema : airline_recommends
-- MAGIC volume : raw_data
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Bronze＋Silverテーブルを作る

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2-1. フライト予約履歴 / flight_booking

-- COMMAND ----------

-- Step1: Bronze　(データ取り込み + タイムスタンプ付与)
CREATE OR REFRESH STREAMING LIVE TABLE bz_flight_booking
AS
SELECT
  CAST(booking_id   AS STRING)        AS booking_id,
  CAST(user_id      AS BIGINT)        AS user_id,
  flight_id,
  route_id,
  CAST(flight_date  AS DATE)          AS flight_date,
  CAST(fare_amount  AS DECIMAL(10,2)) AS fare_amount,
  _rescued_data,
  current_timestamp()                 AS cdc_timestamp
FROM cloud_files(
  '/Volumes/${catalog}/${schema}/${volume}/flight_booking',
  'csv',
  map('header','true','inferSchema','true','cloudFiles.inferColumnTypes','true')
);

-- Step2：データ読み込み＋スキーマ宣言＋期待値チェック
CREATE STREAMING LIVE TABLE sv_flight_booking (
  booking_id    STRING        COMMENT "航空券予約番号、例）B9345721",
  user_id       BIGINT        COMMENT "会員ID、例）1",
  flight_id     STRING        COMMENT "便名（機材＋日付で一意）、例）JL006",
  route_id      STRING        COMMENT "区間、例）NYC-NRT",
  flight_date   DATE          COMMENT "出発日、YYYY-MM-DDフォーマット",
  fare_amount   DECIMAL(10,2) COMMENT "運賃、例: 1250.00",
  _rescued_data STRING        COMMENT "Auto Loader がスキーマ不一致（新規列・型不正値など）を検知したときにその『はみ出しデータ』を JSON 文字列で一時退避する救済用カラム",
  CONSTRAINT booking_id_not_null EXPECT (booking_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT user_id_not_null EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT flight_id_not_null EXPECT (flight_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "`sv_flight_booking`：旅客機のフライト予約履歴（Silver）";

-- Step3：CDC マージ (SCD Type-1 = 最新状態だけを保持)
APPLY CHANGES INTO LIVE.sv_flight_booking
FROM STREAM(LIVE.bz_flight_booking)
KEYS (booking_id, user_id, flight_id)
SEQUENCE BY cdc_timestamp
COLUMNS * EXCEPT (_rescued_data, cdc_timestamp)
STORED AS SCD TYPE 1;                 -- 同じキーが再到着したら旧行を上書更新して履歴は残さない

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2-2.  渡航前アンケート / pre_survey

-- COMMAND ----------

-- Step1: Bronze　(データ取り込み + タイムスタンプ付与)
CREATE OR REFRESH STREAMING LIVE TABLE bz_pre_survey AS
SELECT
  CAST(user_id      AS BIGINT)        AS user_id,
  trip_purpose,
  content_category,
  answer_date,
  _rescued_data,
  current_timestamp()                AS cdc_timestamp
FROM cloud_files(
  '/Volumes/${catalog}/${schema}/${volume}/pre_survey',
  'csv',
  map('header','true','inferSchema','true','cloudFiles.inferColumnTypes','true')
);


-- Step2：データ読み込み＋スキーマ宣言＋期待値チェック
CREATE STREAMING LIVE TABLE sv_pre_survey (
  user_id           BIGINT        COMMENT "会員ID、例）1",
  trip_purpose      STRING        COMMENT "旅行目的、例）仕事",
  content_category  STRING        COMMENT "機内エンタメカテゴリ、例）移送手段や便利な情報",
  answer_date       TIMESTAMP     COMMENT "回答日時、YYYY-MM-DD HH:mm:ssフォーマット",
  _rescued_data     STRING        COMMENT "Auto Loader がスキーマ不一致（新規列・型不正値など）を検知したときにその『はみ出しデータ』を JSON 文字列で一時退避する救済用カラム",
  CONSTRAINT user_id_not_null EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "`sv_pre_survey`：渡航前アンケート（Silver）";

-- Step3：CDC マージ (SCD Type-1 = 最新状態だけを保持)
APPLY CHANGES INTO LIVE.sv_pre_survey
FROM STREAM(LIVE.bz_pre_survey)
KEYS (user_id)
SEQUENCE BY answer_date               -- 会員ごとに最新回答を判定
COLUMNS * EXCEPT (_rescued_data, cdc_timestamp)
STORED AS SCD TYPE 1;                 -- 同じキーが再到着したら旧行を上書更新して履歴は残さない

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2-3. コンテンツマスタ / ife_contents

-- COMMAND ----------

-- Step1: Bronze　(データ取り込み + タイムスタンプ付与)
CREATE OR REFRESH STREAMING LIVE TABLE bz_ife_contents
AS SELECT
  CAST(content_id       AS BIGINT)    AS content_id,
  content_category,
  CAST(duration_sec     AS BIGINT)    AS duration_sec,
  content_img_url,
  _rescued_data,
  current_timestamp()                 AS cdc_timestamp
FROM cloud_files(
  '/Volumes/${catalog}/${schema}/${volume}/ife_contents',
  'csv',
  map(
    'header','true',
    'inferSchema','true',
    'cloudFiles.inferColumnTypes','true'
  )
);

-- Step2：スキーマ宣言＋期待値チェック（※ AS SELECTなし＝空宣言のみ）
CREATE STREAMING LIVE TABLE sv_ife_contents (
  content_id        BIGINT    COMMENT "機内エンタメコンテンツID、例）15",
  content_category  STRING    COMMENT "機内エンタメカテゴリ、例）移送手段や便利な情報",
  duration_sec      BIGINT    COMMENT "機内エンタメ作品所要時間（秒）、例）6240",
  content_img_url   STRING    COMMENT "機内エンタメ作品画像URL、例）https://github.com/...",
  _rescued_data     STRING    COMMENT "Auto Loader がスキーマ不一致（新規列・型不正値など）を検知したときにその『はみ出しデータ』を JSON 文字列で一時退避する救済用カラム",
  CONSTRAINT content_id_not_null EXPECT (content_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "`sv_ife_contents`：機内エンタメのコンテンツマスタ（Silver）";

-- Step3：CDC マージ (SCD Type-1 = 最新状態だけを保持)
APPLY CHANGES INTO LIVE.sv_ife_contents
FROM STREAM(LIVE.bz_ife_contents)
KEYS (content_id)
SEQUENCE BY cdc_timestamp
COLUMNS * EXCEPT (_rescued_data, cdc_timestamp)
STORED AS SCD TYPE 1;                        -- 同じキーが再到着したら旧行を上書更新して履歴は残さない

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2-4. 過去視聴ログ / ife_play_logs

-- COMMAND ----------

-- Step1: Bronze　(データ取り込み)
CREATE OR REFRESH STREAMING LIVE TABLE bz_ife_play_logs
AS SELECT
  CAST(user_id          AS BIGINT)    AS user_id,
  flight_id,
  route_id,
  CAST(content_id       AS BIGINT)    AS content_id,
  content_category,
  CAST(duration_sec     AS BIGINT)    AS duration_sec,
  CAST(play_sec         AS BIGINT)    AS play_sec,
  CAST(play_start_at    AS TIMESTAMP) AS play_start_at,
  CAST(play_end_at      AS TIMESTAMP) AS play_end_at,
  CAST(play_start_date  AS DATE)      AS play_start_date,
  _rescued_data,
  current_timestamp()                 AS cdc_timestamp
FROM cloud_files(
  '/Volumes/${catalog}/${schema}/${volume}/ife_play_logs',
  'csv',
  map(
    'header', 'true',
    'inferSchema', 'true',
    'cloudFiles.inferColumnTypes', 'true',
    'cloudFiles.schemaHints', 'flight_id STRING, route_id STRING, duration_sec BIGINT, play_sec BIGINT, play_start_at TIMESTAMP, play_end_at TIMESTAMP, play_start_date DATE'
  )
);

-- Step2：スキーマ宣言＋期待値チェック＋コンテンツマスタ結合(中間テーブル：JOINでcontent_img_urlを付与)
CREATE OR REFRESH STREAMING LIVE TABLE sv_ife_play_logs_prejoin
AS
SELECT
  p.user_id,
  p.flight_id,
  p.route_id,
  p.content_id,
  p.content_category,
  c.content_img_url,
  p.duration_sec,
  p.play_sec,
  p.play_start_at,
  p.play_end_at,
  p.play_start_date,
  p._rescued_data,
  p.cdc_timestamp
FROM STREAM(LIVE.bz_ife_play_logs) AS p
LEFT JOIN LIVE.sv_ife_contents AS c ON p.content_id = c.content_id;

-- Step3: CDC マージ (正式テーブル: 最新状態だけを保持)
CREATE STREAMING LIVE TABLE sv_ife_play_logs (
  user_id           BIGINT    COMMENT "会員ID、例）1",
  flight_id         STRING    COMMENT "便名（機材＋日付で一意）、例）JL006",
  route_id          STRING    COMMENT "区間、例）NYC-NRT",
  content_id        BIGINT    COMMENT "機内エンタメコンテンツID、例）15",
  content_category  STRING    COMMENT "機内エンタメカテゴリ、例）移送手段や便利な情報",
  content_img_url   STRING    COMMENT "機内エンタメ作品画像URL、例）https://github.com/...",
  duration_sec      BIGINT    COMMENT "機内エンタメ作品所要時間（秒）、例）6240",
  play_sec          BIGINT    COMMENT "機内エンタメ視聴時間（秒）、例）5210",
  play_start_at     TIMESTAMP COMMENT "視聴開始日時、YYYY-MM-DD HH:mm:ssフォーマット",
  play_end_at       TIMESTAMP COMMENT "視聴終了日時、YYYY-MM-DD HH:mm:ssフォーマット",
  play_start_date   DATE      COMMENT "視聴開始日、YYYY-MM-DDフォーマット",
  _rescued_data     STRING    COMMENT "Auto Loader がスキーマ不一致（新規列・型不正値など）を検知したときにその『はみ出しデータ』を JSON 文字列で一時退避する救済用カラム",
  CONSTRAINT user_id_not_null EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT play_end_at_not_null EXPECT (play_end_at IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "`sv_ife_play_logs`：機内エンタメ視聴ログ（画像付き）";

APPLY CHANGES INTO LIVE.sv_ife_play_logs
FROM STREAM(LIVE.sv_ife_play_logs_prejoin)
KEYS (user_id, flight_id, content_id, play_start_at)    -- セッション一意キー
SEQUENCE BY play_end_at                      -- 新旧判定 = 終了時刻
COLUMNS * EXCEPT (_rescued_data, cdc_timestamp)
STORED AS SCD TYPE 1;                        -- 同じキーが再到着したら旧行を上書更新して履歴は残さない

