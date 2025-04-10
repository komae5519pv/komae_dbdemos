# Databricks notebook source
# MAGIC %md
# MAGIC # リテール企業を想定したCDPマートを作成
# MAGIC ## やること
# MAGIC - カタログ名だけご自身の作業用のカタログ名に書き換えて、本ノートブックを上から下まで流してください
# MAGIC - クラスタはDBR 16.0 ML以降で実行してください
# MAGIC
# MAGIC 想定のディレクトリ構成
# MAGIC
# MAGIC ```
# MAGIC /<catalog_name>
# MAGIC ├── retail_cdp
# MAGIC │   ├── bronze_xxx      <- ローデータテーブル
# MAGIC │   ├── silver_xxx      <- bronze_xxxをクレンジングしたテーブル
# MAGIC │   ├── gold_xxx        <- silverを使いやすく加工したテーブル
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC 該当スキーマ配下の既存silverテーブルを全て削除

# COMMAND ----------

# スキーマ内のすべてのテーブル名を取得する
tables_df = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}")

# テーブル名が "gold_" で始まるテーブルのみ削除する
for table in tables_df.collect():
    table_name = table["tableName"]
    if table_name.startswith("gold_"):
        spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{table_name}")
        print(f"削除されたテーブル: {table_name}")

print("全ての gold_ で始まるテーブルが削除されました。")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-1. gold_users / ユーザー

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3-1-1. step1 ユーザー情報と基本的な購買情報の統合

# COMMAND ----------

# DBTITLE 1,テーブル作成Step1
# 基本情報 + LTV、購買履歴情報 + ロイヤリティデータのVIEWを作成
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_users_step1 AS
-- Step1: すべてのチャネル購買（ユーザー単位）

WITH ltv_data AS (
    SELECT
        user_id,
        -- LTV：POS（店舗）注文の合計金額
        SUM(CASE WHEN order_type = 'POS' THEN total_amount ELSE 0 END) AS ltv_pos,
        -- LTV：EC（オンライン）注文の合計金額
        SUM(CASE WHEN order_type = 'EC' THEN total_amount ELSE 0 END) AS ltv_ec,
        -- LTV：すべての注文の合計金額
        SUM(total_amount) AS ltv_all_time,
        -- LTV：過去1年間（365日）の注文金額合計
        SUM(CASE WHEN order_date >= date_sub(current_date(), 365) THEN total_amount ELSE 0 END) AS ltv_last_year,
        -- LTV：過去2年間（730日）の注文金額合計
        SUM(CASE WHEN order_date >= date_sub(current_date(), 730) THEN total_amount ELSE 0 END) AS ltv_last_two_years,
        -- F：POS（店舗）での購入回数
        COUNT(DISTINCT CASE WHEN order_type = 'POS' THEN order_id ELSE NULL END) AS f_purchase_count_pos,
        -- F：EC（オンライン）での購入回数
        COUNT(DISTINCT CASE WHEN order_type = 'EC' THEN order_id ELSE NULL END) AS f_purchase_count_ec,
        -- F：すべての購入回数
        COUNT(DISTINCT order_id) AS f_purchase_count_all,
        -- 最終購入日（すべての注文）
        MAX(order_date) AS last_purchase_date_all,
        -- 最終購入日（POS注文のみ）
        MAX(CASE WHEN order_type = 'POS' THEN order_date ELSE NULL END) AS last_purchase_date_pos,
        -- 最終購入日（EC注文のみ）
        MAX(CASE WHEN order_type = 'EC' THEN order_date ELSE NULL END) AS last_purchase_date_ec,
        -- 最初の購入日（すべての注文）
        MIN(order_date) AS first_purchase_date_all,
        -- 最初の購入日（POS注文のみ）
        MIN(CASE WHEN order_type = 'POS' THEN order_date ELSE NULL END) AS first_purchase_date_pos,
        -- 最初の購入日（EC注文のみ）
        MIN(CASE WHEN order_type = 'EC' THEN order_date ELSE NULL END) AS first_purchase_date_ec,
        -- 最初のPOS購入店舗ID
        MIN(CASE WHEN order_type = 'POS' THEN store_id ELSE NULL END) AS first_purchase_store_pos,
        -- 最後のPOS購入店舗ID
        MAX(CASE WHEN order_type = 'POS' THEN store_id ELSE NULL END) AS last_purchase_store_pos,
        -- 最後の購入からの日数
        DATEDIFF(current_date(), MAX(order_date)) AS r_days_since_last_purchase_all,
        -- 最後のPOS購入からの日数
        DATEDIFF(current_date(), MAX(CASE WHEN order_type = 'POS' THEN order_date ELSE NULL END)) AS r_days_since_last_purchase_pos,
        -- 最後のEC購入からの日数
        DATEDIFF(current_date(), MAX(CASE WHEN order_type = 'EC' THEN order_date ELSE NULL END)) AS r_days_since_last_purchase_ec,
        -- 1回あたりの平均注文金額（すべての注文）
        CASE WHEN COUNT(DISTINCT order_id) > 0 THEN SUM(total_amount) / COUNT(DISTINCT order_id) ELSE 0 END AS m_average_order_value_all,
        -- 1回あたりの平均注文金額（POS注文のみ）
        CASE WHEN COUNT(DISTINCT CASE WHEN order_type = 'POS' THEN order_id ELSE NULL END) > 0 THEN 
            SUM(CASE WHEN order_type = 'POS' THEN total_amount ELSE 0 END) / COUNT(DISTINCT CASE WHEN order_type = 'POS' THEN order_id ELSE NULL END) 
            ELSE 0 END AS m_average_order_value_pos,
        -- 1回あたりの平均注文金額（EC注文のみ）
        CASE WHEN COUNT(DISTINCT CASE WHEN order_type = 'EC' THEN order_id ELSE NULL END) > 0 THEN 
            SUM(CASE WHEN order_type = 'EC' THEN total_amount ELSE 0 END) / COUNT(DISTINCT CASE WHEN order_type = 'EC' THEN order_id ELSE NULL END) 
            ELSE 0 END AS m_average_order_value_ec
    FROM (
        SELECT 
            user_id, 
            order_id, 
            total_amount, 
            order_date,
            store_id,
            CASE 
                WHEN source_table = 'silver_pos_orders' THEN 'POS'
                WHEN source_table = 'silver_ec_orders' THEN 'EC'
                ELSE 'UNKNOWN' 
            END AS order_type
        FROM (
            SELECT
              user_id, 
              order_id, 
              total_amount, 
              order_date, 
              store_id, 
              'silver_pos_orders' AS source_table
            FROM {MY_CATALOG}.{MY_SCHEMA}.silver_pos_orders
            UNION ALL
            SELECT
              user_id,
              order_id,
              total_amount,
              order_date,
              NULL AS store_id,
              'silver_ec_orders' AS source_table
            FROM {MY_CATALOG}.{MY_SCHEMA}.silver_ec_orders
        )
    ) 
    GROUP BY user_id
),
-- Step2: 更新日が最新のLINE情報（ユーザー単位）
latest_line_members AS (
    SELECT lm.*,
           ROW_NUMBER() OVER (PARTITION BY line_member_id ORDER BY updated_date DESC) AS rn
    FROM {MY_CATALOG}.{MY_SCHEMA}.silver_line_members lm
),
-- Step3: 更新日が最新のロイヤリティプログラム情報（ユーザー単位）
latest_loyalty_points AS (
    SELECT lp.*,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_date DESC) AS rn
    FROM {MY_CATALOG}.{MY_SCHEMA}.silver_loyalty_points lp
)
SELECT
  u.user_id,                                    -- ユーザーID
  u.name,                                       -- 氏名
  u.gender,                                     -- 性別
  u.birth_date,                                 -- 生年月日
  u.age,                                        -- 年齢
  u.age_group_5,                                -- 年代（5歳刻み）
  u.age_group_10,                               -- 年代（10歳刻み）
  u.pref,                                       -- 居住県
  u.phone_number,                               -- 電話番号
  u.email,                                      -- メールアドレス
  u.registration_date,                          -- 会員登録日
  u.email_permission_flg,                       -- メール許諾フラグ
  u.line_linked_flg,                            -- LINE連携フラグ
  u.line_member_id,                             -- LINE ID
  l.notification_allowed_flg,                   -- LINE通知許諾フラグ
  l.is_blocked_flg,                             -- LINEブロックフラグ
  l.is_friend_flg,                              -- LINE友だち登録フラグ
  u.status,                                     -- 会員ステータス
  u.last_login_date,                            -- 最終ログイン日
  ltv_data.ltv_all_time,                        -- LTV（全期間）
  ltv_data.ltv_pos,                             -- LTV（POS）
  ltv_data.ltv_ec,                              -- LTV（EC）
  ltv_data.ltv_last_year,                       -- LTV（過去1年）
  ltv_data.ltv_last_two_years,                  -- LTV（過去2年）
  ltv_data.last_purchase_date_all,              -- 最終購入日（全体）
  ltv_data.last_purchase_date_pos,              -- 最終購入日（POS）
  ltv_data.last_purchase_date_ec,               -- 最終購入日（EC）
  ltv_data.first_purchase_date_all,             -- 初回購入日（全体）
  ltv_data.first_purchase_date_pos,             -- 初回購入日（POS）
  ltv_data.first_purchase_date_ec,              -- 初回購入日（EC）
  ltv_data.first_purchase_store_pos,            -- 初回購入店舗（POS）
  ltv_data.last_purchase_store_pos,             -- 最終購入店舗（POS）
  ltv_data.f_purchase_count_all,                -- 購入回数（全体）
  ltv_data.f_purchase_count_pos,                -- 購入回数（POS）
  ltv_data.f_purchase_count_ec,                 -- 購入回数（EC）
  ltv_data.r_days_since_last_purchase_all,      -- 最終購入からの経過日数（全体）
  ltv_data.r_days_since_last_purchase_pos,      -- 最終購入からの経過日数（POS）
  ltv_data.r_days_since_last_purchase_ec,       -- 最終購入からの経過日数（EC）
  ltv_data.m_average_order_value_all,           -- 平均注文金額（全体）
  ltv_data.m_average_order_value_pos,           -- 平均注文金額（POS）
  ltv_data.m_average_order_value_ec,            -- 平均注文金額（EC）
  lp.member_rank,                               -- 会員ランク
  lp.points_to_next_rank,                       -- 次のランクまでのポイント
  lp.rank_expiration_date,                      -- ランク有効期限
  lp.point_balance,                             -- ポイント残高
  lp.point_expiration_date                      -- ポイント有効期限
FROM
  {MY_CATALOG}.{MY_SCHEMA}.silver_users u
LEFT JOIN
  latest_line_members l ON u.line_member_id = l.line_member_id AND l.rn = 1
LEFT JOIN
  ltv_data ON u.user_id = ltv_data.user_id
LEFT JOIN
  latest_loyalty_points lp ON u.user_id = lp.user_id AND lp.rn = 1
""")

# 作成されたVIEWからデータを取得して表示
v_users_step1_df = spark.sql("""SELECT * FROM v_users_step1""")

display(v_users_step1_df.count())
display(v_users_step1_df.limit(10))

# COMMAND ----------

# DBTITLE 1,重複チェック
aaa = spark.sql("""
SELECT
  user_id,
  COUNT(*) as count
FROM v_users_step1
GROUP BY user_id
HAVING count > 1
ORDER BY user_id
""")

display(aaa)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3-1-2. step2 カート情報やアンケート情報を含むVIEW作成

# COMMAND ----------

# DBTITLE 1,テーブル作成Step2
# カート情報やアンケート情報を含むVIEWを作成
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_users_step2 AS
-- Step1: 最新のカート情報（ユーザー単位）
WITH cart_data AS (
    SELECT
        c.user_id,
        -- 最後に作成したカートID
        MAX(c.cart_id) AS last_cart_id,
        -- カート破棄の場合だけ、updated_dateをabandoned_dateとして扱う
        MAX(CASE WHEN c.status = 0 THEN c.updated_date ELSE NULL END) AS last_cart_abandoned_date,
        -- カート破棄のうち、在庫がある商品数をカウント
        COUNT(DISTINCT CASE WHEN c.status = 0 AND ci.status = 1 THEN ci.cart_item_id ELSE NULL END) AS abandoned_cart_available_item_count,
        -- JSON形式でカートアイテムリスト（商品ID、数量、価格を含む）を作成し、文字列として扱う
        TO_JSON(COLLECT_LIST(
            CASE WHEN c.status = 0 THEN 
                STRUCT(ci.cart_item_id AS item_id, ci.quantity AS quantity, ci.unit_price AS price)
            ELSE NULL END
        )) AS abandoned_cart_item_list
    FROM {MY_CATALOG}.{MY_SCHEMA}.silver_carts c
    LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_cart_items ci ON c.cart_id = ci.cart_id
    GROUP BY c.user_id
),
-- Step2: 最新のアンケート回答（ユーザー単位）
survey_data AS (
    SELECT
        user_id,
        survey_category AS last_survey_category,        -- 最後のアンケートカテゴリ
        response_content AS last_survey_response,       -- 最後のアンケート回答
        response_date AS last_survey_date,              -- 最後のアンケート回答日
        positive_score AS last_survey_positive_score    -- 最後のアンケートのポジティブスコア
    FROM (
        SELECT
            s.user_id,
            s.survey_category,
            s.response_content,
            s.response_date,
            s.positive_score,
            ROW_NUMBER() OVER (PARTITION BY s.user_id ORDER BY s.response_date DESC) AS rn
        FROM {MY_CATALOG}.{MY_SCHEMA}.silver_survey s
    ) AS subquery
    WHERE subquery.rn = 1   -- 同じユーザー、同じ日に回答があった場合、最初の1つを選ぶ
),
-- Step3: 最新のLINE情報（ユーザー単位）
line_data AS (
    SELECT
        line_member_id,
        last_accessed_date,
        notification_allowed_flg AS line_notification_allowed_flg,
        is_blocked_flg AS line_is_blocked_flg,
        is_friend_flg AS line_is_friend_flg
    FROM (
        SELECT
            lm.line_member_id,
            lm.last_accessed_date,
            lm.notification_allowed_flg,
            lm.is_blocked_flg,
            lm.is_friend_flg,
            ROW_NUMBER() OVER (PARTITION BY lm.line_member_id ORDER BY lm.updated_date DESC) AS rn
        FROM {MY_CATALOG}.{MY_SCHEMA}.silver_line_members lm
    ) AS subquery
    WHERE subquery.rn = 1   -- 同じユーザー、同じ更新日に複数レコードあった場合、最初の1つを選ぶ
),
-- Step4: 初回/最終の購買店舗（ユーザー単位）
pos_first_last_store_data AS (
    SELECT
        po.user_id,
        MIN(po.store_id) AS first_purchase_store_pos,                   -- 最初に購入した店舗
        MAX(po.store_id) AS last_purchase_store_pos                     -- 最後に購入した店舗
    FROM {MY_CATALOG}.{MY_SCHEMA}.silver_pos_orders po
    GROUP BY po.user_id
)
SELECT DISTINCT
    base.user_id,                                             -- 会員ID
    base.registration_date,                                   -- 会員登録日
    base.status,                                              -- 会員ステータス
    base.last_login_date,                                     -- 最終ログイン日
    line_data.last_accessed_date as line_last_accessed_date,  -- LINE最終ログイン日
    base.line_member_id,                                      -- LINE ID
    base.email_permission_flg,                                -- メール許諾フラグ
    base.line_linked_flg,                                     -- LINE連携フラグ
    base.member_rank,                                         -- 会員ランク
    base.points_to_next_rank,                                 -- 次の会員ランクまでの残ポイント
    base.rank_expiration_date,                                -- 会員ランク有効期限
    base.point_balance,                                       -- ポイント残高
    base.point_expiration_date,                               -- ポイント有効期限
    base.name,                                                -- 氏名
    base.gender,                                              -- 性別
    base.birth_date,                                          -- 生年月日
    base.age,                                                 -- 年齢
    base.age_group_5,                                         -- 年代（5歳刻み）
    base.age_group_10,                                        -- 年代（10歳刻み）
    base.pref,                                                -- 居住県
    base.phone_number,                                        -- 電話番号
    base.email,                                               -- メールアドレス
    base.ltv_all_time,                                        -- LTV_全期間
    base.ltv_last_year,                                       -- LTV_過去1年
    base.ltv_last_two_years,                                  -- LTV_過去2年
    base.last_purchase_date_all,                              -- 最終購入日（ALL）
    base.last_purchase_date_pos,                              -- 最終購入日（POS）
    base.last_purchase_date_ec,                               -- 最終購入日（EC）
    base.first_purchase_date_all,                             -- 初回購入日（ALL）
    base.first_purchase_date_pos,                             -- 初回購入日（POS）
    base.first_purchase_date_ec,                              -- 初回購入日（EC）
    base.r_days_since_last_purchase_all,                      -- R_最終購買からの経過日数（ALL）
    base.r_days_since_last_purchase_pos,                      -- R_最終購買からの経過日数（POS）
    base.r_days_since_last_purchase_ec,                       -- R_最終購買からの経過日数（EC）
    base.f_purchase_count_all,                                -- F_過去購買回数（ALL）
    base.f_purchase_count_pos,                                -- F_過去購買回数（POS）
    base.f_purchase_count_ec,                                 -- F_過去購買回数（EC）
    base.m_average_order_value_all,                           -- M_平均会計単価（ALL）
    base.m_average_order_value_pos,                           -- M_平均会計単価（POS）
    base.m_average_order_value_ec,                            -- M_平均会計単価（EC）
    cart_data.last_cart_id,                                   -- 最後のカートID
    cart_data.last_cart_abandoned_date,                       -- 最後のカート破棄日
    cart_data.abandoned_cart_available_item_count,            -- 最後のカートに在庫がある商品の数
    cart_data.abandoned_cart_item_list,                       -- 最後のカートに含まれる商品リスト（JSON形式）
    survey_data.last_survey_category,                         -- 最後のアンケートカテゴリ
    survey_data.last_survey_response,                         -- 最後のアンケート回答
    survey_data.last_survey_date,                             -- 最後のアンケート回答日
    survey_data.last_survey_positive_score,                   -- 最後のアンケートのポジティブスコア
    line_data.line_notification_allowed_flg,                  -- LINE通知許諾フラグ
    line_data.line_is_blocked_flg,                            -- LINEブロックフラグ
    line_data.line_is_friend_flg,                             -- LINE友だち登録フラグ
    pos_first_last_store_data.first_purchase_store_pos,       -- 初回POS購入店舗
    pos_first_last_store_data.last_purchase_store_pos         -- 最後POS購入店舗
FROM
    v_users_step1 base
LEFT JOIN
    cart_data ON base.user_id = cart_data.user_id
LEFT JOIN
    survey_data ON base.user_id = survey_data.user_id
LEFT JOIN
    line_data ON base.line_member_id = line_data.line_member_id
LEFT JOIN
    pos_first_last_store_data ON base.user_id = pos_first_last_store_data.user_id
""")

# 作成されたVIEWからデータを取得して表示
v_users_step2_df = spark.sql("""SELECT * FROM v_users_step2""")

display(v_users_step2_df.count())
display(v_users_step2_df.limit(10))

# COMMAND ----------

# DBTITLE 1,重複チェック
aaa = spark.sql("""
SELECT
  user_id,
  COUNT(*) as count
FROM v_users_step2
GROUP BY user_id
HAVING count > 1
ORDER BY user_id
""")

display(aaa.count())
display(aaa)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3-1-3. step3 テーブル統合
# MAGIC LTV や 最終購入日、購買履歴 に関連するデータを集計し、POSとECの購買履歴からLTVや購入日データを取得して、ユーザーごとに統合

# COMMAND ----------

# DBTITLE 1,テーブル作成Step3
# POSとECの購買履歴、頻繁購入商品カテゴリ、RFM、アンケートデータを含むVIEWを作成
users_step3_df = spark.sql(f"""
-- Step1: 最頻購買商品カテゴリ（ユーザー単位）
WITH frequent_item_data AS (
    SELECT
        user_id,
        -- POSでの最頻購入商品カテゴリを取得
        MAX(CASE WHEN order_type = 'POS' THEN frequent_item_category ELSE NULL END) AS frequent_item_category_pos,
        -- ECでの最頻購入商品カテゴリを取得
        MAX(CASE WHEN order_type = 'EC' THEN frequent_item_category ELSE NULL END) AS frequent_item_category_ec,
        -- 全体での最頻購入商品カテゴリを取得
        MAX(frequent_item_category) AS frequent_item_category_all
    FROM (
        -- POSとECの商品カテゴリをUNION取得
        SELECT 
            user_id, 
            category_name AS frequent_item_category,
            'POS' AS order_type
        FROM {MY_CATALOG}.{MY_SCHEMA}.silver_pos_orders_items
        UNION ALL
        SELECT 
            user_id, 
            category_name AS frequent_item_category,
            'EC' AS order_type
        FROM {MY_CATALOG}.{MY_SCHEMA}.silver_ec_order_items
    ) 
    GROUP BY user_id
),
-- Step2: RFMセグメント（ユーザー単位）
rfm_segments AS (
    SELECT
        user_id,
        -- R_リーセンシーセグメント（ALL）の計算
        CASE 
            WHEN r_days_since_last_purchase_all <= 7 THEN '01_7日以内'
            WHEN r_days_since_last_purchase_all <= 14 THEN '02_14日以内'
            WHEN r_days_since_last_purchase_all <= 30 THEN '03_30日以内'
            WHEN r_days_since_last_purchase_all <= 60 THEN '04_60日以内'
            WHEN r_days_since_last_purchase_all <= 90 THEN '05_90日以内'
            WHEN r_days_since_last_purchase_all <= 150 THEN '06_150日以内'
            WHEN r_days_since_last_purchase_all <= 180 THEN '07_180日以内'
            ELSE '08_それ以上'
        END AS r_recency_segment_all,
        -- R_リーセンシーセグメント（POS）の計算
        CASE 
            WHEN r_days_since_last_purchase_pos <= 7 THEN '01_7日以内'
            WHEN r_days_since_last_purchase_pos <= 14 THEN '02_14日以内'
            WHEN r_days_since_last_purchase_pos <= 30 THEN '03_30日以内'
            WHEN r_days_since_last_purchase_pos <= 60 THEN '04_60日以内'
            WHEN r_days_since_last_purchase_pos <= 90 THEN '05_90日以内'
            WHEN r_days_since_last_purchase_pos <= 150 THEN '06_150日以内'
            WHEN r_days_since_last_purchase_pos <= 180 THEN '07_180日以内'
            ELSE '08_それ以上'
        END AS r_recency_segment_pos,
        -- R_リーセンシーセグメント（EC）の計算
        CASE 
            WHEN r_days_since_last_purchase_ec <= 7 THEN '01_7日以内'
            WHEN r_days_since_last_purchase_ec <= 14 THEN '02_14日以内'
            WHEN r_days_since_last_purchase_ec <= 30 THEN '03_30日以内'
            WHEN r_days_since_last_purchase_ec <= 60 THEN '04_60日以内'
            WHEN r_days_since_last_purchase_ec <= 90 THEN '05_90日以内'
            WHEN r_days_since_last_purchase_ec <= 150 THEN '06_150日以内'
            WHEN r_days_since_last_purchase_ec <= 180 THEN '07_180日以内'
            ELSE '08_それ以上'
        END AS r_recency_segment_ec,
        -- F_フリークエンシーセグメント（ALL）の計算
        CASE 
            WHEN f_purchase_count_all = 1 THEN '01_1回'
            WHEN f_purchase_count_all = 2 THEN '02_2回'
            WHEN f_purchase_count_all = 3 THEN '03_3回'
            WHEN f_purchase_count_all BETWEEN 5 AND 10 THEN '04_5-10回'
            WHEN f_purchase_count_all BETWEEN 11 AND 20 THEN '05_11-20回'
            WHEN f_purchase_count_all BETWEEN 21 AND 50 THEN '06_21-50回'
            WHEN f_purchase_count_all > 50 THEN '07_50回以上'
            ELSE '00_その他'
        END AS f_frequency_segment_all,
        -- F_フリークエンシーセグメント（POS）の計算
        CASE 
            WHEN f_purchase_count_pos = 1 THEN '01_1回'
            WHEN f_purchase_count_pos = 2 THEN '02_2回'
            WHEN f_purchase_count_pos = 3 THEN '03_3回'
            WHEN f_purchase_count_pos BETWEEN 5 AND 10 THEN '04_5-10回'
            WHEN f_purchase_count_pos BETWEEN 11 AND 20 THEN '05_11-20回'
            WHEN f_purchase_count_pos BETWEEN 21 AND 50 THEN '06_21-50回'
            WHEN f_purchase_count_pos > 50 THEN '07_50回以上'
            ELSE '00_その他'
        END AS f_frequency_segment_pos,
        -- F_フリークエンシーセグメント（EC）の計算
        CASE 
            WHEN f_purchase_count_ec = 1 THEN '01_1回'
            WHEN f_purchase_count_ec = 2 THEN '02_2回'
            WHEN f_purchase_count_ec = 3 THEN '03_3回'
            WHEN f_purchase_count_ec BETWEEN 5 AND 10 THEN '04_5-10回'
            WHEN f_purchase_count_ec BETWEEN 11 AND 20 THEN '05_11-20回'
            WHEN f_purchase_count_ec BETWEEN 21 AND 50 THEN '06_21-50回'
            WHEN f_purchase_count_ec > 50 THEN '07_50回以上'
            ELSE '00_その他'
        END AS f_frequency_segment_ec,
        -- M_マネタリーセグメント（ALL）の計算
        CASE 
            WHEN m_average_order_value_all < 500 THEN '01_500円未満'
            WHEN m_average_order_value_all < 1000 THEN '02_500以上-1000円未満'
            WHEN m_average_order_value_all < 2000 THEN '03_1000以上-2000円未満'
            WHEN m_average_order_value_all < 3000 THEN '04_2000以上-3000円未満'
            WHEN m_average_order_value_all < 4000 THEN '05_3000以上-4000円未満'
            WHEN m_average_order_value_all < 5000 THEN '06_4000以上-5000円未満'
            WHEN m_average_order_value_all < 10000 THEN '07_5000以上-10000円未満'
            ELSE '08_10000以上'
        END AS m_monetary_segment_all,
        -- M_マネタリーセグメント（POS）の計算
        CASE 
            WHEN m_average_order_value_pos < 500 THEN '01_500円未満'
            WHEN m_average_order_value_pos < 1000 THEN '02_500以上-1000円未満'
            WHEN m_average_order_value_pos < 2000 THEN '03_1000以上-2000円未満'
            WHEN m_average_order_value_pos < 3000 THEN '04_2000以上-3000円未満'
            WHEN m_average_order_value_pos < 4000 THEN '05_3000以上-4000円未満'
            WHEN m_average_order_value_pos < 5000 THEN '06_4000以上-5000円未満'
            WHEN m_average_order_value_pos < 10000 THEN '07_5000以上-10000円未満'
            ELSE '08_10000以上'
        END AS m_monetary_segment_pos,
        -- M_マネタリーセグメント（EC）の計算
        CASE 
            WHEN m_average_order_value_ec < 500 THEN '01_500円未満'
            WHEN m_average_order_value_ec < 1000 THEN '02_500以上-1000円未満'
            WHEN m_average_order_value_ec < 2000 THEN '03_1000以上-2000円未満'
            WHEN m_average_order_value_ec < 3000 THEN '04_2000以上-3000円未満'
            WHEN m_average_order_value_ec < 4000 THEN '05_3000以上-4000円未満'
            WHEN m_average_order_value_ec < 5000 THEN '06_4000以上-5000円未満'
            WHEN m_average_order_value_ec < 10000 THEN '07_5000以上-10000円未満'
            ELSE '08_10000以上'
        END AS m_monetary_segment_ec
    FROM v_users_step2
)
SELECT 
    base.user_id,                                                    -- 会員ID
    base.registration_date,                                          -- 会員登録日
    base.status,                                                     -- 会員ステータス
    base.last_login_date,                                            -- 最終ログイン日
    base.line_last_accessed_date,                                    -- LINE最終アクセス日
    base.line_member_id,                                             -- LINE会員ID
    base.email_permission_flg,                                       -- メール許諾フラグ
    base.line_linked_flg,                                            -- LINE連携フラグ
    base.line_notification_allowed_flg,                              -- LINE通知許諾フラグ
    base.line_is_blocked_flg,                                        -- LINEブロックフラグ
    base.line_is_friend_flg,                                         -- LINE友だち登録フラグ
    base.member_rank,                                                -- 会員ランク
    base.points_to_next_rank,                                        -- 次の会員ランクまでの残ポイント
    base.rank_expiration_date,                                       -- 会員ランク有効期限
    base.point_balance,                                              -- ポイント残高
    base.point_expiration_date,                                      -- ポイント有効期限
    base.name,                                                       -- 氏名
    base.gender,                                                     -- 性別
    base.birth_date,                                                 -- 生年月日
    base.age,                                                        -- 年齢
    base.age_group_5,                                                -- 年代（5歳刻み）
    base.age_group_10,                                               -- 年代（10歳刻み）
    base.pref,                                                       -- 居住県
    base.phone_number,                                               -- 電話番号
    base.email,                                                      -- メールアドレス
    base.last_cart_id,                                               -- 最終カートID
    base.last_cart_abandoned_date,                                   -- 最終カート破棄日時
    base.abandoned_cart_available_item_count,                        -- 最終カート破棄の商品数（在庫ありのみ）
    base.abandoned_cart_item_list,                                   -- 最終カート破棄の商品リスト
    CAST(base.ltv_all_time AS BIGINT),                               -- LTV_全期間
    CAST(base.ltv_last_year AS BIGINT),                              -- LTV_過去1年
    CAST(base.ltv_last_two_years AS BIGINT),                         -- LTV_過去2年
    base.last_purchase_date_all,                                     -- 最終購入日（ALL）
    base.last_purchase_date_pos,                                     -- 最終購入日（POS）
    base.last_purchase_date_ec,                                      -- 最終購入日（EC）
    base.first_purchase_date_all,                                    -- 初回購入日（ALL）
    base.first_purchase_date_pos,                                    -- 初回購入日（POS）
    base.first_purchase_date_ec,                                     -- 初回購入日（EC）
    CASE 
        WHEN base.first_purchase_date_pos <= base.first_purchase_date_ec OR base.first_purchase_date_ec IS NULL THEN 'POS'
        ELSE 'EC'
    END AS first_purchase_channel,                                   -- 初回購買チャネル
    CASE 
        WHEN base.last_purchase_date_pos >= base.last_purchase_date_ec OR base.last_purchase_date_ec IS NULL THEN 'POS'
        ELSE 'EC'
    END AS last_purchase_channel,                                    -- 最終購買チャネル
    base.first_purchase_store_pos,                                   -- 初回購買店舗（POS）
    base.last_purchase_store_pos,                                    -- 最終購買店舗（POS）
    base.r_days_since_last_purchase_all,                             -- R_最終購買からの経過日数（ALL）
    base.r_days_since_last_purchase_pos,                             -- R_最終購買からの経過日数（POS）
    base.r_days_since_last_purchase_ec,                              -- R_最終購買からの経過日数（EC）
    rfm.r_recency_segment_all,                                       -- R_リーセンシーセグメント（ALL）
    rfm.r_recency_segment_pos,                                       -- R_リーセンシーセグメント（POS）
    rfm.r_recency_segment_ec,                                        -- R_リーセンシーセグメント（EC）
    base.f_purchase_count_all,                                       -- F_過去購買回数（ALL）
    base.f_purchase_count_pos,                                       -- F_過去購買回数（POS）
    base.f_purchase_count_ec,                                        -- F_過去購買回数（EC）
    rfm.f_frequency_segment_all,                                     -- F_フリークエンシーセグメント（ALL）
    rfm.f_frequency_segment_pos,                                     -- F_フリークエンシーセグメント（POS）
    rfm.f_frequency_segment_ec,                                      -- F_フリークエンシーセグメント（EC）
    base.m_average_order_value_all,                                  -- M_平均会計単価（ALL）
    base.m_average_order_value_pos,                                  -- M_平均会計単価（POS）
    base.m_average_order_value_ec,                                   -- M_平均会計単価（EC）
    rfm.m_monetary_segment_all,                                      -- M_マネタリーセグメント（ALL）
    rfm.m_monetary_segment_pos,                                      -- M_マネタリーセグメント（POS）
    rfm.m_monetary_segment_ec,                                       -- M_マネタリーセグメント（EC）
    frequent_item_data.frequent_item_category_all,                   -- 最頻購買商品カテゴリ（ALL）_全期間
    frequent_item_data.frequent_item_category_pos,                   -- 最頻購買商品カテゴリ（POS）_全期間
    frequent_item_data.frequent_item_category_ec,                    -- 最頻購買商品カテゴリ（EC）_全期間
    base.last_survey_response,                                       -- 最終アンケート回答
    base.last_survey_category,                                       -- 最終アンケートカテゴリ
    CAST(ROUND(base.last_survey_positive_score, 2) AS DECIMAL(10,2)) AS last_survey_positive_score  -- 最終アンケートのポジティブスコア（小数第二位まで）
FROM
    v_users_step2 base
LEFT JOIN
    frequent_item_data ON base.user_id = frequent_item_data.user_id
LEFT JOIN
    rfm_segments rfm ON base.user_id = rfm.user_id
""")

# テーブル作成
users_step3_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.gold_users')

display(users_step3_df.count())
display(users_step3_df.limit(10))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.gold_users'

# テーブルコメント
comment = """
`gold_users`テーブルは、ゴールド会員の基本情報やステータス、ランク、ポイント情報を管理するためのデータを格納しています。会員のランクやポイント残高、最終ログイン日など、会員の利用状況やロイヤルティを把握するために使用されます。
"""

spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
    "user_id":"会員ID、文字列、ユニーク（主キー）",
    "registration_date":"会員登録日、日付、YYYY-MM-DDフォーマット",
    "status":"会員ステータス、文字列、例 active, inactive など",
    "last_login_date":"最終ログイン日、日付、YYYY-MM-DDフォーマット",
    "line_last_accessed_date":"LINE最終アクセス日、日付、YYYY-MM-DDフォーマット",
    "line_member_id":"LINE会員ID、文字列、例 U4af4980629",
    "email_permission_flg":"メール許諾フラグ、整数、例 1:許諾、0:拒否",
    "line_linked_flg":"LINE連携フラグ、整数、例 1:連携済、0:未連携",
    "line_notification_allowed_flg":"LINE通知許諾フラグ、整数、例 1:許諾、0:拒否",
    "line_is_blocked_flg":"LINEブロックフラグ、整数、例 1:ブロック、0:非ブロック",
    "line_is_friend_flg":"LINE友だち登録フラグ、整数、例 1:登録済み、0:未登録",
    "member_rank":"会員ランク、文字列、例 BRONZE, SILVER, GOLD, PLATINUM など",
    "points_to_next_rank":"次の会員ランクまでの残ポイント、整数",
    "rank_expiration_date":"会員ランク有効期限、日付、YYYY-MM-DDフォーマット",
    "point_balance":"ポイント残高、整数",
    "point_expiration_date":"ポイント有効期限、日付、YYYY-MM-DDフォーマット",
    "name":"会員氏名、文字列",
    "gender":"性別、例　M: 男性, F: 女性, O: その他",
    "birth_date":"生年月日、日付、YYYY-MM-DDフォーマット",
    "age":"年齢（1歳刻み）、整数",
    "age_group_5":"、年代（5歳刻み）、整数、例：25、30、35",
    "age_group_10":"年代（10歳刻み）、整数、例：20、30、40",
    "pref":"居住県、文字列",
    "phone_number":"電話番号、文字列、例 090-7147-1524",
    "email":"メールアドレス",
    "last_cart_id":"最終カートID、最後に破棄されたカートID",
    "last_cart_abandoned_date":"最終カート破棄日、日付、YYYY-MM-DDフォーマット",
    "abandoned_cart_available_item_count":"最終カート破棄の商品数（在庫ありのみ）、整数",
    "abandoned_cart_item_list":"最終カート破棄の商品リスト（商品ID、商品名、商品単価）、バリアント型",
    "ltv_all_time":"LTV_全期間",
    "ltv_last_year":"LTV_過去1年",
    "ltv_last_two_years":"LTV_過去2年",
    "last_purchase_date_all":"最終購入日（ALL）、日付、YYYY-MM-DDフォーマット",
    "last_purchase_date_pos":"最終購入日（POS）、日付、YYYY-MM-DDフォーマット",
    "last_purchase_date_ec":"最終購入日（EC）、日付、YYYY-MM-DDフォーマット",
    "first_purchase_date_all":"最終購入日（ALL）、日付、YYYY-MM-DDフォーマット",
    "first_purchase_date_pos":"初回購入日（POS）、日付、YYYY-MM-DDフォーマット",
    "first_purchase_date_ec":"初回購入日（EC）、日付、YYYY-MM-DDフォーマット、",
    "first_purchase_channel":"初回購買チャネル、文字列",
    "last_purchase_channel":"最終購買チャネル、文字列",
    "first_purchase_store_pos":"初回購買店舗（POS）、文字列",
    "last_purchase_store_pos":"最終購買店舗（POS）、文字列",
    "r_days_since_last_purchase_all":"R_最終購買からの経過日数（ALL）",
    "r_days_since_last_purchase_pos":"R_最終購買からの経過日数（POS）",
    "r_days_since_last_purchase_ec":"R_最終購買からの経過日数（EC）",
    "r_recency_segment_all":"R_リーセンシーセグメント（ALL）",
    "r_recency_segment_pos":"R_リーセンシーセグメント（POS）",
    "r_recency_segment_ec":"R_リーセンシーセグメント（EC）",
    "f_purchase_count_all":"F_過去購買回数（ALL）",
    "f_purchase_count_pos":"F_過去購買回数（POS）",
    "f_purchase_count_ec":"F_過去購買回数（EC）",
    "f_frequency_segment_all":"F_フリークエンシーセグメント（ALL）",
    "f_frequency_segment_pos":"F_フリークエンシーセグメント（POS）",
    "f_frequency_segment_ec":"F_フリークエンシーセグメント（EC）",
    "m_average_order_value_all":"M_平均会計単価（ALL）",
    "m_average_order_value_pos":"M_平均会計単価（POS）",
    "m_average_order_value_ec":"M_平均会計単価（EC）",
    "m_monetary_segment_all":"M_マネタリーセグメント（ALL）",
    "m_monetary_segment_pos":"M_マネタリーセグメント（POS）",
    "m_monetary_segment_ec":"M_マネタリーセグメント（EC）",
    "frequent_item_category_all":"最頻購買商品カテゴリ（ALL）_全期間、例 野菜",
    "frequent_item_category_pos":"最頻購買商品カテゴリ（POS）_全期間、例 肉類",
    "frequent_item_category_ec":"最頻購買商品カテゴリ（EC）_全期間、例 魚介類",
    "last_survey_response":"最終アンケート回答、例 とても満足してます",
    "last_survey_category":"最終アンケートカテゴリ、例 商品品質",
    "last_survey_positive_score":"最終アンケートポジティブスコア、例 2.91851902008056"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ここ以降は保留・・・
# MAGIC ・
# MAGIC ・

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-2. gold_orders_items / 購買（アイテム単位） 

# COMMAND ----------

# gold_orders_items_df = spark.sql(f"""
# -- Step 1: POS購買履歴とEC購買履歴の統合（アイテム単位）
# WITH pos_ec_purchase_history AS (
#   SELECT
#       'POS' AS purchase_channel,          -- 購入チャネル（POS/EC）
#       poi.order_item_id,                  -- 明細ID
#       poi.user_id,                        -- 会員ID
#       poi.order_id,                       -- 注文ID
#       poi.item_id,                        -- 商品ID
#       poi.category_id,                    -- カテゴリID
#       poi.item_name,                      -- 商品名
#       poi.category_name,                  -- カテゴリ名
#       poi.quantity,                       -- 数量
#       poi.unit_price,                     -- 単価
#       poi.subtotal,                       -- 小計
#       po.total_amount,                    -- 合計金額
#       poi.cancel_flg,                     -- キャンセルフラグ
#       po.order_date,                      -- 注文日時
#       po.store_id,                        -- 店舗ID（POSの場合のみ）
#       po.store_name,                      -- 店舗名（POSの場合のみ）
#       NULL AS cart_id,                    -- カートID（ECの場合のみ）
#       NULL AS points_discount_amount,     -- ポイント割引額（ECの場合のみ）
#       NULL AS coupon_discount_amount,     -- クーポン割引額（ECの場合のみ）
#       NULL AS discount_amount,            -- 割引額（合計）
#       poi.created_date,                   -- 作成日
#       poi.updated_date                    -- 更新日
#   FROM {MY_CATALOG}.{MY_SCHEMA}.silver_pos_orders_items poi
#   JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_pos_orders po ON poi.order_id = po.order_id

#   UNION ALL

#   SELECT
#       'EC' AS purchase_channel,           -- 購入チャネル（POS/EC）
#       eoi.order_item_id,                  -- 明細ID
#       eoi.user_id,                        -- 会員ID
#       eoi.order_id,                       -- 注文ID
#       eoi.item_id,                        -- 商品ID
#       eoi.category_id,                    -- カテゴリID
#       eoi.item_name,                      -- 商品名
#       eoi.category_name,                  -- カテゴリ名
#       eoi.quantity,                       -- 数量
#       eoi.unit_price,                     -- 単価
#       eoi.subtotal,                       -- 小計
#       eo.total_amount,                    -- 合計金額
#       eoi.cancel_flg,                     -- キャンセルフラグ
#       eo.order_date,                      -- 注文日時
#       NULL AS store_id,                   -- 店舗ID（POSの場合のみ）
#       NULL AS store_name,                 -- 店舗名（POSの場合のみ）
#       eo.cart_id,                         -- カートID（ECの場合のみ）
#       eo.points_discount_amount,          -- ポイント割引額（ECの場合のみ）
#       eo.coupon_discount_amount,          -- クーポン割引額（ECの場合のみ）
#       eo.discount_amount,                 -- 割引額（合計）
#       eoi.created_date,                   -- 作成日
#       eoi.updated_date                    -- 更新日
#   FROM {MY_CATALOG}.{MY_SCHEMA}.silver_ec_order_items eoi
#   JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_ec_orders eo ON eoi.order_id = eo.order_id
# ),

# -- Step 2: ロイヤリティポイント情報とLINE情報の追加
# user_loyalty_and_line_info AS (
#     SELECT
#         oi.*,
#         lp.point_balance AS points_earned,  -- 獲得ポイント（スナップショット）
#         lp.points_to_next_rank,             -- 次の会員ランクまでの残ポイント（スナップショット）
#         lp.member_rank,                     -- 会員ランク（スナップショット）
#         u.line_member_id,                   -- LINE会員ID
#         lm.notification_allowed_flg         -- LINE通知許諾フラグ
#     FROM pos_ec_purchase_history oi
#     LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_loyalty_points lp ON oi.user_id = lp.user_id
#     LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_users u ON oi.user_id = u.user_id
#     LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_line_members lm ON u.line_member_id = lm.line_member_id
# ),

# -- Step 3: 在庫情報の追加
# purchase_with_inventory AS (
#     SELECT
#         oi.*,
#         ih.quantity AS stock_quantity   -- 最新の在庫数量（在庫履歴から取得したもの）を追加
#     FROM user_loyalty_and_line_info oi  -- 購入履歴テーブル（ユーザーの購入情報）

#     LEFT JOIN ( -- 各商品と店舗ごとに最新の在庫情報を取得
#         SELECT 
#             item_id,                    -- 商品ID
#             store_id,                   -- 店舗ID
#             inventory_date,             -- 在庫情報の日付
#             quantity,                   -- 在庫数量
#             ROW_NUMBER() OVER (PARTITION BY item_id, store_id ORDER BY inventory_date DESC) as rn
#         FROM {MY_CATALOG}.{MY_SCHEMA}.silver_inventory_history
#     ) ih 
#     -- 購入商品と在庫商品を商品IDで結合
#     ON oi.item_id = ih.item_id
#     -- 店舗がNULLの場合、在庫情報の店舗がNULLであれば結びつける（購入店舗が特定できない場合も考慮）
#     AND (oi.store_id = ih.store_id OR (oi.store_id IS NULL AND ih.store_id IS NULL))
#     -- 購入日よりも前の在庫情報のみを対象とする
#     AND ih.inventory_date <= oi.order_date
#     -- 最新の在庫情報（ROW_NUMBERが1のもの）を選択
#     AND ih.rn = 1
# ),

# -- Step 4: Web行動履歴とアプリ情報の追加
# web_and_app_user_activity AS (
#     SELECT
#         oi.*,
#         CASE WHEN oi.purchase_channel = 'EC' THEN wl.session_id ELSE NULL END AS session_id,           -- セッションID
#         CASE WHEN oi.purchase_channel = 'EC' THEN wl.referrer_url ELSE NULL END AS referrer_url,       -- リファラーURL
#         CASE WHEN oi.purchase_channel = 'EC' THEN wl.device_type ELSE NULL END AS device_type,         -- デバイスタイプ
#         CASE WHEN oi.purchase_channel = 'EC' THEN wl.utm_source ELSE NULL END AS utm_source,           -- UTMソース
#         CASE WHEN oi.purchase_channel = 'EC' THEN wl.utm_medium ELSE NULL END AS utm_medium,           -- UTMメディア
#         CASE WHEN oi.purchase_channel = 'EC' THEN wl.utm_campaign ELSE NULL END AS utm_campaign,       -- UTMキャンペーン
#         CASE WHEN oi.purchase_channel = 'EC' THEN wl.search_query ELSE NULL END AS last_search_keyword,-- 直前検索キーワード
#         CASE WHEN oi.purchase_channel = 'EC' THEN wl.time_spent ELSE NULL END AS time_spent,           -- ページ滞在時間
#         CASE WHEN oi.purchase_channel = 'EC' THEN wl.scroll_depth ELSE NULL END AS scroll_depth,       -- スクロール深度
#         CASE WHEN oi.purchase_channel = 'EC' THEN ae.app_version ELSE NULL END AS app_version          -- アプリバージョン
#     FROM purchase_with_inventory oi
#     LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_web_logs wl 
#         ON oi.user_id = wl.user_id 
#         AND oi.order_date = wl.event_date
#         AND oi.purchase_channel = 'EC'
#     LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_app_events ae
#         ON oi.user_id = ae.user_id
#         AND oi.order_date = ae.event_date
#         AND oi.purchase_channel = 'EC'
# ),

# -- Step 5: リピート購入情報の追加
# repeat_purchase_flag AS (
#     SELECT
#         oi.*,
#         CASE 
#             WHEN LAG(oi.order_date) OVER (PARTITION BY oi.user_id, oi.item_id ORDER BY oi.order_date) IS NOT NULL THEN TRUE
#             ELSE FALSE
#         END AS is_repeat_purchase,            -- リピート購入フラグ
#         DATEDIFF(oi.order_date, LAG(oi.order_date) OVER (PARTITION BY oi.user_id, oi.item_id ORDER BY oi.order_date)) AS days_since_last_purchase              -- 前回同商品購入からの日数
#     FROM web_and_app_user_activity oi
# )

# -- Step 6: アンケート情報の追加
# ,latest_survey_info AS (
#     SELECT
#         oi.*,
#         s.survey_category AS last_survey_category,        -- 最終アンケートカテゴリ（スナップショット）
#         s.positive_score AS last_survey_positive_score    -- 最終アンケートポジティブスコア（スナップショット）
#     FROM repeat_purchase_flag oi
#     LEFT JOIN (
#         SELECT 
#             user_id, 
#             survey_category, 
#             positive_score,
#             ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY response_date DESC) as rn
#         FROM {MY_CATALOG}.{MY_SCHEMA}.silver_survey
#     ) s ON oi.user_id = s.user_id AND s.rn = 1
# )
# SELECT
#     oi.order_item_id,                   -- 明細ID
#     oi.order_id,                        -- 注文ID
#     oi.order_date,                      -- 注文日
#     oi.purchase_channel,                -- 購入チャネル（POS/EC）
#     oi.user_id,                         -- 会員ID
#     gu.registration_date,               -- 会員登録日
#     gu.status as user_status,           -- 会員ステータス
#     gu.last_login_date,                 -- 最終ログイン日
#     gu.line_last_accessed_date,         -- LINE最終アクセス日
#     oi.line_member_id,                  -- LINE会員ID
#     gu.email_permission_flg,            -- メール許諾フラグ
#     gu.line_linked_flg,                 -- LINE連携フラグ
#     COALESCE(gu.line_notification_allowed_flg, 0) as line_notification_allowed_flg,   -- LINE通知許諾フラグ
#     COALESCE(gu.line_is_blocked_flg, 0) as line_is_blocked_flg,                       -- LINEブロックフラグ
#     COALESCE(gu.line_is_friend_flg, 0) as line_is_friend_flg,                         -- LINE友達登録フラグ
#     gu.member_rank,                     -- 会員ランク
#     gu.points_to_next_rank,             -- 次の会員ランクまでの残ポイント
#     gu.rank_expiration_date,            -- 会員ランク有効期限
#     gu.point_balance,                   -- ポイント残高
#     gu.point_expiration_date,           -- ポイント有効期限
#     gu.name as user_name,               -- 会員氏名
#     gu.gender,                          -- 性別
#     gu.birth_date,                      -- 生年月日
#     gu.age,                             -- 年齢
#     gu.age_group_5,                     -- 年代（5歳刻み）
#     gu.age_group_10,                    -- 年代（10歳刻み）
#     gu.pref,                            -- 居住県
#     gu.phone_number,                    -- 電話番号
#     gu.email as user_email,             -- メールアドレス
#     oi.item_id,                         -- 商品ID
#     oi.category_id,                     -- カテゴリID
#     oi.item_name,                       -- 商品名
#     oi.category_name,                   -- カテゴリ名
#     oi.quantity,                        -- 購入数量
#     oi.stock_quantity,                  -- 在庫数量
#     oi.unit_price,                      -- 単価
#     oi.subtotal,                        -- 小計
#     oi.total_amount,                    -- 合計金額
#     oi.cancel_flg,                      -- キャンセルフラグ
#     oi.store_id,                        -- 店舗ID（POSの場合のみ）
#     oi.store_name,                      -- 店舗名（POSの場合のみ）
#     oi.cart_id,                         -- カートID（ECの場合のみ）
#     oi.points_earned,                   -- 獲得ポイント（スナップショット）
#     oi.points_discount_amount,          -- ポイント割引額（ECの場合のみ）
#     oi.coupon_discount_amount,          -- クーポン割引額（ECの場合のみ）
#     oi.discount_amount,                 -- 割引額（合計）
#     oi.is_repeat_purchase,              -- リピート購入フラグ
#     oi.days_since_last_purchase,        -- 前回同商品購入からの日数
#     oi.session_id,                      -- セッションID（ECの場合のみ）
#     oi.referrer_url,                    -- リファラーURL（ECの場合のみ）
#     oi.device_type,                     -- デバイスタイプ
#     oi.utm_source,                      -- UTMソース
#     oi.utm_medium,                      -- UTMメディア
#     oi.utm_campaign,                    -- UTMキャンペーン
#     oi.last_search_keyword,             -- 直前検索キーワード
#     oi.time_spent,                      -- ページ滞在時間
#     oi.scroll_depth,                    -- スクロール深度
#     oi.app_version,                     -- アプリバージョン
#     gu.ltv_all_time,                    -- LTV_全期間
#     gu.ltv_last_year,                   -- LTV_過去1年
#     gu.ltv_last_two_years,              -- LTV_過去2年
#     gu.last_purchase_date_all,          -- 最終購入日（ALL）
#     gu.last_purchase_date_pos,          -- 最終購入日（POS）
#     gu.last_purchase_date_ec,           -- 最終購入日（EC）
#     gu.first_purchase_date_all,         -- 最終購入日（ALL）
#     gu.first_purchase_date_pos,         -- 初回購入日（POS）
#     gu.first_purchase_date_ec,          -- 初回購入日（EC）
#     gu.first_purchase_channel,          -- 初回購買チャネル
#     gu.last_purchase_channel,           -- 最終購買チャネル
#     gu.first_purchase_store_pos,        -- 初回購買店舗（POS）
#     gu.last_purchase_store_pos,         -- 最終購買店舗（POS）
#     gu.r_days_since_last_purchase_all,  -- R_最終購買からの経過日数（ALL）
#     gu.r_days_since_last_purchase_pos,  -- R_最終購買からの経過日数（POS）
#     gu.r_days_since_last_purchase_ec,   -- R_最終購買からの経過日数（EC）
#     gu.r_recency_segment_all,           -- R_リーセンシーセグメント（ALL）
#     gu.r_recency_segment_pos,           -- R_リーセンシーセグメント（POS）
#     gu.r_recency_segment_ec,            -- R_リーセンシーセグメント（EC）
#     gu.f_purchase_count_all,            -- F_過去購買回数（ALL）
#     gu.f_purchase_count_pos,            -- F_過去購買回数（POS）
#     gu.f_purchase_count_ec,             -- F_過去購買回数（EC）
#     gu.f_frequency_segment_all,         -- F_フリークエンシーセグメント（ALL）
#     gu.f_frequency_segment_pos,         -- F_フリークエンシーセグメント（POS）
#     gu.f_frequency_segment_ec,          -- F_フリークエンシーセグメント（EC）
#     gu.m_average_order_value_all,       -- M_平均会計単価（ALL）
#     gu.m_average_order_value_pos,       -- M_平均会計単価（POS）
#     gu.m_average_order_value_ec,        -- M_平均会計単価（EC）
#     gu.m_monetary_segment_all,          -- M_マネタリーセグメント（ALL）
#     gu.m_monetary_segment_pos,          -- M_マネタリーセグメント（POS）
#     gu.m_monetary_segment_ec,           -- M_マネタリーセグメント（EC）
#     gu.frequent_item_category_all,      -- 最頻購買商品カテゴリ（ALL）_全期間
#     gu.frequent_item_category_pos,      -- 最頻購買商品カテゴリ（POS）_全期間
#     gu.frequent_item_category_ec,       -- 最頻購買商品カテゴリ（EC）_全期間
#     gu.last_survey_response,            -- 最終アンケート回答
#     gu.last_survey_category,            -- 最終アンケートカテゴリ
#     gu.last_survey_positive_score       -- 最終アンケートポジティブスコア
# FROM latest_survey_info oi
# LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.gold_users gu ON oi.user_id = gu.user_id
# """)

# # テーブル作成
# gold_orders_items_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.gold_orders_items')

# # テーブル作成の確認
# display(gold_orders_items_df.count())
# display(gold_orders_items_df.limit(10))

# COMMAND ----------

# DBTITLE 1,在庫数チェック
# spark.sql(f"""
# SELECT
#   order_date,
#   store_name,
#   item_name,
#   SUM(quantity) AS total_quantity
# FROM {MY_CATALOG}.{MY_SCHEMA}.gold_orders_items
# WHERE store_id IS NOT NULL
# GROUP BY 1,2,3
# ORDER BY 1 DESC
# """).display()

# COMMAND ----------

# DBTITLE 1,コメント追加
# # テーブル名
# table_name = f'{MY_CATALOG}.{MY_SCHEMA}.gold_orders_items'

# # テーブルコメント
# comment = """
# `gold_orders_items`テーブルは、商品単位の購買履歴（EC+POS）です。各注文の明細情報を管理し、購買行動に基づくセグメント情報を含みます。注文ID、商品ID、数量、価格に加え、購入チャネルや会員情報、ポイント獲得状況、LTVやリピート購入フラグなどのセグメントデータも含まれます。
# """

# spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# # カラムコメント
# column_comments = {
#     "order_item_id":"明細ID、文字列、ユニーク（主キー）",
#     "order_id":"注文ID、文字列、`gold_orders`テーブルの`order_id`にリンクする外部キー",
#     "order_date":"注文日、日付、YYYY-MM-DDフォーマット",
#     "purchase_channel":"購入チャネル",
#     "user_id":"会員ID",
#     "registration_date":"会員登録日、日付、YYYY-MM-DDフォーマット",
#     "user_status":"会員ステータス、例 'active', 'inactive'",
#     "last_login_date":"最終ログイン日、日付、YYYY-MM-DDフォーマット",
#     "line_last_accessed_date":"LINE最終アクセス日、日付、YYYY-MM-DDフォーマット",
#     "line_member_id":"LINE会員ID、例 U4af4980629",
#     "email_permission_flg":"メール許諾フラグ、例 1:許諾、0:拒否",
#     "line_linked_flg":"LINE連携フラグ、例 1:連携済、0:未連携",
#     "line_notification_allowed_flg":"LINE通知許諾フラグ、例 1:許諾、0:拒否",
#     "line_is_blocked_flg":"LINEブロックフラグ、例 1:ブロック、0:非ブロック",
#     "line_is_friend_flg":"LINE友だち登録フラグ、例 1:登録済み、0:未登録",
#     "member_rank":"会員ランク、例 BRONZE, SILVER, GOLD, PLATINUM",
#     "points_to_next_rank":"次の会員ランクまでの残ポイント、整数",
#     "rank_expiration_date":"会員ランク有効期限、日付、YYYY-MM-DDフォーマット",
#     "point_balance":"ポイント残高、整数",
#     "point_expiration_date":"ポイント有効期限、日付、YYYY-MM-DDフォーマット",
#     "user_name":"氏名",
#     "gender":"性別、例　M: 男性, F: 女性, O: その他",
#     "birth_date":"生年月日、日付、YYYY-MM-DDフォーマット",
#     "age":"年齢（1歳刻み）",
#     "age_group_5":"年代（5歳刻み）",
#     "age_group_10":"年代（10歳刻み）",
#     "pref":"居住県",
#     "phone_number":"電話番号",
#     "user_email":"メールアドレス",
#     "item_id":"商品ID、`silever_items`テーブルの`item_id`にリンクする外部キー",
#     "category_id":"カテゴリID",
#     "item_name":"商品名、例 トマト",
#     "category_name":"カテゴリ名、例 野菜",
#     "quantity":"購入数量",
#     "stock_quantity":"在庫数量",
#     "unit_price":"単価",
#     "subtotal":"小計、`quantity * unit_price`",
#     "total_amount":"合計金額、割引後小計 + 税額",
#     "cancel_flg":"キャンセルフラグ",
#     "store_id":"店舗ID、POSの場合のみ",
#     "store_name":"店舗名、POSの場合のみ",
#     "cart_id":"カートID、ECの場合のみ",
#     "points_earned":"獲得ポイント",
#     "points_discount_amount":"ポイント割引額、ECの場合のみ",
#     "coupon_discount_amount":"クーポン割引額、ECの場合のみ",
#     "discount_amount":"割引額（合計）、ポイント＋クーポンの割引額合計",
#     "is_repeat_purchase":"リピート購入フラグ、同一商品の再購入か否か、例 FALSE, TRUE",
#     "days_since_last_purchase":"前回同商品購入からの日数",
#     "session_id":"セッションID、ECの場合のセッションID",
#     "referrer_url":"リファラーURL、ECの場合の参照元URL",
#     "device_type":"デバイスタイプ",
#     "utm_source":"UTMソース",
#     "utm_medium":"UTMメディア",
#     "utm_campaign":"UTMキャンペーン",
#     "last_search_keyword":"直前検索キーワード",
#     "time_spent":"ページ滞在時間",
#     "scroll_depth":"スクロール深度",
#     "app_version":"アプリバージョン",
#     "ltv_all_time":"LTV_全期間",
#     "ltv_last_year":"LTV_過去1年",
#     "ltv_last_two_years":"LTV_過去2年",
#     "last_purchase_date_all":"最終購入日（ALL）、日付、YYYY-MM-DDフォーマット",
#     "last_purchase_date_pos":"最終購入日（POS）、日付、YYYY-MM-DDフォーマット",
#     "last_purchase_date_ec":"最終購入日（EC）、日付、YYYY-MM-DDフォーマット",
#     "first_purchase_date_all":"最終購入日（ALL）、日付、YYYY-MM-DDフォーマット",
#     "first_purchase_date_pos":"初回購入日（POS）、日付、YYYY-MM-DDフォーマット",
#     "first_purchase_date_ec":"初回購入日（EC）、日付、YYYY-MM-DDフォーマット",
#     "first_purchase_channel":"初回購買チャネル",
#     "last_purchase_channel":"最終購買チャネル",
#     "first_purchase_store_pos":"初回購買店舗（POS）",
#     "last_purchase_store_pos":"最終購買店舗（POS）",
#     "r_days_since_last_purchase_all":"R_最終購買からの経過日数（ALL）",
#     "r_days_since_last_purchase_pos":"R_最終購買からの経過日数（POS）",
#     "r_days_since_last_purchase_ec":"R_最終購買からの経過日数（EC）",
#     "r_recency_segment_all":"R_リーセンシーセグメント（ALL）",
#     "r_recency_segment_pos":"R_リーセンシーセグメント（POS）",
#     "r_recency_segment_ec":"R_リーセンシーセグメント（EC）",
#     "f_purchase_count_all":"F_過去購買回数（ALL）",
#     "f_purchase_count_pos":"F_過去購買回数（POS）",
#     "f_purchase_count_ec":"F_過去購買回数（EC）",
#     "f_frequency_segment_all":"F_フリークエンシーセグメント（ALL）",
#     "f_frequency_segment_pos":"F_フリークエンシーセグメント（POS）",
#     "f_frequency_segment_ec":"F_フリークエンシーセグメント（EC）",
#     "m_average_order_value_all":"M_平均会計単価（ALL）",
#     "m_average_order_value_pos":"M_平均会計単価（POS）",
#     "m_average_order_value_ec":"M_平均会計単価（EC）",
#     "m_monetary_segment_all":"M_マネタリーセグメント（ALL）",
#     "m_monetary_segment_pos":"M_マネタリーセグメント（POS）",
#     "m_monetary_segment_ec":"M_マネタリーセグメント（EC）",
#     "frequent_item_category_all":"最頻購買商品カテゴリ（ALL）_全期間",
#     "frequent_item_category_pos":"最頻購買商品カテゴリ（POS）_全期間",
#     "frequent_item_category_ec":"最頻購買商品カテゴリ（EC）_全期間",
#     "last_survey_response":"最終アンケート回答、文字列",
#     "last_survey_category":"最終アンケートカテゴリ、文字列",
#     "last_survey_positive_score":"最終アンケートポジティブスコア、例 2.91851902008056"
# }

# for column, comment in column_comments.items():
#     # シングルクォートをエスケープ
#     escaped_comment = comment.replace("'", "\\'")
#     sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
#     spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-3. gold_orders / 購買（会計単位）

# COMMAND ----------

# gold_orders_df = spark.sql(f"""
# -- Step 1: 会計単位のデータを作成
# WITH orders AS (
#     SELECT DISTINCT
#         order_id,                                   -- 注文ID
#         order_date,                                 -- 注文日
#         user_id,                                    -- 会員ID
#         registration_date,                          -- 会員登録日
#         status AS user_status,                      -- 会員ステータス
#         last_login_date,                            -- 最終ログイン日
#         line_member_id,                             -- LINE会員ID
#         email_permission_flg,                       -- メール許諾フラグ
#         line_linked_flg,                            -- LINE連携フラグ
#         line_notification_allowed_flg,              -- LINE通知許諾フラグ
#         line_is_blocked_flg,                        -- LINEブロックフラグ
#         line_is_friend_flg,                         -- LINE友だち登録フラグ
#         member_rank,                                -- 会員ランク
#         points_to_next_rank,                        -- 次の会員ランクまでの残ポイント
#         rank_expiration_date,                       -- 会員ランク有効期限
#         point_balance,                              -- ポイント残高
#         point_expiration_date,                      -- ポイント有効期限
#         user_name,                                  -- 氏名
#         gender,                                     -- 性別
#         birth_date,                                 -- 生年月日
#         age,                                        -- 年齢
#         age_group_5,                                -- 年代（5歳刻み）
#         age_group_10,                               -- 年代（10歳刻み）
#         pref,                                       -- 居住県
#         phone_number,                               -- 電話番号
#         user_email,                                 -- メールアドレス
#         cancel_flg,                                 -- キャンセルフラグ
#         purchase_channel,                           -- 購入チャネル
#         store_id,                                   -- 店舗ID
#         store_name,                                 -- 店舗名
#         cart_id,                                    -- カートID
#         points_earned,                              -- 獲得ポイント
#         points_discount_amount,                     -- ポイント割引額
#         coupon_discount_amount,                     -- クーポン割引額
#         discount_amount,                            -- 割引額（合計）
#         session_id,                                 -- セッションID
#         referrer_url,                               -- リファラーURL
#         device_type,                                -- デバイスタイプ
#         utm_source,                                 -- UTMソース
#         utm_medium,                                 -- UTMメディア
#         utm_campaign,                               -- UTMキャンペーン
#         last_search_keyword,                        -- 直前検索キーワード
#         time_spent,                                 -- ページ滞在時間
#         scroll_depth,                               -- スクロール深度
#         app_version,                                -- アプリバージョン
#         ltv_all_time,                               -- LTV_全期間
#         ltv_last_year,                              -- LTV_過去1年
#         ltv_last_two_years,                         -- LTV_過去2年
#         last_purchase_date_all,                     -- 最終購入日（ALL）
#         last_purchase_date_pos,                     -- 最終購入日（POS）
#         last_purchase_date_ec,                      -- 最終購入日（EC）
#         first_purchase_date_all,                    -- 初回購入日（ALL）
#         first_purchase_date_pos,                    -- 初回購入日（POS）
#         first_purchase_date_ec,                     -- 初回購入日（EC）
#         first_purchase_channel,                     -- 初回購買チャネル
#         last_purchase_channel,                      -- 最終購買チャネル
#         first_purchase_store_pos,                   -- 初回購買店舗（POS）
#         last_purchase_store_pos,                    -- 最終購買店舗（POS）
#         r_days_since_last_purchase_all,             -- R_最終購買からの経過日数（ALL）
#         r_days_since_last_purchase_pos,             -- R_最終購買からの経過日数（POS）
#         r_days_since_last_purchase_ec,              -- R_最終購買からの経過日数（EC）
#         r_recency_segment_all,                      -- R_リーセンシーセグメント（ALL）
#         r_recency_segment_pos,                      -- R_リーセンシーセグメント（POS）
#         r_recency_segment_ec,                       -- R_リーセンシーセグメント（EC）
#         f_purchase_count_all,                       -- F_過去購買回数（ALL）
#         f_purchase_count_pos,                       -- F_過去購買回数（POS）
#         f_purchase_count_ec,                        -- F_過去購買回数（EC）
#         f_frequency_segment_all,                    -- F_フリークエンシーセグメント（ALL）
#         f_frequency_segment_pos,                    -- F_フリークエンシーセグメント（POS）
#         f_frequency_segment_ec,                     -- F_フリークエンシーセグメント（EC）
#         m_average_order_value_all,                  -- M_平均会計単価（ALL）
#         m_average_order_value_pos,                  -- M_平均会計単価（POS）
#         m_average_order_value_ec,                   -- M_平均会計単価（EC）
#         m_monetary_segment_all,                     -- M_マネタリーセグメント（ALL）
#         m_monetary_segment_pos,                     -- M_マネタリーセグメント（POS）
#         m_monetary_segment_ec,                      -- M_マネタリーセグメント（EC）
#         frequent_item_category_all,                 -- 最頻購買商品カテゴリ（ALL）_全期間
#         frequent_item_category_pos,                 -- 最頻購買商品カテゴリ（POS）_全期間
#         frequent_item_category_ec,                  -- 最頻購買商品カテゴリ（EC）_全期間
#         last_survey_response,                       -- 最終アンケート回答
#         last_survey_category,                       -- 最終アンケートカテゴリ
#         last_survey_positive_score                  -- 最終アンケートポジティブスコア
#     FROM {MY_CATALOG}.{MY_SCHEMA}.gold_orders_items
# ),
# order_details AS (
#     SELECT
#         order_id,                                   -- 注文ID
#         SUM(quantity) AS order_quantity,            -- 会計数量（会計単位）
#         SUM(total_amount) AS order_amount,          -- 会計金額（会計単位）
#         -- 商品詳細をリストで保持
#         collect_list(named_struct(
#             'category_id', category_id,
#             'category_name', category_name,
#             'item_id', item_id,
#             'item_name', item_name,
#             'quantity', quantity,
#             'unit_price', unit_price
#         )) AS order_details                        -- 会計詳細
#     FROM {MY_CATALOG}.{MY_SCHEMA}.gold_orders_items
#     GROUP BY order_id
# )
# SELECT
#     o.order_id,                                 -- 注文ID
#     o.order_date,                               -- 注文日
#     o.user_id,                                  -- 会員ID
#     o.registration_date,                        -- 会員登録日
#     o.user_status,                              -- 会員ステータス
#     o.last_login_date,                          -- 最終ログイン日
#     o.line_member_id,                           -- LINE会員ID
#     o.email_permission_flg,                     -- メール許諾フラグ
#     o.line_linked_flg,                          -- LINE連携フラグ
#     o.line_notification_allowed_flg,            -- LINE通知許諾フラグ
#     o.line_is_blocked_flg,                      -- LINEブロックフラグ
#     o.line_is_friend_flg,                       -- LINE友だち登録フラグ
#     o.member_rank,                              -- 会員ランク
#     o.points_to_next_rank,                      -- 次の会員ランクまでの残ポイント
#     o.rank_expiration_date,                     -- 会員ランク有効期限
#     o.point_balance,                            -- ポイント残高
#     o.point_expiration_date,                    -- ポイント有効期限
#     o.user_name,                                -- 氏名
#     o.gender,                                   -- 性別
#     o.birth_date,                               -- 生年月日
#     o.age,                                      -- 年齢
#     o.age_group_5,                              -- 年代（5歳刻み）
#     o.age_group_10,                             -- 年代（10歳刻み）
#     o.pref,                                     -- 居住県
#     o.phone_number,                             -- 電話番号
#     o.user_email,                               -- メールアドレス
#     d.order_details,                            -- 会計詳細（カテゴリ別）
#     d.order_quantity,                           -- 会計数量
#     d.order_amount,                             -- 会計金額
#     o.cancel_flg,                               -- キャンセルフラグ
#     o.purchase_channel,                         -- 購入チャネル
#     o.store_id,                                 -- 店舗ID
#     o.store_name,                               -- 店舗名
#     o.cart_id,                                  -- カートID
#     o.points_earned,                            -- 獲得ポイント
#     o.points_discount_amount,                   -- ポイント割引額
#     o.coupon_discount_amount,                   -- クーポン割引額
#     o.discount_amount,                          -- 割引額（合計）
#     o.session_id,                               -- セッションID
#     o.referrer_url,                             -- リファラーURL
#     o.device_type,                              -- デバイスタイプ
#     o.utm_source,                               -- UTMソース
#     o.utm_medium,                               -- UTMメディア
#     o.utm_campaign,                             -- UTMキャンペーン
#     o.last_search_keyword,                      -- 直前検索キーワード
#     o.time_spent,                               -- ページ滞在時間
#     o.scroll_depth,                             -- スクロール深度
#     o.app_version,                              -- アプリバージョン
#     o.ltv_all_time,                             -- LTV_全期間
#     o.ltv_last_year,                            -- LTV_過去1年
#     o.ltv_last_two_years,                       -- LTV_過去2年
#     o.last_purchase_date_all,                   -- 最終購入日（ALL）
#     o.last_purchase_date_pos,                   -- 最終購入日（POS）
#     o.last_purchase_date_ec,                    -- 最終購入日（EC）
#     o.first_purchase_date_all,                  -- 初回購入日（ALL）
#     o.first_purchase_date_pos,                  -- 初回購入日（POS）
#     o.first_purchase_date_ec,                   -- 初回購入日（EC）
#     o.first_purchase_channel,                   -- 初回購買チャネル
#     o.last_purchase_channel,                    -- 最終購買チャネル
#     o.first_purchase_store_pos,                 -- 初回購買店舗（POS）
#     o.last_purchase_store_pos,                  -- 最終購買店舗（POS）
#     o.r_days_since_last_purchase_all,           -- R_最終購買からの経過日数（ALL）
#     o.r_days_since_last_purchase_pos,           -- R_最終購買からの経過日数（POS）
#     o.r_days_since_last_purchase_ec,            -- R_最終購買からの経過日数（EC）
#     o.r_recency_segment_all,                    -- R_リーセンシーセグメント（ALL）
#     o.r_recency_segment_pos,                    -- R_リーセンシーセグメント（POS）
#     o.r_recency_segment_ec,                     -- R_リーセンシーセグメント（EC）
#     o.f_purchase_count_all,                     -- F_過去購買回数（ALL）
#     o.f_purchase_count_pos,                     -- F_過去購買回数（POS）
#     o.f_purchase_count_ec,                      -- F_過去購買回数（EC）
#     o.f_frequency_segment_all,                  -- F_フリークエンシーセグメント（ALL）
#     o.f_frequency_segment_pos,                  -- F_フリークエンシーセグメント（POS）
#     o.f_frequency_segment_ec,                   -- F_フリークエンシーセグメント（EC）
#     o.m_average_order_value_all,                -- M_平均会計単価（ALL）
#     o.m_average_order_value_pos,                -- M_平均会計単価（POS）
#     o.m_average_order_value_ec,                 -- M_平均会計単価（EC）
#     o.m_monetary_segment_all,                   -- M_マネタリーセグメント（ALL）
#     o.m_monetary_segment_pos,                   -- M_マネタリーセグメント（POS）
#     o.m_monetary_segment_ec,                    -- M_マネタリーセグメント（EC）
#     o.frequent_item_category_all,               -- 最頻購買商品カテゴリ（ALL）_全期間
#     o.frequent_item_category_pos,               -- 最頻購買商品カテゴリ（POS）_全期間
#     o.frequent_item_category_ec,                -- 最頻購買商品カテゴリ（EC）_全期間
#     o.last_survey_response,                     -- 最終アンケート回答
#     o.last_survey_category,                     -- 最終アンケートカテゴリ
#     o.last_survey_positive_score                -- 最終アンケートポジティブスコア
# FROM orders o
# JOIN order_details d
#     ON o.order_id = d.order_id
# """)

# # テーブル作成
# gold_orders_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.gold_orders')

# # テーブル作成の確認
# display(gold_orders_df.count())
# display(gold_orders_df.limit(10))

# COMMAND ----------

# DBTITLE 1,コメント追加
# # テーブル名
# table_name = f'{MY_CATALOG}.{MY_SCHEMA}.gold_orders'

# # テーブルコメント
# comment = """
# `gold_orders`テーブルは、会計単位の購買履歴（EC+POS）です。注文ID、注文日、会員ID、購入チャネル、獲得ポイントなどを格納します。会員ランクやリピート購入フラグなど、ユーザーの購買行動を分析するためのセグメント情報も含まれています。
# """

# spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# # カラムコメント
# column_comments = {
#     "order_id":"注文ID、文字列、ユニーク（主キー）および`gold_orders_items`テーブルの`order_id`にリンクする外部キー",
#     "order_date":"注文日、日付、YYYY-MM-DDフォーマット",
#     "user_id":"会員ID、`gold_users`テーブルの`user_id`にリンクする外部キー",
#     "registration_date":"会員登録日、日付、YYYY-MM-DDフォーマット",
#     "user_status":"会員ステータス、例 'active', 'inactive",
#     "last_login_date":"最終ログイン日、日付、YYYY-MM-DDフォーマット",
#     "line_member_id":"LINE会員ID、例 U4af4980629",
#     "email_permission_flg":"メール許諾フラグ、例 1:許諾、0:拒否",
#     "line_linked_flg":"LINE連携フラグ、例 1:連携済、0:未連携",
#     "line_notification_allowed_flg":"LINE通知許諾フラグ、例 1:許諾、0:拒否",
#     "line_is_blocked_flg":"LINEブロックフラグ、例 1:ブロック、0:非ブロック",
#     "line_is_friend_flg":"LINE友だち登録フラグ、例 1:登録済み、0:未登録",
#     "member_rank":"会員ランク、例 BRONZE, SILVER, GOLD, PLATINUM",
#     "points_to_next_rank":"次の会員ランクまでの残ポイント",
#     "rank_expiration_date":"会員ランク有効期限、日付、YYYY-MM-DDフォーマット",
#     "point_balance":"ポイント残高、整数",
#     "point_expiration_date":"ポイント有効期限、日付、YYYY-MM-DDフォーマット",
#     "user_name":"氏名",
#     "gender":"性別、例　M: 男性, F: 女性, O: その他",
#     "birth_date":"生年月日、日付、YYYY-MM-DDフォーマット",
#     "age":"年齢（1歳刻み）",
#     "age_group_5":"年代（5歳刻み）",
#     "age_group_10":"年代（10歳刻み）",
#     "pref":"居住県",
#     "phone_number":"電話番号",
#     "user_email":"メールアドレス",
#     "order_details":"会計詳細、配列型、会計情報のリスト",
#     "order_quantity":"会計数量（会計単位）",
#     "order_amount":"会計金額（会計単位）、割引後小計 + 税額",
#     "cancel_flg":"キャンセルフラグ",
#     "purchase_channel":"購入チャネル、POS または EC",
#     "store_id":"店舗ID、POSの場合のみ",
#     "cart_id":"カートID、ECの場合のみ",
#     "points_earned":"獲得ポイント",
#     "points_discount_amount":"ポイント割引額、ECの場合のみ",
#     "coupon_discount_amount":"クーポン割引額、ECの場合のみ",
#     "discount_amount":"割引額（合計）、ポイント＋クーポンの割引額合計",
#     "session_id":"セッションID、ECの場合のセッションID",
#     "referrer_url":"リファラーURL、ECの場合の参照元URL",
#     "device_type":"デバイスタイプ",
#     "utm_source":"UTMソース",
#     "utm_medium":"UTMメディア",
#     "utm_campaign":"UTMキャンペーン",
#     "last_search_keyword":"直前検索キーワード",
#     "time_spent":"ページ滞在時間",
#     "scroll_depth":"スクロール深度",
#     "app_version":"アプリバージョン",
#     "ltv_all_time":"LTV_全期間",
#     "ltv_last_year":"LTV_過去1年",
#     "ltv_last_two_years":"LTV_過去2年",
#     "last_purchase_date_all":"最終購入日（ALL）、日付、YYYY-MM-DDフォーマット",
#     "last_purchase_date_pos":"最終購入日（POS）、日付、YYYY-MM-DDフォーマット",
#     "last_purchase_date_ec":"最終購入日（EC）、日付、YYYY-MM-DDフォーマット",
#     "first_purchase_date_all":"最終購入日（ALL）、日付、YYYY-MM-DDフォーマット",
#     "first_purchase_date_pos":"初回購入日（POS）、日付、YYYY-MM-DDフォーマット",
#     "first_purchase_date_ec":"初回購入日（EC）、日付、YYYY-MM-DDフォーマット",
#     "first_purchase_channel":"初回購買チャネル",
#     "last_purchase_channel":"最終購買チャネル",
#     "first_purchase_store_pos":"初回購買店舗（POS）",
#     "last_purchase_store_pos":"最終購買店舗（POS）",
#     "r_days_since_last_purchase_all":"R_最終購買からの経過日数（ALL）",
#     "r_days_since_last_purchase_pos":"R_最終購買からの経過日数（POS）",
#     "r_days_since_last_purchase_ec":"R_最終購買からの経過日数（EC）",
#     "r_recency_segment_all":"R_リーセンシーセグメント（ALL）",
#     "r_recency_segment_pos":"R_リーセンシーセグメント（POS）",
#     "r_recency_segment_ec":"R_リーセンシーセグメント（EC）",
#     "f_purchase_count_all":"F_過去購買回数（ALL）",
#     "f_purchase_count_pos":"F_過去購買回数（POS）",
#     "f_purchase_count_ec":"F_過去購買回数（EC）",
#     "f_frequency_segment_all":"F_フリークエンシーセグメント（ALL）",
#     "f_frequency_segment_pos":"F_フリークエンシーセグメント（POS）",
#     "f_frequency_segment_ec":"F_フリークエンシーセグメント（EC）",
#     "m_average_order_value_all":"M_平均会計単価（ALL）",
#     "m_average_order_value_pos":"M_平均会計単価（POS）",
#     "m_average_order_value_ec":"M_平均会計単価（EC）",
#     "m_monetary_segment_all":"M_マネタリーセグメント（ALL）",
#     "m_monetary_segment_pos":"M_マネタリーセグメント（POS）",
#     "m_monetary_segment_ec":"M_マネタリーセグメント（EC）",
#     "frequent_item_category_all":"最頻購買商品カテゴリ（ALL）_全期間",
#     "frequent_item_category_pos":"最頻購買商品カテゴリ（POS）_全期間",
#     "frequent_item_category_ec":"最頻購買商品カテゴリ（EC）_全期間",
#     "last_survey_response":"最終アンケート回答、文字列",
#     "last_survey_category":"最終アンケートカテゴリ、文字列",
#     "last_survey_positive_score":"最終アンケートポジティブスコア、例 2.91851902008056"
# }

# for column, comment in column_comments.items():
#     # シングルクォートをエスケープ
#     escaped_comment = comment.replace("'", "\\'")
#     sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
#     spark.sql(sql_query)
