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

# テーブル名が "silver_" で始まるテーブルのみ削除する
for table in tables_df.collect():
    table_name = table["tableName"]
    if table_name.startswith("silver_"):
        spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{table_name}")
        print(f"削除されたテーブル: {table_name}")

print("全ての silver_ で始まるテーブルが削除されました。")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-1. silver_line_members / LINE会員

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, when

silver_line_members_df = spark.sql(f"""
SELECT DISTINCT
  line_system_id,
  line_member_id,
  to_date(linked_at) AS linked_date,                                            -- タイムスタンプから日付型へ変換
  to_date(last_accessed_at) AS last_accessed_date,                              -- タイムスタンプから日付型へ変換
  COALESCE(CAST(notification_allowed AS INT), 0) AS notification_allowed_flg,   -- BooleanからIntegerへ変換し、NULLは0に
  COALESCE(CAST(is_blocked AS INT), 0) AS is_blocked_flg,                       -- BooleanからIntegerへ変換し、NULLは0に
  COALESCE(CAST(is_friend AS INT), 0) AS is_friend_flg,                         -- BooleanからIntegerへ変換し、NULLは0に
  to_date(created_at) AS created_date,                                          -- タイムスタンプから日付型へ変換
  to_date(updated_at) AS updated_date                                           -- タイムスタンプから日付型へ変換
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_line_members
""")

# テーブル作成
silver_line_members_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_line_members')

display(silver_line_members_df.count())
display(silver_line_members_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_line_members'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_line_members'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'line_system_id',
    'line_member_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (line_system_id, line_member_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_line_members'

# テーブルコメント
comment = """
テーブル名：`silver_line_members / LINE会員マスタ`  
説明：LINE会員の基本情報と状態を管理します。会員の連携状況やアクセス履歴、通知設定やブロック状態、友だち登録状況などを追跡し、ユーザーの利用状況や関与度を把握するために使用されます。  
補足：
* 下記カラムを参照する外部キー  
`silver_users.line_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"line_system_id": "LINEシステムID、自動採番されるLINEシステムID",
	"line_member_id": "LINE会員ID、`silver_users.line_id`を参照する外部キー",
	"linked_date": "LINEアカウントと他システムの連携開始日、YYYY-MM-DDフォーマット",
	"last_accessed_date": "最終アクセス日付、YYYY-MM-DDフォーマット",
	"notification_allowed_flg": "LINE通知許諾フラグ、例:　1=許可, 0=拒否",
	"is_blocked_flg": "ブロックフラグ、例: 1=ブロック, 0=非ブロック",
	"is_friend_flg": "友達登録フラグ、例: 1=友達登録あり, 0=友達登録なし",
	"created_date": "作成日、YYYY-MM-DDフォーマット",
	"updated_date": "更新日、YYYY-MM-DDフォーマット"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2. silver_users / 会員

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, when

silver_users_df = spark.sql(f"""
SELECT DISTINCT
  user_id,
  name,
  gender,
  birth_date,
  CAST(FLOOR(datediff(current_date(), birth_date) / 365.25) AS INT) AS age,                     -- 年齢を計算して追加
  CAST(FLOOR(datediff(current_date(), birth_date) / 365.25 / 5) * 5 AS INT) AS age_group_5,     -- 5歳刻みの年代を計算して追加
  CAST(FLOOR(datediff(current_date(), birth_date) / 365.25 / 10) * 10 AS INT) AS age_group_10,  -- 10歳刻みの年代を計算して追加
  pref,
  phone_number,
  email,
  COALESCE(CAST(email_permission_flg AS INT), 0) AS email_permission_flg,                       -- BooleanからIntegerへ変換
  COALESCE(CAST(line_linked_flg AS INT), 0) AS line_linked_flg,                                 -- BooleanからIntegerへ変換
  line_member_id,
  to_date(registration_date) AS registration_date,                                              -- タイムスタンプから日付型へ変換
  status,
  to_date(last_login_at) AS last_login_date,                                                    -- タイムスタンプから日付型へ変換
  to_date(created_at) AS created_date,                                                          -- タイムスタンプから日付型へ変換
  to_date(updated_at) AS updated_date                                                           -- タイムスタンプから日付型へ変換
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_users
""")

# テーブル作成
silver_users_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_users')

display(silver_users_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_users'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_users'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = ['user_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (user_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,タグ登録
# 変数定義
TABLE_NAME = 'silver_users'
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.{TABLE_NAME}'  # テーブルパス
TAG_NAME = 'security'                                  # タグ名
TAG_VALUE = 'PII'                                      # タグの値

# 設定したいカラムのリスト
columns_to_set_tags = [
    'name',
    'phone_number',
    'email'
]

# 各カラムに対してタグを設定
for column in columns_to_set_tags:
    spark.sql(f'''
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET TAGS ('{TAG_NAME}' = '{TAG_VALUE}');
    ''')

# チェック（タグが正しく設定されたか確認）
display(
    spark.sql(f'''
    SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value
    FROM information_schema.column_tags
    WHERE table_name = '{TABLE_NAME}';
    '''))

# COMMAND ----------

spark.sql(f"""
WITH user_line_linked AS (
  SELECT
    u.user_id,
    u.line_linked_flg,
    u.line_member_id,
    l.is_blocked_flg,
    l.is_friend_flg
  FROM {MY_CATALOG}.{MY_SCHEMA}.silver_users u
  LEFT JOIN
    {MY_CATALOG}.{MY_SCHEMA}.silver_line_members l
  ON u.line_member_id = l.line_member_id  -- line_member_id で結合
)
SELECT
  ROUND(SUM(CASE WHEN user_line_linked.line_member_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS line_member_id_ratio,
  ROUND(SUM(CASE WHEN user_line_linked.is_blocked_flg IS NOT NULL AND user_line_linked.is_blocked_flg = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS line_is_blocked_ratio,
  ROUND(SUM(CASE WHEN user_line_linked.is_friend_flg IS NOT NULL AND user_line_linked.is_friend_flg = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS line_is_friend_ratio
FROM user_line_linked
""").display()

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_users'

# テーブルコメント
comment = """
テーブル名：`silver_users / 会員マスタ`  
説明：スーパーマーケット会員の基本情報（名前、性別、生年月日、住所、連絡先など）や会員ステータス、およびLINE連携状況を管理します。このデータは会員の利用状況や購買行動の分析、マーケティング活動の改善に活用されます。  
補足：
* 下記カラムを参照する外部キー  
`silver_line_members.line_member_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"user_id": "会員ID",
	"name": "氏名、文字列",
	"gender": "性別、文字列",
	"birth_date": "生年月日、YYYY-MM-DDフォーマット",
	"age": "年齢、例: 32",
	"age_group_5": "年代（5歳刻み）、例: 25",
	"age_group_10": "年代（10歳刻み）、例: 40",
	"pref": "居住県、例: 徳島県",
	"phone_number": "電話番号、例: 090-7147-1524",
	"email": "メールアドレス",
	"email_permission_flg": "メール許諾フラグ、例: 1=許可、0=拒否",
	"line_linked_flg": "LINE連携フラグ、例: 1=連携あり、0=連携なし、",
	"line_member_id": "LINE 会員ID、`silver_line_members.line_member_id`を参照する外部キー",
	"line_member_id": "会員登録日、YYYY-MM-DDフォーマット",
	"status": "会員ステータス、例: active='有効', inactive='無効'",
	"last_login_date": "最終ログイン日、YYYY-MM-DDフォーマット",
	"created_date": "作成日、YYYY-MM-DDフォーマット",
	"updated_date": "更新日、YYYY-MM-DDフォーマット"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-3. silver_items / 商品マスタ

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date

# SQLクエリを使用してデータクレンジング
silver_items_df = spark.sql(f"""
SELECT
  item_id,
  category_id,
  item_name,
  category_name,
  price,
  expiration_date,
  arrival_date,
  manufacturer_id,
  to_date(created_at) AS created_date,    -- タイムスタンプから日付型へ変換
  to_date(updated_at) AS updated_date     -- タイムスタンプから日付型へ変換
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_items
""")

# Deltaテーブルとして保存
silver_items_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_items')

display(silver_items_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_items'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_items'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'item_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (item_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_items'

# テーブルコメント
comment = """
テーブル名：`silver_items / 商品マスタ`  
説明：スーパーマーケットの商品に関する詳細データを管理します。商品ID、カテゴリID、商品名、価格、賞味期限、入荷日、メーカーID、作成・更新日時などが含まれます。  
補足：
* 下記カラムを参照する外部キー  
`silver_items.item_id`  
`silver_items.category_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"item_id": "商品ID、ユニーク（主キー）、`silver_items.item_id`を参照する外部キー",
	"category_id": "カテゴリID、`silver_items.category_id`を参照する外部キー",
	"item_name": "商品名、文字列",
	"category_name": "カテゴリ名、文字列",
	"price": "価格、例: 150",
	"expiration_date": "賞味期限、YYYY-MM-DDフォーマット",
	"arrival_date": "入荷日、YYYY-MM-DDフォーマット",
	"manufacturer_id": "メーカーID",
	"created_date": "作成日、YYYY-MM-DDフォーマット",
	"updated_date": "更新日、YYYY-MM-DDフォーマット"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-4. silver_stores / 店舗マスタ

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, col, when, expr

# SQLクエリを使用してデータクレンジング
silver_stores_df = spark.sql(f"""
SELECT
  store_id,
  store_name,
  store_area,
  address,
  postal_code,
  prefecture,
  city,
  phone_number,
  email,
  business_hours,
  closed_days,
  manager_id,
  opening_date,
  CAST(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END AS INT) AS status,   -- 文字列からIntegerへ変換
  to_date(created_at) AS created_date,                                    -- タイムスタンプから日付型へ変換
  to_date(updated_at) AS updated_date                                     -- タイムスタンプから日付型へ変換
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_stores
""")

# Deltaテーブルとして保存
silver_stores_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_stores')

display(silver_stores_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_stores'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_stores'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'store_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (store_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_stores'

# テーブルコメント
comment = """
テーブル名：`silver_stores / 店舗マスタ`  
説明：店舗の基本情報を管理します。店舗名、住所、連絡先、営業時間、店長ID、オープン日、ステータスなど店舗運営に必要な情報を格納します。  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"store_id": "店舗ID、ユニーク（主キー）",
	"store_name": "店舗名、文字列",
	"store_area": "店舗エリア、文字列",
	"address": "住所、文字列",
	"postal_code": "郵便番号、文字列",
	"prefecture": "都道府県、文字列",
	"city": "市区町村、文字列",
	"phone_number": "電話番号、文字列",
	"email": "メールアドレス、メールフォーマット",
	"business_hours": "営業時間、文字列、例 '月-金': '10:00-21:00'",
	"closed_days": "定休日、文字列、例 水曜日",
	"manager_id": "店長ID",
	"opening_date": "オープン日、YYYY-MM-DDフォーマット",
	"status": "ステータス、整数、例 1=有効、0=無効",
	"created_date": "作成日、YYYY-MM-DDフォーマット",
	"updated_date": "更新日、YYYY-MM-DDフォーマット"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-5. silver_staff / 店員マスタ

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, col, when

# SQLクエリを使用してデータクレンジング
silver_staff_df = spark.sql(f"""
SELECT
  staff_id,
  store_id,
  name,
  employee_number,
  email,
  phone_number,
  employment_type,
  hire_date,
  termination_date,
  CAST(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END AS INT) AS status,   -- 文字列からIntegerへ変換
  to_date(created_at) AS created_date,                                    -- タイムスタンプから日付型へ変換
  to_date(updated_at) AS updated_date                                     -- タイムスタンプから日付型へ変換
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_staff
""")

# Deltaテーブルとして保存
silver_staff_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_staff')

display(silver_staff_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_staff'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_staff'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'staff_id',
    'store_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (staff_id, store_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,タグ登録
# 変数定義
TABLE_NAME = 'silver_staff'
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.{TABLE_NAME}'  # テーブルパス
TAG_NAME = 'security'                                  # タグ名
TAG_VALUE = 'PII'                                      # タグの値

# 設定したいカラムのリスト
columns_to_set_tags = [
    'name',
    'email',
    'phone_number'
]

# 各カラムに対してタグを設定
for column in columns_to_set_tags:
    spark.sql(f'''
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET TAGS ('{TAG_NAME}' = '{TAG_VALUE}');
    ''')

# チェック（タグが正しく設定されたか確認）
display(
    spark.sql(f'''
    SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value
    FROM information_schema.column_tags
    WHERE table_name = '{TABLE_NAME}';
    '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_staff'

# テーブルコメント
comment = """
テーブル名：`silver_staff / 店員マスタ`  
説明：スーパーマーケットの従業員情報を管理します。店員ID、店舗ID、氏名、従業員番号、メールアドレス、雇用形態、入社日、退職日、ステータスなどを含みます。各店員は1店舗にのみ属します。  
補足：
* 下記カラムを参照する外部キー  
`silver_stores.store_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"staff_id": "店員ID、ユニーク（主キー）",
	"store_id": "店舗ID、`silver_stores.store_id`を参照する外部キー",
	"name": "名、文字列",
	"employee_number": "業員番号、文字列",
	"email": "ールアドレス、メールフォーマット",
	"phone_number": "話番号、文字列",
	"employment_type": "用形態、文字列、例　'FULL_TIME', 'PART_TIME', 'CONTRACT'",
	"hire_date": "社日、YYYY-MM-DDフォーマット",
	"termination_date": "職日、YYYY-MM-DDフォーマット",
	"status": "テータス、、整数、例 1=有効、0=無効",
	"created_date": "作成日、YYYY-MM-DDフォーマット",
	"updated_date": "更新日、YYYY-MM-DDフォーマット"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-6. silver_pos_orders_items / POS購買履歴（アイテム単位）

# COMMAND ----------

# MAGIC %md
# MAGIC キャンセルは除外

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, col, when

# SQLクエリを使用してデータクレンジング
silver_pos_orders_items_df = spark.sql(f"""
SELECT
  poi.order_item_id,                                        -- 明細ID
  poi.user_id,                                              -- 会員ID
  usr.name AS user_name,                                    -- 氏名
  usr.gender,                                               -- 性別
  usr.birth_date,                                           -- 生年月日
  usr.age,                                                  -- 年齢
  usr.age_group_5,                                          -- 年代（5歳刻み）
  usr.age_group_10,                                         -- 年代（10歳刻み）
  usr.pref,                                                 -- 居住県
  usr.email,                                                -- メールアドレス
  usr.email_permission_flg,                                 -- メール許諾フラグ
  usr.line_linked_flg,                                      -- LINE連携フラグ
  usr.line_member_id,                                       -- LINE会員ID
  usr.registration_date,                                    -- 会員登録日
  usr.status AS user_status,                                -- 会員ステータス
  poi.order_id,                                             -- 注文ID
  po.store_id,                                              -- 店舗ID
  str.store_name,                                           -- 店舗名
  str.store_area,                                           -- 店舗エリア
  str.address,                                              -- 住所
  po.staff_id,                                              -- 従業員ID
  stf.name AS staff_name,                                   -- 従業員名
  stf.employee_number AS staff_employee_number,             -- 従業員電話番号
  poi.item_id,                                              -- 商品ID
  poi.category_id,                                          -- カテゴリID
  poi.item_name,                                            -- 商品名
  poi.category_name,                                        -- カテゴリ名
  poi.quantity,                                             -- 数量
  poi.unit_price,                                           -- 単価
  poi.subtotal,                                             -- 小計
  COALESCE(CAST(poi.cancel_flg AS INT), 0) AS cancel_flg,   -- キャンセルフラグ
  to_date(poi.order_datetime) AS order_date,                -- 注文日
  to_date(poi.created_at) AS created_date,                  -- 作成日
  to_date(poi.updated_at) AS updated_date                   -- 更新日
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders_items poi
LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_users usr ON poi.user_id = usr.user_id
LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders po ON poi.order_id = po.order_id
LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.bronze_stores str ON po.store_id = str.store_id
LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.bronze_staff stf ON po.staff_id = stf.staff_id
WHERE
  poi.cancel_flg = FALSE -- キャンセル除外
""")

# Deltaテーブルとして保存
silver_pos_orders_items_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_pos_orders_items')

display(silver_pos_orders_items_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_pos_orders_items'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_pos_orders_items'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'order_item_id',
    'order_id',
    'item_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (order_item_id, order_id, item_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_pos_orders_items'

# テーブルコメント
comment = """
テーブル名：`silver_pos_orders_items / POS購買履歴（アイテム単位）`  
説明：スーパーマーケットのPOSシステムでの各商品ごとの購買履歴を管理します。購入した商品のID、数量、単価、サブトータル、注文日時などを含みます。キャンセルフラグを管理することで、返品やキャンセルの処理も支援します。  
補足：  
* 会計年度は4月1日から翌年3月31日まで
* 半期の定義
    * 第1四半期: 4月1日-6月30日
    * 第2四半期: 7月1日-9月30日
    * 第3四半期: 10月1日-12月31日
    * 第4四半期: 1月1日-3月31日
* 半期の定義
    * 上半期: 4月1日-9月30日
    * 下半期: 10月1日-3月31日
* 下記カラムを参照する外部キー  
`silver_users.user_id`  
`silver_pos_orders.order_id`  
`silver_pos_orders.store_id`  
`silver_items.item_id`  
`silver_categories.category_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"order_item_id": "明細ID、ユニーク（主キー）",
	"user_id": "会員ID、`silver_users.user_id`を参照する外部キー",
	"user_name": "氏名",
	"gender": "性別",
	"birth_date": "生年月日",
	"age": "年齢",
	"age_group_5": "年代（5歳刻み）",
	"age_group_10": "年代（10歳刻み）",
	"pref": "会員の居住県",
	"email": "メールアドレス",
	"email_permission_flg": "メール許諾フラグ",
	"line_linked_flg": "LINE連携フラグ",
	"line_member_id": "LINE 会員ID",
	"registration_date": "会員登録日",
	"user_status": "会員ステータス",
	"order_id": "注文ID、`silver_pos_orders.order_id`を参照する外部キー",
	"store_id": "店舗ID、`silver_pos_orders.store_id`を参照する外部キー",
	"store_name": "店舗名",
	"store_area": "店舗エリア",
	"staff_id": "従業員ID、`silver_staff.staff_id`を参照する外部キー",
	"staff_name": "従業員氏名",
	"staff_employee_number": "従業員番号",
	"item_id": "商品ID、`silver_items.item_id`を参照する外部キー",
	"category_id": "カテゴリID、`silver_categories.category_id`を参照する外部キー",
	"item_name": "商品名、文字列",
	"category_name": "カテゴリ名、文字列",
	"quantity": "数量、64ビット整数型、例 2",
	"unit_price": "単価、64ビット整数型、例 350",
	"subtotal": "小計、64ビット整数型、例 700",
	"cancel_flg": "キャンセルフラグ、整数、例 1=キャンセル、0=非キャンセル、キャンセルは除外済み",
	"order_date": "注文日、YYYY-MM-DDフォーマット",
	"created_date": "作成日、YYYY-MM-DDフォーマット",
	"updated_date": "更新日、YYYY-MM-DDフォーマット"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-7. silver_pos_orders / POS購買履歴（会計単位）

# COMMAND ----------

# MAGIC %md
# MAGIC キャンセルは除外

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, col, when

# SQLクエリを使用してデータクレンジング
silver_pos_orders_df = spark.sql(f"""
SELECT
  po.order_id,                                                       -- 注文ID
  po.user_id,                                                        -- 会員ID
  usr.name AS user_name,                                             -- 氏名
  usr.gender,                                                        -- 性別
  usr.birth_date,                                                    -- 生年月日
  usr.age,                                                           -- 年齢
  usr.age_group_5,                                                   -- 年代（5歳刻み）
  usr.age_group_10,                                                  -- 年代（10歳刻み）
  usr.pref,                                                          -- 居住県
  usr.email,                                                         -- メールアドレス
  usr.email_permission_flg,                                          -- メール許諾フラグ
  usr.line_linked_flg,                                               -- LINE連携フラグ
  usr.line_member_id,                                                -- LINE 会員ID
  usr.registration_date,                                             -- 会員登録日
  usr.status AS user_status,                                         -- 会員ステータス
  po.store_id,                                                       -- 店舗ID
  str.store_name,                                                    -- 店舗名
  str.store_area,                                                    -- 店舗エリア
  str.address,                                                       -- 住所
  po.staff_id,                                                       -- 店員ID
  stf.name AS staff_name,                                            -- 従業員名
  stf.employee_number AS staff_employee_number,                      -- 従業員電話番号
  po.total_amount,                                                   -- 合計金額（割引前）
  po.points_discount_amount,                                         -- ポイント割引額
  po.coupon_discount_amount,                                         -- クーポン割引額
  po.discount_amount,                                                -- 割引額（合計）
  po.payment_amount,                                                 -- 支払金額（割引後）
  po.payment_method,                                                 -- 支払い方法
  po.points_earned,                                                  -- 獲得ポイント
  COALESCE(CAST(po.coupon_used AS INT), 0) AS coupon_used,           -- クーポン使用フラグ
  po.receipt_number,                                                 -- レシート番号
  COALESCE(CAST(po.cancel_flg AS INT), 0) AS cancel_flg,             -- キャンセルフラグ
  to_date(po.order_datetime) AS order_date,                          -- 注文日
  to_date(po.created_at) AS created_date,                            -- 作成日
  to_date(po.updated_at) AS updated_date                             -- 更新日
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders po
LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.bronze_stores str ON po.store_id = str.store_id
LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_users usr ON po.user_id = usr.user_id
LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.bronze_staff stf ON po.staff_id = stf.staff_id
WHERE
  po.cancel_flg = FALSE
""")

# Deltaテーブルとして保存
silver_pos_orders_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_pos_orders')

# 結果の表示（最初の10行）
display(silver_pos_orders_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_pos_orders'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_pos_orders'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'order_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (order_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_pos_orders'

# テーブルコメント
comment = """
テーブル名：`silver_pos_orders / POS購買履歴（会計単位）`  
説明：スーパーマーケットのPOSシステムでの会計単位ごとの購買履歴を管理します。取引ID、店舗、取引日時、支払金額、割引額、獲得ポイントなどの情報を含み、支払い方法や店員ID、キャンセルフラグなども記録されます。  
補足：  
* 会計年度は4月1日から翌年3月31日まで
* 半期の定義
    * 第1四半期: 4月1日-6月30日
    * 第2四半期: 7月1日-9月30日
    * 第3四半期: 10月1日-12月31日
    * 第4四半期: 1月1日-3月31日
* 半期の定義
    * 上半期: 4月1日-9月30日
    * 下半期: 10月1日-3月31日
* 下記カラムを参照する外部キー  
`silver_users.user_id`  
`silver_stores.store_id`  
`silver_staff.staff_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"order_id": "注文ID、ユニーク（主キー）",
	"user_id": "会員ID、`silver_users.user_id`を参照する外部キー",
	"user_name": "氏名",
	"age": "性別",
	"birth_date": "生年月日",
	"age": "年齢",
	"age_group_5": "年代（5歳刻み）",
	"age_group_10": "年代（10歳刻み）",
	"pref": "会員の居住県",
	"email": "メールアドレス",
	"email_permission_flg": "メール許諾フラグ",
	"line_linked_flg": "LINE連携フラグ",
	"line_member_id": "LINE 会員ID",
	"registration_date": "会員登録日",
	"user_status": "会員ステータス",
	"store_id": "店舗ID、`silver_stores.store_id`を参照する外部キー",
	"store_name": "店舗名、例　大阪府東大阪市店",
	"store_area": "店舗エリア",
	"total_amount": "合計金額（割引前）",
	"points_discount_amount": "ポイント割引額",
	"coupon_discount_amount": "クーポン割引額",
	"discount_amount": "割引額（合計）",
	"payment_amount": "支払金額（割引後）",
	"payment_method": "支払い方法",
	"staff_id": "店員ID、`silver_staff.staff_id`を参照する外部キー",
	"staff_name": "従業員氏名",
	"staff_employee_number": "従業員番号",
	"points_earned": "獲得ポイント、整数",
	"coupon_used": "クーポン使用フラグ、整数、例 1=使用済み、0=未使用",
	"receipt_number": "レシート番号、文字列",
	"cancel_flg": "キャンセルフラグ、整数、例 1=キャンセル、0=非キャンセル",
	"order_date": "注文日、YYYY-MM-DDフォーマット",
	"created_date": "作成日、YYYY-MM-DDフォーマット",
	"updated_date": "更新日、YYYY-MM-DDフォーマット"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-8. silver_ec_order_items / EC購買履歴（アイテム単位）

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, col, when

# SQLクエリを使用してデータクレンジング
silver_ec_order_items_df = spark.sql(f"""
SELECT
  eoi.order_item_id,                                        -- 明細ID
  eoi.user_id,                                              -- 会員ID
  usr.name AS user_name,                                    -- 氏名
  usr.gender,                                               -- 性別
  usr.birth_date,                                           -- 生年月日
  usr.age,                                                  -- 年齢
  usr.age_group_5,                                          -- 年代（5歳刻み）
  usr.age_group_10,                                         -- 年代（10歳刻み）
  usr.pref,                                                 -- 居住県
  usr.email,                                                -- メールアドレス
  usr.email_permission_flg,                                 -- メール許諾フラグ
  usr.line_linked_flg,                                      -- LINE連携フラグ
  usr.line_member_id,                                       -- LINE会員ID
  usr.registration_date,                                    -- 会員登録日
  usr.status AS user_status,                                -- 会員ステータス
  eoi.order_id,                                             -- 注文ID
  eoi.cart_id,                                              -- カートID
  eoi.cart_item_id,                                         -- カートアイテムID
  eoi.item_id,                                              -- 商品ID
  eoi.category_id,                                          -- カテゴリID
  eoi.item_name,                                            -- 商品名
  eoi.category_name,                                        -- カテゴリ名
  eoi.quantity,                                             -- 数量
  eoi.unit_price,                                           -- 単価
  eoi.subtotal,                                             -- 小計
  eoi.total_amount,                                         -- 合計金額
  COALESCE(CAST(eoi.cancel_flg AS INT), 0) AS cancel_flg,   -- キャンセルフラグ
  to_date(eoi.cancelled_at) AS cancelled_date,              -- キャンセル日
  to_date(eoi.order_datetime) AS order_date,                -- 注文日
  to_date(eoi.created_at) AS created_date,                  -- 作成日
  to_date(eoi.updated_at) AS updated_date                   -- 更新日
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_ec_order_items eoi
LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_users usr ON eoi.user_id = usr.user_id
WHERE
  eoi.cancel_flg = FALSE -- キャンセル除外
""")

# Deltaテーブルとして保存
silver_ec_order_items_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_ec_order_items')

display(silver_ec_order_items_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_ec_order_items'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_ec_order_items'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'order_item_id',
    'cart_id',
    'cart_item_id',
    'item_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (order_item_id, cart_id, cart_item_id, item_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_ec_order_items'

# テーブルコメント
comment = """
テーブル名：`silver_ec_order_items / EC購買履歴（アイテム単位）`  
説明：スーパーマーケットのECサイトでの購入履歴をアイテム単位で管理します。注文ID、カートID、商品ID、カテゴリID、商品名、数量、単価、小計、合計金額、キャンセルフラグなどが含まれます。顧客の購入行動や商品動向の分析に活用され、オンライン販売の売上解析に役立ちます。  
補足：  
* 会計年度は4月1日から翌年3月31日まで
* 半期の定義
    * 第1四半期: 4月1日-6月30日
    * 第2四半期: 7月1日-9月30日
    * 第3四半期: 10月1日-12月31日
    * 第4四半期: 1月1日-3月31日
* 半期の定義
    * 上半期: 4月1日-9月30日
    * 下半期: 10月1日-3月31日
* 下記カラムを参照する外部キー  
`silver_users.user_id`  
`silver_stores.store_id`  
`silver_carts.cart_id`  
`silver_items.item_id`  
`silver_categories.category_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"order_item_id": "明細ID、ユニーク（主キー）",
	"user_id": "会員ID、`silver_users.user_id`を参照する外部キー",
	"user_name": "氏名",
	"gender": "性別",
	"birth_date": "生年月日",
	"age": "年齢",
	"age_group_5": "年代（5歳刻み）",
	"age_group_10": "年代（10歳刻み）",
	"pref": "会員の居住県",
	"email": "メールアドレス",
	"email_permission_flg": "メール許諾フラグ",
	"line_linked_flg": "LINE連携フラグ",
	"line_member_id": "LINE 会員ID",
	"registration_date": "会員登録日",
	"user_status": "会員ステータス",
	"order_id": "注文ID、`silver_stores.store_id`を参照する外部キー",
	"cart_id": "カートID、`silver_carts.cart_id`を参照する外部キー",
	"cart_item_id": "カートアイテムID",
	"item_id": "商品ID、`silver_items.item_id`を参照する外部キー",
	"category_id": "カテゴリID、`silver_categories.category_id`を参照する外部キー",
	"item_name": "商品名、文字列",
	"category_name": "カテゴリ名、文字列",
	"quantity": "数量",
	"unit_price": "単価",
	"subtotal": "小計",
	"total_amount": "合計金額",
	"cancel_flg": "キャンセルフラグ、整数、例 1=キャンセル、0=非キャンセル",
	"cancelled_date": "キャンセル日、YYYY-MM-DDフォーマット",
	"order_date": "取引日、YYYY-MM-DDフォーマット",
	"created_date": "作成日、YYYY-MM-DDフォーマット",
	"updated_date": "更新日、YYYY-MM-DDフォーマット"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-9. silver_ec_orders / EC購買履歴（会計単位）

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, col, when

# SQLクエリを使用してデータクレンジング
silver_ec_orders_df = spark.sql(f"""
SELECT
  eo.order_id,
  eo.cart_id,
  eo.user_id,
  usr.name AS user_name,                                      -- 氏名
  usr.gender,                                                 -- 性別
  usr.birth_date,                                             -- 生年月日
  usr.age,                                                    -- 年齢
  usr.age_group_5,                                            -- 年代（5歳刻み）
  usr.age_group_10,                                           -- 年代（10歳刻み）
  usr.pref,                                                   -- 居住県
  usr.email,                                                  -- メールアドレス
  usr.email_permission_flg,                                   -- メール許諾フラグ
  usr.line_linked_flg,                                        -- LINE連携フラグ
  usr.line_member_id,                                         -- LINE会員ID
  usr.registration_date,                                      -- 会員登録日
  usr.status AS user_status,                                  -- 会員ステータス
  eo.total_amount,                                            -- 合計金額（割引前）
  eo.points_discount_amount,                                  -- ポイント割引額
  eo.coupon_discount_amount,                                  -- クーポン割引額
  eo.discount_amount,                                         -- 割引額（合計）
  eo.payment_amount,                                          -- 支払金額（割引後）
  eo.shipping_fee,                                            -- 送料
  eo.payment_method,                                          -- 支払い方法
  eo.payment_status,                                          -- 支払い状況
  eo.shipping_method,                                         -- 配送方法
  eo.shipping_status,                                         -- 配送状況
  eo.shipping_address,                                        -- 配送先住所
  eo.shipping_name,                                           -- 配送先氏名
  eo.shipping_phone,                                          -- 配送先電話番号
  eo.points_earned,                                           -- 獲得ポイント
  COALESCE(CAST(eo.coupon_used AS INT), 0) AS coupon_used,    -- クーポン使用フラグ
  to_date(eo.cancelled_at) AS cancelled_date,                 -- キャンセル日
  to_date(eo.order_datetime) AS order_date,                   -- 注文日
  to_date(eo.created_at) AS created_date,                     -- 作成日
  to_date(eo.updated_at) AS updated_date                      -- 更新日
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_ec_orders eo
LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_users usr ON eo.user_id = usr.user_id
WHERE
  eo.cancel_flg = FALSE -- キャンセル除外
""")

# Deltaテーブルとして保存
silver_ec_orders_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_ec_orders')

display(silver_ec_orders_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_ec_orders'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_ec_orders'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'order_id',
    'cart_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (order_id, cart_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_ec_orders'

# テーブルコメント
comment = """
テーブル名：`silver_ec_orders / EC購買履歴（会計単位）`  
説明：スーパーマーケットのECサイトでの購入履歴を会計単位で管理します。注文の総額、割引額、支払額、配送状況などを含み、支払い方法や配送先情報も追跡します。  
補足：  
* 会計年度は4月1日から翌年3月31日まで
* 半期の定義
    * 第1四半期: 4月1日-6月30日
    * 第2四半期: 7月1日-9月30日
    * 第3四半期: 10月1日-12月31日
    * 第4四半期: 1月1日-3月31日
* 半期の定義
    * 上半期: 4月1日-9月30日
    * 下半期: 10月1日-3月31日
* 下記カラムを参照する外部キー  
`silver_users.user_id`  
`silver_carts.cart_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"order_id": "注文ID、ユニーク（主キー）",
	"cart_id": "カートID、`silver_carts.cart_id`を参照する外部キー",
	"user_id": "会員ID、`silver_users.user_id`を参照する外部キー",
	"user_name": "氏名",
	"gender": "性別",
	"birth_date": "生年月日",
	"age": "年齢",
	"age_group_5": "年代（5歳刻み）",
	"age_group_10": "年代（10歳刻み）",
	"pref": "会員の居住県",
	"email": "メールアドレス",
	"email_permission_flg": "メール許諾フラグ",
	"line_linked_flg": "LINE連携フラグ",
	"line_member_id": "LINE 会員ID",
	"registration_date": "会員登録日",
	"user_status": "会員ステータス",
	"total_amount": "合計金額（割引前）",
	"points_discount_amount": "ポイント割引額",
	"coupon_discount_amount": "クーポン割引額",
	"discount_amount": "割引額（合計）",
	"payment_amount": "支払金額（割引後）",
	"shipping_fee": "送料",
	"payment_method": "支払い方法",
	"payment_status": "支払い状況",
	"shipping_method": "配送方法",
	"shipping_status": "配送状況",
	"shipping_address": "配送先住所",
	"shipping_name": "配送先氏名",
	"shipping_phone": "配送先電話番号",
	"points_earned": "獲得ポイント",
	"coupon_used": "クーポン使用フラグ、整数、例 1=キャンセル、0=非キャンセル",
	"cancelled_date": "キャンセル日、YYYY-MM-DDフォーマット",
	"order_date": "注文日、YYYY-MM-DDフォーマット",
	"created_date": "作成日、YYYY-MM-DDフォーマット",
	"updated_date": "更新日、YYYY-MM-DDフォーマット"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-10. silver_inventory_history / 在庫履歴

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, col, when

# SQLクエリを使用してデータクレンジング
silver_inventory_history_df = spark.sql(f"""
SELECT
  inventory_history_id,
  item_id,
  store_id,
  quantity,
  inventory_type,
  to_date(inventory_datetime) AS inventory_date,  -- タイムスタンプから日付型へ変換
  update_reason,
  related_order_id,
  created_by,
  to_date(created_at) AS created_date,            -- タイムスタンプから日付型へ変換
  to_date(updated_at) AS updated_date             -- タイムスタンプから日付型へ変換
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_inventory_history
""")

# Deltaテーブルとして保存
silver_inventory_history_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_inventory_history')

# 結果の表示（最初の10行）
display(silver_inventory_history_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_inventory_history'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_inventory_history'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'inventory_history_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (inventory_history_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_inventory_history'

# テーブルコメント
comment = """
テーブル名：`silver_inventory_history / 在庫履歴`  
説明：スーパーマーケット商品の在庫履歴を記録します。在庫数量、更新理由、在庫タイプ（在庫、出荷、入荷など）や、関連する注文ID、店舗情報、スタッフ情報も追跡します。在庫の状態変動や更新日時を管理するために使用されます。  
補足：
* 下記カラムを参照する外部キー  
`silver_items.item_id`  
`silver_stores.store_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"inventory_history_id": "在庫履歴ID、ユニーク（主キー）",
	"item_id": "商品ID、`silver_items.item_id`を参照する外部キー",
	"store_id": "店舗ID、`silver_stores.store_id`を参照する外部キー",
	"quantity": "在庫数量、整数",
	"inventory_type": "在庫タイプ、文字列、例 '入荷', '出荷', '棚卸', '返品'",
	"inventory_date": "在庫日、YYYY-MM-DDフォーマット",
	"update_reason": "更新理由、文字列",
	"related_order_id": "関連注文ID",
	"created_by": "作成者ID",
	"created_date": "作成日、YYYY-MM-DDフォーマット",
	"updated_date": "更新日、YYYY-MM-DDフォーマット"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-11. silver_web_logs / Web行動履歴

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, col, when

# SQLクエリを使用してデータクレンジング
silver_web_logs_df = spark.sql(f"""
SELECT
  log_id,
  session_id,
  user_id,
  to_date(event_datetime) AS event_date,  -- タイムスタンプから日付型へ変換
  event_type,
  page_title,
  page_url,
  referrer_url,
  search_query,
  item_id,
  category_id,
  device_type,
  browser,
  os,
  ip_address,
  user_agent,
  time_spent,
  scroll_depth,
  click_position_x,
  click_position_y,
  utm_source,
  utm_medium,
  utm_campaign,
  to_date(created_at) AS created_date     -- タイムスタンプから日付型へ変換
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_web_logs
""")

# Deltaテーブルとして保存
silver_web_logs_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_web_logs')

# 結果の表示（最初の10行）
display(silver_web_logs_df.count())
display(silver_web_logs_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_web_logs'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_web_logs'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'log_id',
    'session_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (log_id, session_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_web_logs'

# テーブルコメント
comment = """
テーブル名：`silver_web_logs / Web行動履歴`  
説明：スーパーマーケットのECサイト上でのユーザーのWeb行動（ページビュー、クリック、検索、カート追加、購入）を記録します。各イベントには、セッションID、会員ID、日時、デバイス情報、検索クエリ、滞在時間、クリック位置などを含みます。  
補足：
* 下記カラムを参照する外部キー  
`silver_users.user_id`  
`silver_items.item_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"log_id": "ログID、ユニーク（主キー）",
	"session_id": "セッションID、JavaScriptでログイン時に紐付く",
	"user_id": "会員ID、`silver_users.user_id`を参照する外部キー",
	"event_date": "イベント日、YYYY-MM-DDフォーマット",
	"event_type": "イベントタイプ、文字列、例 'PAGE_VIEW', 'CLICK', 'SEARCH'",
	"page_title": "ページタイトル、文字列",
	"page_url": "ページURL、文字列",
	"referrer_url": "リファラーURL、文字列",
	"search_query": "検索キーワード、文字列",
	"item_id": "商品ID、`silver_items.item_id`を参照する外部キー",
	"category_id": "カテゴリID",
	"device_type": "デバイスタイプ、例 'DESKTOP', 'MOBILE', 'TABLET'",
	"browser": "ブラウザ、文字列、例 Chrome",
	"os": "S、文字列、例 iOS",
	"ip_address": "Pアドレス、文字列、例 192.168.1.1",
	"user_agent": "ユーザーエージェント、文字列、例 Mozilla/5.0 (iPhone; CPU iPhone OS like Mac OS X) ...",
	"time_spent": "ページ滞在時間、ページ滞在時間（秒）",
	"scroll_depth": "スクロール深度、ページスクロール深度（%）",
	"click_position_x": "クリック位置X、ページ内でクリックされた位置のX座標",
	"click_position_y": "クリック位置Y、ページ内でクリックされた位置のY座標",
	"utm_source": "TMソース、文字列、例 google",
	"utm_medium": "TMメディア、文字列、例 cpc",
	"utm_campaign": "TMキャンペーン、文字列、例 autumn_sale_2024 ",
	"created_date": "作成日、YYYY-MM-DDフォーマット"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-12. silver_cart_items / カートアイテム履歴

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, col, when

# SQLクエリを使用してデータクレンジング
silver_cart_items_df = spark.sql(f"""
SELECT
  cart_item_id,
  cart_id,
  item_id,
  quantity,
  unit_price,
  subtotal,
  session_id,
  user_id,
  to_date(added_at) AS added_date,      -- タイムスタンプから日付型へ変換
  to_date(updated_at) AS updated_date,  -- タイムスタンプから日付型へ変換
  CAST(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END AS INT) AS status  -- 文字列からIntegerへ変換
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_cart_items
""")

# Deltaテーブルとして保存
silver_cart_items_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_cart_items')

# 結果の表示（最初の10行）
display(silver_cart_items_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_cart_items'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_cart_items'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'cart_item_id',
    'cart_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (cart_item_id, cart_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_cart_items'

# テーブルコメント
comment = """
テーブル名：`silver_cart_items / カートアイテム履歴`  
説明：スーパーマーケットのECサイトのカートに追加された商品の履歴を記録します。カートID、商品ID、数量、単価、小計、会員ID、ステータスなどを含み、ユーザーのカート操作を追跡します。  
補足：
* 下記カラムを参照する外部キー  
`silver_carts.cart_id`  
`silver_items.item_id`  
`silver_web_logs.session_id`  
`silver_users.user_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"cart_item_id": "カートアイテムID、ユニーク（主キー）",
	"cart_id": "カートID、`silver_carts.cart_id`を参照する外部キー",
	"item_id": "商品ID、`silver_items.item_id`を参照する外部キー",
	"quantity": "数量",
	"unit_price": "単価",
	"subtotal": "小計",
	"session_id": "セッションID、JavaScriptでログイン時に紐付く、`silver_web_logs.session_id`を参照する外部キー",
	"user_id": "会員ID、`silver_users.user_id`を参照する外部キー",
	"added_date": "追加日、YYYY-MM-DDフォーマット",
	"updated_date": "更新日、YYYY-MM-DDフォーマット",
	"status": "ステータス、例 1: ACTIVE, 0: REMOVED"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-13. silver_carts / カート履歴

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, col, when

# SQLクエリを使用してデータクレンジング
silver_carts_df = spark.sql(f"""
SELECT
  cart_id,
  user_id,
  session_id,
  CASE 
    WHEN status = 'ACTIVE' THEN 1 
    WHEN status = 'ABANDONED' THEN 0 
    ELSE 0
  END AS status, -- ステータスを文字列からIntegerへ変換
  to_date(created_at) AS created_date,    -- タイムスタンプから日付型へ変換
  to_date(updated_at) AS updated_date,    -- タイムスタンプから日付型へ変換
  total_amount,
  item_count
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_carts
""")

# Deltaテーブルとして保存
silver_carts_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_carts')

# 結果の表示（最初の10行）
display(silver_carts_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_carts'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_carts'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'cart_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (cart_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_carts'

# テーブルコメント
comment = """
テーブル名：`silver_carts / カート履歴`  
説明：スーパーマーケットのECサイトにおけるカートの履歴を管理します。各カートのID、会員ID、セッションID、カートのステータス（例：ACTIVE、ABANDONED、CONVERTED）、作成日時、更新日時、合計金額、商品点数を記録し、カートの状態やユーザーの購買行動を追跡します。  
補足：
* 下記カラムを参照する外部キー  
`silver_carts.cart_id`  
`silver_users.user_id`  
`silver_web_logs.session_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"cart_id": "カートID、`silver_carts.cart_id`を参照する外部キー",
	"user_id": "会員ID、`silver_users.user_id`を参照する外部キー",
	"session_id": "セッションID、JavaScriptでログイン時に紐付く、`silver_web_logs.session_id`を参照する外部キー",
	"status": "ステータス、例 1: ACTIVE, 0: ABANDONED",
	"created_date": "作成日、YYYY-MM-DDフォーマット",
	"updated_date": "最終更新日、YYYY-MM-DDフォーマット",
	"total_amount": "合計金額",
	"item_count": "商品点数"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-14. silver_app_events / Appイベント履歴

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, col, when

# SQLクエリを使用してデータクレンジング
silver_app_events_df = spark.sql(f"""
SELECT
  event_id,
  session_id,
  user_id,
  to_date(event_datetime) AS event_date,  -- タイムスタンプから日付型へ変換
  event_type,
  screen_name,
  item_id,
  category_id,
  device_type,
  os,
  app_version,
  ip_address,
  cart_id,
  to_date(created_at) AS created_date     -- タイムスタンプから日付型へ変換
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_app_events
""")

# Deltaテーブルとして保存
silver_app_events_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_app_events')

# 結果の表示（最初の10行）
display(silver_app_events_df.count())
display(silver_app_events_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_app_events'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_app_events'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'event_id',
    'session_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (event_id, session_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_app_events'

# テーブルコメント
comment = """
テーブル名：`silver_app_events / Appイベント履歴`  
説明：スーパーマーケットアプリ内のユーザーイベント（画面遷移、クリック、カート追加、購入など）を記録します。ユーザーID、イベント日時、イベントタイプ、画面名、使用デバイス、OS、アプリバージョン、IPアドレス、カートID（関連イベントの場合）が含みます。  
補足：
* 下記カラムを参照する外部キー  
`silver_users.user_id`  
`silver_items.item_id`  
`silver_carts.cart_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"event_id": "イベントID、ユニーク（主キー）",
	"session_id": "セッションID",
	"user_id": "会員ID、`silver_users.user_id`を参照する外部キー",
	"event_date": "イベント日、YYYY-MM-DDフォーマット",
	"event_type": "イベントタイプ、文字列、例 'APP_OPEN', 'CLICK', 'PURCHASE'",
	"screen_name": "画面名、文字列",
	"item_id": "商品ID",
	"category_id": "カテゴリID、`silver_items.item_id`を参照する外部キー",
	"device_type": "デバイスタイプ、文字列、例 'PHONE', 'TABLET'",
	"os": "オペレーティングシステム、文字列、例 'iOS', 'Android'",
	"app_version": "アプリバージョン、文字列、例 '1.0.3'",
	"ip_address": "Pアドレス、文字列、アクセス元のIPアドレス、例 '192.168.1.1'",
	"cart_id": "カートID、文字列、`silver_carts.cart_id`を参照する外部キー",
	"created_date": "作成日、YYYY-MM-DDフォーマット"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-15. silver_loyalty_points / ロイヤリティプログラム

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import to_date, col, when

# SQLクエリを使用してデータクレンジング
silver_loyalty_points_df = spark.sql(f"""
SELECT
  point_id,
  user_id,
  point_balance,
  point_expiration_date,
  member_rank,
  points_to_next_rank,
  rank_expiration_date,
  CASE 
    WHEN status = 'ACTIVE' THEN 1 
    WHEN status = 'USED' THEN 0 
    ELSE 0 
  END AS status, -- ステータスを文字列からIntegerへ変換
  to_date(earned_at) AS earned_date,      -- タイムスタンプから日付型へ変換
  to_date(created_at) AS created_date,    -- タイムスタンプから日付型へ変換
  to_date(updated_at) AS updated_date     -- タイムスタンプから日付型へ変換
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_loyalty_points
""")

# Deltaテーブルとして保存
silver_loyalty_points_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_loyalty_points')

# 結果の表示（最初の10行）
display(silver_loyalty_points_df.limit(10))

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_loyalty_points'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_loyalty_points'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'point_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (point_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_loyalty_points'

# テーブルコメント
comment = """
テーブル名：`silver_loyalty_points / ロイヤリティプログラム`  
説明：スーパーマーケット会員のロイヤリティポイント履歴を管理します。ポイント残高、獲得日時、ランク、ステータスなどを記録し、会員のロイヤリティ状態を追跡します。  
補足：
* 下記カラムを参照する外部キー  
`silver_users.user_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"point_id": "ポイントID、ユニーク（主キー）",
	"user_id": "会員ID、`silver_users.user_id`を参照する外部キー",
	"point_balance": "ポイント残高、整数",
	"point_expiration_date": "ポイント有効期限、YYYY-MM-DDフォーマット",
	"member_rank": "会員ランク、文字列",
	"points_to_next_rank": "次の会員ランクまでの残ポイント、整数",
	"rank_expiration_date": "会員ランク有効期限、YYYY-MM-DDフォーマット",
	"status": "ステータス、整数、例 0: 有効、1: 無効（使用済み）",
	"earned_date": "獲得日、YYYY-MM-DDフォーマット",
	"created_date": "作成日、YYYY-MM-DDフォーマット",
	"updated_date": "更新日、YYYY-MM-DDフォーマット"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-16. silver_survey / アンケート回答履歴

# COMMAND ----------

# DBTITLE 1,Step1: TEMPテーブル作成
from pyspark.sql.functions import to_date, col, when

# SQLクエリを使用してデータクレンジング
silver_survey_df = spark.sql(f"""
SELECT
  response_id,
  user_id,
  survey_category,
  response_content,
  to_date(response_datetime) AS response_date,  -- タイムスタンプから日付型へ変換
  to_date(created_at) AS created_date           -- タイムスタンプから日付型へ変換
FROM
  {MY_CATALOG}.{MY_SCHEMA}.bronze_survey
""")

# # Deltaテーブルとして保存
# silver_survey_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_survey')

# 結果の表示（最初の10行）
display(silver_survey_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **問い合わせのポジネガスコアリング（1-5段階評価）**
# MAGIC
# MAGIC Hugging Face Transformersライブラリを使用([参考](https://qiita.com/ka201504/items/7e2c938b26c1494f332d))
# MAGIC - 多言語対応の感情分析に適した事前学習済みのBERTモデル
# MAGIC - モデル: [nlptown/bert-base-multilingual-uncased-sentiment](https://huggingface.co/gunkaynar/bert-base-multilingual-uncased-sentiment)

# COMMAND ----------

# DBTITLE 1,Step2: 感情スコア
from pyspark.sql.functions import col, udf, translate, pandas_udf
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from transformers import pipeline
import pandas as pd

# パイプライン作成
classifier = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")

schema = StructType([
    StructField("emotion", StringType(), True),
    StructField("probability", FloatType(), True)
])

# 感情分析を実行するPandas UDF定義
@pandas_udf(schema)
def analyze_sentiment_udf(text_series: pd.Series) -> pd.DataFrame:
    results = []
    for text in text_series:
        try:
            analysis = classifier(text)[0]
            results.append((analysis['label'], analysis['score']))
        except Exception as e:
            results.append(("unknown", 0.0))
    return pd.DataFrame(results, columns=['emotion', 'probability'])

# キャッシュを使用してfinal_dfを最適化
silver_survey_df.cache()

# COMMAND ----------

# DBTITLE 1,Step3: テーブル作成
# 感情分析とデータ処理を一度の操作で実行
survey_with_positive_score_df = (silver_survey_df
    .withColumn("sentiment", analyze_sentiment_udf(col("response_content")))
    .select(
        'response_id',
        'user_id',
        'survey_category',
        'response_content',
        col("sentiment.emotion").alias("emotion"),
        col("sentiment.probability").alias("probability"),
        'response_date',
        'created_date'
    )
    .withColumn("emotion_stars", translate(col("emotion"), " stars", "").cast("tinyint"))
    .withColumn("positive_score", col("emotion_stars") * col("probability"))
    .drop("emotion", "probability", "emotion_stars")
    .orderBy(col("positive_score").desc())
)

# テーブル作成
survey_with_positive_score_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.silver_survey')

display(survey_with_positive_score_df)

# COMMAND ----------

# DBTITLE 1,主キー設定
# 変数定義
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.silver_survey'                 # テーブルパス
PK_CONSTRAINT_NAME = f'pk_silver_survey'                               # 主キー

# NOT NULL制約の追加
columns_to_set_not_null = [
    'response_id']

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (response_id);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

# COMMAND ----------

# DBTITLE 1,コメント追加
# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_survey'

# テーブルコメント
comment = """
テーブル名：`silver_survey / アンケート回答履歴`  
説明：スーパーマーケット会員が回答したアンケートの履歴を管理します。各アンケートのカテゴリ、回答内容、回答日時、会員IDを記録し、顧客のフィードバックを分析するためのデータを提供します。ポジティブスコアも付与済み。  
補足：
* 下記カラムを参照する外部キー  
`silver_users.user_id`  
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
	"response_id": "回答ID、ユニーク（主キー）",
	"user_id": "会員ID、`silver_users.user_id`を参照する外部キー",
	"survey_category": "カテゴリ、文字列",
	"response_content": "回答内容、文字列",
	"response_date": "回答日、YYYY-MM-DDフォーマット",
	"created_date": "作成日、YYYY-MM-DDフォーマット",
	"positive_score": "ポジティブスコア、浮動小数展、範囲は1~5"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)
