# Databricks notebook source
# MAGIC %md
# MAGIC クラスタ：DBR 16.0 MLで実行してください

# COMMAND ----------

# MAGIC %md
# MAGIC # リテール企業を想定したCDPマートを作成
# MAGIC ## やること
# MAGIC - カタログ名だけご自身の作業用のカタログ名に書き換えて、本ノートブックを上から下まで流してください
# MAGIC - クラスタはDBR 16.0 ML以降で実行してください
# MAGIC - [テーブル定義書](https://docs.google.com/spreadsheets/d/19Og_NcmxG_fy7uenBC6EFIq-fEa6x8H5lNxqMJGmoh4/edit?gid=2046115905#gid=2046115905)に基づくテーブルを作ります
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
# MAGIC 該当スキーマ配下の既存bronzeテーブルを全て削除

# COMMAND ----------

# スキーマ内のすべてのテーブル名を取得する
tables_df = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}")

# テーブル名が "bronze_" で始まるテーブルのみ削除する
for table in tables_df.collect():
    table_name = table["tableName"]
    if table_name.startswith("bronze_"):
        spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{table_name}")
        print(f"削除されたテーブル: {table_name}")

print("全ての bronze_ で始まるテーブルが削除されました。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-1. bronze_line_members / LINE会員マスタ

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import col, when, lit, current_timestamp, rand, concat, lpad, row_number
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType, TimestampType
from pyspark.sql.window import Window

# ウィンドウ関数を定義
window_spec = Window.orderBy(lit(1))  # 全行を1つのウィンドウとして扱う

# LINE会員マスタのスキーマを定義（必要に応じて利用）
schema_line_members = StructType([
    StructField("line_system_id", LongType(), False),           # LINEシステムID
    StructField("line_member_id", StringType(), False),         # LINE会員ID
    StructField("linked_at", TimestampType(), False),           # 連携日時
    StructField("last_accessed_at", TimestampType(), True),     # 最終アクセス日時
    StructField("notification_allowed", BooleanType(), False),  # LINE通知許諾フラグ
    StructField("is_blocked", BooleanType(), False),            # ブロックフラグ
    StructField("is_friend", BooleanType(), False),             # 友だち登録フラグ
    StructField("created_at", TimestampType(), False),          # 作成日時
    StructField("updated_at", TimestampType(), True)            # 更新日時
])

# LINE会員マスタのデータを生成
line_members_df = (
    spark.range(1000)  # 1000件のレコード生成
    .withColumn("line_system_id", row_number().over(window_spec))                                           # LINEシステムID（ユニークな連番）
    .withColumn("line_member_id", concat(lit("U"), lpad(col("line_system_id").cast("string"), 10, "0")))    # LINE会員ID（主キー）
    .withColumn("linked_at", current_timestamp())                                                           # 連携日
    .withColumn("last_accessed_at", when(rand() > 0.2, current_timestamp()).otherwise(None))                # 最終アクセス日
    .withColumn("notification_allowed", (rand() > 0.3))                                                     # LINE通知許諾フラグ、30%が通知許諾
    .withColumn("is_blocked", when(rand() < 0.15, True).otherwise(False))                                   # ブロックフラグ
    .withColumn("is_friend", when(rand() < 0.3, True).otherwise(False))                                     # 友だち登録フラグ
    .withColumn("created_at", current_timestamp())                                                          # 作成日
    .withColumn("updated_at", current_timestamp())                                                          # 更新日
    .drop("id")
)

# Deltaテーブル作成
line_members_df.write.mode("overwrite").format("delta").saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_line_members')

# レコード数とサンプルデータの表示
display(line_members_df.count())
# display(line_members_df)
display(line_members_df.limit(10))

# COMMAND ----------

# DBTITLE 1,重複チェック
# 重複チェック
bronze_line_members_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_line_members")

# line_system_id の重複確認
line_system_id_duplicates = bronze_line_members_df.groupBy("line_system_id").count().filter("count > 1").count()

# line_id の重複確認
line_member_id_duplicates = bronze_line_members_df.groupBy("line_member_id").count().filter("count > 1").count()

print(f"LINEシステムIDの重複: {line_system_id_duplicates}")
print(f"LINE会員IDの重複: {line_member_id_duplicates}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-2. bronze_users / 会員マスタ

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import col, when, lit, expr, current_timestamp, rand, array
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, TimestampType
import random

# LINE会員マスタからLINE IDを取得
line_members_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_line_members")
line_ids = line_members_df.select("line_member_id").rdd.flatMap(lambda x: x).collect()

# 名前と性別のリストを作成
def generate_names():
    first_names_male = ['次郎', '健太', '翔太', '直樹', '大輔', '拓也', '悠斗', '誠', '亮', '和也', '徹', '一郎', '慎太郎', '裕樹', '哲也']
    first_names_female = ['花子', '美咲', '葵', 'さおり', '結衣', '菜々子', '美優', '玲奈', '愛子', '由美子', '恵美', '真由美', '千春', '陽子', '麻衣']
    last_names = ['田中', '佐藤', '鈴木', '高橋', '渡辺', '伊藤', '山田', '中村', '小林', '加藤']
    
    male_names = [(random.choice(last_names) + ' ' + random.choice(first_names_male), 'M') for _ in range(350)]     # 男性 350名
    female_names = [(random.choice(last_names) + ' ' + random.choice(first_names_female), 'F') for _ in range(650)] # 女性 650名
    
    return male_names + female_names

# 県のリストを生成
prefectures = [
    "東京都", "栃木県", "千葉県", "大阪府", "福島県", "長野県", "新潟県", "静岡県",
    "岐阜県", "群馬県", "茨城県", "宮城県", "石川県", "富山県", "滋賀県", "福井県",
    "神奈川県", "兵庫県", "和歌山県", "秋田県", "岡山県", "広島県", "愛媛県", "長崎県",
    "北海道"
]

# スキーマ定義
schema = StructType([
    StructField("user_id", StringType(), False),                # 会員ID
    StructField("name", StringType(), False),                   # 氏名
    StructField("gender", StringType(), True),                  # 性別
    StructField("birth_date", DateType(), True),                # 生年月日
    StructField("pref", StringType(), True),                    # 居住県
    StructField("phone_number", StringType(), True),            # 電話番号
    StructField("email", StringType(), False),                  # メールアドレス
    StructField("email_permission_flg", BooleanType(), False),  # メール許諾フラグ
    StructField("line_linked_flg", BooleanType(), False),       # LINE連携フラグ
    # StructField("line_member_id", StringType(), False),         # LINE ID -> 後から特定の割合で付与するのでここではカラム含めない
    StructField("registration_date", DateType(), False),        # 会員登録日
    StructField("status", StringType(), False),                 # 会員ステータス
    StructField("last_login_at", TimestampType(), True),        # 最終ログイン日時
    StructField("created_at", TimestampType(), False),          # 作成日時
    StructField("updated_at", TimestampType(), True)            # 更新日時
])

# 基本的なユーザーデータを生成
names_gender = generate_names()

user_df = spark.createDataFrame([
    (f'cdp{str(i+1).zfill(12)}', name, gender)
    for i, (name, gender) in enumerate(names_gender)
], ["user_id", "name", "gender"])

# その他のカラムを追加
prefectures_array = ",".join([f"'{p}'" for p in prefectures])
pref_expr = f"array({prefectures_array})[int(rand() * {len(prefectures)})]"

# 生年月日を生成する際、10歳未満を除外する
# 現在の日付から最低でも10年（3650日）前の誕生日を設定
user_df = user_df.withColumn("birth_date", expr("date_sub(current_date(), int(rand() * (18250 - 3650) + 3650))"))
user_df = user_df.withColumn("pref", expr(pref_expr))
user_df = user_df.withColumn("phone_number", expr("concat(lpad(cast(rand() * 900 + 100 as int), 3, '0'), '-', lpad(cast(rand() * 9000 + 1000 as int), 4, '0'), '-', lpad(cast(rand() * 9000 + 1000 as int), 4, '0'))"))
user_df = user_df.withColumn("email", expr("concat('user', lpad(cast(rand() * 10000 as int), 5, '0'), '@example.com')"))
user_df = user_df.withColumn("email_permission_flg", (rand() < 0.35).cast("boolean"))
user_df = user_df.withColumn("line_linked_flg", (rand() < 0.42).cast("boolean"))
user_df = user_df.withColumn("registration_date", expr("date_sub(current_date(), int(rand() * 2190))"))
user_df = user_df.withColumn("status", when(rand() < 0.75, "active").otherwise("inactive"))
user_df = user_df.withColumn("last_login_at", when(rand() < 0.3, current_timestamp()).otherwise(None))
user_df = user_df.withColumn("created_at", col("registration_date").cast("timestamp"))
user_df = user_df.withColumn("updated_at", col("created_at"))

# LINE IDを割り当て
line_id_broadcast = spark.sparkContext.broadcast(line_ids)
line_id_array = ",".join([f"'{id}'" for id in line_id_broadcast.value])
line_id_expr = f"array({line_id_array})[int(rand() * {len(line_id_broadcast.value)})]"
user_df = user_df.withColumn("line_member_id", when(col("line_linked_flg"), expr(line_id_expr)).otherwise(None))

# テーブル作成
user_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_users')

display(user_df.count())
display(user_df.limit(10))

# COMMAND ----------

# DBTITLE 1,タグ登録
# 変数定義
TABLE_NAME = 'bronze_users'
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

# DBTITLE 1,重複チェック
# 重複チェック
bronze_users_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_users")

# user_id の重複確認
user_id_duplicates = bronze_users_df.groupBy("user_id").count().filter("count > 1").count()

print(f"会員IDの重複: {user_id_duplicates}")

# COMMAND ----------

# DBTITLE 1,LINE ID紐付率・ブロック率・友達率チェック
spark.sql(f"""
WITH user_line_linked AS (
  SELECT
    u.user_id,
    u.line_linked_flg,
    u.line_member_id,
    l.is_blocked,
    l.is_friend
  FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_users u
  LEFT JOIN
    {MY_CATALOG}.{MY_SCHEMA}.bronze_line_members l
  ON u.line_member_id = l.line_member_id
)
SELECT
  ROUND(SUM(CASE WHEN user_line_linked.line_member_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS line_member_id_ratio,
  ROUND(SUM(CASE WHEN user_line_linked.is_blocked = True THEN 1 ELSE 0 END) / COUNT(*), 2) AS line_is_blocked_ratio,
  ROUND(SUM(CASE WHEN user_line_linked.is_friend = True THEN 1 ELSE 0 END) / COUNT(*), 2) AS line_is_friend_ratio
FROM user_line_linked
""").display()

# COMMAND ----------

# DBTITLE 1,LINE連携率、メール許諾チェック
from pyspark.sql.functions import count, sum, when, col, round

# 集計を行う
summary = user_df.agg(
    count("user_id").alias("total_users"),
    sum(when(col("line_linked_flg") == True, 1).otherwise(0)).alias("line_linked_users"),
    sum(when(col("email_permission_flg") == True, 1).otherwise(0)).alias("email_permission_users")
)

# 比率を計算
summary = summary.withColumn("line_linked_rate", round(col("line_linked_users") / col("total_users") * 100, 2))
summary = summary.withColumn("email_permission_rate", round(col("email_permission_users") / col("total_users") * 100, 2))

# 結果を表示
display(summary.select(
    col("total_users").alias("会員数"),
    col("line_linked_users").alias("LINE連携数"),
    col("line_linked_rate").alias("LINE連携率(%)"),
    col("email_permission_users").alias("メール許諾数"),
    col("email_permission_rate").alias("メール許諾率(%)")
))

# COMMAND ----------

# DBTITLE 1,居住県チェック
# from pyspark.sql.functions import count, countDistinct

# # 県ごとの件数を集計
# user_df_count = (user_df
#           .groupBy("pref")
#           .agg(count("*").alias("cnt"))
#           .orderBy("cnt", ascending= False))

# display(user_df_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-3. bronze_items / 商品マスタ

# COMMAND ----------

# DBTITLE 1,テーブル作成
import random
from datetime import datetime, timedelta
from decimal import Decimal
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType, DateType, TimestampType

# 商品カテゴリと商品名のリスト
categories = {
    100: ("野菜・果物", ["トマト", "きゅうり", "ほうれん草", "キャベツ", "大根", "にんじん", "じゃがいも", "玉ねぎ", "りんご", "みかん"]),
    200: ("肉類", ["鶏もも肉", "豚バラ肉", "牛ステーキ", "合挽き肉", "鶏むね肉", "豚ロース肉", "牛ひき肉", "ササミ", "鴨肉", "ラムチョップ"]),
    300: ("魚介類", ["鮭の切り身", "マグロの刺身", "アサリ", "エビ", "イカ", "カツオ", "タコ", "ホタテ", "サンマ", "ブリ"]),
    400: ("乳製品", ["牛乳", "チーズ", "バター", "ヨーグルト", "生クリーム", "カッテージチーズ", "クリームチーズ", "カルピス", "チョコレートミルク", "ヤクルト"]),
    500: ("パン・ベーカリー", ["食パン", "クロワッサン", "ロールパン", "バターロール", "フランスパン", "デニッシュ", "ベーグル", "サンドイッチ", "パンケーキ", "ピザパン"]),
    600: ("お菓子・スナック", ["ポテトチップス", "クッキー", "チョコレート", "キャンディー", "クラッカー", "あられ", "グミ", "スナック菓子", "ポップコーン", "プリン"]),
    700: ("冷凍食品", ["冷凍餃子", "冷凍ピザ", "冷凍野菜", "アイスクリーム", "冷凍唐揚げ", "冷凍たこ焼き", "冷凍チャーハン", "冷凍パスタ", "冷凍フライドポテト", "冷凍カレー"]),
    800: ("飲料", ["ミネラルウォーター", "コーヒー", "紅茶", "ジュース", "炭酸飲料", "スポーツドリンク", "お茶", "野菜ジュース", "エナジードリンク", "ココア"]),
    900: ("調味料", ["醤油", "味噌", "マヨネーズ", "塩", "胡椒", "砂糖", "酢", "オリーブオイル", "ソース", "だしの素"]),
    1000: ("インスタント食品", ["カップラーメン", "インスタントカレー", "レトルトスープ", "即席みそ汁", "レトルトパスタソース", "インスタント焼きそば", "即席おかゆ", "インスタント蕎麦", "インスタントおにぎり", "カップスープ"]),
    1100: ("日用品", ["トイレットペーパー", "ティッシュペーパー", "ハンドソープ", "洗剤", "スポンジ", "シャンプー", "歯ブラシ", "歯磨き粉", "柔軟剤", "キッチンペーパー"]),
    1200: ("ペット用品", ["ドッグフード", "キャットフード", "ペットおやつ", "ペット用シャンプー", "キャットトイレ砂", "ペット用おもちゃ", "ペットシーツ", "ケージ", "リード", "ペット用ベッド"]),
    1300: ("健康・美容", ["ビタミン剤", "プロテイン", "サプリメント", "スキンケアクリーム", "シャンプー", "リップクリーム", "化粧水", "日焼け止め", "フェイスマスク", "保湿クリーム"]),
    1400: ("ベビー用品", ["おむつ", "ベビーフード", "ミルク", "おしりふき", "哺乳瓶", "ベビーローション", "ベビーパウダー", "ベビーソープ", "ベビー用歯ブラシ", "ベビービブ"]),
    1500: ("酒類", ["ビール", "ワイン", "焼酎", "日本酒", "ウイスキー", "カクテルベース", "サワー", "スピリッツ", "シードル", "リキュール"]),
    1600: ("乾物・缶詰", ["乾燥わかめ", "のり", "ひじき", "ツナ缶", "コーン缶", "サバ缶", "トマト缶", "豆の缶詰", "果物缶", "スープ缶"]),
    1700: ("冷凍スイーツ", ["アイスクリーム", "シャーベット", "冷凍ケーキ", "冷凍たい焼き", "冷凍どら焼き", "冷凍ワッフル", "冷凍クレープ", "冷凍まんじゅう", "冷凍フルーツバー", "冷凍パフェ"]),
    1800: ("米・パン・麺類", ["白米", "玄米", "クロワッサン", "ベーグル", "乾麺うどん", "乾麺そうめん", "パスタ", "冷凍ごはん", "もち", "うどん"]),
    1900: ("キッチン用品", ["ラップ", "アルミホイル", "調理用ハサミ", "ピーラー", "おろし器", "計量カップ", "鍋", "フライパン", "包丁", "保存容器"]),
    2000: ("清涼飲料・嗜好飲料", ["炭酸水", "麦茶", "緑茶", "トマトジュース", "レモンジュース", "フルーツジュース", "ブラックコーヒー", "ミルクティー", "栄養ドリンク", "甘酒"]),
    2100: ("冷凍フルーツ・野菜", ["冷凍ブルーベリー", "冷凍ストロベリー", "冷凍ブロッコリー", "冷凍アスパラガス", "冷凍かぼちゃ", "冷凍スイートコーン", "冷凍マンゴー", "冷凍チェリー", "冷凍ミックスベジタブル", "冷凍枝豆"]),
}

# メーカーIDリスト (仮定のメーカー)
manufacturer_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# 商品マスタのスキーマ定義
schema_items = StructType([
    StructField("item_id", LongType(), False),              # 商品ID
    StructField("category_id", LongType(), False),          # カテゴリID
    StructField("item_name", StringType(), False),          # 商品名
    StructField("category_name", StringType(), False),      # カテゴリ名
    StructField("price", LongType(), False),                # 価格 (BIGINT)
    StructField("expiration_date", DateType(), True),       # 賞味期限
    StructField("arrival_date", DateType(), True),          # 入荷日
    StructField("manufacturer_id", LongType(), True),       # メーカーID
    StructField("created_at", TimestampType(), False),      # 作成日時
    StructField("updated_at", TimestampType(), False)       # 更新日時
])

# 賞味期限の生成関数（ランダムな未来日付）
def random_future_date():
    return datetime.today().date() + timedelta(days=random.randint(30, 365))

# 入荷日の生成関数（ランダムな過去日付）
def random_past_date():
    return datetime.today().date() - timedelta(days=random.randint(1, 180))

# サンプルデータ生成
items_data = []
item_id = 1001
for category_id, (category_name, item_names) in categories.items():
    for item_name in item_names:
        price = random.randint(100, 1000)                                       # 価格を整数でランダムに設定
        expiration_date = random_future_date() if category_id < 900 else None   # 調味料や日用品は賞味期限なし
        arrival_date = random_past_date()
        manufacturer_id = random.choice(manufacturer_ids)
        created_at = datetime.now()
        updated_at = created_at

        items_data.append((
            item_id,            # 商品ID
            category_id,        # カテゴリID
            item_name,          # 商品名
            category_name,      # カテゴリ名
            price,              # 価格
            expiration_date,    # 賞味期限
            arrival_date,       # 入荷日
            manufacturer_id,    # メーカーID
            created_at,         # 作成日時
            updated_at          # 更新日時
        ))
        item_id += 1

# DataFrameの作成
items_df = spark.createDataFrame(items_data, schema=schema_items)

# Deltaテーブルとして保存
items_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_items')

display(items_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-4. bronze_stores / 店舗マスタ

# COMMAND ----------

# DBTITLE 1,テーブル作成
import random
import string
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType, DateType, TimestampType
from datetime import datetime, timedelta

# 店舗エリアと都道府県のデータ
prefecture_store_counts = {
    "東京都": {"count": 20, "area": "関東地方"},
    "神奈川県": {"count": 10, "area": "関東地方"},
    "千葉県": {"count": 5, "area": "関東地方"},
    "埼玉県": {"count": 5, "area": "関東地方"},
    "大阪府": {"count": 7, "area": "関西地方"},
    "兵庫県": {"count": 5, "area": "関西地方"},
    "愛知県": {"count": 5, "area": "中部地方"},
    "北海道": {"count": 5, "area": "北海道"}
}

cities_by_prefecture = {
    "東京都": ["渋谷区", "新宿区", "中央区", "港区", "中野区", "豊島区", "江東区", "品川区", "目黒区"],
    "神奈川県": ["横浜市", "川崎市", "相模原市", "藤沢市", "厚木市"],
    "千葉県": ["千葉市", "船橋市", "松戸市", "市川市", "柏市"],
    "埼玉県": ["さいたま市", "川口市", "川越市", "所沢市", "熊谷市"],
    "大阪府": ["大阪市", "堺市", "高槻市", "枚方市", "東大阪市", "吹田市", "茨木市"],
    "兵庫県": ["神戸市", "姫路市", "西宮市", "尼崎市"],
    "愛知県": ["名古屋市", "豊橋市", "岡崎市"],
    "北海道": ["札幌市"]
}

# ランダムなアルファベット文字列を生成する関数
def generate_random_string(length=8):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(length))

# closed_daysに基づいてbusiness_hoursを生成する関数
def generate_business_hours(closed_day):
    if closed_day == "なし":
        return '"月-金": "10:00-21:00", "土-日": "11:00-20:00"'
    elif closed_day == "日曜日":
        return '"月-土": "10:00-21:00"'
    elif closed_day == "月曜日":
        return '"火-日": "10:00-21:00"'
    elif closed_day == "火曜日":
        return '"月、水-日": "10:00-21:00"'
    elif closed_day == "水曜日":
        return '"月-火、木-日": "10:00-21:00"'
    else:
        return '"月-金": "10:00-21:00", "土": "11:00-20:00"'

# サンプルデータを直接DataFrameに追加する
store_data = []

store_id = 1
for prefecture, details in prefecture_store_counts.items():
    cities = cities_by_prefecture[prefecture]
    for _ in range(details["count"]):
        store_name = f'{prefecture}{random.choice(cities)}店'
        address = f'{prefecture}{random.choice(cities)}神南{random.randint(1, 5)}-{random.randint(1, 20)}-{random.randint(1, 20)}'
        postal_code = f'150-{random.randint(1000, 9999)}'
        phone_number = f'03-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}'
        email = f'{generate_random_string()}@example.com'
        closed_days = random.choice(["水曜日", "月曜日", "日曜日", "なし", "火曜日"])
        business_hours = generate_business_hours(closed_days)
        manager_id = random.randint(100, 999)
        opening_date = datetime(2020, random.randint(1, 12), random.randint(1, 28)).date()
        status = 'ACTIVE' if random.random() < 0.95 else 'INACTIVE'
        created_at = datetime.now() - timedelta(days=random.randint(0, 365))
        updated_at = created_at + timedelta(days=random.randint(30, 300))

        store_data.append((
            store_id, store_name, details["area"], address, postal_code, prefecture, random.choice(cities),
            phone_number, email, business_hours, closed_days, manager_id, opening_date, status, created_at, updated_at
        ))

        store_id += 1

# 店舗データのスキーマ定義
schema_stores = StructType([
    StructField("store_id", LongType(), False),           # 店舗ID
    StructField("store_name", StringType(), False),       # 店舗名
    StructField("store_area", StringType(), False),       # 店舗エリア
    StructField("address", StringType(), True),           # 住所
    StructField("postal_code", StringType(), True),       # 郵便番号
    StructField("prefecture", StringType(), True),        # 都道府県
    StructField("city", StringType(), True),              # 市区町村
    StructField("phone_number", StringType(), True),      # 電話番号
    StructField("email", StringType(), True),             # メールアドレス
    StructField("business_hours", StringType(), True),    # 営業時間
    StructField("closed_days", StringType(), True),       # 定休日
    StructField("manager_id", LongType(), True),          # 店長ID
    StructField("opening_date", DateType(), True),        # オープン日
    StructField("status", StringType(), False),           # ステータス('ACTIVE', 'INACTIVE')
    StructField("created_at", TimestampType(), False),    # 作成日時
    StructField("updated_at", TimestampType(), False)     # 更新日時
])

# Spark DataFrameに変換
store_df = spark.createDataFrame(store_data, schema_stores)

# Deltaテーブルに保存
store_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_stores')

display(store_df.limit(10))

# COMMAND ----------

# DBTITLE 1,アクティブ店舗チェック
# from pyspark.sql.functions import col

# # ステータスが"ACTIVE"の店舗数を集計
# active_store_count = store_df.filter(col("status") == "ACTIVE").count()

# print(f"ステータスがアクティブな店舗数: {active_store_count}")

# COMMAND ----------

# DBTITLE 1,店舗県チェック
# from pyspark.sql import functions as F

# # 県ごとの店舗数を集計
# prefecture_store_count_df = store_df.groupBy("prefecture").agg(
#     F.count("*").alias("store_count")
# ).orderBy(F.col("store_count").desc())

# # 集計結果を表示
# display(prefecture_store_count_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-5. bronze_staff / 店員マスタ

# COMMAND ----------

# DBTITLE 1,テーブル作成
import random
from datetime import datetime, timedelta, date
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, TimestampType

# 店員データを生成する関数
def generate_balanced_staff_data(store_df, total_staff, min_staff_per_store):
    # アクティブな店舗だけを選択
    active_store_ids = store_df.filter(store_df.status == "ACTIVE").select("store_id").rdd.flatMap(lambda x: x).collect()
    employment_types = ["FULL_TIME", "PART_TIME", "CONTRACT"]
    statuses = ["ACTIVE", "INACTIVE"]
    
    staff_data = []
    staff_id = 1001

    # 各店舗に最低人数を割り当て、その後余った店員をランダムに配分
    for store_id in active_store_ids:
        # 各店舗に最低 min_staff_per_store 人を割り当てる
        for _ in range(min_staff_per_store):
            name = f'山田 太郎{staff_id}'  # サンプル名
            employee_number = f'EMP-{str(staff_id+1234).zfill(6)}'
            email = f'taro.yamada{staff_id}@example.com'
            phone_number = f'090-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}'
            employment_type = random.choice(employment_types)
            hire_date = date(2020, random.randint(1, 12), random.randint(1, 28))
            termination_date = None if random.random() < 0.9 else date(2024, random.randint(1, 9), random.randint(1, 28))  # 90%は退職していない
            status = 'ACTIVE' if random.random() < 0.9 else random.choice(statuses[1:])
            created_at = datetime(2024, random.randint(1, 9), random.randint(1, 28), random.randint(8, 18), random.randint(0, 59), random.randint(0, 59))
            updated_at = created_at + timedelta(days=random.randint(30, 300))

            staff_data.append((
                staff_id, store_id, name, employee_number, email, phone_number, employment_type, hire_date, termination_date, status, created_at, updated_at
            ))
            staff_id += 1

    # 残りの店員をランダムにアクティブな店舗に追加配分
    remaining_staff = total_staff - len(active_store_ids) * min_staff_per_store
    for _ in range(remaining_staff):
        store_id = random.choice(active_store_ids)
        name = f'山田 太郎{staff_id}'  # サンプル名
        employee_number = f'EMP-{str(staff_id+1234).zfill(6)}'
        email = f'taro.yamada{staff_id}@example.com'
        phone_number = f'090-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}'
        employment_type = random.choice(employment_types)
        hire_date = date(2020, random.randint(1, 12), random.randint(1, 28))
        termination_date = None if random.random() < 0.9 else date(2024, random.randint(1, 9), random.randint(1, 28))  # 90%は退職していない
        status = 'ACTIVE' if random.random() < 0.9 else random.choice(statuses[1:])
        created_at = datetime(2024, random.randint(1, 9), random.randint(1, 28), random.randint(8, 18), random.randint(0, 59), random.randint(0, 59))
        updated_at = created_at + timedelta(days=random.randint(30, 300))

        staff_data.append((
            staff_id, store_id, name, employee_number, email, phone_number, employment_type, hire_date, termination_date, status, created_at, updated_at
        ))
        staff_id += 1

    return staff_data

# スキーマ定義
schema_staff = StructType([
    StructField("staff_id", LongType(), False),                # 店員ID
    StructField("store_id", LongType(), False),                # 店舗ID
    StructField("name", StringType(), False),                  # 氏名
    StructField("employee_number", StringType(), False),       # 従業員番号
    StructField("email", StringType(), False),                 # メールアドレス
    StructField("phone_number", StringType(), True),           # 電話番号
    StructField("employment_type", StringType(), False),       # 雇用形態
    StructField("hire_date", DateType(), False),               # 入社日
    StructField("termination_date", DateType(), True),         # 退職日
    StructField("status", StringType(), False),                # ステータス('ACTIVE', 'INACTIVE')
    StructField("created_at", TimestampType(), False),         # 作成日時
    StructField("updated_at", TimestampType(), False)          # 更新日時
])

# サンプルデータ生成
staff_data = generate_balanced_staff_data(store_df, total_staff=1000, min_staff_per_store=10)

# データフレーム作成
staff_df = spark.createDataFrame(staff_data, schema_staff)

# テーブル作成
staff_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_staff')

display(staff_df.limit(10))

# COMMAND ----------

# DBTITLE 1,所属店舗の店員数チェック
# # 店員データと店舗データを結合して、店舗ごとの店員数を集計
# # SQLクエリを使用して、店舗ごとの店員数を集計し、降順にソート

# spark.sql(f"""
#     SELECT 
#         store_name,  -- 店舗名
#         COUNT(staff_id) AS staff_count  -- 店舗ごとの店員数を集計
#     FROM 
#         {MY_CATALOG}.{MY_SCHEMA}.bronze_staff AS s
#     JOIN 
#         {MY_CATALOG}.{MY_SCHEMA}.bronze_stores AS st ON s.store_id = st.store_id
#     GROUP BY 
#         store_name
#     ORDER BY 
#         staff_count DESC
# """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-6. bronze_cart_items / カートアイテム履歴

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import col, rand, current_timestamp
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType
import random
from datetime import datetime, timedelta

# 商品マスタから商品IDを取得
items_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_items')
item_ids = [row.item_id for row in items_df.select('item_id').distinct().collect()]

# 会員マスタから会員IDを取得
user_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_users')
user_ids = [row.user_id for row in user_df.select('user_id').distinct().collect()]

# ランダムな日時を生成する関数
def random_datetime(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )

# アイテム数の上限割合に従ってカートごとのアイテム数を割り当てる関数
def assign_items_distribution(cart_ids):
    cart_items_distribution = {}
    for cart_id in cart_ids:
        rand_val = random.random()
        if rand_val < 0.5:
            cart_items_distribution[cart_id] = random.randint(1, 4)  # 5個未満
        elif rand_val < 0.7:
            cart_items_distribution[cart_id] = random.randint(5, 9)  # 5個以上10個未満
        elif rand_val < 0.9:
            cart_items_distribution[cart_id] = random.randint(10, 14)  # 10個以上15個未満
        else:
            cart_items_distribution[cart_id] = random.randint(15, 20)  # 15個以上20個以下
    return cart_items_distribution

# サンプルデータの生成設定
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)
num_records = 10000  # 作成するカートアイテムの数
num_unique_carts = 1000  # 使用するユニークなカートIDの数

# ユニークなカートIDのリストを準備し、カートごとのアイテム数を事前に割り当て
cart_ids = [random.randint(10001, 20000) for _ in range(num_unique_carts)]
cart_items_distribution = assign_items_distribution(cart_ids)

# カートアイテムデータ生成
cart_items_data = []
cart_item_id = 100001

for cart_id, max_items in cart_items_distribution.items():
    for _ in range(max_items):
        added_at = random_datetime(start_date, end_date)
        quantity = random.randint(1, 5)
        unit_price = int(random.uniform(1000.0, 10000.0))  # BIGINTに変更
        session_id = f"session_{random.randint(100000, 999999)}"
        
        # 会員IDを35%の確率で設定
        user_id = random.choice(user_ids) if random.random() < 0.35 else None
        
        cart_items_data.append((
            cart_item_id,                                      # cart_item_id
            cart_id,                                           # cart_id
            random.choice(item_ids),                           # item_id
            quantity,                                          # quantity
            unit_price,                                        # unit_price (BIGINT)
            quantity * unit_price,                             # subtotal (BIGINT)
            session_id,                                        # session_id
            user_id,                                           # user_id
            added_at,                                          # added_at
            added_at + timedelta(minutes=random.randint(1, 60)), # updated_at
            random.choice(['ACTIVE', 'REMOVED'])               # status
        ))
        cart_item_id += 1

# カートアイテム履歴のスキーマを定義 (unit_price と subtotal を BIGINT に変更)
schema_cart_items = StructType([
    StructField("cart_item_id", LongType(), False),       # カートアイテムID
    StructField("cart_id", LongType(), False),            # カートID
    StructField("item_id", LongType(), False),            # 商品ID
    StructField("quantity", IntegerType(), False),        # 数量
    StructField("unit_price", LongType(), False),         # 単価 (BIGINT)
    StructField("subtotal", LongType(), False),           # 小計 (BIGINT)
    StructField("session_id", StringType(), True),        # セッションID
    StructField("user_id", StringType(), True),           # 会員ID (NULL許容)
    StructField("added_at", TimestampType(), False),      # 追加日時
    StructField("updated_at", TimestampType(), False),    # 更新日時
    StructField("status", StringType(), False)            # ステータス
])

# DataFrameの作成
cart_items_df = spark.createDataFrame(cart_items_data, schema_cart_items)

# Deltaテーブル作成
cart_items_df.write.mode("overwrite").format("delta").saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_cart_items')

# データ数とサンプル表示
display(cart_items_df.count())
display(cart_items_df.limit(10))

# COMMAND ----------

# DBTITLE 0,カートアイテム履歴チェック
res = spark.sql(f"""
SELECT
  cart_id,
  COUNT(*) as count
FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_cart_items
GROUP BY 1
ORDER BY 2 DESC
""")
display(res)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-7. bronze_carts / カート履歴

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import col, lit, sum as spark_sum, count as spark_count, min as spark_min, max as spark_max
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, IntegerType
import random
from datetime import datetime, timedelta

# カートアイテム履歴からカートIDと関連情報を取得
cart_items_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_cart_items')

# 会員マスタから会員IDを取得
user_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_users')
user_ids = [row.user_id for row in user_df.select('user_id').distinct().collect()]

# カートサマリー：カートごとの合計金額と商品点数を計算
cart_summary_df = cart_items_df.groupBy("cart_id").agg(
    spark_sum("subtotal").cast(LongType()).alias("total_amount"),           # 合計金額をBIGINTに変更
    spark_count("item_id").alias("item_count"),
    spark_min("user_id").alias("user_id"),                                  # 最初のユーザーIDを使用
    spark_min("session_id").alias("session_id"),                            # 最初のセッションIDを使用
    lit("ACTIVE").alias("status"),                                          # 仮のステータス（後で上書き）
    spark_min("added_at").alias("created_at"),                              # 最初の追加日時を作成日時として使用
    spark_max("updated_at").alias("updated_at")                             # 最後の更新日時を最終更新日時として使用
)

# カートステータスをランダムに割り当てる関数
def assign_status():
    rand_val = random.random()
    if rand_val < 0.15:
        return 'ACTIVE'            # 有効（まだ購入前）
    elif rand_val < 0.85:
        return 'ABANDONED'         # カート破棄
    else:
        return 'CONVERTED'         # 購入完了

# カートサマリーにランダムなステータス（"ACTIVE"、"ABANDONED"、"CONVERTED"）を割り当て
cart_summary_with_status = cart_summary_df.rdd.map(lambda row: row.asDict()).map(
    lambda row: {**row, 'status': assign_status()}
).toDF()

# 不足データを新しく作成するためのランダムデータ生成
num_new_carts = 1000 - cart_summary_with_status.count()
new_cart_data = []
for i in range(num_new_carts):
    created_at = datetime(2024, random.randint(1, 12), random.randint(1, 28), random.randint(0, 23), random.randint(0, 59))
    status = assign_status()                                                # ランダムなステータス割り当て
    new_cart_data.append((
        20001 + i,                                                          # 新しいカートID
        random.choice(user_ids),                                            # 会員IDを会員マスタから取得
        f"session_{random.randint(100000, 999999)}",                        # セッションID
        status,                                                             # ランダムなステータス
        created_at,                                                         # 作成日時
        created_at + timedelta(minutes=random.randint(1, 60)),              # 最終更新日時
        int(random.uniform(1000.0, 10000.0)),                               # 合計金額をBIGINTに変更
        random.randint(1, 10)                                               # 商品点数
    ))

# DataFrameに変換して結合するためにスキーマを定義
schema_carts = StructType([
    StructField("cart_id", LongType(), False),              # カートID
    StructField("user_id", StringType(), True),             # 会員ID
    StructField("session_id", StringType(), True),          # セッションID
    StructField("status", StringType(), False),             # カートステータス
    StructField("created_at", TimestampType(), False),      # 作成日時
    StructField("updated_at", TimestampType(), False),      # 最終更新日時
    StructField("total_amount", LongType(), False),         # 合計金額 (BIGINT)
    StructField("item_count", IntegerType(), False)         # 商品点数
])

# 新しいカートデータをDataFrameに変換
new_cart_df = spark.createDataFrame(new_cart_data, schema=schema_carts)

# カラム順を明示的に揃える
cart_summary_with_status = cart_summary_with_status.select("cart_id", "user_id", "session_id", "status", "created_at", "updated_at", "total_amount", "item_count")
new_cart_df = new_cart_df.select("cart_id", "user_id", "session_id", "status", "created_at", "updated_at", "total_amount", "item_count")

# カート履歴データフレーム作成と結合 (unionByName を使用)
carts_df = cart_summary_with_status.unionByName(new_cart_df)

# Deltaテーブル作成
carts_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_carts')

display(carts_df.count())
display(carts_df.limit(10))

# COMMAND ----------

# DBTITLE 1,カートステータスチェック
# ステータスごとのカウントを計算
status_counts = carts_df.groupBy("status").count()

# 全体のカウントを取得
total_count = carts_df.count()

# ステータスごとの割合を計算
status_ratios = status_counts.withColumn("ratio", col("count") / total_count * 100)

# 結果を表示
display(status_ratios)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-8. bronze_pos_orders_items / POS購買履歴（アイテム単位）

# COMMAND ----------

# DBTITLE 1,テーブル作成
import random
from datetime import datetime, timedelta
from decimal import Decimal
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, LongType, DecimalType, BooleanType, StringType
from pyspark.sql.functions import col

# 商品マスタ、会員マスタ、カートアイテム履歴のデータを取得
items_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_items")
user_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_users")
cart_items_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_cart_items")

# 商品ID、カテゴリID、カテゴリ名、商品名、価格を商品マスタから取得
product_info = items_df.select("item_id", "category_id", "category_name", "item_name", "price").collect()
product_data = {row.item_id: (row.category_id, row.category_name, row.item_name, row.price) for row in product_info}

# 食品と飲料カテゴリのIDリスト
high_priority_categories = [100, 200, 300, 800]  # 野菜・果物、肉類、魚介類、飲料
high_priority_items = [item_id for item_id in product_data if product_data[item_id][0] in high_priority_categories]
normal_priority_items = [item_id for item_id in product_data if product_data[item_id][0] not in high_priority_categories]

# 会員IDリストと性別情報を取得
user_info = user_df.select("user_id", "gender").collect()
user_data = {row.user_id: row.gender for row in user_info}

# 購買頻度の生成関数（指定の偏りに従う）
def generate_purchase_frequency(num_users):
    frequencies = []
    for _ in range(num_users):
        rand = random.random()
        if rand < 0.05:
            frequencies.append(1)
        elif rand < 0.25:
            frequencies.append(random.randint(2, 5))
        elif rand < 0.55:
            frequencies.append(random.randint(6, 10))
        elif rand < 0.80:
            frequencies.append(random.randint(11, 20))
        else:
            frequencies.append(random.randint(21, 30))
    return frequencies

# リピート期間生成関数
def generate_days_since_last_purchase(num_users):
    days_since_last = []
    for _ in range(num_users):
        rand = random.random()
        if rand < 0.55:
            days_since_last.append(random.randint(7, 14))
        elif rand < 0.80:
            days_since_last.append(random.randint(15, 30))
        elif rand < 0.95:
            days_since_last.append(random.randint(31, 90))
        else:
            days_since_last.append(random.randint(91, 180))
    return days_since_last

# POSデータ生成関数
def generate_pos_order_item_data(users_df):
    num_users = len(user_info)
    frequencies = generate_purchase_frequency(num_users)
    days_since_last = generate_days_since_last_purchase(num_users)
    
    pos_order_data = []
    
    for i, user_id in enumerate(user_data.keys()):
        last_purchase_date = datetime.now() - timedelta(days=days_since_last[i])
        gender = user_data[user_id]
        
        for _ in range(frequencies[i]):
            # 性別によって高優先カテゴリのアイテムを選択する確率を調整
            if gender == "F" and random.random() < 0.8:
                item_id = random.choice(high_priority_items)  # 女性は80%の確率で食品・飲料カテゴリ
            elif gender == "M" and random.random() < 0.6:
                item_id = random.choice(high_priority_items)  # 男性は60%の確率で食品・飲料カテゴリ
            else:
                item_id = random.choice(normal_priority_items)  # その他のカテゴリ
            
            category_id, category_name, item_name, unit_price = product_data[item_id]

            quantity = random.randint(1, 5)
            subtotal = unit_price * quantity
            pos_order_data.append({
                "order_item_id": random.randint(50000001, 59999999),
                "user_id": user_id,
                "order_id": random.randint(5000001, 5999999),
                "item_id": item_id,
                "category_id": category_id,
                "item_name": item_name,
                "category_name": category_name,
                "quantity": quantity,
                "unit_price": unit_price,
                "subtotal": subtotal,
                "cancel_flg": random.random() < 0.05,
                "order_datetime": last_purchase_date,
                "created_at": last_purchase_date,
                "updated_at": datetime.now()
            })

    return pos_order_data

# サンプルデータ生成
pos_order_item_data = generate_pos_order_item_data(user_df)

# スキーマ定義（unit_priceとsubtotalをBIGINTに変更）
schema_pos_orders_items = StructType([
    StructField("order_item_id", LongType(), False),            # 明細ID
    StructField("user_id", StringType(), False),                # 会員ID
    StructField("order_id", LongType(), False),                 # 注文ID
    StructField("item_id", LongType(), False),                  # 商品ID
    StructField("category_id", LongType(), False),              # カテゴリID
    StructField("item_name", StringType(), False),              # 商品名
    StructField("category_name", StringType(), False),          # カテゴリ名
    StructField("quantity", IntegerType(), False),              # 数量
    StructField("unit_price", LongType(), False),               # 単価
    StructField("subtotal", LongType(), False),                 # 小計
    StructField("cancel_flg", BooleanType(), False),            # キャンセルフラグ
    StructField("order_datetime", TimestampType(), True),       # 取引日時
    StructField("created_at", TimestampType(), False),          # 作成日時
    StructField("updated_at", TimestampType(), True)            # 更新日時
])

# DataFrameの作成と保存
pos_order_item_df = spark.createDataFrame(pos_order_item_data, schema=schema_pos_orders_items)
pos_order_item_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders_items')

# データ数とサンプル表示
display(pos_order_item_df.count())
display(pos_order_item_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Rチェック
# POS購買履歴テーブルの名前を指定
pos_order_item_table = f"{MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders_items"

# SQLクエリを実行
spark.sql(f"""
    -- 最終購買日を取得するサブクエリ
    WITH last_purchase AS (
        SELECT 
            user_id,
            MAX(order_datetime) AS last_purchase_date  -- 各ユーザーの最終購買日を取得
        FROM {pos_order_item_table}
        GROUP BY user_id
    ),
    -- 最終購買日からの経過日数を計算するサブクエリ
    days_diff AS (
        SELECT 
            user_id,
            last_purchase_date,
            DATEDIFF(CURRENT_DATE(), last_purchase_date) AS days_since_last_purchase  -- 現在の日付との差分を計算
        FROM last_purchase
    )
    -- 経過日数ごとに会員数を集計
    SELECT 
        days_since_last_purchase,
        COUNT(*) AS member_count  -- 経過日数ごとの会員数を集計
    FROM days_diff
    GROUP BY days_since_last_purchase
    ORDER BY days_since_last_purchase
""").display()

# COMMAND ----------

# DBTITLE 1,Mチェック
# POS購買履歴テーブルの名前を指定
pos_order_item_table = f"{MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders_items"

# SQLクエリを実行
spark.sql(f"""
    -- 各会計単位ごとの合計金額を計算
    WITH order_totals AS (
        SELECT 
            order_id,
            SUM(subtotal) AS order_total  -- 各注文IDごとの合計金額
        FROM {pos_order_item_table}
        GROUP BY order_id
    ),
    -- 合計金額に基づいて価格帯を設定
    price_ranges AS (
        SELECT 
            order_id,
            order_total,
            CASE 
                WHEN order_total >= 0 AND order_total < 1000 THEN '1_0円〜1000円未満'
                WHEN order_total >= 1000 AND order_total < 5000 THEN '2_1001円〜5000円未満'
                WHEN order_total >= 5000 AND order_total < 10000 THEN '3_5001円〜10000円未満'
                WHEN order_total >= 10000 AND order_total < 30000 THEN '4_10000円〜30000円未満'
                ELSE '5_30000円以上'
            END AS price_range  -- 各合計金額の範囲にラベルを付与
        FROM order_totals
    ),
    -- 価格帯ごとの件数を集計
    range_counts AS (
        SELECT 
            price_range,
            COUNT(DISTINCT order_id) AS count  -- 各価格帯の注文件数
        FROM price_ranges
        GROUP BY price_range
    ),
    -- 全体の注文数を取得
    total_count AS (
        SELECT 
            COUNT(DISTINCT order_id) AS total  -- 全体の注文件数
        FROM price_ranges
    )
    -- 各価格帯の件数と全体に対する割合を計算して出力
    SELECT 
        rc.price_range,
        rc.count,
        (rc.count / tc.total) * 100 AS percentage  -- 各価格帯の割合を計算
    FROM range_counts rc
    CROSS JOIN total_count tc
    ORDER BY rc.price_range
""").display()

# COMMAND ----------

# DBTITLE 1,Fチェック
# pos_order_item_dfを一時テーブルとして登録
pos_order_item_df.createOrReplaceTempView("pos_order_items")

# SQLクエリで各ユーザーの購買回数を集計し、購買頻度グループを作成
spark.sql("""
    WITH user_purchase_counts AS (
        SELECT user_id, COUNT(DISTINCT order_id) AS purchase_count
        FROM pos_order_items
        GROUP BY user_id
    ),
    frequency_groups AS (
        SELECT 
            user_id,
            purchase_count,
            CASE 
                WHEN purchase_count BETWEEN 1 AND 2 THEN '01_1回〜2回'
                WHEN purchase_count BETWEEN 3 AND 10 THEN '02_3回〜10回'
                WHEN purchase_count BETWEEN 11 AND 20 THEN '03_11回〜20回'
                WHEN purchase_count BETWEEN 21 AND 50 THEN '04_21回〜50回'
                ELSE '05_51回以上'
            END AS frequency_group
        FROM user_purchase_counts
    ),
    group_counts AS (
        SELECT 
            frequency_group,
            COUNT(user_id) AS member_count
        FROM frequency_groups
        GROUP BY frequency_group
    ),
    ratios AS (
        SELECT 
            frequency_group,
            member_count,
            ROUND(member_count * 100.0 / SUM(member_count) OVER (), 2) AS composition_ratio
        FROM group_counts
    )
    SELECT 
        frequency_group,
        member_count,
        composition_ratio,
        ROUND(SUM(composition_ratio) OVER (ORDER BY frequency_group ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS cumulative_ratio
    FROM ratios
    ORDER BY frequency_group
""").display()

# COMMAND ----------

# DBTITLE 1,商品チェック
# # 商品名ごとの点数
# spark.sql(f"""
#     WITH 
#     item_quantity AS (
#         SELECT 
#             item_name,
#             SUM(quantity) AS total_quantity  -- 各商品名ごとの合計点数
#         FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders_items
#         GROUP BY item_name
#     ),
#     category_quantity AS (
#         SELECT 
#             category_name,
#             SUM(quantity) AS total_quantity  -- 各カテゴリ名ごとの合計点数
#         FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders_items
#         GROUP BY category_name
#     )
#     SELECT * FROM item_quantity ORDER BY total_quantity DESC
# """).display()

# カテゴリ名ごとの点数
spark.sql(f"""
    WITH category_quantity AS (
        SELECT 
            category_name,
            SUM(quantity) AS total_quantity  -- 各カテゴリ名ごとの合計点数
        FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders_items
        GROUP BY category_name
    )
    SELECT * FROM category_quantity
    ORDER BY total_quantity DESC
""").display()

# COMMAND ----------

# DBTITLE 1,性別チェック
# 性別ごとの購買比率を集計して表示
spark.sql(f"""
    WITH gender_quantity AS (
        SELECT 
            u.gender,  -- 性別
            SUM(poi.quantity) AS total_quantity  -- 各性別ごとの購入点数
        FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders_items AS poi
        INNER JOIN {MY_CATALOG}.{MY_SCHEMA}.bronze_users AS u
        ON poi.user_id = u.user_id
        GROUP BY u.gender
    ),
    gender_percentage AS (
        SELECT 
            gender,
            total_quantity,
            ROUND((total_quantity / SUM(total_quantity) OVER ()) * 100, 2) AS percentage  -- 各性別の割合（%）
        FROM gender_quantity
    )
    SELECT * FROM gender_percentage
    ORDER BY total_quantity DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-9. bronze_pos_orders / POS購買履歴（会計単位）

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql import functions as F
import random
from datetime import datetime

# マスターデータの取得
store_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_stores").filter("status = 'ACTIVE'")
staff_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_staff").filter("status = 'ACTIVE'")
pos_order_item_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders_items")

# 店舗IDの頻度を調整（東京都、神奈川県、大阪府の店舗を頻出させる設定）
store_ids_tokyo_kanagawa_osaka = store_df.filter(store_df.prefecture.isin(["東京都", "神奈川県", "大阪府"])) \
                                         .select("store_id") \
                                         .rdd.flatMap(lambda x: x).collect()
store_ids_other = store_df.filter(~store_df.prefecture.isin(["東京都", "神奈川県", "大阪府"])) \
                          .select("store_id") \
                          .rdd.flatMap(lambda x: x).collect()
weighted_store_ids = store_ids_tokyo_kanagawa_osaka * 5 + store_ids_other

# 店員データを店舗ごとに集約し、後でランダムに割り当て
staff_df = staff_df.groupBy("store_id").agg(F.collect_list("staff_id").alias("staff_ids"))

# POS購買履歴（アイテム単位）から会計単位データを集計
order_df = pos_order_item_df.groupBy("order_id", "user_id").agg(
    F.sum("subtotal").alias("total_amount"),          # アイテム単位の小計を合計金額として集計
    F.min("order_datetime").alias("order_datetime"),  # 最小の注文日時を取引日時として使用
    F.min("created_at").alias("created_at"),          # 作成日時
    F.max("updated_at").alias("updated_at")           # 更新日時
)

# 店舗情報を結合し、ランダムな支払い方法と店員IDを割り当て
order_df = order_df.withColumn(
    "store_id", F.expr(f"array({', '.join(map(str, weighted_store_ids))})[int(rand() * {len(weighted_store_ids)})]")
).join(staff_df, "store_id", "left").withColumn(
    "staff_id", F.expr("element_at(staff_ids, int(rand() * size(staff_ids)) + 1)")  # 店舗ごとにランダムな店員IDを割り当て
).drop("staff_ids").withColumn(
    "payment_method", F.expr("CASE WHEN rand() < 0.4 THEN 'CASH' WHEN rand() < 0.7 THEN 'CREDIT' WHEN rand() < 0.9 THEN 'DEBIT' ELSE 'QR' END")
)

# ポイント、クーポンの使用などの追加フィールド（金額をBIGINTに変更）
order_df = order_df.withColumn(
    "points_discount_amount", F.round(F.rand() * 1000).cast("LONG")  # ポイント割引額（BIGINT）
).withColumn(
    "coupon_discount_amount", F.round(F.rand() * 100).cast("LONG")  # クーポン割引額（BIGINT）
).withColumn(
    "discount_amount", (F.col("points_discount_amount") + F.col("coupon_discount_amount")).cast("LONG")  # 割引額（BIGINT）
).withColumn(
    "payment_amount", (F.col("total_amount") - F.col("discount_amount")).cast("LONG")  # 支払金額（BIGINT）
).withColumn(
    "points_earned", F.round(F.rand() * 100).cast("LONG")  # 獲得ポイント（BIGINT）
).withColumn(
    "coupon_used", F.expr("CASE WHEN rand() < 0.2 THEN TRUE ELSE FALSE END")
).withColumn(
    "receipt_number", F.concat(F.lit("POS-"), F.date_format(F.col("order_datetime"), "yyyyMMdd"), F.lit("-"), F.format_string("%06d", F.col("order_id")))
).withColumn(
    "cancel_flg", F.expr("CASE WHEN rand() < 0.05 THEN TRUE ELSE FALSE END")
)

# 必要なカラムのみを選択
bronze_pos_orders_df = order_df.select(
    "order_id",                  # 取引ID
    "user_id",                   # 会員ID
    "store_id",                  # 店舗ID
    "order_datetime",            # 取引日時
    "total_amount",              # 合計金額（割引前）
    "points_discount_amount",    # ポイント割引額
    "coupon_discount_amount",    # クーポン割引額
    "discount_amount",           # 割引額（合計）
    "payment_amount",            # 支払金額（割引後）
    "payment_method",            # 支払い方法
    "staff_id",                  # 店員ID
    "points_earned",             # 獲得ポイント
    "coupon_used",               # クーポン使用フラグ
    "receipt_number",            # レシート番号
    "cancel_flg",                # キャンセルフラグ
    "created_at",                # 作成日時
    "updated_at"                 # 更新日時
)

# Deltaテーブルに保存
bronze_pos_orders_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders')

# データ表示
display(bronze_pos_orders_df.count())
display(bronze_pos_orders_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Rチェック
# SQLクエリでの集計処理
spark.sql(f"""
    WITH user_last_order AS (
        -- 各会員ごとの最終購買日を取得
        SELECT 
            user_id,
            MAX(order_datetime) AS last_order_datetime
        FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders
        GROUP BY user_id
    ),
    user_days_since_last_order AS (
        -- 最終購買日からの経過日数を計算
        SELECT 
            user_id,
            DATEDIFF(CURRENT_DATE(), last_order_datetime) AS days_since_last_order
        FROM user_last_order
    )
    
    -- 経過日数ごとに会員数を集計
    SELECT 
        days_since_last_order,
        COUNT(*) AS member_count
    FROM user_days_since_last_order
    GROUP BY days_since_last_order
    ORDER BY days_since_last_order
""").display()

# COMMAND ----------

# DBTITLE 1,Mチェック
# SQLクエリでの集計処理
spark.sql(f"""
    WITH order_totals AS (
        -- 会計単位（order_id）ごとの合計金額を取得
        SELECT 
            order_id, 
            total_amount 
        FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders
    ),
    price_ranges AS (
        -- 合計金額（total_amount）の範囲ごとにグルーピングして範囲名を設定
        SELECT 
            order_id,
            total_amount,
            CASE 
                WHEN total_amount >= 0 AND total_amount < 1000 THEN '1_0円〜1000円未満'
                WHEN total_amount >= 1001 AND total_amount < 5000 THEN '2_1001円〜5000円未満'
                WHEN total_amount >= 5001 AND total_amount < 10000 THEN '3_5001円〜10000円未満'
                WHEN total_amount >= 10000 AND total_amount < 30000 THEN '4_10000円〜30000円未満'
                ELSE '5_30000円以上'
            END AS price_range
        FROM order_totals
    ),
    price_range_counts AS (
        -- 各価格帯ごとの件数を計算
        SELECT 
            price_range,
            COUNT(*) AS count
        FROM price_ranges
        GROUP BY price_range
    ),
    total_count AS (
        -- 全体の件数を計算
        SELECT 
            SUM(count) AS total_count 
        FROM price_range_counts
    )
    
    -- 各価格帯ごとの件数と割合を計算して出力
    SELECT 
        prc.price_range,
        prc.count,
        (prc.count / tc.total_count) * 100 AS percentage
    FROM price_range_counts prc
    CROSS JOIN total_count tc
    ORDER BY price_range
""").display()

# COMMAND ----------

# DBTITLE 1,Fチェック
# SQLクエリでの集計処理
spark.sql(f"""
    WITH order_totals AS (
        -- 会計単位（order_id）ごとの合計金額を取得
        SELECT 
            order_id, 
            total_amount 
        FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders
    ),
    price_ranges AS (
        -- 合計金額（total_amount）の範囲ごとにグルーピングして範囲名を設定
        SELECT 
            order_id,
            total_amount,
            CASE 
                WHEN total_amount >= 0 AND total_amount < 1000 THEN '1_0円〜1000円未満'
                WHEN total_amount >= 1001 AND total_amount < 5000 THEN '2_1001円〜5000円未満'
                WHEN total_amount >= 5001 AND total_amount < 10000 THEN '3_5001円〜10000円未満'
                WHEN total_amount >= 10000 AND total_amount < 30000 THEN '4_10000円〜30000円未満'
                ELSE '5_30000円以上'
            END AS price_range
        FROM order_totals
    ),
    price_range_counts AS (
        -- 各価格帯ごとの件数を計算
        SELECT 
            price_range,
            COUNT(*) AS count
        FROM price_ranges
        GROUP BY price_range
    ),
    total_count AS (
        -- 全体の件数を計算
        SELECT 
            SUM(count) AS total_count 
        FROM price_range_counts
    )
    
    -- 各価格帯ごとの件数と割合を計算して出力
    SELECT 
        prc.price_range,
        prc.count,
        (prc.count / tc.total_count) * 100 AS percentage
    FROM price_range_counts prc
    CROSS JOIN total_count tc
    ORDER BY price_range
""").display()

# COMMAND ----------

# DBTITLE 1,性別チェック
spark.sql(f"""
    WITH gender_counts AS (
        -- 会員マスタから性別情報を結合して男女比率を計算
        SELECT 
            u.gender,
            COUNT(o.user_id) AS member_count
        FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders o
        JOIN {MY_CATALOG}.{MY_SCHEMA}.bronze_users u
            ON o.user_id = u.user_id
        GROUP BY u.gender
    ),
    total_count AS (
        -- 総会員数を計算
        SELECT 
            SUM(member_count) AS total_members
        FROM gender_counts
    )
    
    -- 各性別の割合を計算して結果を表示
    SELECT 
        gender,
        member_count,
        (member_count / total_members) * 100 AS percentage
    FROM gender_counts, total_count
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-10. bronze_ec_order_items / EC購買履歴（アイテム単位）

# COMMAND ----------

# DBTITLE 1,テーブル作成
import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, LongType, BooleanType, StringType

# 商品マスタと会員マスタ、カート履歴とカートアイテム履歴データを取得
items_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_items")
user_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_users")
cart_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_carts")
cart_items_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_cart_items")

# 商品情報とカテゴリデータの準備
product_info = items_df.select("item_id", "category_id", "category_name", "item_name").collect()
product_data = {row.item_id: (row.category_id, row.category_name, row.item_name) for row in product_info}

# 使用するカートIDのリスト取得
cart_ids = cart_items_df.select("cart_id").distinct().rdd.flatMap(lambda x: x).collect()

# EC購買履歴データ生成
def generate_ec_order_item_data(user_df, cart_items_df):
    def generate_purchase_frequency(num_users):
        frequencies = []
        for _ in range(num_users):
            rand = random.random()
            if rand < 0.10:
                frequencies.append(1)
            elif rand < 0.35:
                frequencies.append(random.randint(2, 3))
            elif rand < 0.70:
                frequencies.append(random.randint(4, 10))
            elif rand < 0.90:
                frequencies.append(random.randint(11, 20))
            else:
                frequencies.append(random.randint(21, 50))
        return frequencies

    def generate_days_since_last_purchase(num_users):
        days_since_last = []
        for _ in range(num_users):
            rand = random.random()
            if rand < 0.30:
                days_since_last.append(random.randint(0, 7))
            elif rand < 0.50:
                days_since_last.append(random.randint(8, 30))
            elif rand < 0.80:
                days_since_last.append(random.randint(31, 90))
            else:
                days_since_last.append(random.randint(91, 180))
        return days_since_last
    
    # データ生成の準備
    num_users = len(user_df.collect())
    frequencies = generate_purchase_frequency(num_users)
    days_since_last = generate_days_since_last_purchase(num_users)
    
    ec_order_data = []
    
    for i, user_id in enumerate(user_df.select("user_id").collect()):
        user_id = user_id.user_id
        cart_id = random.choice(cart_ids)
        last_purchase_date = datetime.now() - timedelta(days=days_since_last[i])

        for _ in range(frequencies[i]):
            item_id = random.choice(list(product_data.keys()))
            category_id, category_name, item_name = product_data[item_id]
            quantity = max(1, random.randint(1, 5) if category_id in [100, 200, 300] else random.randint(1, 2))
            
            unit_price = int(random.uniform(1000, 10000))  # ここで整数に変換
            subtotal = unit_price * quantity
            total_amount = subtotal  # 合計金額は小計のまま

            # 商品カテゴリによる頻度調整
            if category_id in [600, 1000, 1100]:  # スナックや日用品
                if random.random() < 0.2:
                    quantity += random.randint(1, 3)
            elif category_id in [400, 500, 800]:  # 飲料・乳製品・パン
                if random.random() < 0.3:
                    quantity += random.randint(1, 2)

            ec_order_data.append({
                "order_item_id": random.randint(40000001, 49999999),                  # 明細ID
                "user_id": user_id,                                                   # 会員ID
                "order_id": random.randint(4000001, 4999999),                         # 注文ID
                "cart_id": cart_id,                                                   # カートID
                "cart_item_id": random.randint(50000001, 59999999),                   # カートアイテムID
                "item_id": item_id,                                                   # 商品ID
                "category_id": category_id,                                           # カテゴリID
                "item_name": item_name,                                               # 商品名
                "category_name": category_name,                                       # カテゴリ名
                "quantity": quantity,                                                 # 数量
                "unit_price": unit_price,                                             # 単価（BIGINT）
                "subtotal": subtotal,                                                 # 小計（BIGINT）
                "total_amount": total_amount,                                         # 合計金額（BIGINT）
                "cancel_flg": random.random() < 0.05,                                 # キャンセルフラグ
                "cancelled_at": datetime.now() if random.random() < 0.05 else None,   # キャンセル日時
                "order_datetime": last_purchase_date,                                 # 受注日時
                "created_at": last_purchase_date,                                     # 作成日時
                "updated_at": datetime.now()                                          # 更新日時
            })

    return ec_order_data

# データ生成とDataFrameの作成
ec_order_item_data = generate_ec_order_item_data(user_df, cart_items_df)
schema_ec_order_items = StructType([
    StructField("order_item_id", LongType(), False),          # 明細ID
    StructField("user_id", StringType(), False),              # 会員ID
    StructField("order_id", LongType(), False),               # 注文ID
    StructField("cart_id", LongType(), False),                # カートID
    StructField("cart_item_id", LongType(), False),           # カートアイテムID
    StructField("item_id", LongType(), False),                # 商品ID
    StructField("category_id", LongType(), False),            # カテゴリID
    StructField("item_name", StringType(), False),            # 商品名
    StructField("category_name", StringType(), False),        # カテゴリ名
    StructField("quantity", IntegerType(), False),            # 数量
    StructField("unit_price", LongType(), False),             # 単価（BIGINT）
    StructField("subtotal", LongType(), False),               # 小計（BIGINT）
    StructField("total_amount", LongType(), False),           # 合計金額（BIGINT）
    StructField("cancel_flg", BooleanType(), False),          # キャンセルフラグ
    StructField("cancelled_at", TimestampType(), True),       # キャンセル日時
    StructField("order_datetime", TimestampType(), False),    # 受注日時
    StructField("created_at", TimestampType(), False),        # 作成日時
    StructField("updated_at", TimestampType(), False)         # 更新日時
])

# EC購買履歴（アイテム単位）のテーブル作成
ec_order_items_df = spark.createDataFrame(ec_order_item_data, schema=schema_ec_order_items)
ec_order_items_df.write.mode("overwrite").format("delta").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_ec_order_items")

# データ数とサンプル表示
display(ec_order_items_df.count())
display(ec_order_items_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Rチェック
spark.sql(f"""
    WITH last_purchase_dates AS (
        SELECT
            user_id,
            MAX(created_at) AS last_purchase_date  -- 会員ごとの最終購買日を取得
        FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_ec_order_items
        GROUP BY user_id
    ),
    days_since_last_purchase AS (
        SELECT
            user_id,
            DATEDIFF(CURRENT_DATE, last_purchase_date) AS days_since_last_purchase  -- 現在の日付との差分を計算
        FROM last_purchase_dates
    ),
    member_counts AS (
        SELECT
            days_since_last_purchase,
            COUNT(*) AS member_count  -- 経過日数ごとの会員数を集計
        FROM days_since_last_purchase
        GROUP BY days_since_last_purchase
    )
    SELECT
        days_since_last_purchase,
        member_count
    FROM member_counts
    ORDER BY days_since_last_purchase
""").display()

# COMMAND ----------

# DBTITLE 1,Mチェック
spark.sql(f"""
    WITH order_totals AS (
        SELECT
            order_id,
            SUM(subtotal) AS order_total  -- 各会計（order_id）の合計金額を計算
        FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_ec_order_items
        GROUP BY order_id
    ),
    order_total_groups AS (
        SELECT
            order_total,
            CASE
                WHEN order_total >= 0 AND order_total < 1000 THEN '1_0円〜1000円未満'
                WHEN order_total >= 1001 AND order_total < 5000 THEN '2_1001円〜5000円未満'
                WHEN order_total >= 5001 AND order_total < 10000 THEN '3_5001円〜10000円未満'
                WHEN order_total >= 10000 AND order_total < 30000 THEN '4_10000円〜30000円未満'
                ELSE '5_30000円以上'
            END AS price_range
        FROM order_totals
    ),
    price_range_counts AS (
        SELECT
            price_range,
            COUNT(*) AS count
        FROM order_total_groups
        GROUP BY price_range
    )
    SELECT
        price_range,
        count,
        (count / SUM(count) OVER ()) * 100 AS percentage  -- 割合を計算
    FROM price_range_counts
    ORDER BY price_range
""").display()

# COMMAND ----------

# DBTITLE 1,Fチェック
spark.sql(f"""
    WITH user_purchase_counts AS (
        SELECT
            user_id,
            COUNT(DISTINCT order_id) AS purchase_count  -- ユーザーごとのユニークな注文IDをカウント
        FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_ec_order_items
        GROUP BY user_id
    ),
    frequency_groups AS (
        SELECT
            user_id,
            purchase_count,
            CASE 
                WHEN purchase_count BETWEEN 1 AND 2 THEN '1回〜2回'
                WHEN purchase_count BETWEEN 3 AND 10 THEN '3回〜10回'
                WHEN purchase_count BETWEEN 11 AND 20 THEN '11回〜20回'
                WHEN purchase_count BETWEEN 21 AND 50 THEN '21回〜50回'
                ELSE '51回以上'
            END AS frequency_group  -- 購買回数に基づきグループ分け
        FROM user_purchase_counts
    ),
    member_counts AS (
        SELECT
            frequency_group,
            COUNT(*) AS member_count  -- 各グループの会員数を集計
        FROM frequency_groups
        GROUP BY frequency_group
    )
    SELECT
        frequency_group,
        member_count
    FROM member_counts
    ORDER BY frequency_group
""").display()

# COMMAND ----------

# DBTITLE 1,商品チェック
# # 商品名ごとの点数を集計
# spark.sql(f"""
#     SELECT 
#         item_name, 
#         SUM(quantity) AS total_quantity  -- 各商品の合計数量
#     FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_ec_order_items
#     GROUP BY item_name
#     ORDER BY total_quantity DESC
# """).display()

# 商品カテゴリごとの点数を集計
spark.sql(f"""
    SELECT 
        category_name, 
        SUM(quantity) AS total_quantity  -- 各カテゴリの合計数量
    FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_ec_order_items
    GROUP BY category_name
    ORDER BY total_quantity DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-11. bronze_ec_orders / EC購買履歴（会計単位）

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructType, StructField, StringType, BooleanType, TimestampType

# 支払い方法を設定（偏りあり）
def generate_payment_method():
    return F.expr("CASE WHEN rand() < 0.6 THEN 'CREDIT' WHEN rand() < 0.8 THEN 'DEBIT' WHEN rand() < 0.95 THEN 'BANK_TRANSFER' ELSE 'COD' END")

# 支払い状況を設定（比率調整）
def generate_payment_status():
    return F.expr("CASE WHEN rand() < 0.8 THEN 'PAID' WHEN rand() < 0.15 THEN 'PENDING' ELSE 'FAILED' END")

# 配送状況を設定（偏りあり）
def generate_shipping_status():
    return F.expr("CASE WHEN rand() < 0.7 THEN 'DELIVERED' WHEN rand() < 0.9 THEN 'SHIPPED' ELSE 'PREPARING' END")

# サンプルの配送先住所を作成
def generate_shipping_address():
    return F.expr("CASE WHEN rand() < 0.5 THEN '東京都渋谷区神南1-2-3' ELSE '大阪市北区梅田1-1-1' END")

# EC購買履歴（会計単位）を生成する関数
def generate_random_ec_orders(user_df, ec_order_items_df):
    # EC購買履歴（アイテム単位）から注文ごとのカートIDと集計を行う
    order_summary = ec_order_items_df.groupBy("order_id", "user_id", "cart_id").agg(
        F.sum("subtotal").alias("total_amount"),  # 割引前の合計金額
        F.min("created_at").alias("order_datetime")  # 注文日時
    )
    
    # 割引額、支払金額、送料などを追加
    order_summary = order_summary.withColumn(
        "points_discount_amount", F.expr("CASE WHEN rand() < 0.3 THEN round(rand() * 500) ELSE 0 END")  # 30%の確率でポイント割引
    ).withColumn(
        "coupon_discount_amount", F.expr("CASE WHEN rand() < 0.2 THEN round(rand() * 100) ELSE 0 END")  # 20%の確率でクーポン割引
    ).withColumn(
        "discount_amount", F.col("points_discount_amount") + F.col("coupon_discount_amount")  # 割引額の合計
    ).withColumn(
        "payment_amount", F.col("total_amount") - F.col("discount_amount")  # 支払金額（割引後）
    ).withColumn(
        "shipping_fee", F.lit(480)  # 仮の送料を設定
    ).withColumn(
        "payment_method", generate_payment_method()
    ).withColumn(
        "payment_status", generate_payment_status()
    ).withColumn(
        "shipping_method", F.expr("CASE WHEN rand() < 0.8 THEN '宅配便' ELSE 'メール便' END")  # 配送方法
    ).withColumn(
        "shipping_status", generate_shipping_status()
    ).withColumn(
        "shipping_address", generate_shipping_address()
    ).withColumn(
        "shipping_name", F.expr("CASE WHEN rand() < 0.5 THEN '山田 花子' ELSE '田中 太郎' END")  # 配送先氏名
    ).withColumn(
        "shipping_phone", F.expr("CASE WHEN rand() < 0.5 THEN '090-1234-5678' ELSE '080-9876-5432' END")  # 配送先電話番号
    ).withColumn(
        "points_earned", F.round(F.col("total_amount") * 0.01)  # 獲得ポイントは合計金額の1%
    ).withColumn(
        "coupon_used", F.expr("CASE WHEN rand() < 0.2 THEN TRUE ELSE FALSE END")  # 20%の確率でクーポン使用
    ).withColumn(
        "cancel_flg", F.expr("CASE WHEN rand() < 0.05 THEN TRUE ELSE FALSE END")  # 5%の確率でキャンセル
    ).withColumn(
        "cancelled_at", F.when(F.col("cancel_flg") == True, F.current_timestamp()).otherwise(None)  # キャンセル日時
    ).withColumn(
        "created_at", F.col("order_datetime")  # 作成日時を注文日時と同じに設定
    ).withColumn(
        "updated_at", F.current_timestamp()  # 現在の日時を更新日時に設定
    )

    # カラムの順序を定義に従って並べ替え
    order_summary = order_summary.select(
        "order_id", "cart_id", "user_id", "total_amount", "points_discount_amount", "coupon_discount_amount", 
        "discount_amount", "payment_amount", "shipping_fee", "payment_method", "payment_status", 
        "shipping_method", "shipping_status", "shipping_address", "shipping_name", "shipping_phone", 
        "points_earned", "coupon_used", "cancel_flg", "cancelled_at", "order_datetime", "created_at", 
        "updated_at"
    )
    
    return order_summary

# スキーマ定義
schema_ec_orders = StructType([
    StructField("order_id", LongType(), False),                         # 注文ID
    StructField("cart_id", LongType(), False),                          # カートID
    StructField("user_id", StringType(), False),                        # 会員ID
    StructField("total_amount", LongType(), False),                     # 合計金額（割引前）
    StructField("points_discount_amount", LongType(), True),            # ポイント割引額
    StructField("coupon_discount_amount", LongType(), True),            # クーポン割引額
    StructField("discount_amount", LongType(), True),                   # 割引額の合計
    StructField("payment_amount", LongType(), False),                   # 支払金額（割引後）
    StructField("shipping_fee", LongType(), True),                      # 送料
    StructField("payment_method", StringType(), False),                 # 支払い方法
    StructField("payment_status", StringType(), False),                 # 支払い状況
    StructField("shipping_method", StringType(), True),                 # 配送方法
    StructField("shipping_status", StringType(), True),                 # 配送状況
    StructField("shipping_address", StringType(), True),                # 配送先住所
    StructField("shipping_name", StringType(), True),                   # 配送先氏名
    StructField("shipping_phone", StringType(), True),                  # 配送先電話番号
    StructField("points_earned", LongType(), True),                     # 獲得ポイント
    StructField("coupon_used", BooleanType(), False),                   # クーポン使用フラグ
    StructField("cancel_flg", BooleanType(), False),                    # キャンセルフラグ
    StructField("cancelled_at", TimestampType(), True),                 # キャンセル日時
    StructField("order_datetime", TimestampType(), False),              # 受注日時
    StructField("created_at", TimestampType(), False),                  # 作成日時
    StructField("updated_at", TimestampType(), False)                   # 更新日時
])

# 会員マスタをテーブルから取得
user_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_users')

# EC購買履歴（アイテム単位）をテーブルから読み込み
ec_order_items_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_ec_order_items')

# サンプルデータ生成
ec_orders_df = generate_random_ec_orders(user_df, ec_order_items_df)

# テーブル作成
ec_orders_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_ec_orders')

# データ表示
display(ec_orders_df.count())
display(ec_orders_df.limit(10))

# COMMAND ----------

# DBTITLE 1,支払い方法チェック
# 支払い方法ごとの件数と割合を計算
# SQLクエリを使用して、支払い方法ごとの件数と割合を計算し、降順にソート

spark.sql(f"""
    SELECT 
        payment_method,  -- 支払い方法
        COUNT(*) AS count,  -- 支払い方法ごとの件数をカウント
        (COUNT(*) / (SELECT COUNT(*) FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_ec_orders)) * 100 AS percentage  -- 割合を計算
    FROM 
        {MY_CATALOG}.{MY_SCHEMA}.bronze_ec_orders
    GROUP BY 
        payment_method
    ORDER BY 
        count DESC  -- 件数で降順にソート
""").display()

# COMMAND ----------

# DBTITLE 1,配送状況チェック
# 配送状況ごとの件数と割合を計算
# SQLクエリを使用して、配送状況ごとの件数と割合を計算し、降順にソート

spark.sql(f"""
    SELECT 
        shipping_status,  -- 配送状況
        COUNT(*) AS count,  -- 配送状況ごとの件数をカウント
        (COUNT(*) / (SELECT COUNT(*) FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_ec_orders)) * 100 AS percentage  -- 割合を計算
    FROM 
        {MY_CATALOG}.{MY_SCHEMA}.bronze_ec_orders
    GROUP BY 
        shipping_status
    ORDER BY 
        count DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-12. bronze_inventory_history / 在庫履歴

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import monotonically_increasing_id, col, when, rand, sum as sum_func, round, lit
from pyspark.sql.types import IntegerType, StringType, LongType
from pyspark.sql.window import Window

# EC購買（アイテム単位）データの読み込み
ec_order_items_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_ec_order_items')

# POS購買（アイテム単位）データの読み込み
pos_order_items_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders_items')

# 店舗情報をEC/POSデータに統合するための処理
orders_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders')

# ECとPOSのデータに店舗情報を結合
ec_order_items_with_store = ec_order_items_df.join(orders_df, 'order_id', 'left').select(
    ec_order_items_df.order_item_id,
    ec_order_items_df.item_id,
    ec_order_items_df.quantity,
    orders_df.store_id,
    orders_df.staff_id,
    ec_order_items_df.order_datetime
)

pos_order_items_with_store = pos_order_items_df.join(orders_df, 'order_id', 'left').select(
    pos_order_items_df.order_item_id,
    pos_order_items_df.item_id,
    pos_order_items_df.quantity,
    orders_df.store_id,
    orders_df.staff_id,
    pos_order_items_df.order_datetime
)

# 在庫履歴データを生成する関数
def generate_inventory_history_data_from_purchase_data(ec_data, pos_data):
    # ECデータとPOSデータを統合
    combined_data = ec_data.union(pos_data)

    # 店舗ごと商品ごとの販売数合計を計算
    window = Window.partitionBy("store_id", "item_id")
    total_sales = combined_data.groupBy("store_id", "item_id").agg(sum_func("quantity").alias("total_quantity"))

    # 在庫状態と数量を同時に計算
    inventory_data = total_sales.withColumn(
        "random_value", rand()
    ).withColumn(
        "inventory_quantity",
        when(col("random_value") < 0.2, 0)  # 20%の確率で在庫切れ
        .when(col("random_value") < 0.6, round(col("total_quantity") * col("random_value") * 0.5))  # 40%の確率で在庫少なめ
        .otherwise(round(col("total_quantity") * (1 + col("random_value"))))  # 40%の確率で十分な在庫
    ).withColumn(
        "inventory_type",
        when(col("inventory_quantity") == 0, 'OUT_OF_STOCK')
        .when(col("random_value") < 0.4, 'IN_STOCK')
        .otherwise('IN_TRANSIT')
    ).withColumn(
        "update_reason",
        when(col("inventory_quantity") == 0, '出荷')
        .when(col("random_value") < 0.4, '出荷')
        .when(col("random_value") < 0.8, '入荷')
        .otherwise('棚卸')
    )

    # 元のデータと在庫データを結合
    combined_data_with_inventory = combined_data.join(inventory_data, ["store_id", "item_id"])

    # 必要なカラムのみを選択し、適切なデータ型を指定
    result = combined_data_with_inventory.select(
        monotonically_increasing_id().cast(LongType()).alias("inventory_history_id"),
        col("item_id").cast(LongType()),
        col("store_id").cast(LongType()),
        col("inventory_quantity").cast(IntegerType()).alias("quantity"),
        col("inventory_type"),
        col("order_datetime").alias("inventory_datetime"),
        col("update_reason"),
        col("order_item_id").cast(LongType()).alias("related_order_id"),
        col("staff_id").cast(LongType()).alias("created_by"),
        col("order_datetime").alias("created_at"),
        col("order_datetime").alias("updated_at")
    )

    return result

# 実際にPOSとECデータを使って在庫履歴データを生成
inventory_history_data_df = generate_inventory_history_data_from_purchase_data(ec_order_items_with_store, pos_order_items_with_store)

# テーブル作成
inventory_history_data_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_inventory_history')

# データ表示
display(inventory_history_data_df.count()) 
display(inventory_history_data_df.limit(10))

# COMMAND ----------

print(inventory_history_data_df.agg({'store_id': 'min'}).collect()[0][0])
print(inventory_history_data_df.agg({'store_id': 'max'}).collect()[0][0])

# COMMAND ----------

# from pyspark.sql.functions import monotonically_increasing_id, col, when, rand, sum as sum_func, round, lit
# from pyspark.sql.types import IntegerType, StringType, LongType
# from pyspark.sql.window import Window

# # EC購買（アイテム単位）データの読み込み
# ec_order_items_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_ec_order_items')

# # POS購買（アイテム単位）データの読み込み
# pos_order_items_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders_items')

# # 店舗情報をEC/POSデータに統合するための処理
# orders_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders')

# # ECとPOSのデータに店舗情報を結合
# ec_order_items_with_store = ec_order_items_df.join(orders_df, 'order_id', 'left').select(
#     ec_order_items_df.order_item_id,
#     ec_order_items_df.item_id,
#     ec_order_items_df.quantity,
#     orders_df.store_id,
#     orders_df.staff_id,
#     ec_order_items_df.order_datetime
# )

# pos_order_items_with_store = pos_order_items_df.join(orders_df, 'order_id', 'left').select(
#     pos_order_items_df.order_item_id,
#     pos_order_items_df.item_id,
#     pos_order_items_df.quantity,
#     orders_df.store_id,
#     orders_df.staff_id,
#     pos_order_items_df.order_datetime
# )

# # 在庫履歴データを生成する関数
# def generate_inventory_history_data_from_purchase_data(ec_data, pos_data):
#     # ECデータとPOSデータを統合
#     combined_data = ec_data.union(pos_data)

#     # 店舗ごと商品ごとの販売数合計を計算
#     window = Window.partitionBy("store_id", "item_id")
#     total_sales = combined_data.groupBy("store_id", "item_id").agg(sum_func("quantity").alias("total_quantity"))

#     # 在庫数量を計算
#     inventory_quantity = total_sales.withColumn(
#         "inventory_quantity",
#         when(rand() < 0.2, 0)  # 20%の確率で在庫切れ
#         .when(rand() < 0.5, round(col("total_quantity") * rand() * 0.5))  # 40%の確率で在庫少なめ
#         .otherwise(round(col("total_quantity") * (1 + rand())))  # 40%の確率で十分な在庫
#     )

#     # 元のデータと在庫数量を結合
#     combined_data_with_inventory = combined_data.join(inventory_quantity, ["store_id", "item_id"])

#     # 必要なカラムのみを選択し、適切なデータ型を指定
#     result = combined_data_with_inventory.select(
#         monotonically_increasing_id().cast(LongType()).alias("inventory_history_id"),
#         col("item_id").cast(LongType()),
#         col("store_id").cast(LongType()),
#         col("inventory_quantity").cast(IntegerType()).alias("quantity"),

#         # inventory_typeをランダムに選択
#         when(rand() < 0.33, 'IN_STOCK') \
#         .when(rand() < 0.67, 'IN_TRANSIT') \
#         .otherwise('RESERVED').alias('inventory_type'),

#         col("order_datetime").alias("inventory_datetime"),

#         # update_reasonをランダムに選択
#         when(rand() < 0.5, '入荷') \
#         .when(rand() < 0.8, '出荷') \
#         .when(rand() < 0.95, '棚卸') \
#         .otherwise('返品').alias('update_reason'),

#         col("order_item_id").cast(LongType()).alias("related_order_id"),
#         col("staff_id").cast(LongType()).alias("created_by"),
#         col("order_datetime").alias("created_at"),
#         col("order_datetime").alias("updated_at")
#     )

#     return result

# # 実際にPOSとECデータを使って在庫履歴データを生成
# inventory_history_data_df = generate_inventory_history_data_from_purchase_data(ec_order_items_with_store, pos_order_items_with_store)

# # テーブル作成
# inventory_history_data_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_inventory_history')

# # データ表示
# display(inventory_history_data_df.count()) 
# display(inventory_history_data_df.limit(10))

# COMMAND ----------

# from pyspark.sql.functions import monotonically_increasing_id, col, when, rand
# from pyspark.sql.types import IntegerType, StringType, LongType

# # EC購買（アイテム単位）データの読み込み
# ec_order_items_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_ec_order_items')

# # POS購買（アイテム単位）データの読み込み
# pos_order_items_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders_items')

# # 店舗情報をEC/POSデータに統合するための処理
# orders_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_pos_orders')

# # ECとPOSのデータに店舗情報を結合
# ec_order_items_with_store = ec_order_items_df.join(orders_df, 'order_id', 'left').select(
#     ec_order_items_df.order_item_id,
#     ec_order_items_df.item_id,
#     ec_order_items_df.quantity,
#     orders_df.store_id,
#     orders_df.staff_id,
#     ec_order_items_df.order_datetime
# )

# pos_order_items_with_store = pos_order_items_df.join(orders_df, 'order_id', 'left').select(
#     pos_order_items_df.order_item_id,
#     pos_order_items_df.item_id,
#     pos_order_items_df.quantity,
#     orders_df.store_id,
#     orders_df.staff_id,
#     pos_order_items_df.order_datetime
# )

# # 在庫履歴データを生成する関数
# def generate_inventory_history_data_from_purchase_data(ec_data, pos_data):
#     # ECデータとPOSデータを統合
#     combined_data = ec_data.union(pos_data)

#     # 必要なカラムのみを選択し、適切なデータ型を指定
#     combined_data = combined_data.select(
#         monotonically_increasing_id().cast(LongType()).alias("inventory_history_id"),
#         col("item_id").cast(LongType()),
#         col("store_id").cast(LongType()),
#         col("quantity").cast(IntegerType()),

#         # inventory_typeをランダムに選択
#         when(rand() < 0.33, 'IN_STOCK') \
#         .when(rand() < 0.67, 'IN_TRANSIT') \
#         .otherwise('RESERVED').alias('inventory_type'),

#         col("order_datetime").alias("inventory_datetime"),

#         # update_reasonをランダムに選択
#         when(rand() < 0.5, '入荷') \
#         .when(rand() < 0.8, '出荷') \
#         .when(rand() < 0.95, '棚卸') \
#         .otherwise('返品').alias('update_reason'),

#         col("order_item_id").cast(LongType()).alias("related_order_id"),
#         col("staff_id").cast(LongType()).alias("created_by"),
#         col("order_datetime").alias("created_at"),
#         col("order_datetime").alias("updated_at")
#     )
#     return combined_data

# # 実際にPOSとECデータを使って在庫履歴データを生成
# inventory_history_data_df = generate_inventory_history_data_from_purchase_data(ec_order_items_with_store, pos_order_items_with_store)

# # テーブル作成
# inventory_history_data_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_inventory_history')

# # データ表示
# display(inventory_history_data_df.count()) 
# display(inventory_history_data_df.limit(10))

# COMMAND ----------

# DBTITLE 1,店舗ごとの在庫数チェック
# 在庫履歴テーブルと店舗テーブルを結合して、店舗ごとの在庫数を集計し、結果を表示
spark.sql(f"""
    SELECT 
        s.store_name,
        SUM(i.quantity) AS total_quantity
    FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_inventory_history AS i
    JOIN {MY_CATALOG}.{MY_SCHEMA}.bronze_stores AS s
    ON i.store_id = s.store_id
    GROUP BY s.store_name
    ORDER BY total_quantity DESC
""").display()

# COMMAND ----------

# DBTITLE 1,商品名ごとの在庫数チェック
# 在庫履歴と商品情報を結合して、商品ごとの在庫数を集計し、結果を表示
spark.sql(f"""
    SELECT 
        i.item_name,
        SUM(h.quantity) AS total_quantity
    FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_inventory_history AS h
    JOIN {MY_CATALOG}.{MY_SCHEMA}.bronze_items AS i
    ON h.item_id = i.item_id
    GROUP BY i.item_name
    ORDER BY total_quantity DESC
""").display()

# COMMAND ----------

# DBTITLE 1,更新理由チェック
# 更新理由ごとの在庫点数を集計し、結果を表示
spark.sql(f"""
    SELECT 
        update_reason,
        SUM(quantity) AS total_quantity
    FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_inventory_history
    GROUP BY update_reason
    ORDER BY total_quantity DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-13. bronze_web_logs / Web行動履歴

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import col, rand, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, IntegerType
import random
from datetime import datetime, timedelta

# 会員マスタから会員IDを取得
user_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_users')
user_ids = [row.user_id for row in user_df.select('user_id').distinct().collect()]

# EC購買データを取得
ec_order_items_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_ec_order_items')
ec_order_items = ec_order_items_df.select('user_id', 'order_datetime', 'item_id').collect()

# ランダムな日時を生成する関数
def random_datetime(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

# サンプルの検索キーワードリスト（スーパーのECサイト向け）
search_queries = [
    "りんご 1kgパック", "バナナ", "牛乳", "卵", "鶏肉", "豚肉", "トマト",
    "キャベツ", "オレンジジュース", "食パン", "バター", "ヨーグルト",
    "チーズ", "コーヒー豆"
]

# イベントタイプと画面の組み合わせを定義
event_page_combinations = {
    'PAGE_VIEW': ['ホーム画面', '注文履歴', '商品検索', '商品詳細', 'プロフィール', 'カート', 'お気に入り'],
    'CLICK': ['ホーム画面', '注文履歴', '商品検索', '商品詳細', 'プロフィール', 'カート', 'お気に入り'],
    'SEARCH': ['ホーム画面', '商品検索'],
    'ADD_TO_CART': ['商品詳細', 'お気に入り'],
    'PURCHASE': ['カート']
}

# サンプルデータを生成
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)
num_records = 10000

web_logs_data = []
for i in range(num_records):
    session_id = f"session_{random.randint(100000, 999999)}"
    
    # 会員IDを35%の確率で設定
    is_member = random.random() < 0.35
    user_id = random.choice(user_ids) if is_member else None
    
    # イベントタイプの設定（現実的な偏り）
    event_type = random.choices(
        ['PAGE_VIEW', 'CLICK', 'SEARCH', 'ADD_TO_CART', 'PURCHASE'],
        weights=[50, 20, 15, 10, 5]
    )[0]

    # イベントタイプに基づいて画面を選択
    page_title = random.choice(event_page_combinations[event_type])

    # 購買イベントの場合、EC購買データから情報を取得
    if event_type == 'PURCHASE' and is_member:
        user_purchases = [item for item in ec_order_items if item.user_id == user_id]
        if user_purchases:
            purchase = random.choice(user_purchases)
            event_datetime = purchase.order_datetime
            item_id = purchase.item_id
        else:
            event_datetime = random_datetime(start_date, end_date)
            item_id = None
    else:
        event_datetime = random_datetime(start_date, end_date)
        item_id = random.randint(1000, 9999) if event_type in ['ADD_TO_CART', 'PURCHASE'] else None

    # 検索キーワードの設定
    search_query = random.choice(search_queries) if event_type == 'SEARCH' else None

    # デバイスタイプとブラウザ、オペレーティングシステムの設定
    device_type = random.choices(['MOBILE', 'DESKTOP', 'TABLET'], weights=[60, 30, 10])[0]
    browser = random.choices(['Chrome', 'Safari', 'Firefox', 'Edge', 'Others'], weights=[65, 20, 5, 5, 5])[0]
    os = random.choices(['Android', 'iOS', 'Windows', 'macOS', 'Linux'], weights=[45, 25, 20, 5, 5])[0]
    
    # ユーザーエージェントの生成（簡易版）
    user_agent = f"{browser}/{random.randint(60,100)}.0 ({os})"

    # UTMソース、UTMメディア、リファラーURLの設定
    utm_source = random.choices(['google', 'yahoo', 'facebook', 'newsletter', 'unknown'], weights=[40, 10, 20, 20, 10])[0]
    utm_medium = random.choices(['cpc', 'banner', 'email', 'social', 'unknown'], weights=[50, 10, 20, 15, 5])[0]
    referrer_url = random.choices(["https://www.google.com", "https://www.yahoo.com", "https://www.facebook.com", None], weights=[50, 10, 20, 20])[0]

    web_logs_data.append((
        i + 1000001,                                                        # ログID
        session_id,                                                         # セッションID
        user_id,                                                            # 会員ID (35%の確率で設定)
        event_datetime,                                                     # イベント日時
        event_type,                                                         # イベントタイプ
        page_title,                                                         # ページタイトル
        f"https://example.com/{page_title.lower().replace(' ', '_')}",      # ページURL
        referrer_url,                                                       # リファラーURL
        search_query,                                                       # 検索キーワード
        item_id,                                                            # 商品ID
        random.randint(100,199),                                            # カテゴリID
        device_type,                                                        # デバイスタイプ
        browser,                                                            # ブラウザ
        os,                                                                 # OS
        f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",         # IPアドレス
        user_agent,                                                         # ユーザーエージェント
        random.randint(10,300),                                             # ページ滞在時間
        random.randint(0,100),                                              # スクロール深度
        random.randint(0,1920),                                             # クリック位置X
        random.randint(0,1080),                                             # クリック位置Y
        utm_source,                                                         # UTMソース
        utm_medium,                                                         # UTMメディア
        random.choice(["autumn_sale_2024", "spring_promo_2024"]),           # UTMキャンペーン
        event_datetime                                                      # 作成日時
    ))

# Web行動履歴のスキーマを定義
schema_web_logs = StructType([
    StructField("log_id", LongType(), False),                # ログID
    StructField("session_id", StringType(), False),          # セッションID
    StructField("user_id", StringType(), True),              # 会員ID
    StructField("event_datetime", TimestampType(), False),   # イベント日時
    StructField("event_type", StringType(), False),          # イベントタイプ
    StructField("page_title", StringType(), True),           # ページタイトル
    StructField("page_url", StringType(), True),             # ページURL
    StructField("referrer_url", StringType(), True),         # リファラーURL
    StructField("search_query", StringType(), True),         # 検索キーワード
    StructField("item_id", LongType(), True),                # 商品ID
    StructField("category_id", LongType(), True),            # カテゴリID
    StructField("device_type", StringType(), True),          # デバイスタイプ
    StructField("browser", StringType(), True),              # ブラウザ
    StructField("os", StringType(), True),                   # オペレーティングシステム
    StructField("ip_address", StringType(), True),           # IPアドレス
    StructField("user_agent", StringType(), True),           # ユーザーエージェント情報
    StructField("time_spent", IntegerType(), True),          # ページ滞在時間
    StructField("scroll_depth", IntegerType(), True),        # スクロール深度（%）
    StructField("click_position_x", IntegerType(), True),    # クリック位置X座標
    StructField("click_position_y", IntegerType(), True),    # クリック位置Y座標
    StructField("utm_source", StringType(), True),           # UTMソース
    StructField("utm_medium", StringType(), True),           # UTMメディア
    StructField("utm_campaign", StringType(), True),         # UTMキャンペーン
    StructField("created_at", TimestampType(), False)        # 作成日時
])

# DataFrameの作成
web_logs_df = spark.createDataFrame(web_logs_data, schema=schema_web_logs)

# Deltaテーブル作成
web_logs_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_web_logs')

# データ表示
display(web_logs_df.count())
display(web_logs_df.limit(10))

# COMMAND ----------

# DBTITLE 1,イベントタイプ構成比チェック
# クエリを実行し、結果を表示
spark.sql(f"""
SELECT
    event_type,
    COUNT(*) as count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
FROM
    {MY_CATALOG}.{MY_SCHEMA}.bronze_web_logs
GROUP BY
    event_type
ORDER BY
    count DESC
""").display()

# COMMAND ----------

# DBTITLE 1,会員ID紐付いてる割合チェック
# クエリを実行し、結果を表示
spark.sql(f"""
SELECT 
    (COUNT(CASE WHEN user_id IS NOT NULL THEN 1 END) * 100.0 / COUNT(*)) AS member_percentage
FROM
    {MY_CATALOG}.{MY_SCHEMA}.bronze_web_logs
""").display()

# COMMAND ----------

# DBTITLE 1,サンキーチャート？
# # クエリを実行し、結果を表示
# spark.sql(f"""
# WITH ordered_events AS (
#   SELECT
#     user_id,
#     event_type,
#     event_datetime,
#     ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_datetime) AS event_order
#   FROM {MY_CATALOG}.{MY_SCHEMA}.bronze_web_logs
#   WHERE user_id IS NOT NULL
# ),
# event_transitions AS (
#   SELECT
#     e1.event_type AS source,   -- ソースイベント
#     e2.event_type AS target,   -- ターゲットイベント
#     e1.user_id
#   FROM ordered_events e1
#   JOIN ordered_events e2
#     ON e1.user_id = e2.user_id
#     AND e1.event_order = e2.event_order - 1
# )
# SELECT
#   source,   -- ソースイベント
#   target,   -- ターゲットイベント
#   COUNT(*) AS weight   -- 遷移回数（重み）
# FROM event_transitions
# GROUP BY source, target
# ORDER BY weight DESC
# """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-14. bronze_app_events / Appイベント履歴

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import col, rand, current_timestamp
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
import random
from datetime import datetime, timedelta

# 会員マスタから会員IDを取得
user_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_users')
user_ids = [row.user_id for row in user_df.select('user_id').distinct().collect()]

# EC購買データを取得
ec_order_items_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_ec_order_items')
ec_order_items = ec_order_items_df.select('user_id', 'order_datetime', 'item_id', 'category_id').collect()

# カート履歴からカートIDを取得
cart_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_carts')
cart_ids = [row.cart_id for row in cart_df.select('cart_id').distinct().collect()]

# ランダムな日時を生成する関数
def random_datetime(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

# イベントタイプと画面名の組み合わせを定義
event_screen_combinations = {
    'APP_OPEN': ['ホーム画面', '商品詳細'],
    'CLICK': ['ホーム画面', '注文履歴', '商品詳細', 'プロフィール', 'カート'],
    'ADD_TO_CART': ['注文履歴', '商品詳細'],
    'PURCHASE': ['カート']
}

# セッションIDを生成する関数
def generate_session_id():
    return f"session_{random.randint(100000, 999999)}"

# ユーザーごとのセッションを管理するディクショナリ
user_sessions = {}

# セッションの有効期限を確認し、必要に応じて新しいセッションを開始する関数
def get_or_create_session(user_id, event_datetime):
    if user_id not in user_sessions or (event_datetime - user_sessions[user_id]['last_activity']).total_seconds() > 1800:
        # 30分（1800秒）以上の間隔があれば新しいセッションを開始
        user_sessions[user_id] = {
            'session_id': generate_session_id(),
            'last_activity': event_datetime
        }
    else:
        # 既存のセッションの最終アクティビティ時間を更新
        user_sessions[user_id]['last_activity'] = event_datetime
    
    return user_sessions[user_id]['session_id']

# サンプルデータを生成
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)
num_records = 10000

app_events_data = []
for i in range(num_records):
    user_id = random.choice(user_ids)
    event_datetime = random_datetime(start_date, end_date)
    session_id = get_or_create_session(user_id, event_datetime)
    
    event_type = random.choices(['APP_OPEN', 'CLICK', 'ADD_TO_CART', 'PURCHASE'], weights=[50, 20, 15, 5])[0]
    screen_name = random.choice(event_screen_combinations[event_type])
    
    # 購買イベントの場合、EC購買データから情報を取得
    if event_type == 'PURCHASE':
        user_purchases = [item for item in ec_order_items if item.user_id == user_id]
        if user_purchases:
            purchase = random.choice(user_purchases)
            event_datetime = purchase.order_datetime
            item_id = purchase.item_id
            category_id = purchase.category_id
        else:
            event_datetime = random_datetime(start_date, end_date)
            item_id = None
            category_id = None
    else:
        event_datetime = random_datetime(start_date, end_date)
        if event_type in ['CLICK', 'ADD_TO_CART']:
            item = random.choice(ec_order_items)
            item_id = item.item_id
            category_id = item.category_id
        else:
            item_id = None
            category_id = None

    # カートIDをイベントタイプに応じて追加
    cart_id = random.choice(cart_ids) if event_type in ['ADD_TO_CART', 'PURCHASE'] else None
    
    app_events_data.append((
        i + 1000001,                                                            # イベントID
        session_id,                                                             # セッションID
        user_id,                                                                # 会員ID
        event_datetime,                                                         # イベント日時
        event_type,                                                             # イベントタイプ
        screen_name,                                                            # 画面名
        item_id,                                                                # 商品ID
        category_id,                                                            # カテゴリID
        random.choices(['PHONE', 'TABLET'], weights=[85, 15])[0],               # デバイスタイプ
        random.choices(['iOS', 'Android'], weights=[60, 40])[0],                # OS
        f"{random.randint(1,2)}.{random.randint(0,9)}.{random.randint(0,9)}",   # アプリバージョン
        f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",             # IDアドレス
        cart_id,                                                                # カートID
        event_datetime                                                          # 作成日時
    ))

# アプリイベント履歴のスキーマを定義
schema_app_events = StructType([
    StructField("event_id", LongType(), False),             # イベントID
    StructField("session_id", StringType(), False),         # セッションID
    StructField("user_id", StringType(), False),            # 会員ID
    StructField("event_datetime", TimestampType(), False),  # イベント日時
    StructField("event_type", StringType(), False),         # イベントタイプ
    StructField("screen_name", StringType(), True),         # 画面名
    StructField("item_id", LongType(), True),               # 商品ID
    StructField("category_id", LongType(), True),           # カテゴリID
    StructField("device_type", StringType(), False),        # デバイスタイプ
    StructField("os", StringType(), False),                 # OS
    StructField("app_version", StringType(), True),         # アプリバージョン
    StructField("ip_address", StringType(), True),          # IDアドレス
    StructField("cart_id", LongType(), True),               # カートID
    StructField("created_at", TimestampType(), False)       # 作成日時
])

# DataFrameの作成
app_events_df = spark.createDataFrame(app_events_data, schema=schema_app_events)

# Deltaテーブル作成
app_events_df.write.mode('overwrite').format('delta').saveAsTable(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_app_events')

# データフレームの表示
display(app_events_df.count())
display(app_events_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Appイベント構成比チェック
# クエリを実行し、結果を表示
spark.sql(f"""
SELECT
    event_type,
    COUNT(*) as count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
FROM
    {MY_CATALOG}.{MY_SCHEMA}.bronze_app_events
GROUP BY
    event_type
ORDER BY
    count DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-15. bronze_loyalty_points / ロイヤリティプログラム

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import col, when, lit, expr, current_timestamp, monotonically_increasing_id, rand, date_add, to_date
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, DateType, TimestampType
import random
from datetime import datetime, timedelta

# 会員マスタから会員IDを取得
user_df = spark.table(f'{MY_CATALOG}.{MY_SCHEMA}.bronze_users')
user_ids = [row.user_id for row in user_df.select('user_id').distinct().collect()]

# ランダムな日時を生成する関数
def random_datetime(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )

# サンプルデータを生成
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)
num_records = len(user_ids)

loyalty_points_data = []
for i in range(num_records):
    earned_at = random_datetime(start_date, end_date)
    point_balance = random.randint(0, 10000)
    member_rank = random.choice(['BRONZE', 'SILVER', 'GOLD', 'PLATINUM'])
    
    loyalty_points_data.append((
        1000001 + i,  # point_id
        user_ids[i],  # user_id
        point_balance,  # point_balance
        earned_at.date(),  # point_expiration_date (1年後の計算はDataFrame作成後に行う)
        member_rank,  # member_rank
        random.randint(0, 5000),  # points_to_next_rank
        earned_at.date(),  # rank_expiration_date (1年後の計算はDataFrame作成後に行う)
        random.choice(['ACTIVE', 'USED', 'EXPIRED']),  # status
        earned_at,  # earned_at
        earned_at,  # created_at
        earned_at  # updated_at
    ))

# ロイヤリティプログラムのスキーマを定義
schema_loyalty_points = StructType([
    StructField("point_id", LongType(), False),                 # ポイントID
    StructField("user_id", StringType(), False),                # 会員ID
    StructField("point_balance", IntegerType(), False),         # ポイント残高
    StructField("point_expiration_date", DateType(), True),     # ポイント有効期限
    StructField("member_rank", StringType(), False),            # 会員ランク
    StructField("points_to_next_rank", IntegerType(), False),   # 次のランクまでの残ポイント
    StructField("rank_expiration_date", DateType(), True),      # 会員ランク有効期限
    StructField("status", StringType(), False),                 # ステータス
    StructField("earned_at", TimestampType(), False),           # 獲得日時
    StructField("created_at", TimestampType(), False),          # 作成日時
    StructField("updated_at", TimestampType(), False)           # 更新日時
])

# DataFrameの作成
loyalty_points_df = spark.createDataFrame(loyalty_points_data, schema_loyalty_points)

# 有効期限と会員ランク有効期限を1年後に設定
loyalty_points_df = loyalty_points_df.withColumn("point_expiration_date", date_add(col("earned_at"), 365))
loyalty_points_df = loyalty_points_df.withColumn("rank_expiration_date", date_add(col("earned_at"), 365))

# Deltaテーブル作成
loyalty_points_df.write.mode("overwrite").format("delta").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_loyalty_points")

display(loyalty_points_df.count())
display(loyalty_points_df.limit(10))

# COMMAND ----------

# DBTITLE 1,重複チェック
# 重複チェック
bronze_loyalty_points_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_loyalty_points")

# user_id の重複確認
user_id_duplicates = bronze_loyalty_points_df.groupBy("user_id").count().filter("count > 1").count()

print(f"会員IDの重複: {user_id_duplicates}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-16. bronze_survey / アンケート回答履歴

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql.functions import col, when, lit, expr, current_timestamp, monotonically_increasing_id, rand
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
import random
from datetime import datetime, timedelta

# アンケート回答履歴のスキーマを定義
schema_survey_responses = StructType([
    StructField("response_id", LongType(), False),
    StructField("user_id", StringType(), True),
    StructField("survey_category", StringType(), False),
    StructField("response_content", StringType(), False),
    StructField("response_datetime", TimestampType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), True)  # NULLを許容するように変更
])

# アンケートカテゴリとリアルな要望・不満を反映した回答内容
sample_categories_responses = {
    "商品品質": [
        "野菜の鮮度が非常に良く、いつも新鮮な食材を購入できて満足しています。",
        "肉の品質が素晴らしいです。特に和牛の味は絶品です。",
        "魚介類の種類が豊富で、いつも新鮮なものが選べます。",
        "果物の甘さと質が安定していて、いつも楽しみに購入しています。",
        "野菜の鮮度が日によってばらつきがあり、長持ちしないことがあります。",
        "購入した肉に脂肪が多く、もう少し赤身を増やしてほしいです。",
        "冷凍食品の品質があまりよくなく、解凍すると水っぽくなります。",
        "果物の値段が少し高めなのに、味がイマイチなことがある。",
        "魚の種類が少なく、新鮮さに欠ける日も多いです。"
    ],
    "価格": [
        "全体的に価格が手頃で、家計の助けになっています。",
        "セール品の価格設定がとてもお得だと感じます。",
        "高級品の価格がやや高いように感じます。",
        "日用品の価格が他店と比べて少し高いように思います。",
        "全体的に価格が他店より高めで、毎週通うのが少し負担に感じます。",
        "特売の割引が思ったより少なく、お得感が感じられません。",
        "日用品の価格が高いので、ネットで購入した方が安いことが多いです。",
        "プライベートブランドの商品ももう少し安くしてほしいです。",
        "高級品も良いですが、もっと手頃な価格の選択肢が欲しいです。"
    ],
    "品揃え": [
        "地元の特産品が豊富で、地域の味を楽しめるのが良いです。",
        "輸入食品の種類がもう少し増えると嬉しいです。",
        "健康食品やオーガニック製品の品揃えが充実していて助かります。",
        "季節の商品が豊富で、旬の味を楽しめるのが素晴らしいです。",
        "外国産の食品が少なく、もっとバリエーションを増やしてほしい。",
        "オーガニック食品の品揃えが少なく、定期購入できるようにしてほしい。",
        "冷凍食品の種類が限られていて、もっと選べると便利です。",
        "季節の果物が品薄で、すぐに売り切れてしまいます。",
        "もっとヘルシーな食品や、ダイエット向けの商品を増やしてほしい。"
    ],
    "店舗環境": [
        "店内が清潔で、快適に買い物ができます。",
        "レジの待ち時間が短くて助かります。",
        "駐車場がもう少し広いと便利です。",
        "商品の配置が分かりやすく、効率よく買い物ができます。",
        "駐車場が狭く、週末には混雑して停めるのが大変です。",
        "店内が混みすぎて通路が狭く、カートを押しにくいです。",
        "エアコンの温度が低すぎて、店内が寒いと感じます。",
        "レジに並ぶ時間が長く、急いでいるときには不便です。",
        "店内案内が分かりにくく、欲しい商品がどこにあるか分かりません。"
    ],
    "サービス": [
        "スタッフの対応が丁寧で、質問にも親切に答えてくれます。",
        "商品の場所を尋ねると、すぐに案内してくれて助かります。",
        "レジでの対応が迅速で、スムーズに会計ができます。",
        "試食コーナーがあり、新商品を試せるのが良いです。",
        "宅配サービスがないので、重い商品は買いにくいです。",
        "店員が少なく、質問しようにも見つからないことが多いです。",
        "商品の返品対応が遅く、改善してほしい。",
        "ポイント付与が少ないので、もっと特典を増やしてほしい。",
        "セルフレジが少なく、混雑時には不便です。"
    ]
}

# ランダムな日時を生成する関数
def random_datetime(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )

# サンプルデータを生成
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)
num_responses = 1000

survey_responses = []
for i in range(num_responses):
    response_datetime = random_datetime(start_date, end_date)
    category = random.choice(list(sample_categories_responses.keys()))
    survey_responses.append((
        1000001 + i,  # response_id
        f"cdp{str(random.randint(1, 1000)).zfill(12)}",  # user_id
        category,  # survey_category
        random.choice(sample_categories_responses[category]),  # response_content
        response_datetime,  # response_datetime
        response_datetime,  # created_at
        response_datetime if random.random() > 0.5 else None  # updated_at (50%の確率でNullを許容)
    ))

# DataFrameの作成
survey_responses_df = spark.createDataFrame(survey_responses, schema_survey_responses)

# Deltaテーブル作成
survey_responses_df.write.mode("overwrite").format("delta").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_survey")

# データフレームの表示
display(survey_responses_df.count())
display(survey_responses_df.limit(10))
