# Databricks notebook source
# MAGIC %md
# MAGIC # ETLパイプラインの構築
# MAGIC - レコメンドモデルのトレーニングに必要なデータを加工します  
# MAGIC - サーバレス or DBR 16.0ML以降

# COMMAND ----------

# MAGIC %md
# MAGIC # ライブイベント向け商品レコメンド - スタジアムアナリティクス
# MAGIC <img style='float: right' width='600px' src='https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-notif.png'>
# MAGIC
# MAGIC **ユースケースの概要**
# MAGIC * スポーツのライブ中継では大量の顧客データが生成されます。より良いファン体験を実現するのに活用できます。ファンとの関わりは、売上や顧客維持率の向上に直結します。このノートブックでは、ファンの購買履歴とスタジアムの座席の位置情報をもとにパーソナライズされた割引オファーを作成し、イベント中の追加販売を促進し、より個人的な体験を実現する方法を紹介します。
# MAGIC *目標：ファンへキャンペーンのオファーのプッシュ通知を送信します。
# MAGIC  
# MAGIC **ソリューションのビジネスインパクト**
# MAGIC * **ファン・エンゲージメントと顧客維持：** イベントに参加している間、より良い経験をしているファンは、将来的に別のイベントのために戻ってくる可能性が高くなります。
# MAGIC * **収益の増加：**ファンに パーソナライズされた割引キャンペーンを送信すると、イベント中にスタジアムの中の店からの購入が増えるのでお店(ベンダー)の収益の増加にも繋がります。
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Fproduct_recommender_stadium%2Fnotebook&dt=MEDIA_USE_CASE">
# MAGIC <!-- [metadata={"description":"Product recommendation for live events. This demo shows how to create a personalized discount for a fan based on their purchasing history and location of where they sit in a stadium to drive additional sales during the game and create a more individualized experience.",
# MAGIC  "authors":["dan.morris@databricks.com"],
# MAGIC   "db_resources":{},
# MAGIC   "search_tags":{"vertical": "media", "step": "Data Engineering", "components": ["sparkml", "mlflow", "als", "recommender"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC カタログ構成
# MAGIC ```
# MAGIC /<catalog_name>
# MAGIC ├── product_recommendation_stadium        <- スキーマ
# MAGIC │   ├── ボリューム
# MAGIC │       ├── data/games                    <- games.csv
# MAGIC │       ├── data/purchase_history         <- purchase_history.csv
# MAGIC │       ├── data/vendors                  <- vendors.csv
# MAGIC │   ├── テーブル
# MAGIC │       ├── bz_stadium_vendors            <- 販売店とアイテムマスタ
# MAGIC │       ├── bz_ticket_sales               <- チケットの売上データ（Fakerで生成します）
# MAGIC │       ├── bz_games                      <- イベントのスケジュール
# MAGIC │       ├── bz_point_of_sale              <- POS注文履歴（Fakerで生成します）
# MAGIC │       ├── bz_purchase_history           <- 購入履歴（過去の推奨アイテム履歴含む）
# MAGIC │       ├── sv_sales                      <- トレーニング用データ
# MAGIC │       ├── gd_sections_recommendations   <- 顧客ごとレコメンドTOP10
# MAGIC │       ├── gd_final_recommendations      <- 顧客ごとベストレコメンド
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## ダミデータ作成のdbldatagenのインストール

# COMMAND ----------

# MAGIC %pip install git+https://github.com/databrickslabs/dbldatagen faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC 該当スキーマ配下の既存bronze/silverテーブルを全て削除

# COMMAND ----------

# # スキーマ内のすべてのテーブル名を取得する
# tables_df = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}")

# # テーブル名が "bronze_" で始まるテーブルのみ削除する
# for table in tables_df.collect():
#     table_name = table["tableName"]
#     if table_name.startswith("bz_") or table_name.startswith("sv_"):
#         spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{table_name}")
#         print(f"削除されたテーブル: {table_name}")

# print("全ての bz_ または sv_で始まるテーブルが削除されました。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronzeテーブル作成（データ読込）
# MAGIC ここではデモデータの生成処理を含みます
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-1.png" width="1000px">

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-1. bz_stadium_vendors / VendorとItemのリスト
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-vendors.png" width="400px" style="float: right" />
# MAGIC  
# MAGIC PandasとSparkを使えば、これらのデータを簡単に取り込み、データ型を調整することができます。

# COMMAND ----------

print("Vendors Item type data generation...")
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType

# 商品マスタのスキーマ定義
schema_items = StructType([
    StructField("vendor_id", LongType(), False),              # ベンダーID
    StructField("vendor_location_number", LongType(), False), # ベンダー所在地番号
    StructField("vendor_name", StringType(), False),          # ベンダー名
    StructField("vendor_scope", StringType(), False),         # ベンダースコープ
    StructField("section", LongType(), False),                # セクション
    StructField("item_id", LongType(), False),                # 商品ID
    StructField("item_type", StringType(), False),            # 商品タイプ
    StructField("item", StringType(), False),                 # 商品名
    StructField("price", LongType(), False),                  # 価格
    StructField("error", StringType(), True),                 # エラー
    StructField("item_img_url", StringType(), True)           # 画像URL
])

# CSV読み込み（ヘッダなし）
df_items = spark.read.csv(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/vendors/vendors.csv", 
                          header=False, 
                          schema=schema_items)

# Gitリポジトリ上の画像バスを指定
git_owner="komae5519pv"
git_repo="komae_dbdemos"
git_img_dir = "/product_recommendation_20250411/_images"

# 画像URLカラム追加（マッピングを条件付きで適用）
df_items = df_items.withColumn("item_img_url", 
  F.when(df_items["item"] == "Big Boy Original Double Decker", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/01_burger.jpg?raw=true")
  .when(df_items["item"] == "Big Poppa", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/15_sandwiches.jpg?raw=true")
  .when(df_items["item"] == "Craft Beer", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/26_cluft_beer.jpg?raw=true")
  .when(df_items["item"] == "Wine", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/28_wine.png?raw=true")
  .when(df_items["item"] == "Domestic Beer", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/27_druft_beer.jpg?raw=true")
  .when(df_items["item"] == "Cocktails and Domestic Beer", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/26_cluft_beer.jpg?raw=true")
  .when(df_items["item"] == "Bloody Mary", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/29_bloody_mary.jpg?raw=true")
  .when(df_items["item"] == "Premium Cocktail", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/25_color_juice.jpg?raw=true")
  .when(df_items["item"] == "Seltzer", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/30_Seltzer.jpg?raw=true")
  .when(df_items["item"] == "Well Cocktail", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/25_color_juice.jpg?raw=true")
  .when(df_items["item"] == "Cheeseburger with Fries", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/31_Cheeseburger_with_Fries.jpg?raw=true")
  .when(df_items["item"] == "Double Cheeseburger with Fries", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/31_Cheeseburger_with_Fries.jpg?raw=true")
  .when(df_items["item"] == "Large Cheeseburger with Fries", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/31_Cheeseburger_with_Fries.jpg?raw=true")
  .when(df_items["item"] == "Fries", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/07_potato.jpg?raw=true")
  .when(df_items["item"] == "Coney Dog with Chips", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/11_hotdog.jpg?raw=true")
  .when(df_items["item"] == "Value Meal", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/11_hotdog.jpg?raw=true")
  .when(df_items["item"] == "Pulled Pork Dog with Chips", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/11_hotdog.jpg?raw=true")
  .when(df_items["item"] == "Hot Dog", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/11_hotdog.jpg?raw=true")
  .when(df_items["item"] == "Nacho Grande", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/32_Nacho_Grande.png?raw=true")
  .when(df_items["item"] == "Loaded Nacho Supreme", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/33_Loaded_Nacho_Supreme.jpg?raw=true")
  .when(df_items["item"] == "Pulled Pork Nachos", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/33_Loaded_Nacho_Supreme.jpg?raw=true")
  .when(df_items["item"] == "Milkshakes", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/34_Milkshakes.jpg?raw=true")
  .when(df_items["item"] == "Regular or Large Soda", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/23_red_jouce.jpg?raw=true")
  .when(df_items["item"] == "Bottomless Souvenir Soda", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/25_color_juice.jpg?raw=true")
  .when(df_items["item"] == "Fresh Lemonade", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/24_lime_juice.jpg?raw=true")
  .when(df_items["item"] == "Gatorade", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/23_red_jouce.jpg?raw=true")
  .when(df_items["item"] == "Coffee", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/35_Coffee.jpeg?raw=true")
  .when(df_items["item"] == "Espresso", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/35_Coffee.jpeg?raw=true")
  .when(df_items["item"] == "Cheese Pizza", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/05_pizza.jpg?raw=true")
  .when(df_items["item"] == "Sausage and Peppers Pizza", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/36_Sausage_Peppers_Pizza.jpeg?raw=true")
  .when(df_items["item"] == "Pepperoni Pizza", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/36_Sausage_Peppers_Pizza.jpeg?raw=true")
  .when(df_items["item"] == "Pretzels", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/37_Pretzels.jpg?raw=true")
  .when(df_items["item"] == "Pretzel", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/37_Pretzels.jpg?raw=true")
  .when(df_items["item"] == "Wing Dings with Fries", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/39_Korean_Philly_Cheesesteak.jpeg?raw=true")
  .when(df_items["item"] == "Korean Philly Cheesesteak", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/13_chicken_wing.jpg?raw=true")
  .when(df_items["item"] == "Chicken Tender Basket", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/13_chicken_wing.jpg?raw=true")
  .when(df_items["item"] == "Chicken Hoagie", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/40_Chicken_Hoagie.png?raw=true")
  .when(df_items["item"] == "Lefty's Cheesteak", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/15_sandwiches.jpg?raw=true")
  .when(df_items["item"] == "Slim Jim", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/41_Slim_Jim.png?raw=true")
  .when(df_items["item"] == "Southpaw", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/42_Sandwich.png?raw=true")
  .when(df_items["item"] == "Chicken Tenders with Fries", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/38_Wing_Dings_Fries.jpg?raw=true")
  .when(df_items["item"] == "Grilled Chicken Breast Sandwich ", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/42_Sandwich.png?raw=true")
  .when(df_items["item"] == "Chicken Fingers with Fries", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/38_Wing_Dings_Fries.jpg?raw=true")
  .when(df_items["item"] == "Smoked Turkey Club Sandwich", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/43_Smoked_Turkey_Club_Sandwich.png?raw=true")
  .when(df_items["item"] == "Vegan Buffalo Cauliflower Wrap", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/42_Sandwich.png?raw=true")
  .when(df_items["item"] == "Bottomless Souvenir Popcorn", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/44_Popcorn.jpg?raw=true")
  .when(df_items["item"] == "Cookie", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/45_Cookie.jpg?raw=true")
  .when(df_items["item"] == "Cake Slice", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/46_Cake_Slice.jpg?raw=true")
  .when(df_items["item"] == "Waffle Taco Duo", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/47_Waffle_Taco_Duo.jpg?raw=true")
  .when(df_items["item"] == "Peanuts", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/48_Peanuts.jpg?raw=true")
  .when(df_items["item"] == "Cake Cup", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/49_Cake_Cup.jpg?raw=true")
  .when(df_items["item"] == "Cookies", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/45_Cookie.jpg?raw=true")
  .when(df_items["item"] == "Popcorn", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/44_Popcorn.jpg?raw=true")
  .when(df_items["item"] == "Cheese Chips", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/50_Cheese_Chips.jpg?raw=true")
  .when(df_items["item"] == "Brownie", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/46_Cake_Slice.jpg?raw=true")
  .when(df_items["item"] == "Cheese Cups", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/51_Cheese_Cups.jpg?raw=true")
  .when(df_items["item"] == "Bottled Water", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/52_Bottled_Water.jpg?raw=true")
  .when(df_items["item"] == "Deli Style Corned Beef & Swiss", f"https://github.com/{git_owner}/{git_repo}/blob/main{git_img_dir}/43_Smoked_Turkey_Club_Sandwich.png?raw=true")
  .otherwise(f"")
  )

# テーブル再作成
df_items.write.mode("overwrite").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bz_stadium_vendors")

print(df_items.count())
print(df_items.columns)
display(df_items.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-2. bz_ticket_sales / チケットの売上データ
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-pricing.png" width="400px" style="float: right" />
# MAGIC
# MAGIC チケットの売上データもraw tableに書き込みます。

# COMMAND ----------

print("Ticket sales data generation...")
import dbldatagen as dg
from pyspark.sql.types import *
from pyspark.sql import functions as F
from faker import Faker
fake = Faker()

fake_name = F.udf(fake.name)
fake_phone = F.udf(fake.phone_number)

data_rows = 70000
df_spec = (dg.DataGenerator(spark, name="ticket_sales", rows=data_rows, partitions=4)
                            .withIdOutput()
                            .withColumn("game_id", IntegerType(), values=['3'])
                            .withColumn("ticket_price", IntegerType(), expr="floor(rand() * 350)")
                            .withColumn("gate_entrance", StringType(), values=['a', 'b', 'c', 'd', 'e', ], random=True, weights=[9, 1, 1, 2, 2])
                            .withColumn("section_number", IntegerType(), minValue=100, maxValue=347, random=True)
                            .withColumn("row_number", IntegerType(), minValue=1, maxValue=35 , random=True)
                            .withColumn("seat_number", IntegerType(), minValue=1, maxValue=30, random=True)
                            .withColumn("ticket_type", StringType(), values=['single_game', 'packaged_game', 'season_holder'], random=True, weights=[7, 2, 1]))
df_tickets = df_spec.build().withColumnRenamed("id", "customer_id")
df_tickets = df_tickets.withColumn("customer_name", fake_name())
df_tickets = df_tickets.withColumn("phone_number", fake_phone())

# create table
spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.bz_ticket_sales")
df_tickets.write.saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bz_ticket_sales")

print(df_tickets.count())
print(df_tickets.columns)
display(df_tickets.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-3. bz_games / イベントのスケジュール
# MAGIC
# MAGIC 試合のイベントデータも同じ方法で取り込みます。

# COMMAND ----------

print("Game data generation...")
from pyspark.sql.functions import col, to_date
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

schemaDDL = "game_id int, date_time string, date string, location string, home_team string, away_team string, error string, game_date string"
df_games = spark.read.csv(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/games/games.csv", sep=',', schema=schemaDDL)
df_games = df_games.withColumn("game_date", to_date(col("game_date"),'yyyy-MM-dd'))

# create table
spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.bz_games")
df_games.write.saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bz_games")

print(df_games.count())
print(df_games.columns)
display(df_games.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-4. bz_point_of_sale / POS注文履歴
# MAGIC
# MAGIC POSデータも取り込みます。

# COMMAND ----------

print("Point of sales generation...")
data_rows = 1000000
df_spec = (dg.DataGenerator(spark, name="point_of_sale", rows=data_rows, partitions=4)
                            .withIdOutput()
                            .withColumn("game", IntegerType(), minValue=1, maxValue=97 , random=True)
                            .withColumn("item_purchased", IntegerType(), minValue=1, maxValue=364, random=True)
                            .withColumn("customer", IntegerType(), minValue=1, maxValue=1000, random=True))
                            
df_pos = df_spec.build().withColumnRenamed("id", "order_id")

# create table
spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.bz_point_of_sale")
df_pos.write.saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bz_point_of_sale")

print(df_pos.count())
print(df_pos.columns)
display(df_pos.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-5. bz_purchase_history / 購入履歴

# COMMAND ----------

print("Purchase History to evaluate model efficiency after real game...")
import pandas as pd

pandasDF = pd.read_csv(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/purchase_history/purchase_history.csv")
df_spec = spark.createDataFrame(pandasDF, ['item_id', 'game_id', 'customer_id', 'recommended_item_purchased'])

# create table
spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.bz_purchase_history")
df_spec.write.saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bz_purchase_history")

print(df_spec.count())
print(df_spec.columns)
display(df_spec.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silverテーブル作成（加工）
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-2.png" width="1000px">

# COMMAND ----------

silver_sales_df = spark.sql(f'''
  SELECT * FROM {MY_CATALOG}.{MY_SCHEMA}.bz_ticket_sales t 
  JOIN {MY_CATALOG}.{MY_SCHEMA}.bz_point_of_sale p ON t.customer_id = p.customer
  JOIN {MY_CATALOG}.{MY_SCHEMA}.bz_stadium_vendors s ON p.item_purchased = s.item_id AND t.game_id = p.game;  
''')

# create table
spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.sv_sales")
silver_sales_df.write.saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.sv_sales")

print(silver_sales_df.count())
print(silver_sales_df.columns)
display(silver_sales_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC 最も購入されたアイテムのトップ 10

# COMMAND ----------

spark.sql(f'''
SELECT
  item_id,
  count(item_id) AS item_purchases
FROM {MY_CATALOG}.{MY_SCHEMA}.sv_sales
GROUP BY item_id
ORDER BY item_purchases DESC LIMIT 10
''').show()
