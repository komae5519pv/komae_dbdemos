# Databricks notebook source
# MAGIC %md
# MAGIC ## データ準備
# MAGIC
# MAGIC 今回の予測演習では、Citi Bike NYCのトリップ履歴データを時間単位に集計したものと、Visual Crossingからの時間ごとの天気データを使用します。このノートブックではデータファイルの準備を行いますが、使用するデータセットに関連する利用規約があるため、データへのアクセスやダウンロード方法についての説明は行いません。これらのデータを使用したい場合は、以下のリンク先でデータ提供者のウェブサイトを訪問し、利用規約を確認の上、適切にデータを使用してください。
# MAGIC
# MAGIC * [[利用規約]](https://www.citibikenyc.com/data-sharing-policy) [Citibike NYC トリップ履歴データ](https://s3.amazonaws.com/tripdata/index.html)
# MAGIC * [[利用規約]](https://www.visualcrossing.com/weather-services-terms) [Zipコード10001のVisual Crossing時間ごとの天気データ](https://www.visualcrossing.com/weather/weather-data-services)
# MAGIC
# MAGIC Citi Bike NYCのデータファイルはZIP形式で提供されていることに注意してください。以下の手順では、これらのファイルを解凍し、重複するデータセットを削除して各トリップが一度だけ表示されるように整理したことを前提としています。歴史的なトリップデータが /mnt/citibike/trips フォルダに、Zipコード10001（マンハッタン中心部）の天気データが /mnt/weather/manhattan フォルダにダウンロードされた状態で、それぞれの内容を確認してみましょう。
# MAGIC
# MAGIC
# MAGIC <!-- %md
# MAGIC ##Data Preparation
# MAGIC
# MAGIC For our forecasting exercise, we will make use of Citi Bike NYC trip history data aggregated to an hourly level and hourly weather data from Visual Crossing.  This notebook will prepare our data files for this work, but because of the terms of use associated with the datasets we will use, we will not provide instructions on how to access and download these.  If you wish to make use of these data, please visit the data providers' websites (linked to below), review their Terms of Use (also linked to below), and employ the data appropriately:</p>
# MAGIC
# MAGIC * [[Terms of Use]](https://www.citibikenyc.com/data-sharing-policy) [Citibike NYC Trip History Data](https://s3.amazonaws.com/tripdata/index.html)
# MAGIC * [[Terms of Use]](https://www.visualcrossing.com/weather-services-terms) [Visual Crossing Hourly Weather Data for ZipCode 10001](https://www.visualcrossing.com/weather/weather-data-services)
# MAGIC
# MAGIC Please note that the Citi Bike NYC data files are provided in a ZIP format. The steps below assume you have unzipped these files and removed any files representing overlapping data sets so that each trip is represented once and only once in the folder below. With the historical trip data loaded to a folder we will identify as /mnt/citibike/trips and weather data for Zip Code 10001 (Central Manhattan) downloaded to a folder we will identify as /mnt/weather/manhattan, let's examine the contents of each:  -->

# COMMAND ----------

# カタログ、スキーマ、ボリューム名
MY_CATALOG = "komae_demo" # 使用したいカタログ名に変更してください
MY_SCHEMA = "city_bike_causal_forcasting"
MY_VOLUME_IMPORT = "raw_data"
MY_VOLUME_EXPORT = "export_data"

# モデル名
MODEL_NAME = "city_bike_causal_forcasting"

# COMMAND ----------

# カタログ、スキーマ、ボリューム作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_IMPORT};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_EXPORT};")

# USE設定
spark.sql(f"USE CATALOG {MY_CATALOG};")
spark.sql(f"USE SCHEMA {MY_SCHEMA};")

# ボリュームのサブディレクトリ作成
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/citibike/trips")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/weather/manhattan")

# # SQLで使う変数設定
# spark.conf.set("init_setting.catalog", MY_CATALOG)
# spark.conf.set("init_setting.schema", MY_SCHEMA)
# spark.conf.set("init_setting.volume", MY_VOLUME_IMPORT)
# spark.conf.set("init_setting.volume", MY_VOLUME_EXPORT)

print(f"カタログ: {MY_CATALOG}")
print(f"スキーマ: {MY_CATALOG}.{MY_SCHEMA}")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_EXPORT}")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/citibike/trips")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/weather/manhattan")
print(f"モデル名: {MODEL_NAME}")

# COMMAND ----------

# df = spark.read.format("csv") \
#           .option("header", "true") \
#           .option("inferSchema", "true") \
#           .load(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/citibike/trips")
#           # .load(f"/Volumes/komae_demo/city_bike_causal_forcasting/raw_data/citibike/trips/")

# display(df)
