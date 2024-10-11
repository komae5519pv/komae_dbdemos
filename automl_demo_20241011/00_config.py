# Databricks notebook source
# カタログ、スキーマ、ボリューム名
MY_CATALOG = "komae_demo" # 使用したいカタログ名に変更してください
MY_SCHEMA = "automl_e2e_demo"
MY_VOLUME_IMPORT = "credit_raw_data"
MY_VOLUME_EXPORT = "export_data"

# モデル名
MODEL_NAME = "automl_e2e_demo_fsi_credit_decisioning"

# COMMAND ----------

# DBTITLE 1,カタログ設定
# カタログ、スキーマ、ボリューム作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_IMPORT};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME_EXPORT};")

# USE設定
spark.sql(f"USE CATALOG {MY_CATALOG};")
spark.sql(f"USE SCHEMA {MY_SCHEMA};")

# ボリュームのサブディレクトリ作成
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/credit_bureau")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/fund_trans")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/internalbanking/account")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/internalbanking/customer")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/internalbanking/relationship")
dbutils.fs.mkdirs(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/telco")

# # SQLで使う変数設定
# spark.conf.set("init_setting.catalog", MY_CATALOG)
# spark.conf.set("init_setting.schema", MY_SCHEMA)
# spark.conf.set("init_setting.volume", MY_VOLUME_IMPORT)

print(f"カタログ: {MY_CATALOG}")
print(f"スキーマ: {MY_CATALOG}.{MY_SCHEMA}")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/credit_bureau")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/fund_trans")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/internalbanking/account")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/internalbanking/customer")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/internalbanking/relationship")
print(f"取込ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/telco")
print(f"出力ボリューム: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_EXPORT}")
print(f"モデル名: {MODEL_NAME}")

# COMMAND ----------

# RAWデータのチェック
# files = dbutils.fs.ls(f"/Volumes/{spark.conf.get('init_setting.catalog')}/{spark.conf.get('init_setting.schema')}/{spark.conf.get('init_setting.volume')}/credit_bureau")
# display(files)

# COMMAND ----------

# RAWデータのチェック
# files = dbutils.fs.ls(f"/Volumes/{spark.conf.get('init_setting.catalog')}/{spark.conf.get('init_setting.schema')}/{spark.conf.get('init_setting.volume')}/fund_trans")
# display(files)

# COMMAND ----------

# RAWデータのチェック
# files = dbutils.fs.ls(f"/Volumes/{spark.conf.get('init_setting.catalog')}/{spark.conf.get('init_setting.schema')}/{spark.conf.get('init_setting.volume')}/internalbanking")
# display(files)

# COMMAND ----------

# RAWデータのチェック
# files = dbutils.fs.ls(f"/Volumes/{spark.conf.get('init_setting.catalog')}/{spark.conf.get('init_setting.schema')}/{spark.conf.get('init_setting.volume')}/telco")
# display(files)
