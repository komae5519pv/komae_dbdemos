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

# SQLで使う変数設定
spark.conf.set("init_setting.catalog", MY_CATALOG)
spark.conf.set("init_setting.schema", MY_SCHEMA)
spark.conf.set("init_setting.volume", MY_VOLUME_IMPORT)

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

# MAGIC %md
# MAGIC ## ボリューム配下に該当データを配置
# MAGIC ローデータを該当ボリュームに配置  
# MAGIC ```
# MAGIC /<catalog_name>
# MAGIC ├── automl_e2e_demo                       <- スキーマ
# MAGIC │   ├── credit_raw_data                   <- ボリューム(Import用)
# MAGIC │       ├── credit_bureau                 <- RAWファイルを配置: credit_bureau.json
# MAGIC │       ├── fund_trans                    <- RAWファイルを配置: part-00000-xxx.json, part-00001-xxx.json, part-00002-xxx.json　・・・
# MAGIC │       ├── internalbanking/accounts      <- RAWファイルを配置: accounts.csv
# MAGIC │       ├── internalbanking/customer      <- RAWファイルを配置: customer.csv
# MAGIC │       ├── internalbanking/relationship  <- RAWファイルを配置: relationship.csv
# MAGIC │       ├── telco                         <- RAWファイルを配置: telco.json
# MAGIC │   ├── export_data                       <- ボリューム(Export用)
# MAGIC ```

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

# COMMAND ----------

# 公開Azure Storage Blobから学習データを取得します (WASBプロトコル)
# sourcePath = 'wasbs://public-data@sajpstorage.blob.core.windows.net/customer.csv'
sourcePath = 'wasbs://public-data@sajpstorage.blob.core.windows.net/customer.csv'

df_customer_csv = spark.read.format('csv').option("header","true").option("inferSchema", "true").load(sourcePath)
display(df_customer_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC [デモ用データ <zip> をダウンロード](https://databricks.my.salesforce.com/sfc/dist/version/download/?oid=00D61000000JGc4&ids=068Vp000009Ot6DIAS&d=/a/Vp000000ap5d/rA84k9ApfMCrc4O2leUGOQtQUhnp9R0urZwxDaPb71o)
