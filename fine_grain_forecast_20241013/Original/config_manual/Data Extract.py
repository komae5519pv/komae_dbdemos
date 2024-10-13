# Databricks notebook source
# MAGIC %md
# MAGIC このノートブックの目的は、ソリューションアクセラレーターで使用するデータをダウンロードしてセットアップすることです。このノートブックを実行する前に、Kaggleの自分の認証情報を入力していることを確認してください。
# MAGIC
# MAGIC <!-- %md
# MAGIC The purpose of this notebook is to download and set up the data we will use for the solution accelerator. Before running this notebook, make sure you have entered your own credentials for Kaggle. -->

# COMMAND ----------

# MAGIC %run ../01_config

# COMMAND ----------

# MAGIC %pip install kaggle

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のブロックでKaggleの認証情報の設定値を入力します。ノートブックで使用する認証情報を管理するために、[シークレットスコープ](https://docs.databricks.com/security/secrets/secret-scopes.html)を設定することができます。以下のブロックでは、`solution-accelerator-cicd` シークレットスコープを手動で設定し、社内テスト用に認証情報をそこに保存しています。
# MAGIC
# MAGIC <!-- %md 
# MAGIC Set Kaggle credential configuration values in the block below: You can set up a [secret scope](https://docs.databricks.com/security/secrets/secret-scopes.html) to manage credentials used in notebooks. For the block below, we have manually set up the `solution-accelerator-cicd` secret scope and saved our credentials there for internal testing purposes. -->

# COMMAND ----------

import os
# os.environ['kaggle_username'] = 'YOUR KAGGLE USERNAME HERE'
# replace with your own credential here temporarily or set up a secret scope with your credential
os.environ['kaggle_username'] = dbutils.secrets.get("komae_scope", "kaggle_username")

# os.environ['kaggle_key'] = 'YOUR KAGGLE KEY HERE'
# replace with your own credential here temporarily or set up a secret scope with your credential
os.environ['kaggle_key'] = dbutils.secrets.get("komae_scope", "kaggle_key")

# COMMAND ----------

# MAGIC %md
# MAGIC Kaggleの認証情報を使用してデータをダウンロードします:
# MAGIC
# MAGIC <!-- %md
# MAGIC Download the data from Kaggle using the credentials set above: -->

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd /databricks/driver
# MAGIC export KAGGLE_USERNAME=$kaggle_username
# MAGIC export KAGGLE_KEY=$kaggle_key
# MAGIC kaggle competitions download -c demand-forecasting-kernels-only
# MAGIC unzip -o demand-forecasting-kernels-only.zip

# COMMAND ----------

# MAGIC %md
# MAGIC ダウンロードしたデータをアクセラレーター全体で使用するフォルダに移動します:
# MAGIC
# MAGIC <!-- %md
# MAGIC Move the downloaded data to the folder used throughout the accelerator: -->

# COMMAND ----------

dbutils.fs.mv(f"file:/databricks/driver/train.csv", "/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/train/train.csv")

# COMMAND ----------

# DBTITLE 1,Set up user-scoped database location to avoid conflicts
import re
from pathlib import Path
# Creating user-specific paths and database names
useremail = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username_sql_compatible = re.sub('\W', '_', useremail.split('@')[0])
tmp_data_path = f"/tmp/fine_grain_forecast/data/{useremail}/"
database_name = f"fine_grain_forecast_{username_sql_compatible}"

# Create user-scoped environment
spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")
spark.sql(f"CREATE DATABASE {database_name} LOCATION '{tmp_data_path}'")
spark.sql(f"USE {database_name}")
Path(tmp_data_path).mkdir(parents=True, exist_ok=True)
