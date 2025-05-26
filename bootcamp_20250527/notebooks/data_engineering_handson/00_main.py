# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks データエンジニアリング ハンズオン
# MAGIC このハンズオンでは、Databricksを使用したデータエンジニアリングの基本的な流れを学習します。学習内容は以下の通りです。
# MAGIC 
# MAGIC 1. ウィジェットを使用したパラメーター設定
# MAGIC 2. データファイルをUnity Catalogボリュームにアップロード
# MAGIC 3. Bronzeテーブルの作成
# MAGIC 4. Silverテーブルの作成
# MAGIC 5. Databricksワークフローの作成と実行
# MAGIC
# MAGIC 本ノートブックを実行することで、上記1〜4までの処理が自動的に実行されます。受講者は実行結果を確認し、各ステップで何が行われたかを理解できます。
# MAGIC
# MAGIC 5のワークフローの作成は、**受講者が手動で行う**必要があります。作成手順自体を本ノートブックに記載していますので、受講者はその手順に従ってワークフローを作成します。
# MAGIC 
# MAGIC ## 所用時間
# MAGIC 約35分
# MAGIC 
# MAGIC ## 使用するデータセット
# MAGIC 本ノートブックと同じフォルダにあるIoTセンサーデータ `de_iot_data.csv` を使用します。このデータは、架空のIoTデバイスからのセンサーデータを含んでいます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ウィジェットを使用したパラメーター設定
# MAGIC 1. **以下のセルを実行**してウィジェットを作成します
# MAGIC 2. **ウィジェットに値を入力します**
# MAGIC    - **カタログ名**: `dbacademy`（固定）
# MAGIC    - **スキーマ名**: `labuser`から始まるスキーマ名を指定
# MAGIC 3. ノートブック右上にある**「すべてを実行」**をクリックします
# MAGIC 4. すべての処理が成功するまで待ちます（約3-5分）

# COMMAND ----------

# DBTITLE 1,ウィジェットの作成
dbutils.widgets.text("catalog_name", "dbacademy", "カタログ名")
dbutils.widgets.text("schema_name", "", "スキーマ名")

# COMMAND ----------

# DBTITLE 1,ウィジェットの値を取得
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"📊 カタログ名: {catalog_name}")
print(f"📊 スキーマ名: {schema_name}")

# COMMAND ----------

# DBTITLE 1,必須パラメータのチェック
# ウィジェットに値が設定されているか確認
if not catalog_name:
    raise ValueError("❌ カタログ名が指定されていません。ウィジェットでカタログ名を設定してください。")
if not schema_name:
    raise ValueError("❌ スキーマ名が指定されていません。ウィジェットでスキーマ名を設定してください。")

print(f"カタログ名: {catalog_name}")
print(f"スキーマ名: {schema_name}")

# カタログの存在確認
try:
    spark.sql(f"USE CATALOG {catalog_name}")
    print(f"✅ カタログ '{catalog_name}' の確認が完了しました")
except Exception as e:
    raise ValueError(f"❌ カタログ '{catalog_name}' が存在しません。正しいカタログ名を指定してください。")

# スキーマの存在確認
try:
    spark.sql(f"USE SCHEMA {schema_name}")
    print(f"✅ スキーマ '{schema_name}' の確認が完了しました")
except Exception as e:
    raise ValueError(f"❌ スキーマ '{schema_name}' が存在しません。正しいスキーマ名を指定してください。")

print(f"🎯 必須パラメーターのチェック完了: {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. データファイルをUnity Catalogボリュームにアップロード
# MAGIC 本ノートブックをすべて実行することで、以下の手順が自動的に実行されます。
# MAGIC
# MAGIC 1. **Unity Catalogボリュームの作成**
# MAGIC 2. **データファイルのボリュームへのアップロード**

# COMMAND ----------

# DBTITLE 1,ボリュームの作成
# ボリュームのパスを定義
volume_path = f"{catalog_name}.{schema_name}.files"

try:
   # ボリュームが既に存在するかチェック
   spark.sql(f"DESCRIBE VOLUME {volume_path}")
   print(f"ℹ️  ボリューム '{volume_path}' は既に存在します")
except:
   # ボリュームが存在しない場合は作成
   spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_path}")
   print(f"✅ ボリューム '{volume_path}' を作成しました")

# COMMAND ----------

# DBTITLE 1,データファイルをボリュームに作成
# データファイルのパスを定義
destination_path = f"/Volumes/{catalog_name}/{schema_name}/files/de_iot_data.csv"

# データファイル用のサンプルデータを定義
sample_data = """device_id,timestamp,temperature,humidity,status
DEV001,2024-01-01 10:00:00,23.5,65.2,active
DEV002,2024-01-01 10:01:00,"",70.1,inactive
DEV003,2024-01-01 10:02:00,25.8," ",active
DEV004,2024-01-01 10:03:00,N/A,68.9,error
DEV005,2024-01-01 10:04:00,24.7,N/A,active
DEV006,2024-01-01 10:05:00,26.3,71.2,
DEV007,2024-01-01 10:06:00,22.8,63.5,active
DEV008,2024-01-01 10:07:00,25.1,67.9,maintenance
DEV009,2024-01-01 10:08:00,"",64.8,active
DEV010,2024-01-01 10:09:00,24.3,N/A,inactive"""

# dbutilsを使用してボリュームにファイルを直接作成
dbutils.fs.put(destination_path, sample_data, overwrite=True)
print(f"✅ ファイルを '{destination_path}' に作成しました")

# COMMAND ----------

# DBTITLE 1,作成されたファイルの確認
try:
   df_check = spark.read.option("header", "true").csv(destination_path)
   print("✅ ファイルの作成が完了しています")
   print(f"📁 ファイルパス: {destination_path}")
   print(f"📊 レコード数: {df_check.count()}")
   print("\n📋 データプレビュー:")
   df_check.show(5, truncate=False)

   print("\n💡 GUIでの確認方法:")
   print(f"左サイドバー > Catalog > {catalog_name} > {schema_name} > ボリューム > files > de_iot_data.csv")
   
except Exception as e:
   print(f"❌ ファイルの作成に失敗しました: {e}")
   raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ノートブック実行
# MAGIC 本ノートブックをすべて実行することで、以下の処理が自動的に実行されます。

# MAGIC 1. **Bronzeテーブルの作成**（`01_bronze_layer`ノートブックを実行）
# MAGIC 2. **Silverテーブルの作成**（`02_silver_layer`ノートブックを実行）
# MAGIC 
# MAGIC 各ノートブックの実行結果は、ノートブックのワークフローの開始時刻のリンクをクリックして確認できます。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronzeテーブル作成用ノートブックの実行

# COMMAND ----------

# DBTITLE 1,Bronzeテーブル作成用ノートブックの実行
print("🚀 Bronzeテーブル作成用ノートブックを実行中...")
dbutils.notebook.run("./01_bronze", 300, {
    "catalog_name": catalog_name,
    "schema_name": schema_name
})

print("✅ Bronzeテーブルの作成が完了しました")
print("👀 ノートブックのワークフローの開始時刻のリンクをクリックし、実行された内容を確認してください")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silverテーブル作成用ノートブックの実行

# COMMAND ----------

# DBTITLE 1,Silverテーブル作成用ノートブックの実行
print("🚀 Silverテーブル作成用ノートブックを実行中...")
dbutils.notebook.run("./02_silver", 300, {
    "catalog_name": catalog_name,
    "schema_name": schema_name
})

print("✅ Silverテーブルの作成が完了しました")
print("👀 ノートブックのワークフローの開始時刻のリンクをクリックし、実行された内容を確認してください")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ワークフロー作成手順
# MAGIC 
# MAGIC ### 4.1 ワークフローの作成
# MAGIC 1. 左サイドバーの **「Workflows」** をクリック
# MAGIC 2. 右上の **「Create Job」** ボタンをクリック
# MAGIC 3. **Job名** に `de_iot_workflow` と入力
# MAGIC 4. **「Add task」** をクリック
# MAGIC 
# MAGIC ### 4.2 タスク1の設定（Bronze層）
# MAGIC 1. **Task name**: `bronze_task`
# MAGIC 2. **Type**: `Notebook`
# MAGIC 3. **Source**: `Workspace`
# MAGIC 4. **Path**: `notebooks/data_engineering_handson/01_bronze_layer`
# MAGIC 5. **「Create task」** をクリック
# MAGIC 
# MAGIC ### 4.3 タスク2の設定（Silver層）
# MAGIC 1. **「Add task」** をクリック
# MAGIC 2. **Task name**: `silver_task`
# MAGIC 3. **Type**: `Notebook`
# MAGIC 4. **Source**: `Workspace`
# MAGIC 5. **Path**: `notebooks/data_engineering_handson/02_silver_layer`
# MAGIC 6. **Depends on**: `bronze_task` を選択
# MAGIC 7. **「Create task」** をクリック
# MAGIC 
# MAGIC ### 4.4 ワークフローの実行
# MAGIC 1. 右上の **「Run now」** ボタンをクリック
# MAGIC 2. 実行状況を確認
# MAGIC 3. 各タスクをクリックして詳細ログを確認
# MAGIC 
# MAGIC ### 4.5 スケジュール設定
# MAGIC 1. **「Schedule」** タブをクリック
# MAGIC 2. **「Add schedule」** をクリック
# MAGIC 3. **Schedule type**: `Scheduled`
# MAGIC 4. **Trigger**: `Cron expression`
# MAGIC 5. **Cron expression**: `0 * * * *` (1時間毎)
# MAGIC 6. **Timezone**: `Asia/Tokyo`
# MAGIC 7. **「Create」** をクリック

# COMMAND ----------

# MAGIC %md
# MAGIC 以上でデータエンジニアリング ハンズオンは終了です。お疲れ様でした。