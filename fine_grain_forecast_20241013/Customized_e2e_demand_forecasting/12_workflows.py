# Databricks notebook source
# MAGIC %md
# MAGIC # 自動販売機の需要予測・分析ダッシュボードを作成
# MAGIC ## やること
# MAGIC - 必要なジョブをワークフローで自動化します
# MAGIC - [テーブル定義書](https://docs.google.com/spreadsheets/d/10wdoTxlAGcD5gHjY4_upPYKd1gt4rEupKLSgP5q4uWI/edit?gid=1392031218#gid=1392031218)に基づくテーブルを作成してます
# MAGIC
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/fine_grain_forecast/workflows.png' width='1200'/>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 概要
# MAGIC - 1. 最初に、`01_config`を単独実行してカタログを作成しておいてください
# MAGIC - 2. パイプライン（旧 Delta Live Tables）作成
# MAGIC - 3. ワークフロー作成
# MAGIC
# MAGIC # 手順詳細
# MAGIC #### 1. カタログ、スキーマ、ボリュームの作成
# MAGIC - ノートブック`01_config`を開いてカタログ名を自分用に変更してください（**ここで指定したカタログ名でカタログを作成することになります**）
# MAGIC - `01_config`を上から下まで実行してカタログ、スキーマ、ボリュームを作成します
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/fine_grain_forecast/01_config.png' width='1200'/>
# MAGIC
# MAGIC
# MAGIC #### 2. パイプライン（旧 Delta Live Tables）作成
# MAGIC - General
# MAGIC   - パイプライン名: `demand_forecasting_dlt` 任意の名前でOKです
# MAGIC   - Product edition: `Advanced`
# MAGIC   - Product edition: `Trigggered`
# MAGIC - Source Code
# MAGIC   - Paths: <`03_ETL_DLT`のノートブックパスを選択してください>
# MAGIC - Destination
# MAGIC   - Catalog: <`01_config`で指定したカタログを選択してください>
# MAGIC   - Target schema: `demand_forecast`
# MAGIC - Advanced
# MAGIC   - Configuration
# MAGIC     - `catalog`: <`01_config`で指定したカタログを選択してください>
# MAGIC     - `schema`: `demand_forecast`
# MAGIC     - `volume`: `raw_data`
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/fine_grain_forecast/pipeline_setting.png' width='1200'/>
# MAGIC
# MAGIC
# MAGIC #### 3. ワークフロー作成
# MAGIC - ワークフロー名: `komae_demand_forecasting_wf` 任意の名前でOKです
# MAGIC   - config
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`01_config`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
# MAGIC     - 依存先: ``
# MAGIC   - prep_raw_csv
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`02_prep_raw_csv`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
# MAGIC     - 依存先: `config`
# MAGIC   - ETL
# MAGIC     - 種類: `パイプライン`
# MAGIC     - パス: <ここの手順2で作成したパイプラインを選択してください>
# MAGIC     - 依存先: `prep_raw_csv`
# MAGIC   - ETL_for_ai_query
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`07_ETL_for_ai_query`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
# MAGIC     - 依存先: `ETL`
# MAGIC   - AutoML
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`06_AutoML`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
# MAGIC     - 依存先: `ETL_for_ai_query`
# MAGIC   - batch_scoring
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`08_batch_scoring`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
# MAGIC     - 依存先: `AutoML`
# MAGIC   - model_serving
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`09_model_serving`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
# MAGIC     - 依存先: `AutoML`
# MAGIC   - model_train_and_predict
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`05_model_training`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
# MAGIC     - 依存先: `ETL`
# MAGIC   - ETL_for_dashboard
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`10_ETL_for_dashboard`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
# MAGIC     - 依存先: `model_train_and_predict`
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/fine_grain_forecast/workflow_setting.png' width='1200'/>
# MAGIC
# MAGIC #### 4. ワークフロー実行開始
# MAGIC - 3まででワークフロー作成完了です。画面右上「今すぐ実行」をクリックしてください
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/fine_grain_forecast/workflow_run.png' width='1200'/>
