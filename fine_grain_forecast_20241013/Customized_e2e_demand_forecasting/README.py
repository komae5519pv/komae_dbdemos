# Databricks notebook source
# MAGIC %md
# MAGIC # 自動販売機の需要予測・分析ダッシュボードを作成
# MAGIC ## やること
# MAGIC - 必要なジョブをワークフローで自動化します
# MAGIC - [テーブル定義書](https://docs.google.com/spreadsheets/d/10wdoTxlAGcD5gHjY4_upPYKd1gt4rEupKLSgP5q4uWI/edit?gid=1392031218#gid=1392031218)に基づくテーブルを作成してます
# MAGIC
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/workflows.png?raw=true' width='1200'/>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 概要
# MAGIC - 1. クラスタ作成
# MAGIC - 2. `00_config`を単独実行してカタログを作成しておいてください
# MAGIC - 3. パイプライン（旧 Delta Live Tables）作成
# MAGIC - 4. ワークフロー作成
# MAGIC
# MAGIC # 手順詳細
# MAGIC #### 1. クラスタの作成
# MAGIC - クラスター（汎用コンピュート）の作成
# MAGIC   - 一般
# MAGIC     - コンピュート名：`<名前>_DBR 16.2 ML`
# MAGIC   - パフォーマンス
# MAGIC     - 機械学習：`ON`
# MAGIC     - Databricks Runtime：`16.2 (includes Apache Spark 3.5.2, Scala 2.12)`
# MAGIC   - ワーカータイプ：`rd-fleet.xlarge(メモリ32GB、4 個のコア)`
# MAGIC     - 最小: `2`
# MAGIC     - 最大: `8`
# MAGIC   - Advenced
# MAGIC     - Spark構成
# MAGIC       - `spark.databricks.delta.formatCheck.enabled: false`
# MAGIC       - `spark.scheduler.mode FAIR`
# MAGIC       - `spark.sql.shuffle.partitions 300`
# MAGIC
# MAGIC #### 2. カタログ、スキーマ、ボリュームの作成
# MAGIC - ノートブック`00_config`を開いてカタログ名を自分用に変更してください（**ここで指定したカタログ名でカタログを作成することになります**）
# MAGIC - `00_config`を上から下まで実行してカタログ、スキーマ、ボリュームを作成します
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/01_config.png?raw=true' width='1200'/>
# MAGIC
# MAGIC
# MAGIC #### 3. パイプライン（旧 Delta Live Tables）作成
# MAGIC - General
# MAGIC   - パイプライン名: `demand_forecasting_dlt` 任意の名前でOKです
# MAGIC   - Product edition: `Advanced`
# MAGIC   - Product edition: `Trigggered`
# MAGIC - Source Code
# MAGIC   - Paths: <`03_ETL_DLT`のノートブックパスを選択してください>
# MAGIC - Destination
# MAGIC   - Catalog: <`00_config`で指定したカタログを選択してください>
# MAGIC   - Target schema: `demand_forecast`
# MAGIC - Advanced
# MAGIC   - Configuration
# MAGIC     - `catalog`: <`00_config`で指定したカタログを選択してください>
# MAGIC     - `schema`: `demand_forecast`
# MAGIC     - `volume`: `raw_data`
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/pipeline_setting.png?raw=true' width='1200'/>
# MAGIC
# MAGIC
# MAGIC #### 4. ワークフロー作成
# MAGIC - ワークフロー名: `komae_demand_forecasting_wf` 任意の名前でOKです
# MAGIC   - config
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`00_config`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
# MAGIC     - 依存先: ``
# MAGIC   - load_data
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`01_load_data`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
# MAGIC     - 依存先: `config`
# MAGIC   - prep_raw_csv
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`02_prep_raw_csv`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
# MAGIC     - 依存先: `load_data`
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
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/workflow_setting.png?raw=true' width='1200'/>
# MAGIC
# MAGIC #### 5. ワークフロー実行開始
# MAGIC - 3まででワークフロー作成完了です。画面右上「今すぐ実行」をクリックしてください
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/workflow_run.png?raw=true' width='1200'/>
