# Databricks notebook source
# MAGIC %md
# MAGIC # やること
# MAGIC - 必要なジョブをワークフローで自動化します
# MAGIC - [テーブル定義書](https://docs.google.com/spreadsheets/d/1_h4mpPROH2VJQja3oHIIfhFWIGfDYoSoMsY31HKsOTA/edit?gid=1392031218#gid=1392031218)に基づくテーブルを作成してます
# MAGIC
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/workflows.png?raw=true' width='90%'/>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 概要
# MAGIC - 1. クラスタ作成
# MAGIC - 2. `00_config`を単独実行してカタログを作成しておいてください
# MAGIC - 3. パイプライン（旧 Delta Live Tables）作成
# MAGIC - 4. ワークフロー作成
# MAGIC - 5. ワークフロー実行開始
# MAGIC - 6. Databricks Appsデプロイ
# MAGIC
# MAGIC # 手順詳細
# MAGIC #### 1. クラスタの作成
# MAGIC - クラスター（汎用コンピュート）の作成
# MAGIC   - 一般
# MAGIC     - コンピュート名：`<ご自身で作成したコンピュートの名前>`　DBR 16.0ML以降 推奨です
# MAGIC   - パフォーマンス
# MAGIC     - 機械学習：`ON`
# MAGIC     - Databricks Runtime：`16.0 (includes Apache Spark 3.5.2, Scala 2.12)`
# MAGIC   - ワーカータイプ
# MAGIC     - on Azure：`Standard_D4ds_v5 (メモリ16GB、4 個のコア)`
# MAGIC       - 最小: `2`
# MAGIC       - 最大: `10`
# MAGIC     - on AWS：`rd-fleet.xlarge (メモリ32GB、4 個のコア)`
# MAGIC       - 最小: `2`
# MAGIC       - 最大: `10`
# MAGIC
# MAGIC #### 2. カタログ、スキーマ、ボリュームの作成
# MAGIC - ノートブック`00_config`を開いてカタログ名を自分用に変更してください（**ここで指定したカタログ名でカタログを作成することになります**）
# MAGIC - `00_config`を上から下まで実行してカタログ、スキーマ、ボリュームを作成します
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/00_config.png?raw=true' width='90%'/>
# MAGIC
# MAGIC
# MAGIC #### 3. パイプライン（旧 Delta Live Tables）作成
# MAGIC - パイプライン -> 作成 -> ETLパイプライン
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/DLT_1.png?raw=true' width='90%'/>
# MAGIC
# MAGIC - General
# MAGIC   - パイプライン名: `komae_airline_recomments_dlt` 任意の名前でOKです
# MAGIC   - Product edition: `Advanced`
# MAGIC   - Product edition: `Trigggered`
# MAGIC - Source Code
# MAGIC   - Paths: <`03_ETL_DLT`のノートブックパスを選択してください>
# MAGIC - Destination
# MAGIC   - Catalog: <`00_config`で指定したカタログを選択してください>
# MAGIC   - Target schema: `airline_recommends`
# MAGIC - Advanced
# MAGIC   - Configuration
# MAGIC     - `catalog`: <`00_config`で指定したカタログを選択してください>
# MAGIC     - `schema`: `airline_recommends`
# MAGIC     - `volume`: `raw_data`
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/DTL_2.png?raw=true' width='90%'/>
# MAGIC
# MAGIC
# MAGIC #### 4. ワークフロー作成
# MAGIC <JSONで一括設定する場合>
# MAGIC - [09_yaml_for_workflow]($09_yaml_for_workflow)の通りにワークフローを作成してください
# MAGIC
# MAGIC <マニュアルで設定する場合>
# MAGIC - ワークフロー名: `komae_airline_recomments_dlt` 任意の名前でOKです
# MAGIC   - load_data
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`01_load_data`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 16.0 ML 以上`のクラスタを選択してください>
# MAGIC     - 依存先: ``
# MAGIC   - set_csv
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`02_set_csv`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 16.0 ML 以上`のクラスタを選択してください>
# MAGIC     - 依存先: `load_data`
# MAGIC   - ETL
# MAGIC     - 種類: `パイプライン`
# MAGIC     - パス: <ここの手順2で作成したパイプラインを選択してください>
# MAGIC     - 依存先: `set_csv`
# MAGIC   - train_model
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`04_train_model`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 16.0 ML 以上`のクラスタを選択してください>
# MAGIC     - 依存先: `ETL`
# MAGIC   - get_recommends
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`05_get_recommends`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
# MAGIC     - 依存先: `train_model`
# MAGIC   - feature_serving
# MAGIC     - 種類: `ノートブック`
# MAGIC     - パス: <`07_feature_serving`のノートブックパスを選択してください>
# MAGIC     - クラスタ: <`Runtime 16.0 ML 以上`のクラスタを選択してください>
# MAGIC     - 依存先: `get_recommends`
# MAGIC
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/workflow_settings.png?raw=true' width='90%'/>
# MAGIC
# MAGIC
# MAGIC #### 5. ワークフロー実行開始
# MAGIC - 4まででワークフロー作成完了です。画面右上「今すぐ実行」をクリックしてください
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/workflow_run.png?raw=true' width='90%'/>
# MAGIC
# MAGIC #### 6. Databricks Appsデプロイ
# MAGIC - 設定ファイルを編集
# MAGIC   - ディレクトリ　_apps -> app.yaml　を編集してください
# MAGIC
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/app.yaml.png?raw=true' width='90%'/>
# MAGIC
# MAGIC - クラスター -> アプリ -> 右上「アプリを作成」
# MAGIC - 全体的な流れ：Customでインスタンス作成後、`_apps`フォルダを指定してアプリをデプロイ
# MAGIC   - Custom
# MAGIC   - アプリ名：`<ご自身の名前など>-airline-recommends`
# MAGIC   - 説明：`航空会社の機内レコメンドアプリ`　ご自身にとってわかりやすい説明でOKです
# MAGIC   - Advanced setting：設定なし
# MAGIC
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/databricks_apps.png?raw=true' width='80%'/>
