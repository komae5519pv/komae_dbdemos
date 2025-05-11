# やること
- 必要なジョブをワークフローで自動化します
- [テーブル定義書](https://docs.google.com/spreadsheets/d/1_h4mpPROH2VJQja3oHIIfhFWIGfDYoSoMsY31HKsOTA/edit?gid=1392031218#gid=1392031218)に基づくテーブルを作成してます

<img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/workflows.png?raw=true' width='90%'/>

# 概要
- 1. クラスタ作成
- 2. `00_config`を単独実行してカタログを作成しておいてください
- 3. パイプライン（旧 Delta Live Tables）作成
- 4. ワークフロー作成
- 5. ワークフロー実行開始
- 6. Databricks Appsデプロイ

# 手順詳細
#### 1. クラスタの作成
- クラスター（汎用コンピュート）の作成
  - 一般
    - コンピュート名：`<ご自身で作成したコンピュートの名前>`　DBR 16.0ML以降 推奨です
  - パフォーマンス
    - 機械学習：`ON`
    - Databricks Runtime：`16.0 (includes Apache Spark 3.5.2, Scala 2.12)`
  - ワーカータイプ：`Standard_DS15_v2 (メモリ140GB、20 個のコア)`
    - 最小: `2`
    - 最大: `10`
  - Advenced
    - Spark構成
      - `spark.databricks.pyspark.dataFrameChunk.enabled true`
      - `spark.scheduler.mode FAIR`
      - `spark.sql.shuffle.partitions 300`

#### 2. カタログ、スキーマ、ボリュームの作成
- ノートブック`00_config`を開いてカタログ名を自分用に変更してください（**ここで指定したカタログ名でカタログを作成することになります**）
- `00_config`を上から下まで実行してカタログ、スキーマ、ボリュームを作成します
<img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/00_config.png?raw=true' width='90%'/>


#### 3. パイプライン（旧 Delta Live Tables）作成
- パイプライン -> 作成 -> ETLパイプライン
<img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/DLT_1.png?raw=true' width='90%'/>

- General
  - パイプライン名: `komae_airline_recomments_dlt` 任意の名前でOKです
  - Product edition: `Advanced`
  - Product edition: `Trigggered`
- Source Code
  - Paths: <`03_ETL_DLT`のノートブックパスを選択してください>
- Destination
  - Catalog: <`00_config`で指定したカタログを選択してください>
  - Target schema: `airline_recommends`
- Advanced
  - Configuration
    - `catalog`: <`00_config`で指定したカタログを選択してください>
    - `schema`: `airline_recommends`
    - `volume`: `raw_data`
<img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/DTL_2.png?raw=true' width='90%'/>


#### 4. ワークフロー作成
<JSONで一括設定する場合>
- [09_yaml_for_workflow]($09_yaml_for_workflow)の通りにワークフローを作成してください

<マニュアルで設定する場合>
- ワークフロー名: `komae_airline_recomments_dlt` 任意の名前でOKです
  - load_data
    - 種類: `ノートブック`
    - パス: <`01_load_data`のノートブックパスを選択してください>
    - クラスタ: <`Runtime 16.0 ML 以上`のクラスタを選択してください>
    - 依存先: ``
  - set_csv
    - 種類: `ノートブック`
    - パス: <`02_set_csv`のノートブックパスを選択してください>
    - クラスタ: <`Runtime 16.0 ML 以上`のクラスタを選択してください>
    - 依存先: `load_data`
  - ETL
    - 種類: `パイプライン`
    - パス: <ここの手順2で作成したパイプラインを選択してください>
    - 依存先: `set_csv`
  - train_model
    - 種類: `ノートブック`
    - パス: <`04_train_model`のノートブックパスを選択してください>
    - クラスタ: <`Runtime 16.0 ML 以上`のクラスタを選択してください>
    - 依存先: `ETL`
  - get_recommends
    - 種類: `ノートブック`
    - パス: <`05_get_recommends`のノートブックパスを選択してください>
    - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
    - 依存先: `train_model`
  - feature_serving
    - 種類: `ノートブック`
    - パス: <`07_feature_serving`のノートブックパスを選択してください>
    - クラスタ: <`Runtime 16.0 ML 以上`のクラスタを選択してください>
    - 依存先: `get_recommends`

<img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/workflow_settings.png?raw=true' width='90%'/>


#### 5. ワークフロー実行開始
- 4まででワークフロー作成完了です。画面右上「今すぐ実行」をクリックしてください
<img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/workflow_run.png?raw=true' width='90%'/>

#### 6. Databricks Appsデプロイ
- 設定ファイルを編集
  - ディレクトリ　_apps -> app.yaml　を編集してください

<img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/app.yaml.png?raw=true' width='90%'/>

- クラスター -> アプリ -> 右上「アプリを作成」
- 全体的な流れ：Customでインスタンス作成後、`_apps`フォルダを指定してアプリをデプロイ
  - Custom
  - アプリ名：`<ご自身の名前など>-airline-recommends`
  - 説明：`航空会社の機内レコメンドアプリ`　ご自身にとってわかりやすい説明でOKです
  - Advanced setting：設定なし

<img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_manual/databricks_apps.png?raw=true' width='80%'/>