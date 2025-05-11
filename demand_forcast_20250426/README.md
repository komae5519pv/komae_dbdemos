# 自動販売機の需要予測・分析ダッシュボードを作成
## やること
- 必要なジョブをワークフローで自動化します
- [テーブル定義書](https://docs.google.com/spreadsheets/d/10wdoTxlAGcD5gHjY4_upPYKd1gt4rEupKLSgP5q4uWI/edit?gid=1392031218#gid=1392031218)に基づくテーブルを作成してます

<img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/demand_forcast_20250426/_image_for_notebook/workflows.png?raw=true' width='90%'/>


# 概要
- 1. クラスタ作成
- 2. `00_config`を単独実行してカタログを作成しておいてください
- 3. パイプライン（旧 Delta Live Tables）作成
- 4. ワークフロー作成
- 5. ワークフロー実行開始
- 6. ダッシュボード作成

# 手順詳細
#### 1. クラスタの作成
- クラスター（汎用コンピュート）の作成
  - 一般
    - コンピュート名：`<名前>_DBR 16.2 ML`
  - パフォーマンス
    - 機械学習：`ON`
    - Databricks Runtime：`16.2 (includes Apache Spark 3.5.2, Scala 2.12)`
  - ワーカータイプ：`Standard_D8ds_v5(メモリ32GB、8 個のコア)`
    - 最小: `2`
    - 最大: `8`

#### 2. カタログ、スキーマ、ボリュームの作成
- ノートブック`00_config`を開いてカタログ名を自分用に変更してください（**ここで指定したカタログ名でカタログを作成することになります**）
- `00_config`を上から下まで実行してカタログ、スキーマ、ボリュームを作成します
<img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/demand_forcast_20250426/_image_for_notebook/01_config.png?raw=true' width='90%'/>


#### 3. パイプライン（旧 Delta Live Tables）作成
- General
  - パイプライン名: `demand_forecasting_dlt` 任意の名前でOKです
  - Product edition: `Advanced`
  - Product edition: `Trigggered`
- Source Code
  - Paths: <`03_ETL_DLT`のノートブックパスを選択してください>
- Destination
  - Catalog: <`00_config`で指定したカタログを選択してください>
  - Target schema: `demand_forecast`
- Advanced
  - Configuration
    - `catalog`: <`00_config`で指定したカタログを選択してください>
    - `schema`: `demand_forecast`
    - `volume`: `raw_data`
<img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/demand_forcast_20250426/_image_for_notebook/pipeline_setting.png?raw=true' width='90%'/>


#### 4. ワークフロー作成
<JSONで一括設定する場合>
- [12_yaml_for_workflow]($12_yaml_for_workflow)の通りにワークフローを作成してください

<マニュアルで設定する場合>
- ワークフロー名: `komae_demand_forecasting_wf` 任意の名前でOKです
  - config
    - 種類: `ノートブック`
    - パス: <`00_config`のノートブックパスを選択してください>
    - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
    - 依存先: ``
  - load_data
    - 種類: `ノートブック`
    - パス: <`01_load_data`のノートブックパスを選択してください>
    - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
    - 依存先: `config`
  - prep_raw_csv
    - 種類: `ノートブック`
    - パス: <`02_prep_raw_csv`のノートブックパスを選択してください>
    - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
    - 依存先: `load_data`
  - ETL
    - 種類: `パイプライン`
    - パス: <ここの手順2で作成したパイプラインを選択してください>
    - 依存先: `prep_raw_csv`
  - ETL_for_ai_query
    - 種類: `ノートブック`
    - パス: <`07_ETL_for_ai_query`のノートブックパスを選択してください>
    - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
    - 依存先: `ETL`
  - AutoML
    - 種類: `ノートブック`
    - パス: <`06_AutoML`のノートブックパスを選択してください>
    - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
    - 依存先: `ETL_for_ai_query`
  - model_serving
    - 種類: `ノートブック`
    - パス: <`09_model_serving`のノートブックパスを選択してください>
    - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
    - 依存先: `AutoML`
  - model_train_and_predict
    - 種類: `ノートブック`
    - パス: <`05_model_training`のノートブックパスを選択してください>
    - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
    - 依存先: `ETL`
  - ETL_for_dashboard
    - 種類: `ノートブック`
    - パス: <`10_ETL_for_dashboard`のノートブックパスを選択してください>
    - クラスタ: <`Runtime 15.4 ML LTS以上`のクラスタを選択してください>
    - 依存先: `model_train_and_predict`
<img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/demand_forcast_20250426/_image_for_notebook/workflow_setting.png?raw=true' width='90%'/>

#### 5. ワークフロー実行開始
- 4まででワークフロー作成完了です。画面右上「今すぐ実行」をクリックしてください -> 10~15min程度かかります
<img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/demand_forcast_20250426/_image_for_notebook/workflow_run.png?raw=true' width='90%'/>

#### 6. ダッシュボード作成
- [11_Dashboard]($11_Dashboard)に従い、ダッシュボードを完成させてください 
  - _dashboard/Vending Machine Analysis_JP
  - _dashboard/Vending Machine Analysis_EN