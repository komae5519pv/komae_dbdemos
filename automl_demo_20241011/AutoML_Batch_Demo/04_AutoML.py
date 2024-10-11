# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Data Science on the Databricks Lakehouse
# MAGIC
# MAGIC ## 機械学習（ML）は革新とパーソナライゼーションの鍵
# MAGIC
# MAGIC クレジット関連のデータベースを取り込み、クエリを実行できることは最初のステップですが、競争の激しい市場で成功するにはそれだけでは不十分です。
# MAGIC
# MAGIC 顧客は今、リアルタイムでのパーソナライゼーションや新しい形式のコミュニケーションを期待しています。現代のデータ企業はこれをAIで実現しています。
# MAGIC
# MAGIC <style>
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:650px;height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 35px; float: left; padding: 20px; color: #f9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
# MAGIC .badge_b { 
# MAGIC   height: 35px}
# MAGIC </style>
# MAGIC <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
# MAGIC <div style="font-family: 'DM Sans'">
# MAGIC   <div style="width: 500px; color: #1b3139; margin-left: 50px; float: left">
# MAGIC     <div style="color: #ff5f46; font-size:80px">90%</div>
# MAGIC     <div style="font-size:30px;  margin-top: -20px; line-height: 30px;">
# MAGIC       Enterprise applications will be AI-augmented by 2025 — IDC
# MAGIC     </div>
# MAGIC     <div style="color: #ff5f46; font-size:80px">$10T+</div>
# MAGIC     <div style="font-size:30px;  margin-top: -20px; line-height: 30px;">
# MAGIC        Projected business value creation by AI in 2030 — PwC
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC
# MAGIC   <div class="right_box">
# MAGIC       But a huge challenge is getting ML to work at scale!<br/><br/>
# MAGIC       Most ML projects still fail before getting to production.
# MAGIC   </div>
# MAGIC   
# MAGIC <br style="clear: both">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&org_id=984752964297111&notebook=%2F03-Data-Science-ML%2F03.2-AutoML-credit-decisioning&demo_name=lakehouse-fsi-credit&event=VIEW&path=%2F_dbdemos%2Flakehouse%2Flakehouse-fsi-credit%2F03-Data-Science-ML%2F03.2-AutoML-credit-decisioning&version=1">

# COMMAND ----------

# MAGIC %md 
# MAGIC ### A cluster has been created for this demo
# MAGIC To run this demo, just select the cluster `dbdemos-lakehouse-fsi-credit-konomi_omae` from the dropdown menu ([open cluster configuration](https://adb-984752964297111.11.azuredatabricks.net/#setting/clusters/0914-122336-4u3552ik/configuration)). <br />
# MAGIC *Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('lakehouse-fsi-credit')` or re-install the demo: `dbdemos.install('lakehouse-fsi-credit')`*

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## では、なぜ機械学習とデータサイエンスが難しいのか？
# MAGIC
# MAGIC 企業が直面する主な課題は以下の通りです：
# MAGIC 1. 必要なデータをタイムリーに取り込めない
# MAGIC 2. データのアクセスを適切に管理できない
# MAGIC 3. フィーチャーストアの問題を生データまで追跡できない
# MAGIC
# MAGIC ...そして他にも多くのデータ関連の問題があります。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # データ中心の機械学習
# MAGIC
# MAGIC Databricksでは、機械学習はデータに「接続」する必要がある別の製品やサービスではありません。Lakehouseは単一で統合された製品であり、Databricksにおける機械学習はデータの上に「存在」しているため、データの発見やアクセスができないといった課題はなくなります。
# MAGIC
# MAGIC <br />
# MAGIC <img src="https://raw.githubusercontent.com/borisbanushev/CAPM_Databricks/main/MLontheLakehouse.png" width="1300px" />

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Credit Scoring default prediction(債務不履行予測)
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi-credit-decisioning-ml-2.png" style="float: right" width="800px">
# MAGIC
# MAGIC ## AutoMLを使ったワンクリック展開(デプロイ)
# MAGIC
# MAGIC クレジット判断データを活用して、顧客の信用度を予測し、説明するモデルをどのように構築できるか見てみましょう。
# MAGIC
# MAGIC まず、特徴量ストアからデータを取得し、トレーニングデータセットを作成します。
# MAGIC
# MAGIC その後、Databricks AutoMLを使って、自動的にモデルを構築します。

# COMMAND ----------

# DBTITLE 1,Feature Storeからトレーニングデータセットを読み込む
# Feature Storeからトレーニングデータセットをロード
fs = feature_store.FeatureStoreClient()
features_set = fs.read_table(name=f"{MY_CATALOG}.{MY_SCHEMA}.credit_decisioning_features")
display(features_set)

# COMMAND ----------

# DBTITLE 1,ラベル"defaulted"を作成
credit_bureau_label = (spark.table("credit_bureau_gold")
                            .withColumn("defaulted", F.when(col("CREDIT_DAY_OVERDUE") > 60, 1)
                                                      .otherwise(0))
                            .select("cust_id", "defaulted"))
# データセットの不均衡を確認
df = credit_bureau_label.groupBy('defaulted').count().toPandas()
px.pie(df, values='count', names='defaulted', title='Credit default ratio')

# COMMAND ----------

# DBTITLE 1,トレーニングデータセットを作成（特徴量とラベルを結合）
# 特徴量セットと信用ラベルを結合してトレーニングデータセットを作成
training_dataset = credit_bureau_label.join(features_set, "cust_id", "inner")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## データセットのバランス調整
# MAGIC
# MAGIC モデルのパフォーマンスを向上させるために、データセットをダウンサンプリングおよびアップサンプリングしましょう。

# COMMAND ----------

major_df = training_dataset.filter(col("defaulted") == 0)
minor_df = training_dataset.filter(col("defaulted") == 1)

'''メジャークラス:マイナークラスが3:1になるように調整'''

# マイナークラスの行を複製する
oversampled_df = minor_df.union(minor_df)

# メジャークラスの行をダウンサンプリングする（マイナークラスの割合の3倍の割合をメジャークラスとして抽出）
undersampled_df = major_df.sample(oversampled_df.count()/major_df.count()*3, 42)

# オーバーサンプリングされたマイナークラスの行とアンダーサンプリングされたメジャークラスの行を結合する。これにより、バランスが改善され、十分な情報が保持されます。
train_df = undersampled_df.unionAll(oversampled_df).drop('cust_id').na.fill(0)

# AutoML UIで選択できるように、テーブルとして保存します。
train_df.write.mode('overwrite').saveAsTable('credit_risk_train_df')
px.pie(train_df.groupBy('defaulted').count().toPandas(), values='count', names='defaulted', title='クレジットデフォルト比率')

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## MLFlowとDatabricks AutoMLを使用したクレジットスコアリングモデルの作成の加速
# MAGIC
# MAGIC MLFlowは、モデルのトラッキング、パッケージング、デプロイを可能にするオープンソースプロジェクトです。データサイエンスチームがモデルを作成するたびに、Databricksは使用されたすべてのパラメータとデータを追跡し、自動的にログを記録します。これにより、機械学習モデルの追跡可能性と再現性が保証され、各モデルやバージョンがどのパラメータやデータを使用して構築されたのかを簡単に把握できます。
# MAGIC
# MAGIC ### データチームに力を与えつつ、制御を奪わない透明性のあるソリューション
# MAGIC
# MAGIC DatabricksはMLFlowを使用してモデルのデプロイとガバナンス（MLOps）を簡素化しますが、新しいMLプロジェクトの立ち上げは依然として長く非効率的なプロセスになることがあります。
# MAGIC
# MAGIC 新しいプロジェクトごとに同じ定型コードを作成する代わりに、Databricks AutoMLは、分類、回帰、予測のために最先端のモデルを自動的に生成することができます。
# MAGIC
# MAGIC
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC モデルは一から構築してデプロイすることもできますし、AutoMLが自動的に作成したノートブックを活用することで、機械学習プロジェクトを素早く効率的に開始できます。これにより、数週間分の労力を節約できます。
# MAGIC
# MAGIC <br style="clear: both">
# MAGIC
# MAGIC <img style="float: right" width="600" src="https://raw.githubusercontent.com/borisbanushev/CAPM_Databricks/main/MLFlowAutoML.png"/>
# MAGIC
# MAGIC ### Databricks Auto MLを使用してクレジットスコアリングデータセットを活用
# MAGIC
# MAGIC AutoMLは「Machine Learning」スペースで利用可能です。新しいAutoMLエクスペリメントを開始し、作成した特徴量テーブル（`creditdecisioning_features`）を選択するだけで済みます。
# MAGIC
# MAGIC 予測ターゲットは`defaulted`列です。
# MAGIC
# MAGIC 「開始」をクリックすると、残りはDatabricksが処理します。
# MAGIC
# MAGIC これはUIを使って行うことができますが、[python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)を利用することも可能です。

# COMMAND ----------

# DBTITLE 1,カレントディレクトリを取得
import json
import os

# カレントディレクトリを取得
notebook_path = json.loads(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
)["extraContext"]["notebook_path"]
current_directory = os.path.dirname(notebook_path)

print(current_directory)

# COMMAND ----------

# DBTITLE 1,AutoMLの実行
from databricks import automl
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

# エクスペリメントのパスと名前を設定
xp_path = "/Shared/automl_e2e_demo/experiments"
xp_name = f"automl_credit_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"

# AutoMLを使用して分類モデルをトレーニング
automl_run = automl.classify(
    experiment_name=xp_name,  # 実験名
    experiment_dir=xp_path,  # 実験ディレクトリ
    dataset=train_df.sample(0.1),  # データセット（10%サンプリング）
    target_col="defaulted",  # 目的変数
    timeout_minutes=10  # タイムアウト時間（分）
)

# 全ユーザー(グループ名: account users)にエクスペリメントの権限を設定
w = WorkspaceClient()
try:
    status = w.workspace.get_status(f"{xp_path}/{xp_name}")
    w.permissions.set("experiments", request_object_id=status.object_id, access_control_list=[
        iam.AccessControlRequest(group_name="account users", permission_level=iam.PermissionLevel.CAN_MANAGE)
    ])
    print(f"Experiment on {xp_path}/{xp_name} was set public")
except Exception as e:
    print(f"Error setting up shared experiment {xp_path}/{xp_name} permission: {e}")
# # 全ユーザーがdbdemos共有実験にアクセスできるようにする
# DBDemos.set_experiment_permission(f"{xp_path}/{xp_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## モデルの本番環境へのデプロイ
# MAGIC
# MAGIC モデルの準備が整いました。AutoMLの実行で生成されたノートブックを確認し、必要に応じてカスタマイズすることができます。
# MAGIC
# MAGIC このデモでは、モデルが準備できていると仮定し、Model Registryに本番環境としてデプロイします。

# COMMAND ----------

from mlflow import MlflowClient
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

# Databricks Unity Catalogを使用してモデルを保存します
mlflow.set_registry_uri('databricks-uc')
client = MlflowClient()

# カタログにモデルを追加
latest_model = mlflow.register_model(f'runs:/{automl_run.best_trial.mlflow_run_id}/model', f"{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}")

# UCエイリアスを使用してプロダクション対応としてフラグを立てる
client.set_registered_model_alias(name=f"{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}", alias="prod", version=latest_model.version)

# WorkspaceClientのインスタンスを作成
sdk_client = WorkspaceClient()

# 全ユーザー(グループ名: account users)にモデルの権限を設定
sdk_client.grants.update(c.SecurableType.FUNCTION, f"{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}", 
                         changes=[c.PermissionsChange(add=[c.Privilege["ALL_PRIVILEGES"]], principal="account users")])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 私たちはAutoMLモデルを本番準備完了として移行しました！
# MAGIC
# MAGIC > [dbdemos_fsi_credit_decisioningモデル](#mlflow/models/dbdemos_fsi_credit_decisioning)を開いて、そのアーティファクトを確認し、使用されたパラメータを分析しましょう。モデルの作成に使用されたノートブックへの追跡可能性も含まれています。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 債務不履行リスクを予測するモデルが本番環境にデプロイされました
# MAGIC
# MAGIC ここまでで以下のことを行いました：
# MAGIC * 必要なすべてのデータを単一の信頼できるソースに取り込み、
# MAGIC * すべてのデータを適切に保護（細かなアクセス制御の付与、PIIデータのマスキング、列レベルのフィルタリングの適用など）し、
# MAGIC * 特徴量エンジニアリングによってデータを強化し、
# MAGIC * MLFlow AutoMLを使用して実験を追跡し、機械学習モデルを構築し、
# MAGIC * モデルを登録しました。
# MAGIC
# MAGIC ### 次のステップ
# MAGIC モデルを以下に使用する準備が整いました：
# MAGIC
# MAGIC - ノートブック [03.3-Batch-Scoring-credit-decisioning]($./03.3-Batch-Scoring-credit-decisioning) を使用してバッチ推論を行い、現在銀行サービスを十分に利用していないが信用度の高い顧客を特定して収益を増加させるために利用する（**収益を増やす**）、また現在のクレジット所有者の中で債務不履行の可能性がある顧客を予測し、そのような債務不履行を防ぐために利用する（**リスクを管理する**）、
# MAGIC - リアルタイム推論を [03.4-model-serving-BNPL-credit-decisioning]($./03.4-model-serving-BNPL-credit-decisioning) で行い、銀行内で```今すぐ購入、後で支払い```機能を実現する。
# MAGIC
# MAGIC 追加事項：モデルの説明可能性と公平性を [03.5-Explainability-and-Fairness-credit-decisioning]($./03.5-Explainability-and-Fairness-credit-decisioning) で確認。
