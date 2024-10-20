# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Databricks AutoMLの実行
# MAGIC クラスタ：Runtime 15.4 LTS ML以上

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Credit Scoring default prediction(債務不履行予測)
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi-credit-decisioning-ml-2.png" style="float: right" width="800px">
# MAGIC
# MAGIC ### AutoMLを使ったワンクリック展開(デプロイ)
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
# クレジットカードの不履行を、60日以上の遅延として定義してラベルを付ける
credit_bureau_label = (spark.table("credit_bureau_gold")
                            .withColumn("defaulted", F.when(col("CREDIT_DAY_OVERDUE") > 60, 1)
                                                      .otherwise(0))
                            .select("cust_id", "defaulted"))

# 不履行と非デフォルトの顧客の比率を視覚的に確認し、データの不均衡を評価
df = credit_bureau_label.groupBy('defaulted').count().toPandas()
px.pie(df, values='count', names='defaulted', title='Credit default ratio')

# COMMAND ----------

# DBTITLE 1,トレーニングデータセットを作成（特徴量とラベルを結合）
# 特徴量セットと信用ラベルを結合してトレーニングデータセットを作成
training_dataset = credit_bureau_label.join(features_set, "cust_id", "inner")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### データセットのバランス調整
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
# MAGIC ### AutoMLを使用したクレジットスコアリングモデルの作成
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

# import json
# import os

# # カレントディレクトリを取得
# notebook_path = json.loads(
#     dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
# )["extraContext"]["notebook_path"]
# current_directory = os.path.dirname(notebook_path)

# print(current_directory)

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
    experiment_name=xp_name,            # 実験名
    experiment_dir=xp_path,             # 実験ディレクトリ
    dataset=train_df.sample(0.1),       # データセット（10%サンプリング）
    target_col="defaulted",             # 目的変数
    timeout_minutes=10                  # タイムアウト時間（分）
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
# MAGIC # 2. モデルのModel Registry、及びUnity Catalogへ本番環境として登録
# MAGIC
# MAGIC モデルの準備が整いました。AutoMLの実行で生成されたノートブックを確認し、必要に応じてカスタマイズすることができます。
# MAGIC
# MAGIC このデモでは、モデルが準備できていると仮定し、Model Registry及びUnity Catalogに本番環境としてデプロイします。  
# MAGIC モデルに`@prod`というエイリアスをつけることで本番環境と区別します。

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
