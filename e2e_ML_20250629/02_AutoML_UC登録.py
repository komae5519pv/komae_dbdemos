# Databricks notebook source
# MAGIC %md # 02 AutoMLによるモデル作成・UCへのモデル登録
# MAGIC
# MAGIC このノートブックでは、01で作った特徴量を使ってベストなモデルを作成し、Unity Catalogにベストモデルを登録します
# MAGIC
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/2_automl.png?raw=true' width='1200' />

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. トレーニングデータのロード
# MAGIC

# COMMAND ----------

df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.churn_features")
df = df.drop('customerID')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. AutoMLトレーニング

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2-1. AutoMLエクスペリメントの実行
# MAGIC AutoMLでモデルをトレーニングし、最良モデルを特定します。<br>
# MAGIC
# MAGIC 先ほど作成した特徴量テーブル(`churn_features`)を選択するだけでOKです。<br>
# MAGIC - ML問題タイプは、今回は分類です。<br>
# MAGIC - 予測ターゲットは`churn`カラムです。<br>
# MAGIC - 評価メトリクスは`F1スコア`を使います。<br>
# MAGIC - Demo時には時間短縮のため、5分にセットします。<br>
# MAGIC
# MAGIC この作業はUIでも行えますが、ここでは[python API](https://docs.databricks.com/aws/ja/machine-learning/automl#automl-python-api-1)で操作します。

# COMMAND ----------

from databricks import automl

# AutoMLエクスペリメントの実行
summary = automl.classify(
    dataset=df,
    target_col="churn",
    primary_metric="f1",  # f1
    timeout_minutes=5     # 5分
)

# 最良モデルのrun_idを取得
best_run_id = summary.best_trial.mlflow_run_id


# COMMAND ----------

print(best_run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 暫定: Numpyのパッチ適用
# MAGIC
# MAGIC 下記セルは本来不要な処理ですが、AutoML処理の一時的な処理による後続のモデルサービングデプロイエラーを回避するためのパッチ処理です。<br>
# MAGIC AutoML実行時、Mlflowログプロセスで依存関係が適切にログされておらず、結果としてModel Serving Endpointデプロイが失敗しています。<br>
# MAGIC 次のセルはこの問題を回避するためのパッチ処理です。<br>
# MAGIC
# MAGIC 詳細：AutoMLがMlflowの依存関係、特にNumpyのログ出力を行っておらず、結果としてModel Serving Endpoint内で最新バージョンのNumpyが使用されてしまい、非互換性からデプロイErrorが発生しています。そのため、conda.yaml および requirement.txt にNumpyのバージョンを追記しています。<br>
# MAGIC
# MAGIC モデルサービング時に、トレーニング時と同じバージョンのpandasが使用されるようにします。<br>
# MAGIC [Serving an AutoML model failing when deployed to an endpoint with "Failed to deploy modelName: served entity creation aborted" error](https://kb.databricks.com/en_US/machine-learning/serving-an-automl-model-failing-when-deployed-to-an-endpoint-with-failed-to-deploy-modelname-served-entity-creation-aborted-error)<br>

# COMMAND ----------

import mlflow
import os
import shutil
import tempfile
import yaml
import numpy as np  # numpyのバージョンを取得するためにインポート
from mlflow.tracking import MlflowClient

client = mlflow.tracking.MlflowClient()

# アーティファクトを操作するための一時ディレクトリを作成
tmp_dir = tempfile.mkdtemp()

'''
これは、numpyバージョンを追加する予定の実験のmlflowランIDです

例: 
dbfs:/databricks/mlflow-tracking/4400526906967208/aaf486672a1b45458c9e269747c94931/artifacts/model
run_id = "aaf486672a1b45458c9e269747c94931"
'''

run_id = best_run_id # Run ID


try:
    # conda.yamlをダウンロードして処理
    conda_artifact_path = f"runs:/{run_id}/model/conda.yaml"
    conda_file_path = mlflow.artifacts.download_artifacts(artifact_uri=conda_artifact_path, dst_path=tmp_dir)
    
    with open(conda_file_path, 'r') as file:
        conda_config = yaml.safe_load(file)
    
    # numpyがpip依存関係にリストされているか確認
    pip_dependencies = conda_config.get("dependencies", [])
    pip_section = next((dep for dep in pip_dependencies if isinstance(dep, dict) and "pip" in dep), None)
    
    numpy_in_conda = False
    if pip_section:
        numpy_in_conda = any(pkg.startswith("numpy==") for pkg in pip_section["pip"])
    
    if not numpy_in_conda:
        numpy_version = np.__version__
        print(f"conda.yamlにnumpy=={numpy_version}を追加")
        
        if not pip_section:
            # pipセクションがない場合は作成
            pip_section = {"pip": []}
            conda_config["dependencies"].append(pip_section)
        
        pip_section["pip"].append(f"numpy=={numpy_version}")
        
        # 更新されたconda.yamlをファイルに書き戻す
        with open(conda_file_path, 'w') as file:
            yaml.dump(conda_config, file)
        
        # 更新されたconda.yamlをMLflowにログ
        client.log_artifact(run_id=run_id, local_path=conda_file_path, artifact_path="model")
    
    # requirements.txtをダウンロードして処理
    req_artifact_path = f"runs:/{run_id}/model/requirements.txt"
    req_file_path = mlflow.artifacts.download_artifacts(artifact_uri=req_artifact_path, dst_path=tmp_dir)
    
    with open(req_file_path, 'r') as file:
        requirements = [line.strip() for line in file.readlines()]
    
    numpy_in_requirements = any(pkg.startswith("numpy==") for pkg in requirements)
    
    if not numpy_in_requirements:
        numpy_version = np.__version__
        print(f"requirements.txtにnumpy=={numpy_version}を追加")
        requirements.append(f"numpy=={numpy_version}")
        
        # 更新されたrequirements.txtをファイルに書き戻す
        with open(req_file_path, 'w') as file:
            file.write("\n".join(requirements))
        
        # 更新されたrequirements.txtをMLflowにログ
        client.log_artifact(run_id=run_id, local_path=req_file_path, artifact_path="model")
        
finally:
    # 一時ディレクトリをクリーンアップ
    shutil.rmtree(tmp_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2-3. MLflowにログされたモデルが正常に使えるかテストします

# COMMAND ----------

# DBTITLE 1,テスト推論 loaded from MLflow Experiment
import mlflow
from pyspark.sql.functions import struct, col
logged_model = f'runs:/{best_run_id}/model'

# Spark UDFとしてモデルをロードします。モデルがDouble値を返さない場合、「result_type」を上書きします。
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model)

# Sparkデータフレームでの予測。
predictions_df = df.withColumn('predictions', loaded_model(struct(*map(col, df.columns))))
display(predictions_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. モデルのUnity Catalog(UC)への登録
# MAGIC MLflowを使用して、最良モデルをUCに登録します。<br>
# MAGIC AutoMLでトレーニングしたモデルをモデルサービングエンドポイントにデプロイする場合、mlflow.pyfunc形式でのラップは必須です。

# COMMAND ----------

# best_run_id = "beb9b8563f6844a181da4ff05a1f2d1a"

# COMMAND ----------

# DBTITLE 1,UC登録
from mlflow import MlflowClient
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

# Databricks Unity Catalogを使用してモデルを保存します
mlflow.set_registry_uri('databricks-uc')
client = MlflowClient()

# カタログにモデルを追加
latest_model = mlflow.register_model(f'runs:/{best_run_id}/model', MODEL_NAME)

# UCエイリアスを使用してプロダクション対応としてフラグを立てる
client.set_registered_model_alias(name=f"{MODEL_NAME}", alias="prod", version=latest_model.version)

# WorkspaceClientのインスタンスを作成
sdk_client = WorkspaceClient()

# 全ユーザー(グループ名: account users)にモデルの権限を設定
sdk_client.grants.update(c.SecurableType.FUNCTION, f"{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}", 
                         changes=[c.PermissionsChange(add=[c.Privilege["ALL_PRIVILEGES"]], principal="account users")])

# COMMAND ----------

# DBTITLE 1,テスト推論 loaded from UC
import mlflow

# エイリアス指定（例: Prod）
model_uri = f"models:/{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}@prod"

# モデルロード（pyfunc形式で）
model = mlflow.pyfunc.load_model(model_uri)

# UCテーブルからSpark DataFrameを取得
input_sdf = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.churn_features").drop('customerID')

# Pandasに変換
input_pdf = input_sdf.toPandas()

# 推論
preds = model.predict(input_pdf)
# print(preds)

input_pdf["prediction"] = preds
display(input_pdf.head())
