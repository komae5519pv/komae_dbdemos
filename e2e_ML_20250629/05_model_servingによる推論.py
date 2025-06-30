# Databricks notebook source
# MAGIC %md # 05. モデルサービングによる推論
# MAGIC
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/5_model_serving.png' width='800' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/5_model_serving.png?raw=true' width='1200' />

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ai_query関数によるクエリ
# MAGIC モデルサービングエンドポイントにデプロイされたモデルをクエリ、正常に推論できるかテストします

# COMMAND ----------

# DBTITLE 1,ai_query x SQL
results = spark.sql(f'''
SELECT
  * EXCEPT (churn),
  ai_query(
    'komae_churn_model',
    named_struct(
      "seniorCitizen", seniorCitizen,
      "tenure", tenure,
      "monthlyCharges", monthlyCharges,
      "totalCharges", totalCharges,
      "gender_Female", gender_Female,
      "gender_Male", gender_Male,
      "partner_No", partner_No,
      "partner_Yes", partner_Yes,
      "dependents_No", dependents_No,
      "dependents_Yes", dependents_Yes,
      "phoneService_No", phoneService_No,
      "phoneService_Yes", phoneService_Yes,
      "multipleLines_No", multipleLines_No,
      "multipleLines_Nophoneservice", multipleLines_Nophoneservice,
      "multipleLines_Yes", multipleLines_Yes,
      "internetService_DSL", internetService_DSL,
      "internetService_Fiberoptic", internetService_Fiberoptic,
      "internetService_No", internetService_No,
      "onlineSecurity_No", onlineSecurity_No,
      "onlineSecurity_Nointernetservice", onlineSecurity_Nointernetservice,
      "onlineSecurity_Yes", onlineSecurity_Yes,
      "onlineBackup_No", onlineBackup_No,
      "onlineBackup_Nointernetservice", onlineBackup_Nointernetservice,
      "onlineBackup_Yes", onlineBackup_Yes,
      "deviceProtection_No", deviceProtection_No,
      "deviceProtection_Nointernetservice", deviceProtection_Nointernetservice,
      "deviceProtection_Yes", deviceProtection_Yes,
      "techSupport_No", techSupport_No,
      "techSupport_Nointernetservice", techSupport_Nointernetservice,
      "techSupport_Yes", techSupport_Yes,
      "streamingTV_No", streamingTV_No,
      "streamingTV_Nointernetservice", streamingTV_Nointernetservice,
      "streamingTV_Yes", streamingTV_Yes,
      "streamingMovies_No", streamingMovies_No,
      "streamingMovies_Nointernetservice", streamingMovies_Nointernetservice,
      "streamingMovies_Yes", streamingMovies_Yes,
      "contract_Month-to-month", `contract_Month-to-month`,
      "contract_Oneyear", contract_Oneyear,
      "contract_Twoyear", contract_Twoyear,
      "paperlessBilling_No", paperlessBilling_No,
      "paperlessBilling_Yes", paperlessBilling_Yes,
      "paymentMethod_Banktransfer-automatic", `paymentMethod_Banktransfer-automatic`,
      "paymentMethod_Creditcard-automatic", `paymentMethod_Creditcard-automatic`,
      "paymentMethod_Electroniccheck", paymentMethod_Electroniccheck,
      "paymentMethod_Mailedcheck", paymentMethod_Mailedcheck
    ),
    'FLOAT'  -- モデルが返す型に合わせて指定
  ) AS prediction
FROM {MY_CATALOG}.{MY_SCHEMA}.churn_features
LIMIT 100
''')

print(results.count())
print(results.columns)
display(results.limit(10))

# COMMAND ----------

# DBTITLE 1,ai_query x Python
from pyspark.sql.functions import expr

# テーブルデータの取得
df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.churn_features")

# ai_queryをexprで実行
results = df.withColumn(
    "prediction",
    expr(f"""
    ai_query(
        'komae_churn_model',
        named_struct(
            'seniorCitizen', seniorCitizen,
            'tenure', tenure,
            'monthlyCharges', monthlyCharges,
            'totalCharges', totalCharges,
            'gender_Female', gender_Female,
            'gender_Male', gender_Male,
            'partner_No', partner_No,
            'partner_Yes', partner_Yes,
            'dependents_No', dependents_No,
            'dependents_Yes', dependents_Yes,
            'phoneService_No', phoneService_No,
            'phoneService_Yes', phoneService_Yes,
            'multipleLines_No', multipleLines_No,
            'multipleLines_Nophoneservice', multipleLines_Nophoneservice,
            'multipleLines_Yes', multipleLines_Yes,
            'internetService_DSL', internetService_DSL,
            'internetService_Fiberoptic', internetService_Fiberoptic,
            'internetService_No', internetService_No,
            'onlineSecurity_No', onlineSecurity_No,
            'onlineSecurity_Nointernetservice', onlineSecurity_Nointernetservice,
            'onlineSecurity_Yes', onlineSecurity_Yes,
            'onlineBackup_No', onlineBackup_No,
            'onlineBackup_Nointernetservice', onlineBackup_Nointernetservice,
            'onlineBackup_Yes', onlineBackup_Yes,
            'deviceProtection_No', deviceProtection_No,
            'deviceProtection_Nointernetservice', deviceProtection_Nointernetservice,
            'deviceProtection_Yes', deviceProtection_Yes,
            'techSupport_No', techSupport_No,
            'techSupport_Nointernetservice', techSupport_Nointernetservice,
            'techSupport_Yes', techSupport_Yes,
            'streamingTV_No', streamingTV_No,
            'streamingTV_Nointernetservice', streamingTV_Nointernetservice,
            'streamingTV_Yes', streamingTV_Yes,
            'streamingMovies_No', streamingMovies_No,
            'streamingMovies_Nointernetservice', streamingMovies_Nointernetservice,
            'streamingMovies_Yes', streamingMovies_Yes,
            'contract_Month-to-month', `contract_Month-to-month`,
            'contract_Oneyear', contract_Oneyear,
            'contract_Twoyear', contract_Twoyear,
            'paperlessBilling_No', paperlessBilling_No,
            'paperlessBilling_Yes', paperlessBilling_Yes,
            'paymentMethod_Banktransfer-automatic', `paymentMethod_Banktransfer-automatic`,
            'paymentMethod_Creditcard-automatic', `paymentMethod_Creditcard-automatic`,
            'paymentMethod_Electroniccheck', paymentMethod_Electroniccheck,
            'paymentMethod_Mailedcheck', paymentMethod_Mailedcheck
        ),
        'FLOAT'
    )
    """)
)

print(results.count())
print(results.columns)
display(results.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SDKによる高レベルアクセス
# MAGIC 特徴: REST APIをラップした便利なインターフェース

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-1. Databricks SDK fro Python
# MAGIC [Databricks SDK fro Python](https://databricks-sdk-py.readthedocs.io/en/stable/workspace/serving/serving_endpoints.html)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import pandas as pd

w = WorkspaceClient()

# 特徴量データの取得
features_df = spark.sql(f"""
SELECT * EXCEPT (churn)
FROM {MY_CATALOG}.{MY_SCHEMA}.churn_features
LIMIT 100
""").toPandas()

# レコード形式に変換
records = features_df.to_dict(orient='records')

# エンドポイントクエリ実行
response = w.serving_endpoints.query(
    name=SERVING_ENDPOINT,
    dataframe_records=records  # 特徴量データを直接渡す
)

# 結果をDataFrameに変換
results_df = pd.DataFrame({
    'features': records,
    'prediction': response.predictions
})

# features列を個別のカラムに展開
features_expanded = pd.json_normalize(results_df['features'])
final_df = pd.concat([features_expanded, results_df['prediction']], axis=1)

print(len(final_df))
print(len(final_df.columns))
display(final_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-2. MLflow Deployments SDK
# MAGIC [カスタム モデルのエンドポイントを提供するクエリ](https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/model-serving/score-custom-model-endpoints)

# COMMAND ----------

import mlflow.deployments
import pandas as pd

# クライアント初期化
client = mlflow.deployments.get_deploy_client("databricks")

# 特徴量データの取得（100件サンプリング）
features_df = spark.sql(f"""
SELECT * EXCEPT (churn)
FROM {MY_CATALOG}.{MY_SCHEMA}.churn_features
LIMIT 100
""").toPandas()

# 推論リクエスト送信
response = client.predict(
    endpoint=SERVING_ENDPOINT,
    inputs={
        "dataframe_records": features_df.to_dict(orient='records')
    }
)

# 結果の加工
predictions = response['predictions']  # 数値リストを直接取得
results_df = features_df.copy()
results_df['prediction'] = predictions  # そのまま代入

# 結果表示
print(len(results_df))
print(results_df.columns.tolist())
display(results_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. REST APIによるアクセス

# COMMAND ----------

# MAGIC %md ### トークンの発行
# MAGIC
# MAGIC モデルサービング機能を使って、推論するためには、トークンを発行して置く必要があります。　<br>
# MAGIC `Setting` - `User Settings` - `Access Tokens`
# MAGIC
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/token.png' width='800' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/token.png?raw=true' width='1200' />

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-1. Curl によるアクセス
# MAGIC
# MAGIC サンプルデータとしてこちらの`data.json`をローカルにダウンロードする (右クリックで保存)
# MAGIC
# MAGIC https://sajpstorage.blob.core.windows.net/public-data/data.json

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC 次に、モデルサービングのUI画面にて、Curlタブのコードをコピーして、ローカル端末のターミナル上にペーストします。Tokenの箇所を先ほど発行したTokenを使って置き換えます。
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/curl.png' width='800' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/curl.png?raw=true' width='800' />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/curl2.png' width='800' /> <br> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/curl2.png?raw=true' width='1200' /> <br>
# MAGIC (*) sudo 実行が必要な場合があります。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2. Python コードによるクライアントアクセス
# MAGIC
# MAGIC Pythonコードでも、エンドポイント経由でアクセスする事ができます。<br>
# MAGIC
# MAGIC 以下は、クライアント側のコードで実行しているイメージでご覧ください。

# COMMAND ----------

import os

# Token をコピペ
# os.environ["DATABRICKS_TOKEN"] = '<token>'

# Secretを利用したトークンの引き渡しの場合
os.environ["DATABRICKS_TOKEN"] = dbutils.secrets.get("komae_scope", "ws_token")

# COMMAND ----------

# MAGIC %md 
# MAGIC 次に、モデルサービングのUI画面にて、Pythonタブのコードをコピーして、下のセルにペーストします。
# MAGIC
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/model_python.png' width='800' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/model_python.png?raw=true' width='800' />

# COMMAND ----------

# DBTITLE 1,こちらにペーストしてください(上書き)

import os
import requests
import numpy as np
import pandas as pd
import json

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
  url = 'https://e2-demo-field-eng.cloud.databricks.com/model-endpoint/e2ejmaru_churn_model/3/invocations'
  headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}', 'Content-Type': 'application/json'}
  ds_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  data_json = json.dumps(ds_dict, allow_nan=True)
  response = requests.request(method='POST', headers=headers, url=url, data=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

# COMMAND ----------

# モデルサービングは、比較的小さいデータバッチにおいて低レーテンシーで予測するように設計されています。

sample_data = f'/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/ml_sample.csv'
sample_df = spark.read.format('csv').option("header","true").option("inferSchema", "true").load(sample_data).toPandas()
#display(sample_df)

served_predictions = score_model(sample_df)
print(served_predictions)
