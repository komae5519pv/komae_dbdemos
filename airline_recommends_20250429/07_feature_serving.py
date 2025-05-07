# Databricks notebook source
# MAGIC %md
# MAGIC # 機内コンテンツレコメンドテーブルのサービング
# MAGIC - 機内コンテンツレコメンド（`gd_recom_top6`）をREST API経由でモデルサービング環境に提供します。
# MAGIC   - モデルサービング内ではSpark Sessionがサポートされず、spark sessionを介してUnity Catalogへの読み書きができません。
# MAGIC   - 代わりにオンラインテーブルをバックエンドとして利用し、低レイテンシーで特徴量を提供します。
# MAGIC   - 結果、会員IDとフライトIDに紐づく機内エンタメコンテンツのレコメンドリストを低レイテンシで取得できます。
# MAGIC - サーバレス or DBR 16.0ML以降

# COMMAND ----------

# MAGIC %pip install --upgrade "databricks-feature-engineering>=0.9.0"  --upgrade "databricks-sdk>=0.40.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# レコメンドリストのテーブル（Deltaテーブル）
feature_table_name = f"{MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6"

# オンラインテーブル（レコメンドリストをオンラインテーブル化したもの）
online_table_name=f"{MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6_online"

# Feature Spec（オンラインテーブルを参照する。UC上に関数として格納され、これをFeature Servingにデプロイする）
feature_spec_name = f"{MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6_fs"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Online Tableを作成する
# MAGIC Python SDKを用いてオンラーンテーブルを作成します。  
# MAGIC [公式Docs](https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/feature-store/online-tables)

# COMMAND ----------

# DBTITLE 1,既存削除
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *
from databricks.sdk.errors import NotFound
import time

w = WorkspaceClient()

def delete_and_wait(name: str, timeout_sec=30):
    try:
        # 既存削除
        w.online_tables.delete(name=name)
        
        # 削除完了したかチェック（ポーリング）
        start = time.time()
        while time.time() - start < timeout_sec:
            try:
                w.online_tables.get(name)
                time.sleep(2)
            except NotFound:  # 削除完了時
                print(f"Deleted: {name}")
                return
                
        raise TimeoutError(f"Timeout after {timeout_sec} seconds")
        
    except NotFound:  # 既に存在しない場合
        print(f"Already deleted: {name}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise


delete_and_wait(online_table_name)

# COMMAND ----------

# DBTITLE 1,新規作成
# from pprint import pprint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *
# import mlflow

w = WorkspaceClient()

# オンラインテーブル設定
spec = OnlineTableSpec(
    primary_key_columns=["user_id", "flight_id"],
    source_table_full_name = feature_table_name,
    run_triggered=OnlineTableSpecTriggeredSchedulingPolicy.from_dict({'triggered': 'true'}),
    perform_full_copy=True
)

# オンラインテーブル作成
online_table = OnlineTable(
    name=online_table_name,
    spec=spec
)
w.online_tables.create_and_wait(table=online_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Feature Store エンドポイントを構築する
# MAGIC 公式手順では、
# MAGIC - オンラインテーブル作成 -> FeatureFunction作成 -> FeatureSpec定義 -> Feature Servingエンドポイントのデプロイ   
# MAGIC となりますが、今回静的なデータ取得（複雑な動的処理は不要）なので、FeatureFunction作成は省きます。  
# MAGIC
# MAGIC つまりこの流れで行きます。  
# MAGIC - オンラインテーブル作成 -> FeatureSpec定義 -> Feature Servingエンドポイントのデプロイ   
# MAGIC [公式Docs](https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/feature-store/feature-serving-tutorial#step-3-create-a-function-in-unity-catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-1. Feature Specの定義（FeatureFunction関数なし）

# COMMAND ----------

# DBTITLE 1,既存削除
from databricks.feature_engineering import FeatureLookup, FeatureFunction, FeatureEngineeringClient

fe = FeatureEngineeringClient()

try:
    fe.delete_feature_spec(name=feature_spec_name)  # 既存削除
except Exception as e:
    if "does not exist" not in str(e):
        raise

# COMMAND ----------

# DBTITLE 1,新規作成
from databricks.feature_engineering import FeatureLookup, FeatureFunction, FeatureEngineeringClient

fe = FeatureEngineeringClient()

# ------ オンラインテーブルを指定
'''処理：user_id、flight_idをもとに、UCテーブルからcontents_listを取得します'''
features = [
    FeatureLookup(
        table_name=feature_table_name,          # OnlineTableではなくDeltaテーブルの方を指定することに注意
        lookup_key=["user_id", "flight_id"],    # 複合キー
        feature_names=["contents_list"]         # 取得するレコメンドリスト（ARRAY型）
    )
]

# ------ Feature Spec作成
'''補足メモ：Feature Specは、Unity Catalog上の関数として参照できます。'''
try:
    fe.delete_feature_spec(name=feature_spec_name)  # 既存削除
except Exception as e:
    if "does not exist" not in str(e):
        raise

fe.create_feature_spec(                             # 新規作成
    name=feature_spec_name,
    features=features
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-2. Feature Servingエンドポイントのデプロイ

# COMMAND ----------

# MAGIC %md
# MAGIC 既存のエンドポイントがあると失敗します。その場合は、先にエンドポイントを削除してから実行してください。

# COMMAND ----------

# DBTITLE 1,新規デプロイ
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()

# エンドポイント名
endpoint_name = MODEL_NAME_GET_RECOMMENDS

try:
 status = w.serving_endpoints.create_and_wait(
   name=endpoint_name,
   config = EndpointCoreConfigInput(
     served_entities=[
       ServedEntityInput(
         entity_name=feature_spec_name,
         scale_to_zero_enabled=True,
         workload_size="Small"
       )
     ]
   )
 )
 print(status)
except Exception as e:  # exceptブロックを追加
    print(f"エラーが発生しました: {str(e)}")
    raise

# エンドポイントステータス取得
status = w.serving_endpoints.get(name=endpoint_name)
print(status)

# COMMAND ----------

# from databricks.sdk import WorkspaceClient
# from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
# from databricks.sdk.errors import ResourceConflict

# w = WorkspaceClient()

# endpoint_name = MODEL_NAME_GET_RECOMMENDS

# def create_endpoint_if_not_exists():
#     try:
#       # エンドポイント作成
#       status = w.serving_endpoints.create_and_wait(
#         name=endpoint_name,
#         config = EndpointCoreConfigInput(
#           served_entities=[
#             ServedEntityInput(
#               entity_name=feature_spec_name,
#               scale_to_zero_enabled=True,
#               workload_size="Small"
#             )
#           ]
#         )
#       )
#       print(status)
        
#     except ResourceConflict as e:
#         # 既に存在する場合の処理
#         print(f"※ エンドポイント '{ENDPOINT_NAME}' は既に存在します")
#         print(f"※ デプロイはスキップします")


# # デプロイ
# create_endpoint_if_not_exists()
