# Databricks notebook source
# MAGIC %md
# MAGIC # ワークフローの設定Yamlファイル作成
# MAGIC - ワークフローを一括設定するための設定内容をYamlファイルに出力し、Yaml設定内容を使ってワークフローを一括設定します  
# MAGIC - サーバレス or DBR 16.0ML以降

# COMMAND ----------

# MAGIC %pip install pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. カレントディレクトリ取得

# COMMAND ----------

import os

def get_parent_directory():
    # ノートブックのフルパスを取得
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    full_path = f"/Workspace/{notebook_path.lstrip('/')}"  # パスの正規化
    
    # 親ディレクトリを取得
    parent_dir = os.path.dirname(full_path)
    return parent_dir

# 実行例
current_dir = get_parent_directory()
print("現在のディレクトリ：")
print(f"{current_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. クラスタIDを設定
# MAGIC ここは手動での作業が必要です！  
# MAGIC クラスター -> 該当クラスタ -> 画面右上の３点リーダー -> JSONを表示 -> cluster_idの値をコピー
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/product_recommendation_20250411/_images/step1_get_cluster_json.png?raw=true' width='88%'/>
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/product_recommendation_20250411/_images/step2_copy_cluster_id.png?raw=true' width='88%'/>

# COMMAND ----------

CLUSTER_ID = "0425-101015-kqla2eb4"    # ここにクラスタIDを貼り付けてください

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. パイプラインIDを設定
# MAGIC ここは手動での作業が必要です！  
# MAGIC パイプライン -> 該当パイプライン -> 画面右上「JSON」 -> idの値をコピー
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/pipeline_id.png?raw=true' width='88%'/>

# COMMAND ----------

PIPELINE_ID = "d7968fdf-2869-405d-b991-c655c648921b"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Workflow設定用のYamlファイルを作成
# MAGIC 次のセルを実行すると、同じディレクトリ内にworkflow.yamlというファイルができます  
# MAGIC workflowsを設定する際には、手動のほか、Yaml形式で一括設定できます

# COMMAND ----------

import yaml

def generate_workflow_yaml():
    workflow = {
        "resources": {
            "jobs": {
                WORKFLOW_NAME: {
                    "name": WORKFLOW_NAME,
                    "tasks": [
                        {
                            "task_key": "config",
                            "notebook_task": {
                                "notebook_path": f"{current_dir}/00_config",
                                "source": "WORKSPACE"
                            },
                            "existing_cluster_id": CLUSTER_ID
                        },
                        {
                            "task_key": "load_data",
                            "depends_on": [{"task_key": "config"}],
                            "notebook_task": {
                                "notebook_path": f"{current_dir}/01_load_data",
                                "source": "WORKSPACE"
                            },
                            "existing_cluster_id": CLUSTER_ID
                        },
                        {
                            "task_key": "prep_raw_csv",
                            "depends_on": [{"task_key": "load_data"}],
                            "notebook_task": {
                                "notebook_path": f"{current_dir}/02_prep_raw_csv",
                                "source": "WORKSPACE"
                            },
                            "existing_cluster_id": CLUSTER_ID
                        },
                        {
                            "task_key": "ETL",
                            "depends_on": [{"task_key": "prep_raw_csv"}],
                            "pipeline_task": {
                                "pipeline_id": PIPELINE_ID,
                                "full_refresh": False
                            }
                        },
                        {
                            "task_key": "ETL_for_ai_query",
                            "depends_on": [{"task_key": "ETL"}],
                            "notebook_task": {
                                "notebook_path": f"{current_dir}/07_ETL_for_ai_query",
                                "source": "WORKSPACE"
                            },
                            "existing_cluster_id": CLUSTER_ID
                        },
                        {
                            "task_key": "AutoML",
                            "depends_on": [{"task_key": "ETL_for_ai_query"}],
                            "notebook_task": {
                                "notebook_path": f"{current_dir}/06_AutoML",
                                "source": "WORKSPACE"
                            },
                            "existing_cluster_id": CLUSTER_ID
                        },
                        # {
                        #     "task_key": "batch_scoring",
                        #     "depends_on": [{"task_key": "AutoML"}],
                        #     "notebook_task": {
                        #         "notebook_path": f"{current_dir}/08_batch_scoring",
                        #         "source": "WORKSPACE"
                        #     },
                        #     "existing_cluster_id": CLUSTER_ID
                        # },
                        {
                            "task_key": "model_serving",
                            "depends_on": [{"task_key": "AutoML"}],
                            "notebook_task": {
                                "notebook_path": f"{current_dir}/09_model_serving",
                                "source": "WORKSPACE"
                            },
                            "existing_cluster_id": CLUSTER_ID
                        },
                        {
                            "task_key": "model_train_and_predict",
                            "depends_on": [{"task_key": "ETL"}],
                            "notebook_task": {
                                "notebook_path": f"{current_dir}/05_model_training",
                                "source": "WORKSPACE"
                            },
                            "existing_cluster_id": CLUSTER_ID
                        },
                        {
                            "task_key": "ETL_for_dashboard",
                            "depends_on": [{"task_key": "model_train_and_predict"}],
                            "notebook_task": {
                                "notebook_path": f"{current_dir}/10_ETL_for_dashboard",
                                "source": "WORKSPACE"
                            },
                            "existing_cluster_id": CLUSTER_ID
                        },
                    ],
                    "queue": {"enabled": True}
                }
            }
        }
    }

    with open("workflows.yaml", "w") as file:
        yaml.dump(workflow, file, sort_keys=False)
    
    return "workflows.yaml"

# YAMLファイル生成
generate_workflow_yaml()

# COMMAND ----------

# import yaml
# from datetime import datetime

# # ワークフロー定義のベース作成
# def generate_workflow_yaml():
#     workflow = {
#         "resources": {
#             "jobs": {
#                 f"{WORKFLOW_NAME}": {
#                     "name": f"{WORKFLOW_NAME}",
#                     "tasks": [
#                         {
#                             "task_key": "load_data",
#                             "notebook_task": {
#                                 "notebook_path": f"{current_dir}/01_load_data",
#                                 "source": "WORKSPACE"
#                             },
#                             "existing_cluster_id": f"{CLUSTER_ID}"
#                         },
#                         {
#                             "task_key": "ETL",
#                             "depends_on": [{"task_key": "load_data"}],
#                             "notebook_task": {
#                                 "notebook_path": f"{current_dir}/02_ETL",
#                                 "source": "WORKSPACE"
#                             },
#                             "existing_cluster_id": f"{CLUSTER_ID}"
#                         },
#                         {
#                             "task_key": "train_model",
#                             "depends_on": [{"task_key": "ETL"}],
#                             "notebook_task": {
#                                 "notebook_path": f"{current_dir}/03_train_model",
#                                 "source": "WORKSPACE"
#                             },
#                             "existing_cluster_id": f"{CLUSTER_ID}"
#                         },
#                         {
#                             "task_key": "get_recommends",
#                             "depends_on": [{"task_key": "train_model"}],
#                             "notebook_task": {
#                                 "notebook_path": f"{current_dir}/04_get_recommends",
#                                 "source": "WORKSPACE"
#                             },
#                             "existing_cluster_id": f"{CLUSTER_ID}"
#                         },
#                     ],
#                     "queue": {"enabled": True}
#                 }
#             }
#         }
#     }

#     # YAMLファイル生成
#     filename = f"workflows.yaml"    
#     with open(filename, 'w') as file:
#         yaml.dump(workflow, file, sort_keys=False, default_flow_style=False)
    
#     return filename

# # 関数実行
# generated_file = generate_workflow_yaml()
# print(f"YAMLファイルが生成されました: {generated_file}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ワークフローを設定
# MAGIC step1. 同じディレクトリに作成された、`workflows.yaml`ファイルの中身をコピーしてください
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/product_recommendation_20250411/_images/step3_copy_yaml.png?raw=true' width='88%'/>
# MAGIC
# MAGIC step2. ワークフロー -> 作成 -> ジョブ -> Yamlとして編集
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/product_recommendation_20250411/_images/step4_create_new_job.png?raw=true' width='88%'/>
# MAGIC
# MAGIC step3. ワークフロー -> 作成 -> ジョブ -> Yamlとして編集
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/product_recommendation_20250411/_images/step5_edit_yaml.png?raw=true' width='88%'/>
# MAGIC
# MAGIC step4. 先ほどコピーした`workflows.yaml`ファイルの中身を貼り付けて保存してください
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/product_recommendation_20250411/_images/step6_overwrite_yaml.png?raw=true' width='88%'/>
# MAGIC
# MAGIC step5. これでワークフローの設定が完了です
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/product_recommendation_20250411/_images/step7_complete_workflows_setting.png?raw=true' width='88%'/>
