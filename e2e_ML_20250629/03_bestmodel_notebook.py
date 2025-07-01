# Databricks notebook source
# MAGIC %md # 03.ベストモデルノートブック
# MAGIC
# MAGIC DBR 15.4 ML 以降をお使いください
# MAGIC
# MAGIC こちらは、AutoMLで生成されたベストモデルを作成したノートブックをクローンしたものです。 <br>
# MAGIC こちらのノートブックをベースにチューニングを行なっていきます。
# MAGIC
# MAGIC 今回のチューニングポイントは以下です。
# MAGIC 1. データソースをFeature Storeに変更する。
# MAGIC 1. SHAPを利用するため、設定を Trueに変更し、n=10にする. (確認後に Falseに戻す)
# MAGIC
# MAGIC 他には Hyperoptによるハイパーパラメーター探索を行う。アンサンブル学習させるなどのチューニングが可能です。
# MAGIC
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/3_tuning_model.png' width='800' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/3_tuning_model.png?raw=true' width='1200' />

# COMMAND ----------

# MAGIC %md
# MAGIC # LightGBM Classifier training
# MAGIC - This is an auto-generated notebook.
# MAGIC - To reproduce these results, attach this notebook to a cluster with runtime version **12.2.x-cpu-ml-scala2.12**, and rerun it.
# MAGIC - Compare trials in the [MLflow experiment](#mlflow/experiments/597306789857128).
# MAGIC - Clone this notebook into your project folder by selecting **File > Clone** in the notebook toolbar.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

import mlflow
import databricks.automl_runtime

target_col = "churn"

# COMMAND ----------

# MAGIC %md ## データロード
# MAGIC
# MAGIC デフォルトだとデータソースがMLflowによって、サンプルデータがロードされており、それを利用する形式になっている。<br>
# MAGIC この場合、毎回固定のデータを利用することになるのと、より多くのデータで学習させるために、Feature Storeからデータをロードする形に編集します。

# COMMAND ----------

import os
import uuid
import shutil
import pandas as pd

# data load from table
df_loaded = spark.read.table('churn_features').toPandas()

df_loaded.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### サポートされている列を選択
# MAGIC サポートされている列のみを選択します。これにより、トレーニングに使用されていない余分な列が含まれているデータセットに対して予測を行うモデルをトレーニングすることができます。
# MAGIC `["customerID"]` はパイプラインで削除されます。これらの列が削除される理由の詳細については、AutoML実験ページのアラートタブを参照してください。

# COMMAND ----------

from databricks.automl_runtime.sklearn.column_selector import ColumnSelector
supported_cols = ["contract_Twoyear", "streamingMovies_Nointernetservice", "seniorCitizen", "monthlyCharges", "internetService_DSL", "deviceProtection_No", "partner_No", "onlineSecurity_No", "paperlessBilling_No", "techSupport_Yes", "tenure", "streamingMovies_Yes", "techSupport_No", "onlineBackup_No", "totalCharges", "internetService_No", "paymentMethod_Electroniccheck", "partner_Yes", "dependents_Yes", "streamingTV_Yes", "multipleLines_No", "internetService_Fiberoptic", "streamingTV_Nointernetservice", "contract_Month-to-month", "onlineSecurity_Nointernetservice", "paymentMethod_Creditcard-automatic", "onlineBackup_Nointernetservice", "contract_Oneyear", "phoneService_No", "deviceProtection_Nointernetservice", "dependents_No", "streamingTV_No", "onlineSecurity_Yes", "techSupport_Nointernetservice", "phoneService_Yes", "gender_Male", "onlineBackup_Yes", "streamingMovies_No", "deviceProtection_Yes", "paymentMethod_Mailedcheck", "multipleLines_Yes", "gender_Female", "paymentMethod_Banktransfer-automatic", "paperlessBilling_Yes", "multipleLines_Nophoneservice"]
col_selector = ColumnSelector(supported_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocessors

# COMMAND ----------

# MAGIC %md
# MAGIC ### Boolean columns
# MAGIC 各列の欠損値を補完し、1と0に変換します。

# COMMAND ----------

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from sklearn.preprocessing import OneHotEncoder as SklearnOneHotEncoder


bool_imputers = []

bool_pipeline = Pipeline(steps=[
    ("cast_type", FunctionTransformer(lambda df: df.astype(object))),
    ("imputers", ColumnTransformer(bool_imputers, remainder="passthrough")),
    ("onehot", SklearnOneHotEncoder(handle_unknown="ignore", drop="first")),
])

bool_transformers = [("boolean", bool_pipeline, ["seniorCitizen", "streamingMovies_Nointernetservice", "contract_Twoyear", "internetService_DSL", "deviceProtection_No", "partner_No", "onlineSecurity_No", "paperlessBilling_No", "techSupport_Yes", "streamingMovies_Yes", "techSupport_No", "onlineBackup_No", "internetService_No", "paymentMethod_Electroniccheck", "partner_Yes", "dependents_Yes", "streamingTV_Yes", "multipleLines_No", "internetService_Fiberoptic", "streamingTV_Nointernetservice", "contract_Month-to-month", "onlineSecurity_Nointernetservice", "paymentMethod_Creditcard-automatic", "onlineBackup_Nointernetservice", "contract_Oneyear", "phoneService_No", "deviceProtection_Nointernetservice", "dependents_No", "streamingTV_No", "onlineSecurity_Yes", "techSupport_Nointernetservice", "phoneService_Yes", "gender_Male", "onlineBackup_Yes", "streamingMovies_No", "deviceProtection_Yes", "paymentMethod_Mailedcheck", "multipleLines_Yes", "gender_Female", "paperlessBilling_Yes", "paymentMethod_Banktransfer-automatic", "multipleLines_Nophoneservice"])]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Numerical columns
# MAGIC
# MAGIC 数値列の欠損値は、デフォルトで平均値で補完されます。

# COMMAND ----------

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler

num_imputers = []
num_imputers.append(("impute_mean", SimpleImputer(), ["contract_Month-to-month", "contract_Oneyear", "contract_Twoyear", "dependents_No", "dependents_Yes", "deviceProtection_No", "deviceProtection_Nointernetservice", "deviceProtection_Yes", "gender_Female", "gender_Male", "internetService_DSL", "internetService_Fiberoptic", "internetService_No", "monthlyCharges", "multipleLines_No", "multipleLines_Nophoneservice", "multipleLines_Yes", "onlineBackup_No", "onlineBackup_Nointernetservice", "onlineBackup_Yes", "onlineSecurity_No", "onlineSecurity_Nointernetservice", "onlineSecurity_Yes", "paperlessBilling_No", "paperlessBilling_Yes", "partner_No", "partner_Yes", "paymentMethod_Banktransfer-automatic", "paymentMethod_Creditcard-automatic", "paymentMethod_Electroniccheck", "paymentMethod_Mailedcheck", "phoneService_No", "phoneService_Yes", "seniorCitizen", "streamingMovies_No", "streamingMovies_Nointernetservice", "streamingMovies_Yes", "streamingTV_No", "streamingTV_Nointernetservice", "streamingTV_Yes", "techSupport_No", "techSupport_Nointernetservice", "techSupport_Yes", "tenure", "totalCharges"]))

numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors='coerce'))),
    ("imputers", ColumnTransformer(num_imputers)),
    ("standardizer", StandardScaler()),
])

numerical_transformers = [("numerical", numerical_pipeline, ["seniorCitizen", "streamingMovies_Nointernetservice", "contract_Twoyear", "monthlyCharges", "internetService_DSL", "deviceProtection_No", "partner_No", "onlineSecurity_No", "paperlessBilling_No", "techSupport_Yes", "tenure", "streamingMovies_Yes", "techSupport_No", "onlineBackup_No", "totalCharges", "internetService_No", "paymentMethod_Electroniccheck", "partner_Yes", "dependents_Yes", "streamingTV_Yes", "internetService_Fiberoptic", "multipleLines_No", "streamingTV_Nointernetservice", "contract_Month-to-month", "onlineSecurity_Nointernetservice", "paymentMethod_Creditcard-automatic", "onlineBackup_Nointernetservice", "contract_Oneyear", "phoneService_No", "deviceProtection_Nointernetservice", "dependents_No", "streamingTV_No", "onlineSecurity_Yes", "techSupport_Nointernetservice", "phoneService_Yes", "gender_Male", "onlineBackup_Yes", "streamingMovies_No", "deviceProtection_Yes", "paymentMethod_Mailedcheck", "multipleLines_Yes", "gender_Female", "paperlessBilling_Yes", "paymentMethod_Banktransfer-automatic", "multipleLines_Nophoneservice"])]

# COMMAND ----------

from sklearn.compose import ColumnTransformer

transformers = bool_transformers + numerical_transformers

preprocessor = ColumnTransformer(transformers, remainder="passthrough", sparse_threshold=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train - Validation - Test Split
# MAGIC 入力データはAutoMLによって3つのセットに分割されます：
# MAGIC - 学習用（データセットの60%を使用してモデルを学習）
# MAGIC - バリデーション用（データセットの20%を使用してハイパーパラメータを調整）
# MAGIC - テスト用（データセットの20%を使用して未知データでモデルの真の性能を評価）
# MAGIC
# MAGIC `_automl_split_col_1a33` には各行がどのセットに属するかの情報が含まれています。
# MAGIC このカラムを使って上記3つのセットにデータを分割します。
# MAGIC 分割後はこのカラムは学習に使用しないため削除します。

# COMMAND ----------

from sklearn.model_selection import train_test_split

split_X = df_loaded.drop([target_col], axis=1)
split_y = df_loaded[target_col]

# トレインデータを分割
X_train, split_X_rem, y_train, split_y_rem = train_test_split(split_X, split_y, train_size=0.6, random_state=984664349, stratify=split_y)

# 残りのデータをバリデーションとテストに等しく分割
X_val, X_test, y_val, y_test = train_test_split(split_X_rem, split_y_rem, test_size=0.5, random_state=984664349, stratify=split_y_rem)

# COMMAND ----------

# AutoML completed train - validation - test split internally and used _automl_split_col_1a33 to specify the set
#split_train_df = df_loaded.loc[df_loaded._automl_split_col_1a33 == "train"]
#split_val_df = df_loaded.loc[df_loaded._automl_split_col_1a33 == "val"]
#split_test_df = df_loaded.loc[df_loaded._automl_split_col_1a33 == "test"]
#
## Separate target column from features and drop _automl_split_col_1a33
#X_train = split_train_df.drop([target_col, "_automl_split_col_1a33"], axis=1)
#y_train = split_train_df[target_col]
#
#X_val = split_val_df.drop([target_col, "_automl_split_col_1a33"], axis=1)
#y_val = split_val_df[target_col]
#
#X_test = split_test_df.drop([target_col, "_automl_split_col_1a33"], axis=1)
#y_test = split_test_df[target_col]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 分類モデルの学習
# MAGIC - MLflowに関連する指標を記録して実行を追跡します
# MAGIC - すべての実行は[このMLflow実験](#mlflow/experiments/597306789857128)の下に記録されます
# MAGIC - モデルパラメータを変更し、トレーニングセルを再実行することで、異なるトライアルをMLflow実験に記録できます
# MAGIC - チューニング可能なハイパーパラメータの全リストを表示するには、下のセルの出力を確認してください

# COMMAND ----------

import lightgbm
from lightgbm import LGBMClassifier

help(LGBMClassifier)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 目的関数の定義
# MAGIC 最適なハイパーパラメータを探索するために使用される目的関数です。<br>
# MAGIC デフォルトでは、このノートブックは`hyperopt.fmin` の `max_evals=1` により、この関数を1回だけ実行し、<br>
# MAGIC 固定されたハイパーパラメータで評価しますが、下で定義されている `space` を変更することでハイパーパラメータのチューニングが可能です。<br>
# MAGIC `hyperopt.fmin` は、この関数の返り値を使って損失を最小化するように探索を行います。

# COMMAND ----------

import mlflow
from mlflow.models import Model, infer_signature, ModelSignature
from mlflow.pyfunc import PyFuncModel
from mlflow import pyfunc
import sklearn
from sklearn import set_config
from sklearn.pipeline import Pipeline

from hyperopt import hp, tpe, fmin, STATUS_OK, Trials

# バリデーションデータセットを変換するための別のパイプラインを作成（アーリーストッピング用）
mlflow.sklearn.autolog(disable=True)
pipeline_val = Pipeline([
    ("column_selector", col_selector),
    ("preprocessor", preprocessor),
])
pipeline_val.fit(X_train, y_train)
X_val_processed = pipeline_val.transform(X_val)

def objective(params):
  #with mlflow.start_run(experiment_id="597306789857128") as mlflow_run:
  with mlflow.start_run() as mlflow_run:
    lgbmc_classifier = LGBMClassifier(**params)

    model = Pipeline([
        ("column_selector", col_selector),
        ("preprocessor", preprocessor),
        ("classifier", lgbmc_classifier),
    ])

    # 入力サンプル、メトリクス、パラメータ、モデルの自動ロギングを有効化
    mlflow.sklearn.autolog(
        log_input_examples=True,
        silent=True)

    model.fit(X_train, y_train, classifier__callbacks=[lightgbm.early_stopping(5), lightgbm.log_evaluation(0)], classifier__eval_set=[(X_val_processed,y_val)])

    
    # トレーニングセットのメトリクスを記録
    mlflow_model = Model()
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
    pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
    X_train[target_col] = y_train
    training_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_train,
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "training_" , "pos_label": 1 }
    )
    lgbmc_training_metrics = training_eval_result.metrics
    # バリデーションセットのメトリクスを記録
    X_val[target_col] = y_val
    val_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_val,
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "val_" , "pos_label": 1 }
    )
    lgbmc_val_metrics = val_eval_result.metrics
    # テストセットのメトリクスを記録
    X_test[target_col] = y_test
    test_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_test,
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "test_" , "pos_label": 1 }
    )
    lgbmc_test_metrics = test_eval_result.metrics

    loss = lgbmc_val_metrics["val_f1_score"]

    # メトリクスのキー名を短縮してまとめて表示できるようにする
    lgbmc_val_metrics = {k.replace("val_", ""): v for k, v in lgbmc_val_metrics.items()}
    lgbmc_test_metrics = {k.replace("test_", ""): v for k, v in lgbmc_test_metrics.items()}

    return {
      "loss": loss,
      "status": STATUS_OK,
      "val_metrics": lgbmc_val_metrics,
      "test_metrics": lgbmc_test_metrics,
      "model": model,
      "run": mlflow_run,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### ハイパーパラメータ探索空間の設定
# MAGIC パラメータの探索空間を設定します。以下のパラメータはすべて定数式ですが、探索空間を広げるために変更できます。例えば、決定木分類器をトレーニングする際に、最大ツリー深度を2または3にするには、'max_depth'のキーを`hp.choice('max_depth', [2, 3])`に設定します。以下の`fmin`呼び出しで`max_evals`も増やすようにしてください。
# MAGIC
# MAGIC ハイパーパラメータチューニングに関する詳細は、https://docs.databricks.com/applications/machine-learning/automl-hyperparam-tuning/index.html を参照してください。また、サポートされている探索式のドキュメントについては、http://hyperopt.github.io/hyperopt/getting-started/search_spaces/ を参照してください。
# MAGIC
# MAGIC 使用されるモデルのパラメータに関するドキュメントについては、以下を参照してください:
# MAGIC https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.LGBMClassifier.html
# MAGIC
# MAGIC 注意: 上記のURLは、パッケージの最新リリースバージョンに対応する安定版ドキュメントを指しています。使用されているパッケージバージョンによっては、ドキュメントが若干異なる場合があります。

# COMMAND ----------

space = {
  "colsample_bytree": 0.5708410059314412,
  "lambda_l1": 6.847707632318354,
  "lambda_l2": 0.6053125915718953,
  "learning_rate": 3.897914179935798,
  "max_bin": 222,
  "max_depth": 8,
  "min_child_samples": 256,
  "n_estimators": 794,
  "num_leaves": 506,
  "path_smooth": 30.07447097308719,
  "subsample": 0.5445673215029557,
  "random_state": 653201287,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run trials
# MAGIC 探索空間を広げて複数のモデルをトレーニングする場合、`SparkTrials` に切り替えて
# MAGIC Spark上でトレーニングを並列化します:
# MAGIC
# MAGIC from hyperopt import SparkTrials
# MAGIC trials = SparkTrials()
# MAGIC
# MAGIC
# MAGIC 注意: `Trials` は各ハイパーパラメータセットごとにMLFlowランを開始しますが、`SparkTrials` は<br>
# MAGIC トップレベルのランを1つだけ開始し、各ハイパーパラメータセットごとにサブランを開始します。
# MAGIC
# MAGIC 詳細は http://hyperopt.github.io/hyperopt/scaleout/spark/ を参照してください。

# COMMAND ----------

trials = Trials()
fmin(objective,
     space=space,
     algo=tpe.suggest,
     max_evals=1,  # ハイパーパラメータ探索空間を広げるときはこれを増やします。
     trials=trials)

best_result = trials.best_trial["result"]
model = best_result["model"]
mlflow_run = best_result["run"]

display(
  pd.DataFrame(
    [best_result["val_metrics"], best_result["test_metrics"]],
    index=["validation", "test"]))

set_config(display="diagram")
model

# COMMAND ----------

# MAGIC %md
# MAGIC ## 特徴量の重要度
# MAGIC
# MAGIC SHAPは、機械学習モデルを説明するためのゲーム理論的アプローチであり、特徴量とモデル出力の関係を要約プロットとして提供します。特徴量は重要度の降順でランク付けされ、影響/色は特徴量とターゲット変数の相関を示します。
# MAGIC - SHAP特徴量の重要度を生成することは非常にメモリ集約的な操作であるため、AutoMLがメモリ不足にならないようにするために、デフォルトではSHAPを無効にしています。<br />
# MAGIC   以下で定義されたフラグを`shap_enabled = True`に設定し、このノートブックを再実行してSHAPプロットを表示することができます。
# MAGIC - 各トライアルの計算オーバーヘッドを減らすために、検証セットから単一の例をサンプリングして説明します。<br />
# MAGIC   より徹底的な結果を得るためには、説明のサンプルサイズを増やすか、独自の例を提供してください。
# MAGIC - SHAPは欠損値を含むデータを使用するモデルを説明できません。データセットに欠損値がある場合、背景データと説明する例の両方がモード（最頻値）を使用して補完されます。これにより、計算されたSHAP値に影響を与えます。補完されたサンプルは実際のデータ分布と一致しない可能性があります。
# MAGIC
# MAGIC Shapley値の読み方についての詳細は、[SHAPドキュメント](https://shap.readthedocs.io/en/latest/example_notebooks/overviews/An%20introduction%20to%20explainable%20AI%20with%20Shapley%20values.html)を参照してください。

# COMMAND ----------

# このフラグをTrueに設定してノートブックを再実行すると、SHAPプロットが表示されます
# shap_enabled = False
shap_enabled = True         # 一時的にTrueにセットします

# COMMAND ----------

if shap_enabled:
    from shap import KernelExplainer, summary_plot
    # SHAP Explainerのための背景データをサンプリングします。サンプルサイズを増やすと分散が減少します。
    train_sample = X_train.sample(n=min(100, X_train.shape[0]), random_state=653201287)

    # 説明するために検証セットからいくつかの行をサンプリングします。より徹底した結果を得るためにサンプルサイズを増やします。
    example = X_val.sample(n=min(100, X_val.shape[0]), random_state=653201287)

    # 検証セットからサンプリングした行の特徴量の重要性を説明するためにKernel SHAPを使用します。
    predict = lambda x: model.predict(pd.DataFrame(x, columns=X_train.columns))
    explainer = KernelExplainer(predict, train_sample, link="identity")
    shap_values = explainer.shap_values(example, l1_reg=False)
    summary_plot(shap_values, example, class_names=model.classes_)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 推論（Inference）
# MAGIC [MLflow Model Registry](https://docs.databricks.com/applications/mlflow/model-registry.html) は、チームがMLモデルを共有し、実験からオンラインテストや本番運用まで協力し、<br>
# MAGIC 承認やガバナンスのワークフローと統合し、MLのデプロイやパフォーマンスを監視できるコラボレーションハブです。<br>
# MAGIC 以下のスニペットは、このノートブックでトレーニングしたモデルをモデルレジストリに登録し、後で推論のために取得する方法を示しています。
# MAGIC
# MAGIC > **注意:** このノートブックで既にトレーニングされたモデルの `model_uri` は下のセルで確認できます
# MAGIC
# MAGIC ### モデルレジストリへの登録
# MAGIC
# MAGIC model_name = "Example"
# MAGIC
# MAGIC model_uri = f"runs:/{ mlflow_run.info.run_id }/model"
# MAGIC registered_model_version = mlflow.register_model(model_uri, model_name)
# MAGIC
# MAGIC
# MAGIC ### モデルレジストリからの読み込み
# MAGIC
# MAGIC model_name = "Example"
# MAGIC model_version = registered_model_version.version
# MAGIC
# MAGIC model_uri=f"models:/{model_name}/{model_version}"
# MAGIC model = mlflow.pyfunc.load_model(model_uri=model_uri)
# MAGIC model.predict(input_X)
# MAGIC
# MAGIC
# MAGIC ### 登録せずにモデルを読み込む
# MAGIC
# MAGIC model_uri = f"runs:/{ mlflow_run.info.run_id }/model"
# MAGIC
# MAGIC model = mlflow.pyfunc.load_model(model_uri=model_uri)
# MAGIC model.predict(input_X)

# COMMAND ----------

# 生成されたモデルの model_uri
print(f"runs:/{ mlflow_run.info.run_id }/model")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 検証データの混同行列、ROC曲線、および適合率-再現率曲線
# MAGIC
# MAGIC モデルの検証データに対する混同行列、ROC曲線、および適合率-再現率曲線を示します。
# MAGIC
# MAGIC トレーニングデータおよびテストデータで評価されたプロットについては、MLflowランページのアーティファクトを確認してください。

# COMMAND ----------

# 全ての出力（%md ...）を空のセルに貼り付け、リンクをクリックするとMLflowランページが表示されます
print(f"%md [Link to model run page](#mlflow/experiments/597306789857128/runs/{ mlflow_run.info.run_id }/artifactPath/model)")

# COMMAND ----------

import uuid
from IPython.display import Image

# 一時ディレクトリを作成してMLflowモデルアーティファクトをダウンロード
eval_temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], "tmp", str(uuid.uuid4())[:8])
os.makedirs(eval_temp_dir, exist_ok=True)

# アーティファクトをダウンロード
eval_path = mlflow.artifacts.download_artifacts(run_id=mlflow_run.info.run_id, dst_path=eval_temp_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Confusion matrix for validation dataset

# COMMAND ----------

eval_confusion_matrix_path = os.path.join(eval_path, "confusion_matrix.png")
display(Image(filename=eval_confusion_matrix_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ROC curve for validation dataset

# COMMAND ----------

eval_roc_curve_path = os.path.join(eval_path, "roc_curve_plot.png")
display(Image(filename=eval_roc_curve_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Precision-Recall curve for validation dataset

# COMMAND ----------

eval_pr_curve_path = os.path.join(eval_path, "precision_recall_curve_plot.png")
display(Image(filename=eval_pr_curve_path))
