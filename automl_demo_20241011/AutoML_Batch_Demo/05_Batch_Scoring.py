# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # AutoMLで生成された最良のモデルを使用して、クレジットの信用度をバッチスコアリングする
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-ml-experiment.png" style="float: right" width="600px">
# MAGIC
# MAGIC Databricks AutoMLは、グリッド検索を行い、多くのモデルとメトリクスを生成して、すべての試行の中から最良のモデルを特定します。これは、すべてのコードアーティファクトと実験結果が後から利用できる「透明性のあるアプローチ」で、ベースラインモデルを作成します。
# MAGIC
# MAGIC ここでは、AutoML実験の中から最も良い結果を出したノートブックを選択しました。
# MAGIC
# MAGIC 以下のコードはすべて自動生成されたものです。データサイエンティストとして、これをビジネスの知識に基づいて調整することもできますし、生成されたモデルをそのまま使用することもできます。
# MAGIC
# MAGIC これにより、データサイエンティストは開発時間を数時間節約でき、特に通信会社の支払いデータのような代替データに対して予測因子がわからない場合でも、チームは新しいプロジェクトを迅速に開始し、検証することが可能になります。
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&org_id=984752964297111&notebook=%2F03-Data-Science-ML%2F03.3-Batch-Scoring-credit-decisioning&demo_name=lakehouse-fsi-credit&event=VIEW&path=%2F_dbdemos%2Flakehouse%2Flakehouse-fsi-credit%2F03-Data-Science-ML%2F03.3-Batch-Scoring-credit-decisioning&version=1">

# COMMAND ----------

# MAGIC %md 
# MAGIC ### A cluster has been created for this demo
# MAGIC To run this demo, just select the cluster `dbdemos-lakehouse-fsi-credit-konomi_omae` from the dropdown menu ([open cluster configuration](https://adb-984752964297111.11.azuredatabricks.net/#setting/clusters/0914-122336-4u3552ik/configuration)). <br />
# MAGIC *Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('lakehouse-fsi-credit')` or re-install the demo: `dbdemos.install('lakehouse-fsi-credit')`*

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 既存のデータベースに対してバッチ推論を実行しスコアリング
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi-credit-decisioning-ml-3.png" style="float: right" width="800px">
# MAGIC
# MAGIC <br/><br/>
# MAGIC 私たちのモデルが作成され、MLFlowレジストリ内で本番環境にデプロイされました。
# MAGIC
# MAGIC <br/>
# MAGIC これで`Production`ステージを呼び出してモデルを簡単にロードし、任意のデータエンジニアリングパイプライン（毎晩実行されるジョブ、ストリーミング、あるいはDelta Live Tableパイプライン内でも）で使用できます。
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC 次に、この情報を新しいテーブルとしてFSデータベースに保存し、それを基にダッシュボードやアラートを構築して、リアルタイムの分析を開始します。

# COMMAND ----------

# MLflowのレジストリURIを設定
mlflow.set_registry_uri('databricks-uc')

# モデルをSpark UDFとしてロード
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}@prod", result_type='double')

# COMMAND ----------

# DBTITLE 1,特徴量抽出とクレジット審査の予測
# モデルの入力スキーマから特徴量名を取得
features = loaded_model.metadata.get_input_schema().input_names()

# "credit_decisioning_features"テーブルを読み込み、欠損値を0で埋め、予測列を追加
underbanked_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.credit_decisioning_features").fillna(0) \
                   .withColumn("prediction", loaded_model(F.struct(*features))).cache()

# 結果を表示
display(underbanked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 上記のスコアリング済みデータフレームでは、既存の銀行口座の有無にかかわらず、すべての顧客の信用度を予測するためのエンドツーエンドのプロセスを構築しました。このデータには、バイナリ予測が含まれており、Databricks AutoMLからのすべての知見と、feature store からキュレーションされた特徴量が統合されています。

# COMMAND ----------

# DBTITLE 1,クレジット審査の予測結果をテーブル保存
from pyspark.sql import functions as F

# タイムスタンプカラムを追加
underbanked_df_with_timestamp = underbanked_df.withColumn(
    "prediction_timestamp", 
    F.from_utc_timestamp(F.current_timestamp(), "Asia/Tokyo")
)

# underbanked_predictionテーブルに追加（JSTタイムスタンプ付き）
underbanked_df_with_timestamp.write.mode("append").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.underbanked_prediction")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 推論結果データをS3にエクスポート

# COMMAND ----------

# DBTITLE 1,推論結果のデータをS3にエクスポート
from pyspark.sql import functions as F
from datetime import datetime
import pytz

# 現在の日時を取得してファイル名を生成
jst = pytz.timezone('Asia/Tokyo')
current_time_jst = datetime.now(jst).strftime("%Y%m%d_%H%M%S")
file_name = f"{current_time_jst}_underbanked_prediction.csv"

# CSVファイルとして出力
underbanked_df_with_timestamp.coalesce(1).toPandas() \
    .to_csv(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_EXPORT}/{file_name}", index=False)

print(f"ファイル出力完了: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_EXPORT}/{file_name}")

# COMMAND ----------

# DBTITLE 1,ExportファイルALL削除
# dbutils.fs.rm(f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_EXPORT}", True)
# print(f"ファイル削除完了: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_EXPORT}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 次のステップ
# MAGIC
# MAGIC * モデルをリアルタイム推論にデプロイし、銀行内で```後払い```機能を有効にするには、[03.4-model-serving-BNPL-credit-decisioning]($./03.4-model-serving-BNPL-credit-decisioning) を使用してください。
# MAGIC
# MAGIC または
# MAGIC
# MAGIC * どのような顧客層に対しても公平なモデルを構築することは、FSI（金融サービス業界）のユースケースにおける本番環境向けMLモデルの構築において非常に重要です。<br/>
# MAGIC モデルをLakehouseで [03.5-Explainability-and-Fairness-credit-decisioning]($./03.5-Explainability-and-Fairness-credit-decisioning) を使用して探求してみてください。
