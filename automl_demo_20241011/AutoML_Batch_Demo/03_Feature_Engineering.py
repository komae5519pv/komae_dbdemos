# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # ML: 高い債務不履行確率を持つクレジット所有者の予測
# MAGIC
# MAGIC すべてのデータがロードされ、セキュリティが確保された（**data unification**部分）後、データを探索し、理解し、アクションに繋がるインサイトを作成するための**data decisioning**に進むことができます。
# MAGIC
# MAGIC [イントロダクションノートブック]($../00-Credit-Decisioning)で説明したように、以下の3つのビジネス成果を目指して機械学習（ML）モデルを構築します：
# MAGIC 1. 信用力が高いが現在銀行サービスを十分に利用していない顧客を特定し、クレジット商品を提供する
# MAGIC 2. 高い債務不履行の確率を持つ現在のクレジット所有者を予測し、デフォルト時の損失額を予測する
# MAGIC 3. 顧客が取引を完了するためのクレジット限度額や口座残高が不足している場合、瞬時に少額ローン（後払い）を提供する
# MAGIC
# MAGIC 以下のフローを実装します：
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi-credit-decisioning-ml-0.png" width="1200px">
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&org_id=984752964297111&notebook=%2F03-Data-Science-ML%2F03.1-Feature-Engineering-credit-decisioning&demo_name=lakehouse-fsi-credit&event=VIEW&path=%2F_dbdemos%2Flakehouse%2Flakehouse-fsi-credit%2F03-Data-Science-ML%2F03.1-Feature-Engineering-credit-decisioning&version=1">

# COMMAND ----------

# MAGIC %md 
# MAGIC ### A cluster has been created for this demo
# MAGIC To run this demo, just select the cluster `dbdemos-lakehouse-fsi-credit-konomi_omae` from the dropdown menu ([open cluster configuration](https://adb-984752964297111.11.azuredatabricks.net/#setting/clusters/0914-122336-4u3552ik/configuration)). <br />
# MAGIC *Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('lakehouse-fsi-credit')` or re-install the demo: `dbdemos.install('lakehouse-fsi-credit')`*

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## The need for Enhanced Collaboration(強化されたコラボレーションの必要性)
# MAGIC
# MAGIC 特徴量エンジニアリングは反復的なプロセスです。新しい特徴量を素早く生成し、モデルをテストし、再び特徴量選択やさらに特徴量エンジニアリングを行うというサイクルを何度も繰り返す必要があります。Databricks Lakehouseは、データチームが以下のDatabricksノートブックの機能を通じて非常に効率的にコラボレーションできるようにします：
# MAGIC 1. チームメンバーが同じノートブックを共有し、異なるアクセスモードで共同作業ができる、
# MAGIC 2. 同じデータ上でPython、SQL、Rを同時に使用できる、
# MAGIC 3. Gitリポジトリ（AWS Code Commit、Azure DevOps、GitLab、Githubなど）とネイティブに統合されており、ノートブックをCI/CDツールとして利用できる、
# MAGIC 4. 変数エクスプローラーの使用、
# MAGIC 5. 自動データプロファイリング（以下のセルに表示）、および
# MAGIC 6. GUIベースのダッシュボード（以下のセルに表示）を作成し、Databricks SQLダッシュボードに追加できる。
# MAGIC
# MAGIC これらの機能により、FSI（金融サービス業界）の組織内のチームは、より迅速かつ効率的に最高の機械学習モデルを構築できるようになり、金利上昇などの市場機会を最大限に活用することが可能になります。
# MAGIC

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## データ探索 & 特徴量の作成
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi-credit-decisioning-ml-1.png" style="float: right" width="800px">
# MAGIC
# MAGIC <br/><br/>
# MAGIC データサイエンティストとしての最初のステップは、データを探索し、理解して特徴量を作成することです。
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ここでは、既存のテーブルを使用し、データを変換して機械学習モデルの準備を行います。これらの特徴量は後でDatabricks Feature Storeに保存され、前述の機械学習モデルのトレーニングに使用されます。
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC まずはデータ探索を始めましょう。Databricksには、データプロファイリングが組み込まれており、それを利用して簡単に探索を始めることができます。

# COMMAND ----------

# DBTITLE 1,Use SQL to explore your data
# MAGIC %sql
# MAGIC SELECT * FROM customer_gold WHERE tenure_months BETWEEN 10 AND 150

# COMMAND ----------

# DBTITLE 1,教育レベルと在籍月数による月収の関係分析
# 概要：このグラフは、教育レベル別に在籍月数と月収の合計の関係を示しています。
# 　　　これにより、教育レベルが顧客の収入にどのように影響しているか、また在籍期間が収入にどのように影響しているかを判断できます。
# 処理：
# 　　・在籍月数が10から150の間のデータをフィルタリング
# 　　・在籍月数と教育レベルでグループ化し、月収の合計を計算
# 　　・教育レベルで並び替えてPandasのDataFrameに変換

# customer_gold テーブルからデータを読み込む
data = spark.table("customer_gold") \
              .where("tenure_months BETWEEN 10 AND 150") \
              .groupBy("tenure_months", "education").sum("income_monthly") \
              .orderBy('education').toPandas()

# バーチャートを作成。x軸に在籍月数、y軸に月収の合計、色分けに教育レベルを使用
px.bar(data, x="tenure_months", y="sum(income_monthly)", color="education", title="Wide-Form Input")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # クレジットデフォルト(債務不履行)リスクのための特徴量構築
# MAGIC
# MAGIC クレジットデフォルト(債務不履行)リスクを予測するモデルを構築するためには、多くの特徴量が必要です。ガバナンスを強化し、複数の機械学習プロジェクト向けにデータを一元化するため、特徴量はFeature Storeを使用して保存することができます。

# COMMAND ----------

# DBTITLE 1,顧客情報から特徴量を抽出
# customer_gold_features テーブルから顧客の特徴を抽出し、加工する
# 加工内容：
#  - 現在の年から生年を引いて顧客の年齢を計算
#  - 重複する顧客IDを削除
customer_gold_features = (spark.table("customer_gold")
                               .withColumn('age', int(date.today().year) - col('birth_year'))
                               .select('cust_id', 'education', 'marital_status', 'months_current_address', 'months_employment', 'is_resident',
                                       'tenure_months', 'product_cnt', 'tot_rel_bal', 'revenue_tot', 'revenue_12m', 'income_annual', 'tot_assets', 
                                       'overdraft_balance_amount', 'overdraft_number', 'total_deposits_number', 'total_deposits_amount', 'total_equity_amount', 
                                       'total_UT', 'customer_revenue', 'age', 'avg_balance', 'num_accs', 'balance_usd', 'available_balance_usd')).dropDuplicates(['cust_id'])
display(customer_gold_features)

# COMMAND ----------

# DBTITLE 1,通信データから特徴量を抽出
# telco_gold テーブルから通信サービスに関する顧客の特徴を抽出し、加工する
# 加工内容：
#  - 重複する顧客IDを削除
telco_gold_features = (spark.table("telco_gold")
                            .select('cust_id', 'is_pre_paid', 'number_payment_delays_last12mo', 'pct_increase_annual_number_of_delays_last_3_year', 'phone_bill_amt', \
                                    'avg_phone_bill_amt_lst12mo')).dropDuplicates(['cust_id'])
display(telco_gold_features)

# COMMAND ----------

# DBTITLE 1,金融取引データから特徴量を抽出・送金・受取回数・金額の集計
# fund_trans_gold テーブルから金融取引に関する顧客の特徴を抽出し、加工する
# 加工内容：
#  - 重複する顧客IDを削除
#  - 過去12ヶ月、6ヶ月、3ヶ月の送金と受取の合計回数と合計金額を計算
#  - 過去3ヶ月と12ヶ月、6ヶ月と12ヶ月の取引金額の比率を計算
#  - null値を0で埋める
fund_trans_gold_features = spark.table("fund_trans_gold").dropDuplicates(['cust_id'])

for c in ['12m', '6m', '3m']:
  fund_trans_gold_features = fund_trans_gold_features.withColumn('tot_txn_cnt_'+c, col('sent_txn_cnt_'+c)+col('rcvd_txn_cnt_'+c))\
                                                     .withColumn('tot_txn_amt_'+c, col('sent_txn_amt_'+c)+col('rcvd_txn_amt_'+c))

fund_trans_gold_features = fund_trans_gold_features.withColumn('ratio_txn_amt_3m_12m', F.when(col('tot_txn_amt_12m')==0, 0).otherwise(col('tot_txn_amt_3m')/col('tot_txn_amt_12m')))\
                                                   .withColumn('ratio_txn_amt_6m_12m', F.when(col('tot_txn_amt_12m')==0, 0).otherwise(col('tot_txn_amt_6m')/col('tot_txn_amt_12m')))\
                                                   .na.fill(0)
display(fund_trans_gold_features)

# COMMAND ----------

# DBTITLE 1,すべての特徴量を統合
# 顧客の基本情報と電話サービスに関する特徴を結合
feature_df = customer_gold_features.join(telco_gold_features.alias('telco'), "cust_id", how="left")

# 上記の結果に、金融取引に関する顧客の特徴を追加
feature_df = feature_df.join(fund_trans_gold_features, "cust_id", how="left")
display(feature_df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Databricks Feature Store
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-feature-store.png" style="float:right" width="650" />
# MAGIC
# MAGIC 特徴量が準備できたら、Databricks Feature Storeに保存します。
# MAGIC
# MAGIC Feature Storeの裏側では、Delta Lakeテーブルがサポートしています。これにより、組織内で特徴量の発見や再利用が可能になり、チームの効率が向上します。
# MAGIC
# MAGIC Databricks Feature Storeは、機械学習のプロセスを加速し、簡素化するための高度な機能を提供します。たとえば、**point in time support**や**online-store**を利用して、リアルタイムのサービングのために数ミリ秒以内に特徴量を取得することができます。
# MAGIC
# MAGIC ### なぜDatabricks Feature Storeを使うのか？
# MAGIC
# MAGIC Databricks Feature Storeは、Databricksの他のコンポーネントと完全に統合されています。
# MAGIC
# MAGIC * **Discoverability**。DatabricksワークスペースからアクセスできるFeature StoreのUIを使って、既存の特徴量をブラウズしたり検索したりできます。
# MAGIC
# MAGIC * **リネージ**。Feature Storeで特徴量テーブルを作成すると、そのテーブルを作成するために使用されたデータソースが保存され、アクセス可能になります。特徴量テーブル内の各特徴量に対して、それを使用しているモデル、ノートブック、ジョブ、エンドポイントにもアクセスできます。
# MAGIC
# MAGIC * **バッチおよびオンライン特徴量のリアルタイムサービング**。Feature Storeの特徴量を使ってモデルをトレーニングする場合、モデルは特徴量のメタデータと共にパッケージ化されます。バッチスコアリングやオンライン推論にモデルを使用する際、自動的にFeature Storeから特徴量を取得します。これにより、モデルの展開と更新が非常に簡単になります。
# MAGIC
# MAGIC * **Point-in-time lookups**。Feature Storeは、ポイントインタイムの正確性が求められる時系列やイベントベースのユースケースをサポートしています。
# MAGIC
# MAGIC
# MAGIC Databricks Feature Storeについてさらに詳しく知りたい場合は、`dbdemos.install('feature-store')`を実行してください。

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# デモの状態をクリーンアップするために、すでに存在している場合は feature table テーブルを削除する
drop_fs_table(f"{MY_CATALOG}.{MY_SCHEMA}.credit_decisioning_features")
  
fs.create_table(
    name=f"{MY_CATALOG}.{MY_SCHEMA}.credit_decisioning_features",
    primary_keys=["cust_id"],
    df=feature_df,
    description="クレジット審査のための特徴量")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Next steps
# MAGIC
# MAGIC After creating our features and storing them in the Databricks Feature Store, we can now proceed to the [03.2-AutoML-credit-decisioning]($./03.2-AutoML-credit-decisioning) and build out credit decisioning model.
