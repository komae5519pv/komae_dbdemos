# Databricks notebook source
# MAGIC %md # 07 Create Alert & Dashboard 
# MAGIC
# MAGIC 最後に予測結果を含めたデータの可視化に取り組んでみたいと思います。
# MAGIC
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/7_dbsql.png' width='800' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/7_dbsql.png?raw=true' width='1200' />

# COMMAND ----------

# MAGIC %md ## ダッシュボード作成
# MAGIC
# MAGIC 上記の条件に当てはまる人をすぐに確認できるようにダッシュボードを作成しておきます。アラート通知が来た後や定期的に顧客チェックをするのに役立ちます。
# MAGIC ダッシュボードには以下のような機能があります。
# MAGIC - 共有機能
# MAGIC - PDF出力
# MAGIC - subscriptionによる定期的なダッシュボードイメージのメール配信

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/workshop2022/20220623-quickstart-workshop/sql2.png' width='1000' > </img> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/sql2.png?raw=true' width='1000' > </img>
# MAGIC
# MAGIC
# MAGIC <table>
# MAGIC     <tr>
# MAGIC     <td>
# MAGIC       Databricksメニューの切り替え<br>　（別タブで開くと、画面が切り替わらないため効率的です）
# MAGIC     </td>
# MAGIC     <td>
# MAGIC       <!-- <img src='https://sajpstorage.blob.core.windows.net/workshop2022/20220623-quickstart-workshop/sql1.png' width='600' /> -->
# MAGIC       <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/sql1.png?raw=true' width='600' />
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   
# MAGIC   <tr>
# MAGIC     <td>
# MAGIC       エンドポイントの起動
# MAGIC     </td>
# MAGIC     <td>
# MAGIC       <!-- <img src='https://sajpstorage.blob.core.windows.net/workshop2022/20220623-quickstart-workshop/sql3.png' width='700' /> -->
# MAGIC       <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/sql3.png?raw=true' width='700' />
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   
# MAGIC   <tr>
# MAGIC     <td>
# MAGIC       1.「データ」 <br> 2.「ご自分の作成したスキーマ化のテーブル(churn_prediction)を選択」 <br> 3.「Query」<br>
# MAGIC     </td>
# MAGIC     <td>
# MAGIC       <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/Demo/image/data_query.png' width='700' /> -->
# MAGIC       <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/data_query.png?raw=true' width='700' />
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC
# MAGIC </table>

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 可視化の追加
# MAGIC
# MAGIC ### table  
# MAGIC predictionを先頭に<br>
# MAGIC Conditionで、1.0 の時に赤字にする<br>
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/Demo/image/table_dashboard.png' width='800' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/table_dashboard.png?raw=true' width='800' />
# MAGIC
# MAGIC ### Filter をつける
# MAGIC 「フィルタ追加」 - prediction / single select<br>
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/Demo/image/filter_dashboard.png' width='200' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/filter_dashboard.png?raw=true' width='200' />
# MAGIC
# MAGIC ### Boxplot
# MAGIC
# MAGIC X Column: `PaymentMethod` , Y Column: `MonthlyCharges`  <br>
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/Demo/image/boxplot.png' width='800' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/boxplot.png?raw=true' width='800' />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <table>
# MAGIC     <tr>
# MAGIC     <td>
# MAGIC       クエリーの保存 <br>
# MAGIC       <br>
# MAGIC       クエリー名：　name_workshop (任意)
# MAGIC     </td>
# MAGIC     <td>
# MAGIC       <!-- <img src='https://sajpstorage.blob.core.windows.net/workshop2022/20220623-quickstart-workshop/sql6.png' width='600' /> -->
# MAGIC       <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/imgs/sql6.png?raw=true' width='600' />
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   
# MAGIC   <tr>
# MAGIC     <td>
# MAGIC       Step3: ダッシュボードの作成 <br>
# MAGIC       <br>
# MAGIC       ダッシュボード名： name_dashboard (任意)
# MAGIC     </td>
# MAGIC     <td>
# MAGIC       <!-- <img src='https://sajpstorage.blob.core.windows.net/workshop2022/20220623-quickstart-workshop/sql7.png' width='600' /> -->
# MAGIC       <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/imgs/sql7.png?raw=true' width='600' />
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   
# MAGIC   <tr>
# MAGIC     <td>
# MAGIC       パネルの追加 <br>
# MAGIC     </td>
# MAGIC     <td>
# MAGIC       <!-- <img src='https://sajpstorage.blob.core.windows.net/workshop2022/20220623-quickstart-workshop/sql8.png' width='600' /> -->
# MAGIC       <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/imgs/sql8.png?raw=true' width='600' />
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC
# MAGIC </table>
# MAGIC
# MAGIC ### ダッシュボードの完成
# MAGIC
# MAGIC パネルのサイズや配置を変えて完成させよう! 
# MAGIC
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/Demo/image/ml_dashboard.png' width='1000' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/imgs/ml_dashboard.png?raw=true' width='1000' />

# COMMAND ----------

# MAGIC %md ## 1. アラート作成

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Queryを作成して、Accuracyが 80% を下回ったらアラートを上げるようにします。
# MAGIC
# MAGIC まずは、クエリー(\<name>_alert_query)を作成
# MAGIC
# MAGIC ```
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM churn_prediction WHERE CAST(prediction as INT) = churn) / count(*) over() as Accuracy
# MAGIC FROM
# MAGIC   churn_prediction
# MAGIC LIMIT 1
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC `Alert`メニューに移動し、新規のアラート(\<name>_alert)を作成します。<br>
# MAGIC Trigerは、accuracy < 80 にします。<br>
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/Demo/image/alert.png' width='800' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/alert.png?raw=true' width='800' />
# MAGIC
# MAGIC アラートの通知先は以下の中から選択できます。今回はメールと Slackを設定　<br>（別途Slack Webhook設定などが必要になります）<br>
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/notification.png' width='400' /> -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/notification.png?raw=true' width='400' />
# MAGIC
# MAGIC デフォルトのテンプレートの場合、次のような通知が届きます。
# MAGIC <!-- <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/triger.png' width='400' />  -->
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/triger.png?raw=true' width='400' /> 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ハンズオン環境では、以下のダッシュボードにアクセスできません。（For Demo) <br>
# MAGIC <a href="https://e2-demo-tokyo.cloud.databricks.com/sql/dashboards/db958c05-b74a-45d6-ad81-e1189937d2fe-customerchurn-dashboard?o=5099015744649857" target="_blank">デモ用ダッシュボード</a>	
