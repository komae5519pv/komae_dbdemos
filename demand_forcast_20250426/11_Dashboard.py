# Databricks notebook source
# MAGIC %md
# MAGIC # 自動販売機の需要予測・分析ダッシュボードを作成
# MAGIC ## やること
# MAGIC - 分析用マートを作ってダッシュボードに活用します
# MAGIC - [テーブル定義書](https://docs.google.com/spreadsheets/d/10wdoTxlAGcD5gHjY4_upPYKd1gt4rEupKLSgP5q4uWI/edit?gid=1392031218#gid=1392031218)に基づくテーブルを作成してます
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/dashboard.png?raw=true' width='1200'/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 在庫最適化のための分析ダッシュボード
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/03_BI%20Dashboard_Inventory%20Optimization.png?raw=true' width='1200'/>
# MAGIC
# MAGIC
# MAGIC ### 自動販売機の在庫状況マッピング
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/03_BI%20Dashboard_map.png?raw=true' width='1200'/>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 手順
# MAGIC - 1. `_dashbord/`配下にあるJSONファイルをローカルPCにエクスポートしてください
# MAGIC   - `Vending Machine Analysis_EN.lvdash.json`は英語バージョンです
# MAGIC   - `Vending Machine Analysis_JP.lvdash.json`は日本語バージョンです
# MAGIC - 2. Databricks UI ナビゲーションのダッシュボードで、画面右上から`Create dashboard > Import dashboard from file`をクリック
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/Import%20Dashboard%20JSON_1.png?raw=true' width='1200'/>
# MAGIC
# MAGIC - 3. ポップアップ`Choose file`から、1でエクスポートしたJSONファイルを選択してインポート
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/Import%20Dashboard%20JSON_2.png?raw=true' width='1200'/>
# MAGIC
# MAGIC - 4. ダッシュボード編集画面の`Data`タブからSQLクエリを修正します
# MAGIC   - DataタブからSQLを確認します
# MAGIC   - 全てのSQLを対象に、FROM句で指定されるカタログを修正してください
# MAGIC   - `01_config`で指定したカタログ名を指定してください
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/fine_grain_forecast_20241013/Customized_e2e_demand_forecasting/_image_for_notebook/Dashboard%20Fix%20catalog%20in%20SQL.png?raw=true' width='1200'/>
