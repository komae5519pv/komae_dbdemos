# Databricks notebook source
# MAGIC %md
# MAGIC # サンプルデータでクエリ関数作る for Genie
# MAGIC
# MAGIC やること：
# MAGIC - [サンプルデータセット](https://qiita.com/taka_yayoi/items/3f8ccad13c6efd242be1#retail-org)をロードして、テーブル`bronze_online_retail`を作成します
# MAGIC - テーブル`bronze_online_retail`をクレンジングしたテーブル`silver_online_retail`を作ります
# MAGIC - テーブル`silver_online_retail`を参照する関数を作ります（CREATE FUNCTION）
# MAGIC   - 顧客ごとの購入行動分析：`get_customer_purchases_summary`
# MAGIC   - 商品別売上：`get_product_sales_summary`
# MAGIC   - 顧客セグメンテーション：`get_customer_segment`
# MAGIC   - 時間軸での購買トレンド分析：`get_sales_by_time_summary`
# MAGIC
# MAGIC 下記構成のカタログを作ります
# MAGIC ```
# MAGIC /<catalog_name>                             <- カタログ
# MAGIC ├── sample_data                             <- スキーマ
# MAGIC │   ├── bronze_online_retail                <- テーブル
# MAGIC │   ├── silver_online_retail                <- テーブル
# MAGIC │   ├── get_customer_purchases_summary      <- 関数 for Genie（戻り値：表形式）
# MAGIC │   ├── get_product_sales_summary           <- 関数 for Genie（戻り値：表形式）
# MAGIC │   ├── get_customer_segment                <- 関数 for Genie（戻り値：表形式）
# MAGIC │   ├── get_sales_by_time_summary           <- 関数 for Genie（戻り値：表形式）
# MAGIC │   ├── transform_date                      <- 関数（戻り値：TIMESTAMP）※ 表形式じゃないのでGenieで使えません
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01. カタログ・スキーマ作成

# COMMAND ----------

# カタログ、スキーマ
MY_CATALOG = "komae_demo"        # 任意のカタログ名に変更してください
MY_SCHEMA = "sample_for_genie"

# COMMAND ----------

# カタログ、スキーマ作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA};")

# デフォルトのカタログ、スキーマを設定
spark.sql(f"USE CATALOG {MY_CATALOG};")
spark.sql(f"USE SCHEMA {MY_SCHEMA};")

print(f"カタログ: {MY_CATALOG}")
print(f"スキーマ: {MY_CATALOG}.{MY_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02. bzonseテーブル作成

# COMMAND ----------

# データのロード
df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("dbfs:/databricks-datasets/online_retail/data-001/data.csv")

# テーブルとして保存
df.write.mode("overwrite").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.bronze_online_retail")

# データの表示
# display(df)
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03. silverテーブル作成

# COMMAND ----------

# DBTITLE 1,テーブル作成
# MAGIC %sql
# MAGIC -- 日付型変換用の関数を作成
# MAGIC CREATE OR REPLACE FUNCTION transform_date(InvoiceDate STRING) RETURNS TIMESTAMP
# MAGIC RETURN TO_TIMESTAMP(
# MAGIC     CONCAT(
# MAGIC         LPAD(SPLIT(InvoiceDate, '/')[0], 2, '0'), '/',                -- 月を2桁に変換
# MAGIC         LPAD(SPLIT(InvoiceDate, '/')[1], 2, '0'), '/',                -- 日を2桁に変換
# MAGIC         SPLIT(SPLIT(InvoiceDate, ' ')[0], '/')[2], ' ',               -- 年をそのまま使用
# MAGIC         LPAD(SPLIT(SPLIT(InvoiceDate, ' ')[1], ':')[0], 2, '0'), ':', -- 時を2桁に変換
# MAGIC         SPLIT(SPLIT(InvoiceDate, ' ')[1], ':')[1]                     -- 分をそのまま使用
# MAGIC     ),
# MAGIC     'MM/dd/yy HH:mm'
# MAGIC );
# MAGIC
# MAGIC -- データをDATEとTIMEに分けて保存
# MAGIC CREATE OR REPLACE TABLE silver_online_retail AS
# MAGIC SELECT
# MAGIC     InvoiceNo AS OrderNo,                                               -- 注文番号
# MAGIC     StockCode,                                                          -- 商品コード
# MAGIC     Description,                                                        -- 商品説明
# MAGIC     CAST(Quantity AS INT) AS Quantity,                                  -- 商品点数
# MAGIC     TO_DATE(transform_date(InvoiceDate)) AS OrderDate,                  -- 注文日付（yyyy-mm-dd）
# MAGIC     DATE_FORMAT(transform_date(InvoiceDate), 'HH:mm') AS OrderTime,     -- 注文時間（HH:ss）
# MAGIC     LPAD(
# MAGIC       HOUR(DATE_FORMAT(transform_date(InvoiceDate), 'HH:mm'))
# MAGIC       , 2 , '0') AS OrderHour,                                          -- 注文時間帯（HH）
# MAGIC     CAST(UnitPrice AS DOUBLE) AS UnitPrice,                             -- 商品単価
# MAGIC     CustomerID,                                                         -- 顧客ID
# MAGIC     Country                                                             -- 国
# MAGIC FROM
# MAGIC     bronze_online_retail
# MAGIC WHERE
# MAGIC     CustomerID IS NOT NULL;
# MAGIC
# MAGIC SELECT * FROM silver_online_retail LIMIT 10;

# COMMAND ----------

# DBTITLE 1,テーブル説明・カラムコメントを追加
# テーブル名
table_name = 'silver_online_retail'

# テーブルコメント
comment = """
`silver_online_retail` テーブルは、オンライン小売取引に関連するデータを含んでいます。このテーブルには、注文番号、商品コード、商品の説明、注文数量、注文日と時間、注文時間帯、単価、顧客ID、国といった情報が含まれています。このテーブルは、販売実績、顧客の行動、オンライン小売業の国際的な広がりを理解するために重要です。このテーブルのデータは、人気商品の特定、時間を通じた販売傾向の分析、国別に顧客の好みを理解するためのさまざまな分析に活用できます。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
    "OrderNo": "文字列、ユニーク(主キー)、注文番号、例: '536365'",
    "StockCode": "文字列、商品コード（外部キー）、例: '85123A'",
    "Description": "文字列、商品説明、例: WHITE METAL LANTERN",
    "Quantity": "整数、商品点数",
    "OrderDate": "日付、YYYY-MM-DD、注文日付",
    "OrderTime": "文字列、hh:ss、注文時間",
    "OrderHour": "文字列、hh、注文時間帯",
    "UnitPrice": "浮動小数点、商品単価",
    "CustomerID": "整数、顧客ID（外部キー）、例: 17850",
    "Country": "文字列、国、例: 'United Kingdom'"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04. 分析クエリ

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. 顧客ごとの購入行動分析
# MAGIC
# MAGIC 関数を作成する：`summarize_customer_purchases`

# COMMAND ----------

# DBTITLE 1,関数作成
# MAGIC %sql
# MAGIC -- 顧客ごとの購入情報を集計する関数を作成
# MAGIC CREATE OR REPLACE FUNCTION get_customer_purchases_summary()
# MAGIC RETURNS TABLE (
# MAGIC   CustomerID STRING,
# MAGIC   StockCode STRING,
# MAGIC   total_quantity_purchased INT,
# MAGIC   total_spent DOUBLE
# MAGIC )
# MAGIC RETURN
# MAGIC SELECT
# MAGIC   CustomerID,                                   -- 顧客ID
# MAGIC   StockCode,                                    -- 商品コード
# MAGIC   SUM(Quantity) AS total_quantity_purchased,    -- 商品点数
# MAGIC   SUM(Quantity * UnitPrice) AS total_spent      -- 注文金額
# MAGIC FROM
# MAGIC   silver_online_retail
# MAGIC GROUP BY
# MAGIC   CustomerID, StockCode
# MAGIC ORDER BY
# MAGIC   CustomerID, total_spent DESC;

# COMMAND ----------

# DBTITLE 1,関数削除
# %sql
# -- 関数を削除
# DROP FUNCTION IF EXISTS get_customer_purchases_summary;

# COMMAND ----------

# DBTITLE 1,関数実行
# MAGIC %sql
# MAGIC -- 関数を呼び出して結果を取得
# MAGIC SELECT * FROM get_customer_purchases_summary();

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. 商品別売上分析
# MAGIC
# MAGIC 関数を作成する：`summarize_product_sales`

# COMMAND ----------

# DBTITLE 1,関数作成
# MAGIC %sql
# MAGIC -- 商品ごとの販売情報を集計する関数を作成
# MAGIC CREATE OR REPLACE FUNCTION get_product_sales_summary()
# MAGIC RETURNS TABLE (
# MAGIC   StockCode STRING,
# MAGIC   total_quantity_sold INT,
# MAGIC   total_revenue DOUBLE,
# MAGIC   avg_unit_price DOUBLE,
# MAGIC   distinct_customers INT,
# MAGIC   total_invoices INT
# MAGIC )
# MAGIC RETURN
# MAGIC SELECT
# MAGIC     StockCode,                                        -- 商品コード
# MAGIC     SUM(Quantity) AS total_quantity_sold,             -- 商品の販売点数
# MAGIC     SUM(Quantity * UnitPrice) AS total_revenue,       -- 商品の総売上
# MAGIC     AVG(UnitPrice) AS avg_unit_price,                 -- 商品の平均単価
# MAGIC     COUNT(DISTINCT CustomerID) AS distinct_customers, -- 商品を購入した異なる顧客数
# MAGIC     COUNT(DISTINCT InvoiceNo) AS total_invoices       -- 商品が含まれた注文回数
# MAGIC FROM
# MAGIC     silver_online_retail
# MAGIC GROUP BY
# MAGIC     StockCode
# MAGIC ORDER BY
# MAGIC     total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,関数削除
# %sql
# -- 関数を削除
# DROP FUNCTION IF EXISTS get_product_sales_summary;

# COMMAND ----------

# DBTITLE 1,関数実行
# MAGIC %sql
# MAGIC -- 関数を呼び出して結果を確認
# MAGIC SELECT * FROM get_product_sales_summary();

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. 顧客セグメンテーション
# MAGIC
# MAGIC 関数を作成する：`get_customer_segment`

# COMMAND ----------

# DBTITLE 1,関数作成
# MAGIC %sql
# MAGIC -- 顧客の支出額とセグメントを計算する関数を作成
# MAGIC CREATE OR REPLACE FUNCTION get_customer_segment()
# MAGIC RETURNS TABLE (
# MAGIC   CustomerID STRING,
# MAGIC   total_spent DOUBLE,
# MAGIC   customer_segment STRING
# MAGIC )
# MAGIC RETURN
# MAGIC WITH customer_spending AS (
# MAGIC     SELECT
# MAGIC         CustomerID,                               -- 顧客ID
# MAGIC         SUM(Quantity * UnitPrice) AS total_spent  -- 顧客の総支出額
# MAGIC     FROM
# MAGIC         silver_online_retail
# MAGIC     GROUP BY
# MAGIC         CustomerID
# MAGIC )
# MAGIC SELECT
# MAGIC     CustomerID,                                   -- 顧客ID
# MAGIC     total_spent,                                  -- 顧客の総支出額
# MAGIC     CASE
# MAGIC         WHEN total_spent > 1000 THEN '高価格層' 
# MAGIC         WHEN total_spent BETWEEN 500 AND 1000 THEN '中価格層'
# MAGIC         ELSE '低価格層'
# MAGIC     END AS customer_segment                       -- 顧客セグメント
# MAGIC FROM
# MAGIC     customer_spending
# MAGIC ORDER BY
# MAGIC     total_spent DESC;

# COMMAND ----------

# DBTITLE 1,関数削除
# %sql
# -- 関数を削除
# DROP FUNCTION IF EXISTS get_customer_segment;

# COMMAND ----------

# DBTITLE 1,関数実行
# MAGIC %sql
# MAGIC -- 関数を呼び出して結果を取得
# MAGIC SELECT * FROM get_customer_segment();

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. 時間軸での購買トレンド分析

# COMMAND ----------

# DBTITLE 1,関数作成
# MAGIC %sql
# MAGIC -- 日付と時間帯ごとの購入数量と売上金額を集計する関数を作成
# MAGIC CREATE OR REPLACE FUNCTION get_sales_by_time_summary()
# MAGIC RETURNS TABLE (
# MAGIC   purchase_date DATE,
# MAGIC   purchase_hour STRING,
# MAGIC   total_quantity INT,
# MAGIC   total_revenue DOUBLE
# MAGIC )
# MAGIC RETURN
# MAGIC SELECT
# MAGIC     OrderDate AS purchase_date,                 -- 注文日付
# MAGIC     OrderHour AS purchase_hour,                 -- 注文時間帯
# MAGIC     SUM(Quantity) AS total_quantity,            -- 時間帯ごとに購入された商品点数
# MAGIC     SUM(Quantity * UnitPrice) AS total_revenue  -- 時間帯ごとの売上金額
# MAGIC FROM
# MAGIC     silver_online_retail
# MAGIC GROUP BY
# MAGIC     OrderDate, OrderHour
# MAGIC ORDER BY
# MAGIC     purchase_date, purchase_hour;
# MAGIC

# COMMAND ----------

# DBTITLE 1,関数削除
# %sql
# -- summarize_sales_by_time 関数を削除
# DROP FUNCTION IF EXISTS get_sales_by_time_summary;

# COMMAND ----------

# DBTITLE 1,関数実行
# MAGIC %sql
# MAGIC -- 関数を呼び出して結果を確認
# MAGIC SELECT * FROM get_sales_by_time_summary();
