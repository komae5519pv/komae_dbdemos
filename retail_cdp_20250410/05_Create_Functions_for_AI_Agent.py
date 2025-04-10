# Databricks notebook source
# MAGIC %md
# MAGIC # AI エージェントが使うツール（関数）を作成します
# MAGIC - クラスタはDBR 16.0 ML以降で実行してください

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. エリア、店舗ごとの販売ランキング
# MAGIC 概要：店舗エリア、店舗ごとの売上ランキングを表示（販売点数で降順）  
# MAGIC 関数：`get_store_sales_ranking()`  
# MAGIC 入力例：店舗エリア、店舗の売上ランキングを表示

# COMMAND ----------

# DBTITLE 1,関数作成
spark.sql(f"""
CREATE OR REPLACE FUNCTION {MY_CATALOG}.{MY_SCHEMA}.get_store_sales_ranking(
  limit_rows INT DEFAULT 10
)
  RETURNS TABLE
  READS SQL DATA
  SQL SECURITY DEFINER
COMMENT '店舗エリア、店舗ごとの売上ランキングを表示（売上金額で降順）'
RETURN
    SELECT
      store_area,                                                                 -- 店舗エリア
      store_name,                                                                 -- 店舗名
      address,                                                                    -- 住所
      total_sales,                                                                -- 売上金額
      total_quantity,                                                             -- 販売点数
      uq_user_cnt,                                                                -- 購買顧客uu数
      avg_purchases_per_user                                                      -- 1人あたりの平均購買回数
    FROM (
        SELECT
          store_area,
          store_name,
          address,
          SUM(subtotal) AS total_sales,
          SUM(quantity) AS total_quantity,
          COUNT(DISTINCT user_id) AS uq_user_cnt,
          COUNT(order_item_id) / COUNT(DISTINCT user_id) AS avg_purchases_per_user,
          ROW_NUMBER() OVER (ORDER BY SUM(subtotal) DESC) AS row_num
        FROM
            {MY_CATALOG}.{MY_SCHEMA}.silver_pos_orders_items
        WHERE
            quantity IS NOT NULL
            AND subtotal IS NOT NULL
            AND user_id IS NOT NULL
        GROUP BY 1,2,3
    ) ranked
    WHERE row_num <= limit_rows
    ORDER BY
        total_sales DESC
""")

# COMMAND ----------

# DBTITLE 1,チェック
display(
  spark.sql(f"""
            SELECT * FROM {MY_CATALOG}.{MY_SCHEMA}.get_store_sales_ranking()
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 特定店舗の売れ筋商品ランキング
# MAGIC 概要：特定エリア・店舗の商品別販売数ランキングを表示（販売点数で降順）  
# MAGIC 関数：`get_store_item_sales_ranking()`  
# MAGIC 引数：`p_store_area`: `店舗エリア`、`p_store_name`: `店舗名`  
# MAGIC 入力例：関東地方・東京都品川区店の商品売上ランキング

# COMMAND ----------

# DBTITLE 1,関数作成
spark.sql(f"""
CREATE OR REPLACE FUNCTION {MY_CATALOG}.{MY_SCHEMA}.get_store_item_sales_ranking(
  p_store_area STRING DEFAULT NULL COMMENT "店舗エリア",
  p_store_name STRING DEFAULT NULL COMMENT "店舗名",
  limit_rows INT DEFAULT 10 COMMENT "表示する行数"
)
  RETURNS TABLE
  READS SQL DATA
  SQL SECURITY DEFINER
COMMENT '特定エリア・店舗の商品別売上ランキングを表示（売上金額で降順）'
RETURN
  SELECT
    store_area,                                                    -- 店舗エリア
    store_name,                                                    -- 店舗名
    address,                                                       -- 住所
    item_name,                                                     -- 商品名
    category_name,                                                 -- カテゴリ名
    total_sales,                                                   -- 売上金額
    total_quantity                                                 -- 販売点数
  FROM (
    SELECT
      store_area,
      store_name,
      address,
      item_name,
      category_name,
      SUM(subtotal) AS total_sales,
      SUM(quantity) AS total_quantity,
      ROW_NUMBER() OVER (ORDER BY SUM(subtotal) DESC) AS row_num
    FROM
      {MY_CATALOG}.{MY_SCHEMA}.silver_pos_orders_items
    WHERE
      CASE 
        WHEN p_store_area IS NOT NULL AND p_store_name IS NOT NULL THEN
          store_area = p_store_area AND store_name = p_store_name
        WHEN p_store_area IS NOT NULL THEN
          store_area = p_store_area
        WHEN p_store_name IS NOT NULL THEN
          store_name = p_store_name
        ELSE TRUE
      END
    GROUP BY 1,2,3,4,5
  ) ranked
  WHERE row_num <= limit_rows
  ORDER BY
    total_sales DESC
""")

# COMMAND ----------

display(
  spark.sql(f"""
    SELECT * FROM {MY_CATALOG}.{MY_SCHEMA}.get_store_item_sales_ranking("関東地方", "東京都品川区店")
  """)
)

# COMMAND ----------

# DBTITLE 1,チェック
display(
  spark.sql(f"""
    SELECT * FROM {MY_CATALOG}.{MY_SCHEMA}.get_store_item_sales_ranking(NULL, NULL,50)
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 特定商品の在庫状況を調査
# MAGIC 概要：特定店舗・商品の詳細及び在庫状況  
# MAGIC 関数：`get_store_item_detail_stock()`  
# MAGIC 引数：`p_store_name`: `店舗名`、`p_item_name`: `商品名`  
# MAGIC 入力例：関東地方・東京都品川区店・ブリの在庫状況を表示

# COMMAND ----------

# DBTITLE 1,関数作成
spark.sql(f"""
CREATE OR REPLACE FUNCTION {MY_CATALOG}.{MY_SCHEMA}.get_store_item_detail_stock(
  p_store_name STRING DEFAULT NULL COMMENT "店舗名",
  p_item_name STRING DEFAULT NULL COMMENT "商品名",
  limit_rows INT DEFAULT 10 COMMENT "表示する行数"
)
  RETURNS TABLE
  READS SQL DATA
  SQL SECURITY DEFINER
COMMENT '特定店舗・商品の詳細及び在庫状況を表示（商品IDの降順）'
RETURN
  WITH item_stock AS (
    SELECT 
      store_id,
      item_id,
      quantity,
      inventory_type,
      inventory_date,
      update_reason
    FROM 
      {MY_CATALOG}.{MY_SCHEMA}.silver_inventory_history
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, item_id ORDER BY inventory_date DESC) = 1
    ORDER BY store_id, item_id
  )
  , items_stock AS (
    SELECT
      stk.store_id,
      stk.item_id,
      str.store_area,
      str.store_name,
      str.address,
      itm.category_name,
      itm.item_name,
      itm.price,
      itm.manufacturer_id,
      stk.quantity,
      stk.inventory_type,
      stk.inventory_date
    FROM item_stock stk
    LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_items itm ON stk.item_id = itm.item_id
    LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.silver_stores str ON stk.store_id = str.store_id
    WHERE
      stk.store_id IS NOT NULL AND stk.item_id IS NOT NULL
    ORDER BY 3,4,5
  )
  SELECT
    store_area,
    store_name,
    address,
    category_name,
    item_name,
    price,
    manufacturer_id,
    quantity,
    inventory_type,
    inventory_date
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (ORDER BY store_name, item_name) AS row_num
    FROM items_stock
    WHERE
      (p_store_name IS NULL OR store_name = p_store_name)
      AND (p_item_name IS NULL OR item_name = p_item_name)
  ) ranked
  WHERE row_num <= limit_rows
  ORDER BY store_name, item_name
""")


# COMMAND ----------

display(
  spark.sql(f"""
            -- SELECT * FROM {MY_CATALOG}.{MY_SCHEMA}.get_store_item_detail_stock(NULL, NULL, 1000)
            SELECT * FROM {MY_CATALOG}.{MY_SCHEMA}.get_store_item_detail_stock(NULL, NULL)
  """)
)

# COMMAND ----------

# DBTITLE 1,チェック
display(
  spark.sql(f"""
            SELECT * FROM {MY_CATALOG}.{MY_SCHEMA}.get_store_item_detail_stock("東京都品川区店", "ブリ")
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. AI Agent Playgroundで実行
# MAGIC [Playground](https://adb-984752964297111.11.azuredatabricks.net/ml/playground?o=984752964297111)  
# MAGIC 概要：特定店舗・商品の詳細及び在庫状況  
# MAGIC ツール：
# MAGIC * `get_store_sales_ranking()`　引数：`出力行数`（Option）
# MAGIC * `get_store_item_sales_ranking()`　引数：`店舗エリア`、`店舗名`、`出力行数`（Option）  
# MAGIC * `get_store_item_detail_stock()`　引数：`店舗名`、`商品名`、`出力行数`（Option）  
# MAGIC
# MAGIC システムプロンプト：`あなたはスーパーの売上データの専門家です。質問に対して日本語で回答します。`  
# MAGIC 入力例：`最も売上の多い店舗・商品の在庫状況を教えて`

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/AI_Agent/Playground_1.png' width='1200'/>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/AI_Agent/Playground_2.png' width='1200'/>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/AI_Agent/Playground_3.png' width='1200'/>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/AI_Agent/function_incatalog.png' width='1200'/>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/AI_Agent/tool_selection.png' width='1200'/>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/AI_Agent/system_prompt.png' width='1200'/>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/AI_Agent/user_prompt.png' width='1200'/>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/komae/AI_Agent/AI_Agent_Result.png' width='1200'/>
