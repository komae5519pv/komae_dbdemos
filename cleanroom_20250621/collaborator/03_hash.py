# Databricks notebook source
# MAGIC %md
# MAGIC [Simulated Australia Sales and Opportunities Data](https://adb-984752964297111.11.azuredatabricks.net/marketplace/consumer/listings/4fa1dec3-8918-43a1-b754-528406040cab?o=984752964297111)からDeltaShareテーブル化を流用

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC # ハッシュ化(個人情報)したテーブル作成

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. customers

# COMMAND ----------

# DBTITLE 1,テーブル作成
# from pyspark.sql import functions as F

# # 既存テーブル読み込み
# df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.customers")

# # SHA256でハッシュ化（16進数文字列で出力）
# df = df.withColumn(
#     "customerid_sha256",
#     F.sha2(F.col("customerid").cast("string"), 256)
# )

# df = df.withColumn(
#     "customername_sha256",
#     F.sha2(F.col("customername").cast("string"), 256)
# )

# selected_df = df.select(
#       'customerid_sha256',
#       'customername_sha256',
#       'city',
#       'state'
#       )

# # 新規テーブル作成
# selected_df.write.mode("overwrite") \
#             .format("delta") \
#             .option("overwriteSchema", "true") \
#             .saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.hashed_customers")

# COMMAND ----------

# DBTITLE 1,テーブル設定
# '''
# 変数設定
# '''
# TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.hashed_customers' # テーブルパス
# PK_CONSTRAINT_NAME = f'pk_hashed_customers'               # 主キー制約名

# '''
# コメント追加
# '''
# # テーブルコメント
# comment = """
# テーブル名：`hashed_customers / 顧客の基本情報（匿名化済み）`
# """
# spark.sql(f'COMMENT ON TABLE {TABLE_PATH} IS "{comment}"')

# # カラムコメント
# column_comments = {
#     "customerid_sha256": "顧客ID（SHA256でハッシュ化済）",
#     "customername_sha256": "顧客名（SHA256でハッシュ化済）",
#     "city": "顧客が所在する都市名、例）Sydney",
#     "state": "顧客が所在する州名、例）NSW"
# }

# for column, comment in column_comments.items():
#     escaped_comment = comment.replace("'", "\\'")
#     sql_query = f"ALTER TABLE {TABLE_PATH} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
#     spark.sql(sql_query)

# '''
# NOT NULL制約の追加
# '''
# columns_to_set_not_null = [
#   'customerid_sha256',
# ]

# for column in columns_to_set_not_null:
#     spark.sql(f"""
#     ALTER TABLE {TABLE_PATH}
#     ALTER COLUMN {column} SET NOT NULL;
#     """)

# # 主キー設定
# spark.sql(f'''
# ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
# ''')

# spark.sql(f'''
# ALTER TABLE {TABLE_PATH}
# ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (
#   customerid_sha256
# );
# ''')

# # # チェック
# # display(
# #     spark.sql(f'''
# #     DESCRIBE EXTENDED {TABLE_PATH}
# #     '''))

# '''
# 認定済みタグの追加
# '''
# certified_tag = 'system.Certified'

# try:
#     spark.sql(f"ALTER TABLE {TABLE_PATH} SET TAGS ('{certified_tag}')")
#     print(f"認定済みタグ '{certified_tag}' の追加が完了しました。")

# except Exception as e:
#     print(f"認定済みタグ '{certified_tag}' の追加中にエラーが発生しました: {str(e)}")
#     print("このエラーはタグ機能に対応していないワークスペースで実行した場合に発生する可能性があります。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. orders

# COMMAND ----------

# DBTITLE 1,テーブル作成
from pyspark.sql import functions as F

# 既存テーブル読み込み
df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.orders")

# SHA256でハッシュ化（16進数文字列で出力）
df = df.withColumn(
    "orderid_sha256",
    F.sha2(F.col("orderid").cast("string"), 256)
)

df = df.withColumn(
    "customerid_sha256",
    F.sha2(F.col("customerid").cast("string"), 256)
)

df = df.withColumn(
    "productid_sha256",
    F.sha2(F.col("productid").cast("string"), 256)
)

selected_df = df.select(
      'orderid_sha256',
      'customerid_sha256',
      'productid_sha256',
      'orderdate',
      'quantity',
      'orderamt',
      'salesrep'
      )

# 新規テーブル作成
selected_df.write.mode("overwrite") \
            .format("delta") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.hashed_orders")

# COMMAND ----------

# DBTITLE 1,テーブル設定
'''
変数設定
'''
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.hashed_orders' # テーブルパス
PK_CONSTRAINT_NAME = f'pk_hashed_orders'               # 主キー制約名

'''
コメント追加
'''
# テーブルコメント
comment = """
テーブル名：`hashed_orders / 商品購入などの注文履歴（顧客・商品・営業との関連）`
"""
spark.sql(f'COMMENT ON TABLE {TABLE_PATH} IS "{comment}"')

# カラムコメント
column_comments = {
    "orderid_sha256": "注文ID（SHA256でハッシュ化済）",
    "customerid_sha256": "顧客ID（SHA256でハッシュ化済）",
    "productid_sha256": "商品ID（SHA256でハッシュ化済）",
    "orderdate": "注文日、YYYY-MM-DDフォーマット",
    "quantity": "購入数量、例）16",
    "orderamt": "注文金額、例）1012.48",
    "salesrep": "担当営業名、例）Ravi"
}

for column, comment in column_comments.items():
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {TABLE_PATH} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

'''
NOT NULL制約の追加
'''
columns_to_set_not_null = [
  'orderid_sha256',
]

for column in columns_to_set_not_null:
    spark.sql(f"""
    ALTER TABLE {TABLE_PATH}
    ALTER COLUMN {column} SET NOT NULL;
    """)

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (
  orderid_sha256
);
''')

# # チェック
# display(
#     spark.sql(f'''
#     DESCRIBE EXTENDED {TABLE_PATH}
#     '''))

'''
認定済みタグの追加
'''
certified_tag = 'system.Certified'

try:
    spark.sql(f"ALTER TABLE {TABLE_PATH} SET TAGS ('{certified_tag}')")
    print(f"認定済みタグ '{certified_tag}' の追加が完了しました。")

except Exception as e:
    print(f"認定済みタグ '{certified_tag}' の追加中にエラーが発生しました: {str(e)}")
    print("このエラーはタグ機能に対応していないワークスペースで実行した場合に発生する可能性があります。")
