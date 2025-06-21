# Databricks notebook source
# MAGIC %md
# MAGIC [Simulated Australia Sales and Opportunities Data](https://adb-984752964297111.11.azuredatabricks.net/marketplace/consumer/listings/4fa1dec3-8918-43a1-b754-528406040cab?o=984752964297111)からDeltaShareテーブル化を流用

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC # bronzeテーブル作成

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. customers

# COMMAND ----------

# DBTITLE 1,テーブル作成
# # CSV 読み込み
# csv_path = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/raw_data/customers/customers.csv"
# df = (
#     spark.read.format("csv")
#         .option("header", "true")
#         .option("quote", '"')
#         .option("escape", '"')
#         .option("multiLine", "true")
#         .option("inferSchema", "true")
#         .option("overwriteSchema", "true")
#         .load(csv_path)
# )

# # テーブル保存
# df.write.format("delta").mode("overwrite").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.customers")

# print(df.count())
# print(df.columns)
# display(df.limit(100))

# COMMAND ----------

# DBTITLE 1,テーブル設定
# '''
# 変数設定
# '''
# TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.customers' # テーブルパス
# PK_CONSTRAINT_NAME = f'pk_customers'               # 主キー制約名

# '''
# コメント追加
# '''
# # テーブルコメント
# comment = """
# テーブル名：`customers / 顧客の基本情報（匿名化済み）`
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
# CSV 読み込み
csv_path = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/raw_data/orders/orders.csv"
df = (
    spark.read.format("csv")
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("inferSchema", "true")
        .option("overwriteSchema", "true")
        .load(csv_path)
)

# テーブル保存
df.write.format("delta").mode("overwrite").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.orders")

print(df.count())
print(df.columns)
display(df.limit(100))

# COMMAND ----------

# DBTITLE 1,テーブル設定
'''
変数設定
'''
TABLE_PATH = f'{MY_CATALOG}.{MY_SCHEMA}.orders' # テーブルパス
PK_CONSTRAINT_NAME = f'pk_orders'               # 主キー制約名

'''
コメント追加
'''
# テーブルコメント
comment = """
テーブル名：`orders / 商品購入などの注文履歴（顧客・商品・営業との関連）`
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
