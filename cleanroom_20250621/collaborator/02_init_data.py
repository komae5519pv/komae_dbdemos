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
