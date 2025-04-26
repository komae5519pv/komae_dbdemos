# Databricks notebook source
# MAGIC %md
# MAGIC # デモ用に推論対象のでダミーデータセットを作成します
# MAGIC
# MAGIC トレーニングデータセット`silver_train`をもとに推論用のダミーデータセットを作ります。
# MAGIC - トレーニングデータセットの過去分全て
# MAGIC - トレーニングデータセットの最終日+90日間
# MAGIC
# MAGIC をマージしたデータセットを作成し、`silver_inference_input`テーブルを作成します。

# COMMAND ----------

# MAGIC %run ../01_config

# COMMAND ----------

# 入力データテーブルを読み込み、vm=1, item=1に絞り込む
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW input_view AS
    SELECT 
        DATE_FORMAT(ds, 'yyyy-MM-dd') AS ds,
        vm,
        item,
        y
    FROM {MY_CATALOG}.{MY_SCHEMA}.silver_train
    WHERE vm = 1 AND item = 1
""")

# 最終日を取得
max_date = spark.sql("SELECT MAX(ds) as max_date FROM input_view").collect()[0]['max_date']

# 最終日から90日間の日付シーケンスを生成
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW future_dates AS
    SELECT 
        DATE_FORMAT(date_add('{max_date}', seq), 'yyyy-MM-dd') as ds,
        1 as vm,
        1 as item
    FROM (SELECT explode(sequence(1, 90)) as seq)
""")

# 既存のデータと新しい日付範囲を結合
spark.sql(f"""
    CREATE OR REPLACE TABLE {MY_CATALOG}.{MY_SCHEMA}.silver_inference_input AS
    SELECT ds, vm, item, y FROM input_view
    UNION ALL
    SELECT ds, vm, item, CAST(NULL AS DOUBLE) as y FROM future_dates
    ORDER BY ds
""")

inference_input_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.silver_inference_input")

display(inference_input_df)

# COMMAND ----------

# from pyspark.sql.functions import col, max, date_add, lit, expr, explode, sequence, to_date, when
# from pyspark.sql.types import DateType

# # 入力データテーブルをSpark DataFrameとして読み込む
# input_df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.silver_train")

# # 最終日を取得
# max_date = input_df.agg(max("ds")).collect()[0][0]

# # ユニークなvm-item組み合わせを取得
# vm_item_pairs = input_df.select("vm", "item").distinct()

# # 各vm-item組み合わせに対して90日分の日付を生成
# future_dates = vm_item_pairs.withColumn(
#     "future_ds",
#     explode(sequence(
#         date_add(to_date(lit(max_date)), 1),  # 最終日の翌日から開始
#         date_add(to_date(lit(max_date)), 90),
#         expr("interval 1 day")
#     ))
# )

# # 既存のデータと新しい日付範囲を結合
# future_df = (
#     input_df.select("ds", "vm", "item", "y")
#     .union(
#         future_dates.select(
#             col("future_ds").alias("ds"),
#             "vm",
#             "item",
#             lit(None).cast("double").alias("y")  # yはnull（double型として）
#         )
#     )
# )

# # 日付でソート
# future_df = future_df.orderBy("vm", "item", "ds")

# # Deltaテーブルとして保存
# future_df.write.format("delta").mode("overwrite").saveAsTable(f"{MY_CATALOG}.{MY_SCHEMA}.silver_inference_input")

# COMMAND ----------

# テーブル名
table_name = f'{MY_CATALOG}.{MY_SCHEMA}.silver_inference_input'

# テーブルコメント
table_comment = """
`silver_inference_input`テーブルは、自動販売機の需要予測モデルの推論用データセットです。
このテーブルには、過去の販売データと最終日の翌日から90日間の将来データが含まれています。
将来データの販売数は初期値としてnullが設定されています。
"""

# テーブルにコメントを追加
spark.sql(f'COMMENT ON TABLE {table_name} IS "{table_comment}"')

# カラムコメント
column_comments = {
    "ds": "日付（YYYY-MM-DDフォーマット）",
    "vm": "自動販売機ID、整数",
    "item": "商品ID、整数",
    "y": "販売数、DOUBLE型、例 13.0"
}

# 各カラムにコメントを追加
for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE komae_demo_v2.demand_forecast.silver_inference_input
