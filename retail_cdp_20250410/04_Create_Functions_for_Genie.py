# Databricks notebook source
# MAGIC %md
# MAGIC # Genie用の問い合わせ要約関数を作ります
# MAGIC - サーバレス or クラスタはDBR 16.0 ML以降で実行してください

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {MY_CATALOG}.{MY_SCHEMA}.summarize_inquiries()
RETURNS TABLE (
  survey_category STRING,
  survey_summary STRING,
  response_content ARRAY<STRING>,
  positive_score DOUBLE
)
COMMENT '顧客からの問い合わせをカテゴリ別にまとめ、詳細な問い合わせリストと平均ポジティブスコアを作成します'
RETURN
WITH grouped_inquiries AS (
  SELECT 
    survey_category,
    CONCAT_WS(' ', COLLECT_LIST(response_content)) AS response_content_text,
    ARRAY_AGG(DISTINCT response_content) AS response_contents,
    AVG(positive_score) AS avg_positive_score
  FROM {MY_CATALOG}.{MY_SCHEMA}.silver_survey
  GROUP BY survey_category
)
SELECT 
    survey_category,
    ai_query('databricks-claude-3-7-sonnet',
    '次の顧客の問い合わせ内容を簡潔に要約し、50文字程度の簡潔な文章で出力してください。回答は日本語でお願いします。要約結果のみ出力してください（補足は一切不要）。
    カテゴリ: ' || survey_category || ' 問い合わせ内容: ' || response_content_text || '出力例: 店員の接客態度に不満がある',
    failOnError => False).result AS category_summary,
    response_contents,
    avg_positive_score AS positive_score
FROM grouped_inquiries;
""")
