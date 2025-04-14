# Databricks notebook source
# MAGIC %md
# MAGIC # ファンへキャンペーンのオファーのプッシュ通知を送信
# MAGIC - ファンの購買履歴とスタジアムの座席の位置情報をもとにパーソナライズされた割引オファーを作成し、イベント中の追加販売を促進します。  
# MAGIC - サーバレス or DBR 16.0ML以降

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ファンにキャンペーンのオファーの送信
# MAGIC
# MAGIC これで、全顧客に送るべきアイテムが揃いました！通知をテストしてみましょう。
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-6.png" width="1000px">

# COMMAND ----------

# import IPython
# from IPython.display import display, HTML

# プッシュ通知コンテンツ作成
def send_push_notification(title, subject, item_url, phone_number):
  displayHTML(f"""<div style="border-radius: 10px; background-color: #adeaff; padding: 10px; width: 400px; box-shadow: 2px 2px 2px #F7f7f7; margin-bottom: 3px; display: flex; align-items: center;">
        <div style="width: 30%; padding-right: 10px;">
            <img style="width: 100%; border-radius: 5px;" src="{item_url}"/>
        </div>
        <div style="width: 70%; padding-left: 10px;">
            <div style="padding-bottom: 5px">
                <img style="width: 20px; margin-bottom: -3px" src="https://github.com/komae5519pv/komae_dbdemos/blob/main/product_recommendation_20250411/_images/bell.png?raw=true"/>
                <strong>{title}</strong>
            </div>
            {subject}
        </div>
    </div>""")

# COMMAND ----------

recommendation = spark.sql(f"SELECT * FROM {MY_CATALOG}.{MY_SCHEMA}.gd_final_recommendations WHERE customer_id = 101").collect()[0]

title = "あなたにお得なキャンペーンオファー！"
subject = f"こんにちは！{recommendation['customer_name']}さん! 大好きな{recommendation['item']}を特別価格でご提供いたします。 {recommendation['vendor_name']}で販売されているので、チェックしてみてくださいね!"
item_url = recommendation['item_img_url']
user_cell_phone = recommendation['phone_number']
 
send_push_notification(title, subject, item_url ,user_cell_phone)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. イベントキャンペーンとメトリクスのトラッキング
# MAGIC
# MAGIC イベント中は、すべての指標をリアルタイムで取得し、イベントのKPIをモニターするダッシュボードを構築することができます。
# MAGIC  
# MAGIC ゲーム終了後、プロモーションの成功率を確認し、次のゲームに必要な調整を行うこともできます。
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-7.png" width="1000px">

# COMMAND ----------


## visualisation: Bar, Series groupings: item_purchased, Values: count, Aggregation: SUM

visual_df = spark.sql(f"""
SELECT COUNT(*) AS count, item_purchased FROM (
  SELECT *, CASE WHEN recommended_item_purchased = 1 THEN 'Yes' ELSE 'No' END AS item_purchased
  FROM {MY_CATALOG}.{MY_SCHEMA}.gd_final_recommendations r
  LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.bz_ticket_sales s USING(customer_id)
  LEFT JOIN {MY_CATALOG}.{MY_SCHEMA}.bz_purchase_history p USING(game_id, customer_id)) GROUP BY item_purchased
""")
visual_df.show()
