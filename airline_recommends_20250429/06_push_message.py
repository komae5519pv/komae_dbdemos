# Databricks notebook source
# MAGIC %md
# MAGIC # プッシュメッセージの表示イメージを試す
# MAGIC - 過去視聴ログおよび渡航前アンケート回答結果をもとにパーソナライズされた機内エンタメコンテンツを作成し、プッシュコンテンツを表示します。
# MAGIC   - 実際の配信は行いませんが、メッセージの見た目を確認します。
# MAGIC - サーバレス or DBR 16.0ML以降

# COMMAND ----------

# MAGIC %pip install qrcode[pil]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 機内エンタメコンテンツのプッシュ送信
# MAGIC これで、全顧客に送るべきコンテンツが揃いました！通知をテストしてみましょう。

# COMMAND ----------

# DBTITLE 1,テストデータ
dbutils.widgets.text("user_id", "112")
user_id = dbutils.widgets.get("user_id")

# COMMAND ----------

import base64

# ── プッシュ通知カード描画 ─────────────────────────
def send_push_notification(title, subject, thumb_b64,
                           icon_url="", title_size=16, body_size=14):
    icon_html = (f'<img src="{icon_url}" '
                 f'style="width:{title_size}px; margin-bottom:-3px; margin-right:4px;">'
                 if icon_url else "")
    displayHTML(f"""
    <div style="border-radius:10px; background:#adeaff; padding:10px; width:400px;
                box-shadow:2px 2px 2px #F7f7f7; margin-bottom:3px;
                display:flex; align-items:center; font-family:sans-serif;">

        <!-- 左：サムネイル -->
        <div style="width:30%; padding-right:10px;">
            <img src="data:image/png;base64,{thumb_b64}"
                 style="width:100%; border-radius:5px; object-fit:cover;">
        </div>

        <!-- 右：テキスト -->
        <div style="width:70%; padding-left:10px;">
            <div style="padding-bottom:5px; font-size:{title_size}px; font-weight:600;">
                {icon_html}{title}
            </div>
            <div style="font-size:{body_size}px;">
                {subject}
            </div>
        </div>
    </div>""")

# ── 新テーブルから必要列を取得 ─────────────────────
row = spark.sql(f"""
    SELECT
        user_id,
        flight_id,
        contents_list.content_category[0]  AS cat,
        contents_list.content_img_b64[0]   AS img_b64,
        size(contents_list.content_category) AS total
    FROM   {MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6_bs64
    WHERE  user_id = {user_id}
    LIMIT 1
""").first()

# 取得値
user_id, flight_id, cat, img_b64, total = row

# プッシュ通知文面
title   = f"機内エンタメ 厳選 {total} 作品をお届け！⭐️"
subject = f"あなた向けの『{cat}』を多数ご用意しています。続きを機内ディスプレイでどうぞ！🚀"

# 表示
send_push_notification(
    title       = title,
    subject     = subject,
    thumb_b64   = img_b64,   # base64をそのまま渡す
    title_size  = 15,
    body_size   = 13
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. QRコード画像を表示
# MAGIC プッシュ送信後、機内ディスプレイでスキャンしてレコメンドコンテンツ一覧を表示します。  
# MAGIC 顧客が機内ディスプレイにQRコードをスキャンすると、Model Serving Endpointにリクエストし、API経由でレコメンドリストを取得・画面に表示します。  
# MAGIC ここでは、そのためのQRコード（会員ID、フライトID）の画像をVolumeに保存します。

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from IPython.display import HTML
import qrcode
import os
import base64
import glob

# Volumeパスの設定
volume_path = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_QR}/"

def cleanup_volume():
    """Volume内の既存PNGファイルを全削除"""
    try:
        files = glob.glob(os.path.join(volume_path, "*.png"))
        for f in files:
            os.remove(f)
            print(f"削除完了: {f}")
        print(f"合計{len(files)}件のファイルを削除")
    except Exception as e:
        print(f"クリーンアップエラー: {str(e)}")
        raise

# 既存ファイルの削除実行
cleanup_volume()

# Unity Catalogからデータ取得
df = spark.sql(f"""
SELECT user_id, flight_id 
FROM {MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6_bs64
""")

# QRコード生成関数（UDF用）
def generate_qr_udf(user_id, flight_id):
    try:
        url = f"https://yourapp.com/qr?member_id={user_id}&flight_id={flight_id}"
        qr = qrcode.make(url, box_size=10, border=4)
        
        file_name = f"qr_{user_id}_{flight_id}.png"
        img_path = os.path.join(volume_path, file_name)
        
        qr.save(img_path)
        return img_path
    except Exception as e:
        print(f"QR生成エラー {user_id}-{flight_id}: {str(e)}")
        return None

# UDF登録と処理実行
generate_qr_udf_spark = F.udf(generate_qr_udf, StringType())
result_df = df.withColumn("qr_path", generate_qr_udf_spark("user_id", "flight_id"))

# 処理結果確認
display(result_df.select("user_id", "flight_id", "qr_path"))

# HTML表示関数（変更なし）
def display_qr_html(row):
    img_path = row["qr_path"]
    if img_path and os.path.exists(img_path):
        with open(img_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
            display(HTML(f"""
                <div style="border:2px solid #eee;padding:10px;margin:10px">
                    <h4>会員ID: {row['user_id']} | 便名ID: {row['flight_id']}</h4>
                    <img src="data:image/png;base64,{b64}" style="width:200px;height:200px"/>
                </div>
            """))

# 結果表示（最初の10件）
for row in result_df.limit(10).collect():
    display_qr_html(row)

