# Databricks notebook source
# MAGIC %md
# MAGIC # 機内でディスプレイのコンテンツレコメンドのプッシュ通知を送信
# MAGIC - 過去視聴ログおよび渡航前アンケート回答結果をもとにパーソナライズされた機内エンタメコンテンツを作成し、プッシュ配信します。  
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
dbutils.widgets.text("user_id", "261")
dbutils.widgets.text("flight_id", "JL185")

user_id = dbutils.widgets.get("user_id")
flight_id = dbutils.widgets.get("flight_id")

# COMMAND ----------

import qrcode
from PIL import Image
import io
import base64

# 画像を圧縮してBase64エンコードする関数
def encode_image_to_base64(image_path, quality=50, new_width=300):
    # 画像を開く
    with Image.open(image_path) as img:
        # 解像度を変更（幅をnew_widthに設定）
        width_percent = (new_width / float(img.size[0]))
        new_height = int((float(img.size[1]) * float(width_percent)))
        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)  # ANTIALIAS -> LANCZOS

        # 圧縮してバッファに保存
        buffered = io.BytesIO()
        img.save(buffered, format="PNG", quality=quality)  # qualityを指定して圧縮
        # Base64エンコード
        b64_string = base64.b64encode(buffered.getvalue()).decode('utf-8')
    return f"data:image/png;base64,{b64_string}"

# プッシュ通知カードの関数
def send_push_notification(title, subject, thumb_url, icon_url="",
                           title_size=16, body_size=14):
    """
    title_size … タイトル文字サイズ (px)
    body_size  … 本文文字サイズ (px)
    """
    # 小アイコン（省略可）
    icon_html = (f'<img src="{icon_url}" '
                 f'style="width:{title_size}px; margin-bottom:-3px; margin-right:4px;">'
                 if icon_url else "")

    displayHTML(f"""
    <div style="border-radius:10px; background:#adeaff; padding:10px; width:400px;
                box-shadow:2px 2px 2px #F7f7f7; margin-bottom:3px;
                display:flex; align-items:center; font-family:sans-serif;">

        <!-- 左：サムネイル -->
        <div style="width:30%; padding-right:10px;">
            <img src="{thumb_url}" style="width:100%; border-radius:5px; object-fit:cover;">
        </div>

        <!-- 右：テキスト -->
        <div style="width:70%; padding-left:10px;">
            <!-- タイトル -->
            <div style="padding-bottom:5px; font-size:{title_size}px; font-weight:600;">
                {icon_html}{title}
            </div>
            <!-- 本文 -->
            <div style="font-size:{body_size}px;">
                {subject}
            </div>
        </div>
    </div>""")

# COMMAND ----------

# テーブルからデータ取得（例: 会員ID、便名、画像URLなど）
user_id, flight_id, cat, img_path, total = spark.sql(f"""
    SELECT
           user_id,
           flight_id,
           contents_list.content_category[0]　AS cat,
           contents_list.content_img_url[0] AS img,
           size(contents_list.content_category) AS total
      FROM {MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6
     WHERE user_id = {user_id}
     LIMIT 1
""").first()

# 画像をBase64エンコード
base64_img = encode_image_to_base64(img_path, quality=50, new_width=300)  # 画像のBase64エンコード

# プッシュ通知のタイトル・本文
title   = f"機内エンタメ 厳選 {total} 作品をお届け！"
subject = f"あなた向けの『{cat}』を多数ご用意しています。続きを機内ディスプレイでどうぞ！"

# プッシュ通知表示
send_push_notification(
    title       = title,
    subject     = subject,
    thumb_url   = base64_img,  # Base64エンコードされた画像を通知に使用
    title_size  = 16,
    body_size   = 14
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
FROM {MY_CATALOG}.{MY_SCHEMA}.gd_recom_top6
""").persist()

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

