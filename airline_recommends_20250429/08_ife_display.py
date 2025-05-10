# Databricks notebook source
# MAGIC %md
# MAGIC # 機内コンテンツのレコメンド一覧を表示してみる
# MAGIC - Feature Serving エンドポイントにリクエストして機内コンテンツを表示してみます。
# MAGIC - サーバレス or DBR 16.0ML以降

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. フロントエンド表示サンプルを作ってみる
# MAGIC 機内エンタメのコンテンツレコメンドを機内ディスプレイに表示した場合のイメージを見てみましょう！  
# MAGIC まず、REST APIを介してFeature Servingエンドポイントをクエリします。この時、主キーを指定します。  
# MAGIC [公式Docs](https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/feature-store/feature-serving-tutorial#step-3-create-a-function-in-unity-catalog)

# COMMAND ----------

# DBTITLE 1,テストデータ
dbutils.widgets.text("user_id", "298")
dbutils.widgets.text("flight_id", "NH872")

user_id = dbutils.widgets.get("user_id")
flight_id = dbutils.widgets.get("flight_id")

# COMMAND ----------

import mlflow.deployments
import pandas as pd

client = mlflow.deployments.get_deploy_client("databricks")

# テストデータ準備
test_data = pd.DataFrame([{"user_id": user_id, "flight_id": flight_id}])

# クエリ実行
response = client.predict(
    endpoint="get-airline-recommendations",
    inputs={
        "dataframe_records": test_data.to_dict(orient="records")
    }
)

print(type(response))
print(response)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 取得データを確認してみる

# COMMAND ----------

import json, pprint
pprint.pprint(response, depth=3)   # or  print(json.dumps(response, indent=2))

# COMMAND ----------

for rec in response["outputs"]:
    print(f"ユーザー {rec['user_id']} 様の {rec['flight_id']} 便向けおすすめ:")

    cats = rec["contents_list"]["content_category"]
    imgs = rec["contents_list"]["content_img_b64"]     # ← 正しいキー

    for cat, img_b64 in zip(cats, imgs):
        print(f"  - カテゴリ: {cat}")
        print(f"    画像(b64 先頭30字): {img_b64[:30]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC 機内ディスプレイに表示するレコメンドっぽく表示してみる

# COMMAND ----------

from PIL import Image
import io
import base64
from IPython.display import display, HTML
import ast

# ─────────────────────────────────────────────
# 画像表示用のHTMLテンプレート
# ─────────────────────────────────────────────
html_template = """
<style>
.recommend-block {{
    background: #000;
    border-radius: 10px;
    max-width: 1240px;
    margin: 30px auto 30px auto;
    padding: 25px 15px 35px 15px;
    box-sizing: border-box;
}}
.user-header {{
    color: #fff;
    font-size: 22px;
    margin-bottom: 22px;
    margin-top: 0;
    font-weight: bold;
    text-align: left;
    letter-spacing: 1px;
}}
.tile-container {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 20px;
    max-width: 1200px;
    margin: auto;
    padding: 0;
    background: transparent;
    border-radius: 10px;
}}
.tile {{
    background: #34495e;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    transition: transform 0.3s ease;
}}
.tile:hover {{
    transform: translateY(-5px);
}}
.square-img-box {{
    width: 100%;
    aspect-ratio: 1/1;
    position: relative;
    background: #222;
    border-bottom: 3px solid #3498db;
}}
.tile-image {{
    position: absolute;
    top: 0; left: 0;
    width: 100%;
    height: 100%;
    object-fit: cover;
    object-position: top;
    display: block;
}}
.tile-category {{
    padding: 15px;
    text-align: center;
    color: #fff;
    font-family: Arial, sans-serif;
    font-size: 16px;
    background: #2c3e50;
}}
</style>

{content}
"""

# ─────────────────────────────────────────────
# レスポンスからユーザー情報と画像を取得してHTMLを生成
# ─────────────────────────────────────────────
def generate_user_tiles(response):
    all_html = ""
    for rec in response["outputs"]:
        user_html = f"""
        <div class="recommend-block">
            <div class="user-header">
                お客様[ 会員ID: {rec['user_id']} | フライトID: {rec['flight_id']} ]だけのおすすめ情報♪
            </div>
            <div class="tile-container">
        """
        # ★ ここで Base64 配列を直接取得
        cats = rec["contents_list"]["content_category"]
        imgs = rec["contents_list"]["content_img_b64"]

        for cat, img_b64 in zip(cats, imgs):
            tile = f"""
                <div class="tile">
                    <div class="square-img-box">
                        <img src="data:image/png;base64,{img_b64}" class="tile-image" alt="{cat}">
                    </div>
                    <div class="tile-category">{cat}</div>
                </div>
            """
            user_html += tile

        user_html += "</div></div>"
        all_html += user_html
    return all_html


# ─────────────────────────────────────────────
# 表示
# ─────────────────────────────────────────────
# ユーザーごとのおすすめ情報を表示
user_tiles_content = generate_user_tiles(response)
full_html = html_template.format(content=user_tiles_content)

# HTMLを表示
display(HTML(full_html))

# COMMAND ----------

# MAGIC %md
# MAGIC OK！ではDatabricks Appsで表示しましょう！  
# MAGIC
# MAGIC [_apps/app.py]($_apps/app.py)へGO！
