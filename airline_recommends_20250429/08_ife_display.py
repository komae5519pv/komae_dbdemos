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
dbutils.widgets.text("user_id", "261")
dbutils.widgets.text("flight_id", "JL185")

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

for rec in response['outputs']:
    print(f"ユーザー {rec['user_id']} 様の {rec['flight_id']} 便向けおすすめ:")
    for cat, img in zip(rec['contents_list']['content_category'], 
                       rec['contents_list']['content_img_url']):
        print(f"  - カテゴリ: {cat}")
        print(f"    画像URL: {img}")

# COMMAND ----------

# MAGIC %md
# MAGIC 機内ディスプレイに表示するレコメンドっぽく表示してみる

# COMMAND ----------

# DBTITLE 1,テスト表示
from PIL import Image
import io
import base64
from IPython.display import display, HTML
import ast

# 画像を圧縮してBase64エンコードする関数
def encode_image_to_base64(image_path, quality=50, new_width=300):
    # 画像を開く
    with Image.open(image_path) as img:
        # 解像度を変更（幅をnew_widthに設定）
        width_percent = (new_width / float(img.size[0]))
        new_height = int((float(img.size[1]) * float(width_percent)))
        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

        # 圧縮してバッファに保存
        buffered = io.BytesIO()
        img.save(buffered, format="PNG", quality=quality)  # qualityを指定して圧縮
        # Base64エンコード
        b64_string = base64.b64encode(buffered.getvalue()).decode('utf-8')
    return f"data:image/png;base64,{b64_string}"

# 画像表示用のHTMLテンプレート
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

# レスポンスからユーザー情報と画像を取得してHTMLを生成
def generate_user_tiles(response):
    all_html = ""
    for rec in response['outputs']:
        user_html = f"""
        <div class="recommend-block">
            <div class="user-header">お客様[ 会員ID: {rec['user_id']} | フライトID: {rec['flight_id']} ]だけのおすすめ情報♪</div>
            <div class="tile-container">
        """
        for cat, img_path in zip(rec['contents_list']['content_category'], rec['contents_list']['content_img_url']):
            # 画像パスを圧縮してBase64エンコード
            base64_img = encode_image_to_base64(img_path, quality=50, new_width=300)  # 解像度を300pxに設定
            tile = f"""
                <div class="tile">
                    <div class="square-img-box">
                        <img src="{base64_img}" class="tile-image" alt="{cat}">
                    </div>
                    <div class="tile-category">{cat}</div>
                </div>
            """
            user_html += tile
        user_html += "</div></div>"
        all_html += user_html
    return all_html

# ユーザーごとのおすすめ情報を表示
user_tiles_content = generate_user_tiles(response)
full_html = html_template.format(content=user_tiles_content)

# HTMLを表示
display(HTML(full_html))

# COMMAND ----------

# MAGIC %md
# MAGIC Volumeに保存された画像を表示してみる（デモ用コード）  
# MAGIC Githubに保存した画像URLへのアクセスだと、HTTPSアクセスで表示が不安定になるため、Databricks Appsでは、Databricks SDKのWorkspaceクライアントを用いて、Volumeに保存した画像にアクセスし、Base64でエンコードしてレコメンド一覧をHTML表示します。  
# MAGIC
# MAGIC この処理に時間がかかるため、Feature Servingでレコメンド情報を高速に取得しているメリットを受けにくいですが、デモなので諦めます。

# COMMAND ----------

# DBTITLE 1,Volume画像表示
import base64
from databricks.sdk import WorkspaceClient
from IPython.core.display import display, HTML

# WorkspaceClientインスタンスを作成
w = WorkspaceClient()

# Volumeパス（ご使用のパスに置き換えてください）
volume_path = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_CONTNETS}/1.png"

# ファイルをダウンロード
file_data = w.files.download(volume_path).contents.read()

# base64にエンコード
image_base64 = base64.b64encode(file_data).decode("utf-8")

# HTMLとして画像を表示
html_content = f'<img src="data:image/jpeg;base64,{image_base64}" alt="Image"/>'
display(HTML(html_content))
