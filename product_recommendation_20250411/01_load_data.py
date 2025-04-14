# Databricks notebook source
# MAGIC %md
# MAGIC # Githubのデータをダウンロード
# MAGIC - ここではデモ用のサンプルデータをGithubからボリュームにロードします  
# MAGIC - サーバレス or DBR 16.0ML以降

# COMMAND ----------

# MAGIC %md
# MAGIC カタログ構成 
# MAGIC ```
# MAGIC /<catalog_name>
# MAGIC ├── product_recommendation_stadium        <- スキーマ
# MAGIC │   ├── ボリューム
# MAGIC │       ├── data/games                    <- games.csv
# MAGIC │       ├── data/purchase_history         <- purchase_history.csv
# MAGIC │       ├── data/vendors                  <- vendors.csv
# MAGIC │   ├── テーブル
# MAGIC │       ├── bz_stadium_vendors            <- 販売店とアイテムマスタ
# MAGIC │       ├── bz_ticket_sales               <- チケットの売上データ（Fakerで生成します）
# MAGIC │       ├── bz_games                      <- イベントのスケジュール
# MAGIC │       ├── bz_point_of_sale              <- POS注文履歴（Fakerで生成します）
# MAGIC │       ├── bz_purchase_history           <- 購入履歴（過去の推奨アイテム履歴含む）
# MAGIC │       ├── sv_sales                      <- トレーニング用データ
# MAGIC │       ├── gd_sections_recommendations   <- 顧客ごとレコメンドTOP10
# MAGIC │       ├── gd_final_recommendations      <- 顧客ごとベストレコメンド
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC <!-- ## データの準備
# MAGIC 以下リンクからデータのZipファイルをダウンロードしてVolumeに手動でアップロードしてください。
# MAGIC https://github.com/yulan-yan/Product_recommendation_stadium/blob/main/stadium_recommender.zip -->

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import requests
import os
from concurrent.futures import ThreadPoolExecutor

class DBDemos:
    @staticmethod
    def download_file_from_git(dest, owner, repo, path):
        def download_file(url, destination):
            local_filename = url.split('/')[-1]
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                print(f'Saving {destination}/{local_filename}')
                with open(f'{destination}/{local_filename}', 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

        if not os.path.exists(dest):
            os.makedirs(dest)

        api_url = f'https://api.github.com/repos/{owner}/{repo}/contents{path}'
        files = requests.get(api_url).json()
        download_urls = [f['download_url'] for f in files if isinstance(f, dict) and 'download_url' in f]

        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(lambda url: download_file(url, dest), download_urls)

# COMMAND ----------

# gamesをダウンロード
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/games",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/product_recommendation_20250411/_data/games/"
)

# purchase_historyをダウンロード
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/purchase_history",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/product_recommendation_20250411/_data/purchase_history/"
)

# vendorsをダウンロード
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/vendors",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/product_recommendation_20250411/_data/vendors/"
)
