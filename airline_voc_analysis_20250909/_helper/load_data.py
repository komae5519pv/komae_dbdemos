# Databricks notebook source
# MAGIC %md
# MAGIC # Githubのデータをダウンロード
# MAGIC - ここではデモ用のサンプルデータをGithubからボリュームにロードします  
# MAGIC - サーバレス or DBR 16.0ML以降

# COMMAND ----------

# %run ../00_config

# COMMAND ----------

print("顧客レビューのCSVファイルをダウンロードします...")

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

# 顧客レビューをダウンロード
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/airline_voc_analysis_20250909/_raw_data/"
)

# 顧客レビュー(論点抽出&サマライズ直後)をダウンロード
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_TMP}",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/airline_voc_analysis_20250909/_tmp_data/"
)

# COMMAND ----------

print("顧客レビューのCSVファイルのダウンロードが完了しました！")
