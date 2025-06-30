# Databricks notebook source
# MAGIC %md
# MAGIC デモ用のサンプルデータをGithubからボリュームにロードします

# COMMAND ----------

# %run ../00_setup

# COMMAND ----------

print("Githubからデータダウンロード中...")

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

'''ダウンロード'''
# customer
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/customer",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/e2e_ML_20250629/_data/_raw_data/customer/"
)

# ml_sample
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/ml_sample",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/e2e_ML_20250629/_data/_raw_data/ml_sample/"
)

# data
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/data",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/e2e_ML_20250629/_data/_raw_data/data/"
)

# COMMAND ----------

print("ダウンロードが完了しました！")
