# Databricks notebook source
# MAGIC %md
# MAGIC 変更しないでください

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

# # credit_bureauをダウンロード
# DBDemos.download_file_from_git(
#     dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/credit_bureau",
#     owner="komae5519pv",
#     repo="komae_dbdemos",
#     path="/automl_demo_20241011/_rawdata/credit_bureau/"
# )
