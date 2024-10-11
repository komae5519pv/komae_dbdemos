# Databricks notebook source
# MAGIC %md
# MAGIC ### Githubにある各種データをダウンロードします
# MAGIC これはデモ専用にサンプルデータを準備する処理です
# MAGIC
# MAGIC ローデータを該当ボリュームに配置します  
# MAGIC ```
# MAGIC /<catalog_name>
# MAGIC ├── automl_e2e_demo                       <- スキーマ
# MAGIC │   ├── credit_raw_data                   <- ボリューム(Import用)
# MAGIC │       ├── credit_bureau                 <- RAWファイルを配置: credit_bureau.json
# MAGIC │       ├── fund_trans                    <- RAWファイルを配置: part-00000-xxx.json, part-00001-xxx.json, part-00002-xxx.json　・・・
# MAGIC │       ├── internalbanking/accounts      <- RAWファイルを配置: accounts.csv
# MAGIC │       ├── internalbanking/customer      <- RAWファイルを配置: customer.csv
# MAGIC │       ├── internalbanking/relationship  <- RAWファイルを配置: relationship.csv
# MAGIC │       ├── telco                         <- RAWファイルを配置: telco.json
# MAGIC │   ├── export_data                       <- ボリューム(Export用)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 手動でダウンロードする場合はこちらから  
# MAGIC [デモ用データ <zip> をダウンロード](https://databricks.my.salesforce.com/sfc/dist/version/download/?oid=00D61000000JGc4&ids=068Vp000009Ot6DIAS&d=/a/Vp000000ap5d/rA84k9ApfMCrc4O2leUGOQtQUhnp9R0urZwxDaPb71o)

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Githubからファイルダウンロードするクラス定義
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

# DBTITLE 1,Githubからファイルダウンロード
# credit_bureauをダウンロード
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/credit_bureau",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/automl_demo_20241011/_rawdata/credit_bureau/"
)

# fund_transをダウンロード
DBDemos.download_file_from_git(
    dest="/Volumes/komae_demo/automl_e2e_demo/credit_raw_data/fund_trans",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/automl_demo_20241011/_rawdata/fund_trans/"
)

# accountをダウンロード
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/internalbanking/account",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/automl_demo_20241011/_rawdata/internalbanking/account/"
)

# customerをダウンロード
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/internalbanking/customer",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/automl_demo_20241011/_rawdata/internalbanking/customer/"
)

# relationshipをダウンロード
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/internalbanking/relationship",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/automl_demo_20241011/_rawdata/internalbanking/relationship/"
)

# telcoをダウンロード
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/telco",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/automl_demo_20241011/_rawdata/telco/"
)
