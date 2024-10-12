# Databricks notebook source
# MAGIC %md
# MAGIC ## Githubにある各種データをダウンロードします
# MAGIC これはデモ専用にサンプルデータを準備する処理です
# MAGIC
# MAGIC ローデータを該当ボリュームに配置します  
# MAGIC ```
# MAGIC /<catalog_name>
# MAGIC ├── city_bike_causal_forcasting           <- スキーマ
# MAGIC │   ├── credit_raw_data                   <- ボリューム(Import用)
# MAGIC │       ├── raw_data/citibike/trips       <- RAWファイルを配置
# MAGIC │       ├── raw_data/weather/manhattan    <- RAWファイルを配置
# MAGIC │   ├── export_data                       <- ボリューム(Export用)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### データ準備
# MAGIC 手動でダウンロードする場合はこちらから  
# MAGIC
# MAGIC 今回の予測演習では、Citi Bike NYCのトリップ履歴データを時間単位に集計したものと、Visual Crossingからの時間ごとの天気データを使用します。このノートブックではデータファイルの準備を行いますが、使用するデータセットに関連する利用規約があるため、データへのアクセスやダウンロード方法についての説明は行いません。これらのデータを使用したい場合は、以下のリンク先でデータ提供者のウェブサイトを訪問し、利用規約を確認の上、適切にデータを使用してください。
# MAGIC
# MAGIC * [[利用規約]](https://www.citibikenyc.com/data-sharing-policy) [Citibike NYC トリップ履歴データ](https://s3.amazonaws.com/tripdata/index.html)
# MAGIC * [[利用規約]](https://www.visualcrossing.com/weather-services-terms) [Zipコード10001のVisual Crossing時間ごとの天気データ](https://www.visualcrossing.com/weather/weather-data-services)
# MAGIC
# MAGIC Citi Bike NYCのデータファイルはZIP形式で提供されていることに注意してください。以下の手順では、これらのファイルを解凍し、重複するデータセットを削除して各トリップが一度だけ表示されるように整理したことを前提としています。歴史的なトリップデータが /mnt/citibike/trips フォルダに、Zipコード10001（マンハッタン中心部）の天気データが /mnt/weather/manhattan フォルダにダウンロードされた状態で、それぞれの内容を確認してみましょう。
# MAGIC
# MAGIC
# MAGIC <!-- %md
# MAGIC ##Data Preparation
# MAGIC
# MAGIC For our forecasting exercise, we will make use of Citi Bike NYC trip history data aggregated to an hourly level and hourly weather data from Visual Crossing.  This notebook will prepare our data files for this work, but because of the terms of use associated with the datasets we will use, we will not provide instructions on how to access and download these.  If you wish to make use of these data, please visit the data providers' websites (linked to below), review their Terms of Use (also linked to below), and employ the data appropriately:</p>
# MAGIC
# MAGIC * [[Terms of Use]](https://www.citibikenyc.com/data-sharing-policy) [Citibike NYC Trip History Data](https://s3.amazonaws.com/tripdata/index.html)
# MAGIC * [[Terms of Use]](https://www.visualcrossing.com/weather-services-terms) [Visual Crossing Hourly Weather Data for ZipCode 10001](https://www.visualcrossing.com/weather/weather-data-services)
# MAGIC
# MAGIC Please note that the Citi Bike NYC data files are provided in a ZIP format. The steps below assume you have unzipped these files and removed any files representing overlapping data sets so that each trip is represented once and only once in the folder below. With the historical trip data loaded to a folder we will identify as /mnt/citibike/trips and weather data for Zip Code 10001 (Central Manhattan) downloaded to a folder we will identify as /mnt/weather/manhattan, let's examine the contents of each:  -->

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
# citibikeをダウンロード
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/citibike/trips",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/supply_chain_demand_forecasting_20241012/_rawdata/citibike/trips/"
)

# weatherをダウンロード
DBDemos.download_file_from_git(
    dest=f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/weather/manhattan",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/supply_chain_demand_forecasting_20241012/_rawdata/weather/manhattan/"
)
