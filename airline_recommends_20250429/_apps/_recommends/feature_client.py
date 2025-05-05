import os
import requests
from databricks import sql
from typing import Dict, List, Optional

# 親ディレクトリからインポート
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # 親ディレクトリをsys.pathに追加
from config import Config

class FeatureClient:
    def __init__(self):
        self.host = Config.DATABRICKS_HOST
        self.token = Config.DATABRICKS_TOKEN
        self.endpoint = Config.SERVING_ENDPOINT
        self.recommend_table = Config.RECOMMEND_TABLE

    def _execute_sql(self, query: str, params: list) -> list:
        """汎用SQL実行メソッド"""
        with sql.connect(
            server_hostname=Config.DATABRICKS_SERVER_HOSTNAME,
            http_path=Config.DATABRICKS_HTTP_PATH,
            access_token=self.token
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()

    def get_flight_id(self, user_id: int) -> Optional[str]:
        """フライトID取得"""
        # 会員IDに紐づくフライトIDを一件のみ取得（デモ用の処理）
        query = f"SELECT flight_id FROM {self.recommend_table} WHERE user_id = ? LIMIT 1"
        result = self._execute_sql(query, [user_id])
        return result[0].flight_id if result else None  # プロパティ形式でアクセス

    def get_recommendations(self, user_id: int, flight_id: str) -> Dict:
        """レコメンド情報取得"""
        response = requests.post(
            f"{self.host}/serving-endpoints/{self.endpoint}/invocations",
            headers={"Authorization": f"Bearer {self.token}"},
            json={
                "dataframe_records": [{
                    "user_id": user_id,
                    "flight_id": flight_id
                }]
            },
            timeout=30  # タイムアウト追加（推奨）
        )
        response.raise_for_status()
        return response.json()
