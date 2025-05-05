import os

class Config:
    """
    Databricks連携Dashアプリの設定クラス
    """

    # Databricks SQL接続設定
    DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    RECOMMEND_TABLE = os.getenv("RECOMMEND_TABLE")

    # Feature Servingエンドポイント設定
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    SERVING_ENDPOINT = os.getenv("SERVING_ENDPOINT")

    # アプリケーション固有設定
    APP_TITLE = "機内エンタメレコメンド"
    DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() in ("true", "1", "t")

    # CSS/UI関連
    CARD_STYLE = {
        "background": "#34495e",
        "border-radius": "8px",
        "overflow": "hidden",
        "box-shadow": "0 4px 8px rgba(0,0,0,0.2)",
        "transition": "transform 0.3s ease"
    }

    @classmethod
    def validate(cls):
        """必須設定値のバリデーション"""
        required = [
            cls.DATABRICKS_SERVER_HOSTNAME,
            cls.DATABRICKS_HTTP_PATH,
            cls.DATABRICKS_TOKEN,
            cls.RECOMMEND_TABLE,
            cls.DATABRICKS_HOST,
            cls.SERVING_ENDPOINT,
        ]
        if not all(required):
            raise ValueError("必須の環境変数が設定されていません。.envファイルを確認してください。")

# アプリ起動時に設定値の妥当性チェック
Config.validate()
