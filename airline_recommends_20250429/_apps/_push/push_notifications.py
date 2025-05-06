import qrcode
import requests
from dash import html
from PIL import Image
from io import BytesIO
from databricks import sql
from config import Config

# Databricks接続設定
cfg = Config()

# SQLクエリを汎用的に実行するメソッド
def _execute_sql(query: str, params: list) -> list:
    """汎用SQL実行メソッド"""
    with sql.connect(
        server_hostname=cfg.DATABRICKS_SERVER_HOSTNAME,
        http_path=cfg.DATABRICKS_HTTP_PATH,
        access_token=cfg.DATABRICKS_TOKEN
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()


# プッシュ通知カードの表示関数
def send_push_notification(title, subject, thumb_url, icon_url="",
                           title_size=16, body_size=14):
    icon_html = (f'<img src="{icon_url}" '
                 f'style="width:{title_size}px; margin-bottom:-3px; margin-right:4px;" '
                 f'/>'
                 if icon_url else "")

    return html.Div(
        className="push-notification-card",  # 外部CSSクラスを適用
        children=[
            html.Div(
                className="thumb-container",  # 外部CSSクラスを適用
                children=[
                    html.Img(
                        src=thumb_url,  # 画像URLをそのまま使用
                        style={
                            "width": "100%",
                            "borderRadius": "5px",
                            "objectFit": "cover"
                        }
                    )
                ]
            ),
            html.Div(
                className="text-container",  # 外部CSSクラスを適用
                children=[
                    html.Div(
                        className="title",  # 外部CSSクラスを適用
                        children=[icon_html, title]
                    ),
                    html.Div(
                        className="subject",  # 外部CSSクラスを適用
                        children=[subject]
                    )
                ]
            )
        ]
    )

# プッシュ通知メッセージを生成する関数
def generate_push_message(user_id):
    try:
        print(f"Generating push message for user_id: {user_id}")
        
        # フライトIDを取得
        flight_id = get_flight_id(user_id)
        print(f"Flight ID: {flight_id}")  # デバッグ用に出力
        if not flight_id:
            print("Flight ID not found")
            return send_push_notification(
                title="フライト情報が見つかりません",
                subject="フライトIDの取得に失敗しました",
                thumb_url='https://example.com/error-image.jpg'
            )
        
        # レコメンド情報を取得
        recommendation = get_recommendations(user_id, flight_id)
        print(f"Recommendation: {recommendation}")  # デバッグ用に出力
        if not recommendation:
            print("No recommendations found")
            return send_push_notification(
                title="レコメンド情報が見つかりません",
                subject="レコメンドの取得に失敗しました",
                thumb_url='https://example.com/error-image.jpg'
            )

        # メッセージタイトルとサブタイトルの生成
        cat = recommendation.get('cat', 'データなし')
        total = recommendation.get('total', 0)

        title = f"機内エンタメ 厳選 {total} 作品をお届け！"
        subject = f"あなた向けの『{cat}』など多数ご用意しています。続きを機内ディスプレイでどうぞ！"

        # 画像URLを生成
        image_path = recommendation.get('img', '1.png')
        github_base_url = "https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_images/_contents/"
        
        # 画像ファイル名を取り出し、GitHubのURLに変換
        image_filename = image_path.split('/')[-1]  
        github_image_url = f"{github_base_url}{image_filename}?raw=true"

        return send_push_notification(
            title=title,
            subject=subject,
            thumb_url=github_image_url,  # GitHubの画像URLをそのまま使用
            title_size=16,
            body_size=14
        )

    except Exception as e:
        print(f"通知生成エラー: {str(e)}")
        return send_push_notification(
            title="エラーが発生しました",
            subject="もう一度お試しください。",
            thumb_url='https://example.com/error-image.jpg',
            title_size=16,
            body_size=14
        )

def get_flight_id(user_id):
    """
    会員IDからフライトIDを取得する関数
    """
    query = f"""
        SELECT flight_id
        FROM {cfg.MY_CATALOG}.{cfg.MY_SCHEMA}.gd_recom_top6
        WHERE user_id = ?
        LIMIT 1
    """
    result = _execute_sql(query, [user_id])  # パラメータバインディングで渡す
    return result[0].flight_id if result else None


def get_recommendations(user_id, flight_id):
    """
    会員IDとフライトIDに基づいてレコメンドデータを取得する関数
    """
    query = f"""
        SELECT
            contents_list.content_category[0] AS cat,
            contents_list.content_img_url[0] AS img,
            size(contents_list.content_category) AS total
        FROM {cfg.MY_CATALOG}.{cfg.MY_SCHEMA}.gd_recom_top6
        WHERE user_id = ? AND flight_id = ?
        LIMIT 1
    """
    result = _execute_sql(query, [user_id, flight_id])  # パラメータバインディングで渡す
    return {
        "cat": result[0].cat if result else "データなし",
        "img": result[0].img if result else "https://example.com/default-image.jpg",
        "total": result[0].total if result else 0
    }