import time
import random
import pandas as pd
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State
from _push.push_notifications import generate_push_message
from _recommends.feature_client import FeatureClient
from _recommends.components import RecommendationGrid
from databricks import sql
from config import Config

# -------------------------
# 共通処理の定義
# -------------------------

# Databricks接続設定
cfg = Config()

def sql_query_with_user_token(query: str, user_token: str) -> pd.DataFrame:
    """ユーザートークンを使用してSQLクエリを実行し、結果をDataFrameで返す"""
    with sql.connect(
        server_hostname=cfg.DATABRICKS_SERVER_HOSTNAME,
        http_path=cfg.DATABRICKS_HTTP_PATH,
        access_token=user_token
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# 会員IDリストを取得
def load_user_ids(user_token: str) -> list:
    """会員IDのリストを取得"""
    try:
        user_token = cfg.DATABRICKS_TOKEN
        query = f"""
            SELECT DISTINCT user_id
            FROM {cfg.MY_CATALOG}.{cfg.MY_SCHEMA}.gd_recom_top6
            ORDER BY 1 ASC
        """
        df = sql_query_with_user_token(query, user_token)
        return df['user_id'].tolist() if not df.empty else list(range(1, 11))  # エラー時のフォールバック
    except Exception as e:
        print(f"会員ID取得エラー: {str(e)}")
        return list(range(1, 11))  # エラー時のフォールバック


# レコメンドデータを取得
def load_recommendation(user_id: int, user_token: str) -> dict:
    """会員ごとのレコメンドデータを取得"""
    try:
        user_token = cfg.DATABRICKS_TOKEN
        query = f"""
            SELECT * 
            FROM {cfg.MY_CATALOG}.{cfg.MY_SCHEMA}.gd_recom_top6 
            WHERE user_id = {user_id}
        """
        return sql_query_with_user_token(query, user_token).iloc[0].to_dict()
    except Exception as e:
        print(f"レコメンドデータ取得エラー: {str(e)}")
        return {}

# -------------------------
# Dashアプリ初期化とレイアウト定義
# -------------------------
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
feature_client = FeatureClient()

# QRコード画像URLのパターン
qr_code_urls = [
    "https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_images/_qr_codes/1.png?raw=true",
    "https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_images/_qr_codes/2.png?raw=true",
    "https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_images/_qr_codes/3.png?raw=true",
    "https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_images/_qr_codes/4.png?raw=true"
]

# スマホ画像URL
smartphone_image_url = 'https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_images/_phone_screen/phone_screen.png?raw=true'

# レイアウト
app.layout = html.Div([

    dcc.Store(id='previous-customer-id', data=None),  # previous-customer-id を追加

    # タブ切り替え
    dcc.Tabs([
        # ---------- アプリプッシュメッセージ ----------
        dcc.Tab(label="アプリプッシュメッセージ", children=[
            html.Div(  # .page-container
                [
                    # --------- 左側 (サイドバー) ---------
                    html.Div(
                        [
                            html.H4("会員IDを選択してください", className="mb-3 text-center", style={"color": "black"}),  # 文字色を黒に
                            dcc.Dropdown(
                                id="user-dropdown-push",  # ここでプッシュ通知用の会員IDを選択
                                options=[],
                                placeholder="会員IDを選択",
                                style={"width": "100%"},
                            ),
                        ],
                        className="sidebar",
                        style={"flex": "0 0 20%"}  # 左側を20%に指定
                    ),

                    # --------- 右側 (メイン) ---------
                    html.Div(
                        [
                            # スマホ画像とQRコードを横並びに配置
                            html.Div(
                                style={
                                    'display': 'flex',          # Flexboxで並べる
                                    'justifyContent': 'center', # 中央寄せ
                                    'alignItems': 'center',     # 縦方向にも中央寄せ
                                    'width': '100%',
                                    'height': '100vh',          # 画面縦のサイズに合わせる
                                },
                                children=[
                                    # スマホフレーム画像（背景）
                                    html.Img(
                                        src=smartphone_image_url,
                                        style={
                                            'height': '80%',           # 高さ80%に設定
                                            'width': 'auto',           # 幅は自動調整
                                            'zIndex': '1',
                                            'pointerEvents': 'none',   # 画像のクリックを無効化
                                        }
                                    ),
                                    # QRコードの表示（スマホ画像の右隣）
                                    html.Div(
                                        id="qr-code",
                                        style={
                                            'width': '400px',           # QRコードの幅
                                            'height': '400px',          # QRコードの高さ
                                            'marginLeft': '20px',       # QRコードとスマホ画像の間に隙間を作る
                                            'display': 'block',         # 初期状態で表示する
                                        },
                                        children=[
                                            html.Img(id="qr-code-img", src="", style={"width": "100%", "height": "100%"})
                                        ]
                                    ),
                                ]
                            ),
                        ],
                        className="main-area",
                        style={"flex": "1 1 80%", "display": "flex", "justifyContent": "center"}  # 右側を80%に指定
                    ),
                ],
                className="page-container",
                style={'background-color': '#121212'}  # 背景を黒く変更
            ),

            # プッシュ通知表示部分
            html.Div(id="push-notification-display"),

            # プッシュ通知ボックス部分）
            html.Div(id="push-notification-box", className="push-notification-box"),

        ]),

        # ---------- 機内レコメンド ----------
        dcc.Tab(label="機内レコメンド", children=[
            html.Div(                              # .page-container
                [
                    # --------- 左側 (サイドバー) ---------
                    html.Div(
                        [
                            html.H4("会員IDを選択してください", className="mb-3 text-center"),
                            dcc.Dropdown(
                                id="user-dropdown",
                                options=[],
                                placeholder="会員IDを選択",
                                style={"width": "100%"},
                            ),
                        ],
                        className="sidebar",
                    ),

                    # --------- 右側 (メイン) ---------
                    html.Div(
                        dcc.Loading(
                            id="loading-box",                           # IDを付けてstyleを当てる
                            children=html.Div(id="recommendations-grid"),
                            style={"flex": "1 1 0", "width": "100%"} 
                        ),
                        className="main-area",
                    ),
                ],
                className="page-container",
            )
        ])
    ])
])

# ------------ プッシュメッセージを表示するコールバック ------------
@app.callback(
    Output('push-notification-box', 'className'),
    Output('previous-customer-id', 'data'),
    Input('user-dropdown-push', 'value'),
    State('previous-customer-id', 'data'),
    prevent_initial_call=True
)
def reset_animation(customer_id, previous_id):
    if customer_id == previous_id:
        return "", dash.no_update   # dash.no_update をデータ更新に使って、classNameは必ず変更
    return "show", customer_id      # classNameをリセット


# アニメーションの表示
@app.callback(
    Output('push-notification-box', 'className', allow_duplicate=True),
    Input('previous-customer-id', 'data'),
    prevent_initial_call=True
)
def show_notification(_):
    return "show"  # showクラスを追加して表示


# フィルタの選択肢（会員ID）更新
@app.callback(
    Output("user-dropdown-push", "options"),
    Input("user-dropdown-push", "search_value"),
)
def update_push_user_options(search_value):
    user_token = cfg.DATABRICKS_TOKEN    # トークンを取得
    return load_user_ids(user_token)        # 会員IDリストを返す


# プッシュ通知の更新
@app.callback(
    Output("push-notification-display", "children"),
    Input("user-dropdown-push", "value"),
)
def display_push_notification(user_id):
    """
    会員ID選択後にプッシュ通知を生成して表示
    """
    print(f"Selected User ID: {user_id}")  # デバッグ用に出力
    if not user_id:
        return None
    
    # メッセージを生成して表示
    return generate_push_message(user_id)


# ------------ 会員ID選択時にQRコードをランダムで更新 & 表示 ------------
@app.callback(
    Output("qr-code-img", "src"),
    Output("qr-code", "style"),
    Input("user-dropdown-push", "value")  # 会員IDが選択された場合にQRコードが変更される
)
def show_random_qr_code(user_id):
    # 初期状態でもランダムなQRコードを表示
    qr_code_url = random.choice(qr_code_urls)
    return qr_code_url, {'display': 'block'}  # QRコード画像を表示

# ------------ 機内レコメンド用のコールバック ------------
@app.callback(
    Output("user-dropdown", "options"),
    Input("user-dropdown", "search_value"),
)
def update_user_options(_):
    user_token = cfg.DATABRICKS_TOKEN  # トークンを取得
    return load_user_ids(user_token)  # 会員IDリストを返す

# 機内レコメンドの更新
@app.callback(
    Output("recommendations-grid", "children"),
    Input("user-dropdown", "value"),  # フィルタが選択されるとコールバックが発火
)
def update_recommendations(user_id):
    if not user_id:
        return dbc.Alert("会員IDを選択してください", color="danger")

    try:
        flight_id = feature_client.get_flight_id(user_id)
        if not flight_id:
            return dbc.Alert("該当フライトが見つかりません", color="warning")

        response = feature_client.get_recommendations(user_id, flight_id)
        return RecommendationGrid.create_grid(response)

    except Exception as e:
        return dbc.Alert(f"エラー発生: {e}", color="danger")


if __name__ == "__main__":
    app.run_server(debug=True)
