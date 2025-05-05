import pandas as pd
from dash import Dash, html, dcc, Input, Output
import dash_bootstrap_components as dbc
from _push.push_notifications import generate_push_message  # メッセージ生成処理
from _recommends.feature_client import FeatureClient
from _recommends.components import RecommendationGrid
from databricks import sql
from config import Config

app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
feature_client = FeatureClient()

# スマホ画像URL
smartphone_image_url = 'https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_images/_phone_screen/phone_screen.png?raw=true'

# ---------- レイアウト ----------
app.layout = html.Div([

    # タブ切り替え
    dcc.Tabs([
        # ---------- アプリプッシュメッセージ ----------
        dcc.Tab(label="アプリプッシュメッセージ", children=[
            html.Div([
                # プッシュ通知用フィルタ
                html.H4("会員IDを選択してください", style={"color": "white"}),  # 文字色を白に
                dcc.Dropdown(
                    id="user-dropdown-push",  # ここでプッシュ通知用の会員IDを選択
                    options=[],
                    placeholder="会員IDを選択",
                    style={"width": "100%"},
                ),
                # プッシュ通知表示部分
                html.Div(id="push-notification-display"),  
                # スマホ画像の表示
                html.Div(
                    style={
                        'position': 'relative',
                        'display': 'inline-block',
                        'width': '450px',
                        'height': 'auto'
                    },
                    children=[
                        # スマホフレーム画像（背景）
                        html.Img(
                            src=smartphone_image_url,
                            style={
                                'width': '100%',
                                'height': 'auto',
                                'position': 'relative',
                                'zIndex': '1',
                                'pointerEvents': 'none'  # 画像のクリックを無効化
                            }
                        ),
                    ]
                )
            ], className="push-tab-container", style={'background-color': '#121212'})  # プッシュメッセージ部分の背景を黒に変更
        ]),

        # ---------- 機内レコメンド ----------
        dcc.Tab(label="機内レコメンド", children=[
            html.Div(                              # .page-container
                [
                    # --------- 左側 (サイドバー) ---------
                    html.Div(
                        [
                            html.H4("会員ID選択してください", className="mb-3 text-center"),
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

# ------------ プッシュ配信用のコールバック ------------
# フィルタの選択肢（会員ID）更新
@app.callback(
    Output("user-dropdown-push", "options"),
    Input("user-dropdown-push", "search_value"),
)
def update_push_user_options(search_value):
    try:
        with sql.connect(
            server_hostname=Config.DATABRICKS_SERVER_HOSTNAME,
            http_path=Config.DATABRICKS_HTTP_PATH,
            access_token=Config.DATABRICKS_TOKEN,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT DISTINCT user_id FROM {Config.RECOMMEND_TABLE} ORDER BY user_id"
                )
                return [
                    {"label": str(row.user_id), "value": row.user_id}
                    for row in cursor.fetchall()
                ]
    except Exception as e:
        print(f"Error: {e}")
        return []

# プッシュ通知の更新
@app.callback(
    Output("push-notification-display", "children"),
    Input("user-dropdown-push", "value"),
)
def display_push_notification(user_id):
    """
    会員ID選択後にプッシュ通知を生成して表示
    """
    if not user_id:
        return None
    
    # メッセージを生成して表示
    return generate_push_message(user_id)


# ------------ 機内レコメンド用のコールバック ------------
# フィルタの選択肢（会員ID）更新
@app.callback(
    Output("user-dropdown", "options"),
    Input("user-dropdown", "search_value"),
)
def update_user_options(_):
    """
    目的: ユーザーがフィルタで会員IDを検索した際に、Databricksのテーブルから会員IDの選択肢を取得する。
    処理: ユーザーが検索した値に基づき、会員IDのリストを表示する。このコールバックは、会員IDの検索機能を実現し、ユーザーが選択するための候補を動的に更新します。
    """
    try:
        with sql.connect(
            server_hostname=Config.DATABRICKS_SERVER_HOSTNAME,
            http_path=Config.DATABRICKS_HTTP_PATH,
            access_token=Config.DATABRICKS_TOKEN,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT DISTINCT user_id FROM {Config.RECOMMEND_TABLE} ORDER BY user_id"
                )
                return [
                    {"label": str(row.user_id), "value": row.user_id}
                    for row in cursor.fetchall()
                ]
    except Exception as e:
        print(f"Error: {e}")
        return []

# 機内レコメンドの更新
@app.callback(
    Output("recommendations-grid", "children"),
    Input("user-dropdown", "value"),  # フィルタが選択されるとコールバックが発火
)
def update_recommendations(user_id):
    """
    目的: ユーザーが会員IDを選択した後、その会員IDに基づいて関連するレコメンド画像を表示する。
    処理: 会員IDが選ばれると、その会員に関連するフライトIDを取得し、Feature Servingエンドポイントにリクエストを送る。レコメンド結果を受け取り、ユーザーに適切な画像を表示するグリッドを作成します。このコールバックは、会員IDの選択に連動してレコメンド情報をリアルタイムで表示する目的を持っています。
    """
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