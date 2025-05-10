import time
import random
import dash
import pandas as pd
from dash import dash_table
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State
from _recommends.feature_client import FeatureClient
from _recommends.components import RecommendationGrid
from databricks import sql
from config import Config
import flask

# -------------------------
# 共通処理の定義
# -------------------------

# Databricks接続設定
cfg = Config()

# 画像を Base64 で持つ最新テーブル
RECOM_TABLE = f"{cfg.MY_CATALOG}.{cfg.MY_SCHEMA}.gd_recom_top6_bs64"

# QRコード画像URLのパターン（最初に表示するQRコードを固定）
qr_code_url = 'https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_images/_static/qr_code.png?raw=true'

# スマホ画像URL
smartphone_image_url = 'https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_images/_static/phone_screen.png?raw=true'

# bell画像URL
bell_image_url = 'https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_recommends_20250429/_images/_static/bell.png?raw=true'

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

# -------------------------
# 関数
# -------------------------
# COMMON：会員IDリストを取得
def load_user_ids(user_token: str) -> list:
    """会員IDのリストを取得"""
    try:
        user_token = cfg.DATABRICKS_TOKEN
        query = f"SELECT DISTINCT user_id FROM {RECOM_TABLE} ORDER BY 1"
        df = sql_query_with_user_token(query, user_token)
        return df['user_id'].tolist() if not df.empty else list(range(1, 11))  # エラー時のフォールバック
    except Exception as e:
        print(f"会員ID取得エラー: {str(e)}")
        return list(range(1, 11))  # エラー時のフォールバック

# IFE：レコメンドデータを取得
def ife_load_recommendation(user_id: int, user_token: str) -> dict:
    """会員ごとのレコメンドデータを取得"""
    try:
        user_token = cfg.DATABRICKS_TOKEN
        query = f"SELECT * FROM {RECOM_TABLE} WHERE user_id = {user_id}"
        return sql_query_with_user_token(query, user_token).iloc[0].to_dict()
    except Exception as e:
        print(f"レコメンドデータ取得エラー: {str(e)}")
        return {}

# PUSH：レコメンドデータを取得
def push_load_recommendation(user_id: int) -> dict:
    """会員ごとのレコメンドデータを取得"""
    try:
        user_token = cfg.DATABRICKS_TOKEN
        query = f"""
            SELECT
                contents_list.content_category[0] AS cat,
                contents_list.content_img_b64[0]  AS img_b64,   -- 画像Base64
                size(contents_list.content_category) AS total
            FROM {RECOM_TABLE}
            WHERE user_id = {user_id}
            LIMIT 1
        """
        return sql_query_with_user_token(query, user_token).iloc[0].to_dict()
    except Exception as e:
        print(f"レコメンドデータ取得エラー: {str(e)}")
        return {}

# PUSH：航空券予約データセットを取得
def push_load_booking_data(user_id: int) -> pd.DataFrame:
    """航空券予約データを取得"""
    try:
        user_token = cfg.DATABRICKS_TOKEN
        query = f"""
            SELECT
                user_id,
                booking_id,
                flight_id,
                route_id,
                flight_date
            FROM {RECOM_TABLE}
            WHERE user_id = {user_id}
            LIMIT 1
        """
        return sql_query_with_user_token(query, user_token)
    except Exception as e:
        print(f"航空券予約データ取得エラー: {str(e)}")
        return pd.DataFrame()


# -------------------------
# Dashアプリ初期化とレイアウト定義
# -------------------------
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
feature_client = FeatureClient()
server = app.server

# CSSスタイルを直接適用
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            #notification-box {
                opacity: 0;
                transform: translateY(-50px);
                transition: opacity 0.5s ease-out, transform 0.5s ease-out;
            }
            #notification-box.show {
                opacity: 1;
                transform: translateY(0);
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# ----------------------
# レイアウト
# ----------------------
app.layout = html.Div([

    # タブ切り替え
    dcc.Tabs([
        # ---------- アプリプッシュメッセージ ----------
        dcc.Tab(label="アプリプッシュメッセージ", children=[
            html.Div(  # .page-container

                style={
                    'display': 'flex',
                    'justifyContent': 'center',             # 中央寄せに変更
                    'alignItems': 'center',                 # 垂直方向中央寄せに変更
                    'minHeight': '100vh',
                    'backgroundColor': '#121212',
                    'padding': '10px'
                },
                children=[
                    dcc.Store(id='previous-customer-id', data=1),
                    dcc.Store(id='page-load-trigger', data=0),  # ページロード検出用
                    
                    # ---------------------------------
                    # スマホ画像と通知メッセージ部分（左側）
                    # ---------------------------------
                    html.Div(
                        style={
                            'position': 'relative',
                            'display': 'inline-block',
                            'width': '450px',
                            'height': 'auto'
                        },
                        children=[
                            # ---------------------------------
                            # スマホフレーム画像（背景）
                            # ---------------------------------
                            html.Img(
                                src=smartphone_image_url,
                                style={
                                    'width': '100%',
                                    'height': 'auto',
                                    'position': 'relative',
                                    'zIndex': '1',
                                    'pointerEvents': 'none'
                                }
                            ),
                            
                            # ---------------------------------
                            # プッシュ通知コンテンツ
                            # ---------------------------------
                            html.Div(
                                id="notification-box",
                                style={
                                    "position": "absolute",
                                    "top": "15%",
                                    "left": "7%",
                                    "width": "86%",
                                    "height": "80%",
                                    "zIndex": "2",
                                    "overflowY": "auto",
                                    "padding": "5px",
                                },
                                children=[
                                    html.Div(
                                        style={
                                            "borderRadius": "10px",
                                            "backgroundColor": "rgba(255, 255, 255, 0.9)",
                                            "padding": "5px",
                                            "boxShadow": "2px 2px 2px rgba(0, 0, 0, 0.2)",
                                            "display": "flex",
                                            "alignItems": "center"
                                        },
                                        children=[
                                            # ---------------------------------
                                            # 商品画像部分
                                            # ---------------------------------
                                            html.Div(
                                                style={
                                                    "width": "30%",
                                                    "paddingRight": "5px",
                                                    "minHeight": "50px"
                                                },
                                                children=[
                                                    html.Img(
                                                        id='notification-image',
                                                        style={
                                                            "width": "100%",
                                                            "height": "auto",
                                                            "borderRadius": "5px",
                                                            "objectFit": "cover"
                                                        }
                                                    )
                                                ]
                                            ),
                                            # ---------------------------------
                                            # テキスト部分（幅を拡張）
                                            # ---------------------------------
                                            html.Div(
                                                style={
                                                    "width": "85%",
                                                    "paddingLeft": "5px",
                                                    "wordBreak": "break-word",
                                                    "color": "#000000",
                                                    "fontSize": "0.8rem"
                                                },
                                                children=[
                                                    html.Div(
                                                        style={"paddingBottom": "5px"},
                                                        children=[
                                                            html.Img(
                                                                src=bell_image_url,
                                                                style={
                                                                    "width": "20px", 
                                                                    "marginRight": "5px",
                                                                    "verticalAlign": "middle"
                                                                }
                                                            ),
                                                            html.Strong(
                                                                id="notification-title",
                                                                style={"verticalAlign": "middle"}
                                                            )
                                                        ]
                                                    ),
                                                    html.Div(
                                                        id="notification-subject",
                                                        style={"fontSize": "0.8rem"}
                                                    )
                                                ]
                                            )
                                        ]
                                    )
                                ]
                            )
                        ]
                    ),

                    # ---------------------------------
                    # フィルター・航空券予約番号・QRコード（右側）
                    # ---------------------------------
                    html.Div(
                        style={
                            'width': '40%',
                            'paddingLeft': '20px'
                        },
                        children=[
                            # ---------------------------------
                            # フィルター部分（user_id選択）
                            # ---------------------------------
                            # 説明文を追加
                            html.Div(
                                "会員IDを選択してください",
                                style={
                                    'fontSize': '1rem',
                                    'color': '#FFFFFF',
                                    'marginBottom': '10px'
                                }
                            ),
                            # ドロップダウンメニュー
                            dcc.Dropdown(
                                id='user-id-dropdown-push',
                                # 初期値としてダミー選択肢を設定（ページロード後に更新される）
                                options=[{'label': 'Loading...', 'value': 1}],
                                # value=1,
                                value=None,
                                placeholder="会員IDを選択してください"
                            ),

                            # ---------------------------------
                            # 航空券予約番号
                            # ---------------------------------
                            html.Div(id='recommend-data-container', style={'marginTop': '20px'}),

                            # ---------------------------------
                            # QRコード表示部分
                            # ---------------------------------
                            html.Div(
                                style={
                                    'marginTop': '20px',
                                    'width': '300px',           # QRコードの幅
                                    'height': '300px',          # QRコードの高さ
                                    'marginLeft': 'auto',       # 左右の余白を自動調整（中央寄せ）
                                    'marginRight': 'auto',      # 同様に右側も自動調整
                                    'display': 'block',         # 初期状態で表示する
                                },
                                children=[html.Img(src=qr_code_url, style={"width": "100%", "height": "100%"})]
                            ),
                        ]
                    )
                ]

            )
        ]),

        # ---------- 機内レコメンド ----------
        dcc.Tab(label="機内レコメンド", children=[
            html.Div(                              # .page-container
                [
                    # --------- 左側 (サイドバー) ---------
                    html.Div(
                        [
                            html.H4("会員IDを選択", className="mb-3 text-center", style={"color": "black"}),  # 文字色を黒に
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
# ----------------------
# フィルタ選択肢の初期設定
# ----------------------
@app.callback(
    Output('user-id-dropdown-push', 'options'),
    Input('page-load-trigger', 'data')
)
def update_dropdown_options(_):
    try:
        user_token = cfg.DATABRICKS_TOKEN
        user_ids = load_user_ids(user_token)
        return [{'label': f'{cid}', 'value': cid} for cid in user_ids]
    except Exception as e:
        print(f"ドロップダウン更新エラー: {str(e)}")
        return [{'label': f'Customer {i}', 'value': i} for i in range(1, 11)]

# ----------------------
# 2段階コールバックによるアニメーション制御
# ----------------------
# 1. 最初にクラス名をリセット（アニメーションをリセット）
@app.callback(
    Output('notification-box', 'className'),
    Output('previous-customer-id', 'data'),
    Input('user-id-dropdown-push', 'value'),
    State('previous-customer-id', 'data'),
    prevent_initial_call=True
)
def reset_animation(customer_id, previous_id):
    if customer_id == previous_id:
        return dash.no_update, dash.no_update
    return "", customer_id

# 2. 少し遅延させてから、showクラスを追加（アニメーション開始）
@app.callback(
    Output('notification-box', 'className', allow_duplicate=True),
    Input('previous-customer-id', 'data'),
    prevent_initial_call=True
)
def show_notification(_):
    time.sleep(0.1)  # 短い遅延を入れることでリセットとショーの間に時間差を作る
    return "show"

# 3. 通知内容の更新
@app.callback(
    Output('notification-title', 'children'),
    Output('notification-subject', 'children'),
    Output('notification-image', 'src'),
    Input('user-id-dropdown-push', 'value')
)
def update_notification_content(user_id):
    # まだ何も選ばれていないときは空表示
    if not user_id:
        return "", "", ""
    try:
        rec = push_load_recommendation(user_id)

        title   = f"機内エンタメ 厳選 {rec.get('total',6)} 作品をお届け⭐️"
        subject = f"あなた向けの『{rec.get('cat')}』など多数ご用意しています。続きを機内ディスプレイでどうぞ！🚀"

        # 取得した Base64 をそのまま <img src=""> に入れる
        img_src = (
            f"data:image/png;base64,{rec.get('img_b64','')}"
            if rec.get('img_b64')
            else ""                       # ← 画像が無ければ空
        )

        return title, subject, img_src
        
    except Exception as e:
        print(f"通知更新エラー: {str(e)}")
        return "エラーが発生しました", "", ""

# 4. 機内レコメンドデータの表示更新
@app.callback(
    Output('recommend-data-container', 'children'),
    Input('user-id-dropdown-push', 'value')
)
def update_booking_data(user_id):
    try:
        df = push_load_booking_data(user_id)

        if df.empty:
            return html.Div("航空券予約データがありません", style={'color': '#FFFFFF'})

        # 日本語表記のラベル
        labels = {
            'user_id': '会員ID',
            'booking_id': '航空券予約番号',
            'flight_id': '便名',
            'route_id': '区間',
            'flight_date': '出発日',
        }

        # 各カラムをカードとして表示
        card_elements = []
        for col, label in labels.items():
            card_elements.append(
                html.Div(
                    style={
                        'backgroundColor': '#1e1e2f',  # 濃いネイビー背景
                        'borderRadius': '8px',
                        'padding': '10px',
                        'marginBottom': '10px',
                        'boxShadow': '0 4px 6px rgba(0, 0, 0, 0.3)',  # カードの影
                        'display': 'flex',
                        'alignItems': 'center',
                        'justifyContent': 'space-between'
                    },
                    children=[
                        html.Div(label, style={
                            'fontWeight': 'bold',
                            'fontSize': '1rem',
                            'color': '#FFFFFF'
                        }),
                        html.Div(str(df.iloc[0][col]), style={
                            'fontSize': '1rem',
                            'color': '#FFFFFF'
                        })
                    ]
                )
            )

        return html.Div(card_elements, style={'marginTop': '20px'})

    except Exception as e:
        print(f"航空券予約データ更新エラー: {str(e)}")
        return html.Div("エラーが発生しました", style={'color': '#FFFFFF'})

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
