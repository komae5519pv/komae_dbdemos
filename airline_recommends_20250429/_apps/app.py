from dash import Dash, html, dcc, Input, Output
import dash_bootstrap_components as dbc
from feature_client import FeatureClient
from components import RecommendationGrid
from databricks import sql
from config import Config

app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
feature_client = FeatureClient()

# ---------- 新しいレイアウト ----------
app.layout = html.Div([

    # タブ切り替え部分
    dcc.Tabs([
        dcc.Tab(label="アプリプッシュメッセージ", children=[
            html.Div([
                html.H4("アプリプッシュメッセージページ"),
                html.P("ここにプッシュメッセージを表示するコンテンツが入ります。")
            ])
        ]),

        dcc.Tab(label="機内レコメンド", children=[
            # レコメンドページのレイアウト
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

# ---------- コールバック ----------
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