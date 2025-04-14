import os
from databricks import sql
import pandas as pd
import dash
from dash import dash_table
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
from databricks.sdk.core import Config
import flask
import time

# ---------------------------
# 変数設定
# ---------------------------
MY_CATALOG = "komae_demo_v2"                        # 任意のカタログ名に変えてください
MY_SCHEMA = "product_recommendation_stadium"        # スキーマ名

# 環境変数
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHAUS_ID must be set"

# スマホ画像URL
smartphone_image_url = 'https://github.com/komae5519pv/komae_dbdemos/blob/main/product_recommendation_20250411/_images/stagium_apps.png?raw=true'

# bell画像URL
bell_image_url = 'https://github.com/komae5519pv/komae_dbdemos/blob/main/product_recommendation_20250411/_images/bell.png?raw=true'

# Databricks設定
cfg = Config()

def sql_query_with_user_token(query: str, user_token: str) -> pd.DataFrame:
    """ユーザートークンを使用してSQLクエリを実行"""
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        access_token=user_token
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# ------------------------------
# 会員IDリストを取得
# ------------------------------
def load_customer_ids(user_token: str) -> list:
    """顧客IDのリストを取得"""
    try:
        query = f"""
            SELECT DISTINCT customer_id
            FROM {MY_CATALOG}.{MY_SCHEMA}.gd_final_recommendations
            ORDER BY 1 ASC
        """
        df = sql_query_with_user_token(query, user_token)
        if not df.empty:
            return df['customer_id'].tolist()
        return list(range(1, 11))  # エラー時のフォールバック
    except Exception as e:
        print(f"顧客ID取得エラー: {str(e)}")
        return list(range(1, 11))  # エラー時のフォールバック

# ------------------------------
# レコメンドデータセットを取得
# ------------------------------
def load_recommendation(customer_id: int) -> dict:
    """カタログから顧客ごとのレコメンデーションを取得"""
    try:
        user_token = flask.request.headers.get('X-Forwarded-Access-Token')
        query = f"""
            SELECT * 
            FROM {MY_CATALOG}.{MY_SCHEMA}.gd_final_recommendations 
            WHERE customer_id = {customer_id}
        """
        return sql_query_with_user_token(query, user_token).iloc[0].to_dict()
    except Exception as e:
        print(f"データ取得エラー: {str(e)}")
        return {}

# ------------------------------
# 購買データセットを取得
# ------------------------------
def load_purchase_data(customer_id: int) -> pd.DataFrame:
    """顧客の購買データを取得"""
    try:
        user_token = flask.request.headers.get('X-Forwarded-Access-Token')
        query = f"""
            SELECT * 
            FROM {MY_CATALOG}.{MY_SCHEMA}.gd_final_recommendations 
            WHERE customer_id = {customer_id}
        """
        return sql_query_with_user_token(query, user_token)
    except Exception as e:
        print(f"購買データ取得エラー: {str(e)}")
        return pd.DataFrame()

# ------------------------------
# Dashアプリ初期化
# ------------------------------
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
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
app.layout = html.Div(
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
                        "left": "12%",
                        "width": "76%",
                        "height": "70%",
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
        # フィルターと購買データ部分（右側）
        # ---------------------------------
        html.Div(
            style={
                'width': '40%',
                'paddingLeft': '20px'
            },
            children=[
                # ---------------------------------
                # フィルター部分（customer_id選択）
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
                    id='customer-id-dropdown',
                    # 初期値としてダミー選択肢を設定（ページロード後に更新される）
                    options=[{'label': 'Loading...', 'value': 1}],
                    value=1,
                    placeholder="会員IDを選択してください"
                ),

                # ---------------------------------
                # 購買データ表示部分
                # ---------------------------------
                html.Div(id='purchase-data-container', style={'marginTop': '20px'})
            ]
        )
    ]
)

# ----------------------
# ドロップダウン選択肢の動的更新
# ----------------------
@app.callback(
    Output('customer-id-dropdown', 'options'),
    Input('page-load-trigger', 'data')
)
def update_dropdown_options(_):
    try:
        user_token = flask.request.headers.get('X-Forwarded-Access-Token')
        customer_ids = load_customer_ids(user_token)
        return [{'label': f'{cid}', 'value': cid} for cid in customer_ids]
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
    Input('customer-id-dropdown', 'value'),
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
    Input('customer-id-dropdown', 'value')
)
def update_notification_content(customer_id):
    try:
        recommendation = load_recommendation(customer_id)
        
        title = recommendation.get('campaign_title', 'お得なキャンペーンオファー⭐️')
        subject = (
            f"こんにちは、{recommendation.get('customer_name', 'お客様')}さん！ "
            f"大好きな{recommendation.get('item', '商品')}を特別価格でご提供します♫"
            f"{recommendation.get('vendor_name', '店舗')}で買えるのでチェックしてみてくださいね！🚀"
        )
        image_url = recommendation.get(
            'item_img_url', 
            'https://github.com/komae5519pv/komae_dbdemos/blob/main/product_recommendation_20250411/_images/01_burger.jpg?raw=true'
        )
        
        return title, subject, image_url
    
    except Exception as e:
        print(f"通知更新エラー: {str(e)}")
        return (
            'エラーが発生しました',
            '',
            ''
        )

# 4. 購買データの表示更新
@app.callback(
    Output('purchase-data-container', 'children'),
    Input('customer-id-dropdown', 'value')
)
def update_purchase_data(customer_id):
    try:
        df = load_purchase_data(customer_id)

        if df.empty:
            return html.Div("購買データがありません", style={'color': '#FFFFFF'})

        # 日本語表記のラベル
        labels = {
            'customer_name': '顧客名',
            'phone_number': '電話番号',
            'vendor_name': '店舗名',
            'item': '商品',
            'rating': '評価',
            'distance': '距離',
            'section': 'セクション'
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
        print(f"購買データ更新エラー: {str(e)}")
        return html.Div("エラーが発生しました", style={'color': '#FFFFFF'})

if __name__ == "__main__":
    app.run_server(debug=True)