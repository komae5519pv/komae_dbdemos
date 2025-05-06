from dash import html
from PIL import Image
import io
import base64
from databricks.sdk import WorkspaceClient
from config import Config

# 親ディレクトリからインポート
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # 親ディレクトリをsys.pathに追加

# Databricks接続設定
cfg = Config()

class RecommendationGrid:
    @staticmethod
    def encode_image_to_base64(image_path, quality=50, new_width=300):
        """
        Databricks Volumeパスの画像をBase64エンコード
        PATを使用して認証
        """
        try:
            # WorkspaceClientのインスタンス作成（PAT認証）
            w = WorkspaceClient(
                host=cfg.DATABRICKS_HOST,
                token=cfg.DATABRICKS_TOKEN,  # トークンを取得
                auth_type="pat"                                                # 明示的に認証方式を指定
            )

            # 画像が格納されているパスのファイルをダウンロード
            file_contents = w.files.download(image_path).contents.read()

            # 画像を圧縮してBase64エンコードする処理
            with Image.open(io.BytesIO(file_contents)) as img:
                # 解像度を変更（幅をnew_widthに設定）
                width_percent = (new_width / float(img.size[0]))
                new_height = int((float(img.size[1]) * float(width_percent)))
                img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

                # 圧縮してバッファに保存
                buffered = io.BytesIO()
                img.save(buffered, format="PNG", quality=quality)  # qualityを指定して圧縮
                # Base64エンコード
                b64_string = base64.b64encode(buffered.getvalue()).decode('utf-8')

            return f"data:image/png;base64,{b64_string}"

        except Exception as e:
            print(f"Error encoding image {image_path}: {e}")
            return "data:image/png;base64,<default_base64_image_data>"

    @staticmethod
    def create_grid(response_data: dict) -> html.Div:
        recommendation_blocks = [
            html.Div(
                [
                    html.Div(
                        f"お客様[ 会員ID: {out['user_id']} | フライトID: {out['flight_id']} ]だけのおすすめ情報♪",
                        className="user-header",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Div(
                                        # Base64エンコードされた画像を表示
                                        html.Img(src=RecommendationGrid.encode_image_to_base64(img), className="tile-image"),
                                        className="square-img-box",
                                    ),
                                    html.Div(cat, className="tile-category"),
                                ],
                                className="tile",
                            )
                            for cat, img in zip(
                                out["contents_list"]["content_category"],
                                out["contents_list"]["content_img_url"],  # Volumeパスをそのまま使用
                            )
                        ],
                        className="tile-container",
                    ),
                ],
                className="recommend-block",
            )
            for out in response_data["outputs"]
        ]

        return html.Div(recommendation_blocks, className="recommend-outer")