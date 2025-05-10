from dash import html

# 親ディレクトリからインポート
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # 親ディレクトリをsys.pathに追加

class RecommendationGrid:
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
                                        # Base64 文字列を直接 <img src=""> に埋め込み、画像を表示
                                        html.Img(
                                            src=f"data:image/png;base64,{img_b64}",
                                            className="tile-image"
                                        ),
                                        className="square-img-box",
                                    ),
                                    html.Div(cat, className="tile-category"),
                                ],
                                className="tile",
                            )
                            for cat, img_b64 in zip(
                                out["contents_list"]["content_category"],
                                out["contents_list"]["content_img_b64"],
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