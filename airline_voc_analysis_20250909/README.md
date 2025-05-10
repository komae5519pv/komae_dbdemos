# Bricks Airways 顧客レビューを用いたVoC分析デモ
<div style="display: flex; justify-content: space-between;">
  <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/airline_voc_analysis_20250909/_imgs_tutorial/aircraft_taking_off.png?raw=true' width='70%'/>
</div>

### 概要
Bricks Airways（架空の航空会社）の各サービスに対する顧客レビューデータを用いたVoC（Voice of Customer）分析のデモです。顧客レビューはアンケート設計が行われていないため、さまざまな論点が複合的に混ざった自由記述のデータです。本デモでは、LLMを用いてこれらの顧客レビューデータから特定のカテゴリに該当する文章の抽出・要約を行うことで、文章のカテゴライズを自動化します。  
さらに、カテゴライズされたデータをもとに、ダッシュボードやGenie（Databricksが提供する自然言語による分析ツール）で分析を行います。

### カラム説明
本デモで用いる顧客レビュー（デモ用のサンプルデータ）には、次の情報が含まれます。
| Column Name                       | Description                                                                   |
| --------------------------------- | ----------------------------------------------------------------------------- |
| **review\_id**                    | レビューを一意に識別するシステム採番 ID（主キー）                                                    |
| **user\_id**                      | レビューを書いた会員の ID（会員テーブルへの外部キー）                                                  |
| **review\_date**                  | レビューが投稿された日付                                                                  |
| **review**                        | 顧客が記入したレビュー本文                                                                 |
| **flight\_id**                    | 搭乗機を示す航空機 ID（例: “A390-100”）                                                   |
| **flight\_date**                  | フライト出発日                                                                       |
| **route\_id**                     | 区間を示すルート ID（出発空港コード-到着空港コード）                                                  |
| **seat\_type**                    | 旅行クラス（例: “Economy Class”, “Business Class”, “First Class”, “Premium Economy”） |
| **is\_recommended**               | 顧客が本サービスを推奨するかどうか（`true` / `false`）                                           |
| **rating\_overall**               | 全体評価                                                                          |
| **rating\_seat\_comfort**         | 座席の快適さに対する評価                                                                  |
| **rating\_cabin\_staff\_service** | 客室スタッフのサービスに対する評価                                                             |
| **rating\_ground\_service**       | 地上サービスに対する評価                                                                  |
| **rating\_value\_for\_money**     | 価格に対する価値の評価                                                                   |
| **rating\_food\_beverages**       | 飲食サービスに対する評価                                                                  |
| **rating\_ife**                   | 機内エンターテインメントに対する評価                                                            |
| **rating\_wifi\_connectivity**    | 機内 Wi-Fi の接続性に対する評価                                                           |


#### 処理概要
1. LLMを使ってでレビューから論点抽出＆要約
1. 論点ごとに抽出された要約文章をレコード展開
1. ダッシュボードで可視化