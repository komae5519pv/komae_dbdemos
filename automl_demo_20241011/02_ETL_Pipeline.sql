-- Databricks notebook source
-- MAGIC %md-sandbox  
-- MAGIC <!-- # Delta Live Tablesを使ったデータ取り込みと変換の簡素化
-- MAGIC
-- MAGIC このノートブックでは、データエンジニアとしてクレジット判断データベースを構築します。  
-- MAGIC 生データソースを取り込み、BIやMLワークロードに必要なテーブルを準備します。
-- MAGIC
-- MAGIC 私たちは、以下の4つのデータソースから新しいファイルをBlobストレージに送信し、これをデータウェアハウステーブルに段階的にロードします：
-- MAGIC
-- MAGIC - **内部銀行データ** *(KYC、口座、債権回収、申請、関係データ)* は銀行の内部リレーショナルデータベースから取得され、CDCパイプラインを通じて*1日に1回*取り込まれます。
-- MAGIC - **信用機関データ**（通常、XMLまたはCSV形式で提供され、API経由で*月に1回*アクセス）は政府機関（中央銀行など）から提供され、各顧客に関する多くの貴重な情報が含まれています。このデータを使用して、過去60日間に債務不履行があったかどうかを再計算します。
-- MAGIC - **パートナーデータ** - 銀行内部データを補完するために使用され、*週に1回*取り込まれます。この場合、通信会社データを使用して、銀行顧客の信用度や特性をさらに評価します。
-- MAGIC - **金融取引データ** - クレジットカード取引などの銀行取引であり、Kafkaストリームを通じて*リアルタイム*で提供されます。
-- MAGIC
-- MAGIC ## Delta Live Table: 新鮮で高品質なデータパイプラインを簡単に構築・管理
-- MAGIC
-- MAGIC Databricksは、Delta Live Table（DLT）を使用して、データエンジニアリングを誰でも使えるように簡素化しています。
-- MAGIC
-- MAGIC DLTを使うと、データアナリストがシンプルなSQLで高度なパイプラインを作成できます。
-- MAGIC
-- MAGIC <div>
-- MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
-- MAGIC       <strong>ETL開発の加速</strong> <br/>
-- MAGIC       アナリストやデータエンジニアがシンプルなパイプライン開発とメンテナンスで迅速に革新を進めることができます。
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
-- MAGIC       <strong>運用の複雑さを解消</strong> <br/>
-- MAGIC       複雑な管理タスクを自動化し、パイプライン運用の全体的な可視性を向上させます。
-- MAGIC     </p>
-- MAGIC   </div>
-- MAGIC   <div style="width: 48%; float: left">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
-- MAGIC       <strong>データの信頼性を確保</strong> <br/>
-- MAGIC       BI、データサイエンス、機械学習の精度と有用性を保証するために、組み込みの品質管理と品質モニタリングを提供します。
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
-- MAGIC       <strong>バッチとストリーミングの簡素化</strong> <br/>
-- MAGIC       バッチ処理やストリーミング処理に対応する自己最適化および自動スケーリングのデータパイプラインを提供します。
-- MAGIC     </p>
-- MAGIC </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <br style="clear:both">
-- MAGIC
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right;" width="200px">
-- MAGIC
-- MAGIC ## Delta Lake
-- MAGIC
-- MAGIC Lakehouseで作成するすべてのテーブルはDelta Lakeテーブルとして保存されます。Delta Lakeは、信頼性とパフォーマンスを提供するオープンストレージフレームワークです。  
-- MAGIC 多くの機能（ACIDトランザクション、DELETE/UPDATE/MERGE、クローンゼロコピー、変更データキャプチャなど）を提供しています。  
-- MAGIC Delta Lakeの詳細については、`dbdemos.install('delta-lake')`を実行してください。 -->

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## 個人向けクレジットを分析するためのDelta Live Tableパイプラインの構築
-- MAGIC
-- MAGIC この例では、前述の情報を使用してエンドツーエンドのDLTパイプラインを実装します。メダリオンアーキテクチャを使用しますが、スター・スキーマ、データボルト、または他のモデリング手法も使用できます。
-- MAGIC
-- MAGIC Autoloaderを使って新しいデータを段階的にロードし、その情報を強化してから、MLFlowからモデルを読み込み、クレジット判断の予測を行います。
-- MAGIC
-- MAGIC この情報は、クレジットスコア、クレジット判断、リスクを作成するためにDBSQLダッシュボードを構築する際に使用されます。
-- MAGIC
-- MAGIC 次のフローを実装しましょう：
-- MAGIC
-- MAGIC <div><img width="1000px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi_credit_decisioning_dlt_0.png" /></div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. パイプライン手動設定
-- MAGIC パイプライン > パイプラインを作成  
-- MAGIC 一般
-- MAGIC 下記内容で設定してください
-- MAGIC ```
-- MAGIC パイプライン名: komae_automl_e2e_demo_dlt
-- MAGIC パイプラインモード: トリガー（バッチ処理）
-- MAGIC ```
-- MAGIC
-- MAGIC パイプライン > パイプラインを作成  
-- MAGIC 配信先
-- MAGIC ```
-- MAGIC カタログ: <カタログ名を指定してください>
-- MAGIC スキーマ: automl_e2e_demo
-- MAGIC ```
-- MAGIC
-- MAGIC パイプライン > パイプラインを作成  
-- MAGIC クラスター
-- MAGIC ```
-- MAGIC クラスターポリシー: なし
-- MAGIC クラスタモード: 強化オートスケーリング
-- MAGIC ワーカーの最小数: 1
-- MAGIC ワーカーの最大数: 5
-- MAGIC ```
-- MAGIC
-- MAGIC パイプライン > パイプラインを作成 > Advanced > 設定を追加  
-- MAGIC キーと値を設定してください
-- MAGIC ```
-- MAGIC init_setting.catalog: <カタログ名を指定してください>
-- MAGIC init_setting.schema : automl_e2e_demo
-- MAGIC init_setting.volume : credit_raw_data
-- MAGIC ```
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2. Autoloader (cloud_files) を使用してデータを読み込む
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi_credit_decisioning_dlt_1.png"/>
-- MAGIC
-- MAGIC Autoloaderは、クラウドストレージから数百万のファイルを効率的に取り込むことを可能にし、大規模なスキーマ推論と進化をサポートします。
-- MAGIC
-- MAGIC Autoloaderの詳細については、`dbdemos.install('auto-loader')` を実行してください。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Credit Bureau(信用機関データ)
-- MAGIC
-- MAGIC `Credit Bureau`とは、個人の信用履歴、金融行動、信用度に関する情報であり、信用機関によって収集・管理されています。信用機関は、消費者のクレジットカード利用、ローン返済、その他の金融取引に関する情報を収集し、編纂する企業です。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE credit_bureau_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/${init_setting.catalog}/${init_setting.schema}/${init_setting.volume}/credit_bureau', 'json',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 2. Customer table(顧客情報)
-- MAGIC
-- MAGIC `Customer table`は内部のKYCプロセスから取得され、顧客に関連するデータが含まれています。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customer_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/${init_setting.catalog}/${init_setting.schema}/${init_setting.volume}/internalbanking/customer', 'csv',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true',
                     'cloudFiles.schemaHints', 'passport_expiry date, visa_expiry date, join_date date, dob date'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 3. Relationship table(関係データ)
-- MAGIC
-- MAGIC `Relationship table`は、銀行と顧客の関係を表しています。これも生データベースから取得されます。   
-- MAGIC どのような種類の口座やローンがあるか、どの程度の期間取引しているか、サービスの利用頻度などが含まれます。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE relationship_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/${init_setting.catalog}/${init_setting.schema}/${init_setting.volume}/internalbanking/relationship', 'csv',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 4. Account table(口座)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE account_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/${init_setting.catalog}/${init_setting.schema}/${init_setting.volume}/internalbanking/account', 'csv',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 5. Fund Transfer Table(金融取引データ)
-- MAGIC
-- MAGIC `Fund transfer`は、顧客が行った支払い取引を含むリアルタイムのデータストリームです。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE fund_trans_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/${init_setting.catalog}/${init_setting.schema}/${init_setting.volume}/fund_trans', 'json',
                map('inferSchema', 'true', 
                    'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC #### 6. Telco(通信会社データ)
-- MAGIC
-- MAGIC ここでは、外部および代替データソース、具体的には通信会社のパートナーデータを使用して、内部の銀行データを補強します。このデータには、銀行と通信会社の共通顧客に関する支払い情報が含まれています。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE telco_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/${init_setting.catalog}/${init_setting.schema}/${init_setting.volume}/telco', 'json',
                 map('inferSchema', 'true',
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3. データ品質を強化し、マテリアライズドテーブルを作成
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi_credit_decisioning_dlt_2.png"/>
-- MAGIC
-- MAGIC 次のレイヤーは、よく「シルバー」と呼ばれ、ブロンズレイヤーから**増分**データを取り込み、いくつかの情報をクレンジングします。
-- MAGIC
-- MAGIC また、異なるフィールドに対して[期待値](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html)を設定し、データ品質を強制し追跡しています。これにより、ダッシュボードが関連性を持ち、データ異常による潜在的なエラーを簡単に発見できるようにします。
-- MAGIC
-- MAGIC より高度なDLT機能については、```dbdemos.install('dlt-loans')```や、CDC/SCDT2の例として```dbdemos.install('dlt-cdc')```を実行してください。
-- MAGIC
-- MAGIC これらのテーブルはクリーンで、BIチームが使用する準備が整っています！
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 1. Fund transfer table(金融取引データ)

-- COMMAND ----------

-- DBTITLE 1,金融取引のシルバーテーブル作成
CREATE OR REFRESH LIVE TABLE fund_trans_silver AS
  SELECT
    payer_account.cust_id payer_cust_id, -- 支払い者の顧客ID
    payee_account.cust_id payee_cust_id, -- 受取人の顧客ID
    fund.* -- トランザクションの全データ
  FROM
    live.fund_trans_bronze fund
  LEFT OUTER JOIN live.account_bronze payer_account ON fund.payer_acc_id = payer_account.id
  LEFT OUTER JOIN live.account_bronze payee_account ON fund.payee_acc_id = payee_account.id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 2. Customer table(顧客情報)

-- COMMAND ----------

-- DBTITLE 1,顧客情報のシルバーテーブル作成(関係データと結合・年齢算出)
CREATE OR REFRESH LIVE TABLE customer_silver AS
  SELECT
    * EXCEPT (dob, customer._rescued_data, relationship._rescued_data, relationship.id, relationship.operation),
    year(dob) AS birth_year
  FROM
    live.customer_bronze customer
  LEFT OUTER JOIN live.relationship_bronze relationship ON customer.id = relationship.cust_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 3. Account table(口座)

-- COMMAND ----------

-- DBTITLE 1,口座のシルバーテーブル作成(口座数・USD残高集計)
-- 処理内容に基づいて口座数と平均残高を集計し、さらにUSD通貨に特化した残高情報を結合
CREATE OR REFRESH LIVE TABLE account_silver AS
  WITH cust_acc AS (
      SELECT cust_id, count(1) num_accs, avg(balance) avg_balance 
        FROM live.account_bronze
        GROUP BY cust_id
    )
  SELECT
    acc_usd.cust_id,
    num_accs,                                -- 顧客の口座数
    avg_balance,                             -- 平均残高
    balance balance_usd,                     -- 米ドル残高
    available_balance available_balance_usd, -- 米ドル利用可能残高
    operation                                -- 口座の操作
  FROM
    cust_acc
  LEFT OUTER JOIN live.account_bronze acc_usd ON cust_acc.cust_id = acc_usd.cust_id AND acc_usd.currency = 'USD'

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ### 4. 分析 & MLのための、集計レイヤー
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi_credit_decisioning_dlt_3.png"/>
-- MAGIC
-- MAGIC
-- MAGIC 私たちはDelta Live Tablesを使用してDelta Lake内のすべてのテーブルをキュレーションし、リアルタイムで結合やマスキング、データ制約を適用できるようにしています。データサイエンティストはこれらのデータセットを使用して、特に信用度を予測するための高品質なモデルを構築することができます。また、Unity Catalogの機能として機密データをマスキングしているため、データサイエンティストからデータアナリスト、ビジネスユーザーまで、多くの下流ユーザーに自信を持ってデータを公開することができます。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 1. Credit bureau cleanup(信用機関のクレンジング)
-- MAGIC
-- MAGIC まず、ここでDelta Lakeのテーブルから取得した信用機関データを取り込みます。通常、このデータはAPIを介して取り込まれ、クラウドのオブジェクトストレージに保存されます。

-- COMMAND ----------

-- DBTITLE 1,信用情報のゴールドテーブルを作成
-- 信用情報のゴールドテーブルを作成
-- 顧客IDがNULLでないことを確認し、違反した場合は行を削除する制約を追加
CREATE OR REFRESH LIVE TABLE credit_bureau_gold
  (CONSTRAINT CustomerID_not_null EXPECT (CUST_ID IS NOT NULL) ON VIOLATION DROP ROW)
AS
  SELECT * FROM live.credit_bureau_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 2. Fund transfer table(金融取引データ)
-- MAGIC
-- MAGIC `fund transfer`は、顧客と他の人物との間で行われたピアツーピア(P2P)の支払いを表しています。これにより、各顧客の支払い頻度や金額に関する属性を理解し、クレジットリスクの評価に役立てることができます。

-- COMMAND ----------

-- DBTITLE 1,金融取引データのゴールドテーブル作成
-- 過去12ヶ月、6ヶ月、3ヶ月における送金・受取取引の回数や金額を顧客ごとに集計し、金融取引データのゴールドテーブルを作成します。
CREATE OR REFRESH LIVE TABLE fund_trans_gold AS (
  WITH 12m_payer AS (SELECT
                      payer_cust_id,
                      COUNT(DISTINCT payer_cust_id) dist_payer_cnt_12m,
                      COUNT(1) sent_txn_cnt_12m,
                      SUM(txn_amt) sent_txn_amt_12m,
                      AVG(txn_amt) sent_amt_avg_12m
                    FROM live.fund_trans_silver WHERE cast(datetime AS date) >= cast('2022-01-01' AS date)
                    GROUP BY payer_cust_id),
      12m_payee AS (SELECT
                        payee_cust_id,
                        COUNT(DISTINCT payee_cust_id) dist_payee_cnt_12m,
                        COUNT(1) rcvd_txn_cnt_12m,
                        SUM(txn_amt) rcvd_txn_amt_12m,
                        AVG(txn_amt) rcvd_amt_avg_12m
                      FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= CAST('2022-01-01' AS date)
                      GROUP BY payee_cust_id),
      6m_payer AS (SELECT
                    payer_cust_id,
                    COUNT(DISTINCT payer_cust_id) dist_payer_cnt_6m,
                    COUNT(1) sent_txn_cnt_6m,
                    SUM(txn_amt) sent_txn_amt_6m,
                    AVG(txn_amt) sent_amt_avg_6m
                  FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= CAST('2022-07-01' AS date)
                  GROUP BY payer_cust_id),
      6m_payee AS (SELECT
                    payee_cust_id,
                    COUNT(DISTINCT payee_cust_id) dist_payee_cnt_6m,
                    COUNT(1) rcvd_txn_cnt_6m,
                    SUM(txn_amt) rcvd_txn_amt_6m,
                    AVG(txn_amt) rcvd_amt_avg_6m
                  FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= CAST('2022-07-01' AS date)
                  GROUP BY payee_cust_id),
      3m_payer AS (SELECT
                    payer_cust_id,
                    COUNT(DISTINCT payer_cust_id) dist_payer_cnt_3m,
                    COUNT(1) sent_txn_cnt_3m,
                    SUM(txn_amt) sent_txn_amt_3m,
                    AVG(txn_amt) sent_amt_avg_3m
                  FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= CAST('2022-07-01' AS date)
                  GROUP BY payer_cust_id),
      3m_payee AS (SELECT
                    payee_cust_id,
                    COUNT(DISTINCT payee_cust_id) dist_payee_cnt_3m,
                    COUNT(1) rcvd_txn_cnt_3m,
                    SUM(txn_amt) rcvd_txn_amt_3m,
                    AVG(txn_amt) rcvd_amt_avg_3m
                  FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= CAST('2022-07-01' AS date)
                  GROUP BY payee_cust_id)        
  SELECT c.cust_id, 
    12m_payer.* EXCEPT (payer_cust_id),
    12m_payee.* EXCEPT (payee_cust_id), 
    6m_payer.* EXCEPT (payer_cust_id), 
    6m_payee.* EXCEPT (payee_cust_id), 
    3m_payer.* EXCEPT (payer_cust_id), 
    3m_payee.* EXCEPT (payee_cust_id) 
  FROM live.customer_silver c 
    LEFT JOIN 12m_payer ON 12m_payer.payer_cust_id = c.cust_id
    LEFT JOIN 12m_payee ON 12m_payee.payee_cust_id = c.cust_id
    LEFT JOIN 6m_payer ON 6m_payer.payer_cust_id = c.cust_id
    LEFT JOIN 6m_payee ON 6m_payee.payee_cust_id = c.cust_id
    LEFT JOIN 3m_payer ON 3m_payer.payer_cust_id = c.cust_id
    LEFT JOIN 3m_payee ON 3m_payee.payee_cust_id = c.cust_id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 3. Telco table(通信会社テーブル)
-- MAGIC
-- MAGIC `Telco table`は、特定の見込み客や顧客に関するすべての支払いデータを表しており、非銀行系の信用源に基づいて信用度を理解するために使用されます。

-- COMMAND ----------

-- DBTITLE 1,通信会社データのゴールドテーブル作成
-- 通信会社データを顧客データと結合し、通信会社データのゴールドテーブルを作成します。
CREATE OR REFRESH LIVE TABLE telco_gold AS
SELECT
  customer.id cust_id,
  telco.*
FROM
  live.telco_bronze telco
  LEFT OUTER JOIN live.customer_bronze customer ON telco.user_phone = customer.mobile_phone

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 4. Customer table
-- MAGIC
-- MAGIC 顧客データは、個人識別情報（PII）や顧客属性の記録システムを表しており、他のファクトテーブルと結合されるデータです。

-- COMMAND ----------

-- DBTITLE 1,顧客データのゴールドテーブル作成
-- 顧客データと口座データを結合し、顧客の口座情報を含むゴールドテーブルを作成します。
CREATE OR REFRESH LIVE TABLE customer_gold AS
SELECT
  customer.*,
  account.avg_balance,
  account.num_accs,
  account.balance_usd,
  account.available_balance_usd
FROM
  live.customer_silver customer
  LEFT OUTER JOIN live.account_silver account ON customer.cust_id = account.cust_id 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 5. データサイエンティストユーザー向けにfirstnameを削除したビューを追加
-- MAGIC
-- MAGIC Databricks Delta Lakeテーブルでデータをマスキングする最善の方法は、動的ビューや`is_member`のような関数を使用して、グループに基づいてデータを暗号化またはマスキングすることです。この場合、ユーザーグループに基づいてPII（個人識別情報）データをマスキングする必要があり、`aes_encrypt`という組み込みの暗号化関数を使用しています。さらに、セキュリティの理由から暗号化キー自体はDatabricksのシークレットに保存されています。

-- COMMAND ----------

CREATE LIVE VIEW customer_gold_secured AS
SELECT
  c.* EXCEPT (first_name),
  CASE
    WHEN is_member('data-science-users')
    THEN base64(aes_encrypt(c.first_name, 'YOUR_SECRET_FROM_MANAGER')) -- save secret in Databricks manager and load it in SQL with secret('<YOUR_SCOPE> ', '<YOUR_SECRET_NAME>')
    ELSE c.first_name
  END AS first_name
FROM
  live.customer_gold AS c

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: secure and share data with Unity Catalog
-- MAGIC
-- MAGIC Now that we have ingested all these various sources of data, we can jump to the:
-- MAGIC
-- MAGIC * [Governance with Unity Catalog notebook]($../02-Data-Governance/02-Data-Governance-credit-decisioning) to see how to grant fine-grained access to every user and persona and explore the **data lineage graph**,
-- MAGIC * [Feature Engineering notebook]($../03-Data-Science-ML/03.1-Feature-Engineering-credit-decisioning) and start creating features for our machine learning models,
-- MAGIC * Go back to the [Introduction]($../00-Credit-Decisioning).
