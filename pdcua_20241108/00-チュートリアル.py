# Databricks notebook source
# MAGIC %md
# MAGIC # 1. train.csvから自分のスキーマにDelta Tableを作成してみよう。

# COMMAND ----------

catalog_name = "komae" # ご自身のカタログ名に変更してください
schema_name = "komae_schema" # ご自身のスキーマ名に変更してください
volume_name = "pcdua_volume"

# COMMAND ----------

# カタログ、スキーマ、ボリューム作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name};")

print(f"カタログパス: {catalog_name}")
print(f"スキーマパス: {catalog_name}.{schema_name}")
print(f"ボリュームパス: /Volumes/{catalog_name}/{schema_name}/{volume_name}")

# COMMAND ----------

spark.sql(f"USE {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS train;

# COMMAND ----------

df = spark.read.csv("/Volumes/pcdua_shared_catalog/default/pcdua_volume/train.csv", header=True, inferSchema=True)
df.write.format("delta").mode("overwrite").saveAsTable("train")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from train

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from train
# MAGIC -- limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. 各項目カラムにコメントをつけてみる。

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE train IS "`train`テーブルには、建物とその賃貸情報に関するデータが含まれています。データは特定の時間範囲に抽出された対象年月を示しており、各建物の代表的な賃料も含まれています。この賃料は分析の目的変数として使用されます。その他の重要なフィールドには、建物のID、状態、作成日時、修正日時、建物種別、名前、住所があります。さらに、テーブルには各建物の総戸数、地理座標、建物の構造タイプ、床面積と階数に関連するさまざまな測定値の情報も提供されています。";

# COMMAND ----------

# MAGIC %sql
# MAGIC -- train
# MAGIC COMMENT ON TABLE train IS "`train`テーブルには、建物とその賃貸情報に関するデータが含まれています。データは特定の時間範囲に抽出された対象年月を示しており、各建物の代表的な賃料も含まれています。この賃料は分析の目的変数として使用されます。その他の重要なフィールドには、建物のID、状態、作成日時、修正日時、建物種別、名前、住所があります。さらに、テーブルには各建物の総戸数、地理座標、建物の構造タイプ、床面積と階数に関連するさまざまな測定値の情報も提供されています。";
# MAGIC ALTER TABLE train ALTER COLUMN target_ym  COMMENT '対象年月 各レコードの抽出対象年月日 yyyymm';
# MAGIC ALTER TABLE train ALTER COLUMN money_room  COMMENT '賃料(代表) 対応する物件の賃料。目的変数となる。';
# MAGIC ALTER TABLE train ALTER COLUMN building_id  COMMENT '棟ID AUTO_INCREMENT、UNSIGNED　確認事項参照';
# MAGIC ALTER TABLE train ALTER COLUMN building_status  COMMENT '状態 1: 棟が存在する、9: 棟が存在しない';
# MAGIC ALTER TABLE train ALTER COLUMN building_create_date  COMMENT '作成日時 データ作成日時';
# MAGIC ALTER TABLE train ALTER COLUMN building_modify_date  COMMENT '修正日時 データ修正日時';
# MAGIC ALTER TABLE train ALTER COLUMN building_type  COMMENT '建物種別 1: マンション, 3: アパート, その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN building_name  COMMENT '建物名 ';
# MAGIC ALTER TABLE train ALTER COLUMN building_name_ruby  COMMENT '建物名フリガナ ';
# MAGIC ALTER TABLE train ALTER COLUMN homes_building_name  COMMENT 'HOMES 建物名 掲載時の物件名';
# MAGIC ALTER TABLE train ALTER COLUMN homes_building_name_ruby  COMMENT 'HOMES 建物名フリガナ 掲載時の物件名ルビ';
# MAGIC ALTER TABLE train ALTER COLUMN unit_count  COMMENT '総戸数/総区画数 物件の総数 (部屋:総戸数 土地:総区画数)';
# MAGIC ALTER TABLE train ALTER COLUMN full_address  COMMENT '住所 全住所文字列';
# MAGIC ALTER TABLE train ALTER COLUMN lon  COMMENT '経度 世界測地系';
# MAGIC ALTER TABLE train ALTER COLUMN lat  COMMENT '緯度 世界測地系';
# MAGIC ALTER TABLE train ALTER COLUMN building_structure  COMMENT '建物構造 1:木造 2:ブロック 3:鉄骨造 4:RC 5:SRC 6:PC 7:HPC 9:その他 10:軽量鉄骨 11:ALC 12:鉄筋ブロック 13:CFT(コンクリート充填鋼管)';
# MAGIC ALTER TABLE train ALTER COLUMN total_floor_area  COMMENT '延べ床面積 建物全体の床面積';
# MAGIC ALTER TABLE train ALTER COLUMN building_area  COMMENT '建築面積 建築面積';
# MAGIC ALTER TABLE train ALTER COLUMN floor_count  COMMENT '建物階数(地上) ';
# MAGIC ALTER TABLE train ALTER COLUMN basement_floor_count  COMMENT '建物階数(地下) ';
# MAGIC ALTER TABLE train ALTER COLUMN year_built  COMMENT '築年月 建築年月 yyyymm';
# MAGIC ALTER TABLE train ALTER COLUMN building_land_area  COMMENT '土地面積 ';
# MAGIC ALTER TABLE train ALTER COLUMN land_area_all  COMMENT '敷地全体面積 敷地全体の面積';
# MAGIC ALTER TABLE train ALTER COLUMN unit_area_min  COMMENT '専有面積 下限 ';
# MAGIC ALTER TABLE train ALTER COLUMN unit_area_max  COMMENT '専有面積 上限 ';
# MAGIC ALTER TABLE train ALTER COLUMN building_land_chimoku  COMMENT '地目 地目 　1 :宅地 2: 田 3:畑  4:山林 5 : 雑種地 9 : その他 10:原野 11:田･畑 その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN land_youto  COMMENT '用途地域 1:第一種低層住居専用 2:第二種中高層住居専用 3:第二種住居 4:近隣商業 5:商業 6:準工業 7:工業 8:工業専用 10:第二種低層住居専用 11:第一種中高層住居専用 12:第一種住居 13:準住居 14:田園住居地域 99:無指定';
# MAGIC ALTER TABLE train ALTER COLUMN land_toshi  COMMENT '都市計画 1:市街化区域 2:市街化調整区域 3:非線引区域 4:都市計画区域外 その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN land_chisei  COMMENT '地勢 1:平坦 2:高台 3:低地 4:ひな段 5:傾斜地 9:その他　その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN land_area_kind  COMMENT '土地面積計測方式 1:公簿 2:実測 その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN land_setback_flg  COMMENT 'セットバック 1:無し 2:有り その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN land_setback  COMMENT 'セットバック量 単位：平米';
# MAGIC ALTER TABLE train ALTER COLUMN land_kenpei  COMMENT '建ぺい率 賃貸：土地、売買：土地で「都市計画」が 1:市街化区域 の場合に必須
# MAGIC 単位：%';
# MAGIC ALTER TABLE train ALTER COLUMN land_youseki  COMMENT '容積率 賃貸：土地、売買：土地で都市計画」が 1:市街化区域 の場合に必須
# MAGIC 単位：%';
# MAGIC ALTER TABLE train ALTER COLUMN land_road_cond  COMMENT '接道状況 1:一方 2:角地 3:三方 4:四方 5:二方(除角地) 10:接道なし　その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN land_seigen  COMMENT '法令上の制限 ';
# MAGIC ALTER TABLE train ALTER COLUMN building_area_kind  COMMENT '建物面積計測方式 1:壁芯 2:内法　その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN management_form  COMMENT '管理形態 売買：マンションの場合に必須  1:自主管理 2:一部委託 3:全部委託';
# MAGIC ALTER TABLE train ALTER COLUMN management_association_flg  COMMENT '管理組合有無 1:無し 2:有り　その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN reform_exterior  COMMENT 'リフォーム箇所（外装） スラッシュ(/) 区切り形式 1:屋根 2:外壁';
# MAGIC ALTER TABLE train ALTER COLUMN reform_exterior_other  COMMENT 'リフォーム箇所その他（外装） その他外装リフォーム箇所について';
# MAGIC ALTER TABLE train ALTER COLUMN reform_exterior_date  COMMENT '施工完了年月（外装） YYYYMM形式';
# MAGIC ALTER TABLE train ALTER COLUMN reform_common_area  COMMENT 'リフォーム箇所（共用部分） 共用部分リフォーム箇所について';
# MAGIC ALTER TABLE train ALTER COLUMN reform_common_area_date  COMMENT '施工完了年月（共用部分） YYYYMM形式';
# MAGIC ALTER TABLE train ALTER COLUMN building_tag_id  COMMENT "タグ情報、スラッシュ(/) 区切り形式（例 '230801/310501/220301/290901'）、スラッシュ(/) で分割した値は、`setsubi_info`テーブルの'setsubi_id'にリンク";
# MAGIC ALTER TABLE train ALTER COLUMN unit_id  COMMENT '住戸ID AUTO_INCREMENT、UNSIGNED　確認事項参照';
# MAGIC ALTER TABLE train ALTER COLUMN unit_name  COMMENT '戸名称 戸名称（部屋番号名）';
# MAGIC ALTER TABLE train ALTER COLUMN name_ruby  COMMENT '戸名称フリガナ 戸名称のフリガナ';
# MAGIC ALTER TABLE train ALTER COLUMN room_floor  COMMENT '所在階数 ';
# MAGIC ALTER TABLE train ALTER COLUMN balcony_area  COMMENT 'バルコニー面積 ';
# MAGIC ALTER TABLE train ALTER COLUMN dwelling_unit_window_angle  COMMENT '主要採光面 ';
# MAGIC ALTER TABLE train ALTER COLUMN room_count  COMMENT '間取部屋数 ';
# MAGIC ALTER TABLE train ALTER COLUMN unit_area  COMMENT '専有面積 ';
# MAGIC ALTER TABLE train ALTER COLUMN floor_plan_code  COMMENT '間取り種類コード 部屋数+間取種類 (Sは丸める) 間取り種類 10:R 20:K,SK 30:DK,SDK 40:LK,SLK 50:LDK,SLDK';
# MAGIC ALTER TABLE train ALTER COLUMN reform_date  COMMENT 'リフォーム実施年月日 ';
# MAGIC ALTER TABLE train ALTER COLUMN reform_place  COMMENT 'リフォーム箇所 ';
# MAGIC ALTER TABLE train ALTER COLUMN reform_place_other  COMMENT 'リフォーム箇所その他 ';
# MAGIC ALTER TABLE train ALTER COLUMN reform_wet_area  COMMENT 'リフォーム箇所（水回り） スラッシュ(/) 区切り形式 1:キッチン 2:浴室 3:トイレ 4:洗面所 5:給湯器 6:給排水管';
# MAGIC ALTER TABLE train ALTER COLUMN reform_wet_area_other  COMMENT 'リフォーム箇所その他（水回り） その他水回りリフォーム箇所について';
# MAGIC ALTER TABLE train ALTER COLUMN reform_wet_area_date  COMMENT '施工完了年月（水回り） YYYYMM形式';
# MAGIC ALTER TABLE train ALTER COLUMN reform_interior  COMMENT 'リフォーム箇所（内装） スラッシュ(/) 区切り形式 1:内装全面（床、壁、天井、建具すべて） 2:壁、天井（クロス、塗装等） 3:全室クロス張替 4:床（フローリング等） 5:建具（室内ドア等） 6:サッシ';
# MAGIC ALTER TABLE train ALTER COLUMN reform_interior_other  COMMENT 'リフォーム箇所その他（内装） その他内装リフォーム箇所について';
# MAGIC ALTER TABLE train ALTER COLUMN reform_interior_date  COMMENT '施工完了年月（内装） YYYYMM形式';
# MAGIC ALTER TABLE train ALTER COLUMN reform_etc  COMMENT 'リフォーム備考 その他リフォーム箇所について';
# MAGIC ALTER TABLE train ALTER COLUMN renovation_date  COMMENT 'リノベーション実施年月日 ';
# MAGIC ALTER TABLE train ALTER COLUMN renovation_etc  COMMENT 'リノベーション備考 ';
# MAGIC ALTER TABLE train ALTER COLUMN unit_tag_id  COMMENT 'タグ情報 スラッシュ(/) 区切り形式 タグマスタシート参照';
# MAGIC ALTER TABLE train ALTER COLUMN bukken_id  COMMENT '物件ID ';
# MAGIC ALTER TABLE train ALTER COLUMN snapshot_create_date  COMMENT '作成日時 データの作成日';
# MAGIC ALTER TABLE train ALTER COLUMN new_date  COMMENT '公開日時 データの直近の公開日';
# MAGIC ALTER TABLE train ALTER COLUMN snapshot_modify_date  COMMENT '修正日時 データの修正日';
# MAGIC ALTER TABLE train ALTER COLUMN timelimit_date  COMMENT '情報掲載期限日 データの情報掲載期限日 居住用賃貸(31)：7日以内 それ以外：14日以内';
# MAGIC ALTER TABLE train ALTER COLUMN flg_open  COMMENT '公開可否 0:非公開 1:公開 ※0:非公開(自社HPのみ掲載) 1:公開(HOMES公開＋自社HPに掲載)';
# MAGIC ALTER TABLE train ALTER COLUMN flg_own  COMMENT '自社物フラグ 0:先物 1:自社物';
# MAGIC ALTER TABLE train ALTER COLUMN bukken_type  COMMENT '物件種別 3101:賃貸マンション 3102:賃貸アパート';
# MAGIC ALTER TABLE train ALTER COLUMN flg_investment  COMMENT '投資用物件 0:通常物件 1:投資用物件 2:事業用物件';
# MAGIC ALTER TABLE train ALTER COLUMN empty_number  COMMENT '空き物件数 物件の空き数 (部屋:空部屋数 土地:販売区画数等)';
# MAGIC ALTER TABLE train ALTER COLUMN empty_contents  COMMENT '空き物件内容 部屋:空部屋の番号 土地:区画番号等';
# MAGIC ALTER TABLE train ALTER COLUMN post1  COMMENT '郵便番号(上位) 7ケタ郵便番号の上位3桁';
# MAGIC ALTER TABLE train ALTER COLUMN post2  COMMENT '郵便番号(下位) 7ケタ郵便番号の下位4桁';
# MAGIC ALTER TABLE train ALTER COLUMN addr1_1  COMMENT '都道府県 北海道、青森、秋田、東京等（JISコード）　エリア情報参照';
# MAGIC ALTER TABLE train ALTER COLUMN addr1_2  COMMENT '市区郡町村 渋谷区、横浜市中区等（JISコード）　エリア情報参照';
# MAGIC ALTER TABLE train ALTER COLUMN addr2_name  COMMENT '所在地 文字列 市区郡以降の文字列(常時入力) 郵便番号の順で取得して入力';
# MAGIC ALTER TABLE train ALTER COLUMN addr3_name  COMMENT '所在地詳細_表示部 公開する所在地詳細情報「詳細表示部」「詳細非表示部」どちらかが入力されていないとエラー';
# MAGIC ALTER TABLE train ALTER COLUMN addr4_name  COMMENT '所在地詳細_非表示部 公開しない所在地詳細情報(公開する所在地詳細情報以降)「詳細表示部」「詳細非表示部」どちらかが入力されていないとエラー';
# MAGIC ALTER TABLE train ALTER COLUMN nl  COMMENT '緯度(日本測地系) 日本測地系、ミリ秒形式';
# MAGIC ALTER TABLE train ALTER COLUMN el  COMMENT '経度(日本測地系) 日本測地系、ミリ秒形式';
# MAGIC ALTER TABLE train ALTER COLUMN rosen_name1  COMMENT '路線名１ ';
# MAGIC ALTER TABLE train ALTER COLUMN eki_name1  COMMENT '駅名１ ';
# MAGIC ALTER TABLE train ALTER COLUMN bus_stop1  COMMENT 'バス停名1 バスを使用する場合';
# MAGIC ALTER TABLE train ALTER COLUMN bus_time1  COMMENT 'バス時間1 単位：分';
# MAGIC ALTER TABLE train ALTER COLUMN walk_distance1  COMMENT '徒歩距離1 駅またはバス停からの距離（単位：m）。徒歩時間は距離÷80。';
# MAGIC ALTER TABLE train ALTER COLUMN rosen_name2  COMMENT '路線名２ ';
# MAGIC ALTER TABLE train ALTER COLUMN eki_name2  COMMENT '駅名２ ';
# MAGIC ALTER TABLE train ALTER COLUMN bus_stop2  COMMENT 'バス停名2 バスを使用する場合';
# MAGIC ALTER TABLE train ALTER COLUMN bus_time2  COMMENT 'バス時間2 単位：分';
# MAGIC ALTER TABLE train ALTER COLUMN walk_distance2  COMMENT '徒歩距離2 駅またはバス停からの距離（単位：m）。徒歩時間は距離÷80。';
# MAGIC ALTER TABLE train ALTER COLUMN traffic_other  COMMENT 'その他交通 ＸＸインターから車で10分';
# MAGIC ALTER TABLE train ALTER COLUMN traffic_car  COMMENT '車所要時間 ';
# MAGIC ALTER TABLE train ALTER COLUMN snapshot_land_area  COMMENT '区画面積(代表) 単位：平米';
# MAGIC ALTER TABLE train ALTER COLUMN snapshot_land_shidou  COMMENT '私道負担面積(代表) 単位：平米';
# MAGIC ALTER TABLE train ALTER COLUMN land_shidou_a  COMMENT '私道負担割合分母 ';
# MAGIC ALTER TABLE train ALTER COLUMN land_shidou_b  COMMENT '私道負担割合分子 ';
# MAGIC ALTER TABLE train ALTER COLUMN land_mochibun_a  COMMENT '土地持分分母 ';
# MAGIC ALTER TABLE train ALTER COLUMN land_mochibun_b  COMMENT '土地持分分子 ';
# MAGIC ALTER TABLE train ALTER COLUMN house_area  COMMENT '建物面積/専有面積(代表) 単位：平米';
# MAGIC ALTER TABLE train ALTER COLUMN flg_new  COMMENT '新築・未入居フラグ 0:中古 1:新築・未入居';
# MAGIC ALTER TABLE train ALTER COLUMN house_kanrinin  COMMENT '管理人 売買：マンションのみ必須 1:常駐 2:日勤 3:巡回 4:無 (5:非常駐 V3互換用) その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN room_kaisuu  COMMENT '部屋階数 部屋の所在階数 (マイナスの場合は地下)';
# MAGIC ALTER TABLE train ALTER COLUMN snapshot_window_angle  COMMENT '向き 1:北 2:北東 3:東 4:南東 5:南 6:南西 7:西 8:北西　その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN madori_number_all  COMMENT '間取部屋数(代表) 部屋の数';
# MAGIC ALTER TABLE train ALTER COLUMN madori_kind_all  COMMENT '間取部屋種類(代表) 10:R 20:K 25:SK 30:DK 35:SDK 40:LK 45:SLK 50:LDK 55:SLDK';
# MAGIC ALTER TABLE train ALTER COLUMN money_kyoueki  COMMENT '共益費/管理費(代表) 単位：円';
# MAGIC ALTER TABLE train ALTER COLUMN money_kyoueki_tax  COMMENT '共益費/管理費 税 1:外税 2:税込み 3:税表示無し(税発生せず)　その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN money_rimawari_now  COMMENT '現行利回り 単位：％';
# MAGIC ALTER TABLE train ALTER COLUMN money_shuuzen  COMMENT '修繕積立金(代表) 売買：マンションのみ必須 単位：円';
# MAGIC ALTER TABLE train ALTER COLUMN money_shuuzenkikin  COMMENT '修繕積立基金(代表) 単位：円';
# MAGIC ALTER TABLE train ALTER COLUMN money_sonota_str1  COMMENT 'その他費用名目1 その他必要な費用の名目 その他費用2が入力されている場合は必須';
# MAGIC ALTER TABLE train ALTER COLUMN money_sonota1  COMMENT 'その他費用1 単位：円 その他費用名目1が入力されている場合は必須';
# MAGIC ALTER TABLE train ALTER COLUMN money_sonota_str2  COMMENT 'その他費用名目2 その他必要な費用の名目 その他費用2が入力されている場合は必須';
# MAGIC ALTER TABLE train ALTER COLUMN money_sonota2  COMMENT 'その他費用2 単位：円 その他費用名目2が入力されている場合は必須';
# MAGIC ALTER TABLE train ALTER COLUMN money_sonota_str3  COMMENT 'その他費用名目3 その他必要な費用の名目 その他費用3が入力されている場合は必須';
# MAGIC ALTER TABLE train ALTER COLUMN money_sonota3  COMMENT 'その他費用3 単位：円
# MAGIC その他費用名目3が入力されている場合は必須';
# MAGIC ALTER TABLE train ALTER COLUMN parking_money  COMMENT '駐車場料金(代表) 単位：円';
# MAGIC ALTER TABLE train ALTER COLUMN parking_money_tax  COMMENT '駐車場料金 税 1:外税 2:税込み 3:税表示無し(税発生せず)　その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN parking_kubun  COMMENT '駐車場区分 1:空有 2:空無 3:近隣 4:無 (5:有 V3互換用)　その他: 欠損';
# MAGIC ALTER TABLE train ALTER COLUMN parking_distance  COMMENT '駐車場距離 単位：m';
# MAGIC ALTER TABLE train ALTER COLUMN parking_number  COMMENT '駐車場空き台数 台数が分かっている場合入力 未入力時はなにも表示しない';
# MAGIC ALTER TABLE train ALTER COLUMN parking_memo  COMMENT '駐車場備考 備考';
# MAGIC ALTER TABLE train ALTER COLUMN genkyo_code  COMMENT '現況 (土地の場合)1:更地 2:古屋あり 10:古屋あり更地引渡可 (戸建・マン・外全・外一の場合)1:居住中 2:空家 3:賃貸中 4:未完成';
# MAGIC ALTER TABLE train ALTER COLUMN usable_status  COMMENT '引渡/入居時期 1:即時 2:相談 3:期日指定 (4:未定 V3互換用)　その他: 欠損 現況が居住中、賃貸中、未完成の場合は、期日指定、相談のみ選択可能';
# MAGIC ALTER TABLE train ALTER COLUMN usable_date  COMMENT '引渡/入居年月 年月（引渡/入居時期で期日指定をした場合のみ)';
# MAGIC ALTER TABLE train ALTER COLUMN school_ele_name  COMMENT '小学校名 ';
# MAGIC ALTER TABLE train ALTER COLUMN school_ele_distance  COMMENT '小学校距離 単位：m';
# MAGIC ALTER TABLE train ALTER COLUMN school_ele_code  COMMENT '小学校 学区コード ';
# MAGIC ALTER TABLE train ALTER COLUMN school_jun_name  COMMENT '中学校名 ';
# MAGIC ALTER TABLE train ALTER COLUMN school_jun_distance  COMMENT '中学校距離 単位：m';
# MAGIC ALTER TABLE train ALTER COLUMN school_jun_code  COMMENT '中学校 学区コード ';
# MAGIC ALTER TABLE train ALTER COLUMN convenience_distance  COMMENT 'コンビニ距離 単位：m';
# MAGIC ALTER TABLE train ALTER COLUMN super_distance  COMMENT 'スーパー距離 単位：m';
# MAGIC ALTER TABLE train ALTER COLUMN hospital_distance  COMMENT '総合病院距離 単位：m';
# MAGIC ALTER TABLE train ALTER COLUMN park_distance  COMMENT '公園距離 単位：m';
# MAGIC ALTER TABLE train ALTER COLUMN drugstore_distance  COMMENT 'ドラッグストア距離 単位：m';
# MAGIC ALTER TABLE train ALTER COLUMN bank_distance  COMMENT '銀行距離 単位：m';
# MAGIC ALTER TABLE train ALTER COLUMN shopping_street_distance  COMMENT '商店街距離 単位：m';
# MAGIC ALTER TABLE train ALTER COLUMN est_other_name  COMMENT '施設その他 ';
# MAGIC ALTER TABLE train ALTER COLUMN est_other_distance  COMMENT '施設その他距離 単位：m';
# MAGIC ALTER TABLE train ALTER COLUMN statuses  COMMENT '設備情報 スラッシュ(/) 区切り形式 物件マスタとは別コード 設備情報シート参照';
# MAGIC ALTER TABLE train ALTER COLUMN parking_keiyaku  COMMENT '契約形態 1:駐車場契約必須(賃料に含む) 2:駐車場契約必須（駐車場料金別)';
# MAGIC ALTER TABLE train ALTER COLUMN money_hoshou_company  COMMENT '保証会社費用 保証会社の利用 で 1:利用可 2:利用必須 が選択されている場合は必須';
# MAGIC ALTER TABLE train ALTER COLUMN free_rent_duration  COMMENT 'フリーレント期間 単位：月';
# MAGIC ALTER TABLE train ALTER COLUMN free_rent_gen_timing  COMMENT 'フリーレント賃料発生タイミング yyyymm形式';
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from train
# MAGIC where addr1_1 ='13' and addr1_2 = '102'
# MAGIC limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tag_info;
# MAGIC DROP TABLE IF EXISTS area_info;
# MAGIC DROP TABLE IF EXISTS setsubi_info;

# COMMAND ----------

df = spark.read.csv("/Volumes/pcdua_shared_catalog/default/pcdua_volume/tag_info.csv", header=True, inferSchema=True)
df.write.format("delta").mode("overwrite").saveAsTable("tag_info")

df = spark.read.csv("/Volumes/pcdua_shared_catalog/default/pcdua_volume/area_info.csv", header=True, inferSchema=True)
df.write.format("delta").mode("overwrite").saveAsTable("area_info")

df = spark.read.csv("/Volumes/pcdua_shared_catalog/default/pcdua_volume/setsubi_info.csv", header=True, inferSchema=True)
df.write.format("delta").mode("overwrite").saveAsTable("setsubi_info")

# COMMAND ----------

# DBTITLE 1,カラムにコメントをつける
# MAGIC %sql
# MAGIC -- area_info
# MAGIC COMMENT ON TABLE area_info IS 'area_infoテーブルには、日本の地域情報が含まれています。地域ごとの一意の識別子や市区郡町村や都道府県に関連するIDが含まれています。また、都道府県や市区郡町村の名前も含まれています。';
# MAGIC ALTER TABLE area_info ALTER COLUMN addr1_2  COMMENT "都道府県ID";
# MAGIC ALTER TABLE area_info ALTER COLUMN addr1_1  COMMENT "市区郡町村ID";
# MAGIC ALTER TABLE area_info ALTER COLUMN prefecture  COMMENT "都道府県名（例 '北海道', '東京都', '大阪府'）";
# MAGIC ALTER TABLE area_info ALTER COLUMN shikuchoson  COMMENT "市区郡町村名（例 '札幌市中央区', '千代田区', '大阪市西区'）";
# MAGIC
# MAGIC -- setsubi_info
# MAGIC COMMENT ON TABLE setsubi_info IS 'setsubi_infoテーブルには、物件の設備情報に関連するデータが含まれています。設備番号、設備のID、および設備のタイプなどの情報が含まれています。';
# MAGIC ALTER TABLE setsubi_info ALTER COLUMN setsubi_id  COMMENT "設備ID, `train`テーブルの'building_tag_id'の値をスラッシュ(/) で分割した文字列にリンク";
# MAGIC ALTER TABLE setsubi_info ALTER COLUMN setsubi_shubetsu  COMMENT "設備ID（例 '公営水道', '都市ガス', '敷金なし', '礼金なし', '敷金礼金なし'）";
# MAGIC
# MAGIC -- tag_info
# MAGIC ALTER TABLE tag_info ALTER COLUMN tag_id  COMMENT "タグID";
# MAGIC ALTER TABLE tag_info ALTER COLUMN tag_shubetsu  COMMENT "タグ内容（例 '公共水道', 'プロパンガス', 'ウォークインクローゼット'）";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tag_info limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from area_info limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from setsubi_info limit 10;

# COMMAND ----------

# DBTITLE 1,NULL拒否
# MAGIC %sql
# MAGIC ALTER TABLE train ALTER COLUMN building_id SET NOT NULL;
# MAGIC /*
# MAGIC ALTER TABLE tag_info ALTER COLUMN tag_id SET NOT NULL;
# MAGIC ALTER TABLE area_info ALTER COLUMN addr1_1 SET NOT NULL;
# MAGIC ALTER TABLE setsubi_info ALTER COLUMN setsubi_id SET NOT NULL;
# MAGIC */

# COMMAND ----------

# DBTITLE 1,PK設定
# MAGIC %sql
# MAGIC -- Set Primary Keys
# MAGIC ALTER TABLE train DROP CONSTRAINT IF EXISTS pk_train_building_id;
# MAGIC ALTER TABLE train ADD  CONSTRAINT pk_train_building_id PRIMARY KEY (building_id);
# MAGIC
# MAGIC -- Set Foreign Keys
# MAGIC --ALTER TABLE train DROP CONSTRAINT IF EXISTS fk_train_tag_info;
# MAGIC --ALTER TABLE train ADD CONSTRAINT fk_train_tag_info FOREIGN KEY (building_tag_id) REFERENCES tag_info(tag_id);
# MAGIC
# MAGIC --ALTER TABLE train DROP CONSTRAINT IF EXISTS fk_train_area_info_addr1_1;
# MAGIC --ALTER TABLE train ADD CONSTRAINT fk_train_area_info_addr1_1 FOREIGN KEY (addr1_1) REFERENCES area_info(addr1_1);
# MAGIC
# MAGIC --ALTER TABLE train DROP CONSTRAINT IF EXISTS fk_train_area_info_addr1_2;
# MAGIC --ALTER TABLE train ADD CONSTRAINT fk_train_area_info_addr1_2 FOREIGN KEY (addr1_2) REFERENCES area_info(addr1_2);
# MAGIC
# MAGIC --ALTER TABLE train DROP CONSTRAINT IF EXISTS fk_train_setsubi_info;
# MAGIC --ALTER TABLE train ADD CONSTRAINT fk_train_setsubi_info FOREIGN KEY (statuses) REFERENCES setsubi_info(setsubi_id);

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. 中間テーブルを作成
# MAGIC
# MAGIC * train テーブルから必要なカラムのみを利用して%sqlで中間テーブル作成
# MAGIC * `Building_ID`及び`money_room`は必ず入れる
# MAGIC * 分析で最重要と思われる下記のデータは必ず入れる
# MAGIC - `building_type`（建物種別）: マンション、アパートなどの種別は賃料に影響を与える可能性があります。
# MAGIC - `unit_area_min`と`unit_area_ma`x`（専有面積の下限と上限）: 部屋の広さは賃料に大きく影響します。
# MAGIC - `floor_count`（建物階数）と`basement_floor_count`（地下階数）: 建物の高さや地下の有無も賃料に影響を与える要素です。
# MAGIC - `year_built`（築年月）: 新しい建物ほど賃料が高い傾向にあります。
# MAGIC - `addr1_1`と`addr1_2`（都道府県コードと市区町村コード）: 物件の位置（立地）は賃料に大きな影響を与えます。
# MAGIC - `room_count`（間取部屋数）: 部屋数も賃料を左右する重要な要素です。
# MAGIC - `statuses`（設備情報）: 設備の充実度は賃料に影響を与える可能性があります。
# MAGIC * `year_built`については建物が作られた年月(yyyymm)なので現在からの月数に変更したい。
# MAGIC * `building_type`、`addr1_1`、`addr1_2`については文字型のカラムにCASTしたい。CAST前の`building_type`、`addr1_1`、`addr1_2`は不要。

# COMMAND ----------

# DBTITLE 1,必要なカラムのみ抽出
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE relevant_train_data AS
# MAGIC SELECT
# MAGIC   Building_ID,
# MAGIC   money_room,
# MAGIC   building_type,
# MAGIC   unit_area_min,
# MAGIC   unit_area_max,
# MAGIC   floor_count,
# MAGIC   basement_floor_count,
# MAGIC   year_built,
# MAGIC   addr1_1,
# MAGIC   addr1_2,
# MAGIC   room_count,
# MAGIC   statuses
# MAGIC FROM
# MAGIC   train

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from relevant_train_data

# COMMAND ----------

# DBTITLE 1,文字列に変換
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE relevant_train_data_2 AS
# MAGIC SELECT
# MAGIC   Building_ID,
# MAGIC   CAST(building_type AS STRING) AS building_type,
# MAGIC   money_room,
# MAGIC   unit_area_min,
# MAGIC   unit_area_max,
# MAGIC   floor_count,
# MAGIC   basement_floor_count,
# MAGIC   CAST(addr1_1 AS STRING) AS addr1_1,
# MAGIC   CAST(addr1_2 AS STRING) AS addr1_2,
# MAGIC   room_count,
# MAGIC   statuses,
# MAGIC   (YEAR(current_date()) * 12 + MONTH(current_date())) - (CAST(SUBSTRING(year_built, 1, 4) AS INT) * 12 + CAST(SUBSTRING(year_built, 5, 2) AS INT)) AS months_since_built
# MAGIC FROM train

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. 中間テーブルを作成
# MAGIC
# MAGIC * trainテーブルから必要なカラムのみを利用して%sqlで中間テーブル作成。名前はrelevant_train_data_3。
# MAGIC * `Building_ID`及び`money_room`は必ず入れる。
# MAGIC * 分析で最重要と思われる下記のデータは必ず入れる。
# MAGIC - `building_type`（建物種別）: マンション、アパートなどの種別は賃料に影響を与える可能性があります。
# MAGIC - `unit_area_min`と`unit_area_max`（専有面積の下限と上限）: 部屋の広さは賃料に大きく影響します。
# MAGIC - `floor_count`（建物階数）と`basement_floor_count`（地下階数）: 建物の高さや地下の有無も賃料に影響を与える要素です。
# MAGIC - `year_built`（築年月）: 新しい建物ほど賃料が高い傾向にあります。
# MAGIC - `addr1_1`と`addr1_2`（都道府県コードと市区町村コード）: 物件の位置（立地）は賃料に大きな影響を与えます。
# MAGIC - `room_count`（間取部屋数）: 部屋数も賃料を左右する重要な要素です。
# MAGIC - `statuses`（設備情報）: 設備の充実度は賃料に影響を与える可能性があります。
# MAGIC * `year_built`については建物が作られた年月(yyyymm)なので現在からの月数に変更したい。
# MAGIC * `building_type`、`addr1_1`、`addr1_2`については文字型のカラムにCASTしたい。CAST前の`building_type`、`addr1_1`、`addr1_2`は不要。
# MAGIC * さらに`addr1_1`と`addr1_2`を"-"で連結した文字型カラムをほしい。`addr1_2`自体のカラムは不要。

# COMMAND ----------

# DBTITLE 1,トレーニングデータを作成
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE relevant_train_data_3 AS
# MAGIC SELECT
# MAGIC   Building_ID,
# MAGIC   money_room,
# MAGIC   CAST(building_type AS STRING) AS building_type,
# MAGIC   floor_count,
# MAGIC   (YEAR(current_date()) * 12 + MONTH(current_date())) - (FLOOR(year_built / 100) * 12 + MOD(year_built, 100)) AS months_since_built,  -- 築年月
# MAGIC   CAST(addr1_1 AS STRING) AS addr1_1,
# MAGIC   room_count,
# MAGIC   CONCAT(CAST(addr1_1 AS STRING), '-', CAST(addr1_2 AS STRING)) AS addr1_aadr2_combined  -- アドレス連結
# MAGIC FROM train

# COMMAND ----------

# DBTITLE 1,テストデータの読み込み
# catalog_name = "komae"
# schema_name  = "komae_schema"
# spark.sql(f"USE {catalog_name}.{schema_name}")

df = spark.read.csv("/Volumes/pcdua_shared_catalog/default/pcdua_volume/test.csv", header=True, inferSchema=True)
# df = spark.read.csv(f"/Volumes/{catalog_name}/{schema_name}/pcdua_volume/test.csv", header=True, inferSchema=True)
df.write.format("delta").mode("overwrite").saveAsTable("test")

# COMMAND ----------

# DBTITLE 1,テストデータを作成
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE relevant_test_data_3 AS
# MAGIC SELECT
# MAGIC   Building_ID,
# MAGIC --  money_room,
# MAGIC   CAST(building_type AS STRING) AS building_type,
# MAGIC   floor_count,
# MAGIC   (YEAR(current_date()) * 12 + MONTH(current_date())) - (FLOOR(year_built / 100) * 12 + MOD(year_built, 100)) AS months_since_built,
# MAGIC   CAST(addr1_1 AS STRING) AS addr1_1,
# MAGIC   room_count,
# MAGIC   CONCAT(CAST(addr1_1 AS STRING), '-', CAST(addr1_2 AS STRING)) AS addr1_aadr2_combined
# MAGIC FROM test

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. AutoML実行(回帰モデル)
# MAGIC - relevant_train_data_3について、money_roomを目的変数としてbuilding_type, floor_count, months_since_built, addr1_1, room_countを説明変数として30分でBestモデルを選択。
# MAGIC - Bestモデルを利用してrelevant_test_data_3テーブルに対して予測値を付与。
# MAGIC - /Volumes/<ご自身のカタログ>/<ご自身のスキーマ>/pcdua_volume/submit.csvに保存。CSVファイルにヘッダーはつけない。

# COMMAND ----------

from databricks import automl

# Load training data
train_df = spark.table("relevant_train_data_3")

# Run AutoML for regression
summary = automl.regress(
    train_df,
    target_col="money_room",
    exclude_cols=["building_type", "floor_count", "months_since_built", "addr1_1", "room_count"],
    # timeout_minutes=30
    timeout_minutes=5
)



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from relevant_test_data_3

# COMMAND ----------

# Get the best model
best_model = summary.best_trial.load_model()

# Load test data
test_df = spark.table("relevant_test_data_3")

# Convert Spark DataFrame to Pandas DataFrame
test_pd_df = test_df.toPandas()

# Make predictions
predictions = best_model.predict(test_pd_df)

# COMMAND ----------

print(f'/Volumes/{catalog_name}/{schema_name}/pcdua_volume/sample_submit.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC SIGNATE提出用のサンプルフォーマットをロード

# COMMAND ----------

# DBTITLE 1,GithubからVolumeにダウンロードするためのヘルパー読み込み
# MAGIC %run ./_init/_init_helper

# COMMAND ----------

# DBTITLE 1,GithubからVolumeにダウンロード
# sample_submit.csvをダウンロード
DBDemos.download_file_from_git(
    dest=f"/Volumes/{catalog_name}/{schema_name}/pcdua_volume",
    owner="komae5519pv",
    repo="komae_dbdemos",
    path="/pdcua_20241108/_submit_temp/"
)

# COMMAND ----------

import pandas as pd
sample_submit = pd.read_csv(f'/Volumes/{catalog_name}/{schema_name}/pcdua_volume/sample_submit.csv', index_col=0, header=None) # 応募用サンプルファイル

# COMMAND ----------

print(predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. 予測結果データをCSVエクスポート

# COMMAND ----------

sample_submit[1] = predictions
sample_submit.to_csv(f'/Volumes/{catalog_name}/{schema_name}/pcdua_volume/submit.csv', header=None)
