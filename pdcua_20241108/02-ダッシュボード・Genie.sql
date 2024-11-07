-- Databricks notebook source
USE komae_demo.pcdua

-- COMMAND ----------

-- DBTITLE 1,ダッシュボード用のテーブル作成
CREATE OR REPLACE TABLE property_info AS
SELECT
  p.target_ym
  ,p.money_room
  ,CASE
    WHEN (p.money_room < 40000) THEN '[1]4万円未満'
    WHEN (p.money_room >= 40000) AND (p.money_room < 50000) THEN '[2]4万円以上5万円未満'
    WHEN (p.money_room >= 50000) AND (p.money_room < 60000) THEN '[3]5万円以上6万円未満'
    WHEN (p.money_room >= 60000) AND (p.money_room < 70000) THEN '[4]6万円以上7万円未満'
    WHEN (p.money_room >= 70000) AND (p.money_room < 80000) THEN '[5]7万円以上8万円未満'
    WHEN (p.money_room >= 80000) AND (p.money_room < 90000) THEN '[6]8万円以上9万円未満'
    WHEN (p.money_room >= 90000) AND (p.money_room < 100000) THEN '[7]9万円以上10万円未満'
    WHEN (p.money_room >= 100000) THEN '[8]10万円以上'
  END as money_room_range -- 賃料範囲
  ,p.building_id
  ,p.building_status
  ,p.building_create_date
  ,p.building_modify_date
  ,p.building_type
  ,p.building_name
  ,p.building_name_ruby
  ,p.homes_building_name
  ,p.homes_building_name_ruby
  ,p.unit_count
  ,p.full_address
  ,p.lon
  ,p.lat
  ,p.building_structure
  ,p.total_floor_area
  ,p.building_area
  ,p.floor_count
  ,p.basement_floor_count
  ,p.year_built
  ,FLOOR(DATEDIFF(current_date(), TO_DATE(p.year_built || '01', 'yyyyMMdd')) / 365) AS building_age -- 築年数
  ,CASE
    WHEN FLOOR(DATEDIFF(current_date(), TO_DATE(p.year_built || '01', 'yyyyMMdd')) / 365) < 3 THEN '[1]3年未満'
    WHEN FLOOR(DATEDIFF(current_date(), TO_DATE(p.year_built || '01', 'yyyyMMdd')) / 365) >= 3 AND FLOOR(DATEDIFF(current_date(), TO_DATE(p.year_built || '01', 'yyyyMMdd')) / 365) < 5 THEN '[2]5年未満'
    WHEN FLOOR(DATEDIFF(current_date(), TO_DATE(p.year_built || '01', 'yyyyMMdd')) / 365) >= 5 AND FLOOR(DATEDIFF(current_date(), TO_DATE(p.year_built || '01', 'yyyyMMdd')) / 365) < 7 THEN '[3]7年未満'
    WHEN FLOOR(DATEDIFF(current_date(), TO_DATE(p.year_built || '01', 'yyyyMMdd')) / 365) >= 7 AND FLOOR(DATEDIFF(current_date(), TO_DATE(p.year_built || '01', 'yyyyMMdd')) / 365) < 10 THEN '[4]10年未満'
    WHEN FLOOR(DATEDIFF(current_date(), TO_DATE(p.year_built || '01', 'yyyyMMdd')) / 365) >= 10 AND FLOOR(DATEDIFF(current_date(), TO_DATE(p.year_built || '01', 'yyyyMMdd')) / 365) < 15 THEN '[5]15年未満'
    WHEN FLOOR(DATEDIFF(current_date(), TO_DATE(p.year_built || '01', 'yyyyMMdd')) / 365) >= 15 AND FLOOR(DATEDIFF(current_date(), TO_DATE(p.year_built || '01', 'yyyyMMdd')) / 365) < 20 THEN '[6]20年未満'
    WHEN FLOOR(DATEDIFF(current_date(), TO_DATE(p.year_built || '01', 'yyyyMMdd')) / 365) >= 25 THEN '[7]25年以上'
  END as building_age_range -- 築年数範囲
  ,p.building_land_area
  ,p.land_area_all
  ,p.unit_area_min
  ,p.unit_area_max
  ,p.building_land_chimoku
  ,p.land_youto
  ,p.land_toshi
  ,p.land_chisei
  ,p.land_area_kind
  ,p.land_setback_flg
  ,p.land_setback
  ,p.land_kenpei
  ,p.land_youseki
  ,p.land_road_cond
  ,p.land_seigen
  ,p.building_area_kind
  ,p.management_form
  ,p.management_association_flg
  ,p.reform_exterior
  ,p.reform_exterior_other
  ,p.reform_exterior_date
  ,p.reform_common_area
  ,p.reform_common_area_date
  ,p.building_tag_id
  ,p.unit_id
  ,p.unit_name
  ,p.name_ruby
  ,p.room_floor
  ,p.balcony_area
  ,p.dwelling_unit_window_angle
  ,p.room_count
  ,p.unit_area
  ,p.floor_plan_code
  ,p.reform_date
  ,p.reform_place
  ,p.reform_place_other
  ,p.reform_wet_area
  ,p.reform_wet_area_other
  ,p.reform_wet_area_date
  ,p.reform_interior
  ,p.reform_interior_other
  ,p.reform_interior_date
  ,p.reform_etc
  ,p.renovation_date
  ,p.renovation_etc
  ,p.unit_tag_id
  ,p.bukken_id
  ,p.snapshot_create_date
  ,p.new_date
  ,p.snapshot_modify_date
  ,p.timelimit_date
  ,p.flg_open
  ,p.flg_own
  ,p.bukken_type
  ,p.flg_investment
  ,p.empty_number
  ,p.empty_contents
  ,p.post1
  ,p.post2
  ,p.addr1_1
  ,p.addr1_2
  ,a.prefecture
  ,a.shikuchoson
  ,p.addr2_name
  ,p.addr3_name
  ,p.addr4_name
  ,p.nl
  ,p.el
  ,p.rosen_name1
  ,p.eki_name1
  ,p.bus_stop1
  ,p.bus_time1
  ,p.walk_distance1
  ,(p.walk_distance1 / 80) as walk_min -- 徒歩所要時間
  ,CASE
    WHEN (p.walk_distance1 / 80) <= 1 THEN '徒歩01分以内'
    WHEN (p.walk_distance1 / 80) >= 2 AND (p.walk_distance1 / 80) <= 5 THEN '徒歩05分以内'
    WHEN (p.walk_distance1 / 80) >= 6 AND (p.walk_distance1 / 80) <= 7 THEN '徒歩07分以内'
    WHEN (p.walk_distance1 / 80) >= 8 AND (p.walk_distance1 / 80) <= 10 THEN '徒歩10分以内'
    WHEN (p.walk_distance1 / 80) >= 11 AND (p.walk_distance1 / 80) <= 15 THEN '徒歩15分以内'
    WHEN (p.walk_distance1 / 80) >= 16 THEN '徒歩16分以上'
  END as walk_min_range -- 徒歩所要時間範囲
  ,p.rosen_name2
  ,p.eki_name2
  ,p.bus_stop2
  ,p.bus_time2
  ,p.walk_distance2
  ,p.traffic_other
  ,p.traffic_car
  ,p.snapshot_land_area
  ,p.snapshot_land_shidou
  ,p.land_shidou_a
  ,p.land_shidou_b
  ,p.land_mochibun_a
  ,p.land_mochibun_b
  ,p.house_area
  ,CASE
    WHEN (house_area < 15) THEN '[1]15平米未満'
    WHEN (house_area >= 15) AND (house_area < 20) THEN '[2]20平米未満'
    WHEN (house_area >= 20) AND (house_area < 25) THEN '[3]25平米未満'
    WHEN (house_area >= 25) AND (house_area < 30) THEN '[4]30平米未満'
    WHEN (house_area >= 30) AND (house_area < 35) THEN '[5]35平米未満'
    WHEN (house_area >= 35) AND (house_area < 40) THEN '[6]40平米未満'
    WHEN (house_area > 40) THEN '[7]40平米以上'
  END AS floor_area -- 占有面積
  ,p.flg_new
  ,CASE
    WHEN flg_new = 0 THEN '中古'
    WHEN flg_new = 1 THEN '新築・未入居'
  END AS property_condition -- 物件状態
  ,p.house_kanrinin
  ,p.room_kaisuu
  ,p.snapshot_window_angle
  ,p.madori_number_all
  ,p.madori_kind_all
  ,p.money_kyoueki
  ,p.money_kyoueki_tax
  ,p.money_rimawari_now
  ,p.money_shuuzen
  ,p.money_shuuzenkikin
  ,p.money_sonota_str1
  ,p.money_sonota1
  ,p.money_sonota_str2
  ,p.money_sonota2
  ,p.money_sonota_str3
  ,p.money_sonota3
  ,p.parking_money
  ,p.parking_money_tax
  ,p.parking_kubun
  ,p.parking_distance
  ,p.parking_number
  ,p.parking_memo
  ,p.genkyo_code
  ,p.usable_status
  ,p.usable_date
  ,p.school_ele_name
  ,p.school_ele_distance
  ,p.school_ele_code
  ,p.school_jun_name
  ,p.school_jun_distance
  ,p.school_jun_code
  ,p.convenience_distance
  ,p.super_distance
  ,p.hospital_distance
  ,p.park_distance
  ,p.drugstore_distance
  ,p.bank_distance
  ,p.shopping_street_distance
  ,p.est_other_name
  ,p.est_other_distance
  ,p.statuses
  ,p.parking_keiyaku
  ,p.money_hoshou_company
  ,p.free_rent_duration
  ,p.free_rent_gen_timing
FROM
  train p
LEFT JOIN
  area_info a
ON
  p.addr1_1 = a.addr1_1 AND p.addr1_2 = a.addr1_2

-- COMMAND ----------

-- DBTITLE 1,カラムコメント追加
ALTER TABLE property_info ALTER COLUMN building_age COMMENT '築年数';
ALTER TABLE property_info ALTER COLUMN building_age_range COMMENT '築年数範囲';
ALTER TABLE property_info ALTER COLUMN walk_min COMMENT '徒歩所要時間';
ALTER TABLE property_info ALTER COLUMN walk_min_range COMMENT '徒歩所要時間範囲';
ALTER TABLE property_info ALTER COLUMN money_room_range COMMENT '賃料範囲';
ALTER TABLE property_info ALTER COLUMN walk_min COMMENT '徒歩所要時間';
ALTER TABLE property_info ALTER COLUMN floor_area COMMENT '占有面積（平米）';
ALTER TABLE property_info ALTER COLUMN property_condition COMMENT '物件状態';
