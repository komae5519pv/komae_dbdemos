-- Databricks notebook source
-- DBTITLE 1,自分専用のカタログ・スキーマ・ボリュームを作成
-- MAGIC %python
-- MAGIC catalog_name = "komae"  # ご自身のカタログ名に変更してください
-- MAGIC schema_name  = "komae_schema" # ご自身のスキーマ名に変更してください
-- MAGIC spark.sql(f"USE {catalog_name}.{schema_name}")

-- COMMAND ----------

CREATE OR REPLACE TABLE relevant_train_data_challenge AS
SELECT
  Building_ID,
  money_room,
  unit_count,
  case when building_structure = 1 then 1 else 0 end as building_structure_1,
  case when building_structure = 3 then 1 else 0 end as building_structure_3,
  case when building_structure = 4 then 1 else 0 end as building_structure_4,
  case when building_structure = 5 then 1 else 0 end as building_structure_5,
  total_floor_area,
  building_area,
  floor_count,
  (YEAR(current_date()) * 12 + MONTH(current_date())) - (CAST(SUBSTRING(year_built, 1, 4) AS INT) * 12 + CAST(SUBSTRING(year_built, 5, 2) AS INT)) as year_built,
  land_area_all,
  unit_area_min,
  unit_area_max,
  case when building_land_chimoku in (1) then 1 else 0 end as building_land_chimoku_1,
  case when land_youto in (3) then 1 else 0 end as land_youto_3,
  case when land_youto in (5) then 1 else 0 end as land_youto_5,
  case when land_toshi in (1) then 1 else 0 end as land_toshi_1,
  case when land_chisei in (1) then 1 else 0 end as land_chisei_1,
  case when land_chisei in (2) then 1 else 0 end as land_chisei_2,
  land_youseki,
  case when management_form in (3) then 1 else 0 end as management_form_3,
  case when management_association_flg in (2) then 1 else 0 end as management_association_flg_2,
  room_floor,
  balcony_area,
  room_count,
  unit_area,
  floor_plan_code,
  case when bukken_type in (3101) then 1 else 0 end as bukken_type_3101,
  empty_number,
----case when addr1_1 in (13) then 1 else 0 end
  walk_distance1,
  walk_distance2,
  snapshot_land_area,
  house_area,
  case when flg_new in (1) then 1 else 0 end as flg_new_1,
  case when house_kanrinin in (1,2,3) then 1 else 0 end as house_kanrinin_1_2_3,
  room_kaisuu,
  case when snapshot_window_angle in (1,2,4,6,8) then 1 else 0 end as snapshot_window_angle_1_2_4_6_8,       -- 
  madori_number_all,
  madori_kind_all,
  money_kyoueki, 
  money_sonota3, 
  parking_money,
  case when parking_money_tax in (2) then 1 else 0 end as parking_money_tax_2,
  case when parking_kubun in (1) then 1 else 0 end as parking_kubun_1,
  parking_distance,
  school_ele_distance, 
  convenience_distance,
  super_distance,
  hospital_distance,
  park_distance,
  drugstore_distance,
  bank_distance,
  shopping_street_distance,
--statuses /*very very needed*/
  case when parking_keiyaku in (1,2) then 1 else 0 end as parking_keiyaku_1_2,
  free_rent_duration
From train

-- COMMAND ----------

select * from relevant_train_data_challenge

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # - AutoML実行。回帰モデル。
-- MAGIC - relevant_train_data_4について、money_roomを目的変数としてbuilding_id以外の全てのカラムを説明変数として30分でBestモデルを選択。
-- MAGIC - Bestモデルを利用してrelevant_test_data_4テーブルに対して予測値を付与。
-- MAGIC - /Volumes/pcdua_shared_catalog/default/pcdua_volume/submit.csvに保存。CSVファイルにヘッダーはつけない。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from databricks import automl
-- MAGIC
-- MAGIC # Load training data
-- MAGIC train_df = spark.table("relevant_train_data_challenge")
-- MAGIC
-- MAGIC # Run AutoML for regression
-- MAGIC summary = automl.regress(
-- MAGIC     train_df,
-- MAGIC     target_col="money_room",
-- MAGIC     exclude_cols=["Building_ID"],
-- MAGIC     timeout_minutes=30
-- MAGIC     # timeout_minutes=5
-- MAGIC )
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE relevant_test_data_challenge AS
SELECT
  Building_ID,
  -- money_room,
  unit_count,
  case when building_structure = 1 then 1 else 0 end as building_structure_1,
  case when building_structure = 3 then 1 else 0 end as building_structure_3,
  case when building_structure = 4 then 1 else 0 end as building_structure_4,
  case when building_structure = 5 then 1 else 0 end as building_structure_5,
  total_floor_area,
  building_area,
  floor_count,
  (YEAR(current_date()) * 12 + MONTH(current_date())) - (CAST(SUBSTRING(year_built, 1, 4) AS INT) * 12 + CAST(SUBSTRING(year_built, 5, 2) AS INT)) as year_built,
  land_area_all,
  unit_area_min,
  unit_area_max,
  case when building_land_chimoku in (1) then 1 else 0 end as building_land_chimoku_1,
  case when land_youto in (3) then 1 else 0 end as land_youto_3,
  case when land_youto in (5) then 1 else 0 end as land_youto_5,
  case when land_toshi in (1) then 1 else 0 end as land_toshi_1,
  case when land_chisei in (1) then 1 else 0 end as land_chisei_1,
  case when land_chisei in (2) then 1 else 0 end as land_chisei_2,
  land_youseki,
  case when management_form in (3) then 1 else 0 end as management_form_3,
  case when management_association_flg in (2) then 1 else 0 end as management_association_flg_2,
  room_floor,
  balcony_area,
  room_count,
  unit_area,
  floor_plan_code,
  case when bukken_type in (3101) then 1 else 0 end as bukken_type_3101,
  empty_number,
----case when addr1_1 in (13) then 1 else 0 end
  walk_distance1,
  walk_distance2,
  snapshot_land_area,
  house_area,
  case when flg_new in (1) then 1 else 0 end as flg_new_1,
  case when house_kanrinin in (1,2,3) then 1 else 0 end as house_kanrinin_1_2_3,
  room_kaisuu,
  case when snapshot_window_angle in (1,2,4,6,8) then 1 else 0 end as snapshot_window_angle_1_2_4_6_8,
  madori_number_all,
  madori_kind_all,
  money_kyoueki,
  money_sonota3,
  parking_money,
  case when parking_money_tax in (2) then 1 else 0 end as parking_money_tax_2,
  case when parking_kubun in (1) then 1 else 0 end as parking_kubun_1,
  parking_distance,
  school_ele_distance,
  convenience_distance,
  super_distance,
  hospital_distance,
  park_distance,
  drugstore_distance,
  bank_distance,
  shopping_street_distance,
--statuses /*very very needed*/
  case when parking_keiyaku in (1,2) then 1 else 0 end as parking_keiyaku_1_2,
  free_rent_duration
From test

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Get the best model
-- MAGIC best_model = summary.best_trial.load_model()
-- MAGIC
-- MAGIC # Load test data
-- MAGIC test_df = spark.table("relevant_test_data_challenge")
-- MAGIC
-- MAGIC # Convert Spark DataFrame to Pandas DataFrame
-- MAGIC test_pd_df = test_df.toPandas()
-- MAGIC
-- MAGIC # Make predictions
-- MAGIC predictions = best_model.predict(test_pd_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC sample_submit = pd.read_csv(f'/Volumes/{catalog_name}/{schema_name}/pcdua_volume/sample_submit.csv', index_col=0, header=None) # 応募用サンプルファイル

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(predictions)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sample_submit[1] = predictions
-- MAGIC sample_submit.to_csv(f'/Volumes/{catalog_name}/{schema_name}/pcdua_volume/submit_new.csv', header=None)
