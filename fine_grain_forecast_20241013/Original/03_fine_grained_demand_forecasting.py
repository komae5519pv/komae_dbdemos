# Databricks notebook source
# MAGIC %md
# MAGIC こちらの一連のノートブックは、以下のリンクでご覧いただけます：https://github.com/databricks-industry-solutions/fine-grained-demand-forecasting。 このソリューションアクセラレーターの詳細については、https://www.databricks.com/solutions/accelerators/demand-forecasting をご覧ください。
# MAGIC
# MAGIC <!-- %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/fine-grained-demand-forecasting. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/demand-forecasting. -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## デモ概要
# MAGIC
# MAGIC このデモでは、Databricksの分散処理を使って、店舗・アイテムごとの詳細な予測を効率的に生成する方法を学びます。  
# MAGIC
# MAGIC <!-- %md
# MAGIC ## Demo Overview
# MAGIC
# MAGIC In this demo, you will learn how to efficiently generate fine-grained forecasts at the store-item level using Databricks' distributed processing capabilities. -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: データの確認
# MAGIC
# MAGIC トレーニングデータセットには、10の異なる店舗で50品目の5年間の店舗・アイテム単位の販売データを使用します。  
# MAGIC このデータは[過去のKaggleコンペティション](https://www.kaggle.com/competitions/demand-forecasting-kernels-only/data)として公開されていますが、今回は公開用Azure Blobから`./02_load_data` ノートブックを使用してダウンロードできます。
# MAGIC
# MAGIC <!-- %md
# MAGIC ## Step 1: Examine the Data
# MAGIC
# MAGIC The training dataset uses 5 years of store-item level sales data for 50 items across 10 different stores.  
# MAGIC This data was publicly available as part of a [past Kaggle competition](https://www.kaggle.com/competitions/demand-forecasting-kernels-only/data), but in this case, it can be downloaded from a public Azure Blob using the `./02_load_data` notebook. -->

# COMMAND ----------

# MAGIC %run "./01_config"

# COMMAND ----------

# MAGIC %md
# MAGIC 需要予測で人気の高いライブラリ、[prophet](https://facebook.github.io/prophet/) を使用します。

# COMMAND ----------

# DBTITLE 1,Install Required Libraries
# MAGIC %pip install prophet

# COMMAND ----------

# DBTITLE 1,Access the Dataset
from pyspark.sql.types import *

# structure of the training data set
train_schema = StructType([
  StructField('date', DateType()),
  StructField('store', IntegerType()),
  StructField('item', IntegerType()),
  StructField('sales', IntegerType())
  ])

# read the training file into a dataframe
train = spark.read.csv(
  f'/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME_IMPORT}/train/train.csv', 
  header=True, 
  schema=train_schema
  )

# make the dataframe queryable as a temporary view
train.createOrReplaceTempView('train')

# show data
display(train)

# COMMAND ----------

# MAGIC %md
# MAGIC 需要予測を行う際、一般的なトレンドや季節性に関心があることが多いです。まずは、年間の販売数量のトレンドを確認しましょう。
# MAGIC
# MAGIC <!-- %md
# MAGIC When performing demand forecasting, we are often interested in general trends and seasonality.  Let's start our exploration by examining the annual trend in unit sales: -->

# COMMAND ----------

# DBTITLE 1,View Yearly Trends
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   year(date) as year, 
# MAGIC   sum(sales) as sales
# MAGIC FROM train
# MAGIC GROUP BY year(date)
# MAGIC ORDER BY year;

# COMMAND ----------

# MAGIC %md
# MAGIC データを見ると、全体的に販売数量が増加しているのは明確です。データをざっと見る限り、数日から数ヶ月、または1年先までは成長が続くと考えても良さそうです。
# MAGIC
# MAGIC 次に季節性を見てみましょう。月ごとに集計すると、売上の増加に伴って拡大する年間の季節的パターンが確認できます。
# MAGIC
# MAGIC <!-- %md
# MAGIC It's very clear from the data that there is a generally upward trend in total unit sales across the stores. If we had better knowledge of the markets served by these stores, we might wish to identify whether there is a maximum growth capacity we'd expect to approach over the life of our forecast.  But without that knowledge and by just quickly eyeballing this dataset, it feels safe to assume that if our goal is to make a forecast a few days, months or even a year out, we might expect continued linear growth over that time span.
# MAGIC
# MAGIC Now let's examine seasonality.  If we aggregate the data around the individual months in each year, a distinct yearly seasonal pattern is observed which seems to grow in scale with overall growth in sales: -->

# COMMAND ----------

# DBTITLE 1,View Monthly Trends
# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   TRUNC(date, 'MM') as month,
# MAGIC   SUM(sales) as sales
# MAGIC FROM train
# MAGIC GROUP BY TRUNC(date, 'MM')
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %md
# MAGIC データを曜日ごとに集計すると、日曜日にピークがあり、月曜日に急落し、週の後半にかけて回復するパターンが見られます。このパターンは5年間ほぼ安定しています。
# MAGIC
# MAGIC <!-- %md
# MAGIC When the data is aggregated by day of the week, we see a pattern where sales peak on Sunday, drop sharply on Monday, and then recover towards the end of the week. This pattern has remained fairly stable over the past five years. -->

# COMMAND ----------

# DBTITLE 1,View Weekday Trends
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   YEAR(date) as year,
# MAGIC   (
# MAGIC     CASE
# MAGIC       WHEN DATE_FORMAT(date, 'E') = 'Sun' THEN 0
# MAGIC       WHEN DATE_FORMAT(date, 'E') = 'Mon' THEN 1
# MAGIC       WHEN DATE_FORMAT(date, 'E') = 'Tue' THEN 2
# MAGIC       WHEN DATE_FORMAT(date, 'E') = 'Wed' THEN 3
# MAGIC       WHEN DATE_FORMAT(date, 'E') = 'Thu' THEN 4
# MAGIC       WHEN DATE_FORMAT(date, 'E') = 'Fri' THEN 5
# MAGIC       WHEN DATE_FORMAT(date, 'E') = 'Sat' THEN 6
# MAGIC     END
# MAGIC   ) % 7 as weekday,
# MAGIC   AVG(sales) as sales
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     date,
# MAGIC     SUM(sales) as sales
# MAGIC   FROM train
# MAGIC   GROUP BY date
# MAGIC  ) x
# MAGIC GROUP BY year, weekday
# MAGIC ORDER BY year, weekday;

# COMMAND ----------

# MAGIC %md
# MAGIC データの基本的なパターンが把握できたので、次に予測の構築方法を探っていきましょう。
# MAGIC
# MAGIC <!-- %md
# MAGIC Now that we are oriented to the basic patterns within our data, let's explore how we might build a forecast. -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: 単一の予測を作成
# MAGIC
# MAGIC 店舗とアイテムの組み合わせごとに予測を生成する前に、Prophetの使い方に慣れるために、まず単一の予測を作成してみることが役立つかもしれません。  
# MAGIC 最初のステップとして、モデルをトレーニングするための過去データセットを準備しましょう。
# MAGIC
# MAGIC <!-- %md
# MAGIC ## Step 2: Build a Single Forecast
# MAGIC
# MAGIC Before attempting to generate forecasts for individual combinations of stores and items, it might be helpful to build a single forecast for no other reason than to orient ourselves to the use of prophet.
# MAGIC
# MAGIC Our first step is to assemble the historical dataset on which we will train the model: -->

# COMMAND ----------

# DBTITLE 1,Retrieve Data for a Single Item-Store Combination
# query to aggregate data to date (ds) level
sql_statement = '''
  SELECT
    CAST(date as date) as ds,
    sales as y
  FROM train
  WHERE store=1 AND item=1
  ORDER BY ds
  '''

# assemble dataset in Pandas dataframe
history_pd = spark.sql(sql_statement).toPandas()

# drop any missing records
history_pd = history_pd.dropna()

# COMMAND ----------

# MAGIC %md
# MAGIC 次に、prophetライブラリをインポートしますが、使用時にやや冗長になるため、環境のログ設定を調整する必要があります。
# MAGIC
# MAGIC <!-- %md
# MAGIC Now, we will import the prophet library, but because it can be a bit verbose when in use, we will need to fine-tune the logging settings in our environment: -->

# COMMAND ----------

# DBTITLE 1,Import Prophet Library
from prophet import Prophet
import logging

# disable informational messages from prophet
logging.getLogger('py4j').setLevel(logging.ERROR)

# COMMAND ----------

# MAGIC %md
# MAGIC データを確認した結果、成長パターンは線形に設定し、週と年の季節性パターンを有効にします。売上が増えるにつれて季節性も強くなるようなので、季節性モードは乗法に設定します。
# MAGIC
# MAGIC <!-- %md
# MAGIC Based on our review of the data, it looks like we should set our overall growth pattern to linear and enable the evaluation of weekly and yearly seasonal patterns. We might also wish to set our seasonality mode to multiplicative as the seasonal pattern seems to grow with overall growth in sales: -->

# COMMAND ----------

# DBTITLE 1,Train Prophet Model
# set model parameters
model = Prophet(
  interval_width=0.95,
  growth='linear',
  daily_seasonality=False,
  weekly_seasonality=True,
  yearly_seasonality=True,
  seasonality_mode='multiplicative'
  )

# fit the model to historical data
model.fit(history_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC モデルのトレーニングが完了したので、それを使って90日間の予測を作成しましょう。
# MAGIC
# MAGIC <!-- %md
# MAGIC Now that we have a trained model, let's use it to build a 90-day forecast: -->

# COMMAND ----------

# DBTITLE 1,Build Forecast
# define a dataset including both historical dates & 90-days beyond the last available date
future_pd = model.make_future_dataframe(
  periods=90, 
  freq='d', 
  include_history=True
  )

# predict over the dataset
forecast_pd = model.predict(future_pd)

display(forecast_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC モデルの結果はどうでしょうか？ここでは、モデルの全体的なトレンドと季節的なトレンドがグラフとして表示されています。
# MAGIC
# MAGIC <!-- %md
# MAGIC How did our model perform? Here we can see the general and seasonal trends in our model presented as graphs: -->

# COMMAND ----------

# DBTITLE 1,Examine Forecast Components
trends_fig = model.plot_components(forecast_pd)
display(trends_fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ここでは、実際のデータと予測データの比較、および将来の予測を見ることができます。  
# MAGIC ただし、グラフが読みやすいように、過去1年分のデータに限定しています。
# MAGIC
# MAGIC <!-- %md
# MAGIC And here, we can see how our actual and predicted data line up as well as a forecast for the future, though we will limit our graph to the last year of historical data just to keep it readable: -->

# COMMAND ----------

# DBTITLE 1,View Historicals vs. Predictions
# Plot the forecast results
predict_fig = model.plot(forecast_pd, xlabel='date', ylabel='sales')

# Adjust the graph: show the past year and 90-day forecast
xlim = predict_fig.axes[0].get_xlim()                   # Get the current range of the X-axis
new_xlim = (xlim[1] - (180.0 + 365.0), xlim[1] - 90.0)  # Set the new range for the X-axis
predict_fig.axes[0].set_xlim(new_xlim)                  # Apply the new X-axis range

# COMMAND ----------

# MAGIC %md
# MAGIC **注意** このビジュアライゼーションは少し複雑です。Bartosz Mikulski が[こちらで素晴らしい解説](https://www.mikulskibartosz.name/prophet-plot-explained/)を提供しているので、ぜひ参考にしてください。簡単に言うと、黒い点は実際のデータを表し、濃い青の線は予測、薄い青の帯は95%の予測区間を示しています。
# MAGIC
# MAGIC <!-- %md
# MAGIC **NOTE** This visualization is a bit busy. Bartosz Mikulski provides [an excellent breakdown](https://www.mikulskibartosz.name/prophet-plot-explained/) of it that is well worth checking out.  In a nutshell, the black dots represent our actuals with the darker blue line representing our predictions and the lighter blue band representing our (95%) uncertainty interval. -->

# COMMAND ----------

# MAGIC %md
# MAGIC 視覚的な確認も有用ですが、予測を評価するためのより良い方法は、実際の値に対する予測値について、平均絶対誤差（MAE）、平均二乗誤差（MSE）、および二乗平均平方根誤差（RMSE）の値を計算することです。
# MAGIC
# MAGIC **UPDATE**  
# MAGIC pandasの機能変更により、日付文字列を正しいデータ型に変換するために *pd.to_datetime* を使用する必要があります。
# MAGIC
# MAGIC <!-- %md
# MAGIC Visual inspection is useful, but a better way to evaluate the forecast is to calculate Mean Absolute Error, Mean Squared Error and Root Mean Squared Error values for the predicted relative to the actual values in our set:
# MAGIC
# MAGIC **UPDATE** A change in pandas functionality requires us to use *pd.to_datetime* to coerce the date string into the right data type. -->

# COMMAND ----------

# DBTITLE 1,Calculate Evaluation metrics
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_error
from math import sqrt
from datetime import date

# get historical actuals & predictions for comparison
actuals_pd = history_pd[ history_pd['ds'] < date(2018, 1, 1) ]['y']
predicted_pd = forecast_pd[ forecast_pd['ds'] < pd.to_datetime('2018-01-01') ]['yhat']

# calculate evaluation metrics
mae = mean_absolute_error(actuals_pd, predicted_pd)
mse = mean_squared_error(actuals_pd, predicted_pd)
rmse = sqrt(mse)

# print metrics to the screen
print( '\n'.join(['MAE: {0}', 'MSE: {1}', 'RMSE: {2}']).format(mae, mse, rmse) )

# COMMAND ----------

# MAGIC %md
# MAGIC prophetは、予測が時間経過とともにどのように維持されるかを評価するための[追加の手段](https://facebook.github.io/prophet/docs/diagnostics.html)を提供しています。  
# MAGIC 予測モデルを構築する際には、これらの手法の使用を強くお勧めしますが、今回はスケーリングの課題に焦点を当てるため、それらについては省略します。
# MAGIC
# MAGIC <!-- %md
# MAGIC prophet provides [additional means](https://facebook.github.io/prophet/docs/diagnostics.html) for evaluating how your forecasts hold up over time. You're strongly encouraged to consider using these and those additional techniques when building your forecast models but we'll skip this here to focus on the scaling challenge. -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: 予測生成のスケールアップ
# MAGIC
# MAGIC 基本的な手順を理解したので、元々の目的である店舗とアイテムの組み合わせごとに多数の詳細なモデルと予測を構築していきます。まずは、店舗・アイテム・日付単位で販売データを集計します。
# MAGIC
# MAGIC **注意**: このデータセットのデータはすでにこの粒度で集計されているはずですが、期待通りのデータ構造を持っていることを確認するために、改めて明示的に集計を行います。
# MAGIC
# MAGIC
# MAGIC <!-- %md
# MAGIC ## Step 3: Scale Forecast Generation
# MAGIC
# MAGIC With the mechanics under our belt, let's now tackle our original goal of building numerous, fine-grain models & forecasts for individual store and item combinations.  We will start by assembling sales data at the store-item-date level of granularity:
# MAGIC
# MAGIC **NOTE**: The data in this data set should already be aggregated at this level of granularity but we are explicitly aggregating to ensure we have the expected data structure. -->

# COMMAND ----------

# DBTITLE 1,Retrieve Data for All Store-Item Combinations
sql_statement = '''
  SELECT
    store,
    item,
    CAST(date as date) as ds,
    SUM(sales) as y
  FROM train
  GROUP BY store, item, ds
  ORDER BY store, item, ds
  '''

store_item_history = (
  spark
    .sql( sql_statement )
    .repartition(sc.defaultParallelism, ['store', 'item'])
  ).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC 店舗・アイテム・日付単位でデータを集計したので、これをどのようにprophetに渡すかを考えます。各店舗とアイテムの組み合わせごとにモデルを作るためには、データから店舗とアイテムのサブセットを渡し、そのサブセットでモデルを学習させ、予測を得ます。予測結果には、店舗とアイテムの識別子を保持し、Prophetモデルが生成した必要なフィールドだけが含まれます。
# MAGIC
# MAGIC <!-- %md
# MAGIC With our data aggregated at the store-item-date level, we need to consider how we will pass our data to prophet. If our goal is to build a model for each store and item combination, we will need to pass in a store-item subset from the dataset we just assembled, train a model on that subset, and receive a store-item forecast back. We'd expect that forecast to be returned as a dataset with a structure like this where we retain the store and item identifiers for which the forecast was assembled and we limit the output to just the relevant subset of fields generated by the Prophet model: -->

# COMMAND ----------

# DBTITLE 1,Define Schema for Forecast Output
from pyspark.sql.types import *

result_schema =StructType([
  StructField('ds',DateType()),
  StructField('store',IntegerType()),
  StructField('item',IntegerType()),
  StructField('y',FloatType()),
  StructField('yhat',FloatType()),
  StructField('yhat_upper',FloatType()),
  StructField('yhat_lower',FloatType())
  ])

# COMMAND ----------

# MAGIC %md
# MAGIC モデルをトレーニングして予測を作成するために、Pandas関数を使います。この関数は、店舗とアイテムごとに整理されたデータを受け取り、予測結果を返します。
# MAGIC
# MAGIC <!-- %md
# MAGIC To train the model and generate a forecast we will leverage a Pandas function.  We will define this function to receive a subset of data organized around a store and item combination.  It will return a forecast in the format identified in the previous cell:
# MAGIC
# MAGIC **UPDATE** With Spark 3.0, pandas functions replace the functionality found in pandas UDFs.  The deprecated pandas UDF syntax is still supported but will be phased out over time.  For more information on the new, streamlined pandas functions API, please refer to [this document](https://databricks.com/blog/2020/05/20/new-pandas-udfs-and-python-type-hints-in-the-upcoming-release-of-apache-spark-3-0.html). -->

# COMMAND ----------

# DBTITLE 1,Define Function to Train Model & Generate Forecast
def forecast_store_item( history_pd: pd.DataFrame ) -> pd.DataFrame:
  
  # TRAIN MODEL AS BEFORE
  # --------------------------------------
  # remove missing values (more likely at day-store-item level)
  history_pd = history_pd.dropna()
  
  # configure the model
  model = Prophet(
    interval_width=0.95,
    growth='linear',
    daily_seasonality=False,
    weekly_seasonality=True,
    yearly_seasonality=True,
    seasonality_mode='multiplicative'
    )
  
  # train the model
  model.fit( history_pd )
  # --------------------------------------
  
  # BUILD FORECAST AS BEFORE
  # --------------------------------------
  # make predictions
  future_pd = model.make_future_dataframe(
    periods=90, 
    freq='d', 
    include_history=True
    )
  forecast_pd = model.predict( future_pd )  
  # --------------------------------------
  
  # ASSEMBLE EXPECTED RESULT SET
  # --------------------------------------
  # get relevant fields from forecast
  f_pd = forecast_pd[ ['ds','yhat', 'yhat_upper', 'yhat_lower'] ].set_index('ds')
  
  # get relevant fields from history
  h_pd = history_pd[['ds','store','item','y']].set_index('ds')
  
  # join history and forecast
  results_pd = f_pd.join( h_pd, how='left' )
  results_pd.reset_index(level=0, inplace=True)
  
  # get store & item from incoming data set
  results_pd['store'] = history_pd['store'].iloc[0]
  results_pd['item'] = history_pd['item'].iloc[0]
  # --------------------------------------
  
  # return expected dataset
  return results_pd[ ['ds', 'store', 'item', 'y', 'yhat', 'yhat_upper', 'yhat_lower'] ]  

# COMMAND ----------

# MAGIC %md
# MAGIC この関数では、モデルのトレーニングや予測の部分は以前と同じで、新しいのは予測結果をPandasでまとめる標準的な処理だけです。
# MAGIC
# MAGIC <!-- %md
# MAGIC There's a lot taking place within our function, but if you compare the first two blocks of code within which the model is being trained and a forecast is being built to the cells in the previous portion of this notebook, you'll see the code is pretty much the same as before. It's only in the assembly of the required result set that truly new code is being introduced and it consists of fairly standard Pandas dataframe manipulations. -->

# COMMAND ----------

# MAGIC %md
# MAGIC では、Pandas関数を使って予測を作成しましょう。  
# MAGIC 店舗とアイテムごとにデータをグループ化し、関数を適用します。そして、データ管理用に今日の日付を *training_date* として追加します。Pandas UDFではなく `applyInPandas()` を使います。
# MAGIC
# MAGIC
# MAGIC <!-- %md
# MAGIC Now let's call our pandas function to build our forecasts.  We do this by grouping our historical dataset around store and item.  We then apply our function to each group and tack on today's date as our *training_date* for data management purposes:
# MAGIC
# MAGIC **UPDATE** Per the previous update note, we are now using applyInPandas() to call a pandas function instead of a pandas UDF. -->

# COMMAND ----------

# DBTITLE 1,Apply Forecast Function to Each Store-Item Combination
from pyspark.sql.functions import current_date

results = (
  store_item_history
    .groupBy('store', 'item')
      .applyInPandas(forecast_store_item, schema=result_schema)
    .withColumn('training_date', current_date() )
    )

results.createOrReplaceTempView('new_forecasts')

display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC 予測結果をクエリ可能なテーブル形式で保存しましょう。
# MAGIC
# MAGIC <!-- %md
# MAGIC We we are likely wanting to report on our forecasts, so let's save them to a queryable table structure: -->

# COMMAND ----------

# DBTITLE 1,Persist Forecast Output
# MAGIC %sql
# MAGIC
# MAGIC -- create forecast table
# MAGIC create table if not exists `${c.catalog}`.`${c.schema}`.forecasts (
# MAGIC   date date,
# MAGIC   store integer,
# MAGIC   item integer,
# MAGIC   sales float,
# MAGIC   sales_predicted float,
# MAGIC   sales_predicted_upper float,
# MAGIC   sales_predicted_lower float,
# MAGIC   training_date date
# MAGIC   )
# MAGIC using delta
# MAGIC partitioned by (date);
# MAGIC
# MAGIC -- load data to it
# MAGIC merge into `${c.catalog}`.`${c.schema}`.forecasts f
# MAGIC using new_forecasts n 
# MAGIC on f.date = n.ds and f.store = n.store and f.item = n.item
# MAGIC when matched then update set f.date = n.ds,
# MAGIC   f.store = n.store,
# MAGIC   f.item = n.item,
# MAGIC   f.sales = n.y,
# MAGIC   f.sales_predicted = n.yhat,
# MAGIC   f.sales_predicted_upper = n.yhat_upper,
# MAGIC   f.sales_predicted_lower = n.yhat_lower,
# MAGIC   f.training_date = n.training_date
# MAGIC when not matched then insert (date,
# MAGIC   store,
# MAGIC   item,
# MAGIC   sales,
# MAGIC   sales_predicted,
# MAGIC   sales_predicted_upper,
# MAGIC   sales_predicted_lower,
# MAGIC   training_date)
# MAGIC values (n.ds,
# MAGIC   n.store,
# MAGIC   n.item,
# MAGIC   n.y,
# MAGIC   n.yhat,
# MAGIC   n.yhat_upper,
# MAGIC   n.yhat_lower,
# MAGIC   n.training_date)

# COMMAND ----------

# MAGIC %md
# MAGIC しかし、各予測の精度はどれくらい良い（または悪い）のでしょうか？Pandas関数を使うことで、各店舗・アイテムの予測に対する評価指標を以下のように生成できます。
# MAGIC
# MAGIC <!-- %md
# MAGIC But how good (or bad) is each forecast?  Using the pandas function technique, we can generate evaluation metrics for each store-item forecast as follows -->

# COMMAND ----------

# DBTITLE 1,Apply Same Techniques to Evaluate Each Forecast
# schema of expected result set
eval_schema =StructType([
  StructField('training_date', DateType()),
  StructField('store', IntegerType()),
  StructField('item', IntegerType()),
  StructField('mae', FloatType()),
  StructField('mse', FloatType()),
  StructField('rmse', FloatType())
  ])

# define function to calculate metrics
def evaluate_forecast( evaluation_pd: pd.DataFrame ) -> pd.DataFrame:
  
  # get store & item in incoming data set
  training_date = evaluation_pd['training_date'].iloc[0]
  store = evaluation_pd['store'].iloc[0]
  item = evaluation_pd['item'].iloc[0]
  
  # calculate evaluation metrics
  mae = mean_absolute_error( evaluation_pd['y'], evaluation_pd['yhat'] )
  mse = mean_squared_error( evaluation_pd['y'], evaluation_pd['yhat'] )
  rmse = sqrt( mse )
  
  # assemble result set
  results = {'training_date':[training_date], 'store':[store], 'item':[item], 'mae':[mae], 'mse':[mse], 'rmse':[rmse]}
  return pd.DataFrame.from_dict( results )

# calculate metrics
results = (
  spark
    .table('new_forecasts')
    .filter('ds < \'2018-01-01\'') # limit evaluation to periods where we have historical data
    .select('training_date', 'store', 'item', 'y', 'yhat')
    .groupBy('training_date', 'store', 'item')
    .applyInPandas(evaluate_forecast, schema=eval_schema)
    )

results.createOrReplaceTempView('new_forecast_evals')

# COMMAND ----------

# MAGIC %md
# MAGIC 再び、各予測の評価指標をクエリ可能なテーブルとして保存します。
# MAGIC
# MAGIC <!-- %md
# MAGIC Once again, we will likely want to report the metrics for each forecast, so we persist these to a queryable table: -->

# COMMAND ----------

# DBTITLE 1,Persist Evaluation Metrics
# MAGIC %sql
# MAGIC
# MAGIC create table if not exists `${c.catalog}`.`${c.schema}`.forecast_evals (
# MAGIC   store integer,
# MAGIC   item integer,
# MAGIC   mae float,
# MAGIC   mse float,
# MAGIC   rmse float,
# MAGIC   training_date date
# MAGIC   )
# MAGIC using delta
# MAGIC partitioned by (training_date);
# MAGIC
# MAGIC insert into `${c.catalog}`.`${c.schema}`.forecast_evals
# MAGIC select
# MAGIC   store,
# MAGIC   item,
# MAGIC   mae,
# MAGIC   mse,
# MAGIC   rmse,
# MAGIC   training_date
# MAGIC from new_forecast_evals;

# COMMAND ----------

# MAGIC %md
# MAGIC これで、各店舗・アイテムの組み合わせごとの予測を作成し、基本的な評価指標を生成しました。この予測データを確認するには、シンプルなクエリを発行できます（ここでは、商品1を対象に店舗1から3までに限定しています）。
# MAGIC
# MAGIC <!-- %md
# MAGIC We now have constructed a forecast for each store-item combination and generated basic evaluation metrics for each.  To see this forecast data, we can issue a simple query (limited here to product 1 across stores 1 through 3): -->

# COMMAND ----------

# DBTITLE 1,Visualize Forecasts
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   store,
# MAGIC   date,
# MAGIC   sales_predicted,
# MAGIC   sales_predicted_upper,
# MAGIC   sales_predicted_lower
# MAGIC FROM `${c.catalog}`.`${c.schema}`.forecasts a
# MAGIC WHERE item = 1 AND
# MAGIC       store IN (1, 2, 3) AND
# MAGIC       date >= '2018-01-01' AND
# MAGIC       training_date=current_date()
# MAGIC ORDER BY store

# COMMAND ----------

# MAGIC %md
# MAGIC これらの予測について、それぞれの信頼性を評価するための指標を取得できます。
# MAGIC
# MAGIC <!-- %md
# MAGIC And for each of these, we can retrieve a measure of help us assess the reliability of each forecast: -->

# COMMAND ----------

# DBTITLE 1,Retrieve Evaluation Metrics
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   store,
# MAGIC   mae,
# MAGIC   mse,
# MAGIC   rmse
# MAGIC FROM `${c.catalog}`.`${c.schema}`.forecast_evals a
# MAGIC WHERE item = 1 AND
# MAGIC       training_date=current_date()
# MAGIC ORDER BY store
