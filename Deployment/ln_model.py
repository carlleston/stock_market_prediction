import findspark
import pyspark
import sys
import ibmos2spark

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import Normalizer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import percent_rank
from pyspark.sql import Window
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.sql import Window
import pyspark.sql.functions as func

from pyspark.sql.functions import udf,col
from pyspark.sql.types import IntegerType

from pyspark import SparkContext, SparkConf
import sys

if __name__ == "__main__":

    conf = SparkConf().setAppName("Spark lnmodel")
    sc = SparkContext(conf=conf)
    spark=SparkSession(sc)
    findspark.init()
    findspark.find()

    df_ts = spark.read.format("csv").option("header", "true").load(sys.argv[1])
    df_ts = df_ts[['Date','Itau_open','BVSP_open','USDBRL_open','Itau_Close','lag_1','lag_2','lag_3']]

    df_news = spark.read.format("csv").option("header", "true").load(sys.argv[2])
    df_news = df_news[['Date', 'Class']]

    df = df_ts.alias('a').join(df_news.alias('b'), on = ['Date'], how = 'outer')
    df = df.withColumn('Date',to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast("timestamp"))).orderBy('Date')
    df = df.withColumn('Itau_open', col('Itau_open').cast('double'))
    df = df.withColumn('BVSP_open', col('BVSP_open').cast('double'))   
    df = df.withColumn('USDBRL_open', col('USDBRL_open').cast('double'))
    df = df.withColumn('Itau_Close', col('Itau_Close').cast('double'))
    df = df.withColumn('lag_1', col('lag_1').cast('double'))
    df = df.withColumn('lag_2', col('lag_2').cast('double'))
    df = df.withColumn('lag_3', col('lag_3').cast('double'))
    window_ff = Window.orderBy('Date')\
                   .rowsBetween(-sys.maxsize, 0)

    read_last = func.last(df['Class'],
                          ignorenulls=True)\
                    .over(window_ff)
    # add columns to the dataframe
    df = df.withColumn('Class', read_last)
    df = df.na.fill({'Class': 'NN'})
    categories = df.select('Class').distinct().rdd.flatMap(lambda x : x).collect()
    categories.sort()
    for category in categories:
        function = udf(lambda item: 1 if item == category else 0, IntegerType())
        new_column_name = 'class'+'_'+category
        df = df.withColumn(new_column_name, function(col('class')))

    df = df[['Date','Itau_open','BVSP_open','USDBRL_open','Itau_Close','class_N','class_NN','class_P','lag_1','lag_2','lag_3']]

    FEATURES_COL1 = ['Itau_open','BVSP_open','USDBRL_open','lag_1','lag_2','lag_3']
    vectorAssembler = VectorAssembler(inputCols=FEATURES_COL1,outputCol="features")
    vdf = vectorAssembler.transform(df.na.drop())
    vdf = vdf.select(['Date','Itau_Close','features','class_N','class_NN','class_P'])
    scale_features = MinMaxScaler(inputCol= 'features', outputCol= 'scaled_features')
    model_scale = scale_features.fit(vdf)
    df_scaled = model_scale.transform(vdf)
    FEATURES_COL1 = ['scaled_features','class_N','class_NN','class_P']
    vectorAssembler = VectorAssembler(inputCols=FEATURES_COL1,outputCol="Col_features")
    df_completed = vectorAssembler.transform(df_scaled)
    df_completed = df_completed.select(['Date','Itau_Close','Col_features'])
    df_completed = df_completed.withColumn("rank", percent_rank().over(Window.partitionBy().orderBy("Date")))
    train_df = df_completed.where("rank <= .95").drop("rank")
    test_df = df_completed.where("rank > .95").drop("rank")
    lr = LinearRegression(featuresCol = 'Col_features', labelCol='Itau_Close')
    lr_model = lr.fit(train_df)
    lr_predictions = lr_model.transform(test_df)
    lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
                     labelCol="Itau_Close",metricName="r2")
    test_result = lr_model.evaluate(test_df)
    predictions = lr_model.transform(test_df)
    predictions.select("prediction","Itau_close").write.option("header", "true").mode("overwrite").parquet('pred.parquet')

