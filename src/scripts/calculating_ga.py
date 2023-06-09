import os
import sys
import pyspark.sql.functions as F 
import math
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from datetime import datetime, timedelta
from pyspark.sql.functions import regexp_replace
from pyspark.sql.window import Window 
from pyspark.sql.types import *
from math import radians, cos, sin, asin, sqrt

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'




u_name = 'mityaov'
hdfs_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net'
date = sys.argv[1]
d = sys.argv[2]
dir_src = sys.argv[3] #/user/master/data/geo/events
geodata_csv = f'{hdfs_path}/user/{u_name}/data/cities/geo.csv'

#исходные данные
input_paths = [f"{dir_src}/date={(datetime.strptime(date, '%Y-%m-%d') - timedelta(days = x)).strftime('%Y-%m-%d')}" for x in range(int(d))]

#Spark-сессия
conf = SparkConf().setAppName(f"datamart_ga-{date}-d{d}")
sc = SparkContext(conf=conf)
spark = SQLContext(sc)
    
geo_csv = spark.read.csv(geodata_csv, sep = ';', header = True)
geo_csv = geo_csv.withColumn("lat",regexp_replace("lat", ",", ".")).withColumn("lng",regexp_replace("lng",",","."))
citygeo_df = geo_csv.select(F.col("id").cast(LongType()).alias("city_id"),(F.col("city")).alias("city_name"),(F.col("lat")).cast(DoubleType()).alias("city_lat"),(F.col("lng")).cast(DoubleType()).alias("city_lon"))

df_events = spark.read.parquet(*input_paths) \
    .where("event_type = 'message' or event_type = 'reaction' or event_type = 'subscription'") \
    .select(F.col("event.message_id").alias("message_id"), F.col("event_type"), F.to_date(F.coalesce(F.col('event.datetime'),F.col('event.message_ts'))).alias("date"), F.col("event.message_from").alias("user_id"), (F.col("lat").cast('double')*math.pi/180).alias("message_lat"),(F.col("lon").cast('double')*math.pi/180).alias("message_lon"))

df_distances = df_events.crossJoin(citygeo_df.hint("broadcast")) \
    .withColumn('dist', F.lit(2)*F.lit(6371)*F.asin(
        F.sqrt(
            F.pow(F.sin((F.col('message_lat')-F.col('city_lat'))/F.lit(2)),2) \
           +F.cos('message_lat') \
           *F.cos('city_lat') \
           *F.pow(F.sin((F.col('message_lon')-F.col('city_lon'))/F.lit(2)),2))
        )
    ) \
    .withColumn('rank', F.row_number().over(Window().partitionBy('message_id').orderBy('dist')))\
    .filter(F.col('rank')==1)\
    .select('message_id', 'event_type', 'date', 'user_id', 'city_name', 'dist')


#показатели по сообщениям
df_message_benchmarks = df_distances.filter(F.col('event_type')=='message') \
    .withColumnRenamed("city_name", "zone_id") \
    .withColumn("week", F.date_trunc("week",F.col('date'))) \
    .withColumn("month", F.date_trunc("month",F.col('date'))) \
    .withColumn("week_message",F.count("message_id").over(Window.partitionBy("zone_id", "week"))) \
    .withColumn("month_message",F.count("message_id").over(Window.partitionBy("zone_id", "month"))) \
    .select('zone_id','month','month_message','week','week_message') \
    .distinct()

    #показатели по реакциям
df_reaction_benchmarks = df_distances.filter(F.col('event_type')=='reaction') \
    .withColumnRenamed("city_name", "zone_id") \
    .withColumn("week", F.date_trunc("week",F.col('date'))) \
    .withColumn("month", F.date_trunc("month",F.col('date'))) \
    .withColumn("week_reaction",F.count("message_id").over(Window.partitionBy("zone_id", "week"))) \
    .withColumn("month_reaction",F.count("message_id").over(Window.partitionBy("zone_id", "month"))) \
    .select('zone_id','month','month_reaction','week','week_reaction') \
    .distinct()

    #показатели по подпискам
df_subscription_benchmarks = df_distances.filter(F.col('event_type')=='subscription') \
    .withColumnRenamed("city_name", "zone_id") \
    .withColumn("week", F.date_trunc("week",F.col('date'))) \
    .withColumn("month", F.date_trunc("month",F.col('date'))) \
    .withColumn("week_subscription",F.count("message_id").over(Window.partitionBy("zone_id", "week"))) \
    .withColumn("month_subscription",F.count("message_id").over(Window.partitionBy("zone_id", "month"))) \
    .select('zone_id','month','month_subscription','week','week_subscription') \
    .distinct()

    #показатели по пользователям
df_users_benchmarks = df_distances \
    .withColumnRenamed("city_name", "zone_id") \
    .withColumn("week", F.date_trunc("week",F.col('date'))) \
    .withColumn("month", F.date_trunc("month",F.col('date'))) \
    .withColumn("row", (F.row_number().over(Window.partitionBy("user_id").orderBy(F.col("date").asc())))).filter(F.col("row") == 1) \
    .withColumn("week_user", (F.count("row").over(Window.partitionBy("zone_id", "week")))) \
    .withColumn("month_user", (F.count("row").over(Window.partitionBy("zone_id", "month")))) \
    .select("zone_id", "week", "month", "week_user", "month_user").distinct()


#формирование финального датафрейма для витрины 
ga_data_mart_df = df_users_benchmarks \
    .join(df_message_benchmarks, ['zone_id','week','month'], how = 'left') \
    .join(df_reaction_benchmarks, ['zone_id','week','month'], how ='left') \
    .join(df_subscription_benchmarks, ['zone_id','week','month'], how ='left') \
    .fillna(0) \
    .select('month','week','zone_id','week_message','week_reaction','week_subscription','week_user','month_message','month_reaction','month_subscription','month_user') \
    .orderBy('month', 'week','zone_id')

    
#Сохранение витрины для аналитиков на hdfs 
( ga_data_mart_df
        .write
            .mode("overwrite") 
            .parquet(f"{hdfs_path}/user/{u_name}/data_marts/geo_analitics_{date}_{d}")
    )