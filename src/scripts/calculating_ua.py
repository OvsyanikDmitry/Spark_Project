import os
import sys
import pyspark.sql.functions as F 
import math
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from datetime import datetime, timedelta
from pyspark.sql.functions import regexp_replace, from_utc_timestamp
from pyspark.sql.window import Window 
from pyspark.sql.functions import *
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
conf = SparkConf().setAppName(f"datamart_ua-{date}-d{d}")
sc = SparkContext(conf=conf)
spark = SQLContext(sc)
    


#geo_scv = f'{hdfs_link}/user/{u_name}/data/cities/geo.csv'
geo_csv = spark.read.csv(geodata_csv, sep = ';', header = True)
geo_csv = geo_csv.withColumn("lat",regexp_replace("lat", ",", ".")).withColumn("lng",regexp_replace("lng",",","."))
citygeo_df = geo_csv.select(F.col("id").cast(LongType()).alias("city_id"),(F.col("city")).alias("city_name"),(F.col("lat")).cast(DoubleType()).alias("city_lat"),(F.col("lng")).cast(DoubleType()).alias("city_lon"))

    
#DF со всеми сообщениями
# Выборка нужных атрибутов событий
df_events = spark.read.parquet(*input_paths) \
    .where("event_type = 'message' and event.message_id is not null") \
    .select(F.col("event.message_id").alias("message_id"), F.to_date(F.coalesce(F.col('event.datetime'),F.col('event.message_ts'))).alias("date"), F.col("event.message_from").alias("user_id"), (F.col("lat").cast('double')*math.pi/180).alias("message_lat"),(F.col("lon").cast('double')*math.pi/180).alias("message_lon"))

df_distances = df_events.crossJoin(citygeo_df.hint("broadcast")) \
    .withColumn('dist', F.lit(2) * F.lit(6371) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.radians(F.col('message_lat')) - F.radians(F.col('city_lat'))) / F.lit(2)), 2) \
            + F.cos(F.radians('message_lat')) \
            * F.cos(F.radians('city_lat')) \
            * F.pow(F.sin((F.radians(F.col('message_lon')) - F.radians(F.col('city_lon'))) / F.lit(2)), 2))
    ) )\
    .withColumn('rank', F.row_number().over(Window().partitionBy('message_id').orderBy('dist'))) \
    .filter(F.col('rank') == 1) \
    .select('message_id', 'date', 'user_id', 'city_name', 'dist', F.lit('event_type').alias('event_type'))
# Вычисление актуальных городов
df_active = df_distances \
    .withColumn("row",F.row_number().over(Window().partitionBy("user_id").orderBy(F.col("date").desc()))) \
    .filter(F.col("row") == 1) \
    .withColumnRenamed("city_name", "act_city") \
    .select('user_id', 'act_city')

# Промежуточный датафрейм
df_tmp = df_distances \
    .withColumn('max_date',F.max('date').over(Window().partitionBy('user_id'))) \
    .withColumn('city_prev',F.lag('city_name',-1,'start').over(Window().partitionBy('user_id').orderBy(F.col('date').desc()))) \
    .filter(F.col('city_name') != F.col('city_prev'))

# Вычисление home_cities
df_home_city = df_tmp \
    .withColumn('date_lag', F.coalesce(
            F.lag('date').over(Window().partitionBy('user_id').orderBy(F.col('date').desc())),
            F.col('max_date')
    )) \
    .withColumn('date_diff',F.datediff(F.col('date_lag'),F.col('date'))) \
    .filter(F.col('date_diff') > 27) \
    .withColumn('row',F.row_number().over(Window.partitionBy("user_id").orderBy(F.col("date").desc()))) \
    .filter(F.col('row') == 1) \
    .drop('dist','city_prev','date_lag','row','date_diff','max_date') 
    #.withColumn("local_time", F.from_utc_timestamp(F.col("date"),F.col('timezone'))) 

local_time_df = (
        df_home_city
        .withColumn("timezone", F.concat(F.lit("Australia/"), F.col('city_name')) )
        .withColumn("local_time", F.from_utc_timestamp(F.col("date"),F.col('timezone')))
        .drop("timezone", "date", "act_city"))
# Вычисление количества и названий посещенных городов
df_cities_count = df_tmp.groupBy("user_id").count().withColumnRenamed("count", "travel_count")
df_cities_names = df_tmp.groupBy("user_id").agg(F.collect_list('city_name').alias('travel_array'))

# Формирование финального датафрейма 
ua_data_mart_df = df_active \
    .join(df_home_city.drop('city_name'), 'user_id', how='left')  \
    .join(df_cities_count, 'user_id', how='left') \
    .join(df_cities_names, 'user_id', how='left') \
    .join(local_time_df, 'user_id', how='left')

ua_data_mart_df = ua_data_mart_df.drop('event_type', 'city_name', 'message_id')

#Сохранение витрины на hdfs 
(ua_data_mart_df.write.mode("overwrite") 
            .parquet(f"{hdfs_path}/user/{u_name}/data_marts/users_analitics_{date}_{d}"))
    



