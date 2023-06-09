import os
import math
import sys
import pyspark.sql.functions as F 
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
conf = SparkConf().setAppName(f"datamart_friends_recomendation-{date}-d{d}")
sc = SparkContext(conf=conf)
spark = SQLContext(sc)
    


#geo_scv = f'{hdfs_link}/user/{u_name}/data/cities/geo.csv'
geo_csv = spark.read.csv(geodata_csv, sep = ';', header = True)
geo_csv = geo_csv.withColumn("lat",regexp_replace("lat", ",", ".")).withColumn("lng",regexp_replace("lng",",","."))
citygeo_df = geo_csv.select(F.col("id").cast(LongType()).alias("city_id"),(F.col("city")).alias("city_name"),(F.col("lat")).cast(DoubleType()).alias("city_lat"),(F.col("lng")).cast(DoubleType()).alias("city_lon"))

df_events = spark.read.parquet(*input_paths) \
    .where(((F.col('event_type') == 'message') & (F.col('event.message_to').isNotNull())) | (F.col('event_type') == 'subscription')
                ).withColumn('message_lat', F.col('lat')/F.lit(57.3)) \
                 .withColumn('message_lon', F.col('lon')/F.lit(57.3)) \
                 .withColumn('user_id',
                    F.when(F.col('event_type') == 'subscription',
                           F.col('event.user')) \
                           .otherwise(F.col('event.message_from'))
                            ).withColumn('ts',F.when(F.col('event_type') == 'subscription',F.col('event.datetime')) \
                    .otherwise(F.col('event.message_ts'))).drop('lat', 'lon', 'date') 
#разделим события на сообщения и подписки
df_messages = df_events.where(F.col('event_type') == 'message')
df_subscriptions = df_events.where(F.col('event_type') == 'subscription')

df_distances = df_events.crossJoin(citygeo_df.hint("broadcast")) \
    .withColumn('dist', F.lit(2)*F.lit(6371)*F.asin(
        F.sqrt(
            F.pow(F.sin((F.col('message_lat')-F.col('city_lat'))/F.lit(2)),2) \
           +F.cos('message_lat') \
           *F.cos('city_lat') \
           *F.pow(F.sin((F.col('message_lon')-F.col('city_lon'))/F.lit(2)),2))
        )
    ) \
    .withColumn('rank', F.row_number().over(Window().partitionBy('user_id').orderBy('dist')))\
    .filter(F.col('rank')==1)\
    .select('event_type','message_lat','message_lon', 'ts', 'user_id', 'city_name', 'dist')

#определим пользователей, подписанных на один канал
users_in_chanel = df_subscriptions.select('event.subscription_channel', 'user_id')
user_left = users_in_chanel.withColumnRenamed('user_id', 'user_left')
user_right = users_in_chanel.withColumnRenamed('user_id', 'user_right')
users_pair = user_left.join(user_right,[user_left.subscription_channel == user_right.subscription_channel, user_left.user_left != user_right.user_right],'inner').select('user_left', 'user_right').distinct()

#определим пользователей не переписывающихся друг с другом
contacts = df_events.select('event.message_from', 'event.message_to').distinct()
users_pair = users_pair.join(contacts, [((users_pair.user_left == contacts.message_from) & (users_pair.user_right == contacts.message_to)) | ((users_pair.user_right == contacts.message_from) & (users_pair.user_left == contacts.message_to))], 'leftanti')

# Определение актуальных значений для пользователей
window = Window().partitionBy('user_id').orderBy(F.desc('ts'))
user_coordinates = df_distances.select(
    'user_id', 
    'city_name',
    F.first('message_lat', True).over(window).alias('act_lat'),
    F.first('message_lon', True).over(window).alias('act_lng'),
    F.first('city_name', True).over(window).alias('zone_id'),
    F.first('ts', True).over(window).alias('act_ts'),
).distinct()

# Формирование финального датафрейма для витрины 3
df_friends_recomendations = users_pair \
    .join(user_coordinates, users_pair.user_left == user_coordinates.user_id, 'left') \
    .withColumnRenamed('user_id', 'lu').withColumnRenamed('act_lat', 'lat1').withColumnRenamed('act_lng', 'lng1').withColumnRenamed('zone_id', 'zone_id1').withColumnRenamed('act_ts', 'act_ts1').withColumnRenamed('tz', 'tz1') \
    .join(user_coordinates, users_pair.user_right == user_coordinates.user_id, 'left') \
    .withColumnRenamed('user_id', 'ru').withColumnRenamed('act_lat', 'lat2').withColumnRenamed('act_lng', 'lng2').withColumnRenamed('zone_id', 'zone_id2').withColumnRenamed('act_ts', 'act_ts2').withColumnRenamed('tz', 'tz2') \
    .withColumn('distance',
        F.lit(2)*F.lit(6371)*F.asin(
            F.sqrt(
                F.pow(F.sin((F.col('lat2') - F.col('lat1'))/F.lit(2)), 2)\
                + F.cos('lat1') * F.cos('lat2') * F.pow(F.sin((F.col('lng2')-F.col('lng1'))/F.lit(2)), 2)
            )
        )
    ).where(F.col('distance') <= 1) \
    .select(
        'user_left',
        'user_right',
        F.current_timestamp().alias('processed_dttm'),
        F.when(F.col('zone_id1') == F.col('zone_id2'), F.col('zone_id1')).alias('zone_id'),
        F.from_utc_timestamp(F.current_timestamp(), 'Australia/Sydney').alias('local_time')
    )
#Сохранение витрины для аналитиков на hdfs 
(df_friends_recomendations
        .write.mode("overwrite").parquet(f"{hdfs_path}/user/{u_name}/data_marts/friends_recomendation{date}_{d}")
    )