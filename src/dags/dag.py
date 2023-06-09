import logging
import os 
import pyspark.sql.functions as F
from airflow import DAG
from datetime import date, datetime
from datetime import timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql.window import Window

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


default_args = {
    'owner': 'mityaov',
    'start_date': datetime.today(),
}


dag = DAG(
    dag_id="Dag_create_and_upload_datamarts",
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
)

#Задача с созданием витрины в разрезе пользователей
calculating_ua_task = SparkSubmitOperator(
    task_id='calculating_ua',
    dag=dag,
    application='hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net/user/mityaov/project_jobs/calculating_ua.py',
    conn_id='yarn_spark',
    application_args=['2022-05-31', '30', '/user/master/data/geo/events'],
    executor_cores=2,
    executor_memory='2g',
    verbose=True
)
logger.info("calculating_ua_task added to dag")

#Задача с созданием витрины в разрезе зон
calculating_ga_task = SparkSubmitOperator(
    task_id='calculating_ga',
    dag=dag,
    application='hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net/user/mityaov/project_jobs/calculating_ga.py',
    conn_id='yarn_spark',
    application_args=['2022-05-31', '30', '/user/master/data/geo/events'],
    executor_cores=2,
    executor_memory='2g',
    verbose=True
)
logger.info("calculating_ga_task added to dag")

#Задача с созданием витрины рекомендации друзей
friends_recomendation_task = SparkSubmitOperator(
    task_id='friends_recomendation',
    dag=dag,
    application='hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net/user/mityaov/project_jobs/friends_recomendation.py',
    conn_id='yarn_spark',
    application_args=['2022-05-31', '30', '/user/master/data/geo/events'],
    executor_cores=2,
    executor_memory='2g',
    verbose=True
)
logger.info("friends_recomendation_task added to dag")


calculating_ua_task >> calculating_ga_task >> friends_recomendation_task

#Как вариант можно запараллелить задачи, но тогда будет нужно больше ресурсов