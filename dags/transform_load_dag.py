from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import sys
import os
from datetime import datetime
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
@dag(
    schedule='@daily',
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['gcp', 'extract']
)
def accidents_transform_load_dag():

    transform_task = SparkSubmitOperator(
        task_id='transform_task',
        application="etl/transform.py",
        name='accidents_transform',
        conn_id='spark_conn',
        verbose=True,
        # conf={
        #     'spark.executor.memory': '1g',
        #     'spark.driver.memory': '1g',
        #     'spark.sql.adaptive.enabled': 'false',
        #     'spark.dynamicAllocation.enabled': 'false',
        #     'spark.serializer': 'org.apache.spark.serializer.KryoSerializer'
        # },
    )
    
    transform_task

accidents_transform_load_dag()