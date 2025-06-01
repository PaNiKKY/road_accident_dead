from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Variable
import sys
import os
from datetime import datetime
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.transform import transform_pipeline

source_blob_name = "accidents_2556"
project_id = Variable.get("project_id")

@dag(
    schedule=None,
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['gcp', 'extract']
)
def accidents_transform_load_dag():

    @task.pyspark(conn_id = "spark_conn")
    def transform_task():
        transform_pipeline(
            projectId=project_id,
            folder_source="raw",
            folder_dest="transformed",
            source_blob_name=source_blob_name,
        )

    transform_task()

accidents_transform_load_dag()