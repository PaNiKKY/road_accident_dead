import json
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.sdk import Variable

from dotenv import load_dotenv
import sys
import os
from datetime import datetime
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.extract import load_to_dataframe
from etl.transform import transform_pipeline

load_dotenv()



project_id = os.getenv("project_id")
bucket_prefix = os.getenv("bucket_prefix")
bigqury_dataset = os.getenv("bigquery_dataset")
bigqury_fact_table = os.getenv("bigquery_fact_table")
schema_file = os.getenv("schema_file")
upsert_file = os.getenv("upsert_file")

bucket_name = f"{project_id}-{bucket_prefix}"

with open(schema_file, "r") as f:
    schema = json.load(f)

with open(upsert_file, "r") as u:
    query_string = u.read()

@dag(
    schedule="*/5 * * * *",
    # schedule=None,
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['gcp', 'etl']
)
def accidents_dag():
    year = Variable.get("year")
    url = os.getenv(f"url_{year}")

    create_bucket_task = GCSCreateBucketOperator(
        task_id='create_bucket_if_not_exists',
        gcp_conn_id='gcp_conn',
        bucket_name=bucket_name,
        project_id=project_id,
    )

    @task(task_id='extract')
    def extract_task():
        print(f"Extracting data from {year}...")
        csv = load_to_dataframe(url)

        gcs_hook = GCSHook(gcp_conn_id='gcp_conn')
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=f'raw/{year}.csv',
            data=csv,
            mime_type='text/csv'
        )

    @task.pyspark(conn_id = "spark_conn", task_id='transform')
    def transform_task():  
        # load the CSV file from GCS
        gcs_hook = GCSHook(gcp_conn_id='gcp_conn')
        data = gcs_hook.download(
            bucket_name=bucket_name, 
            object_name=f'raw/{year}.csv')

        # transform the data with pyspark
        transform_parquet_byte = transform_pipeline(
            string_data=data,
            year=year,
        )

        # upload the transformed data to GCS
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=f'transformed/{year}.parquet',
            data=transform_parquet_byte,
            mime_type='application/parquet'
        )
    

    @task_group(group_id='load')
    def load_task_group():
        gcs_to_bq = GCSToBigQueryOperator(
                task_id='gcs_to_bigquery',
                bucket=bucket_name,
                source_objects=f'transformed/{year}.parquet',
                destination_project_dataset_table=f'{project_id}.{bigqury_dataset}.temp_{bigqury_fact_table}',
                schema_fields=schema,
                source_format='PARQUET',
                write_disposition='WRITE_TRUNCATE',
                gcp_conn_id='gcp_conn',
            )
        
        upsert_to_fact_table = BigQueryInsertJobOperator(
            task_id='upsert_to_fact_table',
            configuration={
                    "query": {
                        "query":query_string.format(
                            project_id=project_id,
                            bigqury_dataset=bigqury_dataset,
                            bigqury_fact_table=bigqury_fact_table
                        ),
                        "useLegacySql": False,
                    }
                },
                gcp_conn_id='gcp_conn',
            )

        gcs_to_bq >> upsert_to_fact_table
    
    @task
    def increase_year():
        Variable.set(key="year", value=str(int(year)+1))

    
        
    chain(create_bucket_task, extract_task(), transform_task(), load_task_group(), increase_year())
    

accidents_dag()