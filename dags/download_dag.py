from airflow.decorators import dag, task
import sys
import os
from datetime import datetime
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.extract import upload_data_to_gcs


@dag(
    schedule='@daily',
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['gcp', 'extract']
)
def download_dag():
    @task
    def extract_task(year):
        project_id = "de-zoomcamp-course-415504"
        bucket_name = f"{project_id}-raw"
        
        destination_blob_name = f'accidents_{year}.csv'

        download_link = f"https://data.go.th/dataset/f5804870-7dc2-42df-86f3-769d6cc2ae23/resource/c61d2448-b953-4a2f-9cd8-6ab8f41ea487/download/_{year}.csv"

        df = pd.read_csv(download_link, encoding="TIS-620")
        
        upload_data_to_gcs(bucket_name, project_id, df, destination_blob_name)
    
    for year in range(2554, 2568):
        extract_task(year)

download_dag()