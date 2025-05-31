import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.gcp_utils import create_bucket, upload_to_gcs

def upload_data_to_gcs(bucket_name, project_id, df, destination_blob_name):
    """
    Extracts data by creating a GCS bucket and uploading a DataFrame to it.
    
    Args:
        bucket_name (str): The name of the GCS bucket.
        project_id (str): The GCP project ID.
        df (pd.DataFrame): The DataFrame to upload.
        destination_blob_name (str): The destination blob name in GCS.
        df (pd.DataFrame): The DataFrame to upload.
    """

    csv = df.to_csv(index=False)
    
    create_bucket(bucket_name, project_id)

    upload_to_gcs(bucket_name, csv, destination_blob_name)

    print(f"Data extracted and uploaded to {destination_blob_name} in bucket {bucket_name}.")

