import os
import sys
from google.cloud import storage
import pandas as pd
from io import BytesIO

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/opt/airflow/key.json"


def create_bucket(bucket_name, project_id=None):
    """Creates a new bucket in Google Cloud Storage."""

    # Initialize a storage client
    storage_client = storage.Client()

    # Create the bucket
    bucket = storage_client.bucket(bucket_name)
    bucket.location = 'US'  # Set the location of the bucket

    try:
        bucket.create(project=project_id, location="us")
        print(f"Bucket {bucket_name} created.")
    except Exception as e:
        print(f"Failed to create bucket {bucket_name}: {e}")

def upload_to_gcs(bucket_name, df, destination_blob_name):
    """Uploads a file to the bucket."""
    # Initialize a storage client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a new blob and upload the file's content
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(df, content_type='text/csv')

    print(f"uploaded to {destination_blob_name}.")

def download_from_gcs(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""
    # Initialize a storage client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Get the blob
    blob = bucket.blob(source_blob_name)

    # Download the blob's content into a DataFrame
    data = blob.download_as_string()
    df = pd.read_csv(BytesIO(data))

    print(f"Downloaded {source_blob_name} from {bucket_name}.")
    return df
