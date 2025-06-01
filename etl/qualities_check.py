from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType, DateType, DoubleType, BooleanType
from pyspark.sql.functions import when, col, struct, split, to_date, array, concat_ws
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.gcp_utils import download_parquet_from_gcs
from src.great_expectations import test_expectation

project_id = "sodium-keel-461511-u2"
bucket_name = f"{project_id}-bucket"
source_blob_name = "transformed/accidents_2555.parquet"

df = download_parquet_from_gcs(bucket_name, source_blob_name)

result = test_expectation(df)

print(f"Expectation result: {result}")
