from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType, DateType, DoubleType, BooleanType
from pyspark.sql.functions import when, col, struct, split, to_date, array, concat_ws
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.gcp_utils import download_from_gcs, upload_to_gcs


def spark_transform(df):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("TransformData") \
        .getOrCreate()
    
    # Read the CSV file into a Spark DataFrame
    sparkDF=spark.createDataFrame(df)


    sparkDF = sparkDF.select(
        col("Dead Conso Id").alias("dead_id").cast(IntegerType()), \
        col("DEAD_YEAR").alias("dead_year").cast(IntegerType()),\
        col("Age").alias("age").cast(IntegerType()),\
        col("Sex").alias("sex").cast(StringType()),\
        col("Risk Helmet").alias("risk_helmet").cast(IntegerType()),\
        col("Risk Safety Belt").alias("risk_safetybelt").cast(IntegerType()),\
        to_date(col("Dead Date Final"), "d/M/yyyy").alias("dead_date_final").cast(DateType()),\
        split(col("Date Rec"), "/").alias("date_rec"),\
        split(col("Time Rec"), " ").alias("time_rec").cast(ArrayType(IntegerType())),\
        col("Acc Sub Dist").alias("sub_district").cast(StringType()),\
        col("Acc Dist").alias("district").cast(StringType()),\
        col("`จ.ที่เสียชีวิต`").alias("province").cast(StringType()),\
        col("Acc La").alias("latitude").cast(DoubleType()),\
        col("Acclong").alias("longitude").cast(DoubleType()),\
        col("Ncause").alias("cause").cast(StringType()),\
        col("Vehicle Merge Final").alias("vehicle_merge_final").cast(StringType())
    ).withColumn("time_rec", col("time_rec").getItem(1))\
    .withColumn("date_rec", array(col("date_rec").getItem(0), col("date_rec").getItem(1), 
                                  col("date_rec").getItem(2)-543).cast(ArrayType(IntegerType())))\
    .withColumn("date_rec", to_date(concat_ws("/", col("date_rec")), "d/M/yyyy"))
    

    sparkDF2 = sparkDF.withColumn(
            "risk_helmet", when(col("risk_helmet") == 1, True)
            .when(col("risk_helmet") == 2, False)
            .otherwise(None)
        ).withColumn(
            "risk_helmet",
            col("risk_helmet").cast(BooleanType())
        ).withColumn(
            "risk_safetybelt", when(col("risk_safetybelt") == 1, True)
            .when(col("risk_safetybelt") == 2, False)
            .otherwise(None)
        ).withColumn(
            "risk_safetybelt",
            col("risk_safetybelt").cast(BooleanType())
        )
    
    # Nested columns sub_district, district, province, latitude, longitude
    sparkDF2 = sparkDF2.withColumn(
        "location",
        struct(
            col("sub_district"),
            col("district"),
            col("province"),
            col("latitude"),
            col("longitude")
        )
    ).drop(
        "sub_district", "district", "province", "latitude", "longitude"
    )

    sparkDF2 = sparkDF2.dropDuplicates()

    sparkDF2.printSchema()

    pd_df = sparkDF2.toPandas()
    spark.stop()
    return pd_df

def transform_pipeline(projectId, folder_source, folder_dest, source_blob_name):
    bucket_name=f"{projectId}-bucket"
    source_name=f"{folder_source}/{source_blob_name}.csv"
    dest_name = f"{folder_dest}/{source_blob_name}.parquet"

    df = download_from_gcs(
            bucket_name=bucket_name,
            source_blob_name=source_name
        )

    if df is not None:
        print(len(df), "rows downloaded from GCS.")
        result_df = spark_transform(df)
        result_df["location"] = result_df["location"].astype("str")
        df_byte = result_df.to_parquet(index=False, engine='pyarrow')
        upload_to_gcs(
            bucket_name=bucket_name,
            df=df_byte,
            destination_blob_name=dest_name,
            file_format='parquet'
        )
        
    else:
        print("No data to transform.")