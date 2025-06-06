from io import BytesIO
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, ArrayType, DateType, DoubleType, BooleanType, BinaryType
from pyspark.sql.functions import when, col, struct, split, to_date, array, concat_ws, date_format, regexp_replace, md5
import sys
import os
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def spark_transform(df, year):
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
        to_date(col("Date Rec"), "M/d/yyyy").alias("date_rec"),\
        split(col("Time Rec"), " ").alias("time_rec").cast(ArrayType(StringType())),\
        col("Acc Sub Dist").alias("sub_district").cast(StringType()),\
        col("Acc Dist").alias("district").cast(StringType()),\
        col("`จ.ที่เสียชีวิต`").alias("province").cast(StringType()),\
        col("Acc La").alias("latitude").cast(DoubleType()),\
        col("Acclong").alias("longitude").cast(DoubleType()),\
        col("Ncause").alias("cause").cast(StringType()),\
        col("Vehicle Merge Final").alias("vehicle_merge_final").cast(StringType())
    ).withColumn("time_rec", col("time_rec").getItem(1).cast(StringType()))\
    
    sparkDF2 = sparkDF.where((col("age") >= 0) | (col("age").isNull()))\
                    .where(col("dead_year") == int(year)-543)

    sparkDF2 = sparkDF2.withColumn(
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

    sparkDF2 = sparkDF2.withColumn(
        "dead_date_final",date_format(col("dead_date_final"), "yyyyMMdd").cast(IntegerType()))\
    .withColumn(
        "date_rec", date_format(col("date_rec"), "yyyyMMdd").cast(IntegerType()))\
    .withColumn(
        "date_rec", col("date_rec")-5430000)\
    .withColumn(
        "time_rec", regexp_replace(col("time_rec"), ":", "").cast(StringType()))\
    .withColumn(
        "dead_id", md5(col("dead_id").cast(BinaryType())))\

    sparkDF2 = sparkDF2.dropDuplicates()

    sparkDF2.printSchema()

    pd_df = sparkDF2.toPandas()
    spark.stop()
    return pd_df

def transform_pipeline(string_data, year):
    df = pd.read_csv(BytesIO(string_data))

    if df is not None:
        print(len(df), "rows downloaded from GCS.")
        result_df = spark_transform(df,year)
        result_df["date_rec"] = result_df["date_rec"].astype("Int64")
        result_df["risk_helmet"] = result_df["risk_helmet"].astype(pd.ArrowDtype(pa.bool_()))
        result_df["risk_safetybelt"] = result_df["risk_safetybelt"].astype(pd.ArrowDtype(pa.bool_()))
        result_df["date_rec"] = result_df["date_rec"].astype("Int64")
        result_df["time_rec"] = result_df["time_rec"].astype(pd.ArrowDtype(pa.string()))

        struct_type =pd.ArrowDtype(
            pa.struct(
                [
                    ("sub_district", pa.string()), 
                    ("district", pa.string()),
                    ("province", pa.string()),
                    ("latitude", pa.float64()),
                    ("longitude", pa.float64())
                ]
            )
        )
        result_df["location"] = result_df["location"].astype(struct_type)
    
        print(result_df.dtypes)

        df_byte = result_df.to_parquet(index=False, engine='pyarrow')
        print(f"Data {len(result_df)} rows transformed successfully.")

        return df_byte
    else:
        print("No data to transform.")
        return None