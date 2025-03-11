from dagster import AssetExecutionContext,Output,asset,MonthlyPartitionsDefinition,AssetIn
import requests
import os
import pandas as pd
import findspark 
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.types as sf
from minio import Minio
from contextlib import contextmanager


@contextmanager
def initialize_spark() :
    spark = SparkSession.builder.appName("PySpark MinIO") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.cores.max", "1") \
        .config("spark.executor.cores", "1") \
        .config("fs.s3a.access.key","oiwyHMjuq2toBimR9SoZ") \
        .config("fs.s3a.secret.key","FU90MZ5jviZnrp8o4mG4Y1gnYUhi7euMxgMUtsuP") \
        .config("fs.s3a.endpoint","http://localhost:9000") \
        .config("fs.s3a.path.style.access","true") \
        .config("fs.s3a.connection.ssl.enabled","false") \
        .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


def write_to_minio(df,minio_bucket,client,table_name ) : 
    found = client.bucket_exists(minio_bucket)
    if not found : 
        client.make_bucket(minio_bucket)
        print("Created bucket", minio_bucket)
    else : print("Bucket", minio_bucket, "already exists")

    try : 
        df.write.mode("overwrite").parquet(f"s3a://{minio_bucket}/{table_name}.parquet")
        print(f"Write DataFrame successfully to bucket {minio_bucket} in Minio")
    except Exception as e : 
        print("Error:", str(e))

@asset(
    description="Ingest Green Taxi Data from local directory into Minio object storage",
    partitions_def=MonthlyPartitionsDefinition(start_date="2023-01-01",end_date="2024-01-01"),
    group_name="bronze",
    key_prefix=['bronze','green']
    # ins={
    #     'retrieve_green_files':AssetIn(key_prefix=['raw','green'])
    # }
)
def bronze_green_taxi(context : AssetExecutionContext) : 
    partition_str = context.partition_key
    context.log.info(f"Fetching green taxi file in {partition_str}")
    month_to_fetch = partition_str[:-3]

    

    client = Minio('localhost:9000',
        access_key="oiwyHMjuq2toBimR9SoZ",
        secret_key="FU90MZ5jviZnrp8o4mG4Y1gnYUhi7euMxgMUtsuP",
        secure=False)
    with initialize_spark() as spark : 
        df = spark.read.format('parquet').load(f'data/green_data/{month_to_fetch}.parquet')
        os.makedirs("data/staging/bronze/green_data",exist_ok=True)
        df.write.mode("overwrite").parquet(f'data/staging/bronze/green_data/{month_to_fetch}.parquet')
        context.log.info(f"Write {month_to_fetch}.parquet to data/staging/bronze/green_data")


        context.log.info(f"Writing {month_to_fetch}.parquet to bucket lakehouse in Minio")

        write_to_minio(df,"lakehouse",client,f"bronze/green_data/{month_to_fetch}")
        context.log.info(f"Write {month_to_fetch}.parquet to bucket lakehouse in Minio successfully")
        # pd_df = df.toPandas()



    pd_df = pd.read_parquet(f"data/staging/bronze/green_data/{month_to_fetch}.parquet")
    return Output(
        value=pd_df,
        metadata={
            'table': 'green_taxi',
            'Number of rows :': pd_df.shape[0],
            'Number of columns :' : pd_df.shape[1],
            'Columns' : pd_df.columns.tolist()
        }
    )

# @asset(
#     description="Ingest Yellow Taxi Data from local directory into Minio object storage",
#     partitions_def=MonthlyPartitionsDefinition(start_date="2023-01-01",end_date="2024-01-01"),
#     group_name="bronze",
#     key_prefix=['bronze','yellow']
#     # ins={
#     #     'retrieve_yellow_files':AssetIn(key_prefix=['raw','yellow'])
#     # }
# )
# def bronze_yellow_taxi(context : AssetExecutionContext) : 
#     partition_str = context.partition_key
#     context.log.info(f"Fetching yellow taxi file in {partition_str}")
#     month_to_fetch = partition_str[:-3]

    

#     client = Minio('localhost:9000',
#         access_key="oiwyHMjuq2toBimR9SoZ",
#         secret_key="FU90MZ5jviZnrp8o4mG4Y1gnYUhi7euMxgMUtsuP",
#         secure=False)
    
#     with initialize_spark() as spark : 
#         df = spark.read.format('parquet').load(f'data/yellow_data/{month_to_fetch}.parquet')
#         context.log.info(f"Writing {month_to_fetch}.parquet to bucket lakehouse in Minio")

#         write_to_minio(df,"lakehouse",client,f"bronze/yellow_data/{month_to_fetch}")
#         context.log.info(f"Write {month_to_fetch}.parquet to bucket lakehouse in Minio successfully")
#         os.makedirs("data/staging/bronze/yellow_data",exist_ok=True)
#         df.write.mode("overwrite").parquet(f'data/staging/bronze/yellow_data/{month_to_fetch}.parquet')
#         context.log.info(f"Write {month_to_fetch}.parquet to data/staging/bronze/yellow_data")
#         # pd_df = df.toPandas()

#     pd_df = pd.read_parquet(f"data/staging/bronze/yellow_data/{month_to_fetch}.parquet")
#     return Output(
#         value=pd_df,
#         metadata={
#             'table': 'yellow_taxi',
#             'Number of rows :': pd_df.shape[0],
#             'Number of columns :' : pd_df.shape[1],
#             'Columns' : pd_df.columns.tolist()
#         }
#     )
    
