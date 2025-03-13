from dagster import AssetExecutionContext,Output,asset,MonthlyPartitionsDefinition,AssetIn
import requests
import os
import pandas as pd
import findspark 
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf

from minio import Minio
from contextlib import contextmanager


@contextmanager
def initialize_spark() :
    spark = SparkSession.builder.appName("PySpark MinIO") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
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
    description="Cleanse and Transform Green Taxi Data",
    partitions_def=MonthlyPartitionsDefinition(start_date="2023-01-01",end_date="2024-01-01"),
    group_name="silver",
    key_prefix=["silver","green"],
    ins={
        'bronze_green_taxi':AssetIn(key_prefix=['bronze','green'])
    }
)
def silver_green_taxi(context : AssetExecutionContext, bronze_green_taxi : pd.DataFrame) : 
    partition_str = context.partition_key
    month_to_cleanse = partition_str[:-3]
    context.log.info(f"Cleanse and Transform {month_to_cleanse}.parquet from Bronze Layer ")
    client = Minio('localhost:9000',
        access_key="oiwyHMjuq2toBimR9SoZ",
        secret_key="FU90MZ5jviZnrp8o4mG4Y1gnYUhi7euMxgMUtsuP",
        secure=False)
    with initialize_spark() as spark : 
        df = spark.createDataFrame(bronze_green_taxi)
        selected_df = df.select('VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime','PULocationID'
                                ,'DOLocationID','RatecodeID','passenger_count', 'trip_distance'
                                ,'fare_amount','extra','mta_tax','tip_amount','tolls_amount'
                                ,'improvement_surcharge', 'total_amount','payment_type','congestion_surcharge')
        selected_df = selected_df.dropDuplicates()
        selected_df = selected_df.withColumn('lpep_pickup_datetime', sf.to_timestamp('lpep_pickup_datetime', 'yyyy-MM-dd HH:mm:ss'))
        selected_df = selected_df.withColumn('lpep_dropoff_datetime', sf.to_timestamp('lpep_dropoff_datetime', 'yyyy-MM-dd HH:mm:ss'))
        
        selected_df = selected_df.withColumnRenamed('lpep_pickup_datetime','pickup_datetime')
        selected_df = selected_df.withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime')
        selected_df = selected_df.dropna(subset=['pickup_datetime','dropoff_datetime'])
        selected_df = selected_df.withColumn('airport_fee',sf.lit(0.0))
        selected_df = selected_df.withColumn('taxi_type',sf.lit('Green'))
        selected_df = selected_df.withColumn('RatecodeID',sf.col('RatecodeID').cast(IntegerType()))
        selected_df = selected_df.withColumn('passenger_count',sf.col('passenger_count').cast(IntegerType()))
        selected_df = selected_df.withColumn('payment_type',sf.col('payment_type').cast(IntegerType()))
        selected_df = selected_df.withColumn('total_surcharges', sf.col('mta_tax')+ sf.col('extra') + sf.col('improvement_surcharge') + sf.col('congestion_surcharge'))
        selected_df = selected_df.drop('extra','mta_tax','improvement_surcharge','congestion_surcharge')
        selected_df = selected_df.withColumn('trip_duration',sf.col('dropoff_datetime').cast(LongType()) - sf.col('pickup_datetime').cast(LongType()))
        selected_df = selected_df.fillna(0)
        write_to_minio(selected_df,"lakehouse",client,f"silver/green_data/{month_to_cleanse}")
        context.log.info(f"Write {month_to_cleanse}.parquet to bucket lakehouse in Minio successfully")
        os.makedirs("data/staging/silver/green_data",exist_ok=True)
        selected_df.write.mode("overwrite").parquet(f'data/staging/silver/green_data/{month_to_cleanse}.parquet')
        context.log.info(f"Write {month_to_cleanse}.parquet to data/staging/silver/green_data")
        # pandas_df = selected_df.toPandas()
    
    pandas_df = pd.read_parquet(f"data/staging/silver/green_data/{month_to_cleanse}.parquet")
    return Output(
        value=pandas_df,
        metadata={
            'table': 'green_taxi',
            'Number of rows :': pandas_df.shape[0],
            'Number of columns :' : pandas_df.shape[1],
            'Columns' : pandas_df.columns.tolist()
        }
    )


@asset(
    description="Cleanse and Transform Yellow Taxi Data",
    partitions_def=MonthlyPartitionsDefinition(start_date="2023-01-01",end_date="2024-01-01"),
    group_name="silver",
    key_prefix=["silver","yellow"],
    ins={
        'bronze_yellow_taxi':AssetIn(key_prefix=['bronze','yellow'])
    }
)
def silver_yellow_taxi(context : AssetExecutionContext, bronze_yellow_taxi : pd.DataFrame) : 
    partition_str = context.partition_key
    month_to_cleanse = partition_str[:-3]
    context.log.info(f"Cleanse and Transform {month_to_cleanse}.parquet from Bronze Layer ")
    client = Minio('localhost:9000',
        access_key="oiwyHMjuq2toBimR9SoZ",
        secret_key="FU90MZ5jviZnrp8o4mG4Y1gnYUhi7euMxgMUtsuP",
        secure=False)
    with initialize_spark() as spark : 
        df = spark.createDataFrame(bronze_yellow_taxi)
        selected_df = df.select('VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime','PULocationID'
                                ,'DOLocationID','RatecodeID','passenger_count', 'trip_distance','fare_amount'
                                ,'extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge', 'total_amount'
                                ,'payment_type','congestion_surcharge','airport_fee')
        selected_df = selected_df.dropDuplicates()
        selected_df = selected_df.withColumn('tpep_pickup_datetime', sf.to_timestamp('tpep_pickup_datetime', 'yyyy-MM-dd HH:mm:ss'))
        selected_df = selected_df.withColumn('tpep_dropoff_datetime', sf.to_timestamp('tpep_dropoff_datetime', 'yyyy-MM-dd HH:mm:ss'))
        selected_df = selected_df.withColumnRenamed('tpep_pickup_datetime','pickup_datetime')
        selected_df = selected_df.withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')
        selected_df = selected_df.withColumn('taxi_type',sf.lit('Yellow'))
        selected_df = selected_df.withColumn('RatecodeID',sf.col('RatecodeID').cast(IntegerType()))
        selected_df = selected_df.withColumn('passenger_count',sf.col('passenger_count').cast(IntegerType()))
        selected_df = selected_df.withColumn('payment_type',sf.col('payment_type').cast(IntegerType()))

        selected_df = selected_df.withColumn('total_surcharges', sf.col('mta_tax')+ sf.col('extra') + sf.col('improvement_surcharge') + sf.col('congestion_surcharge'))
        selected_df = selected_df.drop('extra','mta_tax','improvement_surcharge','congestion_surcharge')
        write_to_minio(selected_df,"lakehouse",client,f"silver/yellow_data/{month_to_cleanse}")
        context.log.info(f"Write {month_to_cleanse}.parquet to bucket lakehouse in Minio successfully")
        os.makedirs("data/staging/silver/yellow_data",exist_ok=True)
        selected_df.write.mode("overwrite").parquet(f'data/staging/silver/yellow_data/{month_to_cleanse}.parquet')
        selected_df.unpersist()
        context.log.info(f"Write {month_to_cleanse}.parquet to data/staging/silver/yellow_data")
        # pandas_df = selected_df.toPandas()

    pandas_df = pd.read_parquet(f"data/staging/silver/yellow_data/{month_to_cleanse}.parquet")
    return Output(
        value=pandas_df,
        metadata={
            'table': 'yellow_taxi',
            'Number of rows :': pandas_df.shape[0],
            'Number of columns :' : pandas_df.shape[1],
            'Columns' : pandas_df.columns.tolist()
        }
    )