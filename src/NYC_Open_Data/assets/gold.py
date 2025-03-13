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
import datetime

@contextmanager
def initialize_spark() :
    spark = SparkSession.builder.appName("PySpark MinIO") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.cores.max", "4") \
        .config("spark.executor.cores", "4") \
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
        if not spark.streams.active:
            spark.stop()


def read_mysql_table(spark: SparkSession,table_name :str) :
    mysql_url = "jdbc:mysql://localhost:3306/nyc_dw"
    mysql_properties = {
        "user": "root",
        "password": "1",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df = spark.read.jdbc(url = mysql_url,table = table_name,properties=mysql_properties)
    return df

def write_mysql_table(df, table_name :str, mode: str) :
    mysql_url = "jdbc:mysql://localhost:3306/nyc_dw"
    mysql_properties = {
        "user": "root",
        "password": "1",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df.write.jdbc(url=mysql_url,table=table_name,properties=mysql_properties,mode=mode)
    print(f"Write {table_name} in MySQL successfully")

def get_latest_time_pickup_dw(spark) : 
    fact_df = read_mysql_table(spark,'fact_nyc')
    num_row = fact_df.select('date_puID').count()
    if num_row == 0 : 
        return datetime.datetime(2000,1,1,0,0,0)
    else : 
        dim_date_pu = read_mysql_table(spark,'dim_date_pu')
        full_df = fact_df.join(dim_date_pu,fact_df.date_puID == dim_date_pu.dateID,how='inner')
        latest_time = full_df.select('date').agg({'date':'max'}).collect()[0][0] 
        return latest_time

@asset(
    description="Inserting new data new data from Vendor Dimension Table in silver layer to Data Warehouse",
    partitions_def=MonthlyPartitionsDefinition(start_date="2023-01-01",end_date="2024-01-01"),
    group_name="gold",
    key_prefix=["gold","dim"],
    ins={
        'silver_green_taxi': AssetIn(key_prefix=['silver','green']),
        'silver_yellow_taxi' : AssetIn(key_prefix=['silver','yellow'])
    }
)
def extract_vendor_table(context : AssetExecutionContext, silver_green_taxi: pd.DataFrame, silver_yellow_taxi: pd.DataFrame) :
# def extract_vendor_table(context : AssetExecutionContext, silver_green_taxi: pd.DataFrame) :
    table_name = 'dim_vendor'
    mode = 'append'

    with initialize_spark() as spark : 
        context.log.info("Extracting the Vendor Information from silver layer")
        green_df = spark.createDataFrame(silver_green_taxi).select('VendorID').dropDuplicates()
        yellow_df = spark.createDataFrame(silver_yellow_taxi).select('VendorID').dropDuplicates()

        new_dim_vendor_df = yellow_df.union(green_df).dropDuplicates().sort('VendorID')

        # new_dim_vendor_df = green_df.dropDuplicates().sort('VendorID')
        context.log.info("Extracting the Vendor Information from Data Warehouse")
        previous_vendor_df = read_mysql_table(spark,table_name)
        new_dim_vendor_df = new_dim_vendor_df.withColumnRenamed('VendorID','VendorID_new')
        full_df = previous_vendor_df.join(new_dim_vendor_df,new_dim_vendor_df.VendorID_new == previous_vendor_df.VendorID,how='right')
        full_df = full_df.where(sf.col('VendorID').isNull())


        if full_df.count() == 0 :
            context.log.info(f"There is no new data for {table_name}")
        else :
            full_df = full_df.withColumn('VendorID',sf.coalesce(full_df['VendorID_new']))
            full_df = full_df.na.fill({'VendorName' : 'Unknown Vendor'})
            full_df = full_df.drop('VendorID_new')
            full_df = full_df.dropDuplicates()
            full_df = full_df.filter(full_df.VendorID != 0)
            context.log.info(f"Writing {table_name} to Data Warehouse")
            write_mysql_table(full_df,table_name,mode)
            context.log.info(f"Finishing updating {table_name} in Data Warehouse")

        # os.makedirs("data/staging/gold",exist_ok=True)
        # context.log.info(f"Writing {table_name}.parquet to data/staging/gold")
        # full_df.write.mode('overwrite').parquet(f'data/staging/gold/{table_name}.parquet')
        # context.log.info(f"Writing {table_name}.parquet to data/staging/gold successfully")
        dim_vendor_pandas = full_df.toPandas()


    return Output(
        value=dim_vendor_pandas,
        metadata={
            'table' : 'dim_vendor',
            'Number of rows' : dim_vendor_pandas.shape[0],
            'Columns' : dim_vendor_pandas.columns.tolist()
        }
    )


@asset(
    description="Inserting new data Payment information from green taxi and yellow taxi in silver layer",
    partitions_def=MonthlyPartitionsDefinition(start_date="2023-01-01",end_date="2024-01-01"),
    group_name='gold',
    key_prefix=['gold','dim'],
    ins={
        'silver_green_taxi' : AssetIn(key_prefix=['silver','green']),
        'silver_yellow_taxi' : AssetIn(key_prefix=['silver','yellow']),
    }
)
def extract_payment_information(context: AssetExecutionContext, silver_green_taxi : pd.DataFrame, silver_yellow_taxi: pd.DataFrame) :
# def extract_payment_information(context: AssetExecutionContext, silver_green_taxi : pd.DataFrame) :
    table_name = 'dim_payment'
    mode = 'append'


    with initialize_spark() as spark : 
        context.log.info("Extracting Payment information from silver layer")
        green_df = spark.createDataFrame(silver_green_taxi).select('payment_type').dropDuplicates()
        yellow_df = spark.createDataFrame(silver_yellow_taxi).select('payment_type').dropDuplicates()

        new_payment_df = green_df.union(yellow_df).dropDuplicates().sort('payment_type')
        # new_payment_df = green_df.dropDuplicates().sort('payment_type')
        context.log.info("Extracting Payment information from Data Warehouse")
        old_payment_df = read_mysql_table(spark,table_name)

        new_payment_df = new_payment_df.withColumnRenamed('payment_type', 'paymentID_new')
        full_df = old_payment_df.join(new_payment_df,new_payment_df.paymentID_new == old_payment_df.paymentID ,how='right')
        full_df = full_df.where(sf.col('paymentID').isNull())

        if full_df.count() == 0 : 
            context.log.info(f'There is no new data for Payment Dimension Table')
        else :
            full_df = full_df.withColumn('paymentID',sf.coalesce(full_df['paymentID_new']))
            full_df = full_df.na.fill({'payment_type':'Unknown Payment Method'})
            full_df = full_df.drop('paymentID_new').dropDuplicates()
            full_df = full_df.dropDuplicates()
            full_df = full_df.filter(full_df.paymentID != 0)
            context.log.info(f'Inserting new data into {table_name} in Data Warehouse')
            write_mysql_table(full_df,table_name,mode)
            context.log.info(f'Inserting new data into {table_name} in Data Warehouse successfully')


        # os.makedirs('data/staging/gold',exist_ok=True)
        # full_df.write.mode('overwrite').parquet(f'data/staging/gold/{table_name}.parquet')
        # context.log.info(f"Writing {table_name}.parquet to data/staging/gold successfully")
        pandas_df = full_df.toPandas()


    return Output(
        value=pandas_df,
        metadata={
            'table' :'dim_payment',
            'Number of rows' : pandas_df.shape[0],
            'Columns' : pandas_df.columns.tolist()
        }
    )


@asset(
    description='Inserting new data Rate information from green taxi and yellow taxi in silver layer to Data Warehouse',
    partitions_def=MonthlyPartitionsDefinition(start_date="2023-01-01",end_date="2024-01-01"),
    group_name='gold',
    key_prefix=['gold','dim'],
    ins={
        'silver_green_taxi': AssetIn(key_prefix=['silver','green']),
        'silver_yellow_taxi': AssetIn(key_prefix=['silver','yellow']),
    }
)
def extract_rate_information(context: AssetExecutionContext, silver_green_taxi: pd.DataFrame, silver_yellow_taxi: pd.DataFrame) :
# def extract_rate_information(context: AssetExecutionContext, silver_green_taxi: pd.DataFrame) :
    table_name = 'dim_rate'
    mode = 'append'

    with initialize_spark() as spark : 
        context.log.info(f'Extracting Rate Dimension Table from silver layer')
        green_df = spark.createDataFrame(silver_green_taxi).select('RatecodeID').dropDuplicates()
        yellow_df = spark.createDataFrame(silver_yellow_taxi).select('RatecodeID').dropDuplicates()

        new_rate_df = green_df.union(yellow_df).dropDuplicates().sort('RatecodeID')
        # new_rate_df = green_df.dropDuplicates().sort('RatecodeID')
        context.log.info('Extracting Rate information from Data Warehouse')
        previous_rate_df = read_mysql_table(spark,table_name)

        new_rate_df = new_rate_df.withColumnRenamed('RatecodeID' , 'RatecodeID_new')
        full_df = previous_rate_df.join(new_rate_df,new_rate_df.RatecodeID_new == previous_rate_df.RatecodeID,how='right')

        full_df = full_df.where(sf.col('RatecodeID').isNull())
        if full_df.count() == 0 : 
            context.log.info(f'There is no new data for Rate Dimension table')
        else :
            full_df = full_df.withColumn('RatecodeID',sf.coalesce(full_df['RatecodeID_new']))
            full_df = full_df.na.fill({'RatecodeName' : 'Unknown Ratecode'})
            full_df = full_df.drop('RatecodeID_new')
            full_df = full_df.filter(full_df.RatecodeID != 0)
            context.log.info(f"Inserting into {table_name} in Data Warehouse")
            write_mysql_table(full_df,table_name,mode)
            context.log.info(f"Inserting into {table_name} in Data Warehouse successfully")
        
        # os.makedirs('data/staging/gold',exist_ok=True)
        # full_df.write.mode('overwrite').parquet(f'data/staging/gold/{table_name}.parquet')
        # context.log.info(f"Writing {table_name}.parquet to data/staging/gold successfully")
        pandas_df = full_df.toPandas()



    return Output(
        value=pandas_df,
        metadata={
            'table' : 'dim_rate',
            'Number of rows' : pandas_df.shape[0],
            'Columns' : pandas_df.columns.tolist()
        }
    )
    


@asset(
    description="Inserting new data for Fact table in Data Warehouse",
    partitions_def=MonthlyPartitionsDefinition(start_date="2023-01-01",end_date="2024-01-01"),
    group_name='gold',
    key_prefix=['gold','fact'],
    ins={
        'extract_rate_information': AssetIn(key_prefix=['gold','dim']),
        'extract_payment_information': AssetIn(key_prefix=['gold','dim']),
        'extract_vendor_table': AssetIn(key_prefix=['gold','dim']),
        'silver_green_taxi': AssetIn(key_prefix=['silver','green']),
        'silver_yellow_taxi': AssetIn(key_prefix=['silver','yellow']),
    }
)
def insert_fact_table(
    context: AssetExecutionContext,
    extract_rate_information: pd.DataFrame,
    extract_payment_information: pd.DataFrame,
    extract_vendor_table: pd.DataFrame,
    silver_green_taxi: pd.DataFrame,
    silver_yellow_taxi: pd.DataFrame
):
    table_name = 'fact_nyc'
    mode = 'append'
    with initialize_spark() as spark : 
        if spark is None:
            raise RuntimeError("SparkSession initialization failed!")
        context.log.info("Extracting data from silver layer")
        green_df = spark.createDataFrame(silver_green_taxi)
        yellow_df = spark.createDataFrame(silver_yellow_taxi)
        
        full_df = green_df.union(yellow_df)
        # full_df = green_df
        latest_time = get_latest_time_pickup_dw(spark)
        print(f"latest_time: {latest_time}, Type: {type(latest_time)}")

        full_df = full_df.filter(sf.col('pickup_datetime') > latest_time)

        full_df = full_df.withColumn('year', sf.year('pickup_datetime'))
        full_df = full_df.withColumn('month', sf.month('pickup_datetime'))
        full_df = full_df.withColumn('day', sf.dayofmonth('pickup_datetime'))

        context.log.info("Extracting dimension data from silver layer")
        dim_date_pu = read_mysql_table(spark,'dim_date_pu')
        dim_date_do = read_mysql_table(spark,'dim_date_do')


        join_df = full_df.join(
            dim_date_pu,
            (full_df.day == dim_date_pu.day) &
            (full_df.month == dim_date_pu.month) &
            (full_df.year == dim_date_pu.year),
            how="inner"
        )
        join_df = join_df.drop('year','month','day','date','dateStr','quarter','year','dayOfMonth','dayOfWeek','Weekday')
        join_df = join_df.withColumnRenamed('dateID','date_puID')


        join_df = join_df.withColumn('year', sf.year('dropoff_datetime'))
        join_df = join_df.withColumn('month', sf.month('dropoff_datetime'))
        join_df = join_df.withColumn('day', sf.dayofmonth('dropoff_datetime'))
        join_df = join_df.join(
            dim_date_do,
            (join_df.day == dim_date_do.day) &
            (join_df.month == dim_date_do.month) &
            (join_df.year == dim_date_do.year),
            how="inner"
        )
        join_df = join_df.drop('year','month','day','date','dateStr','quarter','year','dayOfMonth','dayOfWeek','Weekday')
        join_df = join_df.withColumnRenamed('dateID','date_doID')
        join_df = join_df.withColumnRenamed('payment_type','paymentID')


        dim_type = read_mysql_table(spark,'dim_type')
        
        join_df  = join_df.join(dim_type,join_df.taxi_type == dim_type.typeName,how='inner')
        join_df = join_df.drop('taxi_type','typeName')
        column_order = [
            "PULocationID", "DOLocationID", "typeID", "VendorID",
            "date_puID", "date_doID", "RatecodeID", "paymentID",
            "passenger_count", "trip_distance","trip_duration", "fare_amount",
            "tip_amount", "tolls_amount", "total_amount",
            "airport_fee", "total_surcharges"
        ]

        join_df = join_df.select(column_order)
        join_df = join_df.fillna(0)
        join_df.show()
        write_mysql_table(join_df,table_name,mode)

        # os.makedirs('data/staging/gold',exist_ok=True)
        # join_df.write.mode('overwrite').parquet(f'data/staging/gold/{table_name}.parquet')
        # context.log.info(f"Writing {table_name}.parquet to data/staging/gold successfully")
        

        pandas_df = join_df.toPandas()


    return Output(
        value=pandas_df,
        metadata={
            'table' : 'insert_fact_table',
            'Number of rows' : pandas_df.shape[0],
            'Columns' : pandas_df.columns.tolist()
        }
    )
        