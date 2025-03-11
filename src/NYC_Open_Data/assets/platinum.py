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
        .config("spark.cores.max", "1") \
        .config("spark.executor.cores", "2") \
        .config("fs.s3a.access.key","oiwyHMjuq2toBimR9SoZ") \
        .config("fs.s3a.secret.key","FU90MZ5jviZnrp8o4mG4Y1gnYUhi7euMxgMUtsuP") \
        .config("fs.s3a.endpoint","http://localhost:9000") \
        .config("fs.s3a.path.style.access","true") \
        .config("fs.s3a.connection.ssl.enabled","false") \
        .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    yield spark

def write_to_SQLServer(df,table_name :str, mode :str) : 
    url = "jdbc:sqlserver://DESKTOP-FVCQ1TK\TIEN:1433;databaseName=nyc_datamart"
    properties = {
        "user": "sa",
        "password": "tien",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)
    print(f"Write {table_name} in SQL Server successfully")

def read_mysql_table(spark: SparkSession,table_name :str, database : str) :
    mysql_url = f"jdbc:mysql://localhost:3306/{database}"
    mysql_properties = {
        "user": "root",
        "password": "1",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df = spark.read.jdbc(url = mysql_url,table = table_name,properties=mysql_properties)
    return df

def write_mysql_table(df, table_name :str, mode: str, database : str) :
    mysql_url = f"jdbc:mysql://localhost:3306/{database}"
    mysql_properties = {
        "user": "root",
        "password": "1",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df.write.jdbc(url=mysql_url,table=table_name,properties=mysql_properties,mode=mode)
    print(f"Write {table_name} in MySQL successfully")

@asset(
    description="Create Monthly Report Data Mart",
    group_name='platinum',
    key_prefix=['platinum','report'],
    ins={
        'insert_fact_table' : AssetIn(key_prefix=["gold","fact"])
    }
)
def monthly_report(context: AssetExecutionContext, insert_fact_table) : 

    with initialize_spark() as spark : 
        context.log.info("Process fact table")
        fact_table = read_mysql_table(spark,"fact_nyc",'nyc_dw')
        context.log.info("Aggregate fact table for monthly report")
        
        context.log.info("Process dim date tables")
        dim_date_pu = read_mysql_table(spark,'dim_date_pu','nyc_dw').select('dateID','month')
        dim_date_pu = dim_date_pu.withColumnRenamed('dateID','date_puID')
        dim_date_pu = dim_date_pu.withColumnRenamed('month','month_pu')

        # dim_date_do = read_mysql_table(spark,'dim_date_do','nyc_dw').select('dateID','month')
        # dim_date_do = dim_date_do.withColumnRenamed('dateID','date_doID')
        # dim_date_do = dim_date_do.withColumnRenamed('month','month_do')
        full_df = fact_table.join(dim_date_pu,on='date_puID',how='inner')
        # full_df = full_df.join(dim_date_do,on='date_doID',how='inner')
        # full_df = full_df.drop('date_puID','date_doID')
        full_df = full_df.drop('date_puID')
        monthly_report = full_df.groupBy(
            'PULocationID',
            'DOLocationID',
            'typeID',
            'VendorID',
            'month_pu',
            # 'month_do',
            'RatecodeID',
            'paymentID').agg(
                sf.round(sf.mean('passenger_count'), 3).alias('avg_passenger_count'),
                sf.round(sf.sum('passenger_count'), 3).alias('total_passenger_count'),
                sf.round(sf.mean('trip_distance'), 3).alias('avg_trip_distance'),
                sf.round(sf.sum('trip_distance'), 3).alias('total_trip_distance'),
                sf.round(sf.mean('trip_duration') / (1000*60), 3).alias('avg_trip_duration'),
                sf.round(sf.sum('trip_duration') / (1000*60), 3).alias('total_trip_duration'),
                sf.round(sf.mean('tip_amount'), 3).alias('avg_tip_amount'),
                sf.round(sf.sum('tip_amount'), 3).alias('total_tip_amount'),
                sf.round(sf.mean('tolls_amount'), 3).alias('avg_tolls_amount'),
                sf.round(sf.sum('tolls_amount'), 3).alias('total_tolls_amount'),
                sf.round(sf.mean('total_amount'), 3).alias('avg_total_amount'),
                sf.round(sf.sum('total_amount'), 3).alias('total_total_amount'),
                sf.round(sf.mean('fare_amount'), 3).alias('avg_fare_amount'),
                sf.round(sf.sum('fare_amount'), 3).alias('total_fare_amount'),
                sf.count('ID').alias('total_trips')
            )
        context.log.info("Process dim location tables")
        pu_zone = read_mysql_table(spark,'dim_pu_location','nyc_dw')
        pu_zone = pu_zone.withColumnRenamed('LocationID','PULocationID')
        pu_zone = pu_zone.withColumnRenamed('Borough','PU_Borough')
        pu_zone = pu_zone.withColumnRenamed('Zone','PU_Zone')
        pu_zone = pu_zone.withColumnRenamed('service_zone','PU_service_zone')

        do_zone = read_mysql_table(spark,'dim_do_location','nyc_dw')
        do_zone = do_zone.withColumnRenamed('LocationID','DOLocationID')
        do_zone = do_zone.withColumnRenamed('Borough','DO_Borough')
        do_zone = do_zone.withColumnRenamed('Zone','DO_Zone')
        do_zone = do_zone.withColumnRenamed('service_zone','DO_service_zone')

        context.log.info("Process other dim tables")
        dim_type =read_mysql_table(spark,'dim_type','nyc_dw')
        dim_vendor = read_mysql_table(spark,'dim_vendor','nyc_dw')
        dim_payment = read_mysql_table(spark,'dim_payment','nyc_dw')
        dim_rate = read_mysql_table(spark,'dim_rate','nyc_dw')
        
        monthly_report = monthly_report.join(pu_zone,on='PULocationID',how='inner')
        monthly_report = monthly_report.join(do_zone,on='DOLocationID',how='inner')
        monthly_report = monthly_report.drop('PULocationID','DOLocationID')
        monthly_report = monthly_report.join(dim_type,on='typeID',how='inner')
        monthly_report = monthly_report.drop('typeID')
        monthly_report = monthly_report.join(dim_vendor,on='VendorID',how='inner')
        monthly_report = monthly_report.drop('VendorID')
        monthly_report = monthly_report.join(dim_payment,on='paymentID',how='inner')
        monthly_report = monthly_report.drop('paymentID')
        monthly_report = monthly_report.join(dim_rate,on='RatecodeID',how='inner')
        monthly_report = monthly_report.drop('RatecodeID')
        # write_mysql_table(monthly_report,'monthly_report','append','nyc_datamart')
        write_to_SQLServer(monthly_report,'monthly_report','append')
        monthly_report_pd = monthly_report.toPandas()
        spark.stop()

    return Output(
        value= monthly_report_pd,
        metadata={
            'table' : 'Monthly Report',
            'Number of rows :' : monthly_report_pd.shape[0]
        }
    )
    


@asset(
    description="Create Weekly Report Data Mart",
    group_name='platinum',
    key_prefix=['platinum','report'],
    ins={
        'insert_fact_table' : AssetIn(key_prefix=["gold","fact"])
    }
)
def weekly_report(context: AssetExecutionContext, insert_fact_table) : 

    with initialize_spark() as spark : 
        context.log.info("Process fact table")
        fact_table = read_mysql_table(spark,"fact_nyc",'nyc_dw')
        context.log.info("Aggregate fact table for monthly report")
        
        context.log.info("Process dim date tables")
        dim_date_pu = read_mysql_table(spark,'dim_date_pu','nyc_dw').select('dateID','dayOfWeek','weekOfYear')
        dim_date_pu = dim_date_pu.withColumnRenamed('dateID','date_puID')
        dim_date_pu = dim_date_pu.withColumnRenamed('dayOfWeek','dayOfWeek_pu')
        dim_date_pu = dim_date_pu.withColumnRenamed('weekOfYear','weekOfYear_pu')

        # dim_date_do = read_mysql_table(spark,'dim_date_do','nyc_dw').select('dateID','dayOfWeek')
        # dim_date_do = dim_date_do.withColumnRenamed('dateID','date_doID')
        # dim_date_do = dim_date_do.withColumnRenamed('dayOfWeek','dayOfWeek_do')
        full_df = fact_table.join(dim_date_pu,on='date_puID',how='inner')
        # full_df = full_df.join(dim_date_do,on='date_doID',how='inner')
        # full_df = full_df.drop('date_puID','date_doID')
        full_df = full_df.drop('date_puID')
        weekly_report = full_df.groupBy(
            'PULocationID',
            'DOLocationID',
            'typeID',
            'VendorID',
            'dayOfWeek_pu',
            'weekOfYear_pu',
            'RatecodeID',
            'paymentID').agg(
                sf.round(sf.mean('passenger_count'), 3).alias('avg_passenger_count'),
                sf.round(sf.sum('passenger_count'), 3).alias('total_passenger_count'),
                sf.round(sf.mean('trip_distance'), 3).alias('avg_trip_distance'),
                sf.round(sf.sum('trip_distance'), 3).alias('total_trip_distance'),
                sf.round(sf.mean('trip_duration') / (1000*60), 3).alias('avg_trip_duration'),
                sf.round(sf.sum('trip_duration') / (1000*60), 3).alias('total_trip_duration'),
                sf.round(sf.mean('tip_amount'), 3).alias('avg_tip_amount'),
                sf.round(sf.sum('tip_amount'), 3).alias('total_tip_amount'),
                sf.round(sf.mean('tolls_amount'), 3).alias('avg_tolls_amount'),
                sf.round(sf.sum('tolls_amount'), 3).alias('total_tolls_amount'),
                sf.round(sf.mean('total_amount'), 3).alias('avg_total_amount'),
                sf.round(sf.sum('total_amount'), 3).alias('total_total_amount'),
                sf.round(sf.mean('fare_amount'), 3).alias('avg_fare_amount'),
                sf.round(sf.sum('fare_amount'), 3).alias('total_fare_amount'),
                sf.count('ID').alias('total_trips')
            )
        context.log.info("Process dim location tables")
        pu_zone = read_mysql_table(spark,'dim_pu_location','nyc_dw')
        pu_zone = pu_zone.withColumnRenamed('LocationID','PULocationID')
        pu_zone = pu_zone.withColumnRenamed('Borough','PU_Borough')
        pu_zone = pu_zone.withColumnRenamed('Zone','PU_Zone')
        pu_zone = pu_zone.withColumnRenamed('service_zone','PU_service_zone')

        do_zone = read_mysql_table(spark,'dim_do_location','nyc_dw')
        do_zone = do_zone.withColumnRenamed('LocationID','DOLocationID')
        do_zone = do_zone.withColumnRenamed('Borough','DO_Borough')
        do_zone = do_zone.withColumnRenamed('Zone','DO_Zone')
        do_zone = do_zone.withColumnRenamed('service_zone','DO_service_zone')

        context.log.info("Process other dim tables")
        dim_type =read_mysql_table(spark,'dim_type','nyc_dw')
        dim_vendor = read_mysql_table(spark,'dim_vendor','nyc_dw')
        dim_payment = read_mysql_table(spark,'dim_payment','nyc_dw')
        dim_rate = read_mysql_table(spark,'dim_rate','nyc_dw')
        
        weekly_report = weekly_report.join(pu_zone,on='PULocationID',how='inner')
        weekly_report = weekly_report.join(do_zone,on='DOLocationID',how='inner')
        weekly_report = weekly_report.drop('PULocationID','DOLocationID')
        weekly_report = weekly_report.join(dim_type,on='typeID',how='inner')
        weekly_report = weekly_report.drop('typeID')
        weekly_report = weekly_report.join(dim_vendor,on='VendorID',how='inner')
        weekly_report = weekly_report.drop('VendorID')
        weekly_report = weekly_report.join(dim_payment,on='paymentID',how='inner')
        weekly_report = weekly_report.drop('paymentID')
        weekly_report = weekly_report.join(dim_rate,on='RatecodeID',how='inner')
        weekly_report = weekly_report.drop('RatecodeID')
        # write_mysql_table(weekly_report,'weekly_report','append','nyc_datamart')
        write_to_SQLServer(weekly_report,'weekly_report','append')
        weekly_report_pd = weekly_report.toPandas()
        spark.stop()

    return Output(
        value= weekly_report_pd,
        metadata={
            'table' : 'Monthly Report',
            'Number of rows :' : weekly_report_pd.shape[0]
        }
    )
