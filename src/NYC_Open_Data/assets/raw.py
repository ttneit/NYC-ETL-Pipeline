from dagster import AssetExecutionContext,Output,asset,MonthlyPartitionsDefinition
import requests
import os
import pandas as pd

@asset(
    description="Retrieve the yellow taxi data from the NYC Open Data portal.",
    group_name="raw_files",
    key_prefix=["raw","yellow"],
    # compute_fn="Python",
    partitions_def=MonthlyPartitionsDefinition(start_date="2023-01-01",end_date="2024-01-01")
)
def retrieve_yellow_files(context: AssetExecutionContext) : 
    partition_key = context.partition_key
    context.log.info(f"Fetching yellow taxi data from {partition_key}")
    month_to_fetch = partition_key[:-3]

    raw_yellow_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    os.makedirs("data/yellow_data",exist_ok=True)

    with open(f"data/yellow_data/{month_to_fetch}.parquet","wb") as file : 
        file.write(raw_yellow_trips.content)

    df = pd.read_parquet(f"data/yellow_data/{month_to_fetch}.parquet")
    num_rows = len(df)

    return Output(
        value=df,
        metadata={
            'Number of records' : num_rows,
        }
    )



@asset(
    description="Retrieve the green taxi data from the NYC Open Data portal.",
    group_name="raw_files",
    key_prefix=["raw","green"],
    # compute_fn="Python",
    partitions_def=MonthlyPartitionsDefinition(start_date="2023-01-01",end_date="2024-01-01")
)
def retrieve_green_files(context: AssetExecutionContext) : 
    partition_key = context.partition_key
    context.log.info(f"Fetching green taxi data from {partition_key}")
    month_to_fetch = partition_key[:-3]

    raw_green_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{month_to_fetch}.parquet"
    )

    os.makedirs("data/green_data",exist_ok=True)

    with open(f"data/green_data/{month_to_fetch}.parquet","wb") as file : 
        file.write(raw_green_trips.content)

    df = pd.read_parquet(f"data/green_data/{month_to_fetch}.parquet")
    num_rows = len(df)

    return Output(
        value=df,
        metadata={
            'Number of records' : num_rows,
        }
    )

@asset(
    description="Retrieve the taxi zone data from the NYC Open Data portal.",
    group_name="raw_files",
    key_prefix=["raw","taxi_zone"],
    # compute_fn="Python",
)
def retrieve_taxi_zone(context: AssetExecutionContext) : 
    raw_taxi_zones = requests.get(
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    )
    context.log.info(f"Fetching taxi zone data")
    os.makedirs("data", exist_ok=True)
    path = "data/taxi_zone.csv"
    with open(path, "wb") as file:
        file.write(raw_taxi_zones.content)
    num_rows = len(pd.read_csv(path))
    return Output(
        value=pd.read_csv(path),
        metadata={
            'Number of records': num_rows
        }
    )