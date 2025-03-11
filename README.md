# NYC-ETL-Pipeline
## Introduction
### Goal of project
This project served as a hands-on learning experience in Data Engineering and Big Data. Utilizing PySpark, I constructed a Medallion Architecture ETL pipeline to process raw Parquet data and store it in MinIO. The resulting data was then structured into a Star Schema within MySQL, culminating in a single table in SQL Server, designed to power a Power BI dashboard. This project provided valuable practice in end-to-end data processing.

## Objective
Raw Data Sources is selected from  Public dataset `TLC Trip Record Data` :
<!-- - `Yellow Taxi (Yellow Medallion Taxicabs)` : These are the famous NYC yellow taxis that provide transportation exclusively through street hails. The number of taxicabs is limited by a finite number of medallions issued by the TLC. You access this mode of transportation by standing in the street and hailing an available taxi with your hand. The pickups are not pre-arranged.  -->
- `Green Taxi (Street Hail Livery)` : The SHL program will allow livery vehicle owners to license and outfit their vehicles with green borough taxi branding, meters, credit card machines, and ultimately the right to accept street hails in addition to pre-arranged rides.

## Design
### Directory Tree
- `data` : Contains data folder in raw data sources , and staging data in ETL phase
- `Databases` : Contains 2 SQL files to initialize dimensional schema in MySQL and SQL Server
- `src` : Contains file for ETL Pipeline
    - `NYC_Open_Data` : Contains all files needed for implementating ETL pipeline in Dagster
    - `NYC_Open_Data_tests` : Contain test files for unit testing
- `images` : Contains images which is necessary for `README.MD`
- `Dashboard.pbix` : A PowerBI file contains dashboard 
- `pyproject.toml` : A file includes a tool.dagster section which references the Python module with your Dagster definitions defined and discoverable at the top level
- `setup.py` : A build script with Python package dependencies for your new project as a package. Use this file to specify dependencies.
- `setup.cfg` : An ini file that contains option defaults for setup.py commands.
- `README.MD` : Reports 
### Pipeline Design
0. I use Docker to run some serviecs: Minio and MySQL and Dagster to orchestrate assets
1. NYC Open Data can be downloaded from [Green Taxi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) or taken after run `raw` assets in Dagster.
This data sources is stored in `data` directory for raw data sources
2. Extract the data from local data sources by PySpark and load into Minio for bronze layer
3. From Minio, Pyspark transforms data and loads into silver layer
4. From gold layer, data is transformed into star schema to insert into fact table and dimension tables in MySQL
5. In platinum layer, run some aggregation from data in gold layer 
6. Visualize data in PowerBI 
### Database schema

### Datalake structure

### Data lineage

### Dashboard

## Setup
### Prequisites
### Setup containers
### Import data into MySQL
## Considerations
## Further actions