# NYC-ETL-Pipeline
## Introduction
### Goal of project
This project served as a hands-on learning experience in Data Engineering and Big Data. Utilizing PySpark, I constructed a Medallion Architecture ETL pipeline to process raw Parquet data and store it in MinIO. The resulting data was then structured into a Star Schema within MySQL, culminating in a single table in SQL Server, designed to power a Power BI dashboard. This project provided valuable practice in end-to-end data processing.

## Objective
Raw Data Sources is selected from  Public dataset `TLC Trip Record Data` :
- `Yellow Taxi (Yellow Medallion Taxicabs)` : These are the famous NYC yellow taxis that provide transportation exclusively through street hails. The number of taxicabs is limited by a finite number of medallions issued by the TLC. You access this mode of transportation by standing in the street and hailing an available taxi with your hand. The pickups are not pre-arranged. 
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
Raw Data Sources : 
- 
data
├── green
│   ├── 2023-01.parquet
│   ├── 2023-02.parquet
│   ├── ...
│   ├── 2023-11.parquet
│   ├── 2023-12.parquet
├── yellow
│   ├── 2023-01.parquet
│   ├── 2023-02.parquet
│   ├── ...
│   ├── 2023-11.parquet
│   ├── 2023-12.parquet
├── taxi_zone.csv

- Directory `green` and `yellow` include 12 parquet files for information about taxi trips in 12 months in 2023 
- File `taxi_zone.csv` contains information about all places in New York City.
### Datalake structure
- In minio :

lakehouse
├── bronze
│   └── green_data
│   │   ├── 2023-01.parquet
│   │   ├── 2023-02.parquet
│   │   ├── ...
│   │   ├── 2023-11.parquet
│   │   ├── 2023-12.parquet
├── silver
│   └── green_data
│   │   ├── 2023-01.parquet
│   │   ├── 2023-02.parquet
│   │   ├── ...
│   │   ├── 2023-11.parquet
│   │   ├── 2023-12.parquet

1. The datalake includes 2 sub layers : bronze, silver
2. All files are stored in parquet files for better storage than csv file
3. The data in gold layers is inserted into Data Mart in MySQL

### Data lineage
1. General 

![General](images\overview.png)

2. Raw layer

![Raw](images\raw.png)

- The target of this layer : download data from website and store in local
- This layer is optional (Can use directly from `data` directory)
3. Bronze Layer

![Bronze](images\bronze.png)

- `bronze_green_taxi` : Load parquet file from `green_taxi` directory to Object Storage
- `bronze_yellow_taxi` : Load parquet file from `yellow_taxi` directory to Object Storage
- Assets in this layer 12 partitions for better processing performance and reducing the running time

4. Silver Layer

![Silver](images\silver.png)

- Receive corresponding partition from Bronze layer then :
    - `silver_green_taxi` : Extract needed columns, deduplicate data,rename datetime column, fill nan values, create new columns for `taxi_type`, `trip_duration` 
    - `silver_yellow_taxi` : Extract needed columns, deduplicate data,rename datetime column, fill nan values, create new columns for `taxi_type`, `trip_duration`
- Assets in this layer 12 partitions for better processing performance and reducing the running time

5. Gold Layer

![Gold](images\gold.png)

- Receive corresponding partition from Silver layer then :
    - Data will be transformed into Star Schema before inserting into MySQL   
    - `extract_payment_information` : Extract information about the payment, filter the new data and assign the base value for new data before inserting into data_mart 
    - `extract_rate_information` : Extract information about the rate, filter the new data and assign the base value for new data before inserting into data_mart 
    - `extract_vendor_table` : Extract information about the vendor, filter the new data and assign the base value for new data before inserting into data_mart
    - `insert_fact_table` : Join with the updated Dimension table then insert into fact table in data_mart 
- Assets in this layer 12 partitions for better processing performance and reducing the running time

6. Platinum Layer

![Platinum](images\platinum.png)

- Receive corresponding partition from Gold layer then :
    - Data will be transformed into Star Schema before inserting into MySQL   
    - `weekly_report` : Extract needed information from dimension tables , joining with fact table to create a big table for analyzing (the date diemsion table : extract information about week)
    - `monthly_report` : Extract needed information from dimension tables , joining with fact table to create a big table for analyzing (the date diemsion table : extract information about month)


### Dashboard

![Weekly](images\dashboard.png)

#### Insights
- Payment type : Credit card is the most popular type of payment 
- Trip volume in Weekdays is more than the weekend show that green taxi is used for travelling for work more than going out 
- Monday has the lowest total trips compared to other weekdays but the average distance is by far higher than the other one show that Monday is usually for starting a business trip
- Despite the trend in trip volumne, the average tip amount show the reverse mode . The trip in the weekend is more likely to get tips from customers than in weekdays
- Because the use of green taxi is most for working, the hottest borough in NYC is Manhanttan due to a lot of working places : Wall Street, Midtown Manhattan with many companies : JP Morgan, Google, Facebook
## Setup
### Prequisites
- Set up Spark 3.4.2 , Haddop 3.3.4 and Java SE 8
- Install WSL2 and a local Ubuntu virtual machine 
- Install Docker Desktop
- Install PowerBi Desktop
- Install SQL Server Management Studio (SSMS) and Microsoft SQL Server Developer Edition
- Install Dagster
### Setup containers
- MySQL : 
1. Pull the latest MySQL Docker Image
```
docker pull mysql:latest
```
2. Run the MySQL Docker Container
```
docker run --name my-mysql -e MYSQL_ROOT_PASSWORD=1 -p 3306:3306 -d mysql:latest

```
3. Connect to MySQL 
```
mysql -u root -p
```
- Minio : 
1. Start Container 
```
docker run \
   -p 9000:9000 \
   -p 9001:9001 \
   --name minio1 \
   -v D:\minio\data:/data \
   -e "MINIO_ROOT_USER=ROOTUSER" \
   -e "MINIO_ROOT_PASSWORD=CHANGEME123" \
   quay.io/minio/minio server /data --console-address ":9001"
```
### Initialize data mart in MySQL
1. Copy file to docker container : my-mysql
```
docker cp NYC-ETL-Pipeline/data/taxi_zone.csv my-mysql:/var/lib/mysql-files/taxi_zone.csv
```
2. Execute the `create_datamart.sql` 
[DataMart](/NYC-ETL-Pipeline/Databases/create_datamart.sql)

### Initialize Report table in SQL Server
1. Execute the `create_report.sql` 
[DataMart](/NYC-ETL-Pipeline/Databases/create_report.sql)
## Considerations
- Spped : this is spark in standalone in Windows so the performance is not high and with the big data like `yellow_data` can be crashed at some later stages like platinum layer
- The environment : It's still a combination of Docker comtainer: Minio and MySQL and some services at local : Spark, PowerBI, SQL Server. In the future, selecting open-sources services and dockerizing all services in docker or cloud environment is better choice
## Further actions
- Complete the Dashboard
- Create unit tests to verify the output of each step in the ETL pipeline.
- Implement automatic updates when new data is added to the local directory.
