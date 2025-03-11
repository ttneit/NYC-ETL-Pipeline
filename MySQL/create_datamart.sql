CREATE DATABASE IF NOT EXISTS nyc_dw;

USE nyc_dw;

CREATE TABLE IF NOT EXISTS dim_vendor (
    VendorID INT PRIMARY KEY,
    VendorName VARCHAR(50)
);

INSERT INTO dim_vendor (VendorID,VendorName)
VALUES 
    (1,'Creative Mobile Technologies, LLC'),
    (2,'VeriFone Inc.') ;

CREATE TABLE IF NOT EXISTS dim_date_pu (
    dateID INT AUTO_INCREMENT PRIMARY KEY,
    date DATETIME,
    dateStr VARCHAR(50),
    day INT,
    month INT,
    quarter VARCHAR(5),
    year INT,
    dayOfMonth INT,
    dayOfWeek VARCHAR(20),
    weekOfYear INT,
    Weekday INT
);

CREATE TABLE IF NOT EXISTS dim_date_do (
    dateID INT AUTO_INCREMENT PRIMARY KEY,
    date DATETIME,
    dateStr VARCHAR(50),
    day INT,
    month INT,
    quarter VARCHAR(5),
    year INT,
    dayOfMonth INT,
    dayOfWeek VARCHAR(20),
    weekOfYear INT,
    Weekday INT
);

DELIMITER $$

CREATE PROCEDURE insert_date(IN start_date DATE, IN end_date DATE)
BEGIN
    DECLARE current_day DATE;
    DECLARE weekday_var INT;
    SET current_day = start_date;
    
    WHILE current_day <= end_date DO
        
        IF WEEKDAY(current_day) = 5 OR WEEKDAY(current_day) = 6 THEN  
            SET weekday_var = 1;  
        ELSE  
            SET weekday_var = 0;  
        END IF;

        INSERT INTO dim_date_pu(date, dateStr, day, month, quarter, year, dayOfMonth, dayOfWeek, weekOfYear, Weekday)
        VALUES (
            current_day,
            DATE_FORMAT(current_day, "%Y-%m-%d"),
            DAY(current_day),
            MONTH(current_day),
            CONCAT('Q', QUARTER(current_day)),
            YEAR(current_day),
            DAYOFMONTH(current_day),
            DAYNAME(current_day),
            WEEKOFYEAR(current_day),
            weekday_var
        );
        INSERT INTO dim_date_do(date, dateStr, day, month, quarter, year, dayOfMonth, dayOfWeek, weekOfYear, Weekday)
        VALUES (
            current_day,
            DATE_FORMAT(current_day, "%Y-%m-%d"),
            DAY(current_day),
            MONTH(current_day),
            CONCAT('Q', QUARTER(current_day)),
            YEAR(current_day),
            DAYOFMONTH(current_day),
            DAYNAME(current_day),
            WEEKOFYEAR(current_day),
            weekday_var
        );
        SET current_day = DATE_ADD(current_day, INTERVAL 1 DAY);
    END WHILE;
END $$

DELIMITER ;

CALL insert_date('2023-01-01', '2023-12-31');

CREATE TABLE IF NOT EXISTS dim_rate (
    RatecodeID INT PRIMARY KEY,
    RatecodeName VARCHAR(50)
);

INSERT INTO dim_rate (RatecodeID,RatecodeName)
VALUES 
    (1,'Standard rate'),
    (2,'JFK'),
    (3,'Newark'),
    (4,'Nassau or Westchester'),
    (5,'Negotiated fare'),
    (6,'Group ride'), 
    (99,'Null/unknown');



CREATE TABLE IF NOT EXISTS dim_payment (
    paymentID INT PRIMARY KEY,
    payment_type VARCHAR(50)
);

INSERT INTO dim_payment (paymentID,payment_type)
VALUES 
    (0,'Flex Fare trip'),
    (1,'Credit card'),
    (2,'Cash'),
    (3,'No charge'),
    (4,'Dispute'),
    (5,'Unknown') ,
    (6,'Voided trip');

CREATE TABLE IF NOT EXISTS dim_type (
    typeID INT PRIMARY KEY,
    typeName VARCHAR(50)
);

INSERT INTO dim_type (typeID,typeName)
VALUES 
    (1,'Green'),
    (2,'Yellow');
SET GLOBAL local_infile = 1;

CREATE TABLE IF NOT EXISTS dim_pu_location (
    LocationID INT PRIMARY KEY,
    Borough VARCHAR(50),
    Zone VARCHAR(50),
    service_zone VARCHAR(50)
);
LOAD DATA INFILE '/var/lib/mysql-files/taxi_zone.csv'
INTO TABLE dim_pu_location
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;


CREATE TABLE IF NOT EXISTS dim_do_location (
    LocationID INT PRIMARY KEY,
    borough VARCHAR(50),
    zone VARCHAR(50),
    service_zone VARCHAR(50)
);

LOAD DATA INFILE '/var/lib/mysql-files/taxi_zone.csv'
INTO TABLE dim_do_location
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

CREATE TABLE fact_nyc (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    PULocationID INT,
    DOLocationID INT,
    typeID INT,
    VendorID INT,
    date_puID INT,
    date_doID INT,
    RatecodeID INT,
    paymentID INT,
    passenger_count INT,
    trip_distance FLOAT,
    trip_duration FLOAT,
    fare_amount FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    total_amount FLOAT,
    airport_fee FLOAT,
    total_surcharges FLOAT,

    FOREIGN KEY (PULocationID) REFERENCES dim_pu_location(LocationID),
    FOREIGN KEY (DOLocationID) REFERENCES dim_do_location(LocationID),
    FOREIGN KEY (typeID) REFERENCES dim_type(typeID),
    FOREIGN KEY (paymentID) REFERENCES dim_payment(paymentID),
    FOREIGN KEY (VendorID) REFERENCES dim_vendor(VendorID),
    FOREIGN KEY (date_puID) REFERENCES dim_date_pu(dateID),
    FOREIGN KEY (date_doID) REFERENCES dim_date_do(dateID),
    FOREIGN KEY (RatecodeID) REFERENCES dim_rate(RatecodeID)
);