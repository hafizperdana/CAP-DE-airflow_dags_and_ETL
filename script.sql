CREATE DATABASE if not exists taxi_db location "/user/hive/warehouse/green_taxi.db";

USE taxi_db;

CREATE EXTERNAL TABLE IF NOT EXISTS green_taxi_trip_data (
        VendorID BIGINT,
        lpep_pickup_datetime BIGINT,
        lpep_dropoff_datetime BIGINT,
        store_and_fwd_flag STRING,
        RatecodeID DOUBLE,
        PULocationID BIGINT,
        DOLocationID BIGINT,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        ehail_fee DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        payment_type DOUBLE,
        trip_type DOUBLE,
        congestion_surcharge DOUBLE
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/pablo/data/taxi_data'
    TBLPROPERTIES ('skip.header.line.count'='1');