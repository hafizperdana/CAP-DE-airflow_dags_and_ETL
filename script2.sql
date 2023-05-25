CREATE DATABASE if not exists taxi_dwh location "/user/hive/warehouse/green_taxi_dwh.db";

USE taxi_dwh;

CREATE EXTERNAL TABLE IF NOT EXISTS fact_green_taxi_trip_data (
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
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        payment_type DOUBLE,
        trip_type DOUBLE,
        congestion_surcharge DOUBLE
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/pablo/df_0'
    TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS dim_vendor_name (
        VendorID BIGINT,
        VendorName STRING
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/pablo/df_1'
    TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS dim_payment_method (
        payment_type BIGINT,
        PaymentMethod STRING
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/pablo/df_2'
    TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS dim_rate_desc (
        RateCodeID BIGINT,
        Rate_Desc STRING
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/pablo/df_3'
    TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS dim_trip_type (
        Trip_type BIGINT,
        Trip_Desc STRING
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/pablo/df_4'
    TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS dim_flag_desc (
        Store_and_fwd_flag STRING,
        Flag_Desc STRING
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/pablo/df_5'
    TBLPROPERTIES ('skip.header.line.count'='1');