from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.functions import when

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Remote Hive Metastore Connection") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.sql("select * from taxi_db.green_taxi_trip_data")

# Convert column lpep_pickup_datetime & lpep_dropoff_datetime into datetime format
df = df.withColumn("lpep_pickup_datetime", from_unixtime(col("lpep_pickup_datetime") / 1000000).cast("timestamp"))
df = df.withColumn("lpep_dropoff_datetime", from_unixtime(col("lpep_dropoff_datetime") / 1000000).cast("timestamp"))
df = df.dropna(subset=["Payment_type"])
df = df.filter(col("RateCodeID") != 99.0)
df = df.drop("ehail_fee")

# Making dimension1
df = df.withColumn("VendorName", when(col("VendorID") == 1, "Creative Mobile Technologies, LLC") \
                                 .when(col("VendorID") == 2, "VeriFone Inc") \
                                 .otherwise("Unknown"))

# Making dimension2
df = df.withColumn("PaymentMethod", when(col("Payment_type") == 1, "Credit card") \
                                        .when(col("Payment_type") == 2, "Cash") \
                                        .when(col("Payment_type") == 3, "No charge") \
                                        .when(col("Payment_type") == 4, "Dispute") \
                                        .otherwise("Unknown"))

# Making dimension3
df = df.withColumn("Rate_Desc", when(col("RateCodeID") == 1, "Standard rate") \
                                    .when(col("RateCodeID") == 2, "JFK") \
                                    .when(col("RateCodeID") == 3, "Newark") \
                                    .when(col("RateCodeID") == 4, "Nassau or Westchester") \
                                    .when(col("RateCodeID") == 5, "Negotiated fare") \
                                    .otherwise("Group ride"))

# Making dimension4
df = df.withColumn("Trip_Desc", when(col("Trip_type") == 1, "Street-hail") \
                                    .otherwise("Dispatch"))

# Making dimension5
df = df.withColumn("Flag_Desc", when(col("Store_and_fwd_flag") == "Y", "store and forward trip") \
                                    .otherwise("not a store and forward trip"))

# Seperate dim & fact
df_dim1 = df.selectExpr("VendorID", "VendorName").distinct().orderBy("VendorID")
df_dim2 = df.selectExpr("Payment_type", "PaymentMethod").distinct().orderBy("Payment_type")
df_dim3 = df.selectExpr("RateCodeID", "Rate_Desc").distinct().orderBy("RateCodeID")
df_dim4 = df.selectExpr("Trip_type","Trip_Desc").distinct().orderBy("Trip_type")
df_dim5 = df.selectExpr("Store_and_fwd_flag", "Flag_Desc").distinct()
df_fact = df.selectExpr("VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime", "Store_and_fwd_flag",
                        "RateCodeID", "PULocationID", "DOLocationID", "Passenger_count", "Trip_distance", "Fare_amount", "Extra",
                        "MTA_tax", "Tip_amount", "Tolls_amount", "Improvement_surcharge", "Total_amount", "Payment_type", "trip_type", "congestion_surcharge")

df_dim1.show()
df_dim2.show()
df_dim3.show()
df_dim4.show()
df_dim5.show()
df_fact.show()

df_list = [df_fact, df_dim1, df_dim2, df_dim3, df_dim4, df_dim5]

for i, data_frame in enumerate(df_list):
    file_name = f"df_{i}"  # Generate a unique file name for each DataFrame
    data_frame.coalesce(1).write.parquet(file_name, mode='overwrite')
    print(f"DataFrame {i} saved as {file_name}")