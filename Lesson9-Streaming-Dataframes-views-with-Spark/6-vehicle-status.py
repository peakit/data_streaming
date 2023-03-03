from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType, ArrayType, DateType

# TO-DO: create a kafka message schema StructType including the following JSON elements:
# {"truckNumber":"5169","destination":"Florida","milesFromShop":505,"odomoterReading":50513}
vehicle_status_json_schema = StructType(
    [
        StructField("truckNumber", StringType()),
        StructField("destination", StringType()),
        StructField("milesFromShop", IntegerType()),
        StructField("odometerReading", IntegerType())
    ])

# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("vehicle-status-app").getOrCreate()

#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

#TO-DO: read the vehicle-status kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible 
vehicleStatusRawStreamingDF = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vehicle-status") \
    .option("startingOffsets", "earliest") \
    .load()

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
vehicleStatusSelectExprDF = vehicleStatusRawStreamingDF.selectExpr("CAST(key as string) as key", "CAST(value as string) as value")

#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 

# TO-DO: create a temporary streaming view called "VehicleStatus" 
# it can later be queried with spark.sql
vehicleStatusSelectExprDF.withColumn("value", from_json("value", vehicle_status_json_schema)).select(col('value.*')).createOrReplaceTempView("VehicleStatus")

#TO-DO: using spark.sql, select * from VehicleStatus
vehicleStatusFinalDF = spark.sql("SELECT * FROM VehicleStatus")

# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
# +-----------+------------+-------------+---------------+
# |truckNumber| destination|milesFromShop|odometerReading|
# +-----------+------------+-------------+---------------+
# |       9974|   Tennessee|          221|         335048|
# |       3575|      Canada|          354|          74000|
# |       1444|      Nevada|          257|         395616|
# |       5540|South Dakota|          856|         176293|
# +-----------+------------+-------------+---------------+
vehicleStatusFinalDF.writeStream.outputMode("append").format("console").start().awaitTermination()

