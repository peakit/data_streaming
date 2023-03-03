from pyspark.sql import SparkSession

# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("gear-position-app").getOrCreate()

#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

#TO-DO: read the gear-position kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                           
gearPositionRawStreamingDF = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "gear-position") \
    .option("startingOffsets", "earliest") \
    .load()

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
gearPositionStreamingDF = gearPositionRawStreamingDF.selectExpr("CAST(key as string) truckId", "CAST(value as string) gearPosition")

# TO-DO: create a temporary streaming view called "GearPosition" based on the streaming dataframe
gearPositionStreamingDF.createOrReplaceTempView("GearPosition")

# TO-DO: query the temporary view "GearPosition" using spark.sql 
gearPositionSelectStarDF = spark.sql("SELECT * FROM GearPosition")

# Write the dataframe from the last query to a kafka broker at localhost:9092, with a topic called gear-position-updates
gearPositionSelectStarDF.selectExpr("CAST(truckId as string) as key", "CAST(gearPosition as string) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "gear-position-updates") \
    .option("checkpointLocation", "/tmp/kafkacheckpoint") \
    .start() \
    .awaitTermination()