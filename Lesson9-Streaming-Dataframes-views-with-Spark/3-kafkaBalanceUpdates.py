from pyspark.sql import SparkSession

#TO-DO: create a Spark Session, and name the app something relevant
spark = SparkSession.builder.appName("kafka-console-app").getOrCreate()

#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

#TO-DO: read a stream from the kafka topic 'balance-updates', with the bootstrap server localhost:9092, reading from the earliest message
balanceRawStreamingDF = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'balance-updates') \
    .option('startingOffsets', 'earliest') \
    .load()


#TO-DO: cast the key and value columns as strings and select them using a select expression function
balanceStreamingDF = balanceRawStreamingDF.selectExpr("cast(key AS string) key, cast(value as string) value")

#TO-DO: write the dataframe to the console, and keep running indefinitely
balanceStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()