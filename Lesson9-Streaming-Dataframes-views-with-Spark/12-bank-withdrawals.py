from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, BooleanType, ArrayType, DateType

# TO-DO: create bank withdrawals kafka message schema StructType including the following JSON elements:
#  {"accountNumber":"703934969","amount":625.8,"dateAndTime":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682}
bank_withdrawals_schema = StructType([
    StructField("accountNumber", StringType()),
    StructField("amount", StringType()),
    StructField("dateAndTime", StringType()),
    StructField("transactionId", StringType())
])


# TO-DO: create an atm withdrawals kafka message schema StructType including the following JSON elements:
# {"transactionDate":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682,"atmLocation":"Thailand"}
atm_withdrawals_schema = StructType([
    StructField("transactionDate", StringType()),
    StructField("transactionId", StringType()),
    StructField("atmLocation", StringType())
])


# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("bank-withdrawals-app").getOrCreate()

#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

#TO-DO: read the bank-withdrawals kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
bankWithdrawalsRawDF = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank-withdrawals") \
    .option("startingOffsets", "earliest") \
    .load()

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
bankWithdrawalsDF = bankWithdrawalsRawDF.selectExpr("CAST(key as string) as key", "CAST(value as string) as value")

#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
bankWithdrawalsDF.withColumn("value", from_json("value", bank_withdrawals_schema)).select(col('value.*')).createOrReplaceTempView("BankWithdrawals")

# TO-DO: create a temporary streaming view called "BankWithdrawals" 
# it can later be queried with spark.sql

#TO-DO: using spark.sql, select * from BankWithdrawals into a dataframe
bankWithdrawalsFinalDF = spark.sql("SELECT * FROM BankWithdrawals")

#TO-DO: read the atm-withdrawals kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible
atmWithdrawalsRawDF = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "atm-withdrawals") \
    .option("startingOffsets", "earliest") \
    .load()

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
atmWithdrawalsDF = atmWithdrawalsRawDF.selectExpr("CAST(key as string) as key", "CAST(value as string) as value")

#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
atmWithdrawalsDF.withColumn("value", from_json("value", atm_withdrawals_schema)).select(col('value.*')).createOrReplaceTempView("AtmWithdrawals")

# TO-DO: create a temporary streaming view called "AtmWithdrawals" 
# it can later be queried with spark.sql

#TO-DO: using spark.sql, select * from AtmWithdrawals into a dataframe
atmWithdrawalsFinalDF = spark.sql("SELECT transactionId as atmTransactionId, atmLocation, transactionDate FROM AtmWithdrawals")

#TO-DO: join the atm withdrawals dataframe with the bank withdrawals dataframe
bankAtmWithdrawalsDF = bankWithdrawalsFinalDF.join(atmWithdrawalsFinalDF, expr("""
    transactionId = atmTransactionId
"""))

# TO-DO: write the stream to the kafka in a topic called withdrawals-location, and configure it to run indefinitely, the console will not output anything. You will want to attach to the topic using the kafka-console-consumer inside another terminal, it will look something like this:

# {"accountNumber":"862939503","amount":"844.8","dateAndTime":"Oct 7, 2020 12:33:34 AM","transactionId":"1602030814320","transactionDate":"Oct 7, 2020 12:33:34 AM","atmTransactionId":"1602030814320","atmLocation":"Ukraine"}
bankAtmWithdrawalsDF.selectExpr("CAST(transactionId as string) as key", "to_json(struct(*)) as value") \
    .writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "withdrawals-location") \
    .option("checkpointLocation", "/tmp/kafkacheckpointloc") \
    .start().awaitTermination()
