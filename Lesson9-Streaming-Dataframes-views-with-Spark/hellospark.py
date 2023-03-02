from pyspark.sql import SparkSession

# TO-DO: create a variable with the absolute path to the text file
# /home/workspace/Test.txt
log_file = "/home/workspace/Test.txt"

# TO-DO: create a Spark session
spark = SparkSession.builder.appName("my-first-spark-app").getOrCreate()

# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file 
log_data = spark.read.text(log_file).cache()

# TO-DO: call the appropriate function to filter the data containing
# the letter 'd', and then count the rows that were found
numDs = log_data.filter(log_data.value.contains('d')).count()

# TO-DO: call the appropriate function to filter the data containing
# the letter 's', and then count the rows that were found
numSs = log_data.filter(log_data.value.contains('s')).count()

# TO-DO: print the count for letter 'd' and letter 's'
print(f"There are {numDs} Ds and {numSs} Ss")

# TO-DO: stop the spark application
spark.stop()