from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, window, collect_list,array
from pyspark.sql.types import StructType, StringType, TimestampType

# Define the schema for the JSON data type
schema = StructType() \
    .add("latitude", StringType()) \
    .add("longitude", StringType()) \
    .add("time_received", StringType()) \
    .add("vehicle_id", StringType()) \
    .add("distance_along_trip", StringType()) \
    .add("inferred_direction_id", StringType()) \
    .add("inferred_route_id", StringType()) \
    .add("inferred_trip_id", StringType()) \
    .add("next_scheduled_stop_distance", StringType()) \
    .add("next_scheduled_stop_id", StringType())

# Create a Spark session
spark = SparkSession.builder \
    .appName('StreamTest') \
    .master('local[2]') \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

# Set log level to ERROR
spark.sparkContext.setLogLevel("ERROR")

# Define Kafka broker and topic
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "trial"

# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load() \
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")

# Parse JSON data into columns
df = df.withColumn("data", from_json("value", schema)).select("key", "data.*")

# Ensure the 'time_received' column is of type Timestamp
df = df.withColumn("time_received", col("time_received").cast(TimestampType()))

# Filter out records where latitude and longitude are not null
filtered_df = df.filter(col("latitude").isNotNull() & col("longitude").isNotNull())

# Group data into 5-second windows and collect the positions as tuples
windowedCounts = filtered_df \
    .withWatermark("time_received", "5 seconds") \
    .groupBy(
        window(col("time_received"), "5 seconds")
    ) \
    .agg(
        collect_list(array(col("latitude"), col("longitude"))).alias("positions")
    ) \
    .select(to_json(struct(col("window"), col("positions"))).alias("value"))

# Define the absolute path for the checkpoint directory
checkpoint_directory = "/home/khalil/p2m/kafka/checkpoint"

# Clear the checkpoint directory before starting the query
import shutil
shutil.rmtree(checkpoint_directory, ignore_errors=True)

# Write the windowed output to a Kafka topic called "output_to_flask"
query = windowedCounts \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", "output_to_flask") \
    .option("checkpointLocation", checkpoint_directory) \
    .start()

# Await termination
query.awaitTermination()

# Stop Spark session
spark.stop()


# os.chdir("~/p2m")
# subprocess.Popen(["./spark-3.5.0/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "./spark_batcher.py"])
# ./spark-3.5.0/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ./spark_batcher.py




##########################################################################



# # Write the result to console
# query = df \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()


###############################################################################""




# # Create DataFrame representing the stream of input lines from connection to localhost:9999
# lines = spark \
#     .readStream \
#     .format("socket") \
#     .option("host", "localhost") \
#     .option("port", 9999) \
#     .load()

# # Split the lines into words
# words = lines.select(
#    explode(
#        split(lines.value, " ")
#    ).alias("word")
# )

# # Generate running word count
# wordCounts = words.groupBy("word").count()  

#  # Start running the query that prints the running counts to the console
# query = wordCounts \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()

# query.awaitTermination()

# #to run the spark server recepient
# #$ nc -lk 9999
# #the command above i sto launch the consumer 
# # ./spark-3.5.0/bin/spark-submit ~/p2m/test.py localhost 9999
# Subscribe to 1 topic
