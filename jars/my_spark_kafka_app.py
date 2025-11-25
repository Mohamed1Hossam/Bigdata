from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder \
    .appName("KafkaSparkDebug") \
    .getOrCreate()

print("== Spark session started ==")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "100") \
    .load()

print("== Kafka readStream created ==")

df2 = df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")

query = df2.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "C:/tmp/spark-kafka-checkpoint") \
    .option("truncate", "false") \
    .trigger(processingTime="2 seconds") \
    .start()

print("== Streaming query started, awaiting termination ==")
query.awaitTermination()
print("== Query terminated (shouldn't be reached unless stopped) ==")