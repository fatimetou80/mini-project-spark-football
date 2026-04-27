from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("MauritaniaFootballStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "mauritania_matches") \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType([
    StructField("season", StringType()),
    StructField("date", StringType()),
    StructField("home_team", StringType()),
    StructField("away_team", StringType()),
    StructField("home_goals", IntegerType()),
    StructField("away_goals", IntegerType())
])

json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

json_df = json_df.withColumn(
    "event_time",
    coalesce(
        to_timestamp("date", "yyyy-MM-dd HH:mm"),
        to_timestamp("date", "yyyy-MM-dd"),
        current_timestamp()
    )
)

result = json_df \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("home_team")
    ) \
    .agg(
        sum("home_goals").alias("total_goals"),
        count("*").alias("matches")
    )

query = result.writeStream \
    .format("parquet") \
    .option("path", "/workspace/data/outputs/streaming/football_stats") \
    .option("checkpointLocation", "/workspace/checkpoints/football_stats") \
    .outputMode("append") \
    .start()

query.awaitTermination()
