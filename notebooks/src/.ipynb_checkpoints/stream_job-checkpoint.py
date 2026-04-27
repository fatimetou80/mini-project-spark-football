from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1. Spark Session
spark = SparkSession.builder \
    .appName("MauritaniaFootballStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "mauritania_matches") \
    .option("startingOffsets", "latest") \
    .load()

# 3. Schema JSON
schema = StructType([
    StructField("season", StringType()),
    StructField("date", StringType()),
    StructField("home_team", StringType()),
    StructField("away_team", StringType()),
    StructField("home_goals", IntegerType()),
    StructField("away_goals", IntegerType())
])

# 4. Parse JSON
json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5. Timestamp correction (IMPORTANT)
json_df = json_df.withColumn(
    "date",
    coalesce(
        to_timestamp("date", "yyyy-MM-dd HH:mm"),
        to_timestamp("date", "yyyy-MM-dd")
    )
)

# 6. Streaming logic (window + watermark FIX)
result = json_df \
    .withWatermark("date", "10 minutes") \
    .groupBy(
        window(col("date"), "10 minutes"),
        col("home_team")
    ) \
    .agg(
        sum("home_goals").alias("total_goals"),
        count("*").alias("matches")
    )

# 7. Write to Parquet (IMPORTANT: append only)
query = result.writeStream \
    .format("parquet") \
    .option("path", "/workspace/data/outputs/streaming/football_stats") \
    .option("checkpointLocation", "/workspace/checkpoints/football_stats") \
    .outputMode("append") \
    .start()

query.awaitTermination()
