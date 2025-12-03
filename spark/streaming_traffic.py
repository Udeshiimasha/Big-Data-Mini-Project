# spark/streaming_traffic.py
"""
Spark Structured Streaming job:

- Read traffic-events from Kafka
- Parse JSON
- Compute 5-minute tumbling windows: total vehicle_count, avg_speed (weighted)
- Compute "Congestion Index" (example formula)
- If avg_speed < 10 km/h, write immediate alert to `critical-traffic` topic and to Postgres
- Write window aggregates to Postgres (or Parquet) for nightly batch

Run with: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.0 streaming_traffic.py
(Adjust package version to match your Spark)
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, expr, to_timestamp, avg, sum as _sum, struct
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType

KAFKA_BOOTSTRAP = "localhost:9093"  # External listener for local Spark
INPUT_TOPIC = "traffic-events"
ALERT_TOPIC = "critical-traffic"
POSTGRES_URL = "jdbc:postgresql://localhost:5432/trafficdb"
POSTGRES_USER = "postgres"
POSTGRES_PASS = "postgres"

schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("vehicle_count", IntegerType()) \
    .add("avg_speed", DoubleType())

spark = SparkSession.builder \
    .appName("TrafficStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# value is bytes -> string
json_df = df.selectExpr("CAST(value AS STRING) as json_str")

parsed = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")
# convert timestamp string to timestamp type
parsed = parsed.withColumn("event_time", to_timestamp(col("timestamp")))

# Add watermark for late events (2 minutes tolerance)
parsed_with_watermark = parsed.withWatermark("event_time", "2 minutes")

# Windowed aggregation: 5-minute tumbling windows
windowed = parsed_with_watermark.groupBy(
    col("sensor_id"),
    window(col("event_time"), "5 minutes")
).agg(
    _sum("vehicle_count").alias("sum_vehicle_count"),
    avg("avg_speed").alias("avg_speed")
)

# compute a simple Congestion Index: CI = max(0, (50 - avg_speed) * (sum_vehicle_count / 100))
windowed = windowed.withColumn(
    "congestion_index",
    expr("GREATEST(0, (50 - avg_speed) * (sum_vehicle_count / 100.0))")
)

# Write aggregates to Postgres (append)
def write_aggregates_to_postgres(df, epoch_id):
    df_to_write = df.select(
        col("sensor_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sum_vehicle_count"),
        col("avg_speed"),
        col("congestion_index")
    )
    try:
        (df_to_write
            .write
            .format("jdbc")
            .mode("append")
            .option("url", POSTGRES_URL)
            .option("dbtable", "traffic_window_agg")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASS)
            .save()
        )
        print(f"Written {df_to_write.count()} window aggregates to Postgres (epoch {epoch_id})")
    except Exception as e:
        print(f"Error writing aggregates: {e}")

# Alerts stream: when avg_speed < 10 -> write to Kafka alert topic and Postgres alerts table
alerts = parsed.filter(col("avg_speed") < 10).select(
    col("sensor_id"),
    col("timestamp"),
    col("vehicle_count"),
    col("avg_speed")
)

# write alerts to Kafka (as JSON)
alerts_to_kafka = alerts.select(
    struct(
        col("sensor_id"),
        col("timestamp"),
        col("vehicle_count"),
        col("avg_speed")
    ).alias("value")
).selectExpr("to_json(value) AS value")

alerts_query = alerts_to_kafka.writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("topic", ALERT_TOPIC) \
    .option("checkpointLocation", "/tmp/checkpoints/alerts") \
    .start()

# write alerts to Postgres (sink using foreachBatch)
def write_alerts_batch(df, epoch_id):
    if df.count() > 0:
        try:
            (df.write
                .format("jdbc")
                .mode("append")
                .option("url", POSTGRES_URL)
                .option("dbtable", "traffic_alerts")
                .option("user", POSTGRES_USER)
                .option("password", POSTGRES_PASS)
                .save()
            )
            print(f"Written {df.count()} alerts to Postgres (epoch {epoch_id})")
        except Exception as e:
            print(f"Error writing alerts: {e}")

alerts_write = alerts.writeStream.foreachBatch(write_alerts_batch) \
    .option("checkpointLocation", "/tmp/checkpoints/alerts_pg") \
    .start()

# write windowed aggregates to Postgres via foreachBatch
agg_write = windowed.writeStream.foreachBatch(write_aggregates_to_postgres) \
    .option("checkpointLocation", "/tmp/checkpoints/agg_pg") \
    .start()

print("Spark Streaming started. Waiting for data...")
print("Alerts query started")
print("Aggregates query started")

spark.streams.awaitAnyTermination()

