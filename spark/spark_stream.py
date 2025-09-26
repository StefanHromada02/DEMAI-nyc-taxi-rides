# spark_stream.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, coalesce, to_timestamp, to_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

PGURL  = os.getenv("PGURL", "jdbc:postgresql://postgres:5432/nyc")
PGUSR  = os.getenv("PGUSR", "nyc")
PGPW   = os.getenv("PGPW",  "nyc")
CHECKPOINT = os.getenv("CHECKPOINT", "/chk/taxi_pipe")
KAFKA = os.getenv("KAFKA", "kafka:29092")
PGTBL = os.getenv("PGTBL", "public.rides")

spark = (SparkSession.builder
         .appName("taxi-pipe")
         .getOrCreate())

# Breites Schema: deckt gelbe & grüne Dateien ab
schema = StructType([
    StructField("service_type", StringType()),

    # Zeitspalten (als String; Producer schickt Strings)
    StructField("tpep_pickup_datetime",  StringType()),
    StructField("tpep_dropoff_datetime", StringType()),
    StructField("lpep_pickup_datetime",  StringType()),
    StructField("lpep_dropoff_datetime", StringType()),

    # Locations
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),

    # Core-Metriken
    StructField("trip_distance", DoubleType()),
    StructField("fare_amount",  DoubleType()),
    StructField("tip_amount",   DoubleType()),
    StructField("total_amount", DoubleType()),

    # Zusatzfelder
    StructField("VendorID", IntegerType()),
    StructField("trip_type", IntegerType()),       # green
    StructField("passenger_count", IntegerType()),

    # diverse weitere Felder möglich – ignorieren wir hier (Spark lässt sie unbeachtet)
])

def read_topic(topic):
    return (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")   # <<< NEU
        .load()
        .selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), schema).alias("d"))
        .select("d.*")
        .withColumn("src_topic", lit(topic)))

yellow = read_topic("taxi_yellow")
green  = read_topic("taxi_green")
raw = yellow.unionByName(green, allowMissingColumns=True)

# Pickup/Dropoff je nach Service wählen (yellow=tpep_*, green=lpep_*)
pickup_local  = coalesce(col("tpep_pickup_datetime"),  col("lpep_pickup_datetime"))
dropoff_local = coalesce(col("tpep_dropoff_datetime"), col("lpep_dropoff_datetime"))

# Strings -> timestamp (lokal), dann nach UTC
pickup_ts_local  = to_timestamp(pickup_local)   # interpretiert String ohne TZ als lokale Zeit
dropoff_ts_local = to_timestamp(dropoff_local)

# NYC -> UTC
pickup_utc  = to_utc_timestamp(pickup_ts_local,  "America/New_York")
dropoff_utc = to_utc_timestamp(dropoff_ts_local, "America/New_York")

df = (raw
    .withColumn("pickup_datetime",  pickup_utc)
    .withColumn("dropoff_datetime", dropoff_utc)
    .withColumn("pu_loc", col("PULocationID"))
    .withColumn("do_loc", col("DOLocationID"))
    .withColumn("vendor_id", col("VendorID"))
    # nur die für das Dashboard benötigten Spalten behalten
    .select(
        "service_type",
        "pickup_datetime", "dropoff_datetime",
        "trip_distance", "fare_amount", "tip_amount", "total_amount",
        "pu_loc", "do_loc",
        "vendor_id", "trip_type", "passenger_count",
    )
)

pg_props = {"user": PGUSR, "password": PGPW, "driver": "org.postgresql.Driver"}

def write_batch(batch_df, batch_id):
    (batch_df
        .write
        .mode("append")
        .jdbc(PGURL, PGTBL, properties=pg_props))

query = (df.writeStream
          .foreachBatch(write_batch)
          .option("checkpointLocation", CHECKPOINT)
          .outputMode("append")
          .start())

query.awaitTermination()
