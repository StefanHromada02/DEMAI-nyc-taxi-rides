# spark_stream.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import (
    col, when, coalesce, to_timestamp, to_utc_timestamp, from_json, lit,
    date_format, round as spark_round
)

PGURL  = os.getenv("PGURL", "jdbc:postgresql://postgres:5432/nyc")
PGUSR  = os.getenv("PGUSR", "nyc")
PGPW   = os.getenv("PGPW",  "nyc")
CHECKPOINT = os.getenv("CHECKPOINT", "/chk/taxi_pipe")
KAFKA = os.getenv("KAFKA", "kafka:29092")
PGTBL_Y = os.getenv("PGTBL_Y", "public.rides_yellow")
PGTBL_G = os.getenv("PGTBL_G", "public.rides_green")

spark = (SparkSession.builder
         .appName("taxi-pipe")
         .getOrCreate())

# Breites Schema: deckt gelb & grün ab
schema = StructType([
    StructField("service_type", StringType()),
    # Zeitspalten als String
    StructField("tpep_pickup_datetime",  StringType()),
    StructField("tpep_dropoff_datetime", StringType()),
    StructField("lpep_pickup_datetime",  StringType()),
    StructField("lpep_dropoff_datetime", StringType()),
    # Locations
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    # Kern-Metriken
    StructField("trip_distance", DoubleType()),
    StructField("fare_amount",  DoubleType()),
    StructField("tip_amount",   DoubleType()),
    StructField("total_amount", DoubleType()),
    # Zusatz
    StructField("VendorID", IntegerType()),
    StructField("trip_type", IntegerType()),       # nur green
    StructField("passenger_count", IntegerType()),
])

def read_topic(topic):
    return (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), schema).alias("d"))
        .select("d.*")
        .withColumn("src_topic", lit(topic)))

yellow_raw = read_topic("taxi_yellow")
green_raw  = read_topic("taxi_green")
raw = yellow_raw.unionByName(green_raw, allowMissingColumns=True)

# Timestamps: String -> Timestamp (lokal nach Format) -> UTC (NYC)
fmt = "yyyy-MM-dd HH:mm:ss"  # ggf. anpassen
pickup_str  = coalesce(col("tpep_pickup_datetime"),  col("lpep_pickup_datetime"))
dropoff_str = coalesce(col("tpep_dropoff_datetime"), col("lpep_dropoff_datetime"))
pickup_ts_local  = to_timestamp(pickup_str,  fmt)
dropoff_ts_local = to_timestamp(dropoff_str, fmt)

df = (raw
    .withColumn("pickup_datetime",  to_utc_timestamp(pickup_ts_local,  "America/New_York"))
    .withColumn("dropoff_datetime", to_utc_timestamp(dropoff_ts_local, "America/New_York"))
    .withColumn("fare_amount",  spark_round(col("fare_amount"), 2))
    .withColumn("tip_amount",   spark_round(col("tip_amount"), 2))
    .withColumn("total_amount", spark_round(col("total_amount"), 2))
    # service_type robust ableiten, falls nicht gesetzt
    .withColumn(
        "service_type",
        when(col("service_type").isNull(),
             when(col("src_topic")=="taxi_yellow","yellow")
             .when(col("src_topic")=="taxi_green","green")
        ).otherwise(col("service_type"))
    )
    # optionales Anzeigeformat, nur wenn du's brauchst
    .withColumn("pickup_day", date_format(col("pickup_datetime"), "yyyy-MM-dd"))
)

# Ziel-DataFrames je Tabelle (Spalten müssen zum Ziel-Schema passen!)
df_yellow = (df.filter(col("service_type")=="yellow")
    .select(
        "pickup_datetime", "dropoff_datetime", "trip_distance",
        "fare_amount", "tip_amount", "total_amount",
        col("PULocationID").alias("pu_loc"),
        col("DOLocationID").alias("do_loc"),
        col("VendorID").alias("vendor_id"),
        "passenger_count"
    ))

df_green = (df.filter(col("service_type")=="green")
    .select(
        "pickup_datetime", "dropoff_datetime", "trip_distance",
        "fare_amount", "tip_amount", "total_amount",
        col("PULocationID").alias("pu_loc"),
        col("DOLocationID").alias("do_loc"),
        col("VendorID").alias("vendor_id"),
        "trip_type",       # existiert nur bei green
        "passenger_count"
    ))

pg_props = {"user": PGUSR, "password": PGPW, "driver": "org.postgresql.Driver"}

def write_to_pg(table):
    def _fn(batch_df, batch_id):
        (batch_df
            .write
            .mode("append")
            .jdbc(PGURL, table, properties=pg_props))
    return _fn

qy = (df_yellow.writeStream
        .foreachBatch(write_to_pg(PGTBL_Y))
        .option("checkpointLocation", CHECKPOINT + "_yellow")
        .outputMode("append")
        .start())

qg = (df_green.writeStream
        .foreachBatch(write_to_pg(PGTBL_G))
        .option("checkpointLocation", CHECKPOINT + "_green")
        .outputMode("append")
        .start())

spark.streams.awaitAnyTermination()
