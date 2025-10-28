# spark_stream.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import (
    col, when, coalesce, to_timestamp, to_utc_timestamp, from_json, lit,
    date_format, round as spark_round, count, min as spark_min, max as spark_max,
    sum as spark_sum, lag, sha2, concat_ws
)
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

# ----------------------- Konfiguration -----------------------

PGURL  = os.getenv("PGURL", "jdbc:postgresql://postgres:5432/nyc")
PGUSR  = os.getenv("PGUSR", "nyc")
PGPW   = os.getenv("PGPW",  "nyc")
CHECKPOINT = os.getenv("CHECKPOINT", "/chk/taxi_pipe")
KAFKA = os.getenv("KAFKA", "kafka:29092")
PGTBL_Y = os.getenv("PGTBL_Y", "public.rides_yellow")
PGTBL_G = os.getenv("PGTBL_G", "public.rides_green")

# Geordnete Writes aktivierbar
ORDERED_WRITES = os.getenv("ORDERED_WRITES", "1") == "1"
# Weniger Shuffles, wenn geordnet geschrieben wird
SHUFFLE_PARTS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "1" if ORDERED_WRITES else "200"))

spark = (SparkSession.builder
         .appName("taxi-pipe")
         .getOrCreate())

# Shuffle-Partitionen anpassen
spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTS))

# ----------------------- Schema & Input -----------------------

schema = StructType([
    StructField("service_type", StringType()),
    # yellow
    StructField("tpep_pickup_datetime",  StringType()),
    StructField("tpep_dropoff_datetime", StringType()),
    # green
    StructField("lpep_pickup_datetime",  StringType()),
    StructField("lpep_dropoff_datetime", StringType()),
    # gemeinsam
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_type", IntegerType()),     # nur green sinnvoll, bleibt sonst null
    StructField("payment_type", IntegerType()),  # NEU
])

def read_topic(topic: str):
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

# ----------------------- Transformation -----------------------

# Strings -> Timestamp lokal -> UTC (NYC)
fmt = "yyyy-MM-dd HH:mm:ss"
pickup_str  = coalesce(col("tpep_pickup_datetime"),  col("lpep_pickup_datetime"))
dropoff_str = coalesce(col("tpep_dropoff_datetime"), col("lpep_dropoff_datetime"))
pickup_ts_local  = to_timestamp(pickup_str,  fmt)
dropoff_ts_local = to_timestamp(dropoff_str, fmt)

df = (raw
    .withColumn("pickup_datetime",  to_utc_timestamp(pickup_ts_local,  "America/New_York"))
    .withColumn("dropoff_datetime", to_utc_timestamp(dropoff_ts_local, "America/New_York"))
    .withColumn(
        "service_type",
        when(col("service_type").isNull(),
             when(col("src_topic")=="taxi_yellow","yellow")
             .when(col("src_topic")=="taxi_green","green")
        ).otherwise(col("service_type"))
    )
    .withColumn("pickup_day", date_format(col("pickup_datetime"), "yyyy-MM-dd"))
)

# Ziel-DFs je Tabelle (Spalten müssen zum Ziel passen)
df_yellow = (df.filter(col("service_type")=="yellow")
    .select(
        "pickup_datetime", "dropoff_datetime",
        col("PULocationID").alias("pu_loc"),
        col("DOLocationID").alias("do_loc"),
        "passenger_count",
        "payment_type",
    ))

df_green = (df.filter(col("service_type")=="green")
    .select(
        "pickup_datetime", "dropoff_datetime",
        col("PULocationID").alias("pu_loc"),
        col("DOLocationID").alias("do_loc"),
        "passenger_count",
        "trip_type",
        "payment_type",
    ))

# ----------------------- JDBC Props -----------------------

pg_props = {"user": PGUSR, "password": PGPW, "driver": "org.postgresql.Driver"}

# ----------------------- Batch-Stats -----------------------

def compute_batch_stats(df_in: DataFrame, service_type: str, batch_id: int) -> DataFrame:
    dfb = df_in

    inversed = dfb.filter(col("dropoff_datetime") < col("pickup_datetime")).count()
    null_pickup  = dfb.filter(col("pickup_datetime").isNull()).count()
    null_dropoff = dfb.filter(col("dropoff_datetime").isNull()).count()

    from pyspark.sql.window import Window
    w = Window.orderBy(col("pickup_datetime").asc())
    equal_ts = (dfb.select(col("pickup_datetime"),
                           lag(col("pickup_datetime")).over(w).alias("prev_pickup"))
                  .filter(col("prev_pickup").isNotNull() & (col("pickup_datetime")==col("prev_pickup"))).count())

    key_cols_common = ["pickup_datetime","dropoff_datetime","pu_loc","do_loc","passenger_count","payment_type"]
    key_cols = key_cols_common + (["trip_type"] if service_type=="green" else [])
    df_key = dfb.withColumn("__key", sha2(concat_ws("||", *[col(c).cast("string") for c in key_cols]), 256))
    dupes = (df_key.groupBy("__key").count()
             .filter(col("count") > 1)
             .agg(spark_sum(col("count") - 1)).collect())
    dupes_count = int(dupes[0][0]) if dupes and dupes[0][0] is not None else 0

    rows_total = dfb.count()
    min_ts = dfb.agg(spark_min(col("pickup_datetime"))).collect()[0][0]
    max_ts = dfb.agg(spark_max(col("pickup_datetime"))).collect()[0][0]

    return spark.createDataFrame(
        [(int(batch_id), service_type, rows_total, null_pickup, null_dropoff,
          inversed, equal_ts, dupes_count, min_ts, max_ts)],
        schema="""batch_id LONG, service_type STRING, rows_total LONG,
                  rows_null_pickup LONG, rows_null_dropoff LONG,
                  rows_inversed LONG, rows_equal_ts LONG, rows_dupes LONG,
                  pickup_min TIMESTAMP, pickup_max TIMESTAMP"""
    )


# ----------------------- Writer (Ordered + Stats) -----------------------

def write_ordered_with_stats(table: str, service_type: str):
    def _fn(batch_df: DataFrame, batch_id: int):
        df_out = batch_df
        if ORDERED_WRITES:
            # Älteste zuerst im Batch; ein Task für deterministischere Insert-Reihenfolge
            df_out = df_out.orderBy(col("pickup_datetime").asc()).coalesce(1)

        # 1) Payload in Zieltabelle
        (df_out.write
            .mode("append")
            .jdbc(PGURL, table, properties=pg_props))

        # 2) Metriken in ingest_stats (Tabelle vorher anlegen oder per DDL vorbereiten)
        stats_df = compute_batch_stats(df_out, service_type, batch_id)
        (stats_df.write
            .mode("append")
            .jdbc(PGURL, "public.ingest_stats", properties=pg_props))
    return _fn

# ----------------------- Streaming Queries -----------------------

qy = (df_yellow.writeStream
        .foreachBatch(write_ordered_with_stats(PGTBL_Y, "yellow"))
        .option("checkpointLocation", CHECKPOINT + "_yellow")
        .outputMode("append")
        .start())

qg = (df_green.writeStream
        .foreachBatch(write_ordered_with_stats(PGTBL_G, "green"))
        .option("checkpointLocation", CHECKPOINT + "_green")
        .outputMode("append")
        .start())

spark.streams.awaitAnyTermination()
