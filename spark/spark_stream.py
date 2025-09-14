from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

KAFKA = "localhost:9092"  # im Docker-Netz: kafka:9092
PGURL = "jdbc:postgresql://localhost:5432/nyc"
PGUSR, PGPW = "nyc", "nyc"
PGTBL = "rides"

spark = (SparkSession.builder
         .appName("TaxiPipe")
         .config("spark.jars.packages","org.postgresql:postgresql:42.7.3")
         .getOrCreate())

schema = StructType([
    StructField("service_type", StringType()),
    StructField("pickup_datetime", TimestampType()),
    StructField("dropoff_datetime", TimestampType()),
    StructField("trip_distance", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("pu_loc", IntegerType()),
    StructField("do_loc", IntegerType())
])

def read_topic(topic):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", topic)
           .option("startingOffsets","earliest")
           .load())
    parsed = raw.select(from_json(col("value").cast("string"), schema).alias("d")).select("d.*")
    clean = (parsed.dropna(subset=["pickup_datetime","fare_amount","trip_distance"])
                  .filter((col("fare_amount") >= 0) & (col("trip_distance") >= 0)))
    return clean

yellow = read_topic("taxi_yellow")
green  = read_topic("taxi_green")
clean  = yellow.unionByName(green)

def write_batch(df, epoch):
    (df.write.format("jdbc")
       .option("url", PGURL)
       .option("dbtable", PGTBL)
       .option("user", PGUSR).option("password", PGPW)
       .option("driver", "org.postgresql.Driver")
       .mode("append").save())

q = (clean.writeStream.outputMode("append")
     .foreachBatch(write_batch)
     .option("checkpointLocation","/tmp/chk_taxi_pipe")
     .start())

q.awaitTermination()
