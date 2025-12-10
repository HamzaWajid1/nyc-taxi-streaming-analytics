from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark.storagelevel import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json
import json

# ---------- CONFIG ----------
STREAM_NAME = "nyc-taxi-stream"
REGION = "us-east-2"
BRONZE_PATH = "s3://nyc-taxi-project-hamza/bronze/"
CHECKPOINT = "s3://nyc-taxi-project-hamza/checkpoints/bronze/"
APP_NAME = "nyc-taxi-kinesis-dstream"
BATCH_DURATION = 5  # seconds
# ----------------------------

# Define schema (identical to Structured Streaming)
schema = StructType([
    StructField("VendorID", StringType()),
    StructField("tpep_pickup_datetime", StringType()),
    StructField("tpep_dropoff_datetime", StringType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", DoubleType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", DoubleType()),
    StructField("DOLocationID", DoubleType()),
    StructField("payment_type", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("airport_fee", DoubleType()),
])

# Initialize Spark and StreamingContext
sc = SparkContext(appName=APP_NAME)
ssc = StreamingContext(sc, BATCH_DURATION)
ssc.checkpoint(CHECKPOINT)

# Create Kinesis DStream
kinesis_stream = KinesisUtils.createStream(
    ssc,
    APP_NAME,
    STREAM_NAME,
    f"https://kinesis.{REGION}.amazonaws.com",
    REGION,
    InitialPositionInStream.LATEST,
    checkpointInterval=BATCH_DURATION * 1000,  # milliseconds
    storageLevel=StorageLevel.MEMORY_AND_DISK_2
)

# Convert bytes -> string -> JSON
json_rdd = kinesis_stream.map(lambda record: json.loads(record.decode('utf-8')))

# Function to convert RDD to DataFrame and write to Bronze
def save_rdd_to_s3(rdd):
    if not rdd.isEmpty():
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(rdd, schema=schema)
        # Ensure column order and type match exactly
        df = df.select([col(f.name) for f in schema.fields])
        df.write.mode("append").parquet(BRONZE_PATH)

json_rdd.foreachRDD(save_rdd_to_s3)

# Start streaming
ssc.start()
print("Streaming job started. Writing to Bronze S3 bucket...")
ssc.awaitTermination()
