from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ---------- CONFIG ----------
STREAM_NAME = "EMRS-Spark-Streaming-KinesisDataStream-8b1mpyAiy6Xn"
REGION = "us-east-2"
BRONZE_PATH = "s3://emrserverless-streaming-blog-697365479606-us-east-2/bronze/"
CHECKPOINT = "s3://emrserverless-streaming-blog-697365479606-us-east-2/checkpoints/bronze/"
# ----------------------------

# Define schema to match producer CSV columns
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

# Create Spark session
spark = SparkSession.builder \
    .appName("NYCTaxiKinesisConsumer") \
    .getOrCreate()

# Read from Kinesis using the *correct* EMR 7.x connector
df = spark.readStream \
    .format("aws-kinesis") \
    .option("kinesis.region", REGION) \
    .option("kinesis.streamName", STREAM_NAME) \
    .option("kinesis.consumerType", "GetRecords") \
    .option("kinesis.endpointUrl", f"https://kinesis.{REGION}.amazonaws.com") \
    .option("kinesis.startingposition", "LATEST") \
    .load()

# Convert Kinesis bytes to string
df_string = df.selectExpr("CAST(data AS STRING) as json_str")

# Parse JSON into columns
df_parsed = df_string.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Write stream to S3
query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", BRONZE_PATH) \
    .option("checkpointLocation", CHECKPOINT) \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()

print("Streaming job started. Writing to Bronze S3 bucket...")

query.awaitTermination()


