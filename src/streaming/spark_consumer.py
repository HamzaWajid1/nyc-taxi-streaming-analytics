import os
import sys
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get project root directory
project_root = Path(__file__).parent.parent.parent
bronze_path = project_root / "data" / "bronze"
checkpoint_path = project_root / "data" / "bronze" / "checkpoint"

# Create directories if they don't exist
bronze_path.mkdir(parents=True, exist_ok=True)
checkpoint_path.mkdir(parents=True, exist_ok=True)

# Define schema to match your sample data
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


def main():
    try:
        logger.info("Initializing Spark Session...")
        # Check Java version
        import subprocess
        try:
            java_version_output = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT, text=True)
            logger.info(f"Java version: {java_version_output.split(chr(10))[0]}")
        except Exception as e:
            logger.warning(f"Could not check Java version: {e}")
        
        # Add Kafka connector package - required for Kafka streaming
        # Using Scala 2.13 version to match PySpark 4.0.1
        spark = (
            SparkSession.builder
            .appName("TaxiTripConsumer")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
            .config("spark.sql.streaming.checkpointLocation", str(checkpoint_path))
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
        
        spark.sparkContext.setLogLevel("WARN")  # Reduce Spark verbosity
        logger.info("Spark Session created successfully")

        logger.info("Connecting to Kafka topic 'taxi_trips'...")
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "taxi_trips")
            .option("startingOffsets", "earliest")  # Start from beginning if no checkpoint
            .option("failOnDataLoss", "false")  # Don't fail if Kafka has data loss
            .load()
        )
        logger.info("Kafka connection established")

        # Extract JSON value
        logger.info("Parsing JSON data...")
        json_df = df.selectExpr("CAST(value AS STRING) as json")
        parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

        # Write to Bronze Layer (raw data)
        logger.info(f"Starting streaming query. Writing to: {bronze_path}")
        logger.info(f"Checkpoint location: {checkpoint_path}")
        
        query = (
            parsed_df.writeStream
            .format("json")
            .option("path", str(bronze_path))
            .option("checkpointLocation", str(checkpoint_path))
            .outputMode("append")
            .trigger(processingTime="5 seconds")  # Process every 5 seconds
            .start()
        )
        
        logger.info("Streaming query started successfully!")
        logger.info("Waiting for data from Kafka...")
        logger.info("Press Ctrl+C to stop the consumer")
        
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Stopping streaming query...")
        if 'query' in locals():
            query.stop()
        logger.info("Consumer stopped gracefully")
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        if 'query' in locals():
            query.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
