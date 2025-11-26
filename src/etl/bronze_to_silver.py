# etl/bronze_to_silver.py
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, dayofmonth, when, year, month, to_timestamp, expr

# -------------------------------
# Setup logging
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -------------------------------
# Paths
# -------------------------------
project_root = Path(__file__).parent.parent.parent
bronze_path = project_root / "data" / "bronze"
silver_path = project_root / "data" / "silver"

# Create silver directory if it doesn't exist
silver_path.mkdir(parents=True, exist_ok=True)

# -------------------------------
# Initialize Spark
# -------------------------------
spark = (
    SparkSession.builder
    .appName("Bronze to Silver ETL")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
logger.info("Spark Session created successfully")

# -------------------------------
# Read Bronze JSON
# -------------------------------
logger.info(f"Reading Bronze files from: {bronze_path}")
bronze_df = spark.read.json(f"{bronze_path}/*.json")
logger.info(f"Total records read: {bronze_df.count()}")
bronze_df.show(5)

# -------------------------------
# 1. Cleaning
# -------------------------------
logger.info("Casting numeric columns to proper types...")
# Cast numeric columns to DoubleType before filtering (JSON reader may infer them as strings)
silver_df = bronze_df.withColumn("passenger_count", col("passenger_count").cast("double")) \
    .withColumn("trip_distance", col("trip_distance").cast("double")) \
    .withColumn("fare_amount", col("fare_amount").cast("double")) \
    .withColumn("total_amount", col("total_amount").cast("double")) \
    .withColumn("tip_amount", col("tip_amount").cast("double"))

logger.info("Applying data quality filters...")
silver_df = silver_df.filter(
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("total_amount") > 0) &
    col("tpep_pickup_datetime").isNotNull() &
    col("tpep_dropoff_datetime").isNotNull()
)
logger.info(f"Records after filtering: {silver_df.count()}")

# -------------------------------
# 2. Enrichment
# -------------------------------
logger.info("Adding enriched features...")

# Convert datetime strings to timestamps first
silver_df = silver_df.withColumn(
    "tpep_pickup_datetime",
    to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
)

silver_df = silver_df.withColumn(
    "tpep_dropoff_datetime",
    to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss")
)

# Trip duration in minutes (using unix_timestamp for seconds, then convert to minutes)
silver_df = silver_df.withColumn(
    "trip_duration_minutes",
    expr("(unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0")
)

# Tip percentage (with null safety)
silver_df = silver_df.withColumn(
    "tip_pct",
    when(col("fare_amount") > 0, (col("tip_amount") / col("fare_amount")) * 100).otherwise(0.0)
)

# Timestamp features
silver_df = silver_df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
silver_df = silver_df.withColumn("pickup_day_of_week", dayofweek(col("tpep_pickup_datetime")))
silver_df = silver_df.withColumn("pickup_day_of_month", dayofmonth(col("tpep_pickup_datetime")))

# Trip flags
silver_df = silver_df.withColumn("is_long_trip", when(col("trip_distance") > 20, 1).otherwise(0))
silver_df = silver_df.withColumn("is_short_trip", when(col("trip_distance") < 1, 1).otherwise(0))
silver_df = silver_df.withColumn("high_tip_flag", when(col("tip_pct") > 20, 1).otherwise(0))

# Partitioning columns for saving
silver_df = silver_df.withColumn("pickup_year", year(col("tpep_pickup_datetime")))
silver_df = silver_df.withColumn("pickup_month", month(col("tpep_pickup_datetime")))
silver_df = silver_df.withColumn("pickup_day", dayofmonth(col("tpep_pickup_datetime")))

# Quick stats for QA
logger.info("Data summary for Silver layer:")
silver_df.describe("trip_distance", "fare_amount", "trip_duration_minutes", "tip_pct").show(5)
silver_df.show(5)

# -------------------------------
# 3. Save Silver
# -------------------------------
logger.info(f"Writing Silver layer to: {silver_path} (partitioned by year/month/day)")
silver_df.write.mode("overwrite") \
    .partitionBy("pickup_year", "pickup_month", "pickup_day") \
    .parquet(str(silver_path))

logger.info("Silver layer processed successfully!")
spark.stop()

