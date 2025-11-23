# etl/silver_to_gold.py
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, max as _max, min as _min

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
silver_path = project_root / "data" / "silver"
gold_path = project_root / "data" / "gold"

# Create gold directory if it doesn't exist
gold_path.mkdir(parents=True, exist_ok=True)

# -------------------------------
# Initialize Spark
# -------------------------------
spark = (
    SparkSession.builder
    .appName("Silver to Gold ETL")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
logger.info("Spark Session created successfully")

# -------------------------------
# Read Silver Parquet
# -------------------------------
logger.info(f"Reading Silver files from: {silver_path}")
silver_df = spark.read.parquet(str(silver_path))
logger.info(f"Total records read from Silver: {silver_df.count()}")
silver_df.show(5)

# -------------------------------
# Aggregations for Gold Layer
# -------------------------------
logger.info("Computing Gold-level aggregations...")

# Aggregate by pickup_day, pickup_hour, PULocationID, DOLocationID
gold_df = silver_df.groupBy(
    "pickup_year", "pickup_month", "pickup_day", "pickup_hour",
    "PULocationID", "DOLocationID"
).agg(
    count("*").alias("total_rides"),
    _sum("fare_amount").alias("total_fare"),
    _sum("tip_amount").alias("total_tip"),
    avg("fare_amount").alias("avg_fare"),
    avg("tip_pct").alias("avg_tip_pct"),
    avg("trip_distance").alias("avg_trip_distance"),
    avg("trip_duration_minutes").alias("avg_trip_duration"),
    _sum("is_long_trip").alias("num_long_trips"),
    _sum("is_short_trip").alias("num_short_trips"),
    _sum("high_tip_flag").alias("num_high_tip_trips")
)

logger.info("Gold aggregations completed")
gold_df.show(5)

# -------------------------------
# Save Gold Layer
# -------------------------------
logger.info(f"Writing Gold layer to: {gold_path} (partitioned by year/month/day)")
gold_df.write.mode("overwrite") \
    .partitionBy("pickup_year", "pickup_month", "pickup_day") \
    .parquet(str(gold_path))

logger.info("Gold layer processed successfully!")
spark.stop()
