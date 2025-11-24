# etl/silver_to_gold.py
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, when, lit
)

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
gold_path.mkdir(parents=True, exist_ok=True)

# -------------------------------
# Initialize Spark
# -------------------------------
spark = (
    SparkSession.builder
    .appName("Silver to Gold ETL (Enhanced KPIs)")
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

# -------------------------------
# Cast Important Columns
# -------------------------------
silver_df = (
    silver_df
    .withColumn("PULocationID", col("PULocationID").cast("int"))
    .withColumn("DOLocationID", col("DOLocationID").cast("int"))
)

logger.info("Casted PULocationID & DOLocationID to integer")

# -------------------------------
# Add Rush Hour Flag (optional KPI)
# -------------------------------
silver_df = silver_df.withColumn(
    "rush_hour_flag",
    when(col("pickup_hour").between(7, 10), 1)
    .when(col("pickup_hour").between(16, 19), 1)
    .otherwise(0)
)

logger.info("Added rush_hour_flag column")

# -------------------------------
# Gold-Level Aggregations
# -------------------------------
logger.info("Computing Gold-level aggregations with enhanced KPIs...")

gold_df = silver_df.groupBy(
    "pickup_year", "pickup_month", "pickup_day",
    "pickup_hour", "pickup_day_of_week", "pickup_day_of_month",
    "PULocationID", "DOLocationID"
).agg(
    count("*").alias("total_rides"),

    # Financial metrics
    _sum("fare_amount").alias("total_fare"),
    _sum("tip_amount").alias("total_tip"),
    _sum("total_amount").alias("total_revenue"),
    avg("fare_amount").alias("avg_fare"),

    # Tip metrics
    avg("tip_pct").alias("avg_tip_pct"),
    _sum("high_tip_flag").alias("num_high_tip_trips"),

    # Distance & duration
    avg("trip_distance").alias("avg_trip_distance"),
    avg("trip_duration_minutes").alias("avg_trip_duration"),
    _sum("trip_distance").alias("sum_trip_distance"),

    # Trip categories
    _sum("is_long_trip").alias("num_long_trips"),
    _sum("is_short_trip").alias("num_short_trips"),

    # Rush hour insights
    _sum("rush_hour_flag").alias("num_rush_hour_rides")
)

# -------------------------------
# Add Derived High-Value KPIs
# -------------------------------
gold_df = (
    gold_df
    # Revenue per mile
    .withColumn(
        "revenue_per_mile",
        when(col("sum_trip_distance") > 0,
             col("total_revenue") / col("sum_trip_distance"))
        .otherwise(lit(0.0))
    )
    # Avg revenue per ride
    .withColumn(
        "avg_revenue_per_ride",
        col("total_revenue") / col("total_rides")
    )
    # Avg speed (mph)
    .withColumn(
        "avg_speed_mph",
        when(col("avg_trip_duration") > 0,
             col("avg_trip_distance") / (col("avg_trip_duration") / 60))
        .otherwise(lit(0.0))
    )
    # Tip per mile
    .withColumn(
        "tip_per_mile",
        when(col("sum_trip_distance") > 0,
             col("total_tip") / col("sum_trip_distance"))
        .otherwise(lit(0.0))
    )
)

logger.info("Enhanced KPIs added successfully")
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

