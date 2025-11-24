"""
Phase 3: Load NYC Taxi Gold Layer to PostgreSQL via Spark JDBC
Drop & recreate table before loading data
With partition summary log
"""

from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import os
import psycopg2

# -------------------------
# 1️⃣ Configuration
# -------------------------

# Path to PostgreSQL JDBC jar
POSTGRES_JAR_PATH = "../../data/postgresql-42.7.8.jar"

# Path to Gold folder (root containing year/month/day partitions)
GOLD_FOLDER_PATH = "../../data/gold/"

# PostgreSQL connection details
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "taxi_analytics"
PG_USER = "taxi_user"
PG_PASSWORD = "taxi123"
PG_TABLE = "gold_aggregates"

# -------------------------
# 2️⃣ Drop & Recreate Table
# -------------------------

conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD
)
conn.autocommit = True
cursor = conn.cursor()

# Drop table if exists
cursor.execute(f"DROP TABLE IF EXISTS {PG_TABLE};")
print(f"✅ Dropped table {PG_TABLE} if it existed.")

# Create table
create_table_sql = f"""
CREATE TABLE {PG_TABLE} (
    pickup_hour INT,
    pickup_day_of_week INT,
    pickup_day_of_month INT,
    PULocationID INT,
    DOLocationID INT,
    total_rides BIGINT,
    total_fare DOUBLE PRECISION,
    total_tip DOUBLE PRECISION,
    total_revenue DOUBLE PRECISION,
    avg_fare DOUBLE PRECISION,
    avg_tip_pct DOUBLE PRECISION,
    num_high_tip_trips BIGINT,
    avg_trip_distance DOUBLE PRECISION,
    avg_trip_duration DOUBLE PRECISION,
    sum_trip_distance DOUBLE PRECISION,
    num_long_trips BIGINT,
    num_short_trips BIGINT,
    num_rush_hour_rides BIGINT,
    revenue_per_mile DOUBLE PRECISION,
    avg_revenue_per_ride DOUBLE PRECISION,
    avg_speed_mph DOUBLE PRECISION,
    tip_per_mile DOUBLE PRECISION,
    pickup_year INT,
    pickup_month INT,
    pickup_day INT
);
"""
cursor.execute(create_table_sql)
print(f"✅ Created table {PG_TABLE}.")
cursor.close()
conn.close()

# -------------------------
# 3️⃣ Initialize Spark
# -------------------------

spark = SparkSession.builder \
    .appName("GoldToPostgresPartitioned") \
    .config("spark.jars", POSTGRES_JAR_PATH) \
    .getOrCreate()

print("✅ Spark session started")

# -------------------------
# 4️⃣ Function to Write Partition to PostgreSQL
# -------------------------

def write_partition_to_postgres(df, table_name, jdbc_url, user, password):
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# -------------------------
# 5️⃣ Iterate Over Year/Month/Day Partitions
# -------------------------

jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

total_partitions = 0
total_rows_inserted = 0

# List year folders
years = [d for d in os.listdir(GOLD_FOLDER_PATH) if d.startswith("pickup_year=")]
for year in years:
    year_path = os.path.join(GOLD_FOLDER_PATH, year)
    months = [d for d in os.listdir(year_path) if d.startswith("pickup_month=")]
    for month in months:
        month_path = os.path.join(year_path, month)
        days = [d for d in os.listdir(month_path) if d.startswith("pickup_day=")]
        for day in days:
            day_path = os.path.join(month_path, day)
            print(f"📂 Processing partition: {year}/{month}/{day}")

            # Read parquet partition
            partition_df = spark.read.parquet(day_path)

            # Optional: reduce Spark partitions for JDBC write
            partition_df = partition_df.coalesce(1)

            # Count rows before write
            row_count = partition_df.count()

            # Write to PostgreSQL
            write_partition_to_postgres(
                partition_df,
                PG_TABLE,
                jdbc_url,
                PG_USER,
                PG_PASSWORD
            )

            total_partitions += 1
            total_rows_inserted += row_count

            print(f"✅ Loaded partition {year}/{month}/{day} to PostgreSQL ({row_count} rows)")

# -------------------------
# 6️⃣ Summary Log
# -------------------------

print("=======================================")
print("✅ Pipeline Summary:")
print(f"Total partitions loaded: {total_partitions}")
print(f"Total rows inserted: {total_rows_inserted}")
print("=======================================")

# -------------------------
# 7️⃣ Stop Spark
# -------------------------

spark.stop()
print("🛑 Spark session stopped")

