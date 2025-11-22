import pandas as pd
import os
from pathlib import Path

# Get the project root directory (two levels up from this script)
script_dir = Path(__file__).parent
project_root = script_dir.parent.parent

# Define paths relative to project root
parquet_file = project_root / "data" / "raw" / "yellow_tripdata_2023-01.parquet"
csv_file = project_root / "data" / "raw" / "yellow_tripdata_2023-01.csv"
sample_file = project_root / "data" / "samples" / "sample_1k.csv"

# Check if parquet file exists
if not parquet_file.exists():
    print(f"Error: Parquet file not found at {parquet_file}")
    exit(1)

# Read Parquet file
print(f"Reading parquet file from: {parquet_file}")
df = pd.read_parquet(parquet_file)

# Optional: check first few rows
print("\nFirst few rows:")
print(df.head())

# Save full CSV
print(f"\nSaving full CSV to: {csv_file}")
df.to_csv(csv_file, index=False)

# Create a small sample with first 1000 rows
print(f"Saving sample CSV to: {sample_file}")
df_sample = df.head(1000)
df_sample.to_csv(sample_file, index=False)

print("\n✅ Full CSV and sample CSV created successfully!")
