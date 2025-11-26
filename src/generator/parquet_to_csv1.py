import pandas as pd
import os
from pathlib import Path
import numpy as np

# Get the project root directory (two levels up from this script)
script_dir = Path(__file__).parent
project_root = script_dir.parent.parent

# Define paths relative to project root
parquet_file = project_root / "data" / "raw" / "yellow_tripdata_2023-01.parquet"
csv_file = project_root / "data" / "raw" / "yellow_tripdata_2023-01.csv"
sample_file = project_root / "data" / "samples" / "sample_100k.csv"

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

# ---------------------------
# Create a diverse 1000-row sample
# ---------------------------

print(f"Saving diverse sample CSV to: {sample_file}")

total_rows = len(df)

# If dataset smaller than 1000, fallback to all rows
sample_size = min(100000, total_rows)

# Split into 10 chunks and pick evenly from each (to ensure spread)
num_chunks = 10
rows_per_chunk = sample_size // num_chunks

indices = []

for i in range(num_chunks):
    start = int((total_rows / num_chunks) * i)
    end = int((total_rows / num_chunks) * (i + 1))
    chunk_indices = np.random.choice(range(start, end), rows_per_chunk, replace=False)
    indices.extend(chunk_indices)

# If we still have fewer than sample_size (due to rounding), fill randomly
if len(indices) < sample_size:
    extra_needed = sample_size - len(indices)
    extra_indices = np.random.choice(total_rows, extra_needed, replace=False)
    indices.extend(extra_indices)

df_sample = df.iloc[indices]
df_sample.to_csv(sample_file, index=False)

print("\n✅ Full CSV and diverse sample CSV created successfully!")
