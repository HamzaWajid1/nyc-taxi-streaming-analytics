import boto3
import json
import time
import pandas as pd
import os

# ---------- CONFIG ----------
STREAM_NAME = "EMRS-Spark-Streaming-KinesisDataStream-8b1mpyAiy6Xn"
REGION = "us-east-2"
S3_RAW_PATH = "s3://nyc-taxi-project-hamza/raw/sample_100k.csv"  # optional: local path too
SLEEP_TIME = 0.005  # seconds between events
# ----------------------------

# Initialize Kinesis client
kinesis = boto3.client("kinesis", region_name=REGION)

# Load CSV (local or from S3)
if S3_RAW_PATH.startswith("s3://"):
    import s3fs
    fs = s3fs.S3FileSystem()
    with fs.open(S3_RAW_PATH) as f:
        df = pd.read_csv(f)
else:
    df = pd.read_csv(S3_RAW_PATH)

print(f"Sending {len(df)} events to Kinesis stream '{STREAM_NAME}'...")

for idx, row in df.iterrows():
    event = row.to_dict()
    partition_key = str(event.get("VendorID", idx))  # fallback to index
    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(event),
        PartitionKey=partition_key
    )
    if idx % 500 == 0:
        print(f"{idx} events sent...")
    time.sleep(SLEEP_TIME)

print("All events sent!")
