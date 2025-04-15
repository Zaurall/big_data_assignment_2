#!/bin/bash

source .venv/bin/activate

# Get parquet file name from command line or use default
PARQUET_FILE=${1:-"a.parquet"}
HDFS_PATH="/$PARQUET_FILE"

echo "Using parquet file: $PARQUET_FILE"

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 


unset PYSPARK_PYTHON

echo "Cleaning up previous data..."
hdfs dfs -test -e "$HDFS_PATH" && hdfs dfs -rm "$HDFS_PATH" && echo "Removed $HDFS_PATH"
hdfs dfs -test -e /data && hdfs dfs -rm -r /data && echo "Removed /data directory"
hdfs dfs -test -e /index/data && hdfs dfs -rm -r /index/data && echo "Removed /index/data directory"


# DOWNLOAD a.parquet or any parquet file before you run this
# Be ready that it uses 10GB of memory (RAM)

hdfs dfs -put -f "$PARQUET_FILE" /
echo "$PARQUET_FILE file uploaded to HDFS"
spark-submit \
    --conf spark.executor.memory=10G \
    --conf spark.driver.memory=10G \
    prepare_data.py "$HDFS_PATH"

echo "prepare_data.py completed"
echo "Putting data to hdfs"
hdfs dfs -put data /
echo "Data directory uploaded"
hdfs dfs -ls /data
hdfs dfs -ls /index/data
echo "done data preparation!"