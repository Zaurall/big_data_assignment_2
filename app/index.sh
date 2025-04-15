#!/bin/bash

# Default input path
INPUT_PATH=${1:-"/index/data"}

# Clean up any existing temporary directories
hdfs dfs -rm -r -f /tmp/index/step1
hdfs dfs -rm -r -f /tmp/index/step2
hdfs dfs -rm -r -f /tmp/index/step3

# Make mapreduce scripts executable
chmod +x mapreduce/mapper1.py mapreduce/reducer1.py
chmod +x mapreduce/mapper2.py mapreduce/reducer2.py
chmod +x mapreduce/mapper3.py mapreduce/reducer3.py

# Verify the input directory exists
echo "$INPUT_PATH"
hdfs dfs -test -e "$INPUT_PATH"
if [ $? -ne 0 ]; then
    echo "Error: Input path $INPUT_PATH does not exist!"
    echo "Creating empty directory..."
    hdfs dfs -mkdir -p "$INPUT_PATH"
    hdfs dfs -ls /
    exit 1
fi

echo "Starting first MapReduce job - Term frequencies and positions..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapreduce/mapper1.py,mapreduce/reducer1.py \
    -input "$INPUT_PATH" \
    -output /tmp/index/step1 \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py"

if [ $? -ne 0 ]; then
    echo "First MapReduce job failed"
    exit 1
fi

echo "First MapReduce job succeeded. Sample output:"

echo "Starting second MapReduce job - Document frequencies..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapreduce/mapper2.py,mapreduce/reducer2.py \
    -input /tmp/index/step1 \
    -output /tmp/index/step2 \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py"

if [ $? -ne 0 ]; then
    echo "Second MapReduce job failed"
    exit 1
fi

echo "Second MapReduce job succeeded!"

echo "Starting third MapReduce job - Document metadata..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapreduce/mapper3.py,mapreduce/reducer3.py \
    -input "$INPUT_PATH" \
    -output /tmp/index/step3 \
    -mapper "python3 mapper3.py" \
    -reducer "python3 reducer3.py"

if [ $? -ne 0 ]; then
    echo "Third MapReduce job failed"
    exit 1
fi

echo "Importing data into Cassandra..."
python3 app.py

if [ $? -ne 0 ]; then
    echo "Failed to import data into Cassandra"
    exit 1
fi

echo "Successfully imported index data into Cassandra"
