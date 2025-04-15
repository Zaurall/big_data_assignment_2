from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import re
import sys


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

# Get parquet file path from command line argument or use default
parquet_file = sys.argv[1] if len(sys.argv) > 1 else "/a.parquet"
print(f"Processing parquet file: {parquet_file}")

df = spark.read.parquet(parquet_file)
n = 1000
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)

def normalize_whitespace(text):
    # Replace all whitespace sequences with a single space
    return re.sub(r'\s+', ' ', text).strip()

normalize_whitespace_udf = udf(normalize_whitespace, StringType())

df = df.withColumn("title", normalize_whitespace_udf(col("title")))

def create_doc(row):
    filename = sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_")
    filename = "data/" + re.sub(r'[^a-zA-Z0-9_-]', '', filename)  + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)


df.write.option("sep", "\t").mode("overwrite").csv("/index/data")