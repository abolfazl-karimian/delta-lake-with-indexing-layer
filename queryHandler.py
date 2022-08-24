import pyarrow.compute
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyarrow import fs, parquet as pq
import pandas as pd
import redis
import time

# Spark Confs
_conf = SparkConf()
_conf.setAppName("query")
_conf.set("spark.scheduler.mode", "FAIR")
_conf.set("spark.sql.shuffle.partitions", 10)

# Creating Connections
spark_session = SparkSession.builder.config(conf=_conf).getOrCreate()
pyArrow_hdfs_connector = fs.HadoopFileSystem("172.17.135.31", 9000, user="hadoop")

# No Spark - Just PyArrow
files = ["/delta/test/part-00000-1fb824a5-5936-4323-a765-f35fe8369fc7-c000.snappy.parquet|1",
         "/delta/test/part-00000-1fb824a5-5936-4323-a765-f35fe8369fc7-c000.snappy.parquet|0",
         "/delta/test/part-00000-888a8be6-46ae-4531-8afc-e7be2574c896-c000.snappy.parquet|0"]
start = time.time()
for file in files:
    file, rg = file.split("|")
    parquet_file_reader = pyArrow_hdfs_connector.open_input_file(
        file)
    print(parquet_file_reader)
    parquet_file = pq.ParquetFile(parquet_file_reader).read_row_group(int(rg))
    expr = pyarrow.compute.field("MSISDN") == "100072903330"
    record = parquet_file.filter(expr).to_pandas()
    print(record)
    finish = time.time()
    print(finish - start)

# Spark parquet reader
parquetFile1 = spark_session.read.parquet(files)

start = time.time()
Rows = spark_session.read.parquet(
    "hdfs://172.17.135.31:9000/delta/test/part-00000-1fb824a5-5936-4323-a765-f35fe8369fc7-c000.snappy.parquet",
    "hdfs://172.17.135.31:9000/delta/test/part-00000-1fb824a5-5936-4323-a765-f35fe8369fc7-c000.snappy.parquet",
    "hdfs://172.17.135.31:9000/delta/test/part-00000-888a8be6-46ae-4531-8afc-e7be2574c896-c000.snappy.parquet")\
    .withColumn("file", input_file_name())

Rows.filter(Rows.MSISDN == "100072903330").show(truncate=False)
finish = time.time()
print(finish - start)
