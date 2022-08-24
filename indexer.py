import pyarrow.compute
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyarrow import fs, parquet as pq
import pandas as pd
import redis

# Source, Sync & Checkpoint Paths
table_path = "/delta/partitioned1/"
table_logs_path = f"{table_path}_delta_log/*.json"
stream_to_redis_checkpoint_path = "/Checkpoints/indexing2"

# Spark Confs
_conf = SparkConf()
_conf.setAppName("Indexing")
_conf.set("spark.scheduler.mode", "FAIR")
_conf.set("spark.sql.shuffle.partitions", 10)
_conf.set("spark.sql.streaming.minBatchesToRetain", 50)

# Creating Connections
spark_session = SparkSession.builder.config(conf=_conf).getOrCreate()
pyArrow_hdfs_connector = fs.HadoopFileSystem("172.17.135.31", 9000, user="hadoop")
redis_connector = redis.Redis(host='dn6', port=6379)

schema = (StructType()
          .add(StructField("add", StructType()
                           .add(StructField("path", StringType()))
                           .add(StructField("partitionValues", StringType()))
                           .add(StructField("size", StringType()))
                           .add(StructField("modificationTime", StringType()))
                           .add(StructField("dataChange", BooleanType()))
                           .add(StructField("stats", StringType()))
                           )))

# Cleanse and extract file names
json_data = spark_session.readStream.schema(schema).option("maxFilesPerTrigger", 2).json(table_logs_path)
raw_data = json_data.select("add").na.drop().select("add.path")
files = raw_data.withColumn("full_path", concat(lit(table_path), col("path"))).drop("path")


# files = files.limit(2)
# files.show(10, False)


# def get_rowGroup(address):
#     print(address.full_path)
#     x = "/home/abolfazl/Downloads/userdata5.parquet"
#     parquet_file = pq.ParquetFile(f"{address.full_path}")
#     parquet_file = pq.ParquetFile(x)
#     metadata = parquet_file.metadata
#     print(metadata)
#     print(parquet_file.schema)
#     print(parquet_file.num_row_groups)
#     for rg in range(0, parquet_file.num_row_groups):
#         # Read from local file system
#         parquet_data = parquet_file.read_row_group(rg).column("salary").to_pandas()
#         data_frame = pd.DataFrame(parquet_data).assign(file_name=address.full_path, rg_number=rg)
#         for index, row in data_frame.iterrows():
#             print(row["salary"], f"{row['file_name']}-{row['rg_number']}")
def save_to_redis(MSISDN, file_name, rg_number):
    redis_connector.sadd(MSISDN, f"{file_name}|{rg_number}")


# def explore_rows(address):
#     parquet_file_reader = hdfs_connector.open_input_file(address.full_path)
#     parquet_file = pq.ParquetFile(parquet_file_reader)
#     for rg in range(0, parquet_file.num_row_groups):
#         parquet_data = parquet_file.read_row_group(rg).column("MSISDN").to_pandas()
#         data_frame = pd.DataFrame(parquet_data).assign(file_name=address.full_path, rg_number=rg)
#         for index, row in data_frame.iterrows():
#             # save_to_redis(row["MSISDN"], row['file_name'], row['rg_number'])
#             print(row)


def explore_rows(files_df, batch_id):
    try:
        files_df = files_df.toPandas()
        print("Batch ID", batch_id, ": ", len(files_df.index), "File(s).")
        for index, file in files_df.iterrows():
            print("File In Process: ", file.full_path)
            parquet_file_reader = pyArrow_hdfs_connector.open_input_file(file.full_path)
            parquet_file = pq.ParquetFile(parquet_file_reader)
            for rg in range(0, parquet_file.num_row_groups):
                n = 0
                parquet_data = parquet_file.read_row_group(rg).column("MSISDN").to_pandas()
                data_frame = pd.DataFrame(parquet_data).assign(file_name=file.full_path, rg_number=rg)
                for index, row in data_frame.iterrows():
                    n += 1
                    save_to_redis(row.MSISDN, row.file_name, row.rg_number)
                print("RowGroup", rg, ": ", n, "Rows")
        print(
            "---------------------------------------------------------------------------------------------------------")
    except IOError:
        print("IO ERROR HAPPENED.")


try:
    files.writeStream \
        .foreachBatch(explore_rows) \
        .option("checkpointLocation", stream_to_redis_checkpoint_path) \
        .start() \
        .awaitTermination()
except:
    print("Some ERROR HAPPENED.")

# cat indexing.log | grep Rows | cut -d " " -f 5 | awk '{s+=$1} END {printf "%.0f\n", s}'
