from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from pyspark.conf import SparkConf
import redis

delta_table_path = "hdfs://master:9000/delta/partitioned200"
stream_to_delta_checkpoint_path = "hdfs://master:9000/Checkpoints/partitioned200"

_conf = SparkConf()
_conf.setAppName("store-on-lake")

builder = SparkSession \
    .builder \
    .config(conf=_conf) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.0') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", 10)
session = configure_spark_with_delta_pip(builder).getOrCreate()

session.sparkContext.setLogLevel("WARN")

kafka_df = session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092") \
    .option("subscribe", "cdr_sample") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 200000000) \
    .option("minOffsetsPerTrigger", 200000000) \
    .load() \
    .selectExpr("partition", "CAST(value AS STRING)")

kafka_schema = (StructType()
                .add(StructField("MSISDN", StringType()))
                .add(StructField("CALL_PARTNER", StringType()))
                .add(StructField("IMSI", StringType()))
                .add(StructField("CDR_TYPE", StringType()))
                .add(StructField("DURATION", IntegerType()))
                .add(StructField("REVENUE", IntegerType()))
                .add(StructField("SEQ_NO", IntegerType()))
                .add(StructField("MSC", StringType()))
                .add(StructField("REGION", StringType()))
                .add(StructField("PROVINCE", StringType()))
                .add(StructField("RECORD_DATE", TimestampType()))
                )

kafka_df_with_schema = kafka_df.select(from_json(col("value"), kafka_schema).alias("data"), col("partition")).select(
    "data.*").withColumn("DAY", substring(col("RECORD_DATE"), 0, 10))
# "partition", "data.*").withColumn("DAY", substring(col("RECORD_DATE"), 0, 8))
# kafka_df_with_schema.show()
# deduplicated_data = kafka_df_with_schema.withWatermark("RECORD_DATE", "24 hours") \
#     .dropDuplicates(["MSISDN", "CALL_PARTNER", "DURATION", "CDR_TYPE", "IMSI", "SEQ_NO", "RECORD_DATE"])

# You can add post/pre as a partition level104835206
# Partition levels can be : Province - Post/Pre - Type(Network/Voice/SMS)
kafka_df_with_schema.writeStream \
    .format("delta") \
    .option("checkpointLocation", stream_to_delta_checkpoint_path) \
    .partitionBy("DAY", "PROVINCE") \
    .outputMode("append") \
    .start(delta_table_path) \
    .awaitTermination()
