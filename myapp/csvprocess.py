from pyspark.sql import SparkSession

from py4j.java_gateway import java_import

from pyspark.sql.types import StructType, StructField, StringType

import redis
 
# Initialize Spark session

# spark = SparkSession.builder \

#     .appName("outqueue_process_csv") \

#     .master("spark://192.168.1.214:7077") \

#     .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \

#     .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \

#     .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs:///Files/iceberg/warehouse") \

#     .config("spark.executor.memory", "4g") \

#     .config("spark.executor.cores", "2") \

#     .config("spark.cores.max", "8") \

#     .getOrCreate()
 
# # CSV schema

# schema = StructType([

#     StructField('Title', StringType(), True),

#     StructField('Genre', StringType(), True),

#     StructField('ReleaseDate', StringType(), True),

#     StructField('Runtime', StringType(), True),

#     StructField('IMDB Score', StringType(), True),

#     StructField('Language', StringType(), True),

#     StructField('Views', StringType(), True),

#     StructField('AddedDate', StringType(), True)

# ])
 
# # Hadoop configuration

# hadoop_conf = spark._jsc.hadoopConfiguration()

# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
 
# # Redis setup

# r = redis.Redis(host='localhost', port=6379, db=0)

# qname = 'csvfiles'
 
# # Get files to process

# sz = r.llen(qname)
 
# for _ in range(sz):

#     file = r.lpop(qname)

#     file = file.decode()

#     print("Processing File:", file)

#     path_to_process = f"hdfs:///Files/multiple_files/{file}"

#     # Read CSV file

#     df = spark.read.format("csv") \

#         .option("header", "true") \

#         .schema(schema) \

#         .load(path_to_process)
 
#     print("Read the datafile")

#     # Write to Iceberg table

#     df.writeTo("hadoop_cat.db_name.csv_table").append()

#     print("Written the datafile")

#     # Move file to processed folder

#     path_to_move = f"hdfs:///Files/csv/{file}"

#     path_to_move = spark._jvm.org.apache.hadoop.fs.Path(path_to_move)

#     path_to_process = spark._jvm.org.apache.hadoop.fs.Path(path_to_process)

#     fs.rename(path_to_process, path_to_move)
 
# spark.stop()
 


# Spark session
spark = SparkSession.builder \
    .appName("csv_process") \
    .master("spark://192.168.1.214:7077") \
    .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs:///Files/iceberg/warehouse") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "8") \
    .getOrCreate()

# CSV schema
schema = StructType([
    StructField('Title', StringType(), True),
    StructField('Genre', StringType(), True),
    StructField('ReleaseDate', StringType(), True),
    StructField('Runtime', StringType(), True),
    StructField('IMDB Score', StringType(), True),
    StructField('Language', StringType(), True),
    StructField('Views', StringType(), True),
    StructField('AddedDate', StringType(), True)
])

# Hadoop configuration
hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

# Redis setup
r = redis.Redis(host='localhost', port=6379, db=0)
qname = 'csvfiles'

# Get files to process
sz = r.llen(qname)

for _ in range(sz):
    file = r.lpop(qname)
    file = file.decode()

    print("Processing File:", file)

    path_to_process = f"hdfs:///Files/multiple_files/{file}"

    # Read CSV file
    df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(path_to_process)

    print("Read the datafile")

    # Write to Iceberg table
    df.writeTo("hadoop_cat.db_name.csv_table").append()

    print("Written the datafile")

    # Move file to processed folder
    path_to_move = f"hdfs:///Files/csv/{file}"
    path_to_move = spark._jvm.org.apache.hadoop.fs.Path(path_to_move)
    path_to_process = spark._jvm.org.apache.hadoop.fs.Path(path_to_process)

    fs.rename(path_to_process, path_to_move)

spark.stop()
