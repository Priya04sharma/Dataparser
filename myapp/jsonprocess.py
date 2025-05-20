from pyspark.sql import SparkSession

from py4j.java_gateway import java_import

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from queue import Queue

import time

import redis
 
 
spark = SparkSession.builder \
    .appName("json_process") \
    .master("spark://192.168.1.214:7077") \
    .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs:///Files/iceberg/warehouse") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "8") \
    .getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()


fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
 
schema = StructType([StructField('petal.length', DoubleType(), True), StructField('petal.width', DoubleType(), True), StructField('sepal.length', DoubleType(), True), StructField('sepal.width', DoubleType(), True), StructField('variety', StringType(), True)])
 
r = redis.Redis(host='localhost', port=6379, db=0)

qname = 'jsonfiles'
 
 
sz = r.llen(qname)
for _ in range(sz):
    file = r.lpop(qname)
    file = file.decode()
    print("Processing File:", file)

    path_to_process = f"hdfs:///Files/multiple_files/{file}"

    df = spark.read \
        .format("json") \
        .schema(schema) \
        .load(path_to_process)

    print("Read the datafile")

    df.writeTo("hadoop_cat.db_name.json_table").append()

    print("Written the datafile")

    path_to_move = f"hdfs:///Files/json/{file}"

    path_to_move = spark._jvm.org.apache.hadoop.fs.Path(path_to_move)
    path_to_process = spark._jvm.org.apache.hadoop.fs.Path(path_to_process)

    fs.rename(path_to_process, path_to_move)

 
spark.stop()
 