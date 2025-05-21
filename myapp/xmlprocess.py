from pyspark.sql import SparkSession

from py4j.java_gateway import java_import

from pyspark.sql.types import StructType, StructField, StringType

from queue import Queue

import time

import redis
def run(spark):
    # spark = (
    #     SparkSession.builder
    #     .appName("xml_process")
    #     .master("spark://192.168.1.214:7077")
    #     .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog")
    #     .config("spark.sql.catalog.hadoop_cat.type", "hadoop")
    #     .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs:///Files/iceberg/warehouse")
    #     .config("spark.executor.memory", "4g")
    #     .config("spark.executor.cores", "2")
    #     .config("spark.cores.max", "8")
    #     .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.15.0")
    #     .getOrCreate()
    # )

    
    schema = StructType([StructField('auction_info', StructType([StructField('bid_increment', StringType(), True), StructField('closed', StringType(), True), StructField('current_bid', StringType(), True), StructField('high_bidder', StructType([StructField('bidder_name', StringType(), True), StructField('bidder_rating', StringType(), True)]), True), StructField('id_num', StringType(), True), StructField('location', StringType(), True), StructField('notes', StringType(), True), StructField('num_bids', StringType(), True), StructField('num_items', StringType(), True), StructField('opened', StringType(), True), StructField('started_at', StringType(), True), StructField('time_left', StringType(), True)]), True), StructField('bid_history', StructType([StructField('highest_bid_amount', StringType(), True), StructField('quantity', StringType(), True)]), True), StructField('buyer_protection_info', StringType(), True), StructField('item_info', StructType([StructField('brand', StringType(), True), StructField('cpu', StringType(), True), StructField('description', StringType(), True), StructField('hard_drive', StringType(), True), StructField('memory', StringType(), True)]), True), StructField('payment_types', StringType(), True), StructField('seller_info', StructType([StructField('seller_name', StringType(), True), StructField('seller_rating', StringType(), True)]), True), StructField('shipping_info', StringType(), True)])

    hadoop_conf = spark._jsc.hadoopConfiguration()

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    r = redis.Redis(host='localhost', port=6379, db=0)

    qname = 'xmlfiles'
    
    
    sz = r.llen(qname)
    for _ in range(sz):
        file = r.lpop(qname)
        file = file.decode()

        print("Processing Filewwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww:", file)

        path_to_process = f"hdfs:///Files/multiple_files/{file}"
        print(path_to_process)

        df = (
            spark.read.format("xml")
            .option("rowTag", "listing")
            .schema(schema)
            .load(path_to_process)
        )

        print("Read the datafile")

        df.writeTo("hadoop_cat.db_name.xml_table").append()

        print("Written the datafile")

        path_to_move = f"hdfs:///Files/xml/{file}"
        
        path_to_move = spark._jvm.org.apache.hadoop.fs.Path(path_to_move)
        path_to_process = spark._jvm.org.apache.hadoop.fs.Path(path_to_process)

        fs.rename(path_to_process, path_to_move)

    
    
    