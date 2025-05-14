# yourapp/utils/spark_session.py
from pyspark.sql import SparkSession

_spark = None

def get_spark_session():
    global _spark
    if _spark is None:
        _spark = SparkSession.builder \
            .appName("Django Spark Session") \
            .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
            .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs:///Files/iceberg/warehouse") \
            .getOrCreate()
    return _spark
