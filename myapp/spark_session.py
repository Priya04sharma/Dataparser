# spark_session.py

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergTablePreview") \
    .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_cat.warehouse", "/Files/iceberg/warehouse") \
    .getOrCreate()
