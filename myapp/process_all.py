from pyspark.sql import SparkSession
import xmlprocess
import jsonprocess
import csvprocess
import pdfprocess

def main():
    spark = (
    SparkSession.builder
    .appName("all_in_process")
    .master("spark://192.168.1.214:7077")
    .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hadoop_cat.type", "hadoop")
    .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs:///Files/iceberg/warehouse")
    .config("spark.executor.memory", "14g")
    .config("spark.executor.cores", "8")
    .config("spark.cores.max", "32")
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.15.0")
    .getOrCreate()
    )

    xmlprocess.run(spark)
    jsonprocess.run(spark)
    csvprocess.run(spark)
    pdfprocess.run(spark)

    spark.stop()

if __name__ == "__main__":
    main()
