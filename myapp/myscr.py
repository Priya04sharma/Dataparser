from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CatalogMigration") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs://Files/iceberg/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()



# Define variables for database and table names
db_name = "db_name"
table_name = "csv_table"  # replace with your actual table name

# Set current catalog to hadoop_cat to access the original table metadata
spark.catalog.setCurrentCatalog("hadoop_cat")

# Get the location of the existing table
location = spark.sql(f"DESCRIBE FORMATTED {db_name}.{table_name}") \
                .filter("col_name = 'Location'") \
                .select("data_type") \
                .collect()[0][0]

print(f"Table location: {location}")

# Now switch to spark_catalog (optional, but good for clarity)
spark.catalog.setCurrentCatalog("spark_catalog")

# Create the table in spark_catalog using Iceberg and the existing location
spark.sql(f"""
    CREATE TABLE {db_name}.{table_name}
    USING iceberg
    LOCATION '{location}'
""")

print(f"Table {db_name}.{table_name} registered in spark_catalog at location {location}")
